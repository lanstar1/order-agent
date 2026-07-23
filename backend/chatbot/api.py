#!/usr/bin/env python3
"""사내 영업·CS 상담 챗봇 REST API + 웹 UI 서빙.

    uvicorn chatbot.api:app --host 0.0.0.0 --port 8080
    # http://localhost:8080/        웹 UI
    # http://localhost:8080/docs    OpenAPI
    # http://localhost:8080/cert/   인증 조회 API(cert_lookup) 그대로 마운트

이 계층이 지키는 것(코어의 절대규칙을 API 에서 깨뜨리지 않기 위한 것들)
  * **cert 경로 응답 텍스트를 손대지 않는다.** engine 이 만든 text 를 그대로 실어 나른다.
    notices 를 본문에서 떼어내지도 않는다 — policy.py 가 붙인 주의문구가 UI 버그 하나로
    사라지면 안 된다. UI 의 주의문구 박스는 '한 번 더' 보여주는 것이지 본문의 대체가 아니다.
  * **모드는 조용히 넘어가지 않는다.** internal/customer 외의 값은 422 (Enum 검증).
    'customerr' 를 internal 로 흡수하면 고객에게 사내 전용 정보(파일 경로·O* 추론매핑)가 나간다.
  * **근거 배지를 서버가 계산한다.** 무엇을 근거로 만든 답인지의 판정을 UI(JS)에 맡기면
    화면마다 달라진다. sources[] -> badges[] 변환은 여기 한 곳에서만 한다.
  * 인덱스·KB·마스터는 기동 시 1회 로드한다(6.3MB JSON 재파싱 금지).
"""
from __future__ import annotations

import os
import time
import traceback
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from cert_lookup import cli as cert_cli
from cert_lookup import loader as cert_loader
from cert_lookup import policy as cert_policy
from cert_lookup.api import app as cert_app

from . import config, engine, history, llm, router, session, spec

TITLE = '랜스타 사내 영업·CS 상담 챗봇 API'
VERSION = '1.0.0'
WEB_DIR = Path(__file__).resolve().parent / 'web'
INDEX_HTML = WEB_DIR / 'index.html'


class Mode(str, Enum):
    """internal = 사내 영업·CS(파일 경로·O* 추론매핑 노출). customer = 고객 직접 노출."""
    internal = cert_policy.INTERNAL
    customer = cert_policy.CUSTOMER


MODE_Q = Query(Mode.internal, description='internal(기본, 사내 전용) | customer')

#: 기동 시 수집한 로드 결과. /health 가 이걸 읽는다.
STATE: Dict[str, Any] = {'started_at': None, 'kb': {}, 'master': {}, 'errors': []}


# ---------------------------------------------------------------- 근거 배지

#: sources[].kind -> 화면 배지. 담당자가 '무엇을 근거로 한 답인가'를 한눈에 봐야 한다.
#: level: 'strong' 확정 근거 / 'weak' 참고 / 'ai' AI 추정(확정 사실 아님)
BADGE_SPEC: Dict[str, Dict[str, str]] = {
    config.SOURCE_CERT: {
        'key': 'cert', 'label': '인증KB', 'level': 'strong',
        'hint': 'cert_lookup 인증·기술자료 KB. 이 경로는 제품분석·상담이력을 섞지 않습니다.'},
    config.SOURCE_HISTORY: {
        'key': 'history', 'label': '상담이력', 'level': 'weak',
        'hint': '2023~2025년 과거 상담 기록. 현재 사양·정책과 다를 수 있습니다.'},
    config.SOURCE_SPEC: {
        'key': 'spec', 'label': '제품분석(AI추정)', 'level': 'ai',
        'hint': '제품 이미지 한 장을 보고 만든 AI 추론입니다. 확정 사양이 아닙니다.'},
    config.SOURCE_LLM: {
        'key': 'llm', 'label': 'LLM생성', 'level': 'ai',
        'hint': '문장 다듬기에만 LLM 을 썼습니다. 근거 원문이 함께 표시됩니다.'},
    config.SOURCE_RULE: {
        'key': 'rule', 'label': '규칙기반', 'level': 'weak',
        'hint': 'LLM 없이 규칙으로 조립한 응답입니다.'},
}

ROUTE_LABEL = {'cert': '인증·기술자료', 'legacy': '일반 문의', 'mixed': '혼합(섹션 분리)'}


def badges(sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """sources[] 를 화면 배지로 접는다. 등장 순서 보존, 같은 종류는 건수만 누적."""
    out: List[Dict[str, Any]] = []
    seen: Dict[str, Dict[str, Any]] = {}
    for s in sources or []:
        kind = s.get('kind') or ''
        spec_ = BADGE_SPEC.get(kind)
        if spec_ is None:                      # 미등록 근거도 숨기지 않는다
            spec_ = {'key': 'other', 'label': kind or '기타', 'level': 'weak', 'hint': ''}
        b = seen.get(spec_['key'])
        if b is None:
            b = dict(spec_, count=0, sections=[], models=[])
            seen[spec_['key']] = b
            out.append(b)
        b['count'] += 1
        sec = s.get('section')
        if sec is not None and sec not in b['sections']:
            b['sections'].append(sec)
        m = s.get('model')
        if m and m not in b['models']:
            b['models'].append(m)
    return out


def collect_files(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """응답에 딸린 파일 위치. cert 경로에서만 나온다(customer 모드는 경로가 비어 있다)."""
    out: List[Dict[str, Any]] = []
    cert = (data or {}).get('cert') or {}
    for d in cert.get('documents') or []:
        f = d.get('file') or {}
        ref = {
            'doc_id': d.get('doc_id'),
            'doc_type': d.get('doc_type'),
            'title_ko': d.get('title_ko'),
            'filename': f.get('filename') or d.get('filename') or '',
            'path': f.get('path') or d.get('path') or '',
            'url': f.get('url') or '',
            'folder_url': f.get('folder_url') or '',
            'status': f.get('status') or '',
            'note': f.get('note') or '',
            'deliverable': d.get('deliverable'),
            'quotable': d.get('quotable'),
            'text_extractable': d.get('text_extractable'),
        }
        if ref['filename'] or ref['path'] or ref['url'] or ref['folder_url']:
            out.append(ref)
    return out


def reply_envelope(reply: engine.ChatReply, session_id: str, mode: str,
                   elapsed_ms: float) -> Dict[str, Any]:
    """모든 대화 응답이 같은 모양으로 나간다: answer(사람) + badges(근거) + data(기계)."""
    d = reply.as_dict()
    return {
        'ok': True,
        'answer': reply.text,               # 사람이 읽는 본문 — 절대 재작성하지 않는다
        'text': reply.text,
        'route': reply.route,
        'route_label': ROUTE_LABEL.get(reply.route, reply.route),
        'route_reason': (reply.data or {}).get('route_reason', ''),
        'sources': d['sources'],
        'badges': badges(d['sources']),
        'notices': d['notices'],
        'needs_clarification': d['needs_clarification'],
        'candidates': d['candidates'],
        'files': collect_files(reply.data or {}),
        'models': (reply.data or {}).get('models') or [],
        'model_source': (reply.data or {}).get('model_source', ''),
        'llm_used': bool(((reply.data or {}).get('legacy') or reply.data or {})
                         .get('llm_used')),
        'session_id': session_id,
        'mode': mode,
        'elapsed_ms': round(elapsed_ms, 1),
        'data': d['data'],
    }


# ---------------------------------------------------------------- 앱 수명주기

@asynccontextmanager
async def lifespan(app: FastAPI):
    """인덱스 1회 로드. 마운트된 서브앱의 lifespan 은 실행되지 않으므로 KB 도 여기서 연다."""
    STATE['errors'] = []
    t0 = time.time()
    try:
        kb = cert_loader.load_kb()
        cert_cli.drive_resolver(kb)          # 동기화 루트 탐색도 프로세스당 1회
        STATE['kb'] = {
            'loaded': True,
            'documents': len(kb.docs), 'families': len(kb.families),
            'kc_models': len(kb.kc), 'rohs_requests': len(kb.rohs_requests),
            'problems': list(kb.problems or []),
        }
        cert_app.state.kb = kb               # 마운트된 cert 앱도 같은 인스턴스를 본다
    except Exception as exc:                 # KB 가 없어도 서버는 떠야 진단이 가능하다
        STATE['kb'] = {'loaded': False, 'problems': [f'{type(exc).__name__}: {exc}']}
        STATE['errors'].append(f'cert KB 로드 실패: {exc}')
    try:
        idx = history.get_index()            # 상담이력 인덱스 빌드(실측 0.42초)
        master = spec.load_master()
        STATE['master'] = {
            'loaded': True,
            'path': str(config.MASTER_PATH),
            'models': len(master),
            'qa_rows': len(idx.rows),
            'models_with_history': len(idx.models),
            'dropped_template': idx.dropped_template,
            'key_merges': len(getattr(idx, 'key_merges', {}) or {}),
            'aliases': len(idx.aliases),
        }
    except Exception as exc:
        STATE['master'] = {'loaded': False, 'path': str(config.MASTER_PATH),
                           'error': f'{type(exc).__name__}: {exc}'}
        STATE['errors'].append(f'제품 마스터 로드 실패: {exc}')
    STATE['started_at'] = time.time()
    STATE['boot_sec'] = round(time.time() - t0, 3)
    yield


app = FastAPI(title=TITLE, version=VERSION, lifespan=lifespan,
              description='랜스타 사내 영업·CS 상담 챗봇. 인증 문의는 cert_lookup 이 유일 근거이며, '
                          '제품분석은 이미지 기반 AI 추정이라 확정 사양이 아니다.')

_origins = [o.strip() for o in os.environ.get('CERT_KB_CORS_ORIGINS', '*').split(',')
            if o.strip()] or ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials='*' not in _origins,
    allow_methods=['GET', 'POST', 'OPTIONS'],
    allow_headers=['*'],
)

#: 한 포트에서 챗봇과 인증 조회 API 를 모두 쓴다.
app.mount('/cert', cert_app)


# ---------------------------------------------------------------- 요청 모델

class ChatRequest(BaseModel):
    message: str = Field(..., description='사용자 문장. 예: "LS-6UTPD-3MG RoHS 인증서 주세요"')
    session_id: str = Field('', description='대화 맥락 키. 비우면 상태 없는 1회성 응답.')
    mode: Mode = Field(Mode.internal, description='internal(기본) | customer. 오타는 422.')


class ClarifyRequest(BaseModel):
    session_id: str = Field(..., min_length=1,
                            description='되묻기를 받은 그 세션. 비우면 대기 상태가 없다.')
    choice: str = Field(..., min_length=1,
                        description="후보 선택. '2번' / '두번째' / 'LS-6UTPD' 전부 인식한다.")
    mode: Mode = Field(Mode.internal)


# ---------------------------------------------------------------- 오류 처리

@app.exception_handler(ValueError)
async def _value_error(request, exc: ValueError):
    """코어가 올린 ValueError(모드 오타, 모델 스코프 누락)는 fail closed 로 422.

    Enum 검증이 이미 모드를 막지만, 이 핸들러가 없으면 검증을 우회한 경로에서
    ValueError 가 500 으로 나가 '서버 장애'와 '잘못된 요청'이 구분되지 않는다.
    """
    return JSONResponse(status_code=422, content={
        'ok': False, 'detail': {'message': str(exc), 'type': 'ValueError'}})


def _guard(fn, *args, **kwargs):
    """코어 예외를 500 대신 의미 있는 HTTP 로 바꾼다."""
    try:
        return fn(*args, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail={'message': str(exc)}) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail={
            'message': f'데이터 파일을 열 수 없습니다: {exc}',
            'hint': 'GET /health 의 problems 를 확인하십시오.'}) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={
            'message': f'{type(exc).__name__}: {exc}',
            'trace': traceback.format_exc(limit=4).splitlines()[-3:]}) from exc


# ---------------------------------------------------------------- 웹 UI

@app.get('/', include_in_schema=False)
def ui():
    if not INDEX_HTML.exists():
        raise HTTPException(status_code=404, detail={
            'message': '웹 UI 파일이 없습니다.', 'expected': str(INDEX_HTML)})
    return FileResponse(str(INDEX_HTML), media_type='text/html; charset=utf-8')


@app.get('/api', summary='엔드포인트 목록')
def api_index() -> Dict[str, Any]:
    return {
        'name': TITLE, 'version': VERSION,
        'endpoints': [
            'GET  /                     사내 웹 UI',
            'GET  /health               KB·마스터·LLM 상태',
            'POST /chat                 {message, session_id?, mode?}',
            'POST /clarify              {session_id, choice, mode?}',
            'GET  /history?model=       모델별 과거 상담 이력 전량',
            'GET  /spec?model=          제품분석(화이트리스트 + 오염 플래그)',
            'GET  /route?message=       0단계 라우팅 판정만(디버깅)',
            'GET  /session/{id}         세션 상태',
            'GET  /cert/                인증 조회 API (cert_lookup)',
        ],
        'badge_legend': [dict(v, kind=k) for k, v in BADGE_SPEC.items()],
        'modes': [m.value for m in Mode],
        'docs': '/docs',
        'warning': ('제품분석(제품분석 필드)은 제품 이미지 기반 AI 추정이며 확정 사실이 아닙니다. '
                    '인증·자료 문의의 근거로는 절대 사용하지 않습니다.'),
    }


# ---------------------------------------------------------------- 상태

def _ensure_state() -> None:
    """lifespan 이 안 돈 경우(마운트·테스트 클라이언트)에도 실상태를 반영한다.

    조회 자체는 지연 로딩으로 정상 동작하는데 /health 만 '미로드'라고 답하면
    운영자가 멀쩡한 서버를 죽은 것으로 오판한다.
    """
    if not (STATE.get('kb') or {}).get('loaded'):
        try:
            kb = cert_loader.load_kb()
            if kb.docs:
                STATE['kb'] = {'loaded': True, 'lazy': True,
                               'documents': len(kb.docs), 'families': len(kb.families),
                               'kc_models': len(kb.kc),
                               'rohs_requests': len(kb.rohs_requests),
                               'problems': list(kb.problems or [])}
        except Exception:                       # 진단용 엔드포인트가 죽으면 안 된다
            pass
    if not (STATE.get('master') or {}).get('loaded'):
        try:
            master = spec.load_master()
            idx = history.get_index()
            if master:
                STATE['master'] = {'loaded': True, 'lazy': True,
                                   'path': str(config.MASTER_PATH),
                                   'models': len(master), 'qa_rows': len(idx.rows),
                                   'models_with_history': len(idx.models)}
        except Exception:
            pass


@app.get('/health', summary='KB 로딩 · 마스터 로딩 · LLM 가용 여부')
def health() -> Dict[str, Any]:
    _ensure_state()
    kb_state = dict(STATE.get('kb') or {})
    master_state = dict(STATE.get('master') or {})
    problems: List[str] = list(STATE.get('errors') or [])
    problems += [f'인증KB: {p}' for p in (kb_state.get('problems') or [])]
    if not kb_state.get('loaded'):
        problems.append('인증KB 미로드 — 인증 문의에 답할 수 없습니다.')
    if not master_state.get('loaded'):
        problems.append('제품 마스터 미로드 — 상담이력·제품분석 경로가 동작하지 않습니다.')
    if not INDEX_HTML.exists():
        problems.append(f'웹 UI 파일 없음: {INDEX_HTML}')
    key_set = bool(os.environ.get(config.LLM_API_KEY_ENV))
    llm_ok = llm.available()
    return {
        'ok': not problems,
        'version': VERSION,
        'uptime_sec': round(time.time() - (STATE.get('started_at') or time.time()), 1),
        'boot_sec': STATE.get('boot_sec'),
        'cert_kb': kb_state,
        'master': master_state,
        'llm': {
            'available': llm_ok,
            'api_key_env': config.LLM_API_KEY_ENV,
            'api_key_present': key_set,
            'model': config.LLM_MODEL,
            'note': ('LLM 은 문장 다듬기 보조입니다. 키가 없어도 챗봇 전체가 규칙 기반으로 '
                     '정상 동작합니다(기본 동작).' if not llm_ok else
                     'LLM 은 legacy 경로 문장 다듬기에만 사용되며 인증 경로에는 개입하지 않습니다.'),
        },
        'sessions': len(session.STORE),
        'web_ui': {'path': str(INDEX_HTML), 'exists': INDEX_HTML.exists()},
        'modes': [m.value for m in Mode],
        'problems': problems,
    }


# ---------------------------------------------------------------- 대화

@app.post('/chat', summary='상담 한 턴')
def chat(req: ChatRequest = Body(...)) -> Dict[str, Any]:
    t0 = time.time()
    reply = _guard(engine.chat, req.message, req.session_id, req.mode.value)
    return reply_envelope(reply, req.session_id, req.mode.value,
                          (time.time() - t0) * 1000)


@app.post('/clarify', summary='되묻기 응답 처리')
def clarify(req: ClarifyRequest = Body(...)) -> Dict[str, Any]:
    """대기 중인 되묻기에 후보를 골라 답한다.

    후보를 특정하지 못하면 **임의로 고르지 않는다** — ok=false 로 다시 물어본다.
    (모델을 잘못 고르면 다른 제품의 인증 자료를 자신 있게 답하게 된다.)
    """
    sess = session.get(req.session_id)
    pending = sess.pending_clarification
    if not pending:
        raise HTTPException(status_code=409, detail={
            'message': '이 세션에는 대기 중인 되묻기가 없습니다.',
            'session_id': req.session_id,
            'hint': 'POST /chat 응답의 needs_clarification=true 일 때만 사용합니다.'})
    cands = list(pending.get('candidates') or [])
    chosen = session.resolve_choice(req.choice, cands)
    if not chosen:
        return {'ok': False, 'resolved': None, 'candidates': cands,
                'session_id': req.session_id, 'mode': req.mode.value,
                'needs_clarification': True,
                'answer': (f'{config.GREETING} 선택하신 항목을 특정하지 못했습니다. '
                           f'후보 중 하나를 정확히 알려주십시오: {", ".join(cands)}'),
                'message': '후보를 특정하지 못했습니다.'}
    t0 = time.time()
    reply = _guard(engine.chat, req.choice, req.session_id, req.mode.value)
    out = reply_envelope(reply, req.session_id, req.mode.value, (time.time() - t0) * 1000)
    out['resolved'] = chosen
    out['original_message'] = pending.get('message')
    return out


@app.get('/session/{session_id}', summary='세션 상태(직전 모델·되묻기 대기)')
def session_state(session_id: str) -> Dict[str, Any]:
    s = session.get(session_id)
    return {'ok': True, **s.as_dict(),
            'history': [{'message': t['message'], 'route': t['route'], 'at': t['at']}
                        for t in s.turns[-10:]]}


@app.get('/route', summary='0단계 라우팅 판정만(디버깅)')
def route_only(message: str = Query(..., min_length=1,
                                    description='판정할 문장')) -> Dict[str, Any]:
    r = _guard(router.route, message)
    return {'ok': True, 'route': r['route'],
            'route_label': ROUTE_LABEL.get(r['route'], r['route']),
            'reason': r['reason'], 'parsed': r['parsed'], 'flags': r['flags'],
            'models': engine.extract_models(message)}


# ---------------------------------------------------------------- 근거 직접 조회

@app.get('/history', summary='모델별 과거 상담 이력(담당자 직접 확인용)')
def model_history(model: str = Query(..., min_length=1, description='모델명. 예: LS-UC314'),
                  query: Optional[str] = Query(None, description='주면 유사도 순으로 정렬'),
                  include_siblings: bool = Query(True, description='형제 모델 이력 포함'),
                  limit: int = Query(200, ge=1, le=2000)) -> Dict[str, Any]:
    """PII 는 인덱스 빌드 시점에 이미 마스킹돼 있다. URL 은 인용 직전에 제거한다.

    `quotable=false` 인 행은 그대로 고객에게 복사하면 안 된다(시점 의존 정보 등).
    """
    key = history.normalize_model_key(model)
    if query:
        hits = _guard(history.search_qa, router.mask_models(query).strip(), key,
                      limit=limit, include_siblings=include_siblings)
        ranked = True
    else:
        hits = _guard(history.history_for_model, key, include_siblings)[:limit]
        ranked = False
    rows = []
    for h in hits:
        row = h.as_dict()
        row['answer_clean'] = history.clean_answer(row['answer'])
        row['answer_cert_stripped'], row['cert_dropped'] = config.strip_cert_claims(
            row['answer_clean'])
        rows.append(row)
    exact = history.model_row_count(key)
    return {
        'ok': True, 'model': key, 'requested': model,
        'ranked_by_query': ranked, 'query': query,
        'count': len(rows), 'exact_rows': exact,
        'include_siblings': include_siblings,
        'source': config.SOURCE_HISTORY,
        'notices': [config.HISTORY_NOTICE] if rows else [],
        'rows': rows,
    }


@app.get('/spec', summary='제품 사양 참고 정보(화이트리스트 + 오염 플래그)')
def model_spec(model: str = Query(..., min_length=1,
                                  description='모델명. 예: LS-300HO')) -> Dict[str, Any]:
    """★ 이 정보는 제품 이미지 한 장을 보고 만든 AI 추정이다. 확정 사양이 아니다.

    `usable_as_spec=false` 면 사양 답변 근거로 쓰면 안 되고, `cert_contaminated=true` 면
    원본 제품분석에 근거 없는 인증 주장이 있었다는 뜻이다(본 응답에서는 문장 단위로 삭제됨).
    """
    ctx = _guard(spec.spec_context, model)
    frags = _guard(spec.cert_fragments, ctx.get('model') or model)
    return {
        'ok': True,
        'model': ctx.get('model'),
        'requested': model,
        'found': ctx.get('found'),
        'has_analysis': ctx.get('has_analysis'),
        'usable_as_spec': ctx.get('usable_as_spec'),
        'verification': ctx.get('verification'),
        'verification_reason': ctx.get('verification_reason'),
        'cert_contaminated': ctx.get('cert_contaminated'),
        'cert_claims_removed': frags,
        'dropped_cert': ctx.get('dropped_cert'),
        'quant_flags': ctx.get('quant_flags'),
        'blocked_fields': ctx.get('blocked_fields'),
        'excluded_fields': ctx.get('excluded_fields'),
        'fields': ctx.get('fields'),
        'rendered': spec.render_spec(ctx),
        'image': ctx.get('image'),
        'category': ctx.get('category'),
        'product_name': ctx.get('product_name'),
        'source': config.SOURCE_SPEC,
        'ai_generated': True,
        'notices': ctx.get('notices'),
        'warning': ('제품분석은 제품 이미지 기반 AI 추정입니다. 확정 사실이 아니며 '
                    '인증·자료 문의의 근거로 사용해서는 안 됩니다.'),
    }


@app.get('/models/suggest', summary='상품명으로 모델 후보 제안(자동 선택 금지)')
def suggest(text: str = Query(..., min_length=1, description="예: '4K 120hz hdmi 선택기 3:1'"),
            limit: int = Query(5, ge=1, le=20)) -> Dict[str, Any]:
    cands = _guard(history.guess_models_by_name, text, limit)
    return {'ok': True, 'text': text,
            'candidates': [{'model': m, 'score': s, 'name': n} for m, s, n in cands],
            'note': '별칭 사전 기반 제안입니다. 자동 선택하지 말고 담당자가 고르십시오.'}
