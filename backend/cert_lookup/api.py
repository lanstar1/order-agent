#!/usr/bin/env python3
"""REST API — 챗봇/사내 웹UI 가 붙는 얇은 계층.

    uvicorn cert_lookup.api:app --host 0.0.0.0 --port 8080

여기서는 판단하지 않는다. 모델 해석·보유 판정·주의문구는 전부 엔진(search/policy/render)
이 이미 끝낸 것을 그대로 실어 나른다. 이 계층이 지키는 것은 세 가지뿐이다.

  * 인덱스는 기동 시 1회만 읽는다(6MB JSONL 재파싱 금지).
  * 모드는 조용히 넘어가지 않는다 — internal/customer 외의 값은 422 로 거절한다.
    ('customerr' 를 internal 로 흡수하면 고객에게 사내 전용 정보가 나간다.)
  * 되묻기(stem_ambiguous)와 미발견은 에러가 아니다. 챗봇이 이어서 되물어야 하므로
    HTTP 200 + needs_clarification / suggestions 로 돌려준다.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from . import cli, loader, policy as policy_mod, render, search
from .drive import find_drive_root

TITLE = '랜스타 제품 인증·기술자료 조회 API'
VERSION = '1.0.0'


class Mode(str, Enum):
    """internal = 사내 영업·CS(추론 매핑 노출). customer = 고객 직접 노출."""
    internal = policy_mod.INTERNAL
    customer = policy_mod.CUSTOMER


MODE_Q = Query(Mode.internal, description='internal(기본, 사내 전용) | customer')


# ---------------------------------------------------------------- 앱 수명주기

@asynccontextmanager
async def lifespan(app: FastAPI):
    kb = loader.load_kb()                 # 인덱스 1회 로드 후 프로세스 내내 재사용
    cli.drive_resolver(kb)                # 동기화 루트 탐색도 여기서 한 번만
    app.state.kb = kb
    yield


app = FastAPI(title=TITLE, version=VERSION, lifespan=lifespan,
              description='모델명으로 인증·기술자료 보유현황과 파일 위치를 조회한다. '
                          '없는 자료는 만들어내지 않고 "미보유 + 공급사 요청 필요"로 답한다.')

#: 사내망 배포라 기본은 전체 허용. 외부 노출 시 CERT_KB_CORS_ORIGINS 로 좁힌다.
_origins = [o.strip() for o in os.environ.get('CERT_KB_CORS_ORIGINS', '*').split(',')
            if o.strip()] or ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials='*' not in _origins,   # '*' 와 credentials 는 브라우저가 거부한다
    allow_methods=['GET', 'POST', 'OPTIONS'],
    allow_headers=['*'],
)


def _kb() -> loader.KnowledgeBase:
    return loader.load_kb()


# ---------------------------------------------------------------- 응답 봉투

def envelope(ans: render.Answer, **extra: Any) -> Dict[str, Any]:
    """모든 엔드포인트가 같은 모양으로 답한다: answer(사람) + data(기계)."""
    data = ans.data or {}
    kind = data.get('kind', '')
    out: Dict[str, Any] = {
        'ok': True,
        'kind': kind,
        'query': data.get('query'),
        'mode': data.get('mode'),
        'found': kind not in ('not_found', 'ambiguous', 'need_model'),
        'needs_clarification': kind in ('ambiguous', 'need_model'),
        'candidates': data.get('candidates') or [],
        'suggestions': data.get('suggestions') or [],
        'notices': data.get('notices') or [],
        'answer': ans.text,
        'data': data,
    }
    out.update(extra)
    return out


NEED_MODEL_TEXT = (f'{render.GREETING} 문의하신 내용에서 모델명을 확인하지 못했습니다. '
                   'LS-6UTPD-3MG 처럼 제품 모델명을 알려주시면 보유 자료를 바로 확인해 '
                   '드리겠습니다.')


def _need_model(query: str, mode: str, parsed: Optional[Dict[str, Any]] = None) -> render.Answer:
    return render.Answer(NEED_MODEL_TEXT,
                         {'kind': 'need_model', 'query': query, 'mode': mode,
                          'parsed': parsed or {}, 'notices': []})


def _today() -> Optional[str]:
    """만료 판정 기준일. 요청 파라미터로 받지 않는다 — 과거 날짜를 넣으면 만료 경고가
    통째로 사라져 만료된 인증서가 유효한 것처럼 응답된다. 테스트만 환경변수로 바꾼다."""
    return os.environ.get('CERT_KB_TODAY') or None


def _check_doc_type(doc_type: str) -> str:
    dt = search.normalize_doc_type(doc_type)
    if dt is None:
        raise HTTPException(status_code=400, detail={
            'message': f'알 수 없는 자료유형: {doc_type}',
            'known_doc_types': list(loader.KNOWN_DOC_TYPES),
            'hint': "'MSDS/SDS' 는 경로에 슬래시를 쓸 수 없으므로 'MSDS' 로 요청하십시오.",
        })
    return dt


# ---------------------------------------------------------------- 핵심 조회

def _lookup_answer(model: str, doc_type: Optional[str], mode: str,
                   intent: str = 'files', today: Optional[str] = None) -> Dict[str, Any]:
    """모델(+자료유형) 조회 -> 봉투. 문서 전체 목록을 함께 싣는다."""
    kb = _kb()
    pol = policy_mod.get_policy(mode)
    res = search.lookup(model, doc_type, kb, today)

    if res.ambiguous:
        return envelope(render.render_ambiguous(res))
    if intent == 'kc' or (not res.cert.found and res.kc.status != search.KC_NOT_INDEXED):
        return envelope(render.render_kc(res.kc, pol))
    if not res.found:
        return envelope(render.render_not_found(res))

    if intent == 'content' and res.type_result is not None:
        ans = render.render_content(res, pol)
    elif res.type_result is not None:
        ans = render.render_type(res, pol)
    else:
        ans = render.render_family(res.cert, pol, res.kc)

    docs = (cli.family_documents(res.cert, pol, kb, today) if res.type_result is None
            else [cli.doc_dict(d, pol)
                  for d in pol.filter_documents(res.type_result.documents,
                                                inferred=res.type_result.inferred)[0]])
    return envelope(ans,
                    documents=docs,
                    families=res.cert.families,
                    members=res.cert.members,
                    coverage=res.cert.coverage,
                    kc_status=res.kc.status,
                    match_reason=res.cert.resolution.reason,
                    confident=res.cert.resolution.confident)


# ---------------------------------------------------------------- 엔드포인트

@app.get('/', summary='엔드포인트 목록')
def index() -> Dict[str, Any]:
    return {
        'name': TITLE, 'version': VERSION,
        'endpoints': [
            'GET  /health',
            'GET  /lookup?model=LS-6UTPD-3MG&mode=internal',
            'GET  /lookup/{model}/{doc_type}',
            'GET  /kc?model=LS-OVERC',
            'GET  /kc/{model}/contains?item=필증',
            'GET  /file/{doc_id}',
            'GET  /coverage?model=LS-6UTPD-3MG',
            'POST /ask   {"message": "...", "mode": "internal"}',
        ],
        'doc_types': list(loader.KNOWN_DOC_TYPES),
        'modes': [m.value for m in Mode],
        'docs': '/docs',
    }


@app.get('/health', summary='기동 상태와 인덱스 규모')
def health() -> Dict[str, Any]:
    kb = _kb()
    root = find_drive_root()
    return {
        'ok': not kb.problems,
        'version': VERSION,
        'counts': {'documents': len(kb.docs), 'families': len(kb.families),
                   'kc_models': len(kb.kc), 'rohs_requests': len(kb.rohs_requests)},
        'drive': {
            'sync_root': root,
            'sync_root_found': bool(root),
            'api_credentials': bool(os.environ.get('CERT_KB_DRIVE_TOKEN')
                                    or os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')),
            'api_disabled': os.environ.get('CERT_KB_DRIVE_DISABLE_API') == '1',
        },
        'cors_origins': _origins,
        'problems': kb.problems,
    }


@app.get('/lookup', summary='모델 조회(보유현황 + 문서 목록 + 응답문)')
def lookup(model: str = Query(..., description='모델명. 예: LS-6UTPD-3MG'),
           mode: Mode = MODE_Q,
           doc_type: Optional[str] = Query(None, description='선택. 지정 시 유형별 조회'),
           intent: str = Query('files', pattern='^(files|content|kc|coverage)$')
           ) -> Dict[str, Any]:
    dt = _check_doc_type(doc_type) if doc_type else None
    if intent == 'coverage':
        dt, intent = None, 'files'
    return _lookup_answer(model, dt, mode.value, intent, _today())


# model 에 {model:path} 를 쓰는 이유: 제품군 키 'LS-FMD / LS-FMP' 처럼 슬래시가 든 이름이
# 실제로 하나 있다. 평범한 {model} 이면 라우팅이 쪼개져 404 가 나고, 되묻기 후보로 그 이름을
# 돌려준 챗봇이 그대로 재질의하면 막힌다. 마지막 세그먼트는 doc_type 으로 고정된다.
@app.get('/lookup/{model:path}/{doc_type}', summary='자료유형 지정 조회')
def lookup_type(model: str, doc_type: str, mode: Mode = MODE_Q) -> Dict[str, Any]:
    return _lookup_answer(model, _check_doc_type(doc_type), mode.value, 'files', _today())


@app.get('/coverage', summary='유형별 O/자재/O*/X 요약')
def coverage(model: str = Query(...), mode: Mode = MODE_Q) -> Dict[str, Any]:
    kb = _kb()
    pol = policy_mod.get_policy(mode.value)
    res = search.lookup(model, None, kb, _today())
    if res.ambiguous:
        return envelope(render.render_ambiguous(res))
    if not res.cert.found:
        if res.kc.status != search.KC_NOT_INDEXED:
            return envelope(render.render_kc(res.kc, pol))
        return envelope(render.render_not_found(res))

    matrix: Dict[str, Any] = {}
    for col, tr in res.cert.types.items():
        matrix[col] = {
            'mark': tr.mark, 'label': render.MARK_LABEL.get(tr.mark, tr.mark),
            'held': tr.held, 'inferred': tr.inferred, 'bundle': tr.bundle,
            'documents': len(tr.documents),
            'material_covered': tr.material_covered,
            'material_missing': tr.material_missing,
            'material_coverage_known': tr.material_coverage_known,
            'secondary_only': tr.secondary_only,
            'not_held_reason': tr.not_held_reason,
        }
    ans = render.render_family(res.cert, pol, res.kc)
    return envelope(ans, coverage=res.cert.coverage, matrix=matrix,
                    other_types=res.cert.other_types,
                    families=res.cert.families, members=res.cert.members,
                    document_count=res.cert.document_count,
                    kc_status=res.kc.status)


@app.get('/kc', summary='KC 적합등록 자료 + 내부 서류 목록')
def kc(model: str = Query(..., description='모델명. 예: LS-OVERC'),
       mode: Mode = MODE_Q) -> Dict[str, Any]:
    kb = _kb()
    pol = policy_mod.get_policy(mode.value)
    res = search.lookup(model, None, kb)
    if res.kc.resolution.ambiguous:
        return envelope(render.render_ambiguous(res))
    ans = render.render_kc(res.kc, pol)
    return envelope(ans,
                    status=res.kc.status,
                    kc_model=res.kc.model,
                    contents=res.kc.contents,
                    kinds=res.kc.kinds,
                    has_certificate=res.kc.has_certificate,
                    cert_families=res.cert.families)


@app.get('/kc/{model:path}/contains', summary='zip 을 열지 않고 내부 서류 포함 여부')
def kc_contains(model: str, item: str = Query(..., description='예: 필증 / 외관도 / 위임서'),
                mode: Mode = MODE_Q) -> Dict[str, Any]:
    kb = _kb()
    res = search.lookup(model, None, kb)
    if res.kc.resolution.ambiguous:
        return envelope(render.render_ambiguous(res))
    k = res.kc
    info = cli.kc_contains(k, item)
    ans = render.render_kc_contains(k, info, model, policy_mod.get_policy(mode.value))
    return envelope(ans, item=info['item'], matched_kind=info['kind'],
                    contains=info['contains'], files=info['files'],
                    matched_by=info['matched_by'])


@app.get('/file/{doc_id}', summary='드라이브 링크/로컬 경로 해석')
def file(doc_id: str, mode: Mode = MODE_Q) -> Dict[str, Any]:
    ref = cli.file_ref(doc_id, mode.value, _kb())
    if ref is None:
        raise HTTPException(status_code=404, detail={
            'message': f'문서를 찾을 수 없습니다: {doc_id}',
            'hint': 'doc_id 는 /lookup 응답의 documents[].doc_id 값입니다.'})
    ref['ok'] = True
    return ref


# ---------------------------------------------------------------- 자연어 진입점

class AskRequest(BaseModel):
    """챗봇 통합용 단일 진입점 본문."""
    message: str = Field(..., description='사용자 문장. 예: "LS-6UTPD-3MG RoHS 인증서 주세요"')
    mode: Mode = Field(Mode.internal)
    override_model: Optional[str] = Field(None, description='모델 추출 결과를 덮어쓴다')
    override_doc_type: Optional[str] = Field(None, description='자료유형을 덮어쓴다')


@app.post('/ask', summary='자연어 한 줄 -> 라우팅')
def ask(req: AskRequest = Body(...)) -> Dict[str, Any]:
    parsed = cli.parse_message(req.message)
    model = req.override_model or parsed.model
    doc_type = req.override_doc_type or parsed.doc_type
    intent = parsed.intent
    if req.override_doc_type and intent == 'coverage':
        intent = 'files'
    if not model:
        return envelope(_need_model(req.message, req.mode.value, parsed.as_dict()),
                        parsed=parsed.as_dict())
    dt = _check_doc_type(doc_type) if doc_type else None
    if intent == 'coverage':
        dt, intent = None, 'files'
    out = _lookup_answer(model, dt, req.mode.value, intent, _today())
    extra = cli.unanswered_notice(parsed, model, dt)
    if extra:
        out['answer'] = f"{out['answer']}\n\n{extra}"
        out.setdefault('notices', []).append(extra)
    out['parsed'] = parsed.as_dict()
    out['query'] = req.message
    return out
