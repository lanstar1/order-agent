#!/usr/bin/env python3
"""챗봇 본체 — 0단계 라우팅 결과에 따라 근거를 고르고 응답을 조립한다.

경로별 규칙(어기면 사고가 나는 것들)
  · cert  : cert_lookup 이 **유일한 근거**. 제품분석·상담이력을 섞지 않고, answer 문자열과
            notices[] 를 **그대로** 내보낸다. LLM 을 태우지 않는다.
            (`LS-UC314 RoHS 시험성적서 있나요` 의 상담이력 top-1 은 다른 모델의
             "시험 성적서를 가지고 있지 않습니다" 다. 점수 필터가 아니라 호출 차단으로 막는다.)
  · legacy: 상담이력(모델 스코프 필수) → 미스면 제품분석(화이트리스트) → 그래도 없으면 담당자 확인.
  · mixed : 두 경로를 각각 태워 **섹션으로 분리**한다. 근거를 한 문단에 섞지 않는다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import cert_lookup
from cert_lookup import cli as cert_cli
from cert_lookup import loader as cert_loader
from cert_lookup import policy as cert_policy
from cert_lookup import resolver as cert_resolver
from cert_lookup import search as cert_search

from . import config, history, llm, router, session, spec


@dataclass
class ChatReply:
    text: str
    route: str = 'legacy'
    sources: List[Dict[str, Any]] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)
    needs_clarification: bool = False
    candidates: List[str] = field(default_factory=list)
    notices: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {'text': self.text, 'route': self.route, 'sources': self.sources,
                'data': self.data, 'needs_clarification': self.needs_clarification,
                'candidates': self.candidates, 'notices': self.notices}

    def __str__(self) -> str:
        return self.text


# ---------------------------------------------------------------- 모델 해석

_GAZ: Optional[List[str]] = None


def _gazetteer() -> List[str]:
    """정규식이 0건일 때만 쓰는 KB 이름 사전(최장일치)."""
    global _GAZ
    if _GAZ is None:
        kb = cert_loader.load_kb()
        names = set(kb.all_model_names()) | set(kb.kc_models or [])
        names |= set(history.get_index().models)
        names = {n for n in names
                 if len(n) >= 5 and not cert_loader.WILDCARD_MEMBER.search(n)}
        _GAZ = sorted(names, key=len, reverse=True)
    return _GAZ


def reset_cache() -> None:
    global _GAZ
    _GAZ = None
    history.reset_cache()
    spec.reset_cache()


def extract_models(text: str) -> List[str]:
    """resolver 정규식 → 사전 폴백. 정규식은 LSN-/DP../OM./USB.- 까지 이미 커버한다."""
    hits = cert_resolver.extract_models(text)
    if hits:
        return hits
    up = re.sub(r'[_\s]+', '-', (text or '').upper())
    out: List[str] = []
    for name in _gazetteer():
        i = up.find(name)
        if i < 0:
            continue
        before = up[i - 1] if i else ' '
        after = up[i + len(name)] if i + len(name) < len(up) else ' '
        if before.isalnum() or after.isalnum():
            continue
        out.append(name)
        if len(out) >= 3:
            break
    if out:
        return out
    return _bare_code_models(text)


#: 접두를 뗀 모델 코드. 담당자는 'LS-' 를 빼고 '6utpd-3mg' 처럼 치는 일이 잦다.
#: 숫자/영문으로 시작하고 하이픈이 든 토큰만 후보로 본다(일반 단어를 모델로 오인하지 않게).
_BARE_CODE_RX = re.compile(r'\b([0-9A-Z]{1,6}(?:[.\-][0-9A-Z.]{1,10}){1,4})\b')
#: 하이픈 없는 코드('300HO'). 숫자와 영문이 섞인 4~12자만 본다.
_BARE_SOLID_RX = re.compile(r'\b(?=[0-9A-Z]{4,12}\b)(?=[^\s]*\d)(?=[^\s]*[A-Z])([0-9A-Z]{4,12})\b')
_BARE_PREFIXES = ('LS-', 'LSP-', 'LSN-')


def _bare_code_models(text: str) -> List[str]:
    """'6utpd-3mg' 처럼 접두가 빠진 입력을 KB 에 실재하는 모델로 복원한다.

    복원은 **KB 사전에 그 이름이 실제로 있을 때만** 인정한다. 접두를 붙여 만든 이름이
    사전에 없으면 버린다 — 없는 모델을 지어내면 엉뚱한 제품군 자료가 붙는다.
    """
    up = cert_resolver.normalize_model(text)
    known = set(_gazetteer())
    out: List[str] = []
    for tok in _BARE_CODE_RX.findall(up):
        if any(tok.startswith(p) for p in _BARE_PREFIXES):
            continue
        for pre in _BARE_PREFIXES:
            cand = pre + tok
            if cand in known and cand not in out:
                out.append(cand)
                break
        else:
            # 사전에 없으면 제품군 해석까지 시도한다(길이 변형은 사전에 없을 수 있다)
            for pre in _BARE_PREFIXES:
                cand = pre + tok
                if cert_resolver.resolve_cert(cand).found and cand not in out:
                    out.append(cand)
                    break
        if len(out) >= 3:
            break
    if out:
        return out

    # 하이픈 없는 코드('300ho'). 접두를 붙인 이름의 **골격**(숫자를 지운 형태)이 KB 에
    # 실재할 때만 인정한다 — 'LS-#HO' 는 LS-1600HO 로 실재하므로 LS-300HO 를 모델로 읽고
    # "자료 없음 + 같은 라인 제안"으로 답할 수 있다. 반면 '120hz'→'LS-#HZ' 는 실재하지
    # 않으니 버린다. 이 게이트가 없으면 '4k'·'2024' 가 전부 모델명이 된다.
    skeletons = {cert_resolver._skeleton(f) for f in cert_loader.load_kb().family_keys}
    for tok in _BARE_SOLID_RX.findall(up):
        for pre in _BARE_PREFIXES:
            cand = pre + tok
            if cand in known:
                if cand not in out:
                    out.append(cand)
                break
            if cert_resolver._skeleton(cand) not in skeletons:
                continue
            # 골격이 맞아도 여러 제품군의 앞부분일 뿐이면 모델명이 아니다.
            # 'usb3.0 허브' 의 'USB3' 이 그런 경우로, LS-USB3.0-AMAF/AMAM/… 의 어간이다.
            if cert_resolver.resolve_cert(cand).reason == 'stem_ambiguous':
                continue
            if cand not in out:
                out.append(cand)
            break
        if len(out) >= 2:
            break
    return out


def _resolve_models(message: str, parsed: Dict[str, Any],
                    sess: session.Session) -> Tuple[List[str], str]:
    """(모델 목록, 출처). 지시어는 직전 모델로 해석한다."""
    models = list(parsed.get('models') or [])
    if models:
        return models, 'message'
    models = extract_models(message)
    if models:
        return models, 'gazetteer'
    # 여기 도달하면 models 는 항상 비어 있다. `or not models` 를 두면 지시어 게이트가
    # 죽은 코드가 되고, 모델명 없는 모든 발화가 직전 모델을 승계한다 — 무상태로는
    # 되묻는 문장("4K 120hz 선택기 인증서 주세요")이 앞 대화 때문에 엉뚱한 모델의
    # 보유현황으로 답해진다. 지시어나 후속 표지가 있을 때만 승계한다.
    if sess.last_models and (session.has_deictic(message) or _is_followup(message)):
        return list(sess.last_models), 'session'
    return [], 'none'


#: 새 모델을 말하지 않고 앞 문맥을 이어가는 표지. 이 표현이 있을 때만 직전 모델을 잇는다.
_FOLLOWUP_RX = re.compile(
    r'^\s*(그럼|그러면|그리고|또|추가로|참고로|네|넵|예)\b|'
    r'(도\s*(있|주|되|가능|보내)|도요|은요|는요|만요|말고|대신)|'
    r'^\s*\S{0,12}(자료|서류|성적서|인증서|도면|사양서|데이터시트|매뉴얼)\s*(도|는|은|요|만)')


#: 짧은 문장에 문서명사만 있으면 앞 문맥을 잇는 발화로 본다('자료 있나요?').
#: 길이 제한이 핵심이다 — '4K 120hz hdmi 선택기 3:1 인증서 받아볼 수 있을까요?' 처럼
#: 모델명 대신 제품을 서술한 새 문의는 승계하면 안 된다(엉뚱한 모델의 자료를 답하게 된다).
_DOC_NOUN_RX = re.compile(
    r'자료|서류|성적서|인증서|증명서|도면|사양서|데이터시트|매뉴얼|스펙|'
    r'RoHS|REACH|MSDS|CE\b|UL\b|KC\b', re.IGNORECASE)
_FOLLOWUP_MAX_LEN = 20


def _is_followup(message: str) -> bool:
    """모델명 없이 앞 문맥을 잇는 발화인가('RoHS도 있나요?', '도면도요', '자료 있나요?')."""
    msg = (message or '').strip()
    if _FOLLOWUP_RX.search(msg):
        return True
    return len(msg) <= _FOLLOWUP_MAX_LEN and bool(_DOC_NOUN_RX.search(msg))


# ---------------------------------------------------------------- 근거 정리

def _usable_answer(hit: history.Hit, query: str) -> Tuple[str, List[str]]:
    """상담이력 답변을 인용 가능한 형태로 정리한다.

    · talk_* 는 병합 대화 로그라 줄 단위 발췌
    · URL 은 유효성 미검증이라 제거
    · 인증 문구는 문장 단위로 삭제 — 인증은 cert_lookup 만이 근거다
    """
    notes: List[str] = []
    text = history.clean_answer(hit.row.answer)
    if hit.row.talk:
        text = history.talk_excerpt(text, query)
        notes.append(config.HISTORY_TALK_NOTICE)
    text, dropped = config.strip_cert_claims(text)
    if dropped:
        notes.append('※ 과거 답변의 인증 관련 문구는 현재 KB 와 충돌할 수 있어 제외했습니다. '
                     '인증·시험성적서는 별도 조회가 필요합니다.')
    if hit.row.volatile:
        notes.append(config.HISTORY_VOLATILE_NOTICE)
    return text.strip(), notes


def _render_history(hits: List[history.Hit], query: str,
                    header: str) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    lines: List[str] = []
    notices: List[str] = []
    srcs: List[Dict[str, Any]] = []
    shown = 0
    for h in hits:
        text, notes = _usable_answer(h, query)
        if not text:
            continue                                  # 인증 문구만 있던 답변 등
        shown += 1
        tag = '인용 가능' if h.quotable else '참고'
        if h.row.volatile:
            tag = '참고(시점 의존)'
        extra = ' · 형제 모델 이력' if h.sibling else ''
        score = f' · 유사도 {h.score:.2f}' if h.score else ''
        lines.append(f'[{shown}] {tag}{extra}{score} — {h.row.model} '
                     f'({"카카오톡 상담" if h.row.talk else "쇼핑몰 문의"})')
        lines.append(f'  Q. {h.row.question.strip()}')
        lines.append(f'  A. {text}')
        for n in notes:
            if n not in notices:
                notices.append(n)
        srcs.append({'kind': config.SOURCE_HISTORY, 'model': h.row.model,
                     'score': round(h.score, 3), 'level': h.level,
                     'source': h.row.source, 'quotable': h.quotable})
    if not lines:
        return '', notices, srcs
    if config.HISTORY_NOTICE not in notices:
        notices.append(config.HISTORY_NOTICE)
    return header + '\n' + '\n'.join(lines), notices, srcs


def _render_spec(ctx: Dict[str, Any]) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    body = spec.render_spec(ctx)
    if not body:
        return '', list(ctx.get('notices') or []), []
    head = '제품 참고 정보(제품 이미지 기반 AI 분석)'
    if ctx['image'].get('url'):
        head += f"\n제품 이미지: {ctx['image']['url']}"
    src = [{'kind': config.SOURCE_SPEC, 'model': ctx['model'],
            'verification': ctx['verification'],
            'fields': sorted(ctx['fields'].keys())}]
    return head + '\n' + body, list(ctx.get('notices') or []), src


# ---------------------------------------------------------------- cert 경로

def _need_model_reply(route_name: str, message: str,
                      reason: str) -> ChatReply:
    cands = history.guess_models_by_name(message)
    lines = [f'{config.GREETING} {config.NEED_MODEL_TEXT}']
    names = [m for m, _s, _n in cands]
    if cands:
        lines.append('')
        lines.append('혹시 다음 모델인가요?')
        for m, s, name in cands:
            lines.append(f'· {m} — {name} (유사도 {s:.2f})')
    return ChatReply(text='\n'.join(lines), route=route_name,
                     sources=[{'kind': config.SOURCE_RULE, 'detail': '모델 미확정 되묻기'}],
                     data={'kind': 'need_model', 'reason': reason,
                           'name_candidates': [{'model': m, 'score': s, 'name': n}
                                               for m, s, n in cands]},
                     needs_clarification=True, candidates=names)


def _cert_section(message: str, parsed: Dict[str, Any], models: List[str],
                  flags: Dict[str, Any], mode: str,
                  model_override: Optional[str] = None) -> Dict[str, Any]:
    """cert_lookup 원문을 그대로 담아 돌려준다. 텍스트를 재작성하지 않는다."""
    model = model_override or (models[0] if models else '')
    if not model:
        return {'text': '', 'need_model': True, 'notices': [], 'data': {},
                'candidates': [], 'needs_clarification': True}
    doc_type = parsed.get('doc_type')
    intent = parsed.get('cert_intent') or 'files'
    ans = cert_cli.answer_for(model, doc_type, mode=mode, intent=intent)
    text = ans.text
    asked = list(parsed.get('models') or [])
    if model_override:
        # 되묻기로 고른 뒤에는 원래의 모호한 줄기(LS-HD2)를 '못 답한 항목'으로 남기지 않는다
        asked = [m for m in asked if not model_override.upper().startswith(m.upper())]
    pm = cert_cli.ParsedMessage(message=message, models=asked,
                                doc_types=list(parsed.get('doc_types') or []),
                                intent=intent)
    extra = cert_cli.unanswered_notice(pm, model,
                                       cert_search.normalize_doc_type(doc_type or ''))
    if extra:
        text = f'{text}\n\n{extra}'               # 요청 절반이 조용히 사라지지 않도록
    if flags.get('unsupported_docs'):
        text = f'{text}\n\n' + config.UNSUPPORTED_DOC_TEXT.format(
            items=', '.join(flags['unsupported_docs']))
    kind = (ans.data or {}).get('kind', '')
    return {'text': text, 'need_model': False,
            'notices': list((ans.data or {}).get('notices') or []),
            'data': dict(ans.data or {}, model_used=model, intent=intent),
            'candidates': list((ans.data or {}).get('candidates') or []),
            'needs_clarification': kind in ('ambiguous', 'need_model'),
            'kind': kind}


# ---------------------------------------------------------------- legacy 경로

def _legacy_section(message: str, models: List[str], flags: Dict[str, Any],
                    use_llm: bool = True,
                    mode: str = cert_policy.INTERNAL) -> Dict[str, Any]:
    """1·2단계 응답 조각.

    mode='customer' 는 고객이 직접 읽는 경로다. 상담이력 원문은 **다른 고객의 대화**라
    통째로 노출하지 않고(주소·주문번호가 섞여 있다), 사내 전용 문구도 쓰지 않는다.
    """
    customer = (mode == cert_policy.CUSTOMER)
    notices: List[str] = []
    srcs: List[Dict[str, Any]] = []
    data: Dict[str, Any] = {}

    if flags.get('driver'):
        return {'text': config.DRIVER_TEXT + '\n' + config.ESCALATION,
                'notices': [], 'sources': [{'kind': config.SOURCE_RULE,
                                            'detail': '드라이버/설치파일 문의'}],
                'data': {'kind': 'driver'}, 'need_model': False}
    if flags.get('unsupported_docs'):
        notices.append(config.UNSUPPORTED_DOC_TEXT.format(
            items=', '.join(flags['unsupported_docs'])))

    if not models:
        return {'text': '', 'notices': notices, 'sources': [], 'data': {},
                'need_model': True}

    model = models[0]
    # ---- 1단계: 상담이력(모델 스코프 필수) --------------------------------
    # 모델 코드는 스코프로 이미 썼다. 질의 문자열에 남겨두면 n-gram 이 희석돼 점수가 낮아진다
    # (임계값 캘리브레이션도 '모델코드를 지운 질문' 기준으로 측정된 값이다).
    query = router.mask_models(message).strip()
    hits = history.search_qa(query, model)
    pool = history.model_row_count(model)
    data['history_pool'] = pool
    data['history_query'] = query
    data['history_hits'] = [h.as_dict() for h in hits]
    body, hnotices, hsrcs = _render_history(hits, query, f'{model} 과거 상담 이력')
    if not body and 0 < pool <= config.SMALL_POOL and not customer:
        # 상담건수 중앙값이 2건이라 대부분의 모델은 top-k 랭킹이 무의미하다 → 전량 노출
        allrows = history.history_for_model(model, include_siblings=False)
        body, hnotices, hsrcs = _render_history(
            allrows, query, f'{model} 상담 이력 전체({pool}건) — 담당자 확인용')
        data['history_full_dump'] = True
    notices.extend(n for n in hnotices if n not in notices)
    srcs.extend(hsrcs)

    # ---- 2단계: 제품분석(화이트리스트) ------------------------------------
    ctx = spec.spec_context(model)
    data['spec'] = {k: v for k, v in ctx.items() if k != 'notices'}
    sbody, snotices, ssrcs = _render_spec(ctx)
    has_cite = any(h.quotable for h in hits)
    if sbody and not has_cite:
        body = (body + '\n\n' + sbody) if body else sbody
        notices.extend(n for n in snotices if n not in notices)
        srcs.extend(ssrcs)
    elif not sbody and not body:
        notices.extend(n for n in snotices if n not in notices)

    if not body:
        text = (f'{model} 에 대해 확인 가능한 상담 이력과 참고 정보가 없습니다.\n'
                f'{config.ESCALATION}')
        srcs.append({'kind': config.SOURCE_RULE, 'detail': '근거 없음 — 에스컬레이션'})
        return {'text': text, 'notices': notices, 'sources': srcs, 'data': data,
                'need_model': False}

    # ---- 선택: LLM 문장 다듬기(없으면 그대로) ------------------------------
    # 인사말은 호출부가 붙이므로 LLM 이 붙인 것은 떼고, 근거 원문은 항상 함께 남긴다
    # (담당자가 무엇을 근거로 만든 문장인지 확인할 수 있어야 한다).
    text = body
    if use_llm and llm.available():
        polished = llm.polish(message, body,
                              extra_rules='근거에 있는 모델명 외 다른 모델을 언급하지 마십시오.')
        if polished:
            head = polished.strip()
            if head.startswith(config.GREETING):
                head = head[len(config.GREETING):].strip()
            srcs.append({'kind': config.SOURCE_LLM, 'model': config.LLM_MODEL})
            data['llm_used'] = True
            text = head + '\n\n— 근거 원문 —\n' + body
    if not any(s['kind'] == config.SOURCE_LLM for s in srcs):
        srcs.append({'kind': config.SOURCE_RULE, 'detail': 'LLM 미사용(규칙 기반 요약)'})
    if not any(h.quotable for h in hits):
        text = text + '\n' + config.ESCALATION
    return {'text': text, 'notices': notices, 'sources': srcs, 'data': data,
            'need_model': False}


# ---------------------------------------------------------------- 진입점

def chat(message: str, session_id: str = '', mode: str = 'internal') -> ChatReply:
    """사내 영업·CS 상담 챗봇 한 턴.

    mode 는 cert_lookup.policy 가 검증한다('internal'|'customer', 오타는 ValueError).
    """
    cert_policy.get_policy(mode)                   # 오타 모드를 조용히 통과시키지 않는다
    msg = (message or '').strip()
    sess = session.get(session_id)

    # -- 되묻기 대기 상태 처리 -------------------------------------------
    pending = sess.pending_clarification
    if pending and msg:
        chosen = session.resolve_choice(msg, pending.get('candidates') or [])
        if chosen:
            sess.pending_clarification = None
            sess.remember([chosen], pending.get('doc_type'))
            return _dispatch(pending.get('message') or msg, sess, mode,
                             model_override=chosen, resumed=True)

    if not msg:
        return ChatReply(text=f'{config.GREETING} 문의 내용을 입력해 주십시오.',
                         sources=[{'kind': config.SOURCE_RULE, 'detail': '빈 입력'}])
    return _dispatch(msg, sess, mode)


def _dispatch(message: str, sess: session.Session, mode: str,
              model_override: Optional[str] = None,
              resumed: bool = False) -> ChatReply:
    # 라우터는 cert_cli 정규식(LS- 접두 필수)으로만 모델을 본다. 접두 없이 친 '300ho' 나
    # 세션으로 확정된 후속턴은 그 정규식에 안 걸려, 문장 안에 모델명이 없다는 이유로
    # legacy 로 샌다. 여기서 먼저 해석해 라우터에 알려준다.
    recovered = extract_models(message)
    known = bool(model_override) or bool(recovered) or bool(
        sess.last_models and (session.has_deictic(message) or _is_followup(message)))
    r = router.route(message, known_model=known)
    parsed = r['parsed']
    flags = r['flags']
    route_name = r['route']
    models, model_src = _resolve_models(message, parsed, sess)
    if model_override:
        models, model_src = [model_override], 'clarification'

    base_data: Dict[str, Any] = {'route_reason': r['reason'], 'parsed': parsed,
                                 'flags': flags, 'model_source': model_src,
                                 'models': models, 'resumed': resumed,
                                 'mode': mode}

    if route_name == 'cert':
        reply = _cert_only(message, parsed, models, flags, mode, model_override)
    elif route_name == 'mixed':
        reply = _mixed(message, parsed, models, flags, mode, model_override)
    else:
        reply = _legacy_only(message, models, flags, mode)

    reply.route = route_name
    reply.data = dict(base_data, **reply.data)
    if models:
        sess.remember(models, parsed.get('doc_type'))
    if reply.needs_clarification and reply.candidates:
        sess.pending_clarification = {'message': message, 'candidates': reply.candidates,
                                      'doc_type': parsed.get('doc_type')}
    elif not reply.needs_clarification:
        sess.pending_clarification = None
    sess.add_turn(message, reply.text, route_name)
    return reply


def _cert_only(message: str, parsed: Dict[str, Any], models: List[str],
               flags: Dict[str, Any], mode: str,
               model_override: Optional[str]) -> ChatReply:
    if not models:
        return _need_model_reply('cert', message, '인증·자료 문의이나 모델 미확정')
    sec = _cert_section(message, parsed, models, flags, mode, model_override)
    return ChatReply(text=sec['text'],
                     sources=[{'kind': config.SOURCE_CERT,
                               'model': sec['data'].get('model_used'),
                               'doc_type': parsed.get('doc_type'),
                               'kind_detail': sec.get('kind')}],
                     data={'cert': sec['data']},
                     needs_clarification=sec['needs_clarification'],
                     candidates=sec['candidates'],
                     notices=sec['notices'])


def _legacy_only(message: str, models: List[str], flags: Dict[str, Any],
                 mode: str = cert_policy.INTERNAL) -> ChatReply:
    sec = _legacy_section(message, models, flags, mode=mode)
    if sec.get('need_model'):
        rep = _need_model_reply('legacy', message, '일반 문의이나 모델 미확정 — '
                                                   '모델 없이 상담이력을 검색하면 '
                                                   'top-1 의 78.5%가 다른 모델입니다')
        rep.notices = sec['notices']
        return rep
    text = f'{config.GREETING} {sec["text"]}'
    if sec['notices']:
        text = text + '\n\n' + '\n'.join(sec['notices'])
    return ChatReply(text=text, sources=sec['sources'], data=sec['data'],
                     notices=sec['notices'])


def _mixed(message: str, parsed: Dict[str, Any], models: List[str],
           flags: Dict[str, Any], mode: str,
           model_override: Optional[str]) -> ChatReply:
    """근거를 한 문단에 섞지 않는다 — 섹션을 나눈다."""
    if not models:
        return _need_model_reply('mixed', message, '혼합 문의이나 모델 미확정')
    cert_sec = _cert_section(message, parsed, models, flags, mode, model_override)
    # 일반 문의 섹션에는 인증 근거를 넣지 않는다(반대 방향도 마찬가지)
    legacy_flags = dict(flags, unsupported_docs=[])
    legacy_sec = _legacy_section(message, models, legacy_flags, mode=mode)

    parts = [f'{config.GREETING} 문의하신 내용을 두 갈래로 나눠 답변드립니다.', '',
             '━━ [1] 인증·기술자료 ━━', cert_sec['text']]
    if legacy_sec.get('need_model') or not legacy_sec['text']:
        parts += ['', '━━ [2] 일반 문의 ━━',
                  f'해당 항목은 근거가 부족합니다. {config.ESCALATION}']
    else:
        parts += ['', '━━ [2] 일반 문의 ━━', legacy_sec['text']]
        if legacy_sec['notices']:
            parts += ['', *legacy_sec['notices']]
    sources = [{'kind': config.SOURCE_CERT, 'section': 1,
                'model': cert_sec['data'].get('model_used')}] + \
              [dict(s, section=2) for s in legacy_sec.get('sources') or []]
    return ChatReply(text='\n'.join(parts), sources=sources,
                     data={'cert': cert_sec['data'], 'legacy': legacy_sec.get('data') or {}},
                     needs_clarification=cert_sec['needs_clarification'],
                     candidates=cert_sec['candidates'],
                     notices=cert_sec['notices'] + [n for n in legacy_sec['notices']
                                                    if n not in cert_sec['notices']])
