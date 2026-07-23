#!/usr/bin/env python3
"""터미널 조회 — API 없이 엔진을 그대로 두드린다.

    python3 -m cert_lookup.cli LS-6UTPD-3MG RoHS
    python3 -m cert_lookup.cli LS-OVERC --kc
    python3 -m cert_lookup.cli --ask "LS-HDAOC-30M RoHS 성적서 주세요"
    python3 -m cert_lookup.cli --selftest

표준 라이브러리만 쓴다(fastapi 가 없는 PC 에서도 검증할 수 있어야 하므로).
자연어 파서(parse_message)도 여기 둔다 — api.py 가 이 모듈에서 가져다 쓴다.
반대 방향(cli -> api)의 의존은 없다.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import loader, policy as policy_mod, render, resolver, search
from . import drive as drive_mod
from .drive import DriveResolver
from .policy import Policy

# ---------------------------------------------------------------- 자연어 파싱

#: 모델 코드 정규식은 resolver 가 이미 갖고 있다. 기존 챗봇의
#: r'(LS[P]?-[A-Za-z0-9\-\.]+)' 를 LSN- 접두와 비LS 모델(DP12MM-10M, OM4-*, USB2.0-*)까지
#: 넓힌 것으로, 제품군 키 427개와 KC 모델 118개를 전부 잡는 것을 확인했다.
MODEL_RX = resolver.MODEL_RX

#: 'KC 자료' 의도. 'KC' 는 모델 토큰을 지운 뒤 단어 경계로만 본다(LS-KC... 오탐 방지).
KC_RX = re.compile(r'(?<![A-Za-z0-9])KC(?![A-Za-z0-9])', re.IGNORECASE)
KC_WORDS = ('적합등록', '적합인증', '방송통신기자재', '전파인증', '필증')

#: '내용/수치를 알려달라' 는 의도. 파일 전달이 아니라 인용 가능 여부를 먼저 답해야 한다.
CONTENT_WORDS = ('수치', '함량', 'ppm', 'PPM', '얼마', '내용이', '내용을', '본문',
                 '뭐라고', '기재', '적혀', '적혀있', '인용', '몇 ', '몇%', '수치가')

#: 보유현황 요약 의도.
COVERAGE_WORDS = ('뭐뭐', '어떤 자료', '보유 현황', '보유현황', '있는 자료', '무슨 자료',
                  '자료 목록', '현황')

_ALIAS_ITEMS: List[Tuple[str, str]] = sorted(
    list(search.DOC_TYPE_ALIASES.items()) + [(t.upper(), t) for t in loader.KNOWN_DOC_TYPES],
    key=lambda kv: -len(kv[0]))
_ASCII_RX = {a: re.compile(r'(?<![A-Za-z0-9])' + re.escape(a) + r'(?![A-Za-z0-9])',
                           re.IGNORECASE)
             for a, _ in _ALIAS_ITEMS if a.isascii()}


def _strip_models(text: str) -> str:
    """자료유형을 찾기 전에 모델 토큰을 지운다(LS-6UTPD 의 'UTP', LS-UL... 오탐 방지)."""
    return MODEL_RX.sub(' ', text or '')


#: 'RoHS 성적서' 의 '성적서' 는 별개 자료유형이 아니라 앞 유형을 꾸미는 말이다.
#: 이 별칭으로만 잡힌 유형은 앞에 다른 유형이 있으면 버린다('시험성적서 주세요' 는 남는다).
QUALIFIER_ALIASES = frozenset({'성적서', '시험성적서', '인증', '인증서'})


def detect_doc_types(text: str) -> List[str]:
    """문장에 등장한 자료유형을 등장 순서대로. 없으면 빈 리스트."""
    cleaned = unicodedata.normalize('NFC', _strip_models(text or ''))
    best: Dict[str, Tuple[int, bool]] = {}      # doc_type -> (위치, 수식어로만 잡혔는가)
    for alias, dt in _ALIAS_ITEMS:
        rx = _ASCII_RX.get(alias)
        if rx is not None:
            m = rx.search(cleaned)
            pos = m.start() if m else -1
        else:
            pos = cleaned.find(alias)
        if pos < 0:
            continue
        qualifier = alias in QUALIFIER_ALIASES
        prev = best.get(dt)
        if prev is None or pos < prev[0]:
            best[dt] = (pos, qualifier)
        elif not qualifier:
            best[dt] = (prev[0], False)
    ordered = sorted(best.items(), key=lambda kv: kv[1][0])
    return [dt for i, (dt, (_pos, qual)) in enumerate(ordered) if not (qual and i > 0)]


@dataclass
class ParsedMessage:
    """자연어 한 줄에서 뽑아낸 라우팅 정보."""
    message: str
    models: List[str] = field(default_factory=list)
    doc_types: List[str] = field(default_factory=list)
    intent: str = 'files'                 # files | content | kc | coverage

    @property
    def model(self) -> Optional[str]:
        return self.models[0] if self.models else None

    @property
    def doc_type(self) -> Optional[str]:
        return self.doc_types[0] if self.doc_types else None

    def as_dict(self) -> Dict[str, Any]:
        return {'message': self.message, 'models': self.models, 'model': self.model,
                'doc_types': self.doc_types, 'doc_type': self.doc_type,
                'intent': self.intent}


def unanswered_notice(parsed: 'ParsedMessage', model: str,
                      doc_type: Optional[str]) -> Optional[str]:
    """한 번에 답한 것 외에 문의에 더 있었던 항목.

    '두 모델 다 주세요' 처럼 요청의 절반이 조용히 사라진 채 완결된 답변처럼 보이면 안 된다.
    """
    rest = [m for m in parsed.models if m != (model or '').upper()]
    rest += [d for d in parsed.doc_types if d != doc_type]
    if not rest:
        return None
    return f"※ 함께 문의하신 {', '.join(rest)} 는 별도 조회가 필요합니다."


def parse_message(message: str) -> ParsedMessage:
    """'LS-6UTPD-3MG RoHS 인증서 주세요' -> 모델·자료유형·의도."""
    text = message or ''
    out = ParsedMessage(message=text, models=resolver.extract_models(text),
                        doc_types=detect_doc_types(text))
    stripped = _strip_models(text)
    if KC_RX.search(stripped) or any(w in stripped for w in KC_WORDS):
        out.intent = 'kc'
    elif any(w in stripped for w in CONTENT_WORDS):
        out.intent = 'content'
    elif not out.doc_types and any(w in stripped for w in COVERAGE_WORDS):
        out.intent = 'coverage'
    return out


# ---------------------------------------------------------------- KC 포함 여부

#: 'contains?item=필증' 의 item 을 서류 종류로 접는다.
KIND_ALIASES: Dict[str, str] = {}
for _kind, _keys in search.KC_DOC_KINDS:
    KIND_ALIASES[_kind] = _kind
    for _k in _keys:
        KIND_ALIASES.setdefault(_k, _kind)


def kc_contains(kc: search.KCResult, item: str) -> Dict[str, Any]:
    """zip 을 열지 않고 내부 서류 포함 여부를 판정한다."""
    raw = unicodedata.normalize('NFC', (item or '').strip())
    flat = re.sub(r'\s+', '', raw)
    kind = KIND_ALIASES.get(flat) or KIND_ALIASES.get(raw)
    if not kind and len(flat) >= 2:
        # '서'·'증' 같은 한 글자는 어느 별칭에나 걸려 엉뚱한 서류를 '포함' 으로 답하게 된다.
        # 두 글자 이상만 부분일치를 허용하고, 가장 길게 겹치는 별칭을 고른다(순서 의존 제거).
        hits = [(len(a), k) for a, k in KIND_ALIASES.items()
                if a and (a in flat or flat in a)]
        if hits:
            kind = max(hits)[1]
    if kind:
        files = kc.kinds.get(kind) or []
        return {'item': item, 'kind': kind, 'contains': bool(files),
                'files': files, 'matched_by': 'kind'}
    # 종류로 접히지 않으면 파일명 부분일치로 답한다(추측하지 않고 있는 그대로).
    files = [search._kc_basename(c) for c in kc.contents
             if flat and flat in re.sub(r'\s+', '', search._kc_basename(c))]
    return {'item': item, 'kind': None, 'contains': bool(files),
            'files': files, 'matched_by': 'filename'}


# ---------------------------------------------------------------- 문서 직렬화

def doc_dict(d: search.DocView, pol: Policy) -> Dict[str, Any]:
    """문서 한 건의 기계용 표현. 파일 위치는 정책이 허용할 때만 넣는다."""
    out: Dict[str, Any] = {
        'doc_id': d.doc_id, 'doc_type': d.doc_type, 'all_types': d.all_types,
        'title_ko': d.title_ko, 'models': d.models,
        'scope': d.scope, 'scope_ko': d.scope_ko, 'scope_target': d.scope_target,
        'provision': d.provision, 'material_cats': d.material_cats,
        'issuer': d.issuer, 'doc_number': d.doc_number, 'issue_date': d.issue_date,
        'valid_until': d.valid_until, 'expired': d.expired, 'standards': d.standards,
        'caveat_ko': policy_mod.effective_caveat(d), 'caveat_from_default': d.caveat_missing,
        'deliverable': d.deliverable, 'text_extractable': d.text_extractable,
        'quotable': pol.may_quote(d), 'shareable': pol.may_share_file(d),
        'secondary_type': d.secondary_type, 'names_query': d.names_query,
        'source': d.source, 'pages': d.pages, 'size_bytes': d.size_bytes,
    }
    if pol.show_file_location:
        out['filename'] = d.filename
        out['path'] = d.path
    return out


def family_documents(cert: search.FamilyResult, pol: Policy,
                     kb: Optional[loader.KnowledgeBase] = None,
                     today: Optional[str] = None) -> List[Dict[str, Any]]:
    """제품군의 전체 문서 목록(정책 필터 적용).

    추론 매핑 여부는 문서가 속한 매트릭스 축의 마크로 판정한다 — customer 모드에서
    O* 축의 문서가 목록에 새어 나가면 안 된다.
    """
    kb = kb or loader.load_kb()
    inferred_types = {t for t, tr in cert.types.items() if tr.inferred}
    seen: Dict[str, Dict[str, Any]] = {}
    for fam in cert.families:
        for rec in kb.documents(fam):
            did = rec.get('doc_id')
            if not did or did in seen:
                continue
            view = search.make_doc_view(rec, [cert.resolution.normalized], today=today)
            types = set(view.all_types or [view.doc_type])
            inferred = bool(types & inferred_types)
            visible, _hidden = pol.filter_documents([view], inferred=inferred)
            if visible:
                seen[did] = doc_dict(view, pol)
    return sorted(seen.values(), key=lambda d: (d['doc_type'], d['title_ko']))


# ---------------------------------------------------------------- 조회 헬퍼

def drive_resolver(kb: Optional[loader.KnowledgeBase] = None) -> DriveResolver:
    """동기화 루트 탐색이 한 번만 일어나도록 프로세스당 하나만 만든다."""
    return drive_mod.get_resolver()


def file_ref(doc_id: str, mode: str = policy_mod.INTERNAL,
             kb: Optional[loader.KnowledgeBase] = None) -> Optional[Dict[str, Any]]:
    """doc_id 로 파일 위치를 해석한다. 문서가 없으면 None."""
    kb = kb or loader.load_kb()
    rec = kb.docs.get(doc_id)
    if rec is None:
        return None
    pol = policy_mod.get_policy(mode)
    view = search.make_doc_view(rec)
    out: Dict[str, Any] = {'doc_id': doc_id, 'mode': pol.mode,
                           'document': doc_dict(view, pol)}
    # 사내 모드에서 '단독 전달 불가' 는 고객에게 그냥 주지 말라는 뜻이지, 담당자가 열어
    # 보지도 말라는 뜻이 아니다. 그 판단을 하려면 열어 봐야 한다. 게다가 링크는 열리는데
    # 다운로드만 막으면 아무것도 못 막고 혼란만 준다. 경고는 카드에 이미 붙는다.
    internal_review = (pol.mode != policy_mod.CUSTOMER
                       and view.deliverable is False
                       and not view.attribution_rejected)
    if not pol.may_share_file(view) and not internal_review:
        out['allowed'] = False
        out['file'] = None
        out['reason'] = ('단독 전달 불가 자료' if not view.deliverable else
                         '출처 매핑 미확정 자료' if view.attribution_rejected else
                         '고객 모드에서는 완제품 단위 문서만 전달합니다')
        out['notices'] = [policy_mod.NOT_DELIVERABLE_NOTICE if not view.deliverable
                          else policy_mod.ATTRIBUTION_REJECTED_NOTICE]
        return out
    if not pol.show_file_location:
        out['allowed'] = True
        out['file'] = None
        out['reason'] = '고객 모드에서는 파일 경로를 직접 노출하지 않습니다'
        out['notices'] = [policy_mod.CUSTOMER_FILE_NOTICE]
        return out
    ref = drive_resolver(kb).resolve_document(rec)
    out['allowed'] = True
    # DriveRef 필드를 손으로 나열하면 새 필드가 조용히 빠진다 — file_id 가 빠져
    # 다운로드가 ID 없이 호출돼 전건 실패했다. dataclass 를 그대로 펼친다.
    out['file'] = {**asdict(ref), 'ok': ref.ok}
    out['notices'] = ([policy_mod.SCAN_NOTICE] if not view.text_extractable else [])
    return out


def answer_for(model: str, doc_type: Optional[str] = None,
               mode: str = policy_mod.INTERNAL, intent: str = 'files',
               kb: Optional[loader.KnowledgeBase] = None,
               today: Optional[str] = None) -> render.Answer:
    """respond() 얇은 래퍼 — coverage 의도는 자료유형을 무시한다."""
    if intent == 'coverage':
        doc_type, intent = None, 'files'
    return render.respond(model, doc_type, mode=mode, intent=intent, kb=kb, today=today)


# ---------------------------------------------------------------- CLI

REQUIRED_CASES: Sequence[Tuple[str, Optional[str], str]] = (
    ('LS-6UTPD-3MG', 'RoHS', 'files'),
    ('LS-RGB-15MM', None, 'files'),
    ('LS-LKPG', None, 'files'),
    ('LS-1600HQ', 'RoHS', 'files'),
    ('LS-HDAOC-30M', 'RoHS', 'files'),
    ('LS-OVERC', None, 'kc'),
    ('LS-HDMI-AD20-1M', 'RoHS', 'files'),
    #: 8번째 필수 케이스 — 스캔 문서 내용 질문. LS-6UTPD RoHS 4건 중 1건이
    #: text_extractable=false 라 '원문 인용 불가' 가 실제로 나오는 조합이다.
    ('LS-6UTPD-3MG', 'RoHS', 'content'),
)


def _selftest(mode: str, today: Optional[str]) -> int:
    kb = loader.load_kb()
    for i, (model, dt, intent) in enumerate(REQUIRED_CASES, 1):
        label = f'{model}' + (f' {dt}' if dt else '') + (f' [{intent}]' if intent != 'files' else '')
        print(f'\n{"=" * 78}\n[{i}/{len(REQUIRED_CASES)}] {label}\n{"=" * 78}')
        print(answer_for(model, dt, mode=mode, intent=intent, kb=kb, today=today).text)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='python3 -m cert_lookup.cli',
        description='랜스타 제품 인증·기술자료 조회 (사내용)',
        epilog='예) python3 -m cert_lookup.cli LS-6UTPD-3MG RoHS')
    p.add_argument('model', nargs='?', help='모델명 (예: LS-6UTPD-3MG)')
    p.add_argument('doc_type', nargs='?',
                   help='자료유형 (RoHS/CE/CoC/Data sheet/Fluke test/REACH/…)')
    p.add_argument('--mode', choices=[policy_mod.INTERNAL, policy_mod.CUSTOMER],
                   default=policy_mod.INTERNAL, help='기본 internal(사내 전용)')
    p.add_argument('--intent', choices=['files', 'content', 'kc', 'coverage'],
                   default=None, help='기본 files')
    p.add_argument('--kc', action='store_true', help='--intent kc 축약')
    p.add_argument('--coverage', action='store_true', help='보유현황 요약만')
    p.add_argument('--ask', metavar='문장', help='자연어 한 줄로 조회')
    p.add_argument('--contains', metavar='항목', help='KC zip 내부 서류 포함 여부')
    p.add_argument('--file', metavar='DOC_ID', help='문서 파일 위치 해석')
    p.add_argument('--today', metavar='YYYY-MM-DD', help='만료 판정 기준일(테스트용)')
    p.add_argument('--json', action='store_true', help='구조화 JSON 출력')
    p.add_argument('--selftest', action='store_true', help='필수 8개 케이스 실행')
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    kb = loader.load_kb()
    for prob in kb.problems:
        print(f'[경고] {prob}', file=sys.stderr)

    if args.selftest:
        return _selftest(args.mode, args.today)

    if args.file:
        ref = file_ref(args.file, args.mode, kb)
        if ref is None:
            print(f'doc_id 를 찾을 수 없습니다: {args.file}', file=sys.stderr)
            return 2
        print(json.dumps(ref, ensure_ascii=False, indent=2))
        return 0

    intent = args.intent or ('kc' if args.kc else 'coverage' if args.coverage else 'files')
    model, doc_type = args.model, args.doc_type
    parsed: Optional[ParsedMessage] = None

    if args.ask:
        parsed = parse_message(args.ask)
        model = model or parsed.model
        doc_type = doc_type or parsed.doc_type
        if args.intent is None and not args.kc and not args.coverage:
            intent = parsed.intent
        if not model:
            print('모델명을 찾지 못했습니다. LS- 로 시작하는 모델명을 포함해 주십시오.',
                  file=sys.stderr)
            return 2

    if not model:
        build_parser().print_help()
        return 2

    if args.contains:
        res = search.lookup(model, None, kb)
        if res.kc.resolution.ambiguous:
            ans = render.render_ambiguous(res)
            print(ans.text)
            return 0
        info = kc_contains(res.kc, args.contains)
        ans = render.render_kc_contains(res.kc, info, model, policy_mod.get_policy(args.mode))
        if args.json:
            print(json.dumps(ans.data, ensure_ascii=False, indent=2))
        else:
            print(ans.text)
        return 0

    if doc_type and search.normalize_doc_type(doc_type) is None:
        print(f'알 수 없는 자료유형: {doc_type}\n'
              f"사용 가능: {', '.join(loader.KNOWN_DOC_TYPES)}", file=sys.stderr)
        return 2

    ans = answer_for(model, doc_type, mode=args.mode, intent=intent, kb=kb, today=args.today)
    if parsed is not None:
        extra = unanswered_notice(parsed, model, search.normalize_doc_type(doc_type or ''))
        if extra:
            ans = render.Answer(f'{ans.text}\n\n{extra}', ans.data)
    if args.json:
        payload = {'answer': ans.text, 'data': ans.data}
        if parsed is not None:
            payload['parsed'] = parsed.as_dict()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(ans.text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
