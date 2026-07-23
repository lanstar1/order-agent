#!/usr/bin/env python3
"""조회 엔진 — 제품군/자료유형/KC/보유현황.

여기서 나오는 값은 전부 "데이터가 말하는 사실"이고, 무엇을 보여줄지(추론 매핑 노출, 파일
링크 허용)는 policy 가, 문장으로 만드는 것은 render 가 맡는다.

지켜야 할 사실 몇 가지:
  * coverage 마크는 precomputed 다. 재계산하지 않고 문자열 그대로 읽는다('*' 로 끝나면 추론).
  * coverage 는 5개 축밖에 없다. REACH·UL·Drawing 같은 유형을 X 라고 답하면 오답이므로
    documents 를 all_types 로 직접 훑는다.
  * '자재' 인데 material_coverage 키가 없는 조합이 28건 있다 — missing 을 지어내면 안 된다.
  * family.documents[] 에는 deliverable/caveat_ko/text_extractable 가 없다. loader 가 붙여둔
    manifest 레코드를 쓴다.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from . import loader, resolver
from .loader import BUNDLE_TYPES, MATRIX_COLS, KnowledgeBase

#: 사용자가 쓰는 표현 -> manifest doc_type.
DOC_TYPE_ALIASES = {
    'ROHS': 'RoHS', '로하스': 'RoHS', 'RHOS': 'RoHS', '유해물질': 'RoHS',
    'CE': 'CE', 'CE인증': 'CE', 'CE인증서': 'CE', 'CE마크': 'CE',
    'COC': 'CoC', '원산지': 'CoC', '원산지증명': 'CoC', '원산지증명서': 'CoC',
    'DATASHEET': 'Data sheet', 'DATA SHEET': 'Data sheet', '데이터시트': 'Data sheet',
    '사양서': 'Data sheet', '스펙': 'Data sheet', '규격서': 'Data sheet', '제품사양': 'Data sheet',
    'FLUKE': 'Fluke test', 'FLUKE TEST': 'Fluke test', '플루크': 'Fluke test',
    '채널테스트': 'Fluke test', '전송성능': 'Fluke test',
    'REACH': 'REACH', '리치': 'REACH',
    'MSDS': 'MSDS/SDS', 'SDS': 'MSDS/SDS', 'MSDS/SDS': 'MSDS/SDS', '물질안전보건자료': 'MSDS/SDS',
    'PFAS': 'PFAS', 'UL': 'UL', 'UL인증': 'UL',
    'TEST REPORT': 'Test report', 'TESTREPORT': 'Test report', '시험성적서': 'Test report',
    '성적서': 'Test report',
    'DRAWING': 'Drawing', '도면': 'Drawing', '캐드': 'Drawing', 'CAD': 'Drawing',
    'MANUAL': 'Manual', '매뉴얼': 'Manual', '설명서': 'Manual',
    'OTHER-CERT': 'Other-cert', 'HDMI-DP-CERT': 'HDMI-DP-cert',
}

#: 같은 유형 문서가 여러 건일 때 '인증서'를 먼저 집기 위한 힌트.
_CERT_HINT = re.compile(r'cert|证书|認證|인증', re.IGNORECASE)
#: 마크 우열(여러 제품군이 매칭됐을 때 대표 마크 선정용).
_MARK_RANK = {'O': 5, '자재': 4, 'O*': 3, '자재*': 2, 'X': 0, '': 0}

_SOURCE_CONF = {'high': 3, 'medium': 2, 'low': 1, 'unresolved': 0}
#: 'revise' 는 사람이 보고 고치라고 표시한 것이라 미검수('')보다 뒤에 둔다.
_SOURCE_VERIFIED = {'confirm': 3, '': 2, 'revise': 1, 'reject': -1}

# KC zip 내부 파일명 -> 서류 종류. 파일 하나가 여러 종류일 수 있어 break 하지 않는다.
KC_DOC_KINDS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ('필증', ('적합등록필증', '적합등록증', '필증', '인증서')),
    ('외관도', ('외관도',)),
    ('부품배치도', ('부품배치도', '부품배치', '장착사진', '내부사진')),
    ('시험성적서', ('진본성적서', '표준성적서', '시험성적서', '성적서', 'EMC_')),
    ('위임서', ('위임서', '위임장', '대리인지정서', '대리인지정(위임)서', '대리인')),
    ('확인서', ('부합증명확인서', '부합확인서', '부합증명서', '확인서')),
    ('변경신고서', ('변경신고서',)),
    ('면허세', ('면허세',)),
    ('보완통보서', ('부적합통보서', '수정사항통보서', '보완사항통보서', '통보서', '공문')),
    ('제품사진', ('제품사진',)),
)
_KC_REPORT_RX = re.compile(r'^\d{2}KC\d{3}_')

# 자료 미보유 사유 코드
X_NO_REQUEST = 'no_request_history'          # (미사용) 아래 주석 참조
X_REQUESTED_NOT_PROVIDED = 'requested_not_provided'   # 요청했으나 공급사 미제공
#: '요청한 적이 없다'는 단정은 더 이상 하지 않는다.
#: 발송메일 전수 판독(자료요청이력.csv) 결과 기존 RoHS요청이력.csv 는 실제 요청의 37.6%
#: 밖에 담고 있지 않았고, '미요청 건'이라고 답하던 13건 중 12건에 요청 메일이 실재했다.
#: 대장에 없다고 요청이 없었다는 뜻은 아니므로(판독 대상도 전체의 일부다) 사유를 비워 둔다.
X_NO_EVIDENCE = 'no_evidence'
KC_FULL, KC_PARTIAL, KC_NONE, KC_NOT_INDEXED = 'FULL', 'CERT_PDF_ONLY', 'NONE', 'NOT_INDEXED'


def normalize_doc_type(text: str) -> Optional[str]:
    """'로하스', 'rohs', 'RoHS' 를 manifest 의 doc_type 문자열로 맞춘다."""
    if not text:
        return None
    raw = text.strip()
    for t in loader.KNOWN_DOC_TYPES:
        if raw.lower() == t.lower():
            return t
    return DOC_TYPE_ALIASES.get(re.sub(r'\s+', ' ', raw).upper())


# ---------------------------------------------------------------- 문서 뷰

@dataclass
class DocView:
    """응답에 필요한 문서 정보 한 벌(게이트 플래그 포함)."""
    doc_id: str
    doc_type: str
    all_types: List[str]
    title_ko: str
    scope: str
    scope_ko: str
    scope_target: str
    provision: str
    material_cats: List[str]
    deliverable: bool                 # False = 단독 전달 불가(링크 금지)
    text_extractable: bool            # False = 스캔, 원문 인용 금지
    caveat_ko: str
    caveat_missing: bool
    issuer: str
    doc_number: str
    issue_date: str
    valid_until: str
    expired: bool
    standards: List[str]
    lang: str
    pages: int
    size_bytes: int
    path: str
    filename: str
    folder_id: str
    source: Dict[str, str]            # 대표 출처 1건
    source_count: int
    attribution_rejected: bool        # 출처 매핑이 명시적으로 반박된 문서
    inferred_source: bool             # sweep=retry 기반 추론 매핑 근거 문서
    secondary_type: bool              # doc_type 이 아니라 all_types 로 매칭됨
    names_query: bool                 # 질의 모델이 문서에 직접 표기돼 있는가
    models: List[str] = field(default_factory=list)

    @property
    def quotable(self) -> bool:
        """원문 내용을 인용해도 되는가(스캔 문서는 불가)."""
        return self.text_extractable


#: 응답에 내보내는 출처 필드. evidence·sweep 은 KB 구축 당시의 영문 감사 메모라
#: 담당자에게 가치가 없고, 빌드 머신의 절대경로가 박힌 건이 14건 있다 → 아예 내보내지 않는다.
_SOURCE_PUBLIC = ('mail_key', 'sender', 'subject', 'date', 'confidence', 'verified')


def _clean_source(src: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in src.items() if k in _SOURCE_PUBLIC}


def _pick_source(sources: Sequence[Dict[str, str]]) -> Dict[str, str]:
    """대표 출처. 사람이 검수한 결론(verified)이 자동 산출 confidence 보다 앞선다."""
    if not sources:
        return {}
    best = sorted(sources, key=lambda s: (_SOURCE_VERIFIED.get(s.get('verified', ''), 0),
                                          _SOURCE_CONF.get(s.get('confidence', ''), 0),
                                          s.get('date', '')), reverse=True)[0]
    return _clean_source(best)


def _is_expired(valid_until: str, today: Optional[str] = None) -> bool:
    # valid_until 은 890건 중 5건만 채워져 있다. 빈 값은 '만료'가 아니라 '표기 없음'이다.
    v = (valid_until or '').strip()
    if len(v) != 10:
        return False
    return v < (today or date.today().isoformat())


def make_doc_view(rec: Dict[str, Any], queried: Sequence[str] = (),
                  secondary_type: bool = False, today: Optional[str] = None) -> DocView:
    """manifest 레코드를 응답용 뷰로 변환한다."""
    sources = rec.get('sources') or []
    drive = rec.get('drive') or {}
    models = [m.upper() for m in (rec.get('models') or [])]
    return DocView(
        doc_id=rec.get('doc_id', ''),
        doc_type=rec.get('doc_type', ''),
        all_types=list(rec.get('all_types') or []),
        title_ko=rec.get('title_ko', ''),
        scope=rec.get('scope', ''),
        scope_ko=rec.get('scope_ko', ''),
        scope_target=rec.get('scope_target') or rec.get('product_in_doc') or rec.get('title_ko', ''),
        provision=rec.get('provision', ''),
        material_cats=list(rec.get('material_cats') or []),
        deliverable=bool(rec.get('deliverable')),
        text_extractable=bool(rec.get('text_extractable')),
        caveat_ko=(rec.get('caveat_ko') or '').strip(),
        caveat_missing=not (rec.get('caveat_ko') or '').strip(),
        issuer=rec.get('issuer', ''),
        doc_number=rec.get('doc_number', ''),
        issue_date=rec.get('issue_date', ''),
        valid_until=rec.get('valid_until', ''),
        expired=_is_expired(rec.get('valid_until', ''), today),
        standards=list(rec.get('standards') or []),
        lang=rec.get('lang', ''),
        pages=int(rec.get('pages') or 0),
        size_bytes=int(rec.get('size_bytes') or 0),
        path=rec.get('path', ''),
        filename=drive.get('filename') or '',
        folder_id=drive.get('folder_id') or '',
        source=_pick_source(sources),
        source_count=len(sources),
        attribution_rejected=bool(sources) and all(s.get('verified') == 'reject' for s in sources),
        inferred_source=any(s.get('sweep') == 'retry' and
                            s.get('confidence') in ('medium', 'low') for s in sources),
        secondary_type=secondary_type,
        names_query=any(q in models for q in queried),
        models=models,
    )


def _sort_docs(views: List[DocView]) -> List[DocView]:
    """'CE 인증서 주세요'에 시험성적서 본문을 먼저 집지 않도록 하는 우선순위."""
    return sorted(views, key=lambda d: (
        not d.deliverable,
        d.secondary_type,
        not d.text_extractable,
        not _CERT_HINT.search(d.filename or ''),
        not d.issue_date,           # 발행일 미상은 뒤로
        -_iso_rank(d.issue_date),   # 같은 조건이면 최신본 우선
        d.filename,
    ))


def _iso_rank(v: str) -> int:
    # issue_date 3건은 '2018-12-10 (초판); ...' 같은 자유서술이라 파싱하지 않는다.
    if len(v or '') != 10:
        return 0
    return int(v.replace('-', '') or 0)


# ---------------------------------------------------------------- RoHS 요청이력

def rohs_history(family_keys: Sequence[str],
                 kb: Optional[KnowledgeBase] = None) -> List[Dict[str, Any]]:
    """해당 제품군의 RoHS 요청 이력. 토큰은 절단 아티팩트가 섞여 있어 제품군으로 정규화한다."""
    kb = kb or loader.load_kb()
    targets = {k.upper() for k in family_keys}
    out: List[Dict[str, Any]] = []
    for req in kb.rohs_requests:
        for tok in req.models:
            r = resolver.resolve_cert(tok, kb)
            if not r.found:
                continue                      # LS-5 / LSN-BOO 같은 파싱 절단 토큰
            if not ({k.upper() for k in r.keys} & targets):
                continue
            out.append({'date': req.date, 'subject': req.subject, 'model': tok,
                        'status': req.status.get(tok.upper(), '')})
            break
    return sorted(out, key=lambda r: r['date'])


# ---------------------------------------------------------------- 자료유형 결과

@dataclass
class TypeResult:
    """한 제품군 × 한 자료유형."""
    doc_type: str
    mark: str                       # O / O* / 자재 / 자재* / X (매트릭스 밖이면 계산값)
    in_matrix: bool                 # precomputed coverage 축인가
    held: bool
    inferred: bool                  # O* / 자재* — 추론 매핑
    bundle: bool                    # 자재 / 자재* — 자재별 성적서 묶음
    documents: List[DocView] = field(default_factory=list)
    material_covered: List[str] = field(default_factory=list)
    material_missing: List[str] = field(default_factory=list)
    material_coverage_known: bool = False
    #: 매트릭스는 X 인데 all_types 로만 걸린 문서가 있는 경우(예: doc_type=Test report 인 CE 성적서).
    secondary_only: bool = False
    #: 미확보 자재 중 다른 유형 문서로만 확보된 것 {자재: [doc_type]}. 커버로 착각하면 안 된다.
    missing_covered_elsewhere: Dict[str, List[str]] = field(default_factory=dict)
    not_held_reason: str = ''
    request_history: List[Dict[str, Any]] = field(default_factory=list)
    marks_by_family: Dict[str, str] = field(default_factory=dict)

    @property
    def scanned_documents(self) -> List[DocView]:
        """원문 인용이 불가능한 문서들."""
        return [d for d in self.documents if not d.text_extractable]


def _docs_of_type(kb: KnowledgeBase, family_keys: Sequence[str], doc_type: str,
                  queried: Sequence[str], today: Optional[str] = None) -> List[DocView]:
    seen: Set[str] = set()
    views: List[DocView] = []
    for fam in family_keys:
        for rec in kb.documents(fam):
            did = rec.get('doc_id')
            if did in seen:
                continue
            primary = rec.get('doc_type') == doc_type
            secondary = not primary and doc_type in (rec.get('all_types') or [])
            if not (primary or secondary):
                continue
            seen.add(did)
            views.append(make_doc_view(rec, queried, secondary_type=secondary, today=today))
    return _sort_docs(views)


def _merge_material_coverage(kb: KnowledgeBase, family_keys: Sequence[str],
                             doc_type: str) -> Tuple[List[str], List[str], bool]:
    covered: Set[str] = set()
    missing: Set[str] = set()
    known = False
    for fam in family_keys:
        rec = kb.family(fam) or {}
        mc = (rec.get('material_coverage') or {}).get(doc_type)
        if not mc:
            continue                     # '자재'인데 커버리지가 없는 조합 28건 — 지어내지 않는다
        known = True
        covered |= set(mc.get('covered') or [])
        missing |= set(mc.get('missing') or [])
    return sorted(covered), sorted(missing - covered), known


def type_result(family_keys: Sequence[str], doc_type: str,
                kb: Optional[KnowledgeBase] = None, queried: Sequence[str] = (),
                today: Optional[str] = None) -> TypeResult:
    """제품군(복수 가능) 기준으로 특정 자료유형의 보유 상태를 판정한다."""
    kb = kb or loader.load_kb()
    docs = _docs_of_type(kb, family_keys, doc_type, queried, today)
    in_matrix = doc_type in MATRIX_COLS

    marks: Dict[str, str] = {}
    if in_matrix:
        for fam in family_keys:
            marks[fam] = ((kb.family(fam) or {}).get('coverage') or {}).get(doc_type, 'X')
        # 별표는 '더 나쁜 마크'가 아니라 경고 플래그다. 우열 비교는 별표를 뗀 값으로 하고
        # 별표 자체는 sticky 하게 유지한다 — 그러지 않으면 O* 가 O 에 밀려 주의문구가 사라진다.
        starred = any(m.endswith('*') for m in marks.values())
        # 제품군 매트릭스가 O 인데 모델 단위로만 O* 인 칸이 7개 있다(LS-OM3-LCLC RoHS 등).
        # 제품군 답변은 members 전체에 적용되므로 형제 모델의 별표도 함께 본다.
        if not starred:
            scope = set(queried)
            for fam in family_keys:
                scope.update((kb.family(fam) or {}).get('members') or [])
            starred = any((kb.model_marks.get((m or '').upper()) or {})
                          .get(doc_type, '').endswith('*') for m in scope)
        base = max((m.rstrip('*') for m in marks.values()),
                   key=lambda m: _MARK_RANK.get(m, 0)) if marks else 'X'
        mark = f'{base}*' if starred and base != 'X' else base
    else:
        # 매트릭스 밖 유형은 문서에서 직접 판정한다(X 로 단정하면 오답).
        primary = [d for d in docs if not d.secondary_type]
        pool = primary or docs
        if not pool:
            mark = 'X'
        elif any(d.scope == 'finished_product' for d in pool):
            mark = 'O'
        elif all(d.scope == 'company_wide' for d in pool):
            mark = '전사'          # 제조사 단위 인증. 모델별 인증서로 답하면 안 된다
        else:
            mark = '자재'
        marks = {fam: mark for fam in family_keys}

    # coverage 는 doc_type(주 타입)만 세므로 all_types 로만 걸린 문서를 '미보유'로 답하면 안 된다.
    secondary_only = bool(docs) and mark == 'X' and all(d.secondary_type for d in docs)
    res = TypeResult(doc_type=doc_type, mark=mark, in_matrix=in_matrix,
                     held=bool(docs) and (mark != 'X' or secondary_only),
                     inferred=mark.endswith('*'),
                     bundle=mark.startswith('자재'),
                     secondary_only=secondary_only,
                     documents=docs, marks_by_family=marks)

    if res.bundle or doc_type in BUNDLE_TYPES:
        cov, miss, known = _merge_material_coverage(kb, family_keys, doc_type)
        res.material_covered, res.material_missing, res.material_coverage_known = cov, miss, known
        # 예: LS-HDAOC 의 절연 자료는 존재하지만 doc_type 이 CoC 라 RoHS 묶음에 안 들어간다.
        for d in docs:
            if not d.secondary_type:
                continue
            for cat in d.material_cats:
                if cat in miss:
                    res.missing_covered_elsewhere.setdefault(cat, [])
                    if d.doc_type not in res.missing_covered_elsewhere[cat]:
                        res.missing_covered_elsewhere[cat].append(d.doc_type)

    if not res.held:
        hist = rohs_history(family_keys, kb) if doc_type == 'RoHS' else []
        # 발송메일 전수 판독 대장(전 자료유형). 질의 모델과 제품군 members 를 함께 조회한다.
        scope = set(queried) | {f.upper() for f in family_keys}
        for fam in family_keys:
            scope.update(m.upper() for m in ((kb.family(fam) or {}).get('members') or []))
        for m in sorted(scope):
            led = kb.request_ledger.get((m, doc_type))
            if led:
                res.request_history.append({
                    'date': led.get('last') or led.get('first', ''),
                    'first': led.get('first', ''), 'count': led.get('count', ''),
                    'subject': led.get('subject', ''), 'model': m,
                    'to': led.get('to', ''), 'quote': led.get('quote', ''),
                })
        res.request_history.extend(hist)
        res.not_held_reason = X_REQUESTED_NOT_PROVIDED if res.request_history else X_NO_EVIDENCE
    return res


# ---------------------------------------------------------------- 제품군 결과

@dataclass
class FamilyResult:
    """제품군 보유 현황 요약."""
    query: str
    resolution: resolver.Resolution
    families: List[str] = field(default_factory=list)
    members: List[str] = field(default_factory=list)          # 사람에게 보여줄 멤버
    members_raw: List[str] = field(default_factory=list)
    match_rule: str = ''
    coverage: Dict[str, str] = field(default_factory=dict)     # 5축 대표 마크
    types: Dict[str, TypeResult] = field(default_factory=dict)  # 5축 상세
    other_types: List[str] = field(default_factory=list)        # 그 외 보유 유형
    company_wide: List[DocView] = field(default_factory=list)   # 제조사 전사 인증
    document_count: int = 0
    sibling_only: bool = False        # 질의 모델명이 문서에 직접 없는 경우

    @property
    def found(self) -> bool:
        return bool(self.families)


def family_result(query: str, kb: Optional[KnowledgeBase] = None,
                  today: Optional[str] = None) -> FamilyResult:
    """모델명 하나로 제품군 전체 보유현황을 만든다."""
    kb = kb or loader.load_kb()
    res = resolver.resolve_cert(query, kb)
    out = FamilyResult(query=query, resolution=res)
    if not res.found:
        return out

    out.families = res.keys
    queried = [res.normalized]
    raw_members: List[str] = []
    for fam in res.keys:
        rec = kb.family(fam) or {}
        raw_members.extend(rec.get('members') or [])
        out.match_rule = out.match_rule or rec.get('match_rule', '')
    out.members_raw = sorted(set(raw_members))
    out.members = loader.display_members(out.members_raw) or out.members_raw

    docs: Dict[str, Dict[str, Any]] = {}
    for fam in res.keys:
        for rec in kb.documents(fam):
            docs[rec['doc_id']] = rec
    out.document_count = len(docs)

    for col in MATRIX_COLS:
        tr = type_result(res.keys, col, kb, queried, today)
        out.types[col] = tr
        out.coverage[col] = tr.mark

    present: Set[str] = set()
    for rec in docs.values():
        present.update(rec.get('all_types') or [rec.get('doc_type')])
    out.other_types = sorted(t for t in present if t and t not in MATRIX_COLS)

    out.company_wide = _sort_docs([make_doc_view(r, queried, today=today)
                                   for r in docs.values() if r.get('scope') == 'company_wide'])

    if len(out.members_raw) > 1:
        out.sibling_only = not any(res.normalized in [m.upper() for m in (r.get('models') or [])]
                                   for r in docs.values())
    return out


# ---------------------------------------------------------------- KC 결과

def _kc_basename(name: str) -> str:
    """zip 내부 경로에서 파일명만. 폴더 접두사에 모델명이 들어 있어 먼저 떼어내야 한다."""
    n = unicodedata.normalize('NFC', name or '').replace('﻿', '')
    return n.rsplit('/', 1)[-1]


def classify_kc_contents(contents: Sequence[str]) -> Dict[str, List[str]]:
    """zip 내부 파일명을 서류 종류로 분류한다(zip 을 열지 않는다)."""
    out: Dict[str, List[str]] = {}
    for raw in contents or []:
        base = _kc_basename(raw)
        flat = re.sub(r'\s+', '', base)
        for kind, keys in KC_DOC_KINDS:
            hit = any(k in flat for k in keys)
            if not hit and kind == '시험성적서':
                hit = bool(_KC_REPORT_RX.match(flat))
            if hit:
                out.setdefault(kind, []).append(base)
    return out


@dataclass
class KCResult:
    """KC 적합등록 자료 조회 결과."""
    query: str
    resolution: resolver.Resolution
    status: str = KC_NOT_INDEXED
    model: str = ''
    record: Dict[str, Any] = field(default_factory=dict)
    contents: List[str] = field(default_factory=list)
    kinds: Dict[str, List[str]] = field(default_factory=dict)
    received_date: str = ''
    last_mentioned: str = ''
    sender: str = ''
    mail_subject: str = ''
    size_bytes: int = 0
    versions: int = 0
    note: str = ''
    tentative: bool = False           # prefix/stem 추정 매칭

    @property
    def has_certificate(self) -> bool:
        """적합등록필증(또는 적합등록증)이 zip 에 들어 있는가."""
        return '필증' in self.kinds

    def has_kind(self, kind: str) -> bool:
        """'외관도 포함?' 같은 질문에 zip 을 열지 않고 답한다."""
        return kind in self.kinds


def kc_result(query: str, kb: Optional[KnowledgeBase] = None) -> KCResult:
    """KC 인덱스 조회. has_kc 는 True/'partial'/False 3상태다."""
    kb = kb or loader.load_kb()
    res = resolver.resolve_kc(query, kb)
    out = KCResult(query=query, resolution=res)
    if not res.found:
        return out

    rec = kb.kc_record(res.key) or {}
    out.model = rec.get('model', res.key)
    out.record = rec
    out.tentative = not res.confident
    out.note = rec.get('note', '') or ''
    has = rec.get('has_kc')
    if has is True:
        out.status = KC_FULL
        out.contents = list(rec.get('contents') or [])
        out.kinds = classify_kc_contents(out.contents)
        out.received_date = loader.to_iso_date(rec.get('received_date'))
        out.sender = rec.get('sender', '')
        out.mail_subject = rec.get('mail_subject', '')
        out.size_bytes = int(rec.get('size_bytes') or 0)
        out.versions = int(rec.get('versions') or 1)
    elif has == 'partial':
        out.status = KC_PARTIAL
        out.received_date = loader.to_iso_date(rec.get('received_date'))
    else:
        out.status = KC_NONE
        out.last_mentioned = loader.to_iso_date(rec.get('last_mentioned'))
    return out


# ---------------------------------------------------------------- 통합 조회

@dataclass
class LookupResult:
    """인증자료 + KC 를 함께 담은 조회 결과. 한쪽이 없어도 다른 쪽으로 답할 수 있다."""
    query: str
    doc_type: Optional[str]
    cert: FamilyResult
    kc: KCResult
    type_result: Optional[TypeResult] = None

    @property
    def ambiguous(self) -> bool:
        return self.cert.resolution.ambiguous or (
            not self.cert.found and self.kc.resolution.ambiguous)

    @property
    def found(self) -> bool:
        return self.cert.found or self.kc.status != KC_NOT_INDEXED


def lookup(query: str, doc_type: Optional[str] = None,
           kb: Optional[KnowledgeBase] = None, today: Optional[str] = None) -> LookupResult:
    """모델명(+선택적 자료유형)으로 두 인덱스를 모두 조회한다."""
    kb = kb or loader.load_kb()
    dt = normalize_doc_type(doc_type) if doc_type else None
    cert = family_result(query, kb, today)
    kc = kc_result(query, kb)
    out = LookupResult(query=query, doc_type=dt, cert=cert, kc=kc)
    if dt and cert.found:
        out.type_result = cert.types.get(dt) or type_result(
            cert.families, dt, kb, [cert.resolution.normalized], today)
    return out
