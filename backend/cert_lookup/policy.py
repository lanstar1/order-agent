#!/usr/bin/env python3
"""응답 정책 — 무엇을 보여주고 무엇에 주의문구를 붙일지.

두 모드가 있다.
  internal : 사내 영업·CS 담당자. 추론 매핑(O*/자재*)도 보여주되 주의문구를 강제한다.
  customer : 고객 직접 노출. 추론 매핑 자료는 목록에서 아예 빼고 "확인 후 회신"으로 대체한다.
             파일은 링크로 바로 뿌리지 않고 자료요청서 양식을 안내한다(기존 상담 정책과 일치).

두 모드 공통으로 막는 것: deliverable=False 문서의 파일 전달, 출처 매핑이 반박된 문서의 인용,
스캔 문서(text_extractable=false)의 내용 인용.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .search import (X_NO_REQUEST, X_REQUESTED_NOT_PROVIDED, DocView, TypeResult)

INTERNAL, CUSTOMER = 'internal', 'customer'

# ---------------------------------------------------------------- 주의문구

INFERRED_NOTICE = (
    '※ 추론 매핑(O*/자재*) 자료입니다. 라인 단위 인증서를 제품군에 대응시킨 것으로 '
    '대표 모델 시험 기반이며, 고객사 제출 전 해당 모델이 인증서 적용범위에 포함되는지 '
    '확인이 필요합니다.')
BUNDLE_NOTICE = (
    '※ 완제품 단위 인증서가 아니라 자재별 시험성적서 묶음입니다. 수입 케이블은 도체·절연·'
    '자켓·차폐 등 자재별 성적서를 묶어 근거로 삼는 것이 정상입니다.')
BUNDLE_MISSING_NOTICE = '※ 미확보 자재: {}. 해당 자재 성적서는 공급사에 추가 요청이 필요합니다.'
BUNDLE_MISSING_ELSEWHERE_NOTICE = (
    '※ 미확보 자재 중 {mat} 은(는) {other} 문서로만 확보돼 있어 {dt} 성적서로는 여전히 '
    '미확보입니다.')
BUNDLE_COMPLETE_NOTICE = (
    '※ 핵심 자재(도체/절연/자켓/차폐) 기준 누락은 없습니다. 커넥터·도금 등 부가 자재의 '
    '성적서 유무는 별도 확인이 필요합니다.')
BUNDLE_UNKNOWN_NOTICE = (
    '※ 자재별 성적서 묶음이며, 전 자재를 모두 커버하는지는 검증되지 않았습니다.')
SCAN_NOTICE = (
    '※ 스캔 이미지 문서라 원문 텍스트를 추출할 수 없습니다. 파일 전달은 가능하지만 '
    '수치·문구 인용은 원본을 직접 확인해 주십시오.')
NOT_DELIVERABLE_NOTICE = (
    '※ 단독 전달 불가로 분류된 자료입니다. 고객 제공 전 담당자 확인이 필요합니다.')
EXPIRED_NOTICE = '※ 문서에 표기된 유효기간({})이 경과했습니다. 갱신본 확인이 필요합니다.'
NO_VALID_UNTIL = '문서에 유효기간 표기 없음'
SIBLING_NOTICE = '※ 질의하신 모델명이 문서에 직접 표기돼 있지는 않습니다. {}'
#: prefix 매칭은 오타든 별개 제품이든 조용히 삼킨다(LS-6UTPDD 가 LS-6UTPD 로 붙는다).
#: 확정으로 답하지 않고 이 문구를 반드시 함께 낸다.
TENTATIVE_MATCH_NOTICE = (
    '※ 입력하신 {q} 는 등록 제품군 {key} 기준 추정 매칭입니다. '
    '동일 제품이 맞는지 확인 후 사용해 주십시오.')
NOT_HELD_BASE = '현재 확보된 자료가 없습니다. 공급사에 요청이 필요합니다.'
NOT_HELD_NO_REQUEST = (
    NOT_HELD_BASE + ' (과거 요청 이력 자체가 없어 매핑 누락이 아니라 미요청 건입니다.)')
NOT_HELD_REQUESTED = (
    NOT_HELD_BASE + ' 과거 {n}회 요청 이력({dates})이 있으나 공급사가 제공하지 않았습니다.')
CUSTOMER_INFERRED_REPLACEMENT = (
    '해당 유형은 적용범위 확인이 필요하여 담당자 확인 후 회신드리겠습니다.')
CUSTOMER_FILE_NOTICE = (
    '자료 제공은 자료요청서 양식 접수 후 진행됩니다: '
    'https://www.lanmart.co.kr/shop/board/view.php?id=notice&no=667')
ATTRIBUTION_REJECTED_NOTICE = '※ 출처 매핑이 확정되지 않은 자료라 근거에서 제외했습니다.'
COMPANY_WIDE_NOTICE = (
    '※ 특정 모델의 인증서가 아니라 제조사(공장) 전사 단위 인증입니다. 모델별 인증서 요청에 '
    '이 자료만 제공하면 안 됩니다.')

KC_NO_TEXT_NOTICE = (
    '※ KC 자료는 zip 내부 문서 본문이 색인돼 있지 않습니다. 필증 번호·유효기간·시험규격은 '
    '파일을 직접 확인해 주십시오.')
KC_NONE_NOTICE = (
    '(주)아이케이씨인증원과 KC 적합등록 진행 이력은 확인되나{when} 완료자료를 수신한 기록이 '
    '없습니다. 인증원(leh@i-kc.co.kr) 재요청이 필요합니다.')
KC_PARTIAL_NOTICE = (
    '적합등록필증 PDF 1건만 보유하고 있습니다. 진본성적서·외관도·부품배치도·위임서·확인서 등 '
    '완료자료 일체는 미수신 상태로, 필요 시 인증원 재요청이 필요합니다.')
KC_NOT_INDEXED_NOTICE = (
    'KC 자료 색인 범위 밖 모델입니다. KC 적합등록 진행 여부 자체를 확인해야 합니다.')
KC_TENTATIVE_NOTICE = (
    '※ 입력하신 {q} 는 KC 등록모델 {base} 기준으로 안내드립니다(추정 매칭 — 확인 필요).')

#: caveat_ko 가 비어 있는 3건은 전부 공급사 자체 사양서다 — '주의사항 없음'으로 두면 안 된다.
DEFAULT_CAVEAT = {
    'Data sheet': '공급사가 발행한 사양서이며 제3자 인증서가 아닙니다.',
    'Drawing': '공급사가 발행한 도면이며 제3자 인증서가 아닙니다.',
}
DEFAULT_CAVEAT_GENERIC = '전달 시 적용범위와 발행 주체를 함께 안내해 주십시오.'


def effective_caveat(doc: DocView) -> str:
    """caveat_ko 는 가공하지 않고 그대로. 비어 있을 때만 유형별 기본 문구로 대체한다."""
    if doc.caveat_ko:
        return doc.caveat_ko
    return DEFAULT_CAVEAT.get(doc.doc_type, DEFAULT_CAVEAT_GENERIC)


# ---------------------------------------------------------------- 정책

@dataclass
class Policy:
    """모드별 노출 규칙."""
    mode: str = INTERNAL

    @property
    def show_inferred(self) -> bool:
        """추론 매핑(O*/자재*) 자료를 목록에 넣는가."""
        return self.mode != CUSTOMER

    @property
    def show_file_location(self) -> bool:
        """드라이브 링크·로컬 경로를 응답에 노출하는가."""
        return self.mode != CUSTOMER

    @property
    def show_undeliverable(self) -> bool:
        """단독 전달 불가 자료를 '보유 사실'로라도 보여주는가."""
        return self.mode != CUSTOMER

    def may_share_file(self, doc: DocView) -> bool:
        """이 문서의 파일을 실제로 내보내도 되는가(하드 게이트)."""
        if not doc.deliverable or doc.attribution_rejected:
            return False
        # 고객 모드에서는 완제품 단위 문서만 — 자재/부품 성적서 원본을 단독 전달하지 않는다
        return not (self.mode == CUSTOMER and doc.scope != 'finished_product')

    def may_quote(self, doc: DocView) -> bool:
        """문서 내용을 인용해도 되는가(스캔 문서는 불가)."""
        return doc.text_extractable and not doc.attribution_rejected

    def filter_documents(self, docs: Sequence[DocView],
                         inferred: bool = False) -> Tuple[List[DocView], List[DocView]]:
        """(보여줄 문서, 감춘 문서) 로 나눈다."""
        visible: List[DocView] = []
        hidden: List[DocView] = []
        for d in docs:
            if d.attribution_rejected:
                hidden.append(d)
            elif inferred and not self.show_inferred:
                hidden.append(d)
            elif not d.deliverable and not self.show_undeliverable:
                hidden.append(d)
            else:
                visible.append(d)
        return visible, hidden

    # -- 주의문구 조립 -------------------------------------------------
    def notices_for_type(self, tr: TypeResult, sibling_only: bool = False,
                         match_rule: str = '') -> List[str]:
        """자료유형 응답에 반드시 따라붙어야 하는 문구들."""
        out: List[str] = []
        if tr.inferred:
            out.append(INFERRED_NOTICE if self.show_inferred
                       else CUSTOMER_INFERRED_REPLACEMENT)
        if tr.bundle:
            out.append(BUNDLE_NOTICE)
            if tr.material_missing:
                out.append(BUNDLE_MISSING_NOTICE.format(', '.join(tr.material_missing)))
                for mat, types in tr.missing_covered_elsewhere.items():
                    out.append(BUNDLE_MISSING_ELSEWHERE_NOTICE.format(
                        mat=mat, other='/'.join(types), dt=tr.doc_type))
            elif tr.material_coverage_known:
                out.append(BUNDLE_COMPLETE_NOTICE)
            else:
                out.append(BUNDLE_UNKNOWN_NOTICE)
        if tr.mark == '전사':
            out.append(COMPANY_WIDE_NOTICE)
        if any(not d.text_extractable for d in tr.documents):
            out.append(SCAN_NOTICE)
        if self.show_undeliverable and any(not d.deliverable for d in tr.documents):
            out.append(NOT_DELIVERABLE_NOTICE)
        for d in tr.documents:
            if d.expired:
                out.append(EXPIRED_NOTICE.format(d.valid_until))
        if sibling_only and match_rule:
            out.append(SIBLING_NOTICE.format(match_rule))
        if any(d.attribution_rejected for d in tr.documents):
            out.append(ATTRIBUTION_REJECTED_NOTICE)
        if self.mode == CUSTOMER and tr.held:
            out.append(CUSTOMER_FILE_NOTICE)
        return dedupe(out)

    def not_held_text(self, tr: TypeResult) -> str:
        """X 응답 문장. 요청 이력 유무로 사유가 갈린다."""
        if tr.not_held_reason == X_REQUESTED_NOT_PROVIDED and tr.request_history:
            dates = ', '.join(sorted({h['date'] for h in tr.request_history}))
            return NOT_HELD_REQUESTED.format(n=len(tr.request_history), dates=dates)
        if tr.not_held_reason == X_NO_REQUEST:
            return NOT_HELD_NO_REQUEST
        return NOT_HELD_BASE


def dedupe(items: Sequence[str]) -> List[str]:
    seen: Dict[str, bool] = {}
    for i in items:
        if i and i not in seen:
            seen[i] = True
    return list(seen)


def get_policy(mode: Optional[str] = None) -> Policy:
    """모드 문자열로 정책 객체를 얻는다.

    알 수 없는 값을 internal 로 흡수하면 'customerr' 오타 하나로 고객에게 사내 전용
    추론매핑 자료와 내부 파일 경로가 나간다. 조용히 넘기지 않고 예외로 세운다.
    """
    if mode is None or mode == '':
        return Policy(mode=INTERNAL)
    m = mode.lower().strip()
    if m not in (INTERNAL, CUSTOMER):
        raise ValueError(f"mode 는 '{INTERNAL}' 또는 '{CUSTOMER}' 여야 합니다: {mode!r}")
    return Policy(mode=m)
