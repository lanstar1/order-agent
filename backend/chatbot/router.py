#!/usr/bin/env python3
"""0단계 의도 분기 — 판단만 하고 답변은 만들지 않는다.

통합방안.md 1-2절 route() 가 원안이지만, 상담 5,313건 전수 실측에서
정밀도 27.7% (cert 173건 중 진짜 자료요청 48건) 로 확인돼 상수를 교체했다.
개선 후 정밀도 93.0% / 재현율 100%.

핵심 교정 4가지
  1. '모델명 + 범용 요청동사'(원안 규칙 B)는 삭제. 오탐 173건 중 100건이 여기서 나왔다
     ('LS-2000HS 아이보리 색상도 있나요' → '있나요' 매칭).
  2. 자료유형을 HARD(단독 발동) / SOFT(요청동사 필수 + 참조용법 배제) 2등급으로 분리.
     Data sheet 는 '스펙' 별칭 때문에 단독 정밀도 11%, Manual 27% 였다.
  3. 드라이버·설치파일·펌웨어(176건)는 cert_lookup KB 에 유형 자체가 없다 → 별도 라벨.
  4. 부정신호(반품/재고/견적)는 veto 가 아니라 '혼합(mixed)' 신호다.
     veto 로 쓰면 '견적서 + 설명서 첨부 부탁' 같은 실제 혼합 문의가 죽는다.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Set

from cert_lookup import cli as cert_cli

# ---------------------------------------------------------------- 자료유형 등급

#: 일상어와 겹치지 않는다 → 단어가 보이면 무조건 인증 경로
HARD_TYPES = frozenset({'RoHS', 'CE', 'UL', 'REACH', 'MSDS/SDS', 'PFAS',
                        'Test report', 'CoC', 'Fluke test', 'HDMI-DP-cert', 'Other-cert'})
#: 문서명이지만 '설명서에 나온 대로' 처럼 참조로 쓰이는 일이 더 많다 → 요청 동사 필수
SOFT_TYPES = frozenset({'Manual', 'Drawing', 'Data sheet'})

# ---------------------------------------------------------------- 요청 동사구

#: 원안에서 '인증서/증명서/서류/자료/파일/양식/제출'을 빼고(→ CERT_NOUNS 로 이동)
#: '받-' 계열 12종과 '부탁/전달/송부/제공/공유/메일로/얻-'을 넣었다.
#: '있나요/주세요/확인' 같은 범용어는 남기되 문서명사 주변 창에서만 평가한다.
REQUEST_WORDS = (
    '받아볼', '받아 볼', '받아보', '받을 수', '받을수', '받고 싶', '받고싶', '받았으면',
    '받아서', '받는', '받기', '전달', '보내주', '보내 주', '송부', '제공', '요청',
    '부탁', '첨부', '공유', '발행', '메일로', '메일 주소', '얻을', '얻고', '얻고싶',
    '필요합니다', '필요해서', '필요한데', '필요합니', '있을까요', '있나요', '있으면',
    '없나요', '없을까요', '주실', '주시면', '주세요', '주십시오', '가능할까요', '확인',
)

#: 모델명·요청동사와 무관하게 단독으로 인증 경로를 여는 문서명사.
#: '(?<!공인)' 이 없으면 '공인인증서로 결재를 하는데' 가 오탐된다.
CERT_NOUN_RX = re.compile(
    r'(?<!공인)인증서|증명서|시험성적서|검사성적서|성적서|시험서|밀\s?시트|'
    r'안전인증|적합등록|적합인증|전파인증|환경자료|유해물질|HS\s?코드|HS\s?CODE|'
    r'원산지|난연|'
    # 맨 '인증'도 열어야 한다. 이게 없으면 'LS-F850 인증 받은 제품인가요?' 가 legacy 로
    # 빠져 제품분석의 'UL94 V-0'·'FDA 승인' 같은 규격 주장이 근거로 나간다(실측 17건).
    # '공인인증서'·'인증번호 입력' 같은 결제·로그인 맥락은 제외한다.
    r'(?<!공인)인증(?!\s?번호|서\s?로그인|\s?로그인)', re.IGNORECASE)

#: '자료/서류' 같은 총칭 명사. 단독으로는 약해서 (모델 확정 + 주변 요청동사) 를 요구한다.
GENERIC_DOC_RX = re.compile(r'자료(?!실)|서류|증빙|스펙\s?시트')

#: KB Drawing 이지만 별칭에 없는 표현. '리모델링' 오탐 방지 룩비하인드 필수.
DRAWING_EXTRA_RX = re.compile(r'(?<!리)모델링|3D\s?모델|STEP\s?파일|외형도|치수도|조립도(?!면)')

#: 부정 신호 — 기각이 아니라 혼합 신호로 쓴다.
#: 'AS' 는 반드시 단어경계로 본다. 그냥 부분문자열로 쓰면 LS-AS302N·LS-PASS-DB9M 처럼
#: 모델명에 AS 가 들어간 제품의 인증 문의가 전부 mixed 로 새어나간다(실측 11개 모델).
NEGATIVE_WORDS = ('반품', '교환', '환불', '배송', '재고', '가격', '단가', '견적',
                  'AS', '불량', '고장', '안 돼', '안돼', '설치 방법', '사용법')
NEGATIVE_RX = re.compile(
    r'반품|교환|환불|배송|재고|가격|단가|견적|불량|고장|안\s?돼|설치\s?방법|사용법|'
    r'에이에스|(?<![A-Za-z])A/?S(?![A-Za-z])')

#: 드라이버/설치파일 — cert_lookup KNOWN_DOC_TYPES 에 없다. cert 로 보내면 전건 '자료 없음'.
DRIVER_RX = re.compile(
    r'드라이버|드라이브\s?(설치|다운)|설치\s?파일|설치파일|설치\s?프로그램|설치프로그램|'
    r'소프트웨어|프로그램\s?(설치|다운)|펌웨어|자료실|driver|\.exe|인스톨', re.IGNORECASE)

#: KB 에 유형 자체가 없는 문서 요청 — '없다'고 답하기보다 담당자 에스컬레이션이 안전하다.
UNSUPPORTED_DOCS = (
    ('카탈로그', re.compile(r'카탈로그|카다록|카타로그|브로슈어|브로셔')),
    ('품질보증서', re.compile(r'품질\s?보증서|보증서(?!류)')),
    ('단종확인서', re.compile(r'단종\s?확인서|단종을?\s?증명')),
    ('MFi/MFM 인증', re.compile(r'MFi|MFM', re.IGNORECASE)),
)

#: SOFT 문서명이 '참조'로 쓰였는지 판정 — 접미사로 거의 100% 분리된다.
SOFT_REF_RX = re.compile(
    r'(설명서|매뉴얼|사양서|규격서|도면|데이터시트)\s*(에|엔|은|대로|처럼|보고|에서|등|방법|상[^기])')
SOFT_NOUN_RX = re.compile(r'설명서|매뉴얼|사양서|규격서|도면|데이터\s?시트|데이타\s?시트|캐드|CAD|'
                          r'datasheet|data\s?sheet|manual|(?<!리)모델링|3D\s?모델|STEP\s?파일|'
                          r'외형도|치수도', re.IGNORECASE)
#: '480정도면 여유있는데' 의 '도면' 부분문자열 오탐 방지
BAD_DRAWING_RX = re.compile(r'[0-9정]도면')
#: 랜스타 상품명에 박힌 마케팅 문구. 라우팅 전에 지우지 않으면 intent 가 kc 로 뒤집힌다.
KC_TITLE_RX = re.compile(r'(KC|kc)\s?인증\s?완료')

#: 되묻기 없이 보유현황을 묻는 표현
COVERAGE_RX = re.compile(r'뭐뭐|어떤\s?자료|무슨\s?자료|보유\s?현황|자료\s?목록|있는\s?자료')

_WIN_BEFORE, _WIN_AFTER = 15, 40


def _clean(msg: str) -> str:
    """상품명 안의 'KC인증 완료' 같은 마케팅 문구 제거."""
    return KC_TITLE_RX.sub(' ', msg or '')


def mask_models(msg: str) -> str:
    """모델 토큰을 **같은 길이의 공백**으로 덮는다(문자 오프셋 보존).

    ASCII 신호어(AS/driver/CAD/manual/MFi)를 모델명 안에서 잡지 않기 위한 것이다.
    detect_doc_types() 는 내부에서 이미 같은 일을 한다.
    """
    return cert_cli.MODEL_RX.sub(lambda m: ' ' * len(m.group(0)), msg or '')


def _doc_types(msg: str) -> Set[str]:
    """cert_lookup.detect_doc_types() 결과에서 부분문자열 오탐을 후처리로 폐기.

    cert_lookup 은 69테스트 통과 자산이라 손대지 않는다. 교정은 여기서 한다.
    """
    types = set(cert_cli.detect_doc_types(msg))
    # '충전기 스펙이 20v인데' — Data sheet 27건 중 17건이 '스펙' 단독이고 TP 0건
    if 'Data sheet' in types and not re.search(
            r'데이터\s?시트|데이타\s?시트|datasheet|data\s?sheet|사양서|규격서|제품사양', msg, re.I):
        types.discard('Data sheet')
    # '480정도면' 의 '도면'
    if 'Drawing' in types and not re.search(r'캐드|CAD|drawing', msg, re.I):
        if '도면' not in BAD_DRAWING_RX.sub(' ', msg):
            types.discard('Drawing')
    if DRAWING_EXTRA_RX.search(msg):
        types.add('Drawing')
    return types


def _request_near(msg: str, rx: re.Pattern) -> bool:
    """문서명사 주변 창(앞15/뒤40) 안에서만 요청 동사구를 본다.

    전체 문장 대상 has_req 판정은 절대 금지 — '번갈아서 보내주는 기능없나요' 가 잡힌다.
    """
    for m in rx.finditer(msg):
        win = msg[max(0, m.start() - _WIN_BEFORE):m.end() + _WIN_AFTER]
        if any(w in win for w in REQUEST_WORDS):
            return True
    return False


def _unsupported(masked: str) -> List[str]:
    return [name for name, rx in UNSUPPORTED_DOCS if rx.search(masked)]


def _cert_intent(msg: str, parsed, types: Set[str], generic: bool) -> str:
    """cert 가 확정된 뒤에만 계산한다.

    parse_message().intent 를 그대로 믿으면 안 된다 — CONTENT_WORDS('얼마','몇 ','적혀')는
    이 코퍼스 171건 전부 오분류였고, 인증 질문에 '수치가 얼마' 가 섞이면 files→content 로
    뒤집혀 파일링크 대신 본문인용 경로로 새어나간다.
    """
    if parsed.intent == 'kc':
        return 'kc'
    if types and any(w in msg for w in cert_cli.CONTENT_WORDS):
        return 'content'          # 자료유형 토큰과 동시출현할 때만 content
    if not types and (generic or COVERAGE_RX.search(msg) or parsed.intent == 'coverage'):
        return 'coverage'
    return 'files'


def route(message: str, known_model: bool = False) -> Dict[str, Any]:
    """0단계 분기.

    known_model: 세션 컨텍스트로 모델이 이미 확정된 후속턴인가. 이 값이 없으면
    '자료 있나요?'·'도면도요' 같은 후속 발화가 문장 안에 모델명이 없다는 이유로
    legacy 로 새어, 인증 문의가 cert_lookup 을 거치지 않는다.

    반환: {'route': 'cert'|'legacy'|'mixed', 'parsed': {...}, 'reason': str, 'flags': {...}}
    """
    msg = _clean(message)
    parsed = cert_cli.parse_message(msg)
    #: 모델 토큰을 덮은 사본 — ASCII 신호어를 모델명 안에서 잡지 않는다
    masked = mask_models(msg)
    types = _doc_types(msg)
    hard = sorted(types & HARD_TYPES)
    soft = sorted(types & SOFT_TYPES)
    cert_noun = bool(CERT_NOUN_RX.search(masked))
    has_neg = bool(NEGATIVE_RX.search(masked))
    is_driver = bool(DRIVER_RX.search(masked))
    unsupported = _unsupported(masked)
    soft_ok = False
    generic_ok = False

    if soft:
        # 참조형 출현을 지운 뒤 남은 출현 주변에서만 요청동사를 본다.
        # '도면에 없는데 도면 원본을 받을 수 있을까요' 같은 참조+요청 혼재도 살아난다.
        soft_ok = _request_near(SOFT_REF_RX.sub(' ' * 4, masked), SOFT_NOUN_RX)
    has_model = bool(parsed.models) or known_model
    if not hard and not cert_noun and has_model:
        generic_ok = _request_near(masked, GENERIC_DOC_RX)

    reason = ''
    if is_driver and not hard and not cert_noun:
        # cert 로 보내면 100% '자료 없음' 오답이 된다(KB 에 driver 유형이 없다)
        return {'route': 'legacy', 'parsed': dict(parsed.as_dict(), cert_intent=None),
                'reason': '드라이버/설치파일 요청 — 인증KB 관리 대상 아님',
                'flags': {'driver': True, 'unsupported_docs': unsupported,
                          'negative': has_neg, 'cert_nouns': cert_noun,
                          'hard': hard, 'soft': soft, 'doc_types': sorted(types)}}

    if hard:
        hit, reason = True, f'HARD 자료유형 {hard}'
    elif cert_noun:
        hit, reason = True, '문서명사(인증서/성적서/원산지/HS코드/난연 등) 단독 발동'
    elif parsed.intent == 'kc':
        hit, reason = True, 'KC 의도(적합등록/전파인증/필증)'
    elif soft:
        hit = soft_ok
        reason = (f'SOFT 자료유형 {soft} + 요청 동사구' if soft_ok
                  else f'SOFT 자료유형 {soft} 이지만 참조 용법(요청 아님)')
    elif generic_ok:
        hit, reason = True, "총칭 문서명사('자료/서류') + 모델 확정 + 요청 동사구"
    elif parsed.intent == 'coverage' and has_model:
        hit, reason = True, '보유현황 문의 + 모델 확정'
    else:
        hit, reason = False, '인증·자료 신호 없음'

    if not hit:
        return {'route': 'legacy', 'parsed': dict(parsed.as_dict(), cert_intent=None),
                'reason': reason,
                'flags': {'driver': is_driver, 'unsupported_docs': unsupported,
                          'negative': has_neg, 'cert_nouns': cert_noun,
                          'hard': hard, 'soft': soft, 'doc_types': sorted(types)}}

    intent = _cert_intent(masked, parsed, types, generic_ok)
    # 부정 신호는 기각이 아니라 혼합 신호 — 두 경로를 각각 태워 섹션을 나눈다
    r = 'mixed' if has_neg else 'cert'
    if has_neg:
        reason += ' + 부정신호 동시출현 → 섹션 분리'
    return {'route': r, 'parsed': dict(parsed.as_dict(), cert_intent=intent),
            'reason': reason,
            'flags': {'driver': is_driver, 'unsupported_docs': unsupported,
                      'negative': has_neg, 'cert_nouns': cert_noun,
                      'hard': hard, 'soft': soft, 'doc_types': sorted(types)}}
