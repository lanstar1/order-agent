#!/usr/bin/env python3
"""경로·상수·환경변수 + 텍스트 안전 필터.

여기에는 '값'과 '순수 문자열 함수'만 둔다. 데이터 로딩은 history/spec 이 한다.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List, Tuple

# ---------------------------------------------------------------- 경로

MASTER_PATH = Path(os.environ.get(
    'LANSTAR_MASTER_PATH', '/Users/lanstar/lanmart/lanstar_product_master.json'))
IMAGE_DIR = Path(os.environ.get(
    'LANSTAR_IMAGE_DIR', '/Users/lanstar/lanmart/product_images'))
#: 동일 이미지가 쇼핑몰에서 공개 서빙된다(실측 200). 4GB 를 서버에 복사하지 않는다.
IMAGE_URL_BASE = os.environ.get(
    'LANSTAR_IMAGE_URL_BASE', 'https://lanmart.co.kr/shop/product/')

# ---------------------------------------------------------------- 응답 문구

GREETING = '랜스타입니다.'
ESCALATION = '정확한 확인을 위해 담당자 확인이 필요합니다.'

#: 제품분석은 이미지 한 장을 보고 만든 추론이다. 실측 오류율 10~17%(제품유형 기준).
SPEC_NOTICE = ('※ 아래 내용은 제품 이미지 기반 AI 참고 정보이며 확정 사양이 아닙니다. '
               '정확한 사양은 데이터시트/도면으로 확인해 주십시오.')
#: 제품유형 교차검증 자체가 불가능한 모델(고객상담 미보유 또는 카테고리 '기타')
SPEC_UNVERIFIED_NOTICE = ('※ 이 모델은 제품유형 교차검증이 불가능해(상담이력 없음/카테고리 미상) '
                          '아래 정보는 참고용 이미지 설명으로만 사용해 주십시오.')
#: 제품유형이 사람 표기와 충돌 — 나머지 필드도 같은 환각 위에서 작성된다(전파율 최대 61%)
SPEC_CONFLICT_NOTICE = ('※ 이 모델의 AI 제품분석은 실제 제품 표기와 충돌해 사양 근거로 쓰지 않았습니다. '
                        '(이미지 기반 오인식 사례)')
HISTORY_NOTICE = ('※ 과거(2023~2025) 상담 기록입니다. 현재 사양·정책과 다를 수 있으니 '
                  '고객 회신 전 확인해 주십시오.')
HISTORY_TALK_NOTICE = ('※ 카카오톡 상담 대화 로그에서 발췌한 내용이라 앞뒤 문맥이 생략돼 있습니다.')
HISTORY_VOLATILE_NOTICE = ('※ 재고·배송·가격·이벤트는 시점 의존 정보라 과거 답변을 그대로 쓰면 안 됩니다. '
                           '현재 값은 실시간 확인이 필요합니다.')
NEED_MODEL_TEXT = ('문의하신 내용에서 제품 모델명을 확인하지 못했습니다. '
                   'LS-6UTPD-3MG 처럼 모델명을 알려주시면 바로 확인해 드리겠습니다.')
#: cert_lookup KB 에 유형 자체가 없는 자료(카탈로그·품질보증서·단종확인서 등)
UNSUPPORTED_DOC_TEXT = ('※ 문의하신 {items} 은(는) 인증·기술자료 KB 관리 대상이 아닙니다. '
                        '담당자 확인이 필요합니다.')
DRIVER_TEXT = ('드라이버·설치파일·펌웨어는 인증·기술자료 KB 관리 대상이 아닙니다. '
               '제품 페이지 자료실 또는 담당자를 통해 확인해 주십시오.')

# ---------------------------------------------------------------- LLM

LLM_MODEL = os.environ.get('LANSTAR_CHAT_MODEL', 'claude-sonnet-5')
LLM_MAX_TOKENS = int(os.environ.get('LANSTAR_CHAT_MAX_TOKENS', '800'))
LLM_TIMEOUT = float(os.environ.get('LANSTAR_CHAT_TIMEOUT', '30'))
LLM_API_KEY_ENV = 'ANTHROPIC_API_KEY'

# ---------------------------------------------------------------- 상담이력 검색

#: 문자 2+3-gram IDF 코사인 실측 캘리브레이션.
#: 완전 무관(OOD) 질의 top-1 = 0.08, 실제 질의 자기제외 top-1 중앙값 = 0.159.
# 임계값은 실측으로 정했다. 모델 스코프 안에서 자기 자신을 제외한 top-1 점수 분포는
# p50 0.095 / p75 0.148 / p90 0.211 / p95 0.262 다(2행 이상 보유 모델 400건 표본).
# 구간별 매칭 품질을 눈으로 확인한 결과 0.25~0.30 은 전건 관련 질문, 0.20~0.25 는 절반,
# 0.20 미만은 잡음이었다. 0.45 는 400건 중 4건만 통과해 '인용 가능'이 사실상 죽은 경로였다.
SCORE_CITE = 0.40          # 이상이면 '인용 가능'
SCORE_REF = 0.25           # 이상이면 '참고' 라벨, 미만은 결과 없음
NGRAM_SIZES: Tuple[int, ...] = (2, 3)
TALK_WEIGHT = 0.7          # talk_* 는 병합 대화 로그라 감점
SIBLING_WEIGHT = 0.85      # 형제 모델 이력은 감점 + 라벨
SMALL_POOL = 5             # 상담 5건 미만이면 top-k 대신 전량 노출
HISTORY_TOP_K = 5

# ---------------------------------------------------------------- 세션

SESSION_TTL_SEC = int(os.environ.get('LANSTAR_CHAT_TTL', str(30 * 60)))
SESSION_MAX_TURNS = 20
SESSION_MAX_SESSIONS = 2000

# ---------------------------------------------------------------- PII 마스킹

#: 상담이력 question 3.2% 에 고객 휴대폰, answer 에 퀵기사 실명+번호, 회사 계좌번호가 있다.
#: 사내용이라도 고객 PII 이므로 예외 없이 마스킹한다.
_PII_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r'\b\d{6}-\d{2}-\d{6}\b'), '[계좌번호]'),
    (re.compile(r'\b\d{3}-\d{2}-\d{5}\b'), '[사업자번호]'),
    (re.compile(r'[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}'), '[이메일]'),
    (re.compile(r'01[016-9][-. ]?\d{3,4}[-. ]?\d{4}'), '[휴대폰]'),
    (re.compile(r'0(?:2|3[1-3]|4[1-4]|5[1-5]|6[1-4])[-. ]\d{3,4}[-. ]\d{4}'), '[전화번호]'),
    # '[로젠 41934222610]' 같은 운송장 — 택배사명이 앞에 있을 때만 지운다(사양 숫자 보호)
    (re.compile(r'(로젠|경동|대한통운|한진|우체국|CJ|롯데|송장|운송장|택배)\s*[:\-]?\s*\d{9,14}'),
     r'\1 [운송장번호]'),
    # 주문번호 — 쇼핑몰 주문번호는 16자리 숫자다(실측 44행). 라벨이 있든 없든 지운다.
    (re.compile(r'(주문\s?번호|오더\s?번호|주문서)\s*[:：\-]?\s*\d{8,20}'), r'\1: [주문번호]'),
    (re.compile(r'(?<![\d.\-])\d{16}(?![\d.\-])'), '[주문번호]'),
    # 배송 주소 — '시/도 + 시군구' 로 시작하는 한국 주소(실측 92행). 번지까지 삼킨다.
    # 쉼표에서 멈추면 '…, 마포한강아이파크 102동 402호' 처럼 상세주소가 남는다 → 쉼표 뒤도
    # 건물명·동·호가 이어지면 계속 삼킨다(실측 13행/12모델).
    (re.compile(
        r'(서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주)'
        r'(?:특별시|광역시|특별자치시|특별자치도|도)?\s*\S{0,12}(?:시|군|구)\s+\S[^\n,]{2,60}'
        r'(?:\s*,[^\n]{0,60}?(?:\d+\s*(?:동|호|층)|아파트|빌딩|타워|오피스텔|빌라|타운)[^\n,]{0,30})*'),
     '[주소]'),
    # 이미 마스킹된 [주소] 뒤에 상세주소만 남은 경우(주소 패턴이 앞부분만 먹은 잔여)
    (re.compile(r'\[주소\]\s*,[^\n]{0,60}?(?:\d+\s*(?:동|호|층))[^\n,]{0,20}'), '[주소]'),
    # 계좌번호 — 은행명이 앞에 있으면 뒤의 숫자열을 지운다. 국민 699601 04 115031,
    # 신한 140-001-885595, 농협 3561292603433, 기업 95701330202013 처럼 형식이 제각각이라
    # 자릿수·구분자를 특정할 수 없다. 은행명을 앵커로 삼는 것이 유일하게 확실한 방법이다.
    (re.compile(
        r'(농협|국민|신한|우리|하나|기업|카카오\s?뱅크|카카오|케이\s?뱅크|토스|새마을|우체국|'
        r'SC제일|씨티|부산|대구|광주|전북|경남|수협|산업|신협|저축)\s*(?:은행)?\s*'
        r'\d[\d\-\s]{7,22}'),
     r'\1은행 [계좌번호]'),
    # 은행명 없이 12자리 이상 연속 숫자 — 계좌·주문번호 외에 이 길이의 사양 값은 없다.
    (re.compile(r'(?<![\d.\-])\d{12,20}(?![\d.\-])'), '[번호]'),
]
#: '성함: 홍길동' 처럼 라벨이 붙은 개인정보
_PII_LABEL_RX = re.compile(r'(성함|이름|수령인|받는분|주문자)\s*[:：]\s*[가-힣]{2,4}')
#: '농협 [계좌번호] 김유휘 입니다' — 계좌 마스크 바로 뒤의 한글 2~4자는 예금주다.
_PII_NAME_AFTER_ACCOUNT_RX = re.compile(r'(\[계좌번호\])\s*(?<![가-힣])[가-힣]{2,4}(?=\s|$|[,.)])')

#: '탁경모 / 010-… / 컴퓨존' 처럼 라벨 없이 이름·연락처를 나열한 A/S 접수 글.
#: 연락처가 이미 마스킹된 뒤라 '마스크 토큰 바로 앞의 한글 2~4자' 로 이름을 잡는다.
_PII_NAME_NEAR_RX = re.compile(
    r'(?<![가-힣])[가-힣]{2,4}(?=\s*[/,·|]\s*\[(?:휴대폰|전화번호|이메일)\])')


def mask_pii(text: str) -> str:
    """상담이력 원문(question/answer)에서 개인정보를 지운다. 양쪽 모두 적용한다."""
    out = text or ''
    for rx, repl in _PII_PATTERNS:
        out = rx.sub(repl, out)
    out = _PII_LABEL_RX.sub(lambda m: f'{m.group(1)}: [이름]', out)
    out = _PII_NAME_NEAR_RX.sub('[이름]', out)
    out = _PII_NAME_AFTER_ACCOUNT_RX.sub(r'\1 [이름]', out)
    return out


# ---------------------------------------------------------------- 재사용 불가 답변

#: '정확한 안내를 위해 아래 내용 확인 후 톡톡 부탁드립니다 / 성함 / 연락처 / …' 182건.
#: 정보 가치 0 인데 유사도는 높게 나온다 → 인덱스에서 제외.
TEMPLATE_RX = re.compile(r'정확한\s*안내를\s*위해\s*아래\s*내용\s*확인|'
                         r'성함\s*/\s*연락처\s*/\s*구매처')

#: 시점 의존 정보. 2023~2025 시점 사실이라 오늘 기준 거의 틀렸다 → 원문 인용 금지.
VOLATILE_RX = re.compile(
    r'재고|품절|단종|입고|출고|발송|배송|택배|송장|운송장|반품|교환|환불|'
    r'가격|단가|견적|할인|이벤트|사은품|무료배송|계좌|입금|'
    r'\d[\d,]*\s*원(?!산지)')

#: 답변 본문의 URL — lanmart 게시판 내부 링크는 현재 유효성 미검증, 외부몰 링크 2.5%.
#: 'talk.naver.com/W4QWIB' 처럼 스킴 없는 표기가 흔해 도메인 형태도 함께 잡는다.
URL_RX = re.compile(r'(?:https?://|www\.)\S+|'
                    r'\b[\w-]+(?:\.[\w-]+)*\.(?:com|co\.kr|kr|net|org|shop|me|io)'
                    r'(?:/\S*)?', re.IGNORECASE)

# ---------------------------------------------------------------- 인증 문구 마스킹

#: 제품분석의 인증 주장은 실측 95% 가 근거 없음(581건 중 KB 'O' 일치 29건).
#: 'CE'·'UL'·'KC'·'KS'·'SDS' 는 단어경계 없이 매칭하면 PIECE/MULTI/BLACK 오탐이 폭발한다.
CERT_TOKEN_RX = re.compile(
    r'인증|(?<![A-Za-z])RoHS(?![A-Za-z])|로하스|'
    r'(?<![A-Za-z])CE(?![A-Za-z])|(?<![A-Za-z])UL(?![A-Za-z0-9])|'
    r'(?<![A-Za-z])KC(?![A-Za-z])|(?<![A-Za-z])KS(?![A-Za-z])|'
    r'(?<![A-Za-z])ISO(?![A-Za-z])|(?<![A-Za-z])FCC(?![A-Za-z])|'
    r'(?<![A-Za-z])ETL(?![A-Za-z])|(?<![A-Za-z])CSA(?![A-Za-z])|'
    r'(?<![A-Za-z])PSE(?![A-Za-z])|(?<![A-Za-z])CCC(?![A-Za-z])|'
    r'(?<![A-Za-z])EMC(?![A-Za-z])|(?<![A-Za-z])TTA(?![A-Za-z])|'
    r'(?<![A-Za-z])REACH(?![A-Za-z])|(?<![A-Za-z])PFAS(?![A-Za-z])|'
    r'(?<![A-Za-z])MSDS(?![A-Za-z])|(?<![A-Za-z])SDS(?![A-Za-z])|'
    r'전안|안전인증|적합등록|전자파|성적서|규격준수|적합성|원더셀|'
    r'(?<![A-Za-z])CB(?![A-Za-z])|(?<![A-Za-z])VDE(?![A-Za-z])|'
    # UL94/UL2464 처럼 뒤에 숫자가 붙는 규격명. 위의 UL 패턴은 lookahead 때문에 못 잡는다.
    r'(?<![A-Za-z])UL\s?\d+|난연|halogen|할로겐\s?프리|무연납|납\s?프리|'
    r'(?<![A-Za-z])FDA(?![A-Za-z])|식약처|식품\s?등급|식품용\s?승인|'
    r'(?<![A-Za-z])WEEE(?![A-Za-z])|(?<![A-Za-z])PROP\s?65(?![A-Za-z])|'
    r'환경\s?규제|규제\s?적합|친환경\s?인증|승인을?\s?받|승인\s?제품',
    re.IGNORECASE)

#: 문장/열거 항목 분리자. 제품분석은 쉼표 열거가 많아 쉼표도 분리자로 본다.
#: 마침표는 **뒤에 공백이 올 때만** 분리자다 — 'CAT.6', 'HDMI 2.1', '3.5mm' 를 쪼개면
#: 사양이 통째로 망가진다.
_SEG_SPLIT_RX = re.compile(r'([,;·\n]|\.\s+|\s및\s|\s또는\s)')


def split_segments(text: str) -> List[Tuple[str, str]]:
    """(조각, 뒤따르는 분리자) 목록. 원래 분리자를 보존해 그대로 되붙일 수 있다."""
    parts = _SEG_SPLIT_RX.split(text or '')
    out: List[Tuple[str, str]] = []
    for i in range(0, len(parts), 2):
        seg = parts[i]
        sep = parts[i + 1] if i + 1 < len(parts) else ''
        if seg.strip() or sep:
            out.append((seg, sep))
    return out


def join_segments(pairs: List[Tuple[str, str]]) -> str:
    out = ''.join(seg + sep for seg, sep in pairs)
    return re.sub(r'\s+', ' ', out).strip(' ,;·\n')


def strip_cert_claims(text: str) -> Tuple[str, List[str]]:
    """인증 관련 토큰이 들어간 문장/열거 항목을 통째로 제거한다.

    필드 단위가 아니라 '문장 단위 삭제'인 이유: 조건부 허용(KB 실시간 대조)은
    제품분석 보유 모델의 73.5% 에서 작동 자체가 불가능하다 → 검증 불가 = 삭제.
    반환: (정제된 텍스트, 삭제된 조각들)
    """
    raw = text or ''
    if not CERT_TOKEN_RX.search(raw):
        return raw, []
    kept: List[Tuple[str, str]] = []
    dropped: List[str] = []
    for seg, sep in split_segments(raw):
        if CERT_TOKEN_RX.search(seg):
            dropped.append(seg.strip())
            continue
        if seg.strip():
            kept.append((seg, sep))
    return join_segments(kept), [d for d in dropped if d]


# ---------------------------------------------------------------- 근거 라벨

SOURCE_HISTORY = '상담이력'
SOURCE_SPEC = '제품분석(이미지 기반 AI)'
SOURCE_CERT = '인증KB(cert_lookup)'
SOURCE_LLM = 'LLM생성'
SOURCE_RULE = '규칙기반'
