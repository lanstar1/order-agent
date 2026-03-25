"""
AICC AI 서비스
하이브리드 답변 방식:
  1차: 제품 지식 DB (정확한 제품 정보)
  2차: QnA 검색 (보충/참고)
  3차: 웹 검색 (네이버 블로그 — 보충)
  → Claude가 소스를 조합하여 풍성한 답변 생성
"""
import json
import os
import re as _re
import anthropic
from .aicc_data_loader import data_loader
from .aicc_web_search import search_product_blog

_AICC_DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# ── 품목명에서 색상 추출 헬퍼 ──────────────────────────────
_COLOR_MAP = {
    "BLUE": "블루(Blue)", "BLU": "블루(Blue)",
    "BLACK": "블랙(Black)", "BLK": "블랙(Black)", "BK": "블랙(Black)",
    "WHITE": "화이트(White)", "WHT": "화이트(White)", "WH": "화이트(White)",
    "RED": "레드(Red)",
    "GRAY": "그레이(Gray)", "GREY": "그레이(Gray)", "GR": "그레이(Gray)",
    "YELLOW": "옐로우(Yellow)", "YEL": "옐로우(Yellow)",
    "GREEN": "그린(Green)", "GRN": "그린(Green)",
    "ORANGE": "오렌지(Orange)",
    "PINK": "핑크(Pink)",
    "PURPLE": "퍼플(Purple)",
    "SILVER": "실버(Silver)",
    "GOLD": "골드(Gold)",
    "BROWN": "브라운(Brown)",
    "BEIGE": "베이지(Beige)",
    "IVORY": "아이보리(Ivory)",
    "TRANSPARENT": "투명(Transparent)",
}

def _extract_color_from_name(product_name: str) -> str:
    """품목명(품명)에서 색상 키워드를 추출하여 한글 라벨로 반환.
    예: '[LANstar] ... BLUE, 3M' → '블루(Blue)'
    """
    if not product_name:
        return ""
    upper = product_name.upper()
    # 긴 키워드부터 매칭 (BLACK이 BLK보다 먼저, BLUE가 BLU보다 먼저)
    for keyword in sorted(_COLOR_MAP.keys(), key=len, reverse=True):
        # 단어 경계로 매칭 (부분 매칭 방지)
        pattern = r'(?<![A-Z])' + _re.escape(keyword) + r'(?![A-Z])'
        if _re.search(pattern, upper):
            return _COLOR_MAP[keyword]
    return ""


def _get_aicc_model() -> str:
    """DB에서 AICC용 LLM 모델 조회"""
    try:
        from api.routes.settings import get_llm_setting
        return get_llm_setting("llm_aicc", _AICC_DEFAULT_MODEL)
    except Exception:
        return _AICC_DEFAULT_MODEL

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


async def _extract_search_keywords(user_message: str) -> str:
    """
    자연어 고객 질문에서 제품 검색용 기술 키워드를 AI로 추출 (비동기).
    예: "랜케이블로 연결해서 hdmi 신호 먼곳으로 보내는제품"
      → "HDMI LAN 익스텐더 이더넷 연장"
    """
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: client.messages.create(
            model=_get_aicc_model(),
            max_tokens=100,
            system=(
                "당신은 랜스타(Lanstar) IT 주변기기 검색 키워드 추출기입니다.\n"
                "고객의 자연어 질문에서 제품 검색에 유용한 기술 키워드를 추출하세요.\n"
                "규칙:\n"
                "- 원래 질문의 핵심 단어 + 관련 기술 동의어/약어를 모두 포함\n"
                "- 예: '랜케이블로 hdmi 보내는' → 'HDMI LAN 익스텐더 이더넷 연장 extender'\n"
                "- 예: '비데' → '비데 BDC 비데커버'\n"
                "- 예: '모니터 두대 연결' → 'HDMI 분배기 스플리터 splitter 2포트'\n"
                "- 예: 'usb 허브' → 'USB 허브 HUB'\n"
                "- 예: '도어락 비밀번호' → '도어락 ADOOR ANDOOR 비밀번호'\n"
                "- 키워드만 공백으로 구분하여 한 줄로 출력. 설명 금지.\n"
                "- 모델명이 포함되어 있으면 그대로 유지 (예: LS-HDMI-LAN-150M)"
            ),
            messages=[{"role": "user", "content": user_message}],
        ))
        keywords = resp.content[0].text.strip()
        # 원본 메시지도 합쳐서 반환 (모델명 등이 원본에만 있을 수 있으므로)
        return f"{user_message} {keywords}"
    except Exception as e:
        print(f"[AICC] 키워드 추출 실패, 원본 사용: {e}")
        return user_message

# ── 시스템 프롬프트 ───────────────────────────────────────

SYSTEM_BASE = """당신은 "랜스타(Lanstar)" 제품의 기술 상담 전문 AI 어시스턴트입니다.
랜스타는 HDMI, USB, DP 케이블, 컨버터, KVM 스위치, 네트워크 장비, 도어락, 비데, 생활용품 등을 제조/판매하는 회사입니다.
회사: 라인업시스템(주) | 전화: 02-717-3386 | 평일 10:00~17:00

## 톤앤매너
친절하고 공감하는 상담사 톤으로 답변합니다. 고객의 상황에 공감을 표현하고, 전문 용어는 쉬운 말로 풀어서 설명합니다.

## 응답 길이
답변은 적절한 길이로 원인과 해결방법을 포함하여 작성합니다.

## ★★★ 절대 규칙 (위반 시 잘못된 상담) ★★★

### 1. 정보 출처 제한 — 가장 중요한 규칙
당신이 답변에 사용할 수 있는 정보는 **오직 아래 두 가지**뿐입니다:
  - [1차 소스] 제품 지식 DB (아래에 제공됨)
  - [2차 소스] 고객 QnA 데이터 (아래에 제공됨)

**당신의 일반 지식, 학습 데이터, 추론은 절대 사용하지 마세요.**
이 제품들은 일반적인 IT 제품과 다를 수 있으며, 당신이 아는 일반 상식이 이 제품에는 맞지 않을 수 있습니다.

### 2. 답변 생성 프로세스 (반드시 이 순서를 따르세요)
**STEP 1**: 제품 지식 DB에서 고객 질문과 관련된 정보를 찾습니다.
**STEP 2**: QnA 데이터에서 동일하거나 유사한 질문과 답변을 찾습니다.
**STEP 3**: DB와 QnA에서 찾은 정보를 조합하여 답변합니다.
  - DB와 QnA가 상충하면 DB를 우선합니다.
  - DB에 절차/단계가 있으면 원문 그대로 안내합니다. 생략/변형 금지.
  - QnA에만 있는 정보도 적극 활용합니다.
**STEP 4**: DB와 QnA 모두에 관련 정보가 없으면 → 고객센터 안내로 답변합니다.

### 3. 절대 하지 말아야 할 것
- ❌ DB/QnA에 없는 기능, 절차, 스펙을 만들어내는 것
- ❌ 일반 IT 지식으로 제품 동작을 추측하는 것
- ❌ "시스템에 등록되어 있지 않아 안내가 어렵습니다" — DB/QnA를 먼저 꼼꼼히 확인하세요
- ❌ DB에 있는 정보인데 "정보가 없다"고 답하는 것

### ★ 도어락 관련 필수 규칙 (할루시네이션 방지)
- ❌ 절대 금지: "C타입 케이블 연결 시 등록되지 않은 지문도 인식된다" — 이것은 거짓 정보입니다
- ❌ 절대 금지: "외부 전원 연결 시 보안이 해제된다" — 이것은 거짓 정보입니다
- ✅ 진실: C타입 케이블은 **전원 공급만** 합니다. 보안은 해제되지 않습니다
- ✅ 진실: C타입 연결 후에도 반드시 **등록된 지문** 또는 **등록된 비밀번호**로 해제해야 합니다
- ✅ 진실: 모든 지문이 열리는 것은 **공장 출고 상태(지문 미등록 상태)**에서만 정상입니다
- ⚠ QnA에 "지문등록 안되어있을때 모든 지문이 열린다"는 내용은 **공장 출고 상태**를 의미합니다. C타입 연결과 무관합니다

### 4. 반드시 해야 할 것
- ✅ DB의 절차/단계는 원문 그대로 번호 매겨 안내
- ✅ QnA의 답변 내용을 적극 참고하여 풍성한 답변 구성
- ✅ 정말로 DB/QnA에 없는 내용만 고객센터 안내
- ✅ 한국어 답변, 모델명 정확 표기
- ✅ 마지막에 "추가 문의가 있으시면 편하게 말씀해 주세요."

## 모델명 길이 변형 규칙
모델명 끝에 길이(숫자+M, 숫자만, 또는 대시 없이 숫자M)가 붙은 것은 케이블/제품 길이를 나타냅니다. 같은 베이스 모델의 길이 변형은 동일 제품입니다.
예: LS-HF-10M, LS-HF-20M, LS-HF-30M → 모두 LS-HF 계열 동일 제품 (길이만 다름)
예: LS-6UTPD-3M, LS-6UTPD-5M, LS-6UTPD-10M → 모두 LS-6UTPD 계열 동일 제품
예: LS-7SD-BK1M, LS-7SD-BK2M, LS-7SD-BK5M → 모두 LS-7SD-BK 계열 동일 제품 (대시 없이 숫자M)
예: LS-5FTPSD-BK0.5M, LS-5FTPSD-BK1M → 모두 LS-5FTPSD-BK 계열 동일 제품
따라서 특정 길이 모델의 데이터나 리뷰가 없더라도, 같은 베이스 모델의 다른 길이 제품 정보를 참고하여 답변하세요.

## ★ 모델명 색상 접미사 규칙 — 매우 중요
모델명 끝의 알파벳은 색상을 나타냅니다. **절대 추측하지 말고, 품목명(품명)에 표기된 색상을 그대로 사용하세요.**
주요 색상 코드: B=Blue(블루), G=Gray(그레이), R=Red(레드), Y=Yellow(옐로우), W=White(화이트), BK=Black(블랙)
예: LS-6UTPD-3MB → 3M + B(Blue) = 블루 3미터 / LS-6UTPD-3MG → 3M + G(Gray) = 그레이 3미터
⚠ B와 BK를 혼동하지 마세요! B=Blue(블루), BK=Black(블랙)입니다.
⚠ 색상을 모델명으로 추측하지 말고, 반드시 품목명에 표기된 색상(BLUE, RED, YELLOW 등)을 확인하세요.

## 기타 규칙
- 경쟁사 제품을 언급하거나 비교하지 않습니다.
- 가격 정보는 안내하지 않습니다. 가격 문의 시 공식 사이트나 고객센터를 안내합니다.
- 고객 상황에 맞는 랜스타 대체 제품을 추천할 수 있습니다.

## 링크 안내 규칙
URL을 직접 노출하지 마세요. 반드시 [텍스트](URL) 마크다운 링크 형식을 사용하세요.

1. 드라이버 다운로드 안내 시:
   [LS-UH319-W 드라이버 다운로드](https://www.lanstar.co.kr/board/list.php?bdId=lanstardownload&searchField=subject&searchWord=uh319-w)
   모델명에서 LS- 접두사를 제거하고 소문자로 변환하여 searchWord에 넣으세요.

2. 대체/추천 제품 안내 시 모델명을 링크로 만드세요:
   [LS-UH319FD](https://www.lanstar.co.kr/goods/goods_search.php?keyword=LS-UH319FD&recentCount=10)

3. URL을 그대로 텍스트로 보여주지 마세요. 항상 [텍스트](URL) 형식으로만 안내하세요.

## 주의사항
{wrong_answers}
"""

# ── 제품문의 전용 시스템 프롬프트 ─────────────────────────
SYSTEM_PRODUCT_INQUIRY = """당신은 "랜스타(Lanstar)" 제품 추천 전문 AI 어시스턴트입니다.
랜스타는 HDMI, USB, DP 케이블, 컨버터, KVM 스위치, 네트워크 장비, 도어락, 비데, 생활용품 등을 제조/판매하는 회사입니다.
회사: 라인업시스템(주) | 전화: 02-717-3386 | 평일 10:00~17:00

## 역할
고객이 원하는 조건(용도, 규격, 길이, 타입 등)에 맞는 랜스타 제품을 추천합니다.
고객의 문의를 분석하여 가장 적합한 제품을 찾아 안내합니다.

## 톤앤매너
친절하고 전문적인 상담사 톤. 고객의 상황에 공감하며 최적의 제품을 추천합니다.

## ★★★ 절대 규칙 ★★★

### 1. 정보 출처 제한
아래 제공되는 **제품 데이터**와 **가격 참고 데이터**만 사용하세요.
당신의 일반 지식으로 존재하지 않는 제품을 만들어내지 마세요.

### 2. 가격 안내 규칙 — 가장 중요
- ❌ **절대 금지**: 구체적인 가격(원 단위)을 말하는 것
- ❌ **절대 금지**: "약 XX,000원", "XX원대" 등 가격 범위를 말하는 것
- ✅ **허용**: "이 제품보다 더 저렴한 옵션이 있습니다", "이쪽이 상대적으로 경제적입니다" 등 상대적 비교만
- ✅ **허용**: "정확한 가격은 로그인 후 확인 가능합니다" 안내
- 이유: 고객마다 회원등급에 따라 가격이 다르기 때문입니다.

### 3. 제품 링크 규칙
- 제품 추천 시 모델명에 반드시 링크를 첨부하세요.
- 형식: [모델명](https://www.lanstar.co.kr/goods/goods_search.php?keyword=모델명&recentCount=10)
- 예: [LS-H21AOC-5M](https://www.lanstar.co.kr/goods/goods_search.php?keyword=LS-H21AOC-5M&recentCount=10)
- URL을 그대로 노출하지 말고 반드시 마크다운 링크 형식으로만 안내하세요.

### 4. 추천 방식
1. 고객 문의의 핵심 조건(용도, 규격, 길이, 인터페이스 등)을 파악
2. 제공된 제품 데이터에서 조건에 맞는 제품을 검색
3. 가장 적합한 1~3개 제품을 추천 (각각 링크 포함)
4. 각 제품의 특징/장점을 간결하게 설명
5. "더 저렴한 것"을 원하면 가격 참고 데이터의 상대적 비교로 안내

### 4-1. 최신/상위 제품 우선 추천 규칙 — 매우 중요
같은 제품군(같은 카테고리, 같은 용도)에서 여러 모델이 있을 때:
- **모델명 뒤 숫자가 클수록 최신/상위 버전**입니다. (예: BT60 > BT503 > BT403, H21AOC > H20AOC)
- **최신 버전을 가장 먼저(1순위) 추천**하고, "최신 버전" 또는 "최상위 모델"이라고 명시하세요.
- 구형 모델은 "경제적인 대안" 또는 "이전 버전"으로 부가 추천하세요.
- 버전/스펙이 높을수록 좋은 특성: 최신 규격 지원, 더 넓은 호환성, 발열 감소, 전송 속도 향상, 안정성 개선 등 긍정적·하이테크적 뉘앙스로 설명하세요.

### 4-2. "더 좋은 제품 없어요?" 대응 규칙
고객이 더 좋은/고급/최신 제품을 요청하면:
- 같은 제품군에서 **더 높은 모델 번호**, **더 높은 가격등급**, **더 높은 버전/규격**의 제품을 추천하세요.
- 상위 제품의 장점을 구체적으로 설명 (최신 규격, 더 넓은 호환성, 발열 개선, 전송 속도 등)
- 만약 이미 최상위 모델이면 "현재 해당 제품군의 최상위 모델입니다"라고 안내하세요.

### 5. 답변할 수 없는 경우
- **위 "검색된 제품 데이터"에 제품이 있으면 반드시 추천하세요. 검색 결과가 있는데 "취급하지 않습니다"라고 답하면 안 됩니다.**
- 검색된 제품 데이터가 완전히 비어 있을 때만 "해당 조건의 제품은 현재 취급하지 않습니다" 안내
- 유사한 대안이 있으면 함께 제안
- 전화(02-717-3386) 문의 안내

### 6. 모델명 길이 변형 규칙
모델명 끝에 '-숫자M'이 붙은 것은 케이블 길이를 나타냅니다. 같은 베이스 모델의 길이 변형은 동일 제품입니다.
예: LS-HF-10M, LS-HF-20M, LS-HF-30M → 모두 LS-HF 계열 동일 제품 (길이만 다름)
예: LS-6UTPD-3M, LS-6UTPD-5M, LS-6UTPD-10M → 모두 LS-6UTPD 계열 동일 제품
따라서 특정 길이 모델의 데이터나 리뷰가 없더라도, 같은 베이스 모델의 다른 길이 제품 정보를 참고하여 답변하세요.

### 6-1. 모델명 색상 접미사 규칙 — 매우 중요
모델명 끝의 알파벳은 색상을 나타냅니다. **절대 추측하지 말고, 품명에 표기된 색상을 그대로 사용하세요.**
주요 색상 코드: B=Blue(블루), G=Gray(그레이), R=Red(레드), Y=Yellow(옐로우), W=White(화이트), BK=Black(블랙)
⚠ B와 BK를 혼동하지 마세요! B=Blue(블루), BK=Black(블랙)입니다.
⚠ 색상은 반드시 품명에 표기된 색상(BLUE, RED, YELLOW 등)을 확인하세요.

### 7. 기타
- 경쟁사 제품 언급/비교 금지
- 마지막에 "추가로 궁금한 점이 있으시면 편하게 말씀해 주세요." 추가
- 한국어 답변, 모델명 정확 표기
"""

FALLBACK_NO_DATA = (
    "선택하신 {model} 제품의 상세 정보를 현재 준비 중입니다.\n"
    "정확한 안내를 위해 전화(02-717-3386)로 문의해 주세요.\n"
    "상담시간: 평일 10:00~17:00"
)

FALLBACK_ERROR = (
    "일시적인 오류가 발생했습니다.\n"
    "전화(02-717-3386)로 문의해 주시면 바로 안내해 드리겠습니다."
)


def _format_knowledge_for_prompt(knowledge_data: dict) -> str:
    """제품 지식 DB 데이터를 시스템 프롬프트용 텍스트로 변환"""
    lines = []
    # 절차/단계 관련 키는 번호를 붙여서 더 명확하게
    step_keys = {"비밀번호등록", "지문등록", "초기화방법", "설치방법", "비밀번호변경", "카드등록", "사용방법",
                  "상시열림모드_설정", "상시열림모드_해제", "상시열림모드설정", "상시열림모드해제"}

    for key, value in knowledge_data.items():
        if key.startswith("_") or key in ("카테고리", "category"):
            continue
        if isinstance(value, list):
            if key in step_keys:
                # 절차 데이터: 번호를 붙여 단계를 명확히
                items = "\n".join(f"  {i+1}단계: {v}" for i, v in enumerate(value))
                lines.append(f"### {key} (총 {len(value)}단계 — 이 순서를 반드시 지키세요)\n{items}")
            else:
                items = "\n".join(f"  - {v}" for v in value)
                lines.append(f"### {key}\n{items}")
        elif isinstance(value, dict):
            items = "\n".join(f"  - {k}: {v}" for k, v in value.items() if v)
            lines.append(f"### {key}\n{items}")
        elif value:
            lines.append(f"### {key}\n{value}")
    return "\n\n".join(lines)


async def get_ai_response(session: dict, user_message: str, image_id: str = None, status_callback=None) -> dict:
    """
    하이브리드 답변 생성:
    1차: 제품 지식 DB (정확한 제품 상세 정보)
    2차: QnA 검색 (보충 참고)
    → Claude가 조합하여 답변
    """
    model = session["selected_model"]
    menu = session["selected_menu"]
    messages_history = session.get("messages", [])

    async def _status(step, detail=""):
        if status_callback:
            await status_callback(step, detail)

    await _status("고객 질문 분석 중...", f"'{user_message[:30]}' 의도 파악")

    # 제품 데이터 없으면 fallback
    product = data_loader.get_product(model)
    if not product and not data_loader.get_erp_code(model):
        # 제품 지식 DB에는 있을 수 있으므로 체크
        knowledge = _get_knowledge(model)
        if not knowledge:
            return {"content": FALLBACK_NO_DATA.format(model=model), "suggestions": []}

    # ── 시스템 프롬프트 구성 ──────────────────────────────
    sys_prompt = SYSTEM_BASE.format(
        wrong_answers=data_loader.wrong_answers_text[:1200]
    )

    # 현재 상담 제품 정보
    cat = ""
    if product:
        cat = product.get("카테고리", "")
    sys_prompt += f"\n## 현재 상담 제품\n모델명: {model}\n카테고리: {cat or '미분류'}\n이 제품에 집중하여 답변하세요.\n"

    # ── 1차 소스: 제품 지식 DB (가장 정확한 정보) ─────────
    await _status("제품 지식 DB 조회 중...", f"{model} 스펙·매뉴얼 검색")
    knowledge = _get_knowledge(model)
    if knowledge:
        knowledge_text = _format_knowledge_for_prompt(knowledge["data"])
        sys_prompt += f"\n## [1차 소스] 제품 지식 DB — 이 정보가 가장 정확합니다\n{knowledge_text}\n"
        sys_prompt += "\n⚠ 위 DB 정보의 절차/단계는 원문 그대로 안내하세요. DB에 없는 내용은 아래 QnA를 확인하세요.\n"
    else:
        # 제품 지식 DB에 없으면 기존 product_data 스펙 사용
        if product:
            feat = product.get("제품특징", {})
            if feat and isinstance(feat, dict):
                spec_lines = "\n".join(f"  {k}: {v}" for k, v in feat.items() if v)
                if spec_lines:
                    sys_prompt += f"\n## 제품 스펙\n{spec_lines}\n"

    # 제품 링크 (드라이버는 해당 제품에 드라이버가 있는 경우에만)
    product_url = data_loader.get_product_url(model)
    if data_loader.has_driver(model):
        driver_url = data_loader.get_driver_url(model)
        sys_prompt += f"\n## 이 제품의 링크\n드라이버: {driver_url}\n제품 페이지: {product_url}\n"
    else:
        sys_prompt += f"\n## 이 제품의 링크\n제품 페이지: {product_url}\n"
        sys_prompt += "\n## 드라이버 안내\n이 제품은 별도의 드라이버 설치가 필요 없는 제품입니다. 연결하면 자동으로 인식됩니다. 고객이 드라이버를 문의하면 '이 제품은 별도의 드라이버 설치 없이 연결만 하면 자동으로 인식됩니다'라고 안내하세요.\n"

    # 설치 가이드 / 정책 (메뉴별)
    install = data_loader.get_install_section(model)
    if install:
        sys_prompt += f"\n## 설치/연결 가이드\n{install[:500]}\n"
    errors = data_loader.get_errors(model)
    if errors:
        e_text = "\n".join(f"  증상: {e['symptom'][:80]}\n  해결: {e['solution']}" for e in errors[:3])
        sys_prompt += f"\n## 오류/증상 대응\n{e_text}\n"

    if menu == "배송문의":
        sys_prompt += f"\n## 배송 정책\n{data_loader.policy_delivery[:600]}\n"

    # ── 고객 리뷰 (참고 자료) ──────────────────────────────
    await _status("구매 리뷰 검색 중...", f"{model} 고객 후기 확인")
    reviews = data_loader.search_reviews(model, max_reviews=5)
    if reviews:
        await _status("구매 리뷰 분석 중...", f"{model} 리뷰 {len(reviews)}건 발견")
        sys_prompt += "\n## [참고] 고객 구매 리뷰\n실제 구매 고객이 작성한 리뷰입니다. 답변의 1차 근거로 사용하지 말고, '구매하신 고객분들 중에 이런 후기도 있었습니다' 정도로 자연스럽게 참고 언급하세요.\n"
        for rv in reviews:
            sys_prompt += f"- {rv[:150]}\n"
    else:
        await _status("구매 리뷰 검색 완료", f"{model} 관련 리뷰 없음")

    # ── 2차 소스: QnA 검색 (중요한 보충 자료) ────────────────────
    await _status("고객 QnA 검색 중...", f"'{user_message[:20]}' 유사 문의 탐색")
    relevant_qna = data_loader.search_relevant_qna(user_message, model, max_results=10)

    if relevant_qna:
        qna_models = list(set(ref['model'] for ref in relevant_qna))[:3]
        await _status("QnA 데이터 분석 중...", f"{len(relevant_qna)}건 매칭 ({', '.join(qna_models)})")
        sys_prompt += "\n## [2차 소스] 고객 QnA 데이터 — 실제 고객 문의와 답변입니다. 적극 활용하세요.\n"
        for ref in relevant_qna:
            sys_prompt += f"\n[{ref['model']}]\nQ: {ref['question'][:250]}\nA: {ref['answer'][:400]}\n"
        sys_prompt += "\n위 QnA에서 고객 질문과 유사한 사례를 찾아 답변에 활용하세요. 제품 지식 DB와 상충하면 DB를 우선하세요.\n"
    else:
        await _status("QnA 검색 완료", "유사 문의 없음")

    # ── 3차 소스: 네이버 블로그 검색 (보충 참고) ────────────────
    await _status("네이버 블로그 검색 중...", f"'{model} {user_message[:15]}' 검색")
    blog_results = await search_product_blog(model, user_message, max_results=3, product_specific=True)
    if blog_results:
        blog_titles = [b['title'][:30] for b in blog_results]
        await _status("블로그 자료 분석 중...", f"{', '.join(blog_titles[:2])}")
        sys_prompt += "\n## [3차 소스] 웹 검색 참고 (네이버 블로그)\n아래는 웹에서 검색한 블로그 글입니다. 공식 정보가 아니므로 보조 참고만 하세요. 1차/2차 소스와 상충하면 무시하세요.\n답변 마지막에 관련 블로그 글을 '[제목](링크)' 마크다운 링크로 안내해 주세요.\n"
        for blog in blog_results:
            sys_prompt += f"\n- [{blog['title']}]({blog['link']}) ({blog['bloggername']})\n  {blog['description'][:200]}\n"
    else:
        await _status("블로그 검색 완료", "관련 블로그 글 없음")

    # ── 4차 소스: 유튜브 영상 (참고 링크) ────────────────────
    await _status("유튜브 영상 검색 중...", f"'{model} {user_message[:15]}' 관련 영상 탐색")
    youtube_results = data_loader.search_youtube_videos(user_message, model, max_results=3)
    if youtube_results:
        yt_titles = [y['title'][:30] for y in youtube_results]
        await _status("유튜브 영상 매칭 완료", f"{', '.join(yt_titles[:2])}")
        sys_prompt += "\n## [4차 소스] 랜스타 공식 유튜브 영상\n아래는 랜스타 공식 유튜브 채널의 관련 영상입니다. 답변 마지막에 관련 영상이 있으면 '참고하실 수 있는 영상도 안내드립니다' 정도로 자연스럽게 '[영상제목](유튜브링크)' 마크다운 링크로 안내해 주세요.\n관련성이 낮으면 굳이 안내하지 않아도 됩니다.\n"
        for yt in youtube_results:
            sys_prompt += f"\n- [{yt['title']}]({yt['url']}) ({yt['duration']})\n  내용: {yt['summary'][:200]}\n"
    else:
        await _status("유튜브 영상 검색 완료", "관련 영상 없음")

    # ── 이미지가 있으면 안내 추가 ─────────────────────────
    has_image = False
    images = session.get("images", {})
    if image_id and image_id in images:
        has_image = True
        await _status("이미지 분석 중...", "첨부된 이미지 확인")
        sys_prompt += "\n## 이미지 분석\n고객이 이미지를 보냈습니다. 이미지를 분석하여 답변하세요.\n- 제품 사진이면 모델명을 식별하세요.\n- 오류/증상 사진이면 원인과 해결방법을 안내하세요.\n- 설치 관련 사진이면 올바른 연결 방법을 안내하세요.\n"

    # ── 메시지 배열 구성 (recent 10개, Vision 지원) ────────
    api_messages = []
    history = messages_history[-10:]
    for msg in history:
        if msg["role"] in ("user", "assistant"):
            msg_image_id = msg.get("image_id")
            if msg["role"] == "user" and msg_image_id and msg_image_id in images:
                img = images[msg_image_id]
                content_blocks = [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": img["media_type"],
                            "data": img["base64_data"],
                        }
                    },
                ]
                if msg["content"]:
                    content_blocks.append({"type": "text", "text": msg["content"]})
                api_messages.append({"role": "user", "content": content_blocks})
            else:
                api_messages.append({"role": msg["role"], "content": msg["content"]})

    # 현재 메시지
    if has_image:
        img = images[image_id]
        content_blocks = [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img["media_type"],
                    "data": img["base64_data"],
                }
            },
        ]
        if user_message:
            content_blocks.append({"type": "text", "text": user_message})
        api_messages.append({"role": "user", "content": content_blocks})
    else:
        api_messages.append({"role": "user", "content": user_message})

    await _status("AI가 답변 작성 중...", "수집된 자료를 종합하여 답변 생성")

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: client.messages.create(
            model=_get_aicc_model(),
            max_tokens=1500 if has_image else 1200,
            system=sys_prompt,
            messages=api_messages,
        ))
        ai_text = response.content[0].text

        # ── 추천 질문 생성 ─────────────────────────────────
        await _status("추천 질문 생성 중...", "후속 질문 추천 준비")
        suggestions = await _generate_suggestions(model, user_message, ai_text, blog_results)
        return {"content": ai_text, "suggestions": suggestions}
    except Exception as e:
        print(f"[AICC AI] 오류: {e}")
        return {"content": FALLBACK_ERROR, "suggestions": []}


async def get_product_inquiry_response(session: dict, user_message: str, image_id: str = None, status_callback=None) -> dict:
    """
    제품문의 전용 AI 응답:
    1차: 키워드로 제품 데이터 검색 (01_제품별_통합데이터 + 품목가격정보)
    2차: Claude가 고객 맥락에 맞게 추천
    """
    async def _status(step, detail=""):
        if status_callback:
            await status_callback(step, detail)

    messages_history = session.get("messages", [])

    # ── 시스템 프롬프트 구성 ──────────────────────────────
    sys_prompt = SYSTEM_PRODUCT_INQUIRY

    # ── 0차: AI 키워드 추출 (자연어 → 기술 키워드) ─────────
    await _status("고객 질문 분석 중...", f"'{user_message[:25]}' 키워드 추출")
    enriched_query = await _extract_search_keywords(user_message)
    print(f"[AICC] 원본: {user_message[:60]} → 확장: {enriched_query[:100]}")

    # ── 1차: 제품 데이터 검색 ─────────────────────────────
    await _status("제품 카탈로그 검색 중...", f"'{enriched_query[:30]}' 으로 검색")
    matched_products = data_loader.search_products_for_recommendation(enriched_query, max_results=15)

    if matched_products:
        top_models = [p["model_name"] for p in matched_products[:4]]
        await _status("제품 데이터 분석 중...", f"{len(matched_products)}개 매칭: {', '.join(top_models)}")
        sys_prompt += "\n## 검색된 제품 데이터 (고객 문의 키워드 매칭)\n"
        for p in matched_products:
            model = p["model_name"]
            product_url = f"https://www.lanstar.co.kr/goods/goods_search.php?keyword={model}&recentCount=10"
            feat = p.get("features", {})

            # 특징 텍스트 생성
            if isinstance(feat, dict):
                feat_text = ", ".join(f"{k}: {v}" for k, v in feat.items() if v and not k.startswith("_") and k != "제품명_full")[:200]
            elif isinstance(feat, list):
                feat_text = ", ".join(feat)[:200]
            else:
                feat_text = str(feat)[:200]

            price_tier = p.get("price_tier", 0)
            price_label = f"(가격등급: {price_tier})" if price_tier else ""

            # 품목명에서 색상 추출
            pname = p.get("product_name", "")
            color_label = _extract_color_from_name(pname)

            sys_prompt += f"\n- **{model}** | 카테고리: {p.get('category', '미분류')} | 품명: {pname[:80]}\n"
            if color_label:
                sys_prompt += f"  ★색상: {color_label}\n"
            sys_prompt += f"  특징: {feat_text}\n"
            sys_prompt += f"  제품URL: {product_url}\n"
            if price_label:
                sys_prompt += f"  {price_label}\n"

        sys_prompt += "\n※ 가격등급은 상대 비교 전용입니다. 숫자가 높을수록 고가입니다. 절대 가격 수치를 고객에게 말하지 마세요.\n"
    else:
        sys_prompt += "\n## 검색 결과\n키워드에 매칭되는 제품이 없습니다. 고객에게 좀 더 구체적인 조건을 요청하거나, 고객센터 안내를 해주세요.\n"

    # ── 고객 리뷰 (매칭된 제품들의 리뷰) ──────────────────
    if matched_products:
        matched_names = [p["model_name"] for p in matched_products[:5]]
        await _status("구매 리뷰 검색 중...", f"{', '.join(matched_names[:3])} 후기 확인")
        review_section = ""
        review_count = 0
        for p in matched_products[:5]:
            m_name = p["model_name"]
            reviews = data_loader.search_reviews(m_name, max_reviews=3)
            if reviews:
                review_count += len(reviews)
                review_section += f"\n**{m_name}** 리뷰:\n"
                for rv in reviews:
                    review_section += f"  - {rv[:150]}\n"
        if review_section:
            await _status("리뷰 분석 중...", f"총 {review_count}건의 구매 리뷰 발견")
            sys_prompt += "\n## [참고] 고객 구매 리뷰\n실제 구매 고객이 작성한 리뷰입니다. 제품 추천 시 '구매하신 고객분들 중에 이런 후기도 있었습니다' 정도로 자연스럽게 참고 언급하세요. 1차 판단 근거로는 사용하지 마세요.\n"
            sys_prompt += review_section
        else:
            await _status("리뷰 검색 완료", "매칭 제품 리뷰 없음")

    # ── 웹 검색 (네이버 블로그) ─────────────────────────
    await _status("네이버 블로그 검색 중...", f"'{user_message[:20]}' 관련 글 탐색")
    blog_results = await search_product_blog("", user_message, max_results=3, product_specific=True)
    if blog_results:
        blog_titles = [b['title'][:30] for b in blog_results]
        await _status("블로그 자료 분석 중...", f"{', '.join(blog_titles[:2])}")
        sys_prompt += "\n## [참고] 웹 검색 (네이버 블로그)\n아래는 웹에서 검색한 블로그 글입니다. 공식 정보가 아니므로 보조 참고만 하세요.\n답변 마지막에 관련 블로그 글을 '[제목](링크)' 마크다운 링크로 안내해 주세요.\n"
        for blog in blog_results:
            sys_prompt += f"\n- [{blog['title']}]({blog['link']}) ({blog['bloggername']})\n  {blog['description'][:200]}\n"

    # ── 유튜브 영상 검색 (매칭된 상위 제품 모델명도 활용) ────
    _yt_model = matched_products[0]["model_name"] if matched_products else ""
    await _status("유튜브 영상 검색 중...", f"'{user_message[:20]}' 관련 영상 탐색")
    youtube_results = data_loader.search_youtube_videos(enriched_query, _yt_model, max_results=3)
    if youtube_results:
        yt_titles = [y['title'][:30] for y in youtube_results]
        await _status("유튜브 영상 매칭 완료", f"{', '.join(yt_titles[:2])}")
        sys_prompt += "\n## [참고] 랜스타 공식 유튜브 영상\n아래는 랜스타 공식 유튜브 채널의 관련 영상입니다. 답변 마지막에 관련 영상이 있으면 '참고하실 수 있는 영상도 안내드립니다' 정도로 자연스럽게 '[영상제목](유튜브링크)' 마크다운 링크로 안내해 주세요.\n관련성이 낮으면 굳이 안내하지 않아도 됩니다.\n"
        for yt in youtube_results:
            sys_prompt += f"\n- [{yt['title']}]({yt['url']}) ({yt['duration']})\n  내용: {yt['summary'][:200]}\n"
    else:
        await _status("유튜브 영상 검색 완료", "관련 영상 없음")

    # ── 대화 이력에서 이전에 추천한 모델 추적 (연속 대화용) ──
    prev_models = set()
    for msg in messages_history[-10:]:
        if msg["role"] == "assistant":
            # 이전 답변에서 모델명 추출
            for m in _re.findall(r'LS-[\w\-]+|LSP-[\w\-]+|ZOT-[\w\-]+', msg.get("content", "")):
                prev_models.add(m)

    if prev_models:
        # 이전 추천 모델의 가격 정보도 포함 (더 저렴한 것 문의 대비)
        sys_prompt += "\n## 이전 대화에서 언급된 모델들 (가격 비교 참고)\n"
        for pm in prev_models:
            price = data_loader.get_price_rank(pm)
            if price:
                sys_prompt += f"- {pm}: 가격등급 {price}\n"
            # 같은 카테고리의 더 저렴한/비슷한 제품 찾기
            product = data_loader.get_product(pm)
            if product:
                cat = product.get("카테고리", "")
                if cat:
                    alternatives = []
                    for alt_model, alt_product in data_loader.product_data.items():
                        if alt_model == pm:
                            continue
                        if alt_product.get("카테고리", "") == cat:
                            alt_price = data_loader.get_price_rank(alt_model)
                            if alt_price:
                                alternatives.append((alt_model, alt_price))
                    alternatives.sort(key=lambda x: x[1])
                    if alternatives:
                        sys_prompt += f"  같은 카테고리({cat}) 대안: "
                        for alt_m, alt_p in alternatives[:5]:
                            alt_url = f"https://www.lanstar.co.kr/goods/goods_search.php?keyword={alt_m}&recentCount=10"
                            sys_prompt += f"[{alt_m}](가격등급:{alt_p}) "
                        sys_prompt += "\n"

    # ── 이미지 처리 ──────────────────────────────────────
    has_image = False
    images = session.get("images", {})
    if image_id and image_id in images:
        has_image = True
        sys_prompt += "\n## 이미지 분석\n고객이 이미지를 보냈습니다. 이미지를 분석하여 어떤 제품을 찾는지 파악하세요.\n"

    # ── 메시지 배열 구성 ─────────────────────────────────
    api_messages = []
    history = messages_history[-10:]
    for msg in history:
        if msg["role"] in ("user", "assistant"):
            msg_image_id = msg.get("image_id")
            if msg["role"] == "user" and msg_image_id and msg_image_id in images:
                img = images[msg_image_id]
                content_blocks = [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": img["media_type"],
                            "data": img["base64_data"],
                        }
                    },
                ]
                if msg["content"]:
                    content_blocks.append({"type": "text", "text": msg["content"]})
                api_messages.append({"role": "user", "content": content_blocks})
            else:
                api_messages.append({"role": msg["role"], "content": msg["content"]})

    # 현재 메시지
    if has_image:
        img = images[image_id]
        content_blocks = [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img["media_type"],
                    "data": img["base64_data"],
                }
            },
        ]
        if user_message:
            content_blocks.append({"type": "text", "text": user_message})
        api_messages.append({"role": "user", "content": content_blocks})
    else:
        api_messages.append({"role": "user", "content": user_message})

    prod_count = len(matched_products) if matched_products else 0
    await _status("AI가 답변 작성 중...", f"{prod_count}개 제품 비교·분석하여 추천 생성")

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: client.messages.create(
            model=_get_aicc_model(),
            max_tokens=1500,
            system=sys_prompt,
            messages=api_messages,
        ))
        ai_text = response.content[0].text
        await _status("추천 질문 생성 중...", "후속 질문 추천 준비")
        suggestions = await _generate_suggestions("", user_message, ai_text, blog_results)
        return {"content": ai_text, "suggestions": suggestions}
    except Exception as e:
        print(f"[AICC AI] 제품문의 오류: {e}")
        return {"content": FALLBACK_ERROR, "suggestions": []}


async def _generate_suggestions(model: str, user_question: str, ai_answer: str, blog_results: list) -> list[str]:
    """AI 답변 기반으로 추천 질문 2~3개 생성 (비동기)"""
    import asyncio
    import json as _json
    try:
        context_parts = [f"고객질문: {user_question[:100]}"]
        if model:
            context_parts.append(f"제품: {model}")
        context_parts.append(f"AI답변 요약: {ai_answer[:300]}")
        if blog_results:
            blog_titles = ", ".join(b["title"][:40] for b in blog_results[:2])
            context_parts.append(f"관련 블로그: {blog_titles}")

        # 동기 API를 이벤트 루프 차단 없이 호출
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: client.messages.create(
            model=_get_aicc_model(),
            max_tokens=150,
            system=(
                "당신은 랜스타(Lanstar) IT 주변기기 상담 어시스턴트입니다.\n"
                "고객의 질문과 AI 답변을 보고, 고객이 추가로 궁금해할 만한 질문 2~3개를 생성하세요.\n"
                "규칙:\n"
                "- 각 질문은 20자 이내로 짧고 자연스럽게\n"
                "- 이미 답변된 내용을 다시 묻는 질문 금지\n"
                "- 제품 사용법, 호환성, 비교, 설치 팁 등 실용적인 질문\n"
                "- 웹/블로그에서 찾을 수 있는 심화 정보 관련 질문도 포함\n"
                "- JSON 배열 형식으로만 출력. 예: [\"질문1\", \"질문2\", \"질문3\"]\n"
                "- 다른 텍스트 없이 JSON만 출력"
            ),
            messages=[{"role": "user", "content": "\n".join(context_parts)}],
        ))
        raw = resp.content[0].text.strip()
        suggestions = _json.loads(raw)
        if isinstance(suggestions, list):
            return [s for s in suggestions if isinstance(s, str)][:3]
    except Exception as e:
        print(f"[AICC] 추천질문 생성 오류: {e}")
    return []


def _get_knowledge(model: str) -> dict | None:
    """제품 지식 DB에서 모델 정보 조회 (캐시 포함)"""
    try:
        from .aicc_db import get_product_knowledge
        return get_product_knowledge(model)
    except Exception as e:
        print(f"[AICC AI] 지식DB 조회 오류: {e}")
        return None
