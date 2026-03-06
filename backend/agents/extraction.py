"""
주문 추출 에이전트
원문 발주서 텍스트에서 상품명 + 수량 + 단위를 구조화된 JSON으로 추출
학습 데이터가 있는 거래처는 few-shot 예시를 활용하여 정확도 향상
"""
import json
import logging
from typing import List
from anthropic import AsyncAnthropic
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)
client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

EXTRACT_SYSTEM = """당신은 B2B 발주서 텍스트에서 상품 주문 정보를 추출하는 전문 AI입니다.

## 역할
거래처가 보낸 원문 발주서(비표준 형식, 줄임말, 오탈자 포함 가능)에서 각 주문 항목을 추출합니다.
당사는 LANstar(랜스타) 브랜드 네트워크/IT 주변기기 전문 유통사입니다.

## 출력 형식 (반드시 JSON 배열만 반환)
[
  {
    "line_no": 1,
    "raw_text": "원문 그대로",
    "product_hint": "상품명/규격 추정 텍스트 (모델번호가 보이면 모델번호 우선)",
    "normalized_hints": ["정규화된 모델명 변형1", "변형2"],
    "qty": 숫자 또는 null,
    "unit": "단위 또는 null",
    "implicit_notes": "암묵적 속성 추론 (없으면 빈 문자열)",
    "detected_specs": {
      "manufacturer": "제조사명 또는 null (불분명하면 null)",
      "category": "케이블/허브/멀티탭 등 또는 null",
      "length": "2M 등 또는 null",
      "color": "블루/그레이 등 또는 null"
    }
  }
]

## 모델명 정규화 규칙 (normalized_hints)
- 원문에서 추정되는 모델명/품번의 변형을 최대 3개까지 생성
- 예시: "6utpd-2mg" → ["6UTPD-2MG", "LS-6UTPD-2MG", "LS-6UTPD-2MB"]
- LS- 접두어가 없으면 LS- 붙인 버전도 추가 (당사 브랜드 모델명은 대부분 LS-로 시작)
- 끝자리 b/g 차이는 색상코드(Blue/Gray)일 수 있으므로 두 가지 모두 생성

## 규칙
1. 상품 항목이 아닌 내용(날짜, 인사말, 배송지 등)은 제외
2. 수량 단위를 명시적으로 파악 (박스=BOX, 개=EA, 롤=ROL 등)
3. 색상/규격이 명시되지 않으면 implicit_notes에 "색상 미지정" 등 기재
4. JSON 외 다른 텍스트는 절대 출력하지 말 것
5. qty는 반드시 숫자(float)여야 하며, 파악 불가 시 null
6. 모델번호가 보이면 product_hint에 모델번호를 우선 기재 (예: "LS-6UTPD-2MG" 또는 "6utpd-2mg")
7. 제조사가 명시되지 않은 경우 detected_specs.manufacturer는 null로 설정"""


async def extract_order_lines(raw_text: str, cust_name: str = "", cust_code: str = "") -> List[dict]:
    """
    원문 발주서에서 주문 라인 추출
    cust_code가 있으면 해당 거래처의 학습 데이터를 few-shot으로 활용
    Returns: [{"line_no":1, "raw_text":"...", "product_hint":"...", "qty":10, "unit":"EA", "implicit_notes":""}]
    """
    # few-shot 학습 데이터 조회
    fewshot_text = ""
    if cust_code:
        try:
            from services.training_service import get_fewshot_examples
            fewshot_text = get_fewshot_examples(cust_code, max_examples=3)
            if fewshot_text:
                logger.info(f"[Extraction] 거래처 {cust_name}({cust_code})의 학습 데이터 활용")
        except Exception as e:
            logger.warning(f"[Extraction] few-shot 데이터 조회 실패: {e}")

    # 학습 데이터가 있으면 시스템 프롬프트에 추가
    system_prompt = EXTRACT_SYSTEM
    if fewshot_text:
        system_prompt += fewshot_text

    user_msg = f"""거래처: {cust_name}

발주서 원문:
{raw_text}

위 발주서에서 주문 항목을 추출해주세요."""

    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}]
        )

        text = response.content[0].text.strip()

        # JSON 블록 추출 (```json ... ``` 형식 대응)
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        lines = json.loads(text)
        logger.info(f"[Extraction] {len(lines)}개 라인 추출 완료" +
                     (f" (few-shot 활용)" if fewshot_text else ""))
        return lines

    except json.JSONDecodeError as e:
        logger.error(f"[Extraction] JSON 파싱 실패: {e}\n원문: {text[:200]}")
        return []
    except Exception as e:
        logger.error(f"[Extraction] 오류: {e}")
        return []
