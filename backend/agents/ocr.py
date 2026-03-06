"""
OCR 에이전트
이미지/PDF 발주서에서 Claude Vision API로 주문 라인 직접 추출
(OCR + 구조화 추출을 한 번의 API 호출로 처리)
"""
import base64
import json
import logging
from pathlib import Path
from typing import List, Tuple
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from anthropic import AsyncAnthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)
client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────
#  OCR + 추출 통합 프롬프트
# ─────────────────────────────────────────
OCR_SYSTEM = """당신은 B2B 발주서 이미지/문서에서 주문 정보를 추출하는 전문 AI입니다.

## 역할
카카오톡 캡처, 사진, 스캔 등 다양한 형태의 발주서에서 각 주문 항목을 추출합니다.

## 출력 형식 (반드시 JSON 배열만 반환)
[
  {
    "line_no": 1,
    "raw_text": "이미지에서 읽은 원문 그대로",
    "product_hint": "상품명/규격 추정 텍스트",
    "qty": 숫자 또는 null,
    "unit": "단위 또는 null",
    "implicit_notes": "암묵적 속성 추론 (없으면 빈 문자열)"
  }
]

## 규칙
1. 상품 항목이 아닌 내용(날짜, 인사말, 배송지, 연락처 등)은 제외
2. 수량 단위를 명시적으로 파악 (박스=BOX, 개=EA, 롤=ROL 등)
3. 이미지가 흐리거나 글씨가 불명확해도 최대한 추출
4. 모델명, 규격, 색상 등 특징적인 표현은 product_hint에 포함
5. JSON 외 다른 텍스트는 절대 출력하지 말 것
6. qty는 반드시 숫자(float)여야 하며, 파악 불가 시 null"""


def _get_media_type(suffix: str) -> str:
    return {
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png":  "image/png",
        ".gif":  "image/gif",
        ".webp": "image/webp",
        ".pdf":  "application/pdf",
    }.get(suffix.lower(), "image/jpeg")


async def ocr_and_extract(image_path: str, cust_name: str = "") -> Tuple[List[dict], str]:
    """
    이미지/PDF 발주서에서 주문 라인 추출 (OCR + 추출 통합)

    Returns:
        (lines, raw_text)
        - lines: [{"line_no", "raw_text", "product_hint", "qty", "unit", "implicit_notes"}]
        - raw_text: 이미지에서 인식된 텍스트 요약 (화면 표시용)
    """
    path = Path(image_path)
    if not path.exists():
        raise FileNotFoundError(f"파일 없음: {image_path}")

    suffix = path.suffix.lower()
    media_type = _get_media_type(suffix)
    is_pdf = (media_type == "application/pdf")

    # 파일 → base64
    with open(path, "rb") as f:
        b64_data = base64.standard_b64encode(f.read()).decode("utf-8")

    hint_text = f"거래처: {cust_name}\n\n이 발주서에서 주문 항목을 추출해주세요." if cust_name else \
                "이 발주서에서 주문 항목을 추출해주세요."

    if is_pdf:
        # PDF는 document 타입으로 전송
        user_content = [
            {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": b64_data,
                },
            },
            {"type": "text", "text": hint_text},
        ]
    else:
        # 이미지 (JPG/PNG/WebP/GIF)
        user_content = [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": b64_data,
                },
            },
            {"type": "text", "text": hint_text},
        ]

    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            system=OCR_SYSTEM,
            messages=[{"role": "user", "content": user_content}],
        )

        text = response.content[0].text.strip()

        # ```json ... ``` 블록 제거
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        lines = json.loads(text)
        # raw_text 합산 (이력·표시용)
        raw_combined = "\n".join(l.get("raw_text", "") for l in lines)

        logger.info(f"[OCR] {len(lines)}개 라인 추출 완료 (파일: {path.name})")
        return lines, raw_combined

    except json.JSONDecodeError as e:
        logger.error(f"[OCR] JSON 파싱 실패: {e}")
        return [], ""
    except Exception as e:
        logger.error(f"[OCR] 오류: {e}", exc_info=True)
        raise
