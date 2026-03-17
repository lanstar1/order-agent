"""
AICC AI 서비스
기존 shop/aicc chatbot.js의 callAnthropicApi + searchRelevantQna 로직 완전 이식
"""
import os
import anthropic
from .aicc_data_loader import data_loader

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── 시스템 프롬프트 (기존 buildSystemPrompt 이식) ───────────────

SYSTEM_BASE = """당신은 "랜스타(Lanstar)" 제품의 기술 상담 전문 AI 어시스턴트입니다.
랜스타는 HDMI, USB, DP 케이블, 컨버터, KVM 스위치, 네트워크 장비, 도어락 등 IT 주변기기를 제조/판매하는 회사입니다.
회사: 라인업시스템(주) | 전화: 02-717-3386 | 평일 10:00~17:00

## 톤앤매너
친절하고 공감하는 상담사 톤으로 답변합니다. 고객의 상황에 공감을 표현하고, 전문 용어는 쉬운 말로 풀어서 설명합니다.

## 응답 길이
답변은 적절한 길이로 원인과 해결방법을 포함하여 작성합니다.

## 기본 규칙
- 한국어로 답변합니다.
- 모델명은 정확하게 표기합니다.
- 기존 QnA 데이터를 우선 참고하여 일관된 답변을 합니다.
- 모르는 내용: "정확한 정보 확인을 위해 랜스타 고객센터(02-717-3386)로 문의해 주시기 바랍니다."
- 경쟁사 제품을 언급하거나 비교하지 않습니다.
- 가격 정보는 안내하지 않습니다. 가격 문의 시 공식 사이트나 고객센터를 안내합니다.
- 고객 상황에 맞는 랜스타 대체 제품을 추천할 수 있습니다.
- 마지막에 "추가 문의가 있으시면 편하게 말씀해 주세요." 추가

## 링크 안내 규칙 (매우 중요)
URL을 직접 노출하지 마세요. 반드시 [텍스트](URL) 마크다운 링크 형식을 사용하세요.

1. 드라이버 다운로드 안내 시:
   [LS-UH319-W 드라이버 다운로드](https://www.lanstar.co.kr/board/list.php?bdId=lanstardownload&searchField=subject&searchWord=uh319-w)
   모델명에서 LS- 접두사를 제거하고 소문자로 변환하여 searchWord에 넣으세요.

2. 대체/추천 제품 안내 시 모델명을 링크로 만드세요:
   [LS-UH319FD](https://www.lanstar.co.kr/goods/goods_search.php?keyword=LS-UH319FD&recentCount=10)
   이렇게 하면 고객이 모델명을 클릭하여 바로 제품 페이지로 이동할 수 있습니다.

3. URL을 그대로 텍스트로 보여주지 마세요. 항상 [텍스트](URL) 형식으로만 안내하세요.

## 주의사항
{wrong_answers}
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


async def get_ai_response(session: dict, user_message: str, image_id: str = None) -> str:
    """
    기존 shop/aicc callAnthropicApi 로직 이식.
    1. 시스템 프롬프트 구성
    2. 제품 정보 + searchRelevantQna 결과 주입
    3. Claude API 호출 (Vision 지원)
    """
    model = session["selected_model"]
    menu = session["selected_menu"]
    messages_history = session.get("messages", [])

    # 제품 데이터 없으면 fallback
    product = data_loader.get_product(model)
    if not product and not data_loader.get_erp_code(model):
        return FALLBACK_NO_DATA.format(model=model)

    # ── 시스템 프롬프트 구성 (기존 buildSystemPrompt) ──────────
    sys_prompt = SYSTEM_BASE.format(
        wrong_answers=data_loader.wrong_answers_text[:1200]
    )

    # 현재 상담 제품 정보 (기존: session.model + category)
    cat = ""
    if product:
        cat = product.get("카테고리", "")
    sys_prompt += f"\n## 현재 상담 제품\n모델명: {model}\n카테고리: {cat or '미분류'}\n이 제품에 집중하여 답변하세요.\n"

    # 제품 스펙 (추가 정보)
    if product:
        feat = product.get("제품특징", {})
        if feat and isinstance(feat, dict):
            spec_lines = "\n".join(f"  {k}: {v}" for k, v in feat.items() if v)
            if spec_lines:
                sys_prompt += f"\n## 제품 스펙\n{spec_lines}\n"

    # 제품 링크 (드라이버는 해당 제품에 드라이버가 있는 경우에만 안내)
    product_url = data_loader.get_product_url(model)
    if data_loader.has_driver(model):
        driver_url = data_loader.get_driver_url(model)
        sys_prompt += f"\n## 이 제품의 링크\n드라이버: {driver_url}\n제품 페이지: {product_url}\n"
    else:
        sys_prompt += f"\n## 이 제품의 링크\n제품 페이지: {product_url}\n"
        sys_prompt += "\n## 드라이버 안내\n이 제품은 별도의 드라이버 설치가 필요 없는 제품입니다. 연결하면 자동으로 인식됩니다. 고객이 드라이버를 문의하면 '이 제품은 별도의 드라이버 설치 없이 연결만 하면 자동으로 인식됩니다'라고 안내하세요.\n"

    # 설치 가이드
    if menu in ("기술문의",):
        install = data_loader.get_install_section(model)
        if install:
            sys_prompt += f"\n## 설치/연결 가이드\n{install[:500]}\n"
        errors = data_loader.get_errors(model)
        if errors:
            e_text = "\n".join(f"  증상: {e['symptom'][:80]}\n  해결: {e['solution']}" for e in errors[:3])
            sys_prompt += f"\n## 오류/증상 대응\n{e_text}\n"

    elif menu == "AS문의":
        sys_prompt += f"\n## A/S 정책\n{data_loader.policy_as[:800]}\n"

    elif menu == "배송문의":
        sys_prompt += f"\n## 배송 정책\n{data_loader.policy_delivery[:600]}\n"

    # ── searchRelevantQna (기존 시스템 그대로) ──────────────────
    relevant_qna = data_loader.search_relevant_qna(user_message, model, max_results=5)

    if relevant_qna:
        sys_prompt += "\n## 참고 QnA 데이터\n"
        for ref in relevant_qna:
            sys_prompt += f"\n[{ref['model']}]\nQ: {ref['question'][:200]}\nA: {ref['answer'][:300]}\n"
        sys_prompt += "\n위 데이터를 참고하되 자연스럽게 답변하세요.\n"

    # ── 이미지가 있으면 시스템 프롬프트에 안내 추가 ──────────────
    has_image = False
    images = session.get("images", {})
    if image_id and image_id in images:
        has_image = True
        sys_prompt += "\n## 이미지 분석\n고객이 이미지를 보냈습니다. 이미지를 분석하여 답변하세요.\n- 제품 사진이면 모델명을 식별하세요.\n- 오류/증상 사진이면 원인과 해결방법을 안내하세요.\n- 설치 관련 사진이면 올바른 연결 방법을 안내하세요.\n"

    # ── 메시지 배열 구성 (기존: recent 10개, Vision 지원) ──────
    api_messages = []
    history = messages_history[-10:]
    for msg in history:
        if msg["role"] in ("user", "assistant"):
            msg_image_id = msg.get("image_id")
            # 과거 메시지에 이미지가 있으면 멀티블록으로 구성
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

    # 현재 메시지 (이미지 포함)
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

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1200 if has_image else 800,
            system=sys_prompt,
            messages=api_messages,
        )
        return response.content[0].text
    except Exception as e:
        print(f"[AICC AI] 오류: {e}")
        return FALLBACK_ERROR
