"""
AICC AI 답변 서비스 — Claude API 호출
"""
import os
import anthropic
from .aicc_data_loader import data_loader

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM_PROMPT_TEMPLATE = """
당신은 랜스타(LANstar) 공식 AI 기술상담사입니다.
회사명: 라인업시스템(주) | 고객센터: 02-717-3386 | 상담시간: 평일 10:00~17:00

[반드시 지켜야 할 규칙]
1. 모르는 내용: "정확한 안내를 위해 전화(02-717-3386)로 문의 주세요" 안내
2. 가격 문의: 정확한 가격은 쇼핑몰(www.lanstar.co.kr)에서 확인 요청
3. 가격지도 적용 제품: 가격 직접 안내 불가, 쇼핑몰 확인 안내
4. 단종 제품: 현행 제품으로 안내 금지, 단종 사실 + 후속 모델 안내
5. 반품/교환: 조건 확인 없이 "가능합니다" 단정 금지
6. 재고: 추측하지 말고 "재고조회 메뉴를 이용해 주세요" 안내
7. 답변: 3~5문장으로 간결하게, 반복 금지
8. 마지막: 항상 "추가 문의가 있으시면 편하게 말씀해 주세요" 추가

[절대 이렇게 안내하면 안 되는 오답 사례 요약]
{wrong_answers}
""".strip()

FALLBACK_NO_DATA = (
    "선택하신 {model_name} 제품의 상세 정보를 현재 준비 중입니다.\n"
    "정확한 안내를 위해 전화(02-717-3386)로 문의해 주세요.\n"
    "상담시간: 평일 10:00~17:00"
)

FALLBACK_AI_ERROR = (
    "죄송합니다. 일시적인 오류가 발생했습니다.\n"
    "전화(02-717-3386)로 문의해 주시면 바로 안내해 드리겠습니다."
)

QUOTE_GUIDE = (
    "견적서는 쇼핑몰 장바구니에서 바로 출력하실 수 있습니다.\n"
    "1. 원하시는 제품을 장바구니에 담아주세요\n"
    "2. 장바구니 페이지에서 '견적서 출력' 버튼을 클릭하세요\n"
    "3. PDF로 저장하거나 출력하실 수 있습니다\n"
    "https://www.lanstar.co.kr/order/cart.php"
)


def build_context(session: dict) -> str:
    """문의 유형에 따라 AI 컨텍스트 구성"""
    model = session["selected_model"]
    menu = session["selected_menu"]
    product = data_loader.get_product(model)

    ctx_parts = []

    # ── 제품 기본 정보 ──────────────────────────────────────
    if product:
        feat = product.get("제품특징", {})
        ctx_parts.append(f"[선택 제품]\n모델명: {model}\n카테고리: {product.get('카테고리', '-')}")
        if feat:
            feat_str = "\n".join(f"  {k}: {v}" for k, v in feat.items() if v)
            ctx_parts.append(f"제품 특징:\n{feat_str}")

        # QnA (최대 5건)
        qna_list = product.get("QnA", [])[:5]
        if qna_list:
            qna_str = "\n".join(
                f"  Q: {q.get('문의', '')[:100]}\n  A: {q.get('답변핵심', '')}"
                for q in qna_list
            )
            ctx_parts.append(f"[관련 QnA]\n{qna_str}")
    else:
        ctx_parts.append(f"[선택 제품]\n모델명: {model}\n(제품 정보 없음 — 일반적인 안내만 가능)")

    # ── 문의 유형별 추가 컨텍스트 ──────────────────────────
    if menu == "기술문의":
        install = data_loader.get_install_guide_section(model)
        if install:
            ctx_parts.append(f"[설치/연결 가이드]\n{install[:600]}")
        compat = data_loader.get_compatibility(model)
        if compat:
            c_str = "\n".join(f"  Q: {c['question'][:80]}\n  A: {c['answer']}" for c in compat[:3])
            ctx_parts.append(f"[호환성 정보]\n{c_str}")
        errors = data_loader.get_errors(model)
        if errors:
            e_str = "\n".join(f"  증상: {e['symptom'][:80]}\n  해결: {e['solution']}" for e in errors[:3])
            ctx_parts.append(f"[오류/증상 대응]\n{e_str}")

    elif menu == "AS문의":
        ctx_parts.append(f"[A/S 정책]\n{data_loader.policy_as[:1000]}")

    elif menu == "배송문의":
        ctx_parts.append(f"[배송 정책]\n{data_loader.policy_delivery[:800]}")

    elif menu == "교환/반품":
        ctx_parts.append(f"[교환/반품 규정]\n{data_loader.policy_return[:800]}")

    elif menu == "제품문의":
        category = product.get("카테고리", "") if product else ""
        golden = data_loader.get_golden_answers_by_category(category)
        if golden:
            g_str = "\n".join(f"  Q: {g['question'][:80]}\n  A: {g['answer']}" for g in golden)
            ctx_parts.append(f"[카테고리 골든앤서]\n{g_str}")

    # ── 가격지도 주의 ────────────────────────────────────────
    if data_loader.is_price_restricted(model):
        ctx_parts.append("⚠️ 이 제품은 온라인 가격지도 적용 제품입니다. 가격을 직접 안내하지 마세요.")

    return "\n\n".join(ctx_parts)


async def get_ai_response(session: dict, user_message: str) -> str:
    """Claude API 호출 → 답변 텍스트 반환"""
    model_name = session["selected_model"]
    product = data_loader.get_product(model_name)

    # 제품 정보가 전혀 없는 경우 → fallback
    if not product and not data_loader.get_erp_code(model_name):
        return FALLBACK_NO_DATA.format(model_name=model_name)

    # 시스템 프롬프트
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        wrong_answers=data_loader.wrong_answers_text[:1500]
    )

    # 컨텍스트 구성
    context = build_context(session)

    # 대화 히스토리 (최근 10턴)
    messages = []
    history = session.get("messages", [])[-20:]  # 최근 20개 메시지 = 10턴

    # 첫 메시지에 컨텍스트 주입
    first_user_content = f"{context}\n\n---\n\n고객 문의: {user_message}" if not history else user_message

    if not history:
        messages = [{"role": "user", "content": first_user_content}]
    else:
        # 이전 대화 히스토리 포함
        for msg in history:
            if msg["role"] in ("user",):
                messages.append({"role": "user", "content": msg["content"]})
            elif msg["role"] in ("assistant",):
                messages.append({"role": "assistant", "content": msg["content"]})
        messages.append({"role": "user", "content": user_message})

    # messages가 비어있거나 첫 번째가 user가 아닌 경우 보정
    if not messages:
        messages = [{"role": "user", "content": first_user_content}]
    elif messages[0]["role"] != "user":
        messages.insert(0, {"role": "user", "content": f"{context}\n\n---\n\n(대화 이력 참고)"})

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=800,
            system=system_prompt,
            messages=messages,
        )
        return response.content[0].text
    except Exception as e:
        print(f"[AICC] Claude API 오류: {e}")
        return FALLBACK_AI_ERROR
