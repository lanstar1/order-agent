"""
AICC AI 서비스
핵심: 이미 선택된 제품 컨텍스트(QnA, 스펙)를 시스템 프롬프트에 강제 주입
→ AI가 "모델명을 알려달라"고 절대 묻지 않도록 설계
"""
import os
import anthropic
from .aicc_data_loader import data_loader

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── 시스템 프롬프트 ─────────────────────────────────────────────

SYSTEM_BASE = """당신은 랜스타(LANstar) 공식 AI 기술상담사입니다.
회사: 라인업시스템(주) | 전화: 02-717-3386 | 평일 10:00~17:00

[절대 규칙]
1. 고객이 이미 제품을 선택했으므로 절대 모델명을 다시 묻지 말 것. 제품 스펙, 치수, 높이 등을 물으면 [제품 스펙]에서 바로 찾아 답변할 것.
2. [이 제품 전용 모범답변]이 있으면 반드시 그 내용을 최우선으로 참조. 일반 지식보다 모범답변이 우선.
3. [이 제품 FAQ]와 [상담 사례]도 적극 활용하여 정확한 답변 제공.
4. 드라이버 다운로드 문의 시 반드시 [드라이버 다운로드] URL을 포함하여 안내.
5. 모르는 내용: "전화(02-717-3386) 문의 바랍니다" 안내
6. 가격 직접 안내 금지 (가격지도 적용 제품 특히 주의)
7. 단종 제품: 현행 제품으로 절대 안내하지 말 것
8. 반품/교환: 조건 확인 없이 "가능" 단정 금지
9. 답변은 명확하고 간결하게 (불필요한 반복 금지)
10. 마지막에 "추가 문의가 있으시면 편하게 말씀해 주세요" 추가

[주의해야 할 오답 사례 요약]
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


def _build_product_context(model: str, menu: str) -> str:
    """
    선택된 제품의 컨텍스트 빌드
    이 내용이 AI 첫 번째 user 메시지에 주입됨 → AI가 모델명을 다시 묻는 버그 방지
    """
    product = data_loader.get_product(model)
    parts = []

    # ── 제품 기본 정보 (항상 포함) ─────────────────────────────
    parts.append(f"[이미 선택된 제품]\n모델명: {model}")
    if product:
        cat = product.get("카테고리", "")
        if cat:
            parts.append(f"카테고리: {cat}")
        feat = product.get("제품특징", {})
        if feat and isinstance(feat, dict):
            spec_lines = "\n".join(f"  {k}: {v}" for k, v in feat.items() if v)
            if spec_lines:
                parts.append(f"제품 스펙:\n{spec_lines}")

    # ── 모델별 골든앤서 (최우선 참조) ──────────────────────────
    golden_model = data_loader.get_golden_by_model(model)
    if golden_model:
        g_text = "\n".join(
            f"  Q: {g['question'][:120]}\n  A: {g['answer'][:300]}"
            + (f"\n  ⚠️ 주의: {g['warning']}" if g.get('warning') else "")
            for g in golden_model
        )
        parts.append(f"[이 제품 전용 모범답변 — 반드시 이 답변을 최우선 참조]\n{g_text}")

    # ── 모델별 FAQ ─────────────────────────────────────────────
    faq_model = data_loader.get_faq_by_model(model)
    if faq_model:
        f_text = "\n".join(
            f"  Q: {f['question'][:120]}\n  A: {f['answer'][:300]}"
            for f in faq_model
        )
        parts.append(f"[이 제품 FAQ]\n{f_text}")

    # ── 관련 QnA (이 제품의 실제 상담 데이터) ──────────────────
    if product:
        qna_list = (
            product.get("QnA", []) +
            product.get("추론QnA", []) +
            product.get("상담대화", [])
        )
        qna_list = qna_list[:8]
        if qna_list:
            qna_text = ""
            for i, q in enumerate(qna_list, 1):
                question = str(q.get("문의", ""))[:120]
                answer = str(q.get("답변핵심", q.get("답변", "")))[:200]
                if question and answer:
                    qna_text += f"\n  [{i}] Q: {question}\n      A: {answer}"
            if qna_text:
                parts.append(f"[이 제품 관련 실제 상담 사례]{qna_text}")

    # ── 드라이버 다운로드 URL ──────────────────────────────────
    driver_url = data_loader.get_driver_url(model)
    parts.append(f"[드라이버 다운로드]\n드라이버 문의 시 반드시 아래 URL을 안내하세요:\n{driver_url}")

    # ── 문의 유형별 추가 컨텍스트 ─────────────────────────────
    if menu in ("기술문의",):
        install = data_loader.get_install_section(model)
        if install:
            parts.append(f"[설치/연결 가이드]\n{install[:500]}")
        compat = data_loader.get_compat(model)
        if compat:
            c_text = "\n".join(
                f"  Q: {c['question'][:80]}\n  A: {c['answer']}"
                for c in compat[:3]
            )
            parts.append(f"[호환성 정보]\n{c_text}")
        errors = data_loader.get_errors(model)
        if errors:
            e_text = "\n".join(
                f"  증상: {e['symptom'][:80]}\n  해결: {e['solution']}"
                for e in errors[:3]
            )
            parts.append(f"[오류/증상 대응]\n{e_text}")

    elif menu == "AS문의":
        parts.append(f"[A/S 정책]\n{data_loader.policy_as[:800]}")

    elif menu == "배송문의":
        parts.append(f"[배송 정책]\n{data_loader.policy_delivery[:600]}")

    elif menu == "제품문의" and product:
        cat = product.get("카테고리", "")
        golden = data_loader.get_golden_by_category(cat)
        if golden:
            g_text = "\n".join(
                f"  Q: {g['question'][:80]}\n  A: {g['answer']}"
                for g in golden
            )
            parts.append(f"[카테고리({cat}) 골든앤서]\n{g_text}")

    # 가격지도 주의
    if data_loader.is_price_restricted(model):
        parts.append("⚠️ 가격지도 적용 제품 — 가격을 직접 안내하지 말 것")

    return "\n\n".join(parts)


async def get_ai_response(session: dict, user_message: str) -> str:
    """
    Claude API 호출 → 답변 반환
    첫 번째 메시지에 제품 컨텍스트 강제 주입 (모델명 재질문 버그 방지)
    """
    model = session["selected_model"]
    menu = session["selected_menu"]
    messages_history = session.get("messages", [])

    # 제품 데이터 없으면 fallback
    product = data_loader.get_product(model)
    if not product and not data_loader.get_erp_code(model):
        return FALLBACK_NO_DATA.format(model=model)

    # 시스템 프롬프트
    system_prompt = SYSTEM_BASE.format(
        wrong_answers=data_loader.wrong_answers_text[:1200]
    )

    # 메시지 배열 구성
    api_messages = []

    if not messages_history:
        # ── 첫 번째 메시지: 제품 컨텍스트 + 사용자 질문 통합 ──
        context = _build_product_context(model, menu)
        first_content = (
            f"{context}\n\n"
            f"---\n"
            f"위 제품 정보와 상담 사례를 참조하여 아래 고객 문의에 답변해 주세요.\n\n"
            f"고객 문의: {user_message}"
        )
        api_messages = [{"role": "user", "content": first_content}]
    else:
        # ── 이후 메시지: 히스토리 유지 (최근 10턴) ─────────────
        history = messages_history[-20:]  # 최근 20개 = 10턴
        for msg in history:
            role = msg["role"]
            if role in ("user",):
                api_messages.append({"role": "user", "content": msg["content"]})
            elif role == "assistant":
                api_messages.append({"role": "assistant", "content": msg["content"]})
        api_messages.append({"role": "user", "content": user_message})

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=800,
            system=system_prompt,
            messages=api_messages,
        )
        return response.content[0].text
    except Exception as e:
        print(f"[AICC AI] 오류: {e}")
        return FALLBACK_ERROR
