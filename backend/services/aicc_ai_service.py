"""
AICC AI 서비스
핵심 변경: 매 메시지마다 키워드 기반 QnA 실시간 검색 → AI에 주입
(기존 내부 시스템 searchRelevantQna 로직 이식)
"""
import os
import re
import anthropic
from .aicc_data_loader import data_loader

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── 시스템 프롬프트 ─────────────────────────────────────────────

SYSTEM_BASE = """당신은 랜스타(LANstar) 공식 AI 기술상담사입니다.
회사: 라인업시스템(주) | 전화: 02-717-3386 | 평일 10:00~17:00

[절대 규칙]
1. 고객이 이미 제품을 선택했으므로 절대 모델명을 다시 묻지 말 것. 제품 스펙, 치수, 높이 등을 물으면 [제품 스펙]에서 바로 찾아 답변할 것.
2. [참고 QnA 데이터]에 고객 질문과 관련된 실제 상담 사례가 포함되어 있습니다. 반드시 이 데이터를 최우선으로 참조하여 답변하세요. 일반 지식보다 QnA 데이터가 우선합니다.
3. 드라이버 설치/다운로드 관련 문의 시 반드시 [드라이버 다운로드 링크]를 포함하여 안내하세요.
4. 모르는 내용: "전화(02-717-3386) 문의 바랍니다" 안내
5. 가격 직접 안내 금지 (가격지도 적용 제품 특히 주의)
6. 단종 제품: 현행 제품으로 절대 안내하지 말 것
7. 반품/교환: 조건 확인 없이 "가능" 단정 금지
8. 답변은 명확하고 간결하게 (불필요한 반복 금지)
9. 마지막에 "추가 문의가 있으시면 편하게 말씀해 주세요" 추가

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


def _search_relevant_qna(query: str, model: str, max_results: int = 8) -> list:
    """
    기존 내부 시스템의 searchRelevantQna 로직 이식.
    사용자 질문 키워드로 QnA를 실시간 검색하여 관련도 높은 항목 반환.
    같은 모델의 QnA에 가산점(+5)
    """
    upper = query.upper()
    words = [w for w in re.split(r'[\s,]+', upper) if len(w) >= 2]
    if not words:
        words = [upper]

    results = []
    product = data_loader.get_product(model)

    # 1. 현재 제품의 QnA 검색 (가장 중요)
    if product:
        all_qna = (
            product.get("QnA", []) +
            product.get("추론QnA", []) +
            product.get("상담대화", [])
        )
        for q in all_qna:
            question = str(q.get("문의", ""))
            answer = str(q.get("답변핵심", q.get("답변", "")))
            if not question or not answer:
                continue
            text = (question + " " + answer).upper()
            score = 5  # 같은 모델 가산점
            for w in words:
                if w in text:
                    score += 1
            if score > 5:  # 키워드 매칭이 1개 이상
                results.append({
                    "model": model,
                    "question": question,
                    "answer": answer,
                    "score": score,
                })

    # 2. 골든앤서에서도 검색
    golden = data_loader.get_golden_by_model(model)
    for g in golden:
        text = (g["question"] + " " + g["answer"]).upper()
        score = 5
        for w in words:
            if w in text:
                score += 1
        if score > 5:
            results.append({
                "model": model,
                "question": g["question"],
                "answer": g["answer"],
                "score": score + 2,  # 골든앤서 추가 가산
            })

    # 3. FAQ에서도 검색
    faq = data_loader.get_faq_by_model(model)
    for f in faq:
        text = (f["question"] + " " + f["answer"]).upper()
        score = 5
        for w in words:
            if w in text:
                score += 1
        if score > 5:
            results.append({
                "model": model,
                "question": f["question"],
                "answer": f["answer"],
                "score": score + 1,
            })

    # 점수 내림차순 정렬
    results.sort(key=lambda x: x["score"], reverse=True)

    # 중복 제거 (질문 앞 50자 기준)
    seen = set()
    unique = []
    for r in results:
        key = r["question"][:50]
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique[:max_results]


def _build_product_context(model: str, menu: str) -> str:
    """제품 기본 정보 + 정적 컨텍스트 (QnA는 별도 동적 검색)"""
    product = data_loader.get_product(model)
    parts = []

    # ── 제품 기본 정보 ─────────────────────────────────────────
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

    # ── 드라이버 다운로드 URL ──────────────────────────────────
    driver_url = data_loader.get_driver_url(model)
    parts.append(
        f"[드라이버 다운로드 링크]\n"
        f"드라이버 설치/다운로드 문의 시 반드시 아래 URL을 안내하세요:\n"
        f"{driver_url}"
    )

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
    핵심: 매 메시지마다 키워드 기반 QnA 실시간 검색하여 컨텍스트 주입
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

    # ── 매 메시지마다 키워드 기반 QnA 검색 ──────────────────────
    relevant_qna = _search_relevant_qna(user_message, model, max_results=8)

    # QnA 컨텍스트 문자열
    qna_context = ""
    if relevant_qna:
        qna_lines = []
        for i, ref in enumerate(relevant_qna, 1):
            qna_lines.append(
                f"[{ref['model']}]\n"
                f"Q: {ref['question'][:200]}\n"
                f"A: {ref['answer'][:300]}"
            )
        qna_context = "\n\n## 참고 QnA 데이터\n" + "\n\n".join(qna_lines)
        qna_context += "\n\n위 데이터를 참고하되 자연스럽게 답변하세요."

    # 메시지 배열 구성
    api_messages = []

    if not messages_history:
        # ── 첫 번째 메시지: 제품 컨텍스트 + QnA + 사용자 질문 ──
        context = _build_product_context(model, menu)
        first_content = (
            f"{context}\n\n"
            f"{qna_context}\n\n"
            f"---\n"
            f"위 제품 정보와 상담 사례를 참조하여 아래 고객 문의에 답변해 주세요.\n\n"
            f"고객 문의: {user_message}"
        )
        api_messages = [{"role": "user", "content": first_content}]
    else:
        # ── 이후 메시지: 히스토리 + 실시간 QnA 검색 결과 주입 ──
        history = messages_history[-20:]
        for msg in history:
            role = msg["role"]
            if role == "user":
                api_messages.append({"role": "user", "content": msg["content"]})
            elif role == "assistant":
                api_messages.append({"role": "assistant", "content": msg["content"]})

        # 현재 질문에 관련 QnA를 함께 전달
        user_content = user_message
        if qna_context:
            user_content = (
                f"{user_message}\n\n"
                f"[참고: 이 질문과 관련된 기존 상담 데이터]{qna_context}"
            )
        api_messages.append({"role": "user", "content": user_content})

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
