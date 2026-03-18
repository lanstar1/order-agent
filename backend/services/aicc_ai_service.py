"""
AICC AI 서비스
하이브리드 답변 방식:
  1차: 제품 지식 DB (정확한 제품 정보)
  2차: QnA 검색 (보충/참고)
  → Claude가 두 소스를 조합하여 풍성한 답변 생성
"""
import json
import os
import anthropic
from .aicc_data_loader import data_loader

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── 시스템 프롬프트 ───────────────────────────────────────

SYSTEM_BASE = """당신은 "랜스타(Lanstar)" 제품의 기술 상담 전문 AI 어시스턴트입니다.
랜스타는 HDMI, USB, DP 케이블, 컨버터, KVM 스위치, 네트워크 장비, 도어락 등 IT 주변기기를 제조/판매하는 회사입니다.
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
- ❌ "C타입 케이블 연결 시 보안 해제" 같은 근거 없는 정보 생성
- ❌ 일반 IT 지식으로 제품 동작을 추측하는 것
- ❌ "시스템에 등록되어 있지 않아 안내가 어렵습니다" — DB/QnA를 먼저 꼼꼼히 확인하세요
- ❌ DB에 있는 정보인데 "정보가 없다"고 답하는 것

### 4. 반드시 해야 할 것
- ✅ DB의 절차/단계는 원문 그대로 번호 매겨 안내
- ✅ QnA의 답변 내용을 적극 참고하여 풍성한 답변 구성
- ✅ 정말로 DB/QnA에 없는 내용만 고객센터 안내
- ✅ 한국어 답변, 모델명 정확 표기
- ✅ 마지막에 "추가 문의가 있으시면 편하게 말씀해 주세요."

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


async def get_ai_response(session: dict, user_message: str, image_id: str = None) -> str:
    """
    하이브리드 답변 생성:
    1차: 제품 지식 DB (정확한 제품 상세 정보)
    2차: QnA 검색 (보충 참고)
    → Claude가 조합하여 답변
    """
    model = session["selected_model"]
    menu = session["selected_menu"]
    messages_history = session.get("messages", [])

    # 제품 데이터 없으면 fallback
    product = data_loader.get_product(model)
    if not product and not data_loader.get_erp_code(model):
        # 제품 지식 DB에는 있을 수 있으므로 체크
        knowledge = _get_knowledge(model)
        if not knowledge:
            return FALLBACK_NO_DATA.format(model=model)

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

    # ── 2차 소스: QnA 검색 (중요한 보충 자료) ────────────────────
    relevant_qna = data_loader.search_relevant_qna(user_message, model, max_results=10)

    if relevant_qna:
        sys_prompt += "\n## [2차 소스] 고객 QnA 데이터 — 실제 고객 문의와 답변입니다. 적극 활용하세요.\n"
        for ref in relevant_qna:
            sys_prompt += f"\n[{ref['model']}]\nQ: {ref['question'][:250]}\nA: {ref['answer'][:400]}\n"
        sys_prompt += "\n위 QnA에서 고객 질문과 유사한 사례를 찾아 답변에 활용하세요. 제품 지식 DB와 상충하면 DB를 우선하세요.\n"

    # ── 이미지가 있으면 안내 추가 ─────────────────────────
    has_image = False
    images = session.get("images", {})
    if image_id and image_id in images:
        has_image = True
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

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500 if has_image else 1200,
            system=sys_prompt,
            messages=api_messages,
        )
        return response.content[0].text
    except Exception as e:
        print(f"[AICC AI] 오류: {e}")
        return FALLBACK_ERROR


def _get_knowledge(model: str) -> dict | None:
    """제품 지식 DB에서 모델 정보 조회 (캐시 포함)"""
    try:
        from .aicc_db import get_product_knowledge
        return get_product_knowledge(model)
    except Exception as e:
        print(f"[AICC AI] 지식DB 조회 오류: {e}")
        return None
