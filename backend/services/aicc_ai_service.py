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
랜스타는 HDMI, USB, DP 케이블, 컨버터, KVM 스위치, 네트워크 장비, 도어락 등 IT 주변기기를 제조/판매하는 회사입니다.
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
- 조건에 맞는 제품이 없으면 솔직히 "해당 조건의 제품은 현재 취급하지 않습니다" 안내
- 유사한 대안이 있으면 함께 제안
- 전화(02-717-3386) 문의 안내

### 6. 기타
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


async def get_product_inquiry_response(session: dict, user_message: str, image_id: str = None) -> str:
    """
    제품문의 전용 AI 응답:
    1차: 키워드로 제품 데이터 검색 (01_제품별_통합데이터 + 품목가격정보)
    2차: Claude가 고객 맥락에 맞게 추천
    """
    messages_history = session.get("messages", [])

    # ── 시스템 프롬프트 구성 ──────────────────────────────
    sys_prompt = SYSTEM_PRODUCT_INQUIRY

    # ── 1차: 제품 데이터 검색 ─────────────────────────────
    matched_products = data_loader.search_products_for_recommendation(user_message, max_results=15)

    if matched_products:
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

            sys_prompt += f"\n- **{model}** | 카테고리: {p.get('category', '미분류')} | 품명: {p.get('product_name', '')[:80]}\n"
            sys_prompt += f"  특징: {feat_text}\n"
            sys_prompt += f"  제품URL: {product_url}\n"
            if price_label:
                sys_prompt += f"  {price_label}\n"

        sys_prompt += "\n※ 가격등급은 상대 비교 전용입니다. 숫자가 높을수록 고가입니다. 절대 가격 수치를 고객에게 말하지 마세요.\n"
    else:
        sys_prompt += "\n## 검색 결과\n키워드에 매칭되는 제품이 없습니다. 고객에게 좀 더 구체적인 조건을 요청하거나, 고객센터 안내를 해주세요.\n"

    # ── 대화 이력에서 이전에 추천한 모델 추적 (연속 대화용) ──
    prev_models = set()
    for msg in messages_history[-10:]:
        if msg["role"] == "assistant":
            # 이전 답변에서 모델명 추출
            import re as _re
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

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            system=sys_prompt,
            messages=api_messages,
        )
        return response.content[0].text
    except Exception as e:
        print(f"[AICC AI] 제품문의 오류: {e}")
        return FALLBACK_ERROR


def _get_knowledge(model: str) -> dict | None:
    """제품 지식 DB에서 모델 정보 조회 (캐시 포함)"""
    try:
        from .aicc_db import get_product_knowledge
        return get_product_knowledge(model)
    except Exception as e:
        print(f"[AICC AI] 지식DB 조회 오류: {e}")
        return None
