"""매입정산 매칭 서비스
거래처 원장의 상품명 ↔ ERP 구매현황/판매현황의 품명 매칭

핵심 로직:
1. 구매현황 매칭 (규칙 기반): 날짜 + 금액 + 수량으로 매칭 — AI 불필요
2. 판매이력 확인 (AI): 누락 건에 대해 날짜 확장 검색 + Claude AI 후보 추천
   (품명이 다를 수 있으므로 AI 추론이 필요)
"""
import json
import re
from datetime import datetime, timedelta
from anthropic import AsyncAnthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ============================================================
# Helper function for field access with fallback keys
# ============================================================

def _get_field(item: dict, *keys, default=""):
    """Try multiple key names and return the first non-empty value.

    Priority order: tries keys in the order provided, returns default if none found.
    Usage: _get_field(item, "prod_cd", "품목코드", "product_code", default="")
    """
    if not item:
        return default
    for key in keys:
        value = item.get(key, "")
        if value and str(value).strip():
            return value
    return default


# ============================================================
# Shipping/delivery keyword detection
# ============================================================

SHIPPING_KEYWORDS = ["배송", "택배", "경동", "로젠", "물류", "운송", "화물", "선불", "착불", "배달", "우편", "등기", "퀵", "용달", "대한통운", "CJ", "한진", "우체국"]

# ============================================================
# Discount keyword detection (매출할인, DC/, 리베이트 등)
# ============================================================

DISCOUNT_KEYWORDS = {"매출할인", "할인", "리베이트", "DC", "할인DC", "에누리"}

def _is_discount_item(item: dict) -> bool:
    """할인 항목인지 판별 (매출할인, DC/, 리베이트 등)"""
    pname = str(item.get("product_name", "") or "").strip()
    pcat = str(item.get("product_category", "") or "").strip()
    combined = f"{pname} {pcat}"
    # 키워드 직접 포함
    if any(kw in combined for kw in DISCOUNT_KEYWORDS):
        return True
    # DC/ prefix 패턴 (예: "DC/상품명")
    if pname.upper().startswith("DC/") or pname.upper().startswith("DC\\"):
        return True
    return False


# ============================================================
# Payment/settlement entry detection (입금, 출금, 기타 결제)
# ============================================================

PAYMENT_TX_TYPES = {"입금", "출금", "결제", "수금", "지급", "이체"}

def _is_payment_entry(item: dict) -> bool:
    """결제/입출금 항목인지 판별 (매칭 대상에서 제외해야 하는 항목)

    - tx_type이 입금/출금/결제/수금/지급/이체
    - tx_type이 '기타'이면서 금액이 매우 큰 음수 (결제 성격)
    - 품목명이 비어있고 tx_type이 매출/매입이 아닌 경우
    """
    tx_type = str(item.get("tx_type", "") or "").strip()

    # 직접 결제 유형
    if tx_type in PAYMENT_TX_TYPES:
        return True

    # '기타' 유형 + 대금성 판단 (금액 100만원 이상 음수이면서 품목명 없음)
    if tx_type == "기타":
        amt = float(item.get("amount", 0) or 0)
        pname = str(item.get("product_name", "") or "").strip()
        if amt < -1000000 and not pname:
            return True
        # 품목명이 "기타"인 경우도 결제성
        if pname in ("기타", ""):
            return True

    return False

def _is_shipping_item(item: dict) -> bool:
    """배송료/운송비 항목인지 판별"""
    name = str(item.get("product_name", "") or "").upper()
    model = str(item.get("model_name", "") or "").upper()
    category = str(item.get("product_category", "") or "").upper()
    combined = f"{name} {model} {category}"
    return any(kw.upper() in combined for kw in SHIPPING_KEYWORDS)


# ============================================================
# 1. 구매현황 매칭 (거래처 원장 vs ERP 구매현황)
# ============================================================

async def match_products_ai(vendor_items: list[dict],
                             erp_items: list[dict]) -> list[dict]:
    """거래처 원장 vs ERP 구매현황 매칭 (규칙 기반: 날짜 + 금액 + 수량)

    1단계 매칭은 AI 없이 날짜/금액/수량으로 충분.
    매칭 안 된 건만 2단계(check_sales_history)에서 AI를 사용.
    """
    return _rule_based_match(vendor_items, erp_items)


# ============================================================
# 2. 판매이력 확인 — 날짜 확장 검색 + AI 후보군 5개
# ============================================================

def _parse_date(date_str: str, year: int = None) -> datetime | None:
    if year is None:
        year = datetime.now().year
    if not date_str:
        return None
    date_str = date_str.strip()
    date_str = re.sub(r'-\d+$', '', date_str)

    patterns = [
        (r'^(\d{2})\.(\d{2})$', lambda m, y=year: datetime(y, int(m[1]), int(m[2]))),
        (r'^(\d{2})/(\d{2})$', lambda m, y=year: datetime(y, int(m[1]), int(m[2]))),
        (r'^(\d{4})/(\d{2})/(\d{2})$', lambda m, y=year: datetime(int(m[1]), int(m[2]), int(m[3]))),
        (r'^(\d{4})(\d{2})(\d{2})$', lambda m, y=year: datetime(int(m[1]), int(m[2]), int(m[3]))),
    ]

    for pattern, factory in patterns:
        m = re.match(pattern, date_str)
        if m:
            try:
                return factory(m)
            except (ValueError, TypeError):
                continue
    return None


def _filter_by_date_range(items: list[dict], target_date: datetime,
                           days_range: int, date_field: str = "date") -> list[dict]:
    if not target_date:
        return items

    start = target_date - timedelta(days=days_range)
    end = target_date + timedelta(days=days_range)
    result = []

    for item in items:
        item_date_str = _get_field(item, date_field, "date", "월/일", "연/월/일", default="")
        item_date = _parse_date(item_date_str)
        if item_date and start <= item_date <= end:
            result.append(item)

    return result


async def check_sales_history(unmatched_items: list[dict],
                               sales_data: list[dict],
                               year: int = 2026) -> list[dict]:
    """매입전표 누락 건에 대해 판매이력 확인 (날짜 확장 검색)"""
    results = []
    date_ranges = [0, 7, 14, None]

    for item in unmatched_items:
        vendor_date_str = item.get("date", "")
        vendor_date = _parse_date(vendor_date_str, year)

        candidates = []
        search_range_used = "전체"

        for days in date_ranges:
            if days is not None:
                filtered_sales = _filter_by_date_range(sales_data, vendor_date, days)
                range_label = f"±{days}일" if days > 0 else "동일날짜"
            else:
                filtered_sales = sales_data
                range_label = "전체기간"

            if not filtered_sales:
                continue

            if client:
                candidates = await _ai_find_candidates(item, filtered_sales, max_candidates=5)
            else:
                candidates = _rule_find_candidates(item, filtered_sales, max_candidates=5)

            if candidates:
                search_range_used = range_label
                break

        has_history = len(candidates) > 0
        best_candidate = candidates[0] if candidates else None

        results.append({
            "vendor_item": item,
            "has_sales_history": has_history,
            "search_range": search_range_used,
            "candidates": candidates,
            "best_candidate": best_candidate,
            "recommendation": _get_recommendation(has_history, best_candidate),
        })

    return results


def verify_mismatch_with_sales(mismatch_items: list[dict],
                                sales_data: list[dict],
                                year: int = 2026) -> list[dict]:
    """금액불일치 항목에 대해 판매현황에서 실제 판매 수량/금액 검증.

    ERP 품목코드가 이미 매칭되어 있으므로 AI 없이 규칙 기반으로 검색.
    판매현황의 품목코드 + 날짜범위(±14일)로 판매 건수와 수량을 집계하여
    거래처원장과 구매전표 중 어느 쪽이 맞는지 근거를 제공한다.
    """
    results = []

    for r in mismatch_items:
        v = r.get("vendor_item", {})
        e = r.get("erp_match", {}) or {}
        v_qty = int(r.get("vendor_qty", v.get("qty", 0)) or 0)
        e_qty = int(r.get("erp_qty", 0) or 0)
        v_amt = float(r.get("vendor_amount", v.get("amount", 0)) or 0)
        e_amt = float(r.get("erp_amount", 0) or 0)

        # ERP 품목코드로 판매현황 검색
        erp_prod_cd = _get_field(e, "prod_cd", "품목코드", "product_code", default="")
        erp_prod_name = _get_field(e, "prod_name", "품명 및 모델", "품명 및 규격", default="")
        vendor_date = _parse_date(v.get("date", ""), year)

        # 날짜 범위 ±14일 내 판매현황 필터
        matched_sales = []
        total_sold_qty = 0
        total_sold_amt = 0

        for sale in sales_data:
            sale_prod_cd = _get_field(sale, "prod_cd", "품목코드", "product_code", default="")
            if not sale_prod_cd or sale_prod_cd != erp_prod_cd:
                continue

            # 날짜 필터 (±14일)
            sale_date_str = _get_field(sale, "date", "월/일", "연/월/일", default="")
            sale_date = _parse_date(sale_date_str, year)
            if vendor_date and sale_date:
                if abs((sale_date - vendor_date).days) > 14:
                    continue

            sale_qty = 0
            try:
                sale_qty = int(float(str(_get_field(sale, "qty", "수량", default=0) or 0).replace(",", "")))
            except (ValueError, TypeError):
                pass
            sale_amt = 0
            try:
                sale_amt = float(str(_get_field(sale, "total", "합계", "합 계", default=0) or 0).replace(",", ""))
            except (ValueError, TypeError):
                pass

            matched_sales.append({
                "date": sale_date_str,
                "prod_cd": sale_prod_cd,
                "prod_name": _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", default=""),
                "cust_name": _get_field(sale, "cust_name", "거래처명", "판매처명", default=""),
                "qty": sale_qty,
                "unit_price": float(_get_field(sale, "unit_price", "단가", default=0) or 0),
                "amount": sale_amt,
                "warehouse": _get_field(sale, "warehouse", "창고", "창고명", default=""),
            })
            total_sold_qty += sale_qty
            total_sold_amt += sale_amt

        # 판정: 판매 수량이 원장/구매전표 중 어느쪽에 가까운지
        verdict = ""
        verdict_code = ""  # "vendor" | "erp" | "unknown"
        ai_reason = ""     # AI 추론 사유
        v_name = v.get("product_name", "")
        if matched_sales:
            # 판매처 수 집계
            cust_names = list(set(s.get("cust_name", "") for s in matched_sales if s.get("cust_name")))
            cust_summary = f"{len(cust_names)}개 거래처({', '.join(cust_names[:3])}{'...' if len(cust_names) > 3 else ''})"

            if v_qty and e_qty and v_qty != e_qty:
                v_diff = abs(total_sold_qty - v_qty)
                e_diff = abs(total_sold_qty - e_qty)
                if v_diff < e_diff:
                    verdict = f"판매 {total_sold_qty}개 → 거래처원장({v_qty}개)과 일치, 구매전표 수정 필요 가능성"
                    verdict_code = "vendor"
                    ai_reason = (
                        f"판매현황에서 {erp_prod_cd} {erp_prod_name}을(를) "
                        f"±14일 내 {cust_summary}에 총 {total_sold_qty}개 판매. "
                        f"거래처원장 {v_qty}개와 일치하므로 구매전표 {e_qty}개는 "
                        f"입력 오류 가능성이 높습니다. 구매전표를 {v_qty}개로 수정하거나 "
                        f"거래처에 확인이 필요합니다."
                    )
                elif e_diff < v_diff:
                    verdict = f"판매 {total_sold_qty}개 → 구매전표({e_qty}개)와 일치, 거래처원장 확인 필요"
                    verdict_code = "erp"
                    ai_reason = (
                        f"판매현황에서 {erp_prod_cd} {erp_prod_name}을(를) "
                        f"±14일 내 {cust_summary}에 총 {total_sold_qty}개 판매. "
                        f"구매전표 {e_qty}개와 일치하므로 거래처원장 {v_qty}개는 "
                        f"거래처 측 기재 오류 가능성이 있습니다. 거래처에 확인 요청이 필요합니다."
                    )
                else:
                    verdict = f"판매 {total_sold_qty}개 — 양쪽 모두 확인 필요"
                    verdict_code = "unknown"
                    ai_reason = (
                        f"판매 {total_sold_qty}개가 원장 {v_qty}개, 구매전표 {e_qty}개 "
                        f"어느 쪽과도 정확히 일치하지 않습니다. "
                        f"분할 입고, 부분 반품 등 복합 거래일 수 있으니 거래처와 직접 확인이 필요합니다."
                    )
            else:
                verdict = f"판매 {total_sold_qty}개 {total_sold_amt:,.0f}원 확인됨"
                verdict_code = "found"
                if total_sold_qty and v_amt and total_sold_amt:
                    ai_reason = (
                        f"{cust_summary}에 총 {total_sold_qty}개 {total_sold_amt:,.0f}원 판매됨. "
                        f"금액 차이 원인을 확인하세요."
                    )
        else:
            if erp_prod_cd:
                verdict = f"판매이력 없음 ({erp_prod_cd}) — 수동 확인 필요"
                ai_reason = f"±14일 내 {erp_prod_cd} 품목의 판매이력이 없습니다. 재고로 보유 중이거나 품목코드가 다를 수 있습니다."
            else:
                verdict = "ERP 품목코드 없어 판매이력 검색 불가"
                ai_reason = "매칭된 ERP 품목코드가 없어 판매현황 검색이 불가합니다."
            verdict_code = "not_found"

        results.append({
            "vendor_item": v,
            "erp_match": e,
            "erp_prod_cd": erp_prod_cd,
            "erp_prod_name": erp_prod_name,
            "vendor_qty": v_qty,
            "erp_qty": e_qty,
            "vendor_amount": v_amt,
            "erp_amount": e_amt,
            "sales_count": len(matched_sales),
            "sales_total_qty": total_sold_qty,
            "sales_total_amt": total_sold_amt,
            "sales_details": matched_sales[:10],  # 상위 10건
            "verdict": verdict,
            "verdict_code": verdict_code,
            "ai_reason": ai_reason,
        })

    return results


def _get_recommendation(has_history: bool, best_candidate: dict | None) -> str:
    if not has_history:
        return "확인 필요 (판매이력 없음 — 미판매 품목이거나 품명이 크게 다를 수 있음)"
    conf = best_candidate.get("confidence", 0) if best_candidate else 0
    if conf >= 0.8:
        return "매입전표 입력 강력 권장 (판매이력 확인, 높은 매칭 신뢰도)"
    elif conf >= 0.5:
        return "매입전표 입력 권장 (판매이력 확인, 품명 확인 필요)"
    else:
        return "수동 확인 필요 (판매이력 있으나 품명 매칭 불확실)"


async def _ai_find_candidates(vendor_item: dict,
                                sales_data: list[dict],
                                max_candidates: int = 5) -> list[dict]:
    """AI로 판매현황에서 후보군 찾기"""
    vendor_info = {
        "name": vendor_item.get("product_name", ""),
        "model": vendor_item.get("model_name", ""),
        "category": vendor_item.get("product_category", ""),
        "qty": vendor_item.get("qty", 0),
        "amount": vendor_item.get("amount", 0),
        "date": vendor_item.get("date", ""),
    }

    # ★ 키워드/모델코드 사전 필터링: 관련성 높은 항목 우선 선택
    v_name_raw = vendor_info["name"]
    v_model_raw = vendor_info["model"]
    v_model_codes = _extract_model_codes(v_name_raw) + _extract_model_codes(v_model_raw)
    v_attrs = _extract_product_attributes(v_name_raw)
    v_keywords = set()
    for code in v_model_codes:
        v_keywords.add(code.upper())
    # 품명에서 핵심 토큰 추출 (3자 이상)
    for token in re.split(r'[\s,/\-_()（）\[\]]+', v_name_raw.upper()):
        token = token.strip()
        if len(token) >= 3:
            v_keywords.add(token)

    # 1단계: 키워드 매칭으로 관련 항목 우선 수집
    priority_items = []
    other_items = []
    for i, sale in enumerate(sales_data):
        s_code = _get_field(sale, "prod_cd", "품목코드", "product_code", default="").upper()
        s_name = _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default="").upper()
        combined = s_code + " " + s_name

        # 모델코드 또는 키워드가 매칭되는지 확인
        matched_kw = False
        for kw in v_keywords:
            if kw in combined:
                matched_kw = True
                break

        # 속성 기반 매칭도 확인
        if not matched_kw and v_attrs.get("category"):
            s_attrs = _extract_product_attributes(s_name)
            if s_attrs.get("category") == v_attrs["category"]:
                matched_kw = True

        if matched_kw:
            priority_items.append((i, sale))
        else:
            other_items.append((i, sale))

    # 우선순위 항목 + 나머지로 200건 구성
    selected = priority_items[:150] + other_items[:max(200 - len(priority_items[:150]), 50)]

    sales_summary = []
    for orig_idx, sale in selected:
        sales_summary.append({
            "idx": orig_idx,
            "code": _get_field(sale, "prod_cd", "품목코드", "product_code", default=""),
            "name": _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default=""),
            "qty": _get_field(sale, "qty", "수량", default=""),
            "amount": _get_field(sale, "total", "합계", default=""),
            "date": _get_field(sale, "date", "월/일", "연/월/일", default=""),
        })

    # 거래처 품명에서 속성 추출 (AI에게 힌트 제공)
    vendor_attrs = _extract_product_attributes(vendor_info["name"])
    vendor_attrs_str = ""
    if vendor_attrs:
        attr_parts = []
        for k, v in vendor_attrs.items():
            if k != "keywords" and v:
                attr_parts.append(f"{k}: {v}")
        if attr_parts:
            vendor_attrs_str = f"\n추출된 제품 특성: {', '.join(attr_parts)}"

    prompt = f"""거래처 원장에서 우리에게 판매한 품목인데, 우리 ERP 구매현황에 매입전표가 없습니다.
이 품목이 우리 ERP 판매현황에서 판매된 적이 있는지 찾아주세요.

## 찾아야 할 품목 (거래처가 우리에게 판매)
{json.dumps(vendor_info, ensure_ascii=False)}{vendor_attrs_str}

## 우리 ERP 판매현황 (우리가 고객에게 판매한 것)
{json.dumps(sales_summary, ensure_ascii=False)}

## 크로스매칭 가이드 (네트워크/케이블/영상기기/전산용품 전문)

### 핵심 원칙
거래처 제품을 우리 자체 브랜드(LS-, LSP-, LSN- 등)로 판매하는 경우가 많습니다.
**품명/모델명이 완전히 달라도 같은 물건**일 수 있습니다!

### 제품 특성 기반 매칭 (우선순위순)
1. **카테고리 + 핵심 스펙 일치** (가장 중요)
   - 케이블류: 규격(HDMI/DP/USB/LAN/UTP/CAT6) + 길이(1M/2M/3M/5M/10M)
   - 허브/선택기: 규격(HDMI/DP/USB) + 포트수(4포트/5포트/8포트)
   - 충전기: 타입(USB-C/PD) + 와트수(30W/65W/100W)
   - 네트워크: 속도(100M/1G/10G) + 포트수

2. **색상 동의어 매칭**
   황색=노란색=Yellow, 적색=빨간색=Red, 청색=파란색=Blue
   흑색=검정색=Black, 백색=흰색=White, 녹색=초록색=Green

3. **브랜드명 무시**: LANSTAR, NEXI, NEXT, MBF, EFM 등 자체 브랜드명 무시
4. **모델코드 대응**: 거래처 모델(ABC-123)과 우리 모델(LS-XXX)은 다르지만 스펙이 같으면 동일 제품

### 수량 참고사항
- 수량은 **참고용**으로만 사용 (일부만 판매하고 재고 보관 가능)
- 수량이 일치하면 가능성 높지만, 불일치해도 제외하지 말 것
- 거래처에서 5개 납품 → 3개 판매 + 2개 재고 가능

### 응답 시 reason에 포함할 내용
- 어떤 특성이 일치하는지 구체적으로 (예: "HDMI 2M 케이블, 4K 지원, 색상 일치(노란색)")
- 확신도가 낮은 경우 그 이유도 명시

## 응답 형식 (JSON 배열, 가능성 높은 순서로 최대 5개)
[
  {{
    "sales_idx": 3,
    "confidence": 0.85,
    "reason": "HDMI 2M 케이블 일치, 4K 지원, 수량 유사"
  }}
]

후보가 없으면 빈 배열 []을 반환하세요.
JSON 배열만 출력하세요."""

    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}]
        )

        result_text = response.content[0].text.strip()
        if result_text.startswith("["):
            ai_candidates = json.loads(result_text)
        else:
            json_match = re.search(r'\[.*\]', result_text, re.DOTALL)
            ai_candidates = json.loads(json_match.group()) if json_match else []

        candidates = []
        for c in ai_candidates[:max_candidates]:
            s_idx = c.get("sales_idx")
            if s_idx is not None and s_idx < len(sales_data):
                sale = sales_data[s_idx]
                candidates.append({
                    "sales_item": sale,
                    "confidence": c.get("confidence", 0.5),
                    "reason": c.get("reason", ""),
                    "product_code": _get_field(sale, "prod_cd", "품목코드", "product_code", default=""),
                    "product_name": _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default=""),
                    "qty": _get_field(sale, "qty", "수량", default=""),
                    "amount": _get_field(sale, "total", "합계", default=""),
                    "date": _get_field(sale, "date", "월/일", "연/월/일", default=""),
                })

        return candidates

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"AI 후보 검색 실패: {e}")
        return _rule_find_candidates(vendor_item, sales_data, max_candidates)


def _rule_find_candidates(vendor_item: dict,
                           sales_data: list[dict],
                           max_candidates: int = 5) -> list[dict]:
    """규칙 기반 판매이력 후보 찾기 (AI 폴백) — 속성 크로스매칭 포함"""
    v_name = _normalize_product_name(vendor_item.get("product_name", ""))
    v_model = _normalize_product_name(vendor_item.get("model_name", ""))
    v_qty = vendor_item.get("qty", 0)
    v_amount = vendor_item.get("amount", 0)

    # 거래처 품명에서 속성 추출
    v_raw_name = vendor_item.get("product_name", "")
    v_attrs = _extract_product_attributes(v_raw_name)

    scored = []
    for i, sale in enumerate(sales_data):
        s_raw_name = _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default="")
        s_name = _normalize_product_name(s_raw_name)
        s_code = _get_field(sale, "prod_cd", "품목코드", "product_code", default="").upper()
        s_qty = _safe_num(_get_field(sale, "qty", "수량", default=0))
        s_amount = _safe_num(_get_field(sale, "total", "합계", default=0))

        score = 0
        match_reasons = []

        # 기존: 품명 직접 매칭
        if v_name and s_name:
            if v_name in s_name or s_name in v_name:
                score += 40
                match_reasons.append("품명 포함 일치")
            common = _longest_common_substring(v_name, s_name)
            if len(common) >= 3:
                score += min(len(common) * 3, 30)
                if len(common) >= 5:
                    match_reasons.append(f"공통문자열({common})")

        if v_model and s_name and v_model in s_name:
            score += 20
            match_reasons.append("모델코드 포함")
        if v_name and s_code and v_name in s_code.replace("-", ""):
            score += 20

        # 기존: 수량/금액 참고
        if v_qty and s_qty and v_qty == s_qty:
            score += 10  # 15→10 (참고용으로 감소)
            match_reasons.append(f"수량 일치({int(v_qty)}개)")
        if v_amount and s_amount and abs(v_amount - s_amount) / max(abs(v_amount), 1) < 0.1:
            score += 10  # 15→10
            match_reasons.append("금액 유사")

        # ★ 신규: 제품 속성 크로스매칭
        if v_attrs:
            s_attrs = _extract_product_attributes(s_raw_name)
            attr_score, attr_reasons = _compute_attribute_similarity(v_attrs, s_attrs)
            if attr_score > 0:
                score += min(attr_score, 50)  # 속성 최대 50점 부여
                match_reasons.extend(attr_reasons)

        if score > 0:
            scored.append((score, i, sale, match_reasons))

    scored.sort(key=lambda x: x[0], reverse=True)

    candidates = []
    for score, idx, sale, reasons in scored[:max_candidates]:
        reason_str = ", ".join(reasons) if reasons else f"규칙 기반 매칭"
        candidates.append({
            "sales_item": sale,
            "confidence": min(score / 120, 1.0),  # 총 만점 ~120→1.0
            "reason": f"{reason_str} (점수: {score})",
            "product_code": _get_field(sale, "prod_cd", "품목코드", "product_code", default=""),
            "product_name": _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default=""),
            "qty": _get_field(sale, "qty", "수량", default=""),
            "amount": _get_field(sale, "total", "합계", default=""),
            "date": _get_field(sale, "date", "월/일", "연/월/일", default=""),
        })

    return candidates


# ============================================================
# 유틸리티
# ============================================================

# ============================================================
# 제품 속성 추출 (크로스매칭용)
# ============================================================

# 색상 동의어 매핑 (정규화)
COLOR_SYNONYMS = {
    "황색": "노란색", "옐로우": "노란색", "yellow": "노란색", "YL": "노란색",
    "적색": "빨간색", "레드": "빨간색", "red": "빨간색", "RD": "빨간색",
    "청색": "파란색", "블루": "파란색", "blue": "파란색", "BL": "파란색",
    "녹색": "초록색", "그린": "초록색", "green": "초록색", "GR": "초록색",
    "백색": "흰색", "화이트": "흰색", "white": "흰색", "WH": "흰색",
    "흑색": "검정색", "블랙": "검정색", "black": "검정색", "BK": "검정색",
    "회색": "회색", "그레이": "회색", "gray": "회색", "grey": "회색",
    "은색": "실버", "실버": "실버", "silver": "실버",
    "투명": "투명", "클리어": "투명", "clear": "투명",
    "핑크": "핑크", "pink": "핑크",
    "오렌지": "오렌지", "주황": "오렌지", "orange": "오렌지",
}

# 카테고리 키워드 → 정규화된 카테고리
CATEGORY_KEYWORDS = {
    # 케이블류
    "HDMI": "HDMI케이블", "DP": "DP케이블", "DISPLAYPORT": "DP케이블",
    "USB": "USB케이블", "LAN": "LAN케이블", "UTP": "LAN케이블", "STP": "LAN케이블",
    "CAT5": "LAN케이블", "CAT5E": "LAN케이블", "CAT6": "LAN케이블", "CAT6A": "LAN케이블",
    "CAT7": "LAN케이블", "CAT8": "LAN케이블", "RJ45": "LAN케이블",
    "DVI": "DVI케이블", "VGA": "VGA케이블", "RGB": "VGA케이블",
    "광케이블": "광케이블", "OPTICAL": "광케이블", "TOSLINK": "광케이블",
    "전원케이블": "전원케이블", "파워케이블": "전원케이블",
    "AUX": "오디오케이블", "3.5MM": "오디오케이블", "RCA": "오디오케이블",
    "SATA": "SATA케이블", "타입C": "USB케이블", "TYPE-C": "USB케이블", "TYPEC": "USB케이블",
    # 허브/선택기/분배기
    "허브": "허브", "HUB": "허브",
    "선택기": "선택기", "SWITCH": "선택기", "셀렉터": "선택기", "SELECTOR": "선택기",
    "분배기": "분배기", "SPLITTER": "분배기", "스플리터": "분배기",
    "KVM": "KVM스위치",
    # 네트워크 장비
    "스위칭허브": "스위칭허브", "L2": "스위칭허브", "L3": "스위칭허브",
    "공유기": "공유기", "라우터": "공유기", "ROUTER": "공유기",
    "AP": "AP", "무선AP": "AP", "액세스포인트": "AP",
    # 충전/전원
    "충전기": "충전기", "어댑터": "어댑터", "ADAPTER": "어댑터",
    "충전독": "충전독", "DOCK": "충전독", "도킹": "충전독",
    "멀티탭": "멀티탭", "전원분배": "멀티탭",
    # 변환
    "컨버터": "컨버터", "CONVERTER": "컨버터", "변환": "컨버터",
    "젠더": "젠더", "GENDER": "젠더",
    "연장": "연장", "EXTENSION": "연장", "EXTENDER": "연장",
    "리피터": "리피터", "REPEATER": "리피터",
    # 부자재
    "부트": "부트", "BOOT": "부트", "보호캡": "부트",
    "타이": "케이블타이", "클립": "클립", "홀더": "홀더",
    "몰드": "몰드", "몰딩": "몰드", "배선": "몰드",
    # 영상장비
    "안테나": "안테나", "TV": "안테나",
    "캡쳐": "캡쳐보드", "CAPTURE": "캡쳐보드",
    # 저장장치
    "SSD": "SSD", "HDD": "HDD", "외장하드": "외장HDD", "USB메모리": "USB메모리",
    # 기타
    "마우스": "마우스", "키보드": "키보드", "모니터": "모니터",
    "케이스": "케이스", "CASE": "케이스",
    "거치대": "거치대", "스탠드": "거치대", "STAND": "거치대",
    "캐리어저울": "캐리어저울", "저울": "캐리어저울",
}

def _extract_product_attributes(name: str) -> dict:
    """제품명에서 매칭에 유용한 속성들을 추출한다.

    네트워크/케이블/영상기기/전산용품 도메인 특화.
    거래처 제품명과 우리 제품명이 다를 때 특성 기반 크로스매칭에 사용.

    Returns:
        {
            "category": "HDMI케이블",
            "length": "2M",
            "color": "노란색",
            "ports": 4,
            "version": "2.1",
            "speed": "10G",
            "watt": "65W",
            "resolution": "4K",
            "raw_specs": ["4K", "120HZ", "2M"],
            "keywords": ["hdmi", "케이블", "4k"]
        }
    """
    if not name:
        return {}

    upper = name.upper().strip()
    attrs = {}

    # 1. 카테고리 추출
    for kw, cat in CATEGORY_KEYWORDS.items():
        if kw.upper() in upper:
            attrs["category"] = cat
            break

    # 2. 길이 추출 (케이블류 핵심 스펙)
    length_m = re.search(r'(\d+(?:\.\d+)?)\s*[Mm미](?:터)?', name)
    if length_m:
        attrs["length"] = f"{length_m.group(1)}M"
    else:
        # "10CM", "50CM" 등
        length_cm = re.search(r'(\d+)\s*[Cc][Mm]', name)
        if length_cm:
            attrs["length"] = f"{length_cm.group(1)}CM"

    # 3. 색상 추출
    name_lower = name.lower()
    for color_kw, normalized in COLOR_SYNONYMS.items():
        if color_kw.lower() in name_lower:
            attrs["color"] = normalized
            break

    # 4. 포트 수 추출
    ports_m = re.search(r'(\d+)\s*(?:포트|PORT|P\b|구)', upper)
    if ports_m:
        attrs["ports"] = int(ports_m.group(1))
    # "1:2", "1:4" 등 분배비율도 포트수 힌트
    ratio_m = re.search(r'1\s*[:\s]\s*(\d+)', name)
    if ratio_m and "ports" not in attrs:
        attrs["ports"] = int(ratio_m.group(1))

    # 5. 버전 추출 (USB, HDMI 등)
    ver_m = re.search(r'(\d+\.\d+)\s*(?:VER|V\b)?', upper)
    if ver_m:
        v = ver_m.group(1)
        if v in ("2.0", "2.1", "3.0", "3.1", "3.2", "1.4", "1.2"):
            attrs["version"] = v

    # 6. 전송속도
    speed_m = re.search(r'(\d+)\s*(?:GBPS|GBE|G\b)', upper)
    if speed_m:
        attrs["speed"] = f"{speed_m.group(1)}G"
    speed_m2 = re.search(r'(\d+)\s*(?:MBPS)', upper)
    if speed_m2 and "speed" not in attrs:
        attrs["speed"] = f"{speed_m2.group(1)}M"

    # 7. 와트 (충전기류)
    watt_m = re.search(r'(\d+)\s*[Ww](?:ATT)?', name)
    if watt_m:
        attrs["watt"] = f"{watt_m.group(1)}W"

    # 8. 해상도
    for res in ["8K", "4K", "2K", "1080P", "FHD", "QHD", "UHD"]:
        if res in upper:
            attrs["resolution"] = res
            break

    # 9. 주파수 (영상기기)
    hz_m = re.search(r'(\d+)\s*HZ', upper)
    if hz_m:
        attrs["hz"] = f"{hz_m.group(1)}HZ"

    # 10. 핵심 키워드 목록 (소문자, 공백/특수문자 제거)
    keywords = set()
    tokens = re.split(r'[\s,/\-_()（）\[\]]+', upper)
    for t in tokens:
        t = t.strip()
        if len(t) >= 2:
            keywords.add(t.lower())
    attrs["keywords"] = list(keywords)

    return attrs


def _compute_attribute_similarity(attrs1: dict, attrs2: dict) -> tuple[float, list[str]]:
    """두 제품 속성 간 유사도 계산 (0~100점, 매칭근거 리스트)

    카테고리 일치가 필수 전제조건. 카테고리가 다르면 0점.
    """
    if not attrs1 or not attrs2:
        return 0.0, []

    score = 0.0
    reasons = []

    # 카테고리 일치 (필수 — 불일치 시 0점)
    cat1 = attrs1.get("category", "")
    cat2 = attrs2.get("category", "")
    if cat1 and cat2:
        if cat1 == cat2:
            score += 30
            reasons.append(f"카테고리 일치({cat1})")
        else:
            return 0.0, []  # 카테고리 불일치 → 매칭 불가
    elif not cat1 and not cat2:
        pass  # 양쪽 다 카테고리 미식별 → 다른 속성으로 판단
    else:
        return 0.0, []  # 한쪽만 카테고리 있는 경우 → 매칭 불가

    # 길이 일치 (케이블류 결정적 스펙)
    len1 = attrs1.get("length", "")
    len2 = attrs2.get("length", "")
    if len1 and len2:
        if len1 == len2:
            score += 25
            reasons.append(f"길이 일치({len1})")
        else:
            score -= 20  # 길이 불일치 → 다른 제품일 가능성 높음
            reasons.append(f"길이 불일치({len1}≠{len2})")

    # 색상 일치
    col1 = attrs1.get("color", "")
    col2 = attrs2.get("color", "")
    if col1 and col2:
        if col1 == col2:
            score += 15
            reasons.append(f"색상 일치({col1})")
        else:
            score -= 10
            reasons.append(f"색상 불일치({col1}≠{col2})")

    # 포트 수 일치
    p1 = attrs1.get("ports")
    p2 = attrs2.get("ports")
    if p1 and p2:
        if p1 == p2:
            score += 20
            reasons.append(f"포트수 일치({p1}포트)")
        else:
            score -= 15
            reasons.append(f"포트수 불일치({p1}≠{p2})")

    # 버전 일치
    v1 = attrs1.get("version", "")
    v2 = attrs2.get("version", "")
    if v1 and v2:
        if v1 == v2:
            score += 10
            reasons.append(f"버전 일치({v1})")
        else:
            score -= 5

    # 해상도 일치
    r1 = attrs1.get("resolution", "")
    r2 = attrs2.get("resolution", "")
    if r1 and r2:
        if r1 == r2:
            score += 10
            reasons.append(f"해상도 일치({r1})")

    # 속도 일치
    sp1 = attrs1.get("speed", "")
    sp2 = attrs2.get("speed", "")
    if sp1 and sp2:
        if sp1 == sp2:
            score += 10
            reasons.append(f"속도 일치({sp1})")

    # 와트 일치
    w1 = attrs1.get("watt", "")
    w2 = attrs2.get("watt", "")
    if w1 and w2:
        if w1 == w2:
            score += 10
            reasons.append(f"와트 일치({w1})")

    # 주파수 일치
    hz1 = attrs1.get("hz", "")
    hz2 = attrs2.get("hz", "")
    if hz1 and hz2:
        if hz1 == hz2:
            score += 5
            reasons.append(f"주파수 일치({hz1})")

    return max(score, 0.0), reasons


def _normalize_product_name(name: str) -> str:
    name = name.upper().strip()
    brands = ["LANSTAR", "LANSTART", "NEXI", "NEXT", "LS전선", "HPE", "CISCO",
              "NETGEAR", "ARUBA", "EFM", "MBF", "넷기어", "시스코"]
    for brand in brands:
        name = name.replace(brand.upper(), "").strip()
    name = re.sub(r'[\s\-_/\\()\[\]]+', '', name)
    return name


def _extract_model_codes(name: str) -> list[str]:
    """품명에서 모델코드 후보를 추출 (예: R8R49A, JL806A, GS516PP, CBS220-16T 등)"""
    if not name:
        return []
    # 영문+숫자 조합 모델코드 패턴 (3글자 이상)
    codes = re.findall(r'[A-Za-z][A-Za-z0-9]{2,}[0-9][A-Za-z0-9]*', name)
    # 숫자+영문 조합 패턴
    codes += re.findall(r'[0-9]+[A-Za-z][A-Za-z0-9]{2,}', name)
    # 하이픈 포함 모델코드 (CBS220-16T 등)
    codes += re.findall(r'[A-Za-z0-9]{3,}-[A-Za-z0-9]+', name)
    # 중복 제거, 대문자 변환, 짧은 것 제외
    seen = set()
    result = []
    for c in codes:
        cu = c.upper()
        if cu not in seen and len(cu) >= 3:
            seen.add(cu)
            result.append(cu)
    return result


def _longest_common_substring(s1: str, s2: str) -> str:
    if not s1 or not s2:
        return ""
    m, n = len(s1), len(s2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    max_len = 0
    end_idx = 0
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s1[i-1] == s2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
                if dp[i][j] > max_len:
                    max_len = dp[i][j]
                    end_idx = i
    return s1[end_idx - max_len:end_idx]


def _safe_num(val) -> float:
    try:
        if isinstance(val, (int, float)):
            return float(val)
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _compute_match_score(v_item: dict, e_item: dict) -> tuple[int, list[str]]:
    """단일 거래처원장 항목과 ERP 항목 간 매칭 스코어 계산"""
    v_name = _normalize_product_name(v_item.get("product_name", ""))
    v_model = _normalize_product_name(v_item.get("model_name", ""))
    v_raw_name = v_item.get("product_name", "")
    v_model_codes = _extract_model_codes(v_raw_name)
    if v_item.get("model_name"):
        v_model_codes += _extract_model_codes(v_item["model_name"])
    v_qty = _safe_num(v_item.get("qty", 0))
    v_amount = _safe_num(v_item.get("amount", 0))
    v_date = _parse_date(v_item.get("date", ""))

    e_raw_name = _get_field(e_item, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default="")
    e_name = _normalize_product_name(e_raw_name)
    e_code = _get_field(e_item, "prod_cd", "품목코드", "product_code", default="").upper()
    e_qty = _safe_num(_get_field(e_item, "qty", "수량", default=0))
    e_amount = _safe_num(_get_field(e_item, "total", "합계", default=0))
    e_date_str = _get_field(e_item, "date", "월/일", "연/월/일", default="")
    e_date = _parse_date(e_date_str)

    score = 0
    reasons = []

    # ── 날짜 매칭 (30점) ──
    if v_date and e_date:
        day_diff = abs((v_date - e_date).days)
        if day_diff == 0:
            score += 30
            reasons.append("날짜 일치")
        elif day_diff <= 3:
            score += 15
            reasons.append(f"날짜 ±{day_diff}일")

    # ── 금액 매칭 (35점) ──
    if v_amount and e_amount:
        amt_diff = abs(v_amount - e_amount)
        if amt_diff <= 1:
            score += 35
            reasons.append("금액 일치")
        elif amt_diff / max(abs(v_amount), 1) < 0.05:
            score += 25
            reasons.append(f"금액 유사 (차이 {int(amt_diff)}원)")

    # ── 수량 매칭 (20점) — 부호 일치 필수 ──
    if v_qty and e_qty:
        if v_qty == e_qty:
            score += 20
            reasons.append("수량 일치")

    # ── 모델코드 매칭 (20점, 강화) ──
    model_matched = False
    if v_model_codes:
        e_full = (e_raw_name + " " + e_code).upper()
        for mc in v_model_codes:
            if mc in e_full:
                score += 20
                reasons.append(f"모델코드 일치 ({mc})")
                model_matched = True
                break

    # ── 품명 유사도 (15점, 보조) ──
    if not model_matched and v_name and e_name:
        if v_name in e_name or e_name in v_name:
            score += 15
            reasons.append("품명 포함")
        elif v_model and v_model in e_name:
            score += 10
            reasons.append("모델명 포함")
        else:
            common = _longest_common_substring(v_name, e_name)
            if len(common) >= 4:
                score += min(len(common) * 2, 10)
                reasons.append(f"품명 유사 ({common})")

    if not model_matched and v_name and e_code and v_name in e_code.replace("-", ""):
        score += 10
        reasons.append("품명↔품목코드 유사")

    return score, reasons


def _rule_based_match(vendor_items: list[dict],
                       erp_items: list[dict]) -> list[dict]:
    """규칙 기반 품목 매칭 — 글로벌 최적화 (2-pass)

    1차: 고신뢰 매칭 (80점+) — 날짜+금액+수량+모델코드 모두 일치하는 건 우선 확보
    2차: 저신뢰 매칭 (50점+) — 나머지 항목 매칭
    → 이렇게 하면 덜 정확한 항목이 정확한 후보를 선점하는 문제 방지
    """
    # 모든 가능한 매칭 스코어 계산
    all_scores = []
    for v_idx, v_item in enumerate(vendor_items):
        for e_idx, e_item in enumerate(erp_items):
            score, reasons = _compute_match_score(v_item, e_item)
            if score >= 50:
                all_scores.append((score, v_idx, e_idx, reasons))

    # 점수 높은 순 정렬 → 최고 점수 매칭부터 확정
    all_scores.sort(key=lambda x: -x[0])

    used_vendor = set()
    used_erp = set()
    match_map = {}  # v_idx → (e_idx, score, reasons)

    for score, v_idx, e_idx, reasons in all_scores:
        if v_idx in used_vendor or e_idx in used_erp:
            continue
        used_vendor.add(v_idx)
        used_erp.add(e_idx)
        match_map[v_idx] = (e_idx, score, reasons)

    # 결과 생성
    results = []
    for v_idx, v_item in enumerate(vendor_items):
        if v_idx in match_map:
            e_idx, score, reasons = match_map[v_idx]
            e_item = erp_items[e_idx]
            match_type = "exact" if score >= 80 else "similar"
            results.append({
                "vendor_item": v_item,
                "erp_match": e_item,
                "match_type": match_type,
                "confidence": min(score / 100, 1.0),
                "reason": ", ".join(reasons),
            })
        else:
            # 최고 점수 기록 (디버그용)
            best = 0
            for score, vi, ei, _ in all_scores:
                if vi == v_idx:
                    best = max(best, score)
                    break
            results.append({
                "vendor_item": v_item,
                "erp_match": None,
                "match_type": "unmatched",
                "confidence": 0.0,
                "reason": f"매칭 실패 (최고점 {best}점)" if best > 0 else "매칭되는 ERP 항목 없음",
            })

    return results
