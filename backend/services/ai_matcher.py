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
        if matched_sales:
            if v_qty and e_qty and v_qty != e_qty:
                v_diff = abs(total_sold_qty - v_qty)
                e_diff = abs(total_sold_qty - e_qty)
                if v_diff < e_diff:
                    verdict = f"판매 {total_sold_qty}개 → 거래처원장({v_qty}개)과 일치, 구매전표 수정 필요 가능성"
                    verdict_code = "vendor"
                elif e_diff < v_diff:
                    verdict = f"판매 {total_sold_qty}개 → 구매전표({e_qty}개)와 일치, 거래처원장 확인 필요"
                    verdict_code = "erp"
                else:
                    verdict = f"판매 {total_sold_qty}개 — 양쪽 모두 확인 필요"
                    verdict_code = "unknown"
            else:
                verdict = f"판매 {total_sold_qty}개 {total_sold_amt:,.0f}원 확인됨"
                verdict_code = "found"
        else:
            if erp_prod_cd:
                verdict = f"판매이력 없음 ({erp_prod_cd}) — 수동 확인 필요"
            else:
                verdict = "ERP 품목코드 없어 판매이력 검색 불가"
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

    sales_summary = []
    for i, sale in enumerate(sales_data[:200]):
        sales_summary.append({
            "idx": i,
            "code": _get_field(sale, "prod_cd", "품목코드", "product_code", default=""),
            "name": _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default=""),
            "qty": _get_field(sale, "qty", "수량", default=""),
            "amount": _get_field(sale, "total", "합계", default=""),
            "date": _get_field(sale, "date", "월/일", "연/월/일", default=""),
        })

    prompt = f"""거래처 원장에서 우리에게 판매한 품목인데, 우리 ERP 구매현황에 매입전표가 없습니다.
이 품목이 우리 ERP 판매현황에서 판매된 적이 있는지 찾아주세요.

## 찾아야 할 품목 (거래처가 우리에게 판매)
{json.dumps(vendor_info, ensure_ascii=False)}

## 우리 ERP 판매현황 (우리가 고객에게 판매한 것)
{json.dumps(sales_summary, ensure_ascii=False)}

## 중요 참고사항
- 품명이 완전히 다를 수 있습니다!
  예: 거래처 "mpc-721 황색 부트" = 우리 "노란색 부트" 또는 "황색부트"
  예: 거래처 "LS-6UTPD-10MG" = 우리 "CAT6 UTP 10M"
- 색상명 변환: 황색↔노란색↔Yellow, 적색↔빨간색↔Red, 청색↔파란색↔Blue 등
- 브랜드명이 다르거나 생략될 수 있음
- 모델명의 약어/변형에 주의
- 수량과 금액이 비슷하면 같은 품목일 가능성 높음

## 응답 형식 (JSON 배열, 가능성 높은 순서로 최대 5개)
[
  {{
    "sales_idx": 3,
    "confidence": 0.85,
    "reason": "품목명 유사, 수량/금액 일치"
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
    """규칙 기반 판매이력 후보 찾기 (AI 폴백)"""
    v_name = _normalize_product_name(vendor_item.get("product_name", ""))
    v_model = _normalize_product_name(vendor_item.get("model_name", ""))
    v_qty = vendor_item.get("qty", 0)
    v_amount = vendor_item.get("amount", 0)

    scored = []
    for i, sale in enumerate(sales_data):
        s_name = _normalize_product_name(
            _get_field(sale, "prod_name", "품명 및 모델", "품명 및 규격", "product_name", default="")
        )
        s_code = _get_field(sale, "prod_cd", "품목코드", "product_code", default="").upper()
        s_qty = _safe_num(_get_field(sale, "qty", "수량", default=0))
        s_amount = _safe_num(_get_field(sale, "total", "합계", default=0))

        score = 0

        if v_name and s_name:
            if v_name in s_name or s_name in v_name:
                score += 40
            common = _longest_common_substring(v_name, s_name)
            if len(common) >= 3:
                score += min(len(common) * 3, 30)

        if v_model and s_name and v_model in s_name:
            score += 20
        if v_name and s_code and v_name in s_code.replace("-", ""):
            score += 20

        if v_qty and s_qty and v_qty == s_qty:
            score += 15
        if v_amount and s_amount and abs(v_amount - s_amount) / max(v_amount, 1) < 0.1:
            score += 15

        if score > 0:
            scored.append((score, i, sale))

    scored.sort(key=lambda x: x[0], reverse=True)

    candidates = []
    for score, idx, sale in scored[:max_candidates]:
        candidates.append({
            "sales_item": sale,
            "confidence": min(score / 100, 1.0),
            "reason": f"규칙 기반 매칭 (점수: {score}/100)",
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
