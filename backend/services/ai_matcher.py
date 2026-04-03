"""AI 품목명 매칭 서비스 (Claude API)
거래처 원장의 상품명 ↔ ERP 구매현황/판매현황의 품명 매칭

핵심 로직:
1. 구매현황 매칭: 거래처 원장 vs ERP 구매현황 비교
2. 판매이력 확인: 누락 건에 대해 동일날짜 → ±1주 → ±2주 확장 검색
3. AI 후보군: 품명이 다를 수 있으므로 가능성 높은 순 5개 후보 추천
"""
import json
import re
from datetime import datetime, timedelta
from anthropic import AsyncAnthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ============================================================
# 1. 구매현황 매칭 (거래처 원장 vs ERP 구매현황)
# ============================================================

async def match_products_ai(vendor_items: list[dict],
                             erp_items: list[dict]) -> list[dict]:
    """AI를 사용하여 거래처 원장 항목과 ERP 구매현황 매칭"""
    if not client:
        return _rule_based_match(vendor_items, erp_items)

    vendor_summary = [
        {
            "idx": i,
            "name": item.get("product_name", ""),
            "model": item.get("model_name", ""),
            "category": item.get("product_category", ""),
            "qty": item.get("qty", 0),
            "amount": item.get("amount", 0),
            "date": item.get("date", ""),
        }
        for i, item in enumerate(vendor_items)
    ]

    erp_summary = [
        {
            "idx": i,
            "code": item.get("품목코드", item.get("product_code", "")),
            "name": item.get("품명 및 모델", item.get("product_name", "")),
            "qty": item.get("수량", item.get("qty", 0)),
            "amount": item.get("합계", item.get("total", 0)),
            "vendor": item.get("구매처명", item.get("vendor_name", "")),
            "date": item.get("월/일", item.get("date", "")),
        }
        for i, item in enumerate(erp_items)
    ]

    prompt = f"""당신은 ERP 매입 정산 전문가입니다. 거래처 원장(거래처가 보낸 매출 내역)과 우리 ERP 구매현황을 비교하여 매칭해야 합니다.

## 거래처 원장 항목 (거래처가 우리에게 판매한 것 = 우리의 매입)
{json.dumps(vendor_summary, ensure_ascii=False, indent=2)}

## ERP 구매현황 (우리가 입력한 매입 전표)
{json.dumps(erp_summary, ensure_ascii=False, indent=2)}

## 매칭 규칙
1. 품목명/모델명이 동일하거나 유사하면 매칭 (브랜드명 차이, 약어 차이 무시)
2. 수량과 금액이 일치하면 매칭 신뢰도 높음
3. 날짜가 같은 달이면 매칭 가능
4. 거래처 원장의 "매출"은 우리 입장에서 "매입"임을 주의
5. 색상명은 다를 수 있음 (황색=노란색=Yellow, 적색=빨간색=Red 등)

## 응답 형식 (반드시 JSON 배열로)
[
  {{
    "vendor_idx": 0,
    "erp_idx": 3,
    "match_type": "exact" | "similar" | "unmatched",
    "confidence": 0.95,
    "reason": "매칭 근거 설명"
  }}
]

매칭되지 않는 항목은 erp_idx를 null로 설정하세요.
JSON 배열만 출력하세요."""

    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}]
        )

        result_text = response.content[0].text.strip()
        if result_text.startswith("["):
            matches = json.loads(result_text)
        else:
            json_match = re.search(r'\[.*\]', result_text, re.DOTALL)
            if json_match:
                matches = json.loads(json_match.group())
            else:
                return _rule_based_match(vendor_items, erp_items)

        results = []
        for match in matches:
            v_idx = match.get("vendor_idx", 0)
            e_idx = match.get("erp_idx")
            results.append({
                "vendor_item": vendor_items[v_idx] if v_idx < len(vendor_items) else {},
                "erp_match": erp_items[e_idx] if e_idx is not None and e_idx < len(erp_items) else None,
                "match_type": match.get("match_type", "unmatched"),
                "confidence": match.get("confidence", 0.0),
                "reason": match.get("reason", ""),
            })
        return results

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"AI 매칭 실패, 규칙 기반으로 폴백: {e}")
        return _rule_based_match(vendor_items, erp_items)


# ============================================================
# 2. 판매이력 확인 — 날짜 확장 검색 + AI 후보군 5개
# ============================================================

def _parse_date(date_str: str, year: int = 2026) -> datetime | None:
    if not date_str:
        return None
    date_str = date_str.strip()
    date_str = re.sub(r'-\d+$', '', date_str)

    patterns = [
        (r'^(\d{2})\.(\d{2})$', lambda m: datetime(year, int(m[1]), int(m[2]))),
        (r'^(\d{2})/(\d{2})$', lambda m: datetime(year, int(m[1]), int(m[2]))),
        (r'^(\d{4})/(\d{2})/(\d{2})$', lambda m: datetime(int(m[1]), int(m[2]), int(m[3]))),
        (r'^(\d{4})(\d{2})(\d{2})$', lambda m: datetime(int(m[1]), int(m[2]), int(m[3]))),
    ]

    for pattern, factory in patterns:
        m = re.match(pattern, date_str)
        if m:
            try:
                return factory(m)
            except ValueError:
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
        item_date_str = item.get(date_field, "") or item.get("월/일", "")
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
            "code": sale.get("품목코드", sale.get("product_code", "")),
            "name": sale.get("품명 및 모델", sale.get("product_name", "")),
            "qty": sale.get("수량", sale.get("qty", "")),
            "amount": sale.get("합계", sale.get("total", "")),
            "date": sale.get("월/일", sale.get("date", "")),
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
                    "product_code": sale.get("품목코드", sale.get("product_code", "")),
                    "product_name": sale.get("품명 및 모델", sale.get("product_name", "")),
                    "qty": sale.get("수량", sale.get("qty", "")),
                    "amount": sale.get("합계", sale.get("total", "")),
                    "date": sale.get("월/일", sale.get("date", "")),
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
            sale.get("품명 및 모델", sale.get("product_name", ""))
        )
        s_code = sale.get("품목코드", sale.get("product_code", "")).upper()
        s_qty = _safe_num(sale.get("수량", sale.get("qty", 0)))
        s_amount = _safe_num(sale.get("합계", sale.get("total", 0)))

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
            "product_code": sale.get("품목코드", sale.get("product_code", "")),
            "product_name": sale.get("품명 및 모델", sale.get("product_name", "")),
            "qty": sale.get("수량", sale.get("qty", "")),
            "amount": sale.get("합계", sale.get("total", "")),
            "date": sale.get("월/일", sale.get("date", "")),
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


def _rule_based_match(vendor_items: list[dict],
                       erp_items: list[dict]) -> list[dict]:
    """규칙 기반 품목 매칭 (AI 없이)"""
    results = []
    used_erp_indices = set()

    for v_item in vendor_items:
        v_name = _normalize_product_name(v_item.get("product_name", ""))
        v_model = _normalize_product_name(v_item.get("model_name", ""))
        v_qty = v_item.get("qty", 0)
        v_amount = v_item.get("amount", 0)

        best_match = None
        best_score = 0

        for e_idx, e_item in enumerate(erp_items):
            if e_idx in used_erp_indices:
                continue

            e_name = _normalize_product_name(
                e_item.get("품명 및 모델", e_item.get("product_name", ""))
            )
            e_code = e_item.get("품목코드", e_item.get("product_code", "")).upper()
            e_qty = _safe_num(e_item.get("수량", e_item.get("qty", 0)))
            e_amount = _safe_num(e_item.get("합계", e_item.get("total", 0)))

            score = 0

            if v_name and e_name:
                if v_name in e_name or e_name in v_name:
                    score += 50
                elif v_model and v_model in e_name:
                    score += 40
                common = _longest_common_substring(v_name, e_name)
                if len(common) >= 4:
                    score += min(len(common) * 3, 25)

            if v_name and e_code and v_name in e_code.replace("-", ""):
                score += 30

            if v_qty and e_qty and v_qty == e_qty:
                score += 25
            if v_amount and e_amount:
                if abs(v_amount - e_amount) / max(v_amount, 1) < 0.05:
                    score += 25

            if score > best_score:
                best_score = score
                best_match = (e_idx, e_item, score)

        if best_match and best_match[2] >= 50:
            e_idx, e_item, score = best_match
            used_erp_indices.add(e_idx)
            match_type = "exact" if score >= 75 else "similar"
            results.append({
                "vendor_item": v_item,
                "erp_match": e_item,
                "match_type": match_type,
                "confidence": min(score / 100, 1.0),
                "reason": f"규칙 기반 매칭 (점수: {score}/100)",
            })
        else:
            results.append({
                "vendor_item": v_item,
                "erp_match": None,
                "match_type": "unmatched",
                "confidence": 0.0,
                "reason": "매칭되는 ERP 항목 없음 — 매입전표 누락 가능성",
            })

    return results
