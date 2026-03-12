"""
판매 에이전트 Python 계산 엔진
- RFM 점수 산출 (5분위수)
- ABC 등급 분류 (파레토)
- 수요 예측 (이동평균 + 지수평활)
- 안전재고 / ROP 산출
- CLV / ACV 산출
- 트렌드 매칭 점수
"""

import math
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional


# ──────────────────────────────────────────────────────────
# 1. RFM 엔진
# ──────────────────────────────────────────────────────────

def _quintile_score(values: list, reverse: bool = False) -> dict:
    """값 리스트 → 5분위수 등급 매핑 (1~5)
    reverse=True이면 값이 작을수록 높은 등급 (Recency용)
    """
    if not values:
        return {}
    sorted_vals = sorted(set(values))
    n = len(sorted_vals)
    if n == 0:
        return {}

    # 5분위 경계 계산
    boundaries = []
    for q in [0.2, 0.4, 0.6, 0.8]:
        idx = int(n * q)
        if idx >= n:
            idx = n - 1
        boundaries.append(sorted_vals[idx])

    def score(v):
        grade = 1
        for b in boundaries:
            if v > b:
                grade += 1
        return grade if not reverse else (6 - grade)

    return {v: score(v) for v in set(values)}


def calculate_rfm(transactions: list, reference_date: Optional[str] = None) -> dict:
    """
    RFM 분석 수행.

    Args:
        transactions: [{"customer_name", "customer_code", "transaction_date", "total_amount", ...}]
        reference_date: 기준일 (YYYY-MM-DD). 없으면 데이터 최대일+1일

    Returns:
        {
            "rfm_results": [{customer_name, customer_code, R, F, M, score, segment, ...}],
            "segments": {segment_name: {count, total_amount, customers}},
            "churn_risk": [{customer_name, risk_level, reason}]
        }
    """
    if not transactions:
        return {"rfm_results": [], "segments": {}, "churn_risk": []}

    # 거래처별 집계
    cust_data = defaultdict(lambda: {
        "code": "", "dates": [], "amounts": [], "count": 0
    })
    for tx in transactions:
        cn = tx.get("customer_name", "")
        if not cn:
            continue
        cd = cust_data[cn]
        cd["code"] = tx.get("customer_code", "")
        td = tx.get("transaction_date", "")
        if td:
            cd["dates"].append(td)
        amt = tx.get("total_amount", 0) or 0
        cd["amounts"].append(amt)
        cd["count"] += 1

    # 기준일 결정
    all_dates = []
    for cd in cust_data.values():
        all_dates.extend(cd["dates"])
    if reference_date:
        ref = datetime.strptime(reference_date, "%Y-%m-%d")
    elif all_dates:
        ref = datetime.strptime(max(all_dates)[:10], "%Y-%m-%d") + timedelta(days=1)
    else:
        ref = datetime.now()

    # R, F, M 원시값 계산
    raw_r = {}  # customer -> 경과일
    raw_f = {}  # customer -> 거래횟수
    raw_m = {}  # customer -> 총액
    for cn, cd in cust_data.items():
        if cd["dates"]:
            last = max(cd["dates"])[:10]
            days = (ref - datetime.strptime(last, "%Y-%m-%d")).days
            raw_r[cn] = max(days, 0)
        else:
            raw_r[cn] = 9999
        raw_f[cn] = cd["count"]
        raw_m[cn] = sum(cd["amounts"])

    # 5분위수 등급 매핑
    r_scores = _quintile_score(list(raw_r.values()), reverse=True)  # 적을수록 높은 등급
    f_scores = _quintile_score(list(raw_f.values()), reverse=False)
    m_scores = _quintile_score(list(raw_m.values()), reverse=False)

    # 세그먼트 분류 규칙
    def classify_segment(r, f, m):
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        if r >= 3 and f >= 3 and m >= 3:
            return "Loyal"
        if r >= 3 and f <= 3:
            return "Potential"
        if r <= 2 and f >= 3:
            return "At Risk"
        return "Hibernating"

    # 결과 생성
    rfm_results = []
    for cn, cd in cust_data.items():
        r_val = raw_r[cn]
        f_val = raw_f[cn]
        m_val = raw_m[cn]
        R = r_scores.get(r_val, 3)
        F = f_scores.get(f_val, 3)
        M = m_scores.get(m_val, 3)
        score = R * 100 + F * 10 + M
        segment = classify_segment(R, F, M)
        last_date = max(cd["dates"])[:10] if cd["dates"] else ""
        rfm_results.append({
            "customer_name": cn,
            "customer_code": cd["code"],
            "R": R, "F": F, "M": M,
            "score": score,
            "segment": segment,
            "total_amount": round(m_val),
            "frequency": f_val,
            "recency_days": r_val,
            "last_date": last_date,
        })

    # 세그먼트 집계
    segments = {}
    for seg_name in ["Champions", "Loyal", "Potential", "At Risk", "Hibernating"]:
        members = [r for r in rfm_results if r["segment"] == seg_name]
        segments[seg_name] = {
            "count": len(members),
            "total_amount": sum(m["total_amount"] for m in members),
            "customers": [m["customer_name"] for m in members],
        }

    # 이탈 위험 감지
    churn_risk = _detect_churn_risk(transactions, cust_data, ref)

    return {
        "rfm_results": sorted(rfm_results, key=lambda x: -x["total_amount"]),
        "segments": segments,
        "churn_risk": churn_risk,
    }


def _detect_churn_risk(transactions, cust_data, ref_date) -> list:
    """이탈 위험 거래처 감지"""
    risks = []
    three_months_ago = ref_date - timedelta(days=90)
    six_months_ago = ref_date - timedelta(days=180)

    for cn, cd in cust_data.items():
        dates = cd["dates"]
        if not dates:
            continue
        last = max(dates)[:10]
        last_dt = datetime.strptime(last, "%Y-%m-%d")

        # 최근/이전 분기 금액 비교
        recent_amt = 0
        prev_amt = 0
        for tx in transactions:
            if tx.get("customer_name") != cn:
                continue
            td = tx.get("transaction_date", "")[:10]
            if not td:
                continue
            tdt = datetime.strptime(td, "%Y-%m-%d")
            amt = tx.get("total_amount", 0) or 0
            if tdt >= three_months_ago:
                recent_amt += amt
            elif tdt >= six_months_ago:
                prev_amt += amt

        # 위험 판정
        no_recent = last_dt < three_months_ago
        if no_recent and prev_amt > 0:
            decline = (prev_amt - recent_amt) / prev_amt if prev_amt else 0
            if decline >= 0.5:
                risks.append({
                    "customer_name": cn,
                    "risk_level": "높음",
                    "reason": f"최근 3개월 거래 없음, 이전 대비 {decline*100:.0f}% 감소",
                    "last_date": last,
                    "recent_amount": round(recent_amt),
                    "prev_amount": round(prev_amt),
                })
            elif decline >= 0.3:
                risks.append({
                    "customer_name": cn,
                    "risk_level": "주의",
                    "reason": f"최근 거래 감소, 이전 대비 {decline*100:.0f}% 감소",
                    "last_date": last,
                    "recent_amount": round(recent_amt),
                    "prev_amount": round(prev_amt),
                })
        elif no_recent and cd["count"] >= 3:
            risks.append({
                "customer_name": cn,
                "risk_level": "주의",
                "reason": f"최근 3개월 거래 없음 (과거 {cd['count']}회 거래)",
                "last_date": last,
                "recent_amount": 0,
                "prev_amount": round(prev_amt),
            })

    return sorted(risks, key=lambda x: 0 if x["risk_level"] == "높음" else 1)


# ──────────────────────────────────────────────────────────
# 2. ABC 등급 분류기
# ──────────────────────────────────────────────────────────

def calculate_abc(transactions: list) -> dict:
    """
    ABC 등급 분류 (파레토 법칙 기반)

    Returns:
        {
            "abc_results": [{product_name, product_code, category, total_amount, total_qty, cumulative_pct, grade}],
            "grade_summary": {A: {count, amount, pct}, B: {...}, C: {...}}
        }
    """
    if not transactions:
        return {"abc_results": [], "grade_summary": {}}

    # 품목별 집계
    prod_data = defaultdict(lambda: {"code": "", "category": "", "amount": 0, "qty": 0})
    for tx in transactions:
        pn = tx.get("product_name", "")
        if not pn:
            continue
        pd = prod_data[pn]
        pd["code"] = tx.get("product_code", "")
        pd["category"] = tx.get("category", "") or "미분류"
        pd["amount"] += tx.get("total_amount", 0) or 0
        pd["qty"] += tx.get("quantity", 0) or 0

    # 매출 내림차순 정렬
    sorted_products = sorted(prod_data.items(), key=lambda x: -x[1]["amount"])
    total_amount = sum(pd["amount"] for _, pd in sorted_products)
    if total_amount == 0:
        return {"abc_results": [], "grade_summary": {}}

    # 누적 비율 → ABC 등급
    abc_results = []
    cumulative = 0
    for pn, pd in sorted_products:
        cumulative += pd["amount"]
        cum_pct = (cumulative / total_amount) * 100

        if cum_pct <= 80:
            grade = "A"
        elif cum_pct <= 95:
            grade = "B"
        else:
            grade = "C"

        abc_results.append({
            "product_name": pn,
            "product_code": pd["code"],
            "category": pd["category"],
            "total_amount": round(pd["amount"]),
            "total_qty": pd["qty"],
            "cumulative_pct": round(cum_pct, 1),
            "grade": grade,
        })

    # 등급별 요약
    grade_summary = {}
    for g in ["A", "B", "C"]:
        members = [r for r in abc_results if r["grade"] == g]
        g_amount = sum(m["total_amount"] for m in members)
        grade_summary[g] = {
            "count": len(members),
            "amount": round(g_amount),
            "pct": round((g_amount / total_amount) * 100, 1) if total_amount else 0,
        }

    return {"abc_results": abc_results, "grade_summary": grade_summary}


# ──────────────────────────────────────────────────────────
# 3. 수요 예측 엔진
# ──────────────────────────────────────────────────────────

def calculate_forecast(transactions: list, forecast_months: int = 3) -> dict:
    """
    수요 예측: 이동평균(3개월) + 지수평활(alpha=0.3)

    Returns:
        {
            "forecast": [{product_name, current_monthly_avg, forecast_3m, trend, mape}],
            "monthly_totals": [{month, amount, count}]
        }
    """
    if not transactions:
        return {"forecast": [], "monthly_totals": []}

    # 월별/품목별 집계
    monthly_prod = defaultdict(lambda: defaultdict(float))
    monthly_total = defaultdict(lambda: {"amount": 0, "count": 0})

    for tx in transactions:
        td = tx.get("transaction_date", "")
        if len(td) < 7:
            continue
        month = td[:7]
        pn = tx.get("product_name", "")
        amt = tx.get("total_amount", 0) or 0
        if pn:
            monthly_prod[pn][month] += amt
        monthly_total[month]["amount"] += amt
        monthly_total[month]["count"] += 1

    # 전체 월 리스트 (정렬)
    all_months = sorted(monthly_total.keys())
    if len(all_months) < 2:
        return {
            "forecast": [],
            "monthly_totals": [{"month": m, "amount": round(monthly_total[m]["amount"]), "count": monthly_total[m]["count"]} for m in all_months]
        }

    # 품목별 예측 (상위 30개)
    prod_totals = defaultdict(float)
    for tx in transactions:
        pn = tx.get("product_name", "")
        if pn:
            prod_totals[pn] += tx.get("total_amount", 0) or 0
    top_products = sorted(prod_totals.items(), key=lambda x: -x[1])[:30]

    forecasts = []
    for pn, _ in top_products:
        series = [monthly_prod[pn].get(m, 0) for m in all_months]
        if sum(series) == 0:
            continue

        # 이동평균 (3개월)
        ma3 = _moving_average(series, 3)
        # 지수평활 (alpha=0.3)
        es = _exponential_smoothing(series, 0.3)

        # 예측: 이동평균과 지수평활의 평균
        last_ma = ma3[-1] if ma3 else 0
        last_es = es[-1] if es else 0
        base_forecast = (last_ma + last_es) / 2

        # 트렌드 판단
        if len(series) >= 3:
            recent_avg = sum(series[-3:]) / 3
            prev_avg = sum(series[-6:-3]) / 3 if len(series) >= 6 else sum(series[:len(series)//2]) / max(len(series)//2, 1)
            if prev_avg > 0:
                growth = (recent_avg - prev_avg) / prev_avg
                if growth > 0.1:
                    trend = "증가"
                elif growth < -0.1:
                    trend = "감소"
                else:
                    trend = "유지"
            else:
                trend = "증가" if recent_avg > 0 else "유지"
        else:
            trend = "유지"

        # MAPE 계산 (마지막 3개월 대상)
        mape = _calculate_mape(series, ma3)

        # 향후 3개월 예측
        forecast_3m = []
        for i in range(forecast_months):
            f = base_forecast * (1 + (0.02 * i if trend == "증가" else -0.02 * i if trend == "감소" else 0))
            forecast_3m.append(round(max(f, 0)))

        monthly_avg = sum(series) / len(series) if series else 0
        forecasts.append({
            "product_name": pn,
            "current_monthly_avg": round(monthly_avg),
            "forecast_3m": forecast_3m,
            "trend": trend,
            "mape": round(mape, 1) if mape is not None else None,
        })

    monthly_totals = [
        {"month": m, "amount": round(monthly_total[m]["amount"]), "count": monthly_total[m]["count"]}
        for m in all_months
    ]

    return {"forecast": forecasts, "monthly_totals": monthly_totals}


def _moving_average(series: list, window: int) -> list:
    """이동평균"""
    if len(series) < window:
        return [sum(series) / len(series)] if series else [0]
    result = []
    for i in range(len(series)):
        if i < window - 1:
            result.append(sum(series[:i+1]) / (i+1))
        else:
            result.append(sum(series[i-window+1:i+1]) / window)
    return result


def _exponential_smoothing(series: list, alpha: float = 0.3) -> list:
    """지수평활법"""
    if not series:
        return [0]
    result = [series[0]]
    for i in range(1, len(series)):
        result.append(alpha * series[i] + (1 - alpha) * result[-1])
    return result


def _calculate_mape(actual: list, predicted: list) -> Optional[float]:
    """MAPE (Mean Absolute Percentage Error)"""
    if not actual or not predicted:
        return None
    n = min(len(actual), len(predicted))
    errors = []
    for i in range(max(0, n - 3), n):  # 마지막 3개
        if actual[i] != 0:
            errors.append(abs(actual[i] - predicted[i]) / abs(actual[i]) * 100)
    return sum(errors) / len(errors) if errors else None


# ──────────────────────────────────────────────────────────
# 4. 안전재고 / ROP 산출
# ──────────────────────────────────────────────────────────

def calculate_safety_stock(transactions: list, lead_time_days: int = 14, service_level: float = 0.95) -> list:
    """
    안전재고 + 발주점(ROP) 산출

    Args:
        transactions: 거래 데이터
        lead_time_days: 리드타임 (일), 기본 14일
        service_level: 서비스 수준 (기본 95%)

    Returns:
        [{product_name, daily_avg, daily_std, safety_stock, rop, grade}]
    """
    # Z값 매핑
    z_map = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}
    z = z_map.get(service_level, 1.645)

    # 품목별 일별 판매량 집계
    prod_daily = defaultdict(lambda: defaultdict(float))
    for tx in transactions:
        pn = tx.get("product_name", "")
        td = tx.get("transaction_date", "")[:10]
        qty = tx.get("quantity", 0) or 0
        if pn and td:
            prod_daily[pn][td] += qty

    if not prod_daily:
        return []

    # 전체 기간 계산
    all_dates = set()
    for daily in prod_daily.values():
        all_dates.update(daily.keys())
    if not all_dates:
        return []
    total_days = max((datetime.strptime(max(all_dates), "%Y-%m-%d") -
                      datetime.strptime(min(all_dates), "%Y-%m-%d")).days, 1)

    results = []
    for pn, daily in prod_daily.items():
        # 일별 수요 통계
        daily_values = [daily.get(d, 0) for d in sorted(all_dates)]
        if not daily_values:
            continue
        avg_d = sum(daily_values) / len(daily_values)
        if avg_d == 0:
            continue
        variance = sum((v - avg_d) ** 2 for v in daily_values) / len(daily_values)
        std_d = math.sqrt(variance)

        # 안전재고 = Z × σ_d × √(L)
        safety = z * std_d * math.sqrt(lead_time_days)
        # ROP = 일 평균 수요 × 리드타임 + 안전재고
        rop = avg_d * lead_time_days + safety

        results.append({
            "product_name": pn,
            "daily_avg_qty": round(avg_d, 2),
            "daily_std_qty": round(std_d, 2),
            "safety_stock": round(safety),
            "rop": round(rop),
            "lead_time_days": lead_time_days,
        })

    return sorted(results, key=lambda x: -x["rop"])[:30]


# ──────────────────────────────────────────────────────────
# 5. CLV / ACV 산출
# ──────────────────────────────────────────────────────────

def calculate_clv_acv(transactions: list, expected_years: int = 3, discount_rate: float = 0.1) -> dict:
    """
    CLV/ACV 산출 + 티어 분류

    Returns:
        {
            "clv_results": [{customer_name, customer_code, annual_amount, frequency, clv, acv, tier}],
            "tier_summary": {Tier 1: {...}, Tier 2: {...}, Tier 3: {...}}
        }
    """
    if not transactions:
        return {"clv_results": [], "tier_summary": {}}

    # 분석 기간 계산
    dates = [tx["transaction_date"][:10] for tx in transactions if tx.get("transaction_date")]
    if not dates:
        return {"clv_results": [], "tier_summary": {}}
    min_date = datetime.strptime(min(dates), "%Y-%m-%d")
    max_date = datetime.strptime(max(dates), "%Y-%m-%d")
    period_years = max((max_date - min_date).days / 365, 0.5)

    # 거래처별 집계
    cust_data = defaultdict(lambda: {"code": "", "amount": 0, "count": 0})
    for tx in transactions:
        cn = tx.get("customer_name", "")
        if not cn:
            continue
        cd = cust_data[cn]
        cd["code"] = tx.get("customer_code", "")
        cd["amount"] += tx.get("total_amount", 0) or 0
        cd["count"] += 1

    results = []
    for cn, cd in cust_data.items():
        annual_amount = cd["amount"] / period_years
        annual_freq = cd["count"] / period_years
        avg_tx = cd["amount"] / cd["count"] if cd["count"] else 0

        # CLV = 평균거래금액 × 연간빈도 × 예상기간 / (1+할인율)^년
        clv = 0
        for yr in range(1, expected_years + 1):
            clv += (avg_tx * annual_freq) / ((1 + discount_rate) ** yr)

        # ACV = 연간 계약 가치
        acv = annual_amount

        results.append({
            "customer_name": cn,
            "customer_code": cd["code"],
            "annual_amount": round(annual_amount),
            "frequency": round(annual_freq, 1),
            "clv": round(clv),
            "acv": round(acv),
            "total_amount": round(cd["amount"]),
            "tier": "",  # 아래에서 설정
        })

    # CLV 기준 정렬 및 티어 분류
    results.sort(key=lambda x: -x["clv"])
    n = len(results)
    for i, r in enumerate(results):
        pct = (i + 1) / n
        if pct <= 0.1:
            r["tier"] = "Tier 1"
        elif pct <= 0.3:
            r["tier"] = "Tier 2"
        else:
            r["tier"] = "Tier 3"

    # 티어 요약
    tier_summary = {}
    for tier in ["Tier 1", "Tier 2", "Tier 3"]:
        members = [r for r in results if r["tier"] == tier]
        tier_summary[tier] = {
            "count": len(members),
            "total_clv": sum(m["clv"] for m in members),
            "total_acv": sum(m["acv"] for m in members),
            "customers": [m["customer_name"] for m in members],
        }

    return {"clv_results": results, "tier_summary": tier_summary}


# ──────────────────────────────────────────────────────────
# 6. 트렌드 매칭 엔진
# ──────────────────────────────────────────────────────────

MEGA_TRENDS = {
    "private_ai": {
        "name": "프라이빗 AI 인프라",
        "description": "기업 내부 AI 모델 학습/추론을 위한 GPU 서버, 고속 네트워크, 대용량 스토리지",
        "keywords": ["gpu", "ai", "딥러닝", "머신러닝", "nvidia", "인공지능", "a100", "h100",
                     "infiniband", "nvme", "100g", "고속", "hpc"],
        "growth_rate": 0.35,
    },
    "edge_computing": {
        "name": "엣지 컴퓨팅",
        "description": "공장, 매장, 지사 등 현장에 소규모 데이터 처리 인프라 배치",
        "keywords": ["엣지", "edge", "iot", "게이트웨이", "소형서버", "마이크로",
                     "산업용", "임베디드", "미니pc"],
        "growth_rate": 0.28,
    },
    "zero_trust": {
        "name": "제로 트러스트 보안",
        "description": "네트워크 경계 없는 보안 모델",
        "keywords": ["방화벽", "firewall", "ngfw", "nac", "ztna", "sase", "보안",
                     "fortinet", "fortigate", "paloalto", "ssl", "vpn", "ips", "ids"],
        "growth_rate": 0.22,
    },
    "sbom_compliance": {
        "name": "SBOM/공급망 보안",
        "description": "소프트웨어 부품 목록 관리, 공급망 투명성",
        "keywords": ["sbom", "공급망", "컴플라이언스", "감사", "규정", "보안감사"],
        "growth_rate": 0.40,
    },
    "haas_subscription": {
        "name": "HaaS 구독 모델",
        "description": "하드웨어를 월정액 서비스로 전환",
        "keywords": ["구독", "리스", "렌탈", "월정액", "as-a-service", "haas", "opex"],
        "growth_rate": 0.30,
    },
}


def calculate_trend_matching(transactions: list, customers: list = None) -> dict:
    """
    거래처-트렌드 매칭 분석

    Returns:
        {
            "trend_matching": [{customer_name, top_trends, opportunity_value}],
            "new_opportunities": [{trend, target_customers, estimated_market, entry_difficulty}],
            "growth_scenarios": {current_annual, conservative, moderate, aggressive}
        }
    """
    if not transactions:
        return {"trend_matching": [], "new_opportunities": [], "growth_scenarios": {}}

    # 분석 기간
    dates = [tx["transaction_date"][:10] for tx in transactions if tx.get("transaction_date")]
    if not dates:
        return {"trend_matching": [], "new_opportunities": [], "growth_scenarios": {}}
    period_years = max((datetime.strptime(max(dates), "%Y-%m-%d") -
                        datetime.strptime(min(dates), "%Y-%m-%d")).days / 365, 0.5)

    total_amount = sum(tx.get("total_amount", 0) or 0 for tx in transactions)
    annual_amount = total_amount / period_years

    # 거래처별 구매 품목
    cust_products = defaultdict(set)
    cust_amounts = defaultdict(float)
    for tx in transactions:
        cn = tx.get("customer_name", "")
        pn = tx.get("product_name", "").lower()
        if cn and pn:
            cust_products[cn].add(pn)
            cust_amounts[cn] += tx.get("total_amount", 0) or 0

    # 트렌드 매칭
    trend_results = []
    for cn in sorted(cust_amounts, key=lambda x: -cust_amounts[x])[:30]:
        products_str = " ".join(cust_products[cn])
        trends = []
        for trend_key, trend_info in MEGA_TRENDS.items():
            match_count = sum(1 for kw in trend_info["keywords"] if kw in products_str)
            if match_count > 0:
                score = min(match_count * 20 + int(trend_info["growth_rate"] * 100), 100)
                trends.append({
                    "trend": trend_info["name"],
                    "trend_key": trend_key,
                    "score": score,
                    "matched_keywords": match_count,
                    "reason": f"관련 품목 {match_count}개 매칭, 시장 성장률 {trend_info['growth_rate']*100:.0f}%",
                })
        trends.sort(key=lambda x: -x["score"])
        opp_value = round(cust_amounts[cn] * 0.2) if trends else 0

        trend_results.append({
            "customer_name": cn,
            "top_trends": trends[:3],
            "opportunity_value": opp_value,
            "current_amount": round(cust_amounts[cn]),
        })

    # 신규 시장 기회
    all_products_str = " ".join(p for prods in cust_products.values() for p in prods)
    new_opportunities = []
    for trend_key, trend_info in MEGA_TRENDS.items():
        match_count = sum(1 for kw in trend_info["keywords"] if kw in all_products_str)
        if match_count <= 1:
            difficulty = "높음" if match_count == 0 else "보통"
            target_custs = [cn for cn in cust_amounts if cust_amounts[cn] > annual_amount / len(cust_amounts)]
            new_opportunities.append({
                "trend": trend_info["name"],
                "trend_key": trend_key,
                "target_customers": target_custs[:10],
                "estimated_market": round(annual_amount * trend_info["growth_rate"]),
                "entry_difficulty": difficulty,
                "growth_rate": f"{trend_info['growth_rate']*100:.0f}%",
            })

    # 3년 성장 시나리오
    growth_scenarios = {
        "current_annual": round(annual_amount),
        "conservative": {
            "rate": 0.10,
            "year1": round(annual_amount * 1.10),
            "year2": round(annual_amount * 1.10 ** 2),
            "year3": round(annual_amount * 1.10 ** 3),
        },
        "moderate": {
            "rate": 0.20,
            "year1": round(annual_amount * 1.20),
            "year2": round(annual_amount * 1.20 ** 2),
            "year3": round(annual_amount * 1.20 ** 3),
        },
        "aggressive": {
            "rate": 0.35,
            "year1": round(annual_amount * 1.35),
            "year2": round(annual_amount * 1.35 ** 2),
            "year3": round(annual_amount * 1.35 ** 3),
        },
    }

    return {
        "trend_matching": trend_results,
        "new_opportunities": new_opportunities,
        "growth_scenarios": growth_scenarios,
    }
