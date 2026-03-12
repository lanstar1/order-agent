"""
Python 정량 분석 엔진 (Claude 호출 없이 즉시 실행)
- RFM 분석 (Mode A only)
- ABC 분류
- CLV/ACV 계산
- 수요 예측
- 안전재고
- 트렌드 매칭
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

# 분석 제외 키워드 (수량 많지만 금액 적은 부자재/소모품 + 비매출 항목)
EXCLUDE_KEYWORDS = [
    "커플러", "젠더", "키스톤잭", "먼지덮개", "부트", "커넥터",
    "콘넥터", "먼지", "boot", "아울렛", "매입", "업체직송", "택배",
]

def _get_model_name(tx: dict) -> str:
    """E열 모델명 우선, 없으면 품명 및 규격(D열) 사용"""
    mn = (tx.get("model_name") or "").strip()
    if mn:
        return mn
    return (tx.get("product_name") or "").strip()

def _is_excluded_tx(tx: dict) -> bool:
    """D열(품명 및 규격) 기준으로 제외 키워드 체크"""
    d_col = (tx.get("product_name") or "").lower()
    return any(kw in d_col for kw in EXCLUDE_KEYWORDS)

def _is_excluded(name: str) -> bool:
    """이름 문자열 기준 제외 키워드 체크 (하위 호환)"""
    lower = name.lower()
    return any(kw in lower for kw in EXCLUDE_KEYWORDS)


def calculate_rfm(txs: list[dict]) -> dict:
    """RFM 분석 (Mode A: 다중 거래처 비교)"""
    if not txs:
        return {"segments": {}, "scores": [], "churn_risk": []}

    cust_data = defaultdict(lambda: {"dates": [], "amounts": [], "count": 0})
    for tx in txs:
        cn = tx.get("customer_name", "")
        if not cn:
            continue
        cust_data[cn]["count"] += 1
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))
        cust_data[cn]["amounts"].append(amt)
        d = tx.get("transaction_date", "")
        if d:
            cust_data[cn]["dates"].append(d)

    today = datetime.now()
    scores = []
    for cust, d in cust_data.items():
        last_date = max(d["dates"]) if d["dates"] else ""
        recency_days = 999
        if last_date:
            try:
                ld = datetime.strptime(last_date[:10], "%Y-%m-%d")
                recency_days = (today - ld).days
            except Exception:
                pass

        total = sum(d["amounts"])
        freq = d["count"]
        r_score = 5 if recency_days < 30 else 4 if recency_days < 60 else 3 if recency_days < 90 else 2 if recency_days < 180 else 1
        f_score = min(5, max(1, freq // 5 + 1))
        m_score = 5 if total > 100_000_000 else 4 if total > 50_000_000 else 3 if total > 10_000_000 else 2 if total > 1_000_000 else 1

        segment = _rfm_segment(r_score, f_score, m_score)
        scores.append({
            "customer_name": cust,
            "recency_days": recency_days,
            "frequency": freq,
            "monetary": total,
            "r": r_score, "f": f_score, "m": m_score,
            "segment": segment,
        })

    # 세그먼트 집계
    segments = defaultdict(lambda: {"count": 0, "total_monetary": 0, "customers": []})
    churn_risk = []
    for s in scores:
        seg = s["segment"]
        segments[seg]["count"] += 1
        segments[seg]["total_monetary"] += s["monetary"]
        segments[seg]["customers"].append(s["customer_name"])
        if s["r"] <= 2 and s["f"] >= 3:
            churn_risk.append({"customer_name": s["customer_name"], "recency_days": s["recency_days"], "monetary": s["monetary"]})

    return {
        "segments": dict(segments),
        "scores": sorted(scores, key=lambda x: x["monetary"], reverse=True),
        "churn_risk": churn_risk,
    }


def _rfm_segment(r, f, m) -> str:
    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    if r >= 3 and f >= 3:
        return "Loyal"
    if r >= 4 and f <= 2:
        return "New Customers"
    if r <= 2 and f >= 3:
        return "At Risk"
    if r <= 2 and f <= 2 and m >= 3:
        return "Hibernating"
    return "Others"


def calculate_abc(txs: list[dict]) -> dict:
    """ABC 분류 (모델명 기반, D열 키워드 제외 필터링)"""
    product_sales = defaultdict(lambda: {"amount": 0, "qty": 0, "count": 0, "name": ""})
    for tx in txs:
        if _is_excluded_tx(tx):
            continue
        pn = _get_model_name(tx)
        if not pn:
            continue
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))
        qty = _safe_num(tx.get("quantity", 0))
        product_sales[pn]["amount"] += amt
        product_sales[pn]["qty"] += qty
        product_sales[pn]["count"] += 1
        product_sales[pn]["name"] = pn

    sorted_products = sorted(product_sales.items(), key=lambda x: x[1]["amount"], reverse=True)
    total = sum(v["amount"] for _, v in sorted_products)

    results = []
    cumulative = 0
    for pc, data in sorted_products:
        cumulative += data["amount"]
        pct = (cumulative / total * 100) if total else 0
        grade = "A" if pct <= 70 else "B" if pct <= 90 else "C"
        results.append({
            "product_code": pc,
            "product_name": data["name"],
            "amount": data["amount"],
            "quantity": data["qty"],
            "tx_count": data["count"],
            "cumulative_pct": round(pct, 1),
            "grade": grade,
        })

    grade_summary = {}
    for g in ["A", "B", "C"]:
        items = [r for r in results if r["grade"] == g]
        grade_summary[g] = {
            "count": len(items),
            "amount": sum(i["amount"] for i in items),
            "pct": round(sum(i["amount"] for i in items) / total * 100, 1) if total else 0,
        }

    return {"products": results, "grade_summary": grade_summary, "total_amount": total}


def calculate_forecast(txs: list[dict]) -> dict:
    """수요 예측 (월별 이동평균)"""
    monthly = defaultdict(lambda: {"amount": 0, "qty": 0, "count": 0})
    for tx in txs:
        d = tx.get("transaction_date", "")
        if not d:
            continue
        month_key = d[:7]  # YYYY-MM
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))
        qty = _safe_num(tx.get("quantity", 0))
        monthly[month_key]["amount"] += amt
        monthly[month_key]["qty"] += qty
        monthly[month_key]["count"] += 1

    sorted_months = sorted(monthly.keys())
    history = [{"month": m, **monthly[m]} for m in sorted_months]

    # 3개월 이동평균 예측
    forecast = []
    if len(history) >= 2:
        window = min(3, len(history))
        recent = history[-window:]
        avg_amount = sum(h["amount"] for h in recent) // window
        avg_qty = sum(h["qty"] for h in recent) // window
        for i in range(1, 4):
            try:
                last = datetime.strptime(sorted_months[-1] + "-01", "%Y-%m-%d")
                next_m = last + timedelta(days=32 * i)
                forecast.append({
                    "month": next_m.strftime("%Y-%m"),
                    "predicted_amount": avg_amount,
                    "predicted_qty": avg_qty,
                })
            except Exception:
                pass

    return {"history": history, "forecast": forecast}


def calculate_safety_stock(txs: list[dict]) -> dict:
    """안전재고 산출"""
    product_monthly = defaultdict(lambda: defaultdict(int))
    for tx in txs:
        pc = tx.get("product_code", "") or tx.get("product_name", "")
        d = tx.get("transaction_date", "")
        if not pc or not d:
            continue
        month_key = d[:7]
        qty = _safe_num(tx.get("quantity", 0))
        product_monthly[pc][month_key] += qty

    results = []
    for pc, monthly in product_monthly.items():
        values = list(monthly.values())
        if not values:
            continue
        avg = sum(values) / len(values)
        variance = sum((v - avg) ** 2 for v in values) / max(len(values) - 1, 1)
        std_dev = variance ** 0.5
        safety = round(std_dev * 1.65)  # 95% 서비스 수준
        results.append({
            "product_code": pc,
            "avg_monthly_qty": round(avg, 1),
            "std_dev": round(std_dev, 1),
            "safety_stock": safety,
            "reorder_point": round(avg + safety),
        })

    return {"products": sorted(results, key=lambda x: x["avg_monthly_qty"], reverse=True)}


def calculate_clv_acv(txs: list[dict]) -> dict:
    """CLV/ACV 계산 (거래처별 생애가치)"""
    cust_data = defaultdict(lambda: {"amounts": [], "dates": []})
    for tx in txs:
        cn = tx.get("customer_name", "")
        if not cn:
            continue
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))
        cust_data[cn]["amounts"].append(amt)
        d = tx.get("transaction_date", "")
        if d:
            cust_data[cn]["dates"].append(d)

    results = []
    tier_summary = {"Tier 1": {"count": 0, "total_clv": 0, "total_acv": 0},
                    "Tier 2": {"count": 0, "total_clv": 0, "total_acv": 0},
                    "Tier 3": {"count": 0, "total_clv": 0, "total_acv": 0}}

    for cust, d in cust_data.items():
        total = sum(d["amounts"])
        dates = sorted(d["dates"])
        months_active = 1
        if len(dates) >= 2:
            try:
                first = datetime.strptime(dates[0][:10], "%Y-%m-%d")
                last = datetime.strptime(dates[-1][:10], "%Y-%m-%d")
                months_active = max(1, (last - first).days / 30)
            except Exception:
                pass
        acv = round(total / max(months_active / 12, 0.1))
        clv = round(acv * 3)  # 3년 추정
        tier = "Tier 1" if clv >= 300_000_000 else "Tier 2" if clv >= 50_000_000 else "Tier 3"

        results.append({
            "customer_name": cust,
            "total_revenue": total,
            "months_active": round(months_active, 1),
            "acv": acv,
            "clv": clv,
            "tier": tier,
        })
        tier_summary[tier]["count"] += 1
        tier_summary[tier]["total_clv"] += clv
        tier_summary[tier]["total_acv"] += acv

    return {
        "clv_results": sorted(results, key=lambda x: x["clv"], reverse=True),
        "tier_summary": tier_summary,
    }


def calculate_trend_matching(txs: list[dict], customers: list[dict]) -> dict:
    """5대 메가트렌드 매칭 분석"""
    TRENDS = [
        {"id": "ai_infra", "name": "Private AI 인프라", "keywords": ["gpu", "ai", "서버", "딥러닝", "hpc", "nvidia"], "growth": 35},
        {"id": "edge", "name": "엣지 컴퓨팅", "keywords": ["edge", "엣지", "iot", "산업용", "소형"], "growth": 28},
        {"id": "zero_trust", "name": "제로트러스트 보안", "keywords": ["방화벽", "보안", "firewall", "vpn", "fortinet", "ips"], "growth": 22},
        {"id": "sbom", "name": "SBOM/공급망 보안", "keywords": ["sbom", "공급망", "인증", "컴플라이언스"], "growth": 40},
        {"id": "haas", "name": "HaaS 구독 모델", "keywords": ["구독", "월정액", "렌탈", "as-a-service", "haas"], "growth": 30},
    ]

    product_names = [tx.get("product_name", "").lower() for tx in txs]
    all_text = " ".join(product_names)

    results = []
    for trend in TRENDS:
        hits = sum(1 for kw in trend["keywords"] if kw in all_text)
        score = min(100, hits * 20)
        results.append({
            "trend_id": trend["id"],
            "trend_name": trend["name"],
            "score": score,
            "annual_growth": trend["growth"],
            "opportunity_level": "높음" if score >= 60 else "보통" if score >= 30 else "낮음",
        })

    return {"trends": sorted(results, key=lambda x: x["score"], reverse=True)}


def calculate_customer_ranking(txs: list[dict]) -> dict:
    """거래처별 매출액 순위 TOP 10 (C열 거래처명, J열 합계 기준)"""
    if not txs:
        return {"top10": [], "total_customers": 0}

    cust_totals = defaultdict(lambda: {"amount": 0, "qty": 0, "tx_count": 0})
    for tx in txs:
        cn = (tx.get("customer_name") or "").strip()
        if not cn:
            continue
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))
        qty = _safe_num(tx.get("quantity", 0))
        cust_totals[cn]["amount"] += amt
        cust_totals[cn]["qty"] += qty
        cust_totals[cn]["tx_count"] += 1

    sorted_custs = sorted(cust_totals.items(), key=lambda x: x[1]["amount"], reverse=True)
    total_amount = sum(v["amount"] for _, v in sorted_custs)

    top10 = []
    for i, (cn, data) in enumerate(sorted_custs[:10]):
        pct = round(data["amount"] / total_amount * 100, 1) if total_amount else 0
        top10.append({
            "customer_name": cn,
            "amount": data["amount"],
            "quantity": data["qty"],
            "tx_count": data["tx_count"],
            "pct": pct,
        })

    return {"top10": top10, "total_customers": len(cust_totals), "total_amount": total_amount}


def calculate_product_trends(txs: list[dict]) -> dict:
    """
    상위 품목의 주간/월간/분기별 판매 추이 분석 + 특이사항 감지
    - E열 모델명 우선 사용, 제외 키워드 필터링
    - 수량 TOP 10 + 금액 TOP 10
    - 주간/월간/분기별 추이 + 이상치(평균 대비 ±50%) 감지
    """
    if not txs:
        return {"top10_by_qty": [], "top10_by_amount": [], "trends": {}, "anomalies": []}

    # 모델명 기반 집계 (D열 키워드 제외 필터링)
    product_totals = defaultdict(lambda: {"qty": 0, "amount": 0, "name": ""})
    for tx in txs:
        if _is_excluded_tx(tx):
            continue
        pn = _get_model_name(tx)
        if not pn:
            continue
        qty = _safe_num(tx.get("quantity", 0))
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))
        product_totals[pn]["qty"] += qty
        product_totals[pn]["amount"] += amt
        product_totals[pn]["name"] = pn

    # TOP 10
    top10_qty = sorted(product_totals.items(), key=lambda x: x[1]["qty"], reverse=True)[:10]
    top10_amt = sorted(product_totals.items(), key=lambda x: x[1]["amount"], reverse=True)[:10]

    top10_by_qty = [{"product_name": k, "quantity": v["qty"], "amount": v["amount"]} for k, v in top10_qty]
    top10_by_amount = [{"product_name": k, "amount": v["amount"], "quantity": v["qty"]} for k, v in top10_amt]

    # 상위 품목 리스트 (합집합, 최대 15개)
    top_products = list(dict.fromkeys([k for k, _ in top10_qty] + [k for k, _ in top10_amt]))[:15]

    # 주간/월간/분기별 집계
    from datetime import datetime as _dt

    weekly = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "amount": 0}))
    monthly = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "amount": 0}))
    quarterly = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "amount": 0}))

    for tx in txs:
        if _is_excluded_tx(tx):
            continue
        pn = _get_model_name(tx)
        d = tx.get("transaction_date", "")
        if not pn or not d or pn not in top_products:
            continue
        qty = _safe_num(tx.get("quantity", 0))
        amt = _safe_num(tx.get("total_amount", tx.get("supply_price", 0)))

        try:
            dt = _dt.strptime(d[:10], "%Y-%m-%d")
            # 주차 키: YYYY-Wxx
            week_key = f"{dt.year}-W{dt.isocalendar()[1]:02d}"
            month_key = d[:7]
            q = (dt.month - 1) // 3 + 1
            quarter_key = f"{dt.year}-Q{q}"

            weekly[pn][week_key]["qty"] += qty
            weekly[pn][week_key]["amount"] += amt
            monthly[pn][month_key]["qty"] += qty
            monthly[pn][month_key]["amount"] += amt
            quarterly[pn][quarter_key]["qty"] += qty
            quarterly[pn][quarter_key]["amount"] += amt
        except (ValueError, TypeError):
            continue

    # 추이 데이터 구성 + 이상치 감지
    trends = {}
    anomalies = []

    for pn in top_products:
        pn_trend = {"weekly": [], "monthly": [], "quarterly": []}

        for period_type, period_data in [("weekly", weekly), ("monthly", monthly), ("quarterly", quarterly)]:
            if pn not in period_data:
                continue
            sorted_keys = sorted(period_data[pn].keys())
            values = []
            for k in sorted_keys:
                v = period_data[pn][k]
                values.append({"period": k, "qty": v["qty"], "amount": v["amount"]})
            pn_trend[period_type] = values

            # 이상치 감지: 평균 대비 ±50%
            if len(values) >= 3:
                avg_qty = sum(v["qty"] for v in values) / len(values)
                avg_amt = sum(v["amount"] for v in values) / len(values)
                for v in values:
                    if avg_qty > 0:
                        qty_ratio = v["qty"] / avg_qty
                        if qty_ratio >= 1.5:
                            anomalies.append({
                                "product_name": pn, "period": v["period"],
                                "period_type": period_type, "metric": "수량",
                                "value": v["qty"], "average": round(avg_qty, 1),
                                "change_pct": round((qty_ratio - 1) * 100, 1),
                                "type": "급증"
                            })
                        elif qty_ratio <= 0.5:
                            anomalies.append({
                                "product_name": pn, "period": v["period"],
                                "period_type": period_type, "metric": "수량",
                                "value": v["qty"], "average": round(avg_qty, 1),
                                "change_pct": round((qty_ratio - 1) * 100, 1),
                                "type": "급감"
                            })
                    if avg_amt > 0:
                        amt_ratio = v["amount"] / avg_amt
                        if amt_ratio >= 1.5:
                            anomalies.append({
                                "product_name": pn, "period": v["period"],
                                "period_type": period_type, "metric": "금액",
                                "value": v["amount"], "average": round(avg_amt),
                                "change_pct": round((amt_ratio - 1) * 100, 1),
                                "type": "급증"
                            })
                        elif amt_ratio <= 0.5:
                            anomalies.append({
                                "product_name": pn, "period": v["period"],
                                "period_type": period_type, "metric": "금액",
                                "value": v["amount"], "average": round(avg_amt),
                                "change_pct": round((amt_ratio - 1) * 100, 1),
                                "type": "급감"
                            })

        trends[pn] = pn_trend

    return {
        "top10_by_qty": top10_by_qty,
        "top10_by_amount": top10_by_amount,
        "trends": trends,
        "anomalies": sorted(anomalies, key=lambda x: abs(x["change_pct"]), reverse=True),
    }


def _safe_num(val) -> int:
    """안전하게 숫자 변환"""
    if val is None:
        return 0
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return 0
