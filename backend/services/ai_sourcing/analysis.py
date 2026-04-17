"""Port of edge-api/src/trend-analysis.ts.

Pure analysis layer — takes a profile and its keyword snapshots and produces
the same cards / summary / heatmap / planner structure consumed by the UI.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from .constants import list_monthly_periods
from .models import TrendKeywordSnapshot, TrendProfile

DEFAULT_BRAND_TERMS = [
    "나이키", "아디다스", "뉴발란스", "유니클로", "자라", "폴로", "샤넬", "디올",
    "구찌", "프라다", "루이비통", "에르메스", "올리비아로렌", "써스데이아일랜드",
    "에고이스트", "모조에스핀", "듀엘", "온앤온", "시슬리", "리스트", "톰보이", "지고트",
    "미샤", "헤라", "설화수", "아이오페", "이니스프리", "에스티로더", "랑콤", "키엘",
    "닥터지", "메디힐", "토리든", "에스트라", "라네즈", "스파오", "탑텐", "무신사",
    "코스", "젝시믹스", "안다르", "오프화이트",
]

EVENT_LABELS = {
    "01": "신년/겨울 준비",
    "02": "신학기 준비",
    "03": "봄 전환",
    "04": "봄 피크",
    "05": "가정의 달",
    "06": "초여름/휴가 준비",
    "07": "여름 휴가",
    "08": "늦여름/가을 준비",
    "09": "가을 전환",
    "10": "가을 피크",
    "11": "연말/블랙프라이데이",
    "12": "연말/겨울 피크",
}


@dataclass
class KeywordMetric:
    keyword: str
    series: list[float]
    sparkline: list[dict]
    appearance_count: int
    overall_average: float
    recent_average: float
    baseline_average: float
    recent_year_average: float
    historical_average: float
    latest_score: float
    delta: float
    momentum: float
    persistence: float
    recent_presence: float
    seasonal_index: float
    seasonality_score: float
    repeatability: float
    volatility: float
    stability: float
    confidence: int
    confidence_label: str
    peak_months: list[str]
    weak_months: list[str]
    peak_month_averages: list[dict]
    seasonal_monthly_average: dict[str, float]
    steady_score: float
    seasonal_score: float
    caution_score: float
    recent_change_score: float


# ---------- helpers ----------


def _average(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std_dev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _average(values)
    variance = _average([(v - mean) ** 2 for v in values])
    return math.sqrt(variance)


def _take_last(values: Sequence[float], count: int) -> list[float]:
    if count <= 0:
        return []
    return list(values[max(0, len(values) - count):])


def _round2(value: float) -> float:
    return round(value * 100) / 100


def _format_signed(value: float) -> str:
    return f"+{value:.1f}" if value > 0 else f"{value:.1f}"


def _normalize_term(value: str) -> str:
    import re

    return re.sub(r"[\s_./()\-]", "", value.strip().lower())


# ---------- brand exclusion ----------


def apply_brand_exclusion(keyword: str, custom_terms: list[str] | None = None) -> bool:
    custom_terms = custom_terms or []
    normalized_keyword = _normalize_term(keyword)
    if not normalized_keyword:
        return False
    excluded = [t for t in (_normalize_term(term) for term in [*DEFAULT_BRAND_TERMS, *custom_terms]) if t]
    for term in excluded:
        if normalized_keyword == term:
            return True
        if normalized_keyword.startswith(term) or normalized_keyword.endswith(term):
            return True
        if len(term) >= 3 and term in normalized_keyword:
            return True
    return False


# ---------- seasonality ----------


def _calculate_seasonality(periods: list[str], series: list[float]) -> dict:
    monthly_buckets: dict[str, list[float]] = {}
    yearly_presence: dict[str, set[str]] = {}
    for period, value in zip(periods, series):
        month_key = period[5:7]
        year_key = period[:4]
        monthly_buckets.setdefault(month_key, []).append(value)
        if value > 0:
            yearly_presence.setdefault(year_key, set()).add(month_key)

    overall_average = _average(series)
    appearance_count = len([v for v in series if v > 0])
    total_years = len({period[:4] for period in periods})
    active_years = len([months for months in yearly_presence.values() if months])

    ranked_months = sorted(
        [
            {
                "month": month,
                "average": _round2(_average(values)),
                "repeatCount": len([v for v in values if v > 0]),
                "totalYears": len(values),
            }
            for month, values in monthly_buckets.items()
        ],
        key=lambda item: item["average"],
        reverse=True,
    )
    primary = ranked_months[0] if ranked_months else None
    secondary = ranked_months[1] if len(ranked_months) > 1 else None
    peak_average = primary["average"] if primary else 0.0
    repeatability = _round2(primary["repeatCount"] / max(1, primary["totalYears"])) if primary else 0.0
    secondary_repeatability = (
        _round2(secondary["repeatCount"] / max(1, secondary["totalYears"])) if secondary else 0.0
    )
    positive_month_averages = [item for item in ranked_months if item["average"] > 0]
    if positive_month_averages and peak_average > 0:
        concentration = _round2(
            peak_average / max(sum(item["average"] for item in positive_month_averages), peak_average)
        )
    else:
        concentration = 0.0
    observation_score = min(1.0, appearance_count / 10)
    year_spread = active_years / max(1, total_years)
    positive_values = [v for v in series if v > 0]
    average_strength = min(1.0, _average(positive_values) / 10) if positive_values else 0.0
    if appearance_count <= 1:
        sparse_penalty = 0.42
    elif appearance_count == 2:
        sparse_penalty = 0.58
    elif appearance_count == 3:
        sparse_penalty = 0.72
    elif appearance_count < 6:
        sparse_penalty = 0.84
    else:
        sparse_penalty = 1.0

    score = _round2(
        min(
            100.0,
            (
                repeatability * 0.34
                + secondary_repeatability * 0.16
                + year_spread * 0.18
                + concentration * 0.1
                + observation_score * 0.14
                + average_strength * 0.08
            )
            * 100
            * sparse_penalty,
        )
    )

    peak_months = [item["month"] for item in ranked_months[:2]]
    low_months = [item["month"] for item in sorted(ranked_months, key=lambda x: x["average"])[:2]]
    ranked_preview = [{"month": item["month"], "average": item["average"]} for item in ranked_months[:4]]
    monthly_average_map = {item["month"]: item["average"] for item in ranked_months}

    return {
        "index": _round2(peak_average / overall_average) if overall_average > 0 else 0.0,
        "score": score,
        "repeatability": repeatability,
        "peakMonths": peak_months,
        "lowMonths": low_months,
        "rankedMonths": ranked_preview,
        "monthlyAverageMap": monthly_average_map,
    }


def _calculate_confidence(
    *,
    total_months: int,
    appearance_count: int,
    repeatability: float,
    volatility: float,
    recent_presence: float,
    overall_average: float,
) -> int:
    observation_score = min(1.0, total_months / 36) if total_months else 0.0
    appearance_score = min(1.0, appearance_count / max(8, total_months * 0.32)) if total_months else 0.0
    repeat_score = min(1.0, repeatability)
    if overall_average > 0:
        stability_score = max(0.0, 1 - volatility / max(overall_average * 1.35, 4))
    else:
        stability_score = 0.2
    recent_score = min(1.0, recent_presence)
    return round(
        (
            observation_score * 0.2
            + appearance_score * 0.24
            + repeat_score * 0.24
            + stability_score * 0.16
            + recent_score * 0.16
        )
        * 100
    )


# ---------- metric construction ----------


def _build_keyword_metrics(
    profile: TrendProfile, periods: list[str], snapshots: list[TrendKeywordSnapshot]
) -> list[KeywordMetric]:
    if not periods:
        return []

    period_index = {period: idx for idx, period in enumerate(periods)}
    series_by_keyword: dict[str, list[float]] = {}

    for snapshot in snapshots:
        index = period_index.get(snapshot.period)
        if index is None:
            continue
        if snapshot.keyword not in series_by_keyword:
            series_by_keyword[snapshot.keyword] = [0.0] * len(periods)
        score = max(profile.resultCount + 1 - snapshot.rank, 0)
        series_by_keyword[snapshot.keyword][index] = max(series_by_keyword[snapshot.keyword][index], score)

    metrics: list[KeywordMetric] = []
    for keyword, series in series_by_keyword.items():
        non_zero = [v for v in series if v > 0]
        appearance_count = len(non_zero)
        recent_window = _take_last(series, min(12, len(series)))
        baseline_window = series[: max(0, len(series) - len(recent_window))]
        recent_average = _average(_take_last(series, min(6, len(series))))
        baseline_average = _average(baseline_window if baseline_window else series)
        recent_year_average = _average(recent_window)
        historical_average = _average(baseline_window if baseline_window else series)
        overall_average = _average(series)
        delta = _round2(recent_year_average - historical_average)
        momentum_denom = _average(
            _take_last(series, min(3, len(series)) + min(3, max(len(series) - 3, 0)))
        )
        momentum = _round2(recent_average - momentum_denom)
        seasonal = _calculate_seasonality(periods, series)
        volatility = _round2(_std_dev(non_zero))
        persistence = _round2(appearance_count / len(periods))
        recent_presence = _round2(
            len([v for v in recent_window if v > 0]) / max(1, len(recent_window))
        )
        stability = _round2(max(0.0, 1 - volatility / max(overall_average * 1.5, 4)))
        confidence = _calculate_confidence(
            total_months=len(periods),
            appearance_count=appearance_count,
            repeatability=seasonal["repeatability"],
            volatility=volatility,
            recent_presence=recent_presence,
            overall_average=overall_average,
        )
        steady_score = _round2(
            overall_average * 3.1
            + persistence * 42
            + recent_presence * 28
            + stability * 22
            + min(confidence / 100, 1.0) * 14
        )
        seasonal_score = _round2(
            seasonal["score"] * 0.72 + seasonal["repeatability"] * 18 + confidence * 0.18 + recent_presence * 8
        )
        caution_score = _round2(
            max(0.0, -delta) * 22
            + max(0.0, 1 - recent_presence) * 24
            + max(0.0, 1 - persistence) * 18
            + volatility * 3.5
            + max(0.0, seasonal["index"] - 1.6) * 8
        )
        recent_change_score = _round2(abs(delta) * 18 + abs(momentum) * 14 + confidence * 0.18)

        metrics.append(
            KeywordMetric(
                keyword=keyword,
                series=series,
                sparkline=[{"period": p, "value": _round2(series[i])} for i, p in enumerate(periods)],
                appearance_count=appearance_count,
                overall_average=_round2(overall_average),
                recent_average=_round2(recent_average),
                baseline_average=_round2(baseline_average),
                recent_year_average=_round2(recent_year_average),
                historical_average=_round2(historical_average),
                latest_score=_round2(series[-1] if series else 0),
                delta=delta,
                momentum=momentum,
                persistence=persistence,
                recent_presence=recent_presence,
                seasonal_index=seasonal["index"],
                seasonality_score=seasonal["score"],
                repeatability=seasonal["repeatability"],
                volatility=volatility,
                stability=stability,
                confidence=confidence,
                confidence_label=("high" if confidence >= 80 else "medium" if confidence >= 58 else "low"),
                peak_months=seasonal["peakMonths"],
                weak_months=seasonal["lowMonths"],
                peak_month_averages=seasonal["rankedMonths"],
                seasonal_monthly_average=seasonal["monthlyAverageMap"],
                steady_score=steady_score,
                seasonal_score=seasonal_score,
                caution_score=caution_score,
                recent_change_score=recent_change_score,
            )
        )
    return metrics


# ---------- keyword item builder ----------


def _build_keyword_item(kind: str, metric: KeywordMetric, periods_or_months: list[str]) -> dict:
    peak_months = [f"{int(month)}월" for month in metric.peak_months]
    period_labels = []
    for value in periods_or_months:
        if "-" in value:
            mm = value[5:7]
            period_labels.append(f"{int(mm)}월 {EVENT_LABELS.get(mm, '시즌')}")
        else:
            period_labels.append(f"{int(value)}월")

    rationale = ""
    if kind == "steady":
        rationale = f"총 {metric.appearance_count}개월 등장했고 최근 1년도 유지력이 있어 장기 운영에 유리합니다."
    elif kind == "seasonal":
        rationale = f"{', '.join(peak_months)}에 반복 강세가 재현되어 계절형 준비 상품으로 보기 좋습니다."
    elif kind == "monthly":
        rationale = f"{', '.join(period_labels)} 준비 구간에서 반복 강세가 보여 월별 준비 리스트에 적합합니다."
    elif kind == "event":
        rationale = f"{' · '.join(period_labels[:2])}에 맞춰 준비하기 좋은 시즌/이벤트형 키워드입니다."
    elif kind == "caution":
        rationale = (
            f"최근 2년 흐름이 {_format_signed(metric.delta)}이고 변동성 "
            f"{metric.volatility:.1f}로 보수적인 접근이 필요합니다."
        )
    elif kind == "recent":
        rationale = f"최근 6~12개월 변화량 {_format_signed(metric.delta)}로 참고용 변화를 보여줍니다."
    else:
        rationale = "장기 데이터 기준으로 반복 패턴을 확인한 키워드입니다."

    return {
        "keyword": metric.keyword,
        "confidence": metric.confidence,
        "confidenceLabel": metric.confidence_label,
        "rationale": rationale,
        "latestScore": metric.latest_score,
        "delta": metric.delta,
        "momentum": metric.momentum,
        "seasonalIndex": metric.seasonal_index,
        "appearanceCount": metric.appearance_count,
        "recommendedPeriods": period_labels if kind == "event" else peak_months,
        "recommendedMonths": [f"{int(m)}월" for m in metric.peak_months],
        "cautionMonths": [f"{int(m)}월" for m in metric.weak_months],
        "sparkline": metric.sparkline,
    }


# ---------- monthly planner / preparation / caution ----------


def _build_lead_signal(metric: KeywordMetric, month: str) -> float:
    prev_month = f"{((int(month) + 10) % 12) + 1:02d}"
    two_before = f"{((int(month) + 9) % 12) + 1:02d}"
    target_average = metric.seasonal_monthly_average.get(month, 0)
    lead_average = _average(
        [metric.seasonal_monthly_average.get(prev_month, 0), metric.seasonal_monthly_average.get(two_before, 0)]
    )
    return target_average - lead_average


def _build_monthly_preparation(metrics: list[KeywordMetric]) -> list[dict]:
    buckets: list[dict[str, Any]] = []
    for idx in range(12):
        month = f"{idx + 1:02d}"
        buckets.append({
            "month": month,
            "label": f"{idx + 1}월",
            "seasonLabel": EVENT_LABELS.get(month, "시즌 준비"),
            "raw_items": [],
        })
    for bucket in buckets:
        month = bucket["month"]
        filtered = []
        for metric in metrics:
            target_average = metric.seasonal_monthly_average.get(month, 0)
            prev_month = f"{((int(month) + 10) % 12) + 1:02d}"
            two_before = f"{((int(month) + 9) % 12) + 1:02d}"
            lead_average = _average(
                [metric.seasonal_monthly_average.get(prev_month, 0), metric.seasonal_monthly_average.get(two_before, 0)]
            )
            if target_average > 0 and target_average >= lead_average * 1.08:
                filtered.append(metric)

        def sort_key(metric: KeywordMetric) -> float:
            target = metric.seasonal_monthly_average.get(month, 0)
            return target + _build_lead_signal(metric, month) + metric.confidence * 0.08

        filtered.sort(key=sort_key, reverse=True)
        bucket["raw_items"] = filtered[:4]
    return [
        {
            "month": bucket["month"],
            "label": bucket["label"],
            "seasonLabel": bucket["seasonLabel"],
            "items": [_build_keyword_item("monthly", metric, [bucket["month"]]) for metric in bucket["raw_items"]],
        }
        for bucket in buckets
    ]


def _build_monthly_caution(metrics: list[KeywordMetric]) -> list[dict]:
    buckets = []
    for idx in range(12):
        month = f"{idx + 1:02d}"
        buckets.append({
            "month": month,
            "label": f"{idx + 1}월",
            "seasonLabel": EVENT_LABELS.get(month, "시즌 주의"),
            "raw_items": [],
        })

    keyword_usage: dict[str, int] = {}

    for bucket in buckets:
        month = bucket["month"]
        specific_candidates = []
        for metric in metrics:
            target_average = metric.seasonal_monthly_average.get(month, 0)
            peak_average = metric.peak_month_averages[0]["average"] if metric.peak_month_averages else 0
            target_weak = month in metric.weak_months
            month_weakness = (peak_average - target_average) / peak_average if peak_average > 0 else 0
            seasonal_gap = max(0.0, peak_average - target_average)
            downward_trend = max(0.0, -metric.delta)
            seasonality_strength = metric.seasonality_score / 100
            broad_keyword_penalty = 18 if (metric.appearance_count >= 36 and seasonality_strength < 0.46) else 0
            has_meaningful_seasonality = peak_average >= max(metric.overall_average * 1.08, 1.4)
            qualifies = (
                metric.caution_score >= 34
                and month not in metric.peak_months
                and has_meaningful_seasonality
                and (
                    target_weak
                    or month_weakness >= 0.38
                    or (downward_trend >= 0.9 and target_average <= metric.overall_average * 0.78)
                )
            )
            if not qualifies:
                continue
            score = (
                metric.caution_score * 0.58
                + (18 if target_weak else 0)
                + month_weakness * 42
                + seasonal_gap * 2.4
                + downward_trend * 10
                + seasonality_strength * 14
                - broad_keyword_penalty
            )
            specific_candidates.append({"metric": metric, "score": score})

        specific_candidates.sort(key=lambda item: item["score"], reverse=True)

        fallback_candidates = []
        for metric in metrics:
            target_average = metric.seasonal_monthly_average.get(month, 0)
            target_weak = month in metric.weak_months
            if metric.caution_score >= 36 and (
                target_weak
                or target_average <= metric.overall_average * 0.76
                or metric.delta < -0.3
            ):
                score = (
                    metric.caution_score
                    + max(0.0, -metric.delta) * 8
                    + max(0.0, metric.overall_average - target_average)
                )
                fallback_candidates.append({"metric": metric, "score": score})
        fallback_candidates.sort(key=lambda item: item["score"], reverse=True)

        pool = specific_candidates if specific_candidates else fallback_candidates
        selected: list[KeywordMetric] = []
        used_in_bucket: set[str] = set()
        while len(selected) < 4:
            candidates = [
                {"metric": c["metric"], "score": c["score"] - keyword_usage.get(c["metric"].keyword, 0) * 22}
                for c in pool
                if c["metric"].keyword not in used_in_bucket
            ]
            if not candidates:
                break
            candidates.sort(key=lambda item: item["score"], reverse=True)
            chosen = candidates[0]["metric"]
            selected.append(chosen)
            used_in_bucket.add(chosen.keyword)
            keyword_usage[chosen.keyword] = keyword_usage.get(chosen.keyword, 0) + 1
        bucket["raw_items"] = selected

    return [
        {
            "month": bucket["month"],
            "label": bucket["label"],
            "seasonLabel": bucket["seasonLabel"],
            "items": [_build_keyword_item("caution", metric, [bucket["month"]]) for metric in bucket["raw_items"]],
        }
        for bucket in buckets
    ]


def _build_monthly_planner(
    periods: list[str],
    metrics: list[KeywordMetric],
    monthly_preparation: list[dict],
    caution_by_month: list[dict],
) -> list[dict]:
    metric_map = {metric.keyword: metric for metric in metrics}
    planner = []
    for idx in range(12):
        month = f"{idx + 1:02d}"
        recommended = next((b["items"] for b in monthly_preparation if b["month"] == month), [])
        caution = next((b["items"] for b in caution_by_month if b["month"] == month), [])
        related = []
        for item in [*recommended, *caution]:
            metric = metric_map.get(item["keyword"])
            if metric is not None:
                related.append(metric)
            if len(related) >= 4:
                break
        monthly_scores = []
        for period in periods:
            if period[5:7] != month:
                continue
            values = []
            for metric in related:
                found = next((p["value"] for p in metric.sparkline if p["period"] == period), 0)
                values.append(found)
            monthly_scores.append({"period": period, "value": _round2(_average(values))})
        confidence_base = [*recommended, *caution][:4]
        month_confidence = (
            round(_average([item["confidence"] for item in confidence_base])) if confidence_base else 0
        )
        planner.append({
            "month": month,
            "label": f"{idx + 1}월",
            "seasonLabel": EVENT_LABELS.get(month, "시즌 판단"),
            "monthConfidence": month_confidence,
            "recommendedKeywords": recommended[:3],
            "cautionKeywords": caution[:3],
            "historicalMonthScores": monthly_scores,
        })
    return planner


# ---------- overview / heatmap / drilldown ----------


def _build_overview_series(metrics: list[KeywordMetric]) -> list[dict]:
    ranked = sorted(
        metrics, key=lambda m: (m.steady_score, m.confidence), reverse=True
    )[:3]
    return [
        {"keyword": metric.keyword, "confidence": metric.confidence, "points": metric.sparkline}
        for metric in ranked
    ]


def _describe_peak_window(metric: KeywordMetric) -> str:
    peak_point = None
    for point in metric.sparkline:
        if not peak_point or point["value"] > peak_point["value"]:
            peak_point = point
    if not peak_point or peak_point["value"] <= 0:
        return "뚜렷한 피크 없음"
    return f"{peak_point['period']} 피크"


def _build_seasonality_heatmap(metrics: list[KeywordMetric]) -> list[dict]:
    def signal(metric: KeywordMetric) -> float:
        return max(metric.steady_score, metric.seasonal_score, metric.recent_change_score)

    selected = sorted(metrics, key=lambda m: (signal(m), m.confidence), reverse=True)[:8]
    heatmap = []
    for metric in selected:
        peak_label = ", ".join(f"{int(m)}월" for m in metric.peak_months)
        weak_label = ", ".join(f"{int(m)}월" for m in metric.weak_months)
        heatmap.append({
            "keyword": metric.keyword,
            "confidence": metric.confidence,
            "confidenceLabel": metric.confidence_label,
            "rationale": f"평균 강세 월은 {peak_label}이며, 장기 반복성은 {round(metric.seasonality_score)}점입니다.",
            "seasonRationale": f"평균적으로 {peak_label}에 강하고, 약세 월은 {weak_label}입니다.",
            "timelineRationale": f"63개월 중 {metric.appearance_count}개월 등장했고 가장 강한 시기는 {_describe_peak_window(metric)}입니다.",
            "recommendedMonths": metric.peak_months,
            "cautionMonths": metric.weak_months,
            "periodCells": [
                {"key": p["period"], "label": p["period"], "value": p["value"]} for p in metric.sparkline
            ],
            "seasonCells": [
                {
                    "key": f"{i + 1:02d}",
                    "label": f"{i + 1}월",
                    "value": _round2(metric.seasonal_monthly_average.get(f"{i + 1:02d}", 0)),
                }
                for i in range(12)
            ],
            "timelineStats": {
                "appearanceCount": metric.appearance_count,
                "peakWindowLabel": _describe_peak_window(metric),
                "recentDelta": metric.delta,
            },
        })
    return heatmap


def _build_drilldown_series(metrics: list[KeywordMetric], featured_keywords: list[str]) -> list[dict]:
    metric_map = {m.keyword: m for m in metrics}
    if featured_keywords:
        selected = [metric_map[k] for k in featured_keywords if k in metric_map]
    else:
        selected = sorted(
            metrics, key=lambda m: m.steady_score + m.seasonal_score, reverse=True
        )[:10]
    series = []
    for metric in selected:
        season_label = (
            "high" if metric.seasonality_score >= 72
            else "medium" if metric.seasonality_score >= 46
            else "low"
        )
        series.append({
            "keyword": metric.keyword,
            "confidence": metric.confidence,
            "confidenceLabel": metric.confidence_label,
            "rationale": f"{metric.keyword}의 최근 흐름과 계절 반복 패턴을 함께 살펴볼 수 있습니다.",
            "observationMonths": metric.appearance_count,
            "recentTrendValue": metric.delta,
            "seasonalityScore": metric.seasonality_score,
            "seasonalityScoreLabel": season_label,
            "recentRetentionValue": _round2(metric.recent_presence * 100),
            "recentTrendExplanation": (
                f"최근 12개월 평균 점수가 이전 구간보다 {_format_signed(metric.delta)} 변했습니다. "
                "값이 클수록 최근 흐름이 강해지고 있다는 뜻입니다."
            ),
            "seasonalityExplanation": (
                "같은 월에 여러 해 반복 등장했는지와 관측 개월 수를 함께 반영한 점수입니다. "
                "한두 번 반짝 뜬 키워드는 점수가 높게 나오지 않도록 보정합니다."
            ),
            "recentRetentionExplanation": (
                "최근 12개월 중 실제로 상위권에 등장한 달의 비율입니다. 높을수록 최근에도 꾸준히 살아 있는 키워드입니다."
            ),
            "recommendedMonths": metric.peak_months,
            "cautionMonths": metric.weak_months,
            "points": metric.sparkline,
            "recentPoints": _take_last_points(metric.sparkline, min(12, len(metric.sparkline))),
            "seasonalityPoints": [
                {"period": f"{i + 1}월", "value": _round2(metric.seasonal_monthly_average.get(f"{i + 1:02d}", 0))}
                for i in range(12)
            ],
        })
    return series


def _take_last_points(points: list[dict], count: int) -> list[dict]:
    if count <= 0:
        return []
    return points[max(0, len(points) - count):]


# ---------- card helpers ----------


def _flatten_monthly_targets(monthly_preparation: list[dict]) -> list[dict]:
    flat = []
    seen: set[str] = set()
    for bucket in monthly_preparation:
        for item in bucket["items"]:
            enriched = dict(item)
            enriched["rationale"] = f"{bucket['label']} {bucket['seasonLabel']} 준비 키워드입니다. {item['rationale']}"
            if enriched["keyword"] in seen:
                continue
            seen.add(enriched["keyword"])
            flat.append(enriched)
    return flat


def _build_event_metrics(monthly_preparation: list[dict], next_periods: list[str]) -> list[dict]:
    next_months = {period[5:7] for period in next_periods}
    source_items = []
    for bucket in monthly_preparation:
        if bucket["month"] in next_months:
            source_items.extend(bucket["items"])
    seen: set[str] = set()
    output = []
    for item in source_items:
        if item["keyword"] in seen:
            continue
        seen.add(item["keyword"])
        event_labels = [
            f"{int(period[5:7])}월 {EVENT_LABELS.get(period[5:7], '시즌')}" for period in next_periods
        ]
        output.append({
            "keyword": item["keyword"],
            "confidence": item["confidence"],
            "confidenceLabel": item["confidenceLabel"],
            "rationale": item["rationale"],
            "latestScore": item["latestScore"],
            "delta": item["delta"],
            "momentum": item["momentum"],
            "seasonalIndex": item["seasonalIndex"],
            "appearanceCount": item["appearanceCount"],
            "recommendedPeriods": event_labels,
            "recommendedMonths": item["recommendedMonths"],
            "cautionMonths": item["cautionMonths"],
            "sparkline": item["sparkline"],
        })
    return output


def _is_keyword_item(value: Any) -> bool:
    return isinstance(value, dict) and "recommendedPeriods" in value and "confidenceLabel" in value


def _build_card(
    kind: str,
    title: str,
    description: str,
    metrics: list[Any],
    next_periods: list[str] | None = None,
) -> dict:
    next_periods = next_periods or []
    items = []
    for metric in metrics[:5]:
        if _is_keyword_item(metric):
            items.append(metric)
        else:
            items.append(_build_keyword_item(kind, metric, next_periods))
    return {"kind": kind, "title": title, "description": description, "items": items}


# ---------- hero / highlights ----------


def _build_hero_metric(id_: str, label: str, item: dict | None, month_label: str | None = None):
    if not item:
        return None
    return {
        "id": id_,
        "label": label,
        "keyword": item["keyword"],
        "confidence": item["confidence"],
        "confidenceLabel": item["confidenceLabel"],
        "rationale": item["rationale"],
        "monthLabel": month_label,
        "sparkline": item["sparkline"],
    }


def _build_hero_metrics(cards: list[dict], monthly_preparation: list[dict], caution_by_month: list[dict], next_periods: list[str]):
    next_month = next_periods[0][5:7] if next_periods else None
    month_label = f"{int(next_month)}월 기준" if next_month else None
    season_label = f"{int(next_month)}월 시즌" if next_month else None

    def first_item_of(kind: str) -> dict | None:
        for card in cards:
            if card["kind"] == kind and card["items"]:
                return card["items"][0]
        return None

    prepare_keyword = None
    if next_month:
        bucket = next((b for b in monthly_preparation if b["month"] == next_month), None)
        if bucket and bucket["items"]:
            prepare_keyword = bucket["items"][0]
    if not prepare_keyword:
        prepare_keyword = first_item_of("monthly")

    steady_keyword = first_item_of("steady")
    event_keyword = first_item_of("event") or first_item_of("seasonal")

    caution_keyword = None
    if next_month:
        bucket = next((b for b in caution_by_month if b["month"] == next_month), None)
        if bucket and bucket["items"]:
            caution_keyword = bucket["items"][0]
    if not caution_keyword:
        caution_keyword = first_item_of("caution")

    hero = [
        _build_hero_metric("prepare-now", "지금 준비해야 할 대표 키워드", prepare_keyword, month_label),
        _build_hero_metric("steady-anchor", "가장 스테디한 키워드", steady_keyword),
        _build_hero_metric("season-window", "다가오는 시즌 준비 키워드", event_keyword, season_label),
        _build_hero_metric("caution-now", "이번 달 조심 키워드", caution_keyword, month_label),
    ]
    return [m for m in hero if m]


def _build_highlights(
    *,
    cards: list[dict],
    monthly_preparation: list[dict],
    caution_by_month: list[dict],
    confidence_score: int,
    included_keyword_count: int,
    excluded_keyword_count: int,
    next_periods: list[str],
) -> list[str]:
    def first_keyword(kind: str) -> str | None:
        for card in cards:
            if card["kind"] == kind and card["items"]:
                return card["items"][0]["keyword"]
        return None

    steady = first_keyword("steady")
    seasonal = first_keyword("seasonal")
    caution = first_keyword("caution")
    next_month = next_periods[0][5:7] if next_periods else ""

    def bucket_keyword(collection: list[dict]) -> str | None:
        for bucket in collection:
            if bucket["month"] == next_month and bucket["items"]:
                return bucket["items"][0]["keyword"]
        return None

    next_monthly_item = bucket_keyword(monthly_preparation)
    next_caution_item = bucket_keyword(caution_by_month)

    return [
        f"가장 스테디한 핵심 키워드는 {steady}입니다." if steady else f"포함 키워드 {included_keyword_count}개 기준으로 장기 점수를 계산했습니다.",
        f"{seasonal}은 같은 시즌에 여러 해 반복된 계절형 강세를 보여줍니다." if seasonal else "강한 계절 반복 키워드는 더 많은 시즌 누적이 필요합니다.",
        (
            f"{int(next_month)}월 준비 키워드로 {next_monthly_item}을 먼저 보는 것이 좋습니다."
            if next_monthly_item and next_month
            else "다가오는 월 준비 키워드는 다음 시즌 데이터가 더 쌓이면 더 선명해집니다."
        ),
        (
            f"신뢰도 {confidence_score}점 기준으로 {next_caution_item or caution}는 이번 시즌 보수적으로 보는 편이 좋습니다."
            if next_caution_item or caution
            else f"브랜드 제외 키워드 {excluded_keyword_count}개를 반영해 일반 상품 트렌드에 집중했습니다."
        ),
    ]


# ---------- period helpers ----------


def _latest_snapshot_period(snapshots: list[TrendKeywordSnapshot]) -> str | None:
    unique = sorted({snapshot.period for snapshot in snapshots}, reverse=True)
    return unique[0] if unique else None


def _build_upcoming_periods(latest_period: str | None, count: int) -> list[str]:
    if not latest_period:
        return []
    year, month = (int(x) for x in latest_period.split("-"))
    periods = []
    month += 1
    for _ in range(count):
        if month > 12:
            month = 1
            year += 1
        periods.append(f"{year}-{month:02d}")
        month += 1
    return periods


def _build_recent_periods(latest_period: str | None, count: int) -> list[str]:
    if not latest_period:
        return []
    year, month = (int(x) for x in latest_period.split("-"))
    periods: list[str] = []
    for _ in range(count):
        periods.insert(0, f"{year}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return periods


# ---------- entry point ----------


def build_trend_analysis(profile: TrendProfile, snapshots: list[TrendKeywordSnapshot]) -> dict:
    latest_period = _latest_snapshot_period(snapshots)
    observed_periods = list_monthly_periods(profile.startPeriod, latest_period) if latest_period else []

    normalized = [s for s in snapshots if s.rank <= profile.resultCount]
    normalized.sort(key=lambda s: (s.period, s.rank))
    visible = [s for s in normalized if not (profile.excludeBrandProducts and s.brandExcluded)]

    metrics = _build_keyword_metrics(profile, observed_periods, visible)
    monthly_preparation = _build_monthly_preparation(metrics)
    caution_by_month = _build_monthly_caution(metrics)
    overview_series = _build_overview_series(metrics)
    seasonality_heatmap = _build_seasonality_heatmap(metrics)
    monthly_planner = _build_monthly_planner(observed_periods, metrics, monthly_preparation, caution_by_month)

    included_keyword_count = len({s.keyword for s in visible})
    excluded_keyword_count = len({s.keyword for s in normalized if s.brandExcluded})
    next_periods = _build_upcoming_periods(latest_period, 3)
    recent_periods = _build_recent_periods(latest_period, 6)

    cards: list[dict] = []
    cards.append(
        _build_card(
            "steady",
            "꾸준히 스테디하게 판매하기 좋은 키워드",
            "5년 구간에서 반복 등장 빈도와 안정성을 함께 고려해 오래 가져가기 좋은 키워드를 추렸습니다.",
            sorted(
                [m for m in metrics if m.appearance_count >= 6 and m.recent_presence > 0.33],
                key=lambda m: (m.steady_score, m.confidence),
                reverse=True,
            ),
        )
    )
    cards.append(
        _build_card(
            "seasonal",
            "계절 반복 키워드",
            "같은 시즌에 여러 해 반복해서 강세를 보인 키워드를 찾았습니다.",
            sorted(
                [m for m in metrics if m.seasonal_index >= 1.25 and m.repeatability >= 0.45],
                key=lambda m: (m.seasonal_score, m.confidence),
                reverse=True,
            ),
        )
    )
    cards.append(
        _build_card(
            "monthly",
            "월별 준비 키워드",
            "특정 월 직전 1~2개월부터 힘을 받는 패턴이 있는 키워드를 월별 준비 관점으로 정리했습니다.",
            _flatten_monthly_targets(monthly_preparation),
        )
    )
    cards.append(
        _build_card(
            "event",
            "시즌/이벤트 준비 키워드",
            "다가오는 이벤트 달과 잘 맞는 키워드를 뽑아 다음 시즌 준비 힌트로 보여줍니다.",
            _build_event_metrics(monthly_preparation, next_periods),
            next_periods,
        )
    )
    cards.append(
        _build_card(
            "caution",
            "조심해야 할 키워드",
            "최근 2년 기준 하향이 이어지거나 변동성이 커서 보수적으로 봐야 하는 키워드입니다.",
            sorted(
                [m for m in metrics if m.caution_score >= 46],
                key=lambda m: (m.caution_score, -m.delta),
                reverse=True,
            ),
        )
    )
    cards.append(
        _build_card(
            "recent",
            "최근 변화 키워드",
            "최근 6~12개월 변화량을 참고용으로만 보여줍니다. 메인 판단보다는 보조 시그널입니다.",
            sorted(
                [m for m in metrics if abs(m.delta) >= 0.7 or abs(m.momentum) >= 0.5],
                key=lambda m: (m.recent_change_score, m.confidence),
                reverse=True,
            ),
            recent_periods,
        )
    )

    hero_metrics = _build_hero_metrics(cards, monthly_preparation, caution_by_month, next_periods)

    featured_keywords = []
    for row in seasonality_heatmap:
        featured_keywords.append(row["keyword"])
    for card in cards:
        for item in card["items"]:
            featured_keywords.append(item["keyword"])
    for metric in hero_metrics:
        featured_keywords.append(metric["keyword"])
    seen_keywords: list[str] = []
    deduped: list[str] = []
    for keyword in featured_keywords:
        if keyword in seen_keywords:
            continue
        seen_keywords.append(keyword)
        deduped.append(keyword)
    keyword_drilldown_series = _build_drilldown_series(metrics, deduped)

    confidence_score = round(sum(m.confidence for m in metrics) / len(metrics)) if metrics else 0

    summary = {
        "resultCount": profile.resultCount,
        "includedKeywordCount": included_keyword_count,
        "excludedKeywordCount": excluded_keyword_count,
        "observedMonths": len(observed_periods),
        "overviewSeries": overview_series,
        "monthlyPreparation": monthly_preparation,
        "cautionByMonth": caution_by_month,
        "highlights": _build_highlights(
            cards=cards,
            monthly_preparation=monthly_preparation,
            caution_by_month=caution_by_month,
            confidence_score=confidence_score,
            included_keyword_count=included_keyword_count,
            excluded_keyword_count=excluded_keyword_count,
            next_periods=next_periods,
        ),
        "heroMetrics": hero_metrics,
        "seasonalityHeatmap": seasonality_heatmap,
        "monthlyPlanner": monthly_planner,
        "keywordDrilldownSeries": keyword_drilldown_series,
    }

    return {"summary": summary, "cards": cards, "confidenceScore": confidence_score}
