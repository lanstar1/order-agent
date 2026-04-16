"""
트렌드 분석 엔진 (TypeScript trend-analysis.ts 변환)

주요 함수:
- build_trend_analysis(snapshots, profile, observed_months): 전체 트렌드 분석 생성
- apply_brand_exclusion(keyword, custom_terms): 브랜드 제외 판정
"""

from typing import TypedDict, Any
from dataclasses import dataclass, asdict, field
from statistics import stdev
from .trend_constants import EVENT_LABELS, DEFAULT_BRAND_EXCLUDE, list_monthly_periods


# ============================================================================
# Type Definitions
# ============================================================================

class TrendAnalysisSeriesPoint(TypedDict):
    """시계열 차트용 포인트"""
    period: str
    value: float


class TrendAnalysisKeyword(TypedDict):
    """키워드 분석 결과"""
    keyword: str
    confidence: int
    confidenceLabel: str  # "high" | "medium" | "low"
    rationale: str
    latestScore: float
    delta: float
    momentum: float
    seasonalIndex: float
    appearanceCount: int
    recommendedPeriods: list[str]
    recommendedMonths: list[str]
    cautionMonths: list[str]
    sparkline: list[TrendAnalysisSeriesPoint]


class TrendAnalysisCard(TypedDict):
    """분석 카드 (종류: steady, seasonal, monthly, event, caution, recent)"""
    kind: str
    title: str
    description: str
    items: list[TrendAnalysisKeyword]


class TrendAnalysisHeroMetric(TypedDict):
    """4개의 대표 메트릭"""
    id: str
    label: str
    keyword: str
    confidence: int
    confidenceLabel: str
    rationale: str
    monthLabel: str | None
    sparkline: list[TrendAnalysisSeriesPoint]


class TrendAnalysisHeatmapCell(TypedDict):
    """히트맵 셀"""
    key: str
    label: str
    value: float


class TrendAnalysisHeatmapRow(TypedDict):
    """히트맵 행 (키워드별 월간 강도)"""
    keyword: str
    confidence: int
    confidenceLabel: str
    rationale: str
    seasonRationale: str
    timelineRationale: str
    recommendedMonths: list[str]
    cautionMonths: list[str]
    periodCells: list[TrendAnalysisHeatmapCell]
    seasonCells: list[TrendAnalysisHeatmapCell]
    timelineStats: dict


class TrendAnalysisOverviewLine(TypedDict):
    """개요 차트 라인"""
    keyword: str
    confidence: int
    points: list[TrendAnalysisSeriesPoint]


class TrendMonthlyPreparationBucket(TypedDict):
    """월별 준비 키워드 버킷"""
    month: str
    label: str
    seasonLabel: str
    items: list[TrendAnalysisKeyword]


class TrendMonthlyExplorer(TypedDict):
    """월별 플래너"""
    month: str
    label: str
    seasonLabel: str
    monthConfidence: int
    recommendedKeywords: list[TrendAnalysisKeyword]
    cautionKeywords: list[TrendAnalysisKeyword]
    historicalMonthScores: list[dict]


class TrendKeywordDrilldownSeries(TypedDict):
    """키워드별 상세 분석"""
    keyword: str
    confidence: int
    confidenceLabel: str
    rationale: str
    observationMonths: int
    recentTrendValue: float
    seasonalityScore: float
    seasonalityScoreLabel: str
    recentRetentionValue: float
    recentTrendExplanation: str
    seasonalityExplanation: str
    recentRetentionExplanation: str
    recommendedMonths: list[str]
    cautionMonths: list[str]
    points: list[TrendAnalysisSeriesPoint]
    recentPoints: list[TrendAnalysisSeriesPoint]
    seasonalityPoints: list[dict]


class TrendAnalysisSummary(TypedDict):
    """전체 분석 요약"""
    resultCount: int
    includedKeywordCount: int
    excludedKeywordCount: int
    observedMonths: int
    overviewSeries: list[TrendAnalysisOverviewLine]
    monthlyPreparation: list[TrendMonthlyPreparationBucket]
    cautionByMonth: list[TrendMonthlyPreparationBucket]
    highlights: list[str]
    heroMetrics: list[TrendAnalysisHeroMetric]
    seasonalityHeatmap: list[TrendAnalysisHeatmapRow]
    monthlyPlanner: list[TrendMonthlyExplorer]
    keywordDrilldownSeries: list[TrendKeywordDrilldownSeries]


@dataclass
class KeywordMetric:
    """키워드별 분석 메트릭 (내부용)"""
    keyword: str
    series: list[float]
    sparkline: list[TrendAnalysisSeriesPoint]
    appearanceCount: int
    overallAverage: float
    recentAverage: float
    baselineAverage: float
    recentYearAverage: float
    historicalAverage: float
    latestScore: float
    delta: float
    momentum: float
    persistence: float
    recentPresence: float
    seasonalIndex: float
    seasonalityScore: float
    repeatability: float
    volatility: float
    stability: float
    confidence: int
    confidenceLabel: str
    peakMonths: list[str]
    weakMonths: list[str]
    peakMonthAverages: list[dict]
    seasonalMonthlyAverage: dict
    steadyScore: float
    seasonalScore: float
    cautionScore: float
    recentChangeScore: float


@dataclass
class MonthlyTarget:
    """월별 타겟 (내부용)"""
    month: str
    label: str
    seasonLabel: str
    items: list[KeywordMetric] = field(default_factory=list)


@dataclass
class SeasonalityResult:
    """계절성 계산 결과"""
    index: float
    score: float
    repeatability: float
    peakMonths: list[str]
    lowMonths: list[str]
    rankedMonths: list[dict]
    monthlyAverageMap: dict


# ============================================================================
# Main Public Function
# ============================================================================

def build_trend_analysis(snapshots: list[dict], profile: dict, observed_months: list[str]) -> dict:
    """
    전체 트렌드 분석 생성

    Args:
        snapshots: trend_snapshots 테이블 데이터
        profile: trend_profiles 테이블 데이터
        observed_months: 관측 월 목록 ["2021-01", "2021-02", ...]

    Returns:
        {
            'summary': TrendAnalysisSummary,
            'cards': list[TrendAnalysisCard],
            'confidenceScore': int
        }
    """
    if not observed_months:
        return {
            'summary': {
                'resultCount': profile.get('result_count', 0),
                'includedKeywordCount': 0,
                'excludedKeywordCount': 0,
                'observedMonths': 0,
                'overviewSeries': [],
                'monthlyPreparation': [],
                'cautionByMonth': [],
                'highlights': [],
                'heroMetrics': [],
                'seasonalityHeatmap': [],
                'monthlyPlanner': [],
                'keywordDrilldownSeries': []
            },
            'cards': [],
            'confidenceScore': 0
        }

    # 데이터 필터링 및 정렬
    normalized_snapshots = [
        s for s in snapshots
        if s.get('rank', 0) <= profile.get('result_count', 0)
    ]
    normalized_snapshots.sort(key=lambda x: (x.get('period', ''), x.get('rank', 0)))

    # 브랜드 제외 처리
    visible_snapshots = [
        s for s in normalized_snapshots
        if not (profile.get('excludeBrandProducts') and s.get('brandExcluded'))
    ]

    # 메인 분석 수행
    metrics = build_keyword_metrics(profile, observed_months, visible_snapshots)
    monthly_preparation = build_monthly_preparation(metrics)
    caution_by_month = build_monthly_caution(metrics)
    overview_series = build_overview_series(metrics)
    seasonality_heatmap = build_seasonality_heatmap(metrics)
    monthly_planner = build_monthly_planner(observed_months, metrics, monthly_preparation, caution_by_month)

    # 키워드 카운팅
    included_keyword_count = len(set(s.get('keyword', '') for s in visible_snapshots))
    excluded_keyword_count = len(set(
        s.get('keyword', '') for s in normalized_snapshots
        if s.get('brandExcluded')
    ))

    # 다가오는/최근 기간
    latest_period = get_latest_snapshot_period(snapshots)
    next_periods = build_upcoming_periods(latest_period, 3)
    recent_periods = build_recent_periods(latest_period, 6)

    # 카드 생성
    cards = [
        build_card(
            "steady",
            "꾸준히 스테디하게 판매하기 좋은 키워드",
            "5년 구간에서 반복 등장 빈도와 안정성을 함께 고려해 오래 가져가기 좋은 키워드를 추렸습니다.",
            [m for m in metrics if m.appearanceCount >= 6 and m.recentPresence > 0.33],
            lambda m: (-m.steadyScore, -m.confidence)
        ),
        build_card(
            "seasonal",
            "계절 반복 키워드",
            "같은 시즌에 여러 해 반복해서 강세를 보인 키워드를 찾았습니다.",
            [m for m in metrics if m.seasonalIndex >= 1.25 and m.repeatability >= 0.45],
            lambda m: (-m.seasonalScore, -m.confidence)
        ),
        build_card(
            "monthly",
            "월별 준비 키워드",
            "특정 월 직전 1~2개월부터 힘을 받는 패턴이 있는 키워드를 월별 준비 관점으로 정리했습니다.",
            flatten_monthly_targets(monthly_preparation),
            None
        ),
        build_card(
            "event",
            "시즌/이벤트 준비 키워드",
            "다가오는 이벤트 달과 잘 맞는 키워드를 뽑아 다음 시즌 준비 힌트로 보여줍니다.",
            build_event_metrics(monthly_preparation, next_periods),
            None,
            next_periods
        ),
        build_card(
            "caution",
            "조심해야 할 키워드",
            "최근 2년 기준 하향이 이어지거나 변동성이 커서 보수적으로 봐야 하는 키워드입니다.",
            [m for m in metrics if m.cautionScore >= 46],
            lambda m: (-m.cautionScore, m.delta)
        ),
        build_card(
            "recent",
            "최근 변화 키워드",
            "최근 6~12개월 변화량을 참고용으로만 보여줍니다. 메인 판단보다는 보조 시그널입니다.",
            [m for m in metrics if abs(m.delta) >= 0.7 or abs(m.momentum) >= 0.5],
            lambda m: (-m.recentChangeScore, -m.confidence),
            recent_periods
        )
    ]

    # 히어로 메트릭
    hero_metrics = build_hero_metrics(cards, monthly_preparation, caution_by_month, next_periods)

    # 키워드 드릴다운 시리즈
    featured_keywords = set()
    for row in seasonality_heatmap:
        featured_keywords.add(row['keyword'])
    for card in cards:
        for item in card['items']:
            featured_keywords.add(item['keyword'])
    for metric in hero_metrics:
        featured_keywords.add(metric['keyword'])

    keyword_drilldown_series = build_keyword_drilldown_series(metrics, list(featured_keywords))

    # 신뢰도 점수
    confidence_score = (
        round(sum(m.confidence for m in metrics) / len(metrics))
        if metrics
        else 0
    )

    # 강조 포인트
    highlights = build_highlights({
        'cards': cards,
        'monthly_preparation': monthly_preparation,
        'caution_by_month': caution_by_month,
        'confidence_score': confidence_score,
        'included_keyword_count': included_keyword_count,
        'excluded_keyword_count': excluded_keyword_count,
        'next_periods': next_periods
    })

    # 최종 요약
    summary: TrendAnalysisSummary = {
        'resultCount': profile.get('result_count', 0),
        'includedKeywordCount': included_keyword_count,
        'excludedKeywordCount': excluded_keyword_count,
        'observedMonths': len(observed_months),
        'overviewSeries': overview_series,
        'monthlyPreparation': monthly_preparation,
        'cautionByMonth': caution_by_month,
        'highlights': highlights,
        'heroMetrics': hero_metrics,
        'seasonalityHeatmap': seasonality_heatmap,
        'monthlyPlanner': monthly_planner,
        'keywordDrilldownSeries': keyword_drilldown_series
    }

    return {
        'summary': summary,
        'cards': cards,
        'confidenceScore': confidence_score
    }


def apply_brand_exclusion(keyword: str, custom_terms: list[str] = None) -> bool:
    """
    키워드가 브랜드 제외 대상인지 판정

    Args:
        keyword: 검사할 키워드
        custom_terms: 추가 제외 용어

    Returns:
        True if should be excluded
    """
    if custom_terms is None:
        custom_terms = []

    normalized = normalize_term(keyword)
    if not normalized:
        return False

    excluded_terms = [normalize_term(t) for t in DEFAULT_BRAND_EXCLUDE + custom_terms]
    excluded_terms = [t for t in excluded_terms if t]

    for term in excluded_terms:
        if not term:
            continue
        if normalized == term:
            return True
        if normalized.startswith(term) or normalized.endswith(term):
            return True
        if len(term) >= 3 and term in normalized:
            return True

    return False


# ============================================================================
# Keyword Metrics Building
# ============================================================================

def build_keyword_metrics(profile: dict, periods: list[str], snapshots: list[dict]) -> list[KeywordMetric]:
    """각 키워드별 분석 메트릭 계산"""
    if not periods:
        return []

    period_index = {period: idx for idx, period in enumerate(periods)}
    series_by_keyword = {}

    # 시계열 데이터 구성
    for snapshot in snapshots:
        period = snapshot.get('period', '')
        keyword = snapshot.get('keyword', '')
        rank = snapshot.get('rank', 0)

        if period not in period_index:
            continue

        if keyword not in series_by_keyword:
            series_by_keyword[keyword] = [0.0] * len(periods)

        score = max(profile.get('result_count', 0) + 1 - rank, 0)
        idx = period_index[period]
        series_by_keyword[keyword][idx] = max(series_by_keyword[keyword][idx], score)

    # 메트릭 계산
    metrics = []
    for keyword, series in series_by_keyword.items():
        non_zero = [v for v in series if v > 0]
        appearance_count = len(non_zero)

        recent_window = series[-min(12, len(series)):]
        baseline_window = series[:-min(12, len(series))] if len(series) > 12 else []

        recent_avg = average(series[-min(6, len(series)):])
        baseline_avg = average(baseline_window if baseline_window else series)
        recent_year_avg = average(recent_window)
        historical_avg = average(baseline_window if baseline_window else series)
        overall_avg = average(series)

        delta = round2(recent_year_avg - historical_avg)

        # Momentum: 최근 3개월 vs 그전 3개월
        recent_3 = series[-3:]
        prev_3 = series[-6:-3] if len(series) >= 6 else series[:3]
        momentum = round2(average(recent_3) - average(prev_3))

        seasonal = calculate_seasonality(periods, series)
        volatility = round2(standard_deviation(non_zero) if len(non_zero) > 1 else 0)
        persistence = round2(appearance_count / len(periods))
        recent_presence = round2(
            len([v for v in recent_window if v > 0]) / max(1, len(recent_window))
        )
        stability = round2(max(0, 1 - volatility / max(overall_avg * 1.5, 4)))

        confidence = calculate_confidence(
            len(periods),
            appearance_count,
            seasonal['repeatability'],
            volatility,
            recent_presence,
            overall_avg
        )

        # 점수 계산
        steady_score = round2(
            overall_avg * 3.1 +
            persistence * 42 +
            recent_presence * 28 +
            stability * 22 +
            min(confidence / 100, 1) * 14
        )

        seasonal_score = round2(
            seasonal['score'] * 0.72 +
            seasonal['repeatability'] * 18 +
            confidence * 0.18 +
            recent_presence * 8
        )

        caution_score = round2(
            max(0, -delta) * 22 +
            max(0, 1 - recent_presence) * 24 +
            max(0, 1 - persistence) * 18 +
            volatility * 3.5 +
            max(0, seasonal['index'] - 1.6) * 8
        )

        recent_change_score = round2(
            abs(delta) * 18 +
            abs(momentum) * 14 +
            confidence * 0.18
        )

        metrics.append(KeywordMetric(
            keyword=keyword,
            series=series,
            sparkline=[
                {'period': p, 'value': round2(series[i])}
                for i, p in enumerate(periods)
            ],
            appearanceCount=appearance_count,
            overallAverage=round2(overall_avg),
            recentAverage=round2(recent_avg),
            baselineAverage=round2(baseline_avg),
            recentYearAverage=round2(recent_year_avg),
            historicalAverage=round2(historical_avg),
            latestScore=round2(series[-1] if series else 0),
            delta=delta,
            momentum=momentum,
            persistence=persistence,
            recentPresence=recent_presence,
            seasonalIndex=seasonal['index'],
            seasonalityScore=seasonal['score'],
            repeatability=seasonal['repeatability'],
            volatility=volatility,
            stability=stability,
            confidence=confidence,
            confidenceLabel='high' if confidence >= 80 else 'medium' if confidence >= 58 else 'low',
            peakMonths=seasonal['peakMonths'],
            weakMonths=seasonal['lowMonths'],
            peakMonthAverages=seasonal['rankedMonths'],
            seasonalMonthlyAverage=seasonal['monthlyAverageMap'],
            steadyScore=steady_score,
            seasonalScore=seasonal_score,
            cautionScore=caution_score,
            recentChangeScore=recent_change_score
        ))

    return metrics


# ============================================================================
# Monthly Preparation & Caution
# ============================================================================

def build_monthly_preparation(metrics: list[KeywordMetric]) -> list[TrendMonthlyPreparationBucket]:
    """월별 준비 키워드 구성"""
    buckets = [
        MonthlyTarget(
            month=f'{i+1:02d}',
            label=f'{i+1}월',
            seasonLabel=EVENT_LABELS.get(f'{i+1:02d}', '시즌 준비')
        )
        for i in range(12)
    ]

    for bucket in buckets:
        candidates = []
        for metric in metrics:
            target_avg = metric.seasonalMonthlyAverage.get(bucket.month, 0)
            prev_month = f'{((int(bucket.month) - 2) % 12) + 1:02d}'
            two_months = f'{((int(bucket.month) - 3) % 12) + 1:02d}'
            lead_avg = average([
                metric.seasonalMonthlyAverage.get(prev_month, 0),
                metric.seasonalMonthlyAverage.get(two_months, 0)
            ])

            if target_avg > 0 and target_avg >= lead_avg * 1.08:
                lead_signal = build_lead_signal(metric, bucket.month)
                candidates.append((metric, target_avg, lead_signal))

        candidates.sort(
            key=lambda x: (
                -x[1],  # target_avg desc
                -x[2],  # lead_signal desc
                -x[0].confidence * 0.08
            )
        )

        bucket.items = [c[0] for c in candidates[:4]]

    return [
        {
            'month': b.month,
            'label': b.label,
            'seasonLabel': b.seasonLabel,
            'items': [
                build_keyword_item('monthly', m, [b.month])
                for m in b.items
            ]
        }
        for b in buckets
    ]


def build_monthly_caution(metrics: list[KeywordMetric]) -> list[TrendMonthlyPreparationBucket]:
    """월별 조심 키워드 구성"""
    buckets = [
        MonthlyTarget(
            month=f'{i+1:02d}',
            label=f'{i+1}월',
            seasonLabel=EVENT_LABELS.get(f'{i+1:02d}', '시즌 주의')
        )
        for i in range(12)
    ]

    keyword_usage = {}

    for bucket in buckets:
        # 특정 후보 선별
        specific_candidates = []
        for metric in metrics:
            target_avg = metric.seasonalMonthlyAverage.get(bucket.month, 0)
            peak_avg = metric.peakMonthAverages[0]['average'] if metric.peakMonthAverages else 0
            target_weak = bucket.month in metric.weakMonths
            month_weakness = (peak_avg - target_avg) / peak_avg if peak_avg > 0 else 0
            seasonality_gap = max(0, peak_avg - target_avg)
            downward_trend = max(0, -metric.delta)
            seasonality_strength = metric.seasonalityScore / 100
            broad_keyword_penalty = 18 if metric.appearanceCount >= 36 and seasonality_strength < 0.46 else 0
            has_meaningful = peak_avg >= max(metric.overallAverage * 1.08, 1.4)

            qualifies = (
                metric.cautionScore >= 34 and
                bucket.month not in metric.peakMonths and
                has_meaningful and
                (target_weak or month_weakness >= 0.38 or
                 (downward_trend >= 0.9 and target_avg <= metric.overallAverage * 0.78))
            )

            if qualifies:
                score = (
                    metric.cautionScore * 0.58 +
                    (18 if target_weak else 0) +
                    month_weakness * 42 +
                    seasonality_gap * 2.4 +
                    downward_trend * 10 +
                    seasonality_strength * 14 -
                    broad_keyword_penalty
                )
                specific_candidates.append((metric, score))

        # 폴백 후보
        fallback_candidates = []
        for metric in metrics:
            target_avg = metric.seasonalMonthlyAverage.get(bucket.month, 0)
            target_weak = bucket.month in metric.weakMonths
            if metric.cautionScore >= 36 and (target_weak or target_avg <= metric.overallAverage * 0.76 or metric.delta < -0.3):
                score = metric.cautionScore + max(0, -metric.delta) * 8 + max(0, metric.overallAverage - target_avg)
                fallback_candidates.append((metric, score))

        # 풀 선택
        pool = sorted(specific_candidates, key=lambda x: -x[1]) if specific_candidates else sorted(fallback_candidates, key=lambda x: -x[1])

        selected = []
        used_in_bucket = set()

        while len(selected) < 4 and pool:
            # 미사용 키워드 필터링 및 사용 패널티 적용
            available = [
                (m, s - keyword_usage.get(m.keyword, 0) * 22)
                for m, s in pool
                if m.keyword not in used_in_bucket
            ]

            if not available:
                break

            best_metric, _ = max(available, key=lambda x: x[1])
            selected.append(best_metric)
            used_in_bucket.add(best_metric.keyword)
            keyword_usage[best_metric.keyword] = keyword_usage.get(best_metric.keyword, 0) + 1

        bucket.items = selected

    return [
        {
            'month': b.month,
            'label': b.label,
            'seasonLabel': b.seasonLabel,
            'items': [
                build_keyword_item('caution', m, [b.month])
                for m in b.items
            ]
        }
        for b in buckets
    ]


# ============================================================================
# Hero Metrics & Other Aggregations
# ============================================================================

def build_hero_metrics(
    cards: list[TrendAnalysisCard],
    monthly_preparation: list[TrendMonthlyPreparationBucket],
    caution_by_month: list[TrendMonthlyPreparationBucket],
    next_periods: list[str]
) -> list[TrendAnalysisHeroMetric]:
    """4개의 대표 메트릭 선별"""
    next_month = next_periods[0][5:7] if next_periods else None

    # 각 타입별 대표 키워드
    prepare_keyword = None
    if next_month:
        for bucket in monthly_preparation:
            if bucket['month'] == next_month and bucket['items']:
                prepare_keyword = bucket['items'][0]
                break
    if not prepare_keyword:
        for card in cards:
            if card['kind'] == 'monthly' and card['items']:
                prepare_keyword = card['items'][0]
                break

    steady_keyword = next(
        (card['items'][0] for card in cards if card['kind'] == 'steady' and card['items']),
        None
    )

    event_keyword = next(
        (card['items'][0] for card in cards if card['kind'] == 'event' and card['items']),
        None
    )
    if not event_keyword:
        event_keyword = next(
            (card['items'][0] for card in cards if card['kind'] == 'seasonal' and card['items']),
            None
        )

    caution_keyword = None
    if next_month:
        for bucket in caution_by_month:
            if bucket['month'] == next_month and bucket['items']:
                caution_keyword = bucket['items'][0]
                break
    if not caution_keyword:
        caution_keyword = next(
            (card['items'][0] for card in cards if card['kind'] == 'caution' and card['items']),
            None
        )

    # 히어로 메트릭 생성
    heroes = []

    if prepare_keyword:
        heroes.append(build_hero_metric(
            'prepare-now',
            '지금 준비해야 할 대표 키워드',
            prepare_keyword,
            f'{int(next_month)}월 기준' if next_month else None
        ))

    if steady_keyword:
        heroes.append(build_hero_metric(
            'steady-anchor',
            '가장 스테디한 키워드',
            steady_keyword
        ))

    if event_keyword:
        heroes.append(build_hero_metric(
            'season-window',
            '다가오는 시즌 준비 키워드',
            event_keyword,
            f'{int(next_month)}월 시즌' if next_month else None
        ))

    if caution_keyword:
        heroes.append(build_hero_metric(
            'caution-now',
            '이번 달 조심 키워드',
            caution_keyword,
            f'{int(next_month)}월 기준' if next_month else None
        ))

    return heroes


def build_hero_metric(
    metric_id: str,
    label: str,
    item: TrendAnalysisKeyword | None,
    month_label: str | None = None
) -> TrendAnalysisHeroMetric | None:
    """단일 히어로 메트릭 생성"""
    if not item:
        return None

    return {
        'id': metric_id,
        'label': label,
        'keyword': item['keyword'],
        'confidence': item['confidence'],
        'confidenceLabel': item['confidenceLabel'],
        'rationale': item['rationale'],
        'monthLabel': month_label,
        'sparkline': item['sparkline']
    }


def build_seasonality_heatmap(metrics: list[KeywordMetric]) -> list[TrendAnalysisHeatmapRow]:
    """계절성 히트맵 (상위 8개 키워드)"""
    selected = sorted(
        metrics,
        key=lambda m: (
            -max(m.steadyScore, m.seasonalScore, m.recentChangeScore),
            -m.confidence
        )
    )[:8]

    rows = []
    for metric in selected:
        peak_months = ', '.join(f'{int(m)}월' for m in metric.peakMonths)
        weak_months = ', '.join(f'{int(m)}월' for m in metric.weakMonths)

        rows.append({
            'keyword': metric.keyword,
            'confidence': metric.confidence,
            'confidenceLabel': metric.confidenceLabel,
            'rationale': f'평균 강세 월은 {peak_months}이며, 장기 반복성은 {round(metric.seasonalityScore)}점입니다.',
            'seasonRationale': f'평균적으로 {peak_months}에 강하고, 약세 월은 {weak_months}입니다.',
            'timelineRationale': f'63개월 중 {metric.appearanceCount}개월 등장했고 가장 강한 시기는 {describe_peak_window(metric)}입니다.',
            'recommendedMonths': metric.peakMonths,
            'cautionMonths': metric.weakMonths,
            'periodCells': [
                {
                    'key': p['period'],
                    'label': p['period'],
                    'value': p['value']
                }
                for p in metric.sparkline
            ],
            'seasonCells': [
                {
                    'key': f'{i+1:02d}',
                    'label': f'{i+1}월',
                    'value': round2(metric.seasonalMonthlyAverage.get(f'{i+1:02d}', 0))
                }
                for i in range(12)
            ],
            'timelineStats': {
                'appearanceCount': metric.appearanceCount,
                'peakWindowLabel': describe_peak_window(metric),
                'recentDelta': metric.delta
            }
        })

    return rows


def build_keyword_drilldown_series(
    metrics: list[KeywordMetric],
    featured_keywords: list[str] = None
) -> list[TrendKeywordDrilldownSeries]:
    """키워드별 상세 분석"""
    metric_map = {m.keyword: m for m in metrics}

    if featured_keywords:
        selected = [
            metric_map[kw] for kw in featured_keywords
            if kw in metric_map
        ]
    else:
        selected = sorted(
            metrics,
            key=lambda m: -(m.steadyScore + m.seasonalScore)
        )[:10]

    series = []
    for metric in selected:
        series.append({
            'keyword': metric.keyword,
            'confidence': metric.confidence,
            'confidenceLabel': metric.confidenceLabel,
            'rationale': f'{metric.keyword}의 최근 흐름과 계절 반복 패턴을 함께 살펴볼 수 있습니다.',
            'observationMonths': metric.appearanceCount,
            'recentTrendValue': metric.delta,
            'seasonalityScore': metric.seasonalityScore,
            'seasonalityScoreLabel': (
                'high' if metric.seasonalityScore >= 72 else
                'medium' if metric.seasonalityScore >= 46 else
                'low'
            ),
            'recentRetentionValue': round2(metric.recentPresence * 100),
            'recentTrendExplanation': f'최근 12개월 평균 점수가 이전 구간보다 {format_signed(metric.delta)} 변했습니다. 값이 클수록 최근 흐름이 강해지고 있다는 뜻입니다.',
            'seasonalityExplanation': '같은 월에 여러 해 반복 등장했는지와 관측 개월 수를 함께 반영한 점수입니다. 한두 번 반짝 뜬 키워드는 점수가 높게 나오지 않도록 보정합니다.',
            'recentRetentionExplanation': '최근 12개월 중 실제로 상위권에 등장한 달의 비율입니다. 높을수록 최근에도 꾸준히 살아 있는 키워드입니다.',
            'recommendedMonths': [f'{int(m)}월' for m in metric.peakMonths],
            'cautionMonths': [f'{int(m)}월' for m in metric.weakMonths],
            'points': metric.sparkline,
            'recentPoints': metric.sparkline[-min(12, len(metric.sparkline)):],
            'seasonalityPoints': [
                {
                    'period': f'{i+1}월',
                    'value': round2(metric.seasonalMonthlyAverage.get(f'{i+1:02d}', 0))
                }
                for i in range(12)
            ]
        })

    return series


def build_monthly_planner(
    periods: list[str],
    metrics: list[KeywordMetric],
    monthly_preparation: list[TrendMonthlyPreparationBucket],
    caution_by_month: list[TrendMonthlyPreparationBucket]
) -> list[TrendMonthlyExplorer]:
    """월별 플래너"""
    metric_map = {m.keyword: m for m in metrics}

    planner = []
    for i in range(12):
        month = f'{i+1:02d}'
        label = f'{i+1}월'
        season_label = EVENT_LABELS.get(month, '시즌 판단')

        # 해당 월의 추천/조심 키워드
        recommended = next(
            (b['items'] for b in monthly_preparation if b['month'] == month),
            []
        )
        caution = next(
            (b['items'] for b in caution_by_month if b['month'] == month),
            []
        )

        related = []
        for item in recommended + caution:
            m = metric_map.get(item['keyword'])
            if m:
                related.append(m)
        related = related[:4]

        # 월별 평균 점수
        monthly_scores = []
        for period in periods:
            if period[5:7] == month:
                values = [
                    next(
                        (p['value'] for p in m.sparkline if p['period'] == period),
                        0
                    )
                    for m in related
                ]
                score = average(values) if values else 0
                monthly_scores.append({
                    'period': period,
                    'value': round2(score)
                })

        # 월별 신뢰도
        confidence_base = (recommended + caution)[:4]
        month_confidence = (
            round(average([item['confidence'] for item in confidence_base]))
            if confidence_base
            else 0
        )

        planner.append({
            'month': month,
            'label': label,
            'seasonLabel': season_label,
            'monthConfidence': month_confidence,
            'recommendedKeywords': recommended[:3],
            'cautionKeywords': caution[:3],
            'historicalMonthScores': monthly_scores
        })

    return planner


def build_overview_series(metrics: list[KeywordMetric]) -> list[TrendAnalysisOverviewLine]:
    """개요 시리즈 (상위 3개 키워드)"""
    selected = sorted(
        metrics,
        key=lambda m: (-m.steadyScore, -m.confidence)
    )[:3]

    return [
        {
            'keyword': m.keyword,
            'confidence': m.confidence,
            'points': m.sparkline
        }
        for m in selected
    ]


def build_event_metrics(
    monthly_preparation: list[TrendMonthlyPreparationBucket],
    next_periods: list[str]
) -> list[TrendAnalysisKeyword]:
    """이벤트 메트릭 (다가오는 달 키워드)"""
    next_months = set(p[5:7] for p in next_periods)
    source_items = [
        item
        for bucket in monthly_preparation
        if bucket['month'] in next_months
        for item in bucket['items']
    ]

    seen = set()
    result = []
    for item in source_items:
        if item['keyword'] not in seen:
            seen.add(item['keyword'])
            result.append(item)

    return result


def flatten_monthly_targets(monthly_preparation: list[TrendMonthlyPreparationBucket]) -> list[TrendAnalysisKeyword]:
    """월별 타겟을 플래튼화"""
    flattened = []
    for bucket in monthly_preparation:
        for item in bucket['items']:
            flattened.append({
                **item,
                'rationale': f"{bucket['label']} {bucket['seasonLabel']} 준비 키워드입니다. {item['rationale']}"
            })

    seen = set()
    result = []
    for item in flattened:
        if item['keyword'] not in seen:
            seen.add(item['keyword'])
            result.append(item)

    return result


# ============================================================================
# Card & Keyword Item Building
# ============================================================================

def build_card(
    kind: str,
    title: str,
    description: str,
    metrics_or_items: list,
    sort_key=None,
    next_periods: list[str] = None
) -> TrendAnalysisCard:
    """분석 카드 생성"""
    # sort_key가 있으면 정렬, 없으면 그냥 앞에서부터 사용
    if sort_key:
        items_to_use = sorted(metrics_or_items, key=sort_key)[:5]
    else:
        items_to_use = metrics_or_items[:5]

    # TrendAnalysisKeyword인지 KeywordMetric인지 확인
    keyword_items = []
    for item in items_to_use:
        if isinstance(item, dict) and 'recommendedPeriods' in item:
            # 이미 TrendAnalysisKeyword
            keyword_items.append(item)
        elif isinstance(item, KeywordMetric):
            # KeywordMetric -> TrendAnalysisKeyword로 변환
            keyword_items.append(
                build_keyword_item(
                    kind,
                    item,
                    next_periods if next_periods else []
                )
            )
        else:
            # KeywordMetric으로 가정
            keyword_items.append(
                build_keyword_item(
                    kind,
                    item,
                    next_periods if next_periods else []
                )
            )

    return {
        'kind': kind,
        'title': title,
        'description': description,
        'items': keyword_items
    }


def build_keyword_item(
    kind: str,
    metric: KeywordMetric,
    periods_or_months: list[str]
) -> TrendAnalysisKeyword:
    """키워드 아이템 생성 (카드용)"""
    peak_months = [f'{int(m)}월' for m in metric.peakMonths]

    # 기간인지 월인지 판별
    period_labels = []
    for val in periods_or_months:
        if '-' in val:  # 기간 형식 (2021-01)
            month_part = val[5:7]
            period_labels.append(
                f"{int(month_part)}월 {EVENT_LABELS.get(month_part, '시즌')}"
            )
        else:  # 월 형식 (01)
            period_labels.append(f'{int(val)}월')

    # 종류별 설명 생성
    if kind == 'steady':
        rationale = f'총 {metric.appearanceCount}개월 등장했고 최근 1년도 유지력이 있어 장기 운영에 유리합니다.'
    elif kind == 'seasonal':
        rationale = f'{", ".join(peak_months)}에 반복 강세가 재현되어 계절형 준비 상품으로 보기 좋습니다.'
    elif kind == 'monthly':
        rationale = f'{", ".join(period_labels)} 준비 구간에서 반복 강세가 보여 월별 준비 리스트에 적합합니다.'
    elif kind == 'event':
        rationale = f'{" · ".join(period_labels[:2])}에 맞춰 준비하기 좋은 시즌/이벤트형 키워드입니다.'
    elif kind == 'caution':
        rationale = f'최근 2년 흐름이 {format_signed(metric.delta)}이고 변동성 {metric.volatility:.1f}로 보수적인 접근이 필요합니다.'
    elif kind == 'recent':
        rationale = f'최근 6~12개월 변화량 {format_signed(metric.delta)}로 참고용 변화를 보여줍니다.'
    else:
        rationale = '장기 데이터 기준으로 반복 패턴을 확인한 키워드입니다.'

    return {
        'keyword': metric.keyword,
        'confidence': metric.confidence,
        'confidenceLabel': metric.confidenceLabel,
        'rationale': rationale,
        'latestScore': metric.latestScore,
        'delta': metric.delta,
        'momentum': metric.momentum,
        'seasonalIndex': metric.seasonalIndex,
        'appearanceCount': metric.appearanceCount,
        'recommendedPeriods': period_labels if kind == 'event' else peak_months,
        'recommendedMonths': peak_months,
        'cautionMonths': [f'{int(m)}월' for m in metric.weakMonths],
        'sparkline': metric.sparkline
    }


def build_highlights(data: dict) -> list[str]:
    """주요 강조 포인트"""
    cards = data['cards']
    monthly_prep = data['monthly_preparation']
    caution_month = data['caution_by_month']
    confidence = data['confidence_score']
    included = data['included_keyword_count']
    excluded = data['excluded_keyword_count']
    next_periods = data['next_periods']

    highlights = []

    # 1. 스테디 키워드
    steady_kw = next(
        (card['items'][0]['keyword'] for card in cards if card['kind'] == 'steady' and card['items']),
        None
    )
    if steady_kw:
        highlights.append(f'가장 스테디한 핵심 키워드는 {steady_kw}입니다.')
    else:
        highlights.append(f'포함 키워드 {included}개 기준으로 장기 점수를 계산했습니다.')

    # 2. 계절 키워드
    seasonal_kw = next(
        (card['items'][0]['keyword'] for card in cards if card['kind'] == 'seasonal' and card['items']),
        None
    )
    if seasonal_kw:
        highlights.append(f'{seasonal_kw}은 같은 시즌에 여러 해 반복된 계절형 강세를 보여줍니다.')
    else:
        highlights.append('강한 계절 반복 키워드는 더 많은 시즌 누적이 필요합니다.')

    # 3. 다음 월 준비 키워드
    next_month = next_periods[0][5:7] if next_periods else None
    next_monthly_kw = None
    if next_month:
        for bucket in monthly_prep:
            if bucket['month'] == next_month and bucket['items']:
                next_monthly_kw = bucket['items'][0]['keyword']
                break

    if next_monthly_kw:
        highlights.append(f'{int(next_month)}월 준비 키워드로 {next_monthly_kw}을 먼저 보는 것이 좋습니다.')
    else:
        highlights.append('다가오는 월 준비 키워드는 다음 시즌 데이터가 더 쌓이면 더 선명해집니다.')

    # 4. 조심 키워드
    next_caution_kw = None
    if next_month:
        for bucket in caution_month:
            if bucket['month'] == next_month and bucket['items']:
                next_caution_kw = bucket['items'][0]['keyword']
                break

    caution_kw = next(
        (card['items'][0]['keyword'] for card in cards if card['kind'] == 'caution' and card['items']),
        None
    )

    if next_caution_kw or caution_kw:
        target = next_caution_kw or caution_kw
        highlights.append(f'신뢰도 {confidence}점 기준으로 {target}는 이번 시즌 보수적으로 보는 편이 좋습니다.')
    else:
        highlights.append(f'브랜드 제외 키워드 {excluded}개를 반영해 일반 상품 트렌드에 집중했습니다.')

    return highlights


# ============================================================================
# Helper Functions
# ============================================================================

def calculate_seasonality(periods: list[str], series: list[float]) -> SeasonalityResult:
    """계절성 계산"""
    monthly_buckets = {}
    yearly_presence = {}

    for idx, period in enumerate(periods):
        month_key = period[5:7]
        year_key = period[:4]

        if month_key not in monthly_buckets:
            monthly_buckets[month_key] = []
        monthly_buckets[month_key].append(series[idx] if idx < len(series) else 0)

        if series[idx] if idx < len(series) else 0 > 0:
            if year_key not in yearly_presence:
                yearly_presence[year_key] = set()
            yearly_presence[year_key].add(month_key)

    overall_avg = average(series)
    appearance_count = sum(1 for v in series if v > 0)
    total_years = len(set(p[:4] for p in periods))
    active_years = len([v for v in yearly_presence.values() if v])

    # 월별 평균 계산 및 정렬
    ranked_months = []
    for month, values in monthly_buckets.items():
        month_avg = average(values)
        repeat_count = sum(1 for v in values if v > 0)
        ranked_months.append({
            'month': month,
            'average': month_avg,
            'repeatCount': repeat_count,
            'totalYears': len(values)
        })

    ranked_months.sort(key=lambda x: -x['average'])

    primary = ranked_months[0] if ranked_months else None
    secondary = ranked_months[1] if len(ranked_months) > 1 else None

    peak_avg = primary['average'] if primary else 0
    repeatability = (
        round2(primary['repeatCount'] / max(1, primary['totalYears']))
        if primary else 0
    )
    secondary_repeatability = (
        round2(secondary['repeatCount'] / max(1, secondary['totalYears']))
        if secondary else 0
    )

    positive_month_avgs = [m for m in ranked_months if m['average'] > 0]
    if positive_month_avgs and peak_avg > 0:
        total_positive = sum(m['average'] for m in positive_month_avgs)
        concentration = round2(peak_avg / max(total_positive, peak_avg))
    else:
        concentration = 0

    observation_score = min(1, appearance_count / 10)
    year_spread = active_years / max(1, total_years)
    non_zero = [v for v in series if v > 0]
    avg_strength = min(1, average(non_zero) / 10) if non_zero else 0

    # 희소성 페널티
    if appearance_count <= 1:
        sparse_penalty = 0.42
    elif appearance_count == 2:
        sparse_penalty = 0.58
    elif appearance_count == 3:
        sparse_penalty = 0.72
    elif appearance_count < 6:
        sparse_penalty = 0.84
    else:
        sparse_penalty = 1

    score = round2(min(
        100,
        (
            repeatability * 0.34 +
            secondary_repeatability * 0.16 +
            year_spread * 0.18 +
            concentration * 0.1 +
            observation_score * 0.14 +
            avg_strength * 0.08
        ) * 100 * sparse_penalty
    ))

    peak_months = [m['month'] for m in ranked_months[:2]]
    low_months = [m['month'] for m in sorted(ranked_months, key=lambda x: x['average'])[:2]]

    return SeasonalityResult(
        index=round2(peak_avg / overall_avg) if overall_avg > 0 else 0,
        score=score,
        repeatability=repeatability,
        peakMonths=peak_months,
        lowMonths=low_months,
        rankedMonths=[
            {'month': m['month'], 'average': m['average']}
            for m in ranked_months[:4]
        ],
        monthlyAverageMap={
            m['month']: m['average']
            for m in ranked_months
        }
    )


def calculate_confidence(
    total_months: int,
    appearance_count: int,
    repeatability: float,
    volatility: float,
    recent_presence: float,
    overall_avg: float
) -> int:
    """신뢰도 계산 (0-100)"""
    observation_score = min(1, total_months / 36)
    appearance_score = min(1, appearance_count / max(8, total_months * 0.32))
    repeat_score = min(1, repeatability)

    if overall_avg > 0:
        stability_score = max(0, 1 - volatility / max(overall_avg * 1.35, 4))
    else:
        stability_score = 0.2

    recent_score = min(1, recent_presence)

    return round(
        (observation_score * 0.2 + appearance_score * 0.24 + repeat_score * 0.24 +
         stability_score * 0.16 + recent_score * 0.16) * 100
    )


def describe_peak_window(metric: KeywordMetric) -> str:
    """피크 윈도우 설명"""
    if not metric.sparkline:
        return '뚜렷한 피크 없음'

    best_point = None
    for point in metric.sparkline:
        if best_point is None or point['value'] > best_point['value']:
            best_point = point

    if best_point and best_point['value'] > 0:
        return f"{best_point['period']} 피크"
    return '뚜렷한 피크 없음'


def build_lead_signal(metric: KeywordMetric, month: str) -> float:
    """선행 신호 계산 (2개월 전 대비)"""
    prev_month = f'{((int(month) - 2) % 12) + 1:02d}'
    two_months = f'{((int(month) - 3) % 12) + 1:02d}'

    target_avg = metric.seasonalMonthlyAverage.get(month, 0)
    lead_avg = average([
        metric.seasonalMonthlyAverage.get(prev_month, 0),
        metric.seasonalMonthlyAverage.get(two_months, 0)
    ])

    return target_avg - lead_avg


def get_latest_snapshot_period(snapshots: list[dict]) -> str | None:
    """최신 스냅샷 기간 추출"""
    periods = sorted(set(s.get('period', '') for s in snapshots), reverse=True)
    return periods[0] if periods else None


def build_upcoming_periods(latest_period: str | None, count: int) -> list[str]:
    """다가오는 기간 생성"""
    if not latest_period:
        return []

    year, month = map(int, latest_period.split('-'))
    periods = []

    month += 1
    for _ in range(count):
        if month > 12:
            month = 1
            year += 1
        periods.append(f'{year:04d}-{month:02d}')
        month += 1

    return periods


def build_recent_periods(latest_period: str | None, count: int) -> list[str]:
    """최근 기간 생성"""
    if not latest_period:
        return []

    year, month = map(int, latest_period.split('-'))
    periods = []

    for _ in range(count):
        periods.insert(0, f'{year:04d}-{month:02d}')
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return periods


def normalize_term(value: str) -> str:
    """용어 정규화"""
    return (
        value.strip()
        .lower()
        .replace(' ', '')
        .replace('_', '')
        .replace('.', '')
        .replace('/', '')
        .replace('(', '')
        .replace(')', '')
        .replace('-', '')
    )


def average(values: list[float]) -> float:
    """평균 계산"""
    if not values:
        return 0
    return sum(values) / len(values)


def standard_deviation(values: list[float]) -> float:
    """표준 편차 계산"""
    if len(values) < 2:
        return 0

    avg = average(values)
    variance = average([(v - avg) ** 2 for v in values])
    return variance ** 0.5


def round2(value: float) -> float:
    """소수점 둘째 자리까지 반올림"""
    return round(value * 100) / 100


def format_signed(value: float) -> str:
    """부호 포함 형식"""
    if value > 0:
        return f'+{value:.1f}'
    return f'{value:.1f}'
