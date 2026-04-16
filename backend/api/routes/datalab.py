"""
데이터랩 - 네이버 쇼핑인사이트 트렌드 분석 라우터
네이버 공식 DataLab API → 월별 키워드 수집 → 트렌드 분석 → Claude LLM 리포트

기존 sourcing-wizard 프로젝트를 order-agent 구조에 통합
(SQLAlchemy async → 동기 get_connection() 래퍼)
"""
import asyncio
import hashlib
import json
import logging
import math
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

import httpx
import numpy as np
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import config as cfg
from db.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/datalab", tags=["datalab"])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  설정값
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NAVER_API_RATE_LIMIT = 0.2          # 초당 5회 → 200ms 간격
TREND_START_PERIOD = "2021-01"
TREND_PAGE_SIZE = 20

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Pydantic 스키마
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CollectRequest(BaseModel):
    category_id: str
    category_name: str
    category_path: str = ""
    device: str = ""
    gender: str = ""
    ages: list[str] = []
    result_count: int = 20
    brand_exclusion: bool = True
    custom_exclusion_terms: str = ""


class KeywordMetrics(BaseModel):
    keyword: str
    confidence: float
    trend_score: float
    appearance_count: int
    recent_score: float
    overall_average: float
    peak_months: list[int]
    weak_months: list[int]
    category: str
    description: str
    sparkline_data: list[float]


class HeroMetrics(BaseModel):
    prepare_now: Optional[str] = None
    steady_anchor: Optional[str] = None
    season_window: Optional[str] = None
    caution: Optional[str] = None


class MonthlyPlan(BaseModel):
    month: int
    label: str
    recommend_keywords: list[str]
    caution_keywords: list[str]


class HeatmapRow(BaseModel):
    keyword: str
    description: str
    monthly_scores: list[float]


class InsightCard(BaseModel):
    card_type: str
    title: str
    description: str
    keywords: list[KeywordMetrics]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DB 테이블 초기화
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def init_datalab_tables():
    """데이터랩 전용 테이블 생성 (init_db 호출 시 실행)"""
    conn = get_connection()
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dl_profiles (
            id          TEXT PRIMARY KEY,
            slug        TEXT UNIQUE NOT NULL,
            category_id TEXT NOT NULL,
            category_name TEXT NOT NULL,
            category_path TEXT DEFAULT '',
            device      TEXT DEFAULT '',
            gender      TEXT DEFAULT '',
            ages        TEXT DEFAULT '[]',
            result_count INTEGER DEFAULT 20,
            brand_exclusion INTEGER DEFAULT 1,
            custom_exclusion_terms TEXT DEFAULT '',
            latest_run_id TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            updated_at  TEXT DEFAULT (datetime('now','localtime'))
        )""")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS dl_runs (
            id              TEXT PRIMARY KEY,
            profile_id      TEXT NOT NULL,
            status          TEXT DEFAULT 'queued',
            total_tasks     INTEGER DEFAULT 0,
            completed_tasks INTEGER DEFAULT 0,
            failed_tasks    INTEGER DEFAULT 0,
            total_snapshots INTEGER DEFAULT 0,
            current_period  TEXT DEFAULT '',
            started_at      TEXT,
            completed_at    TEXT,
            analysis_result TEXT,
            created_at      TEXT DEFAULT (datetime('now','localtime'))
        )""")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS dl_tasks (
            id          TEXT PRIMARY KEY,
            run_id      TEXT NOT NULL,
            profile_id  TEXT NOT NULL,
            period      TEXT NOT NULL,
            status      TEXT DEFAULT 'pending',
            completed_pages INTEGER DEFAULT 0,
            total_pages INTEGER DEFAULT 1,
            snapshot_count  INTEGER DEFAULT 0,
            failure_reason  TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            completed_at TEXT
        )""")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS dl_snapshots (
            id          TEXT PRIMARY KEY,
            profile_id  TEXT NOT NULL,
            period      TEXT NOT NULL,
            rank        INTEGER NOT NULL,
            keyword     TEXT NOT NULL,
            click_count REAL DEFAULT 0,
            device      TEXT DEFAULT '',
            gender      TEXT DEFAULT '',
            ages        TEXT DEFAULT '[]',
            brand_excluded INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        )""")

        # 인덱스
        conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_dl_profiles_slug ON dl_profiles(slug);
            CREATE INDEX IF NOT EXISTS idx_dl_runs_profile ON dl_runs(profile_id);
            CREATE INDEX IF NOT EXISTS idx_dl_runs_status ON dl_runs(status);
            CREATE INDEX IF NOT EXISTS idx_dl_tasks_run ON dl_tasks(run_id);
            CREATE INDEX IF NOT EXISTS idx_dl_snapshots_profile_period ON dl_snapshots(profile_id, period);
        """)

        conn.commit()
        logger.info("[DataLab] 테이블 초기화 완료")
    except Exception as e:
        logger.error(f"[DataLab] 테이블 초기화 실패: {e}")
    finally:
        conn.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  카테고리 목록
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CATEGORY_TREE = {
    "50000000": "패션의류",
    "50000001": "패션잡화",
    "50000002": "화장품/미용",
    "50000003": "디지털/가전",
    "50000004": "가구/인테리어",
    "50000005": "출산/육아",
    "50000006": "식품",
    "50000007": "스포츠/레저",
    "50000008": "생활/건강",
    "50000009": "여가/생활편의",
    "50000010": "면세점",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  네이버 API 호출
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NAVER_DATALAB_SHOPPING_URL = "https://openapi.naver.com/v1/datalab/shopping/categories"

def _naver_headers() -> dict:
    return {
        "X-Naver-Client-Id": cfg.NAVER_SEARCH_ID,
        "X-Naver-Client-Secret": cfg.NAVER_SEARCH_SECRET,
        "Content-Type": "application/json",
    }


def generate_period_list(start: str = None, end: str = None) -> list[str]:
    if not start:
        start = TREND_START_PERIOD
    if not end:
        end = datetime.now().strftime("%Y-%m")
    periods = []
    current = datetime.strptime(start, "%Y-%m")
    end_dt = datetime.strptime(end, "%Y-%m")
    while current <= end_dt:
        periods.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)
    return periods


async def fetch_shopping_insight_keywords(
    category_id: str,
    period: str,
    device: str = "",
    gender: str = "",
    ages: list[str] = None,
    page: int = 1,
    count: int = 20,
) -> list[dict]:
    if ages is None:
        ages = []

    start_date = f"{period}-01"
    year, month = map(int, period.split("-"))
    if month == 12:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{month + 1:02d}-01"
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)
    end_date = end_dt.strftime("%Y-%m-%d")

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": "month",
        "category": category_id,
    }
    if device:
        body["device"] = device
    if gender:
        body["gender"] = gender
    if ages:
        body["ages"] = ages

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            NAVER_DATALAB_SHOPPING_URL,
            headers=_naver_headers(),
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        if "results" in data and len(data["results"]) > 0:
            result = data["results"][0]
            if "data" in result:
                for idx, item in enumerate(result["data"], start=1):
                    results.append({
                        "rank": idx + (page - 1) * count,
                        "keyword": item.get("group", ""),
                        "click_count": item.get("ratio", 0),
                    })
        return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  브랜드 제외
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BRAND_EXCLUSION_LIST = [
    "나이키", "아디다스", "뉴발란스", "컨버스", "반스", "퓨마", "리복",
    "노스페이스", "네파", "코오롱", "K2", "블랙야크", "아이더",
    "유니클로", "자라", "H&M", "무신사",
    "샤넬", "구찌", "루이비통", "프라다", "에르메스",
    "올리비아로렌", "써스데이아일랜드", "지오다노", "폴로",
    "LG", "삼성", "다이슨", "필립스", "소니", "애플",
    "스타벅스", "CJ", "오뚜기", "풀무원", "농심",
]


def apply_brand_exclusion(keyword: str, brand_exclusion: bool, custom_terms: str = "") -> bool:
    if not brand_exclusion:
        return False
    normalized = keyword.strip().lower()
    all_terms = [b.lower() for b in BRAND_EXCLUSION_LIST]
    if custom_terms:
        for term in custom_terms.split(","):
            t = term.strip().lower()
            if t:
                all_terms.append(t)
    for term in all_terms:
        if normalized == term:
            return True
        if normalized.startswith(term):
            return True
        if normalized.endswith(term):
            return True
        if len(term) >= 3 and term in normalized:
            return True
    return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  트렌드 분석 알고리즘
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MONTH_LABELS = [
    "", "신년/겨울 준비", "신학기 준비", "봄 전환", "봄 피크",
    "가정의 달", "초여름/휴가 준비", "여름 휴가", "늦여름/가을 준비",
    "가을 전환", "가을 피크", "연말/블랙프라이데이", "연말/겨울 피크"
]


def _rank_to_score(rank: int, result_count: int = 20) -> float:
    return max(result_count + 1 - rank, 0)


def _build_keyword_series(snapshots: list[dict], result_count: int = 20) -> dict:
    keyword_data = defaultdict(lambda: {"scores": {}, "ranks": {}})
    for snap in snapshots:
        kw = snap["keyword"]
        period = snap["period"]
        rank = snap["rank"]
        score = _rank_to_score(rank, result_count)
        keyword_data[kw]["scores"][period] = score
        keyword_data[kw]["ranks"][period] = rank
    return dict(keyword_data)


def _calculate_confidence(
    appearance_count: int, total_months: int,
    monthly_scores: list[float], recent_12_scores: list[float],
) -> float:
    if total_months == 0 or not monthly_scores:
        return 0
    observation_score = min(total_months / 60, 1.0) * 100
    appearance_score = (appearance_count / total_months) * 100

    monthly_avg = defaultdict(list)
    for i, score in enumerate(monthly_scores):
        month_idx = (i % 12) + 1
        if score > 0:
            monthly_avg[month_idx].append(score)
    repeat_count = sum(1 for m in monthly_avg.values() if len(m) >= 2)
    repeat_score = (repeat_count / 12) * 100

    non_zero = [s for s in monthly_scores if s > 0]
    if len(non_zero) >= 2:
        mean_val = np.mean(non_zero)
        std_val = np.std(non_zero)
        cv = std_val / mean_val if mean_val > 0 else 1.0
        stability_score = max(0, (1 - cv)) * 100
    else:
        stability_score = 0

    if recent_12_scores:
        recent_presence = sum(1 for s in recent_12_scores if s > 0) / len(recent_12_scores)
        recent_score = recent_presence * 100
    else:
        recent_score = 0

    confidence = (
        observation_score * 0.20 +
        appearance_score * 0.24 +
        repeat_score * 0.24 +
        stability_score * 0.16 +
        recent_score * 0.16
    )
    return round(min(max(confidence, 0), 100), 1)


def _calculate_trend_delta(all_scores: list[float], recent_months: int = 12) -> float:
    if len(all_scores) < recent_months + 3:
        return 0.0
    recent = all_scores[-recent_months:]
    historical = all_scores[:-recent_months]
    recent_avg = np.mean([s for s in recent if s > 0]) if any(s > 0 for s in recent) else 0
    hist_avg = np.mean([s for s in historical if s > 0]) if any(s > 0 for s in historical) else 0
    if hist_avg == 0:
        return 0.0
    return round(float(recent_avg - hist_avg), 1)


def _calculate_trend_slope(recent_scores: list[float]) -> float:
    non_zero_indices = [(i, s) for i, s in enumerate(recent_scores) if s > 0]
    if len(non_zero_indices) < 3:
        return 0.0
    x = np.array([i for i, _ in non_zero_indices])
    y = np.array([s for _, s in non_zero_indices])
    n = len(x)
    sum_x = np.sum(x)
    sum_y = np.sum(y)
    sum_xy = np.sum(x * y)
    sum_x2 = np.sum(x ** 2)
    denominator = n * sum_x2 - sum_x ** 2
    if denominator == 0:
        return 0.0
    slope = (n * sum_xy - sum_x * sum_y) / denominator
    return round(float(slope), 2)


def _find_peak_weak_months(period_scores: dict) -> tuple:
    monthly_totals = defaultdict(list)
    for period, score in period_scores.items():
        if score > 0:
            month = int(period.split("-")[1])
            monthly_totals[month].append(score)
    monthly_avg = {}
    for month, scores in monthly_totals.items():
        monthly_avg[month] = np.mean(scores)
    if not monthly_avg:
        return [], []
    sorted_months = sorted(monthly_avg.items(), key=lambda x: x[1], reverse=True)
    return [m for m, _ in sorted_months[:2]], [m for m, _ in sorted_months[-2:]]


def _get_monthly_average_scores(period_scores: dict) -> list[float]:
    monthly_totals = defaultdict(list)
    for period, score in period_scores.items():
        month = int(period.split("-")[1])
        monthly_totals[month].append(score)
    result = []
    for m in range(1, 13):
        if monthly_totals[m]:
            result.append(round(float(np.mean(monthly_totals[m])), 1))
        else:
            result.append(0.0)
    return result


def _classify_keyword(confidence, appearance_count, total_months,
                      recent_presence, trend_delta, monthly_avg_scores) -> str:
    persistence = appearance_count / total_months if total_months > 0 else 0
    if monthly_avg_scores:
        max_score = max(monthly_avg_scores)
        mean_score = np.mean([s for s in monthly_avg_scores if s > 0]) if any(s > 0 for s in monthly_avg_scores) else 0
        seasonal_index = max_score / mean_score if mean_score > 0 else 0
    else:
        seasonal_index = 0
    repeatability = sum(1 for s in monthly_avg_scores if s > 0) / 12

    if trend_delta > 3.0 and recent_presence > 0.5:
        return "rising"
    if trend_delta < -3.0 and recent_presence < 0.4:
        return "caution"
    if repeatability >= 0.45 and seasonal_index >= 1.25:
        return "seasonal"
    if appearance_count >= 6 and recent_presence > 0.33 and persistence > 0.3:
        return "steady"
    if trend_delta < -1.0:
        return "caution"
    return "steady"


def _generate_description(keyword, category, appearance_count, recent_presence, peak_months) -> str:
    if category == "steady":
        return f"총 {appearance_count}개월 등장했고 최근 1년도 유지력이 있어 장기 운영에 유리합니다."
    elif category == "seasonal":
        peak_str = ", ".join([f"{m}월" for m in peak_months[:2]])
        return f"{peak_str}에 반복 강세가 재현되어 계절형 준비 상품으로 보기 좋습니다."
    elif category == "rising":
        return f"최근 상승세가 뚜렷하며, {appearance_count}개월 간 데이터가 축적되어 있습니다."
    elif category == "caution":
        return f"하락 추세에 있으며 변동성이 높아 신중한 접근이 필요합니다."
    return f"총 {appearance_count}개월 등장한 키워드입니다."


def _steady_score(k: KeywordMetrics) -> float:
    persistence = k.appearance_count / 63
    recent_presence = sum(1 for s in k.sparkline_data[-12:] if s > 0) / 12
    stability = 1 - (np.std(k.sparkline_data) / (np.mean(k.sparkline_data) + 0.01))
    return (
        k.overall_average * 3.1 +
        persistence * 42 +
        recent_presence * 28 +
        max(float(stability), 0) * 22 +
        (k.confidence / 100) * 14
    )


def build_analysis(snapshots: list[dict], periods: list[str], result_count: int = 20) -> dict:
    total_months = len(periods)
    keyword_series = _build_keyword_series(snapshots, result_count)
    all_keyword_metrics = []

    for keyword, data in keyword_series.items():
        scores = data["scores"]
        full_series = [scores.get(p, 0) for p in periods]
        appearance_count = sum(1 for s in full_series if s > 0)
        recent_12 = full_series[-12:] if len(full_series) >= 12 else full_series

        confidence = _calculate_confidence(appearance_count, total_months, full_series, recent_12)
        trend_delta = _calculate_trend_delta(full_series)
        trend_slope = _calculate_trend_slope(recent_12)
        peak_months, weak_months = _find_peak_weak_months(scores)
        monthly_avg = _get_monthly_average_scores(scores)

        recent_presence = sum(1 for s in recent_12 if s > 0) / len(recent_12) if recent_12 else 0
        overall_avg = float(np.mean([s for s in full_series if s > 0])) if any(s > 0 for s in full_series) else 0
        recent_score = recent_12[-1] if recent_12 else 0

        cat = _classify_keyword(confidence, appearance_count, total_months,
                                recent_presence, trend_delta, monthly_avg)
        desc = _generate_description(keyword, cat, appearance_count, recent_presence, peak_months)

        metrics = KeywordMetrics(
            keyword=keyword, confidence=confidence, trend_score=trend_slope,
            appearance_count=appearance_count, recent_score=round(float(recent_score), 1),
            overall_average=round(overall_avg, 1),
            peak_months=peak_months, weak_months=weak_months,
            category=cat, description=desc,
            sparkline_data=[round(float(s), 1) for s in full_series],
        )
        all_keyword_metrics.append(metrics)

    # ─── 인사이트 카드 ───
    steady_kws = sorted([k for k in all_keyword_metrics if k.category == "steady"],
                        key=_steady_score, reverse=True)[:8]
    seasonal_kws = sorted([k for k in all_keyword_metrics if k.category == "seasonal"],
                          key=lambda k: k.confidence, reverse=True)[:8]
    rising_kws = sorted([k for k in all_keyword_metrics if k.category == "rising"],
                        key=lambda k: k.trend_score, reverse=True)[:6]
    caution_kws = sorted([k for k in all_keyword_metrics if k.category == "caution"],
                         key=lambda k: k.trend_score)[:6]

    insight_cards = []
    if steady_kws:
        insight_cards.append(InsightCard(
            card_type="steady", title="꾸준히 스테디하게 판매하기 좋은 키워드",
            description="5년 구간에서 반복 등장 빈도와 안정성을 함께 고려해 오래 가져가기 좋은 키워드를 추렸습니다.",
            keywords=steady_kws,
        ))
    if seasonal_kws:
        insight_cards.append(InsightCard(
            card_type="seasonal", title="계절 반복 키워드",
            description="같은 시즌에 여러 해 반복해서 강세를 보인 키워드를 찾았습니다.",
            keywords=seasonal_kws,
        ))
    if rising_kws:
        insight_cards.append(InsightCard(
            card_type="rising", title="최근 급상승 키워드",
            description="최근 트렌드가 뚜렷하게 상승하고 있는 키워드입니다.",
            keywords=rising_kws,
        ))
    if caution_kws:
        insight_cards.append(InsightCard(
            card_type="caution", title="주의가 필요한 키워드",
            description="하락 추세이거나 변동성이 큰 키워드로, 신중한 접근이 필요합니다.",
            keywords=caution_kws,
        ))

    # ─── 히어로 메트릭스 ───
    hero = HeroMetrics()
    if steady_kws:
        hero.steady_anchor = steady_kws[0].keyword
    if seasonal_kws:
        hero.season_window = seasonal_kws[0].keyword
    if rising_kws:
        hero.prepare_now = rising_kws[0].keyword
    if caution_kws:
        hero.caution = caution_kws[0].keyword

    # ─── 히트맵 ───
    heatmap_kws = sorted(all_keyword_metrics, key=lambda k: k.confidence, reverse=True)[:8]
    heatmap = []
    for kw in heatmap_kws:
        m_avg = _get_monthly_average_scores(keyword_series[kw.keyword]["scores"])
        peak_strs = [f"{m}월" for m in kw.peak_months[:2]]
        weak_strs = [f"{m}월" for m in kw.weak_months[:2]]
        d = f"평균적으로 {', '.join(peak_strs)}에 강하고, 약세 월은 {', '.join(weak_strs)}입니다."
        heatmap.append(HeatmapRow(keyword=kw.keyword, description=d, monthly_scores=m_avg))

    # ─── 월별 플래너 ───
    planner = []
    for month in range(1, 13):
        recommend = [k.keyword for k in sorted(all_keyword_metrics, key=lambda x: x.confidence, reverse=True) if month in x.peak_months][:2]
        caution = [k.keyword for k in sorted(all_keyword_metrics, key=lambda x: x.confidence, reverse=True) if month in x.weak_months][:2]
        planner.append(MonthlyPlan(
            month=month, label=MONTH_LABELS[month] if month < len(MONTH_LABELS) else "",
            recommend_keywords=recommend, caution_keywords=caution,
        ))

    # ─── 드릴다운 ───
    drilldowns = sorted(all_keyword_metrics, key=lambda k: k.confidence, reverse=True)[:15]

    return {
        "hero_metrics": hero, "insight_cards": insight_cards,
        "heatmap": heatmap, "monthly_planner": planner, "keyword_drilldowns": drilldowns,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LLM 리포트 생성
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def generate_insight_report(
    category_name: str, hero_metrics: HeroMetrics, top_keywords: list[KeywordMetrics],
) -> Optional[str]:
    if not cfg.ANTHROPIC_API_KEY:
        return _fallback_report(hero_metrics, top_keywords)

    keyword_summary = []
    for kw in top_keywords[:10]:
        keyword_summary.append({
            "키워드": kw.keyword, "신뢰도": kw.confidence, "추세": kw.trend_score,
            "유형": kw.category, "추천월": [f"{m}월" for m in kw.peak_months],
            "주의월": [f"{m}월" for m in kw.weak_months], "등장개월": kw.appearance_count,
        })

    prompt = f"""당신은 이커머스 셀러를 위한 트렌드 분석 전문가입니다.
아래는 네이버 쇼핑 "{category_name}" 카테고리의 최근 5년간 트렌드 분석 결과입니다.

핵심 키워드 데이터:
{keyword_summary}

히어로 지표:
- 가장 스테디한 키워드: {hero_metrics.steady_anchor}
- 지금 준비할 키워드: {hero_metrics.prepare_now}
- 계절 윈도우: {hero_metrics.season_window}
- 주의 키워드: {hero_metrics.caution}

위 데이터를 바탕으로 이커머스 셀러에게 실질적으로 도움되는 실무 추천 리포트를 작성해주세요.

규칙:
1. 정확히 3~4줄로 작성 (각 줄은 마침표로 끝남)
2. 구체적인 키워드명과 수치를 언급
3. "~하는 것이 좋습니다" 등 행동 지향적 문장
4. 한국어로 작성
5. 마크다운 없이 순수 텍스트만"""

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=cfg.ANTHROPIC_API_KEY)
        message = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except Exception as e:
        logger.error(f"Claude API 호출 실패: {e}")
        return _fallback_report(hero_metrics, top_keywords)


def _fallback_report(hero: HeroMetrics, keywords: list[KeywordMetrics]) -> str:
    lines = []
    if hero.steady_anchor:
        lines.append(f"가장 스테디한 핵심 키워드는 {hero.steady_anchor}입니다.")
    seasonal = [k for k in keywords if k.category == "seasonal"]
    if seasonal:
        lines.append(f"{seasonal[0].keyword}은 같은 시즌에 여러 해 반복된 계절형 강세를 보여줍니다.")
    if hero.prepare_now:
        lines.append(f"지금 준비 키워드로 {hero.prepare_now}을 먼저 보는 것이 좋습니다.")
    return "\n".join(lines) if lines else "분석 데이터가 충분하지 않아 리포트를 생성할 수 없습니다."


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SSE 진행 상태 관리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_run_events: dict[str, dict] = {}


def _update_run_status(run_id: str, data: dict):
    _run_events[run_id] = data


def _get_run_status(run_id: str) -> dict:
    return _run_events.get(run_id, {})


async def _sse_progress_generator(run_id: str) -> AsyncGenerator[str, None]:
    last_data = None
    while True:
        data = _get_run_status(run_id)
        if data and data != last_data:
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            last_data = data.copy()
            if data.get("status") in ("completed", "failed", "cancelled"):
                yield f"data: {json.dumps({'status': 'done'})}\n\n"
                break
        await asyncio.sleep(1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  수집 워커 (백그라운드)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _start_collection(run_id: str, profile_id: str):
    """백그라운드 수집 작업"""
    conn = get_connection()
    try:
        # 프로필 로드
        row = conn.execute("SELECT * FROM dl_profiles WHERE id=?", (profile_id,)).fetchone()
        if not row:
            logger.error(f"프로필 없음: {profile_id}")
            return

        profile_data = dict(row)
        ages_list = json.loads(profile_data.get("ages", "[]"))

        # Run 시작
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE dl_runs SET status='running', started_at=? WHERE id=?", (now_str, run_id))
        conn.commit()

        _update_run_status(run_id, {
            "status": "running", "completed_tasks": 0,
            "total_tasks": 0, "current_period": "",
        })

        # 태스크 목록
        tasks = conn.execute(
            "SELECT * FROM dl_tasks WHERE run_id=? ORDER BY period", (run_id,)
        ).fetchall()
        total_tasks = len(tasks)

        completed = 0
        failed = 0
        total_snapshots = 0
        task_times = []

        for task_row in tasks:
            task = dict(task_row)

            # 취소 확인
            run_check = conn.execute("SELECT status FROM dl_runs WHERE id=?", (run_id,)).fetchone()
            if run_check and dict(run_check)["status"] == "cancelled":
                break

            task_start = time.time()
            period = task["period"]
            task_id = task["id"]

            conn.execute("UPDATE dl_tasks SET status='running' WHERE id=?", (task_id,))
            conn.execute("UPDATE dl_runs SET current_period=? WHERE id=?", (period, run_id))
            conn.commit()

            _update_run_status(run_id, {
                "status": "running", "completed_tasks": completed,
                "total_tasks": total_tasks, "current_period": period,
                "failed_tasks": failed, "total_snapshots": total_snapshots,
            })

            try:
                # DB 캐시 확인
                cached = conn.execute(
                    "SELECT COUNT(*) as cnt FROM dl_snapshots WHERE profile_id=? AND period=?",
                    (profile_id, period)
                ).fetchone()
                cache_cnt = dict(cached)["cnt"] if cached else 0

                if cache_cnt > 0:
                    conn.execute(
                        "UPDATE dl_tasks SET status='cached', snapshot_count=?, completed_at=? WHERE id=?",
                        (cache_cnt, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), task_id)
                    )
                    conn.commit()
                    total_snapshots += cache_cnt
                    completed += 1
                    logger.info(f"캐시 사용: {period} ({cache_cnt}건)")
                    continue

                # 네이버 API 호출
                result_count = profile_data.get("result_count", 20)
                pages_needed = 1 if result_count <= 20 else 2
                all_keywords = []

                for page in range(1, pages_needed + 1):
                    keywords = await fetch_shopping_insight_keywords(
                        category_id=profile_data["category_id"],
                        period=period,
                        device=profile_data.get("device", ""),
                        gender=profile_data.get("gender", ""),
                        ages=ages_list,
                        page=page,
                        count=TREND_PAGE_SIZE,
                    )
                    all_keywords.extend(keywords)
                    await asyncio.sleep(NAVER_API_RATE_LIMIT)

                # 브랜드 필터링 + DB 저장
                brand_excl = bool(profile_data.get("brand_exclusion", 1))
                custom_terms = profile_data.get("custom_exclusion_terms", "")

                for kw in all_keywords:
                    excluded = apply_brand_exclusion(kw["keyword"], brand_excl, custom_terms)
                    snap_id = str(uuid.uuid4())
                    conn.execute("""
                        INSERT INTO dl_snapshots (id, profile_id, period, rank, keyword, click_count,
                            device, gender, ages, brand_excluded)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (
                        snap_id, profile_id, period, kw["rank"], kw["keyword"],
                        kw.get("click_count", 0), profile_data.get("device", ""),
                        profile_data.get("gender", ""), json.dumps(ages_list),
                        1 if excluded else 0,
                    ))

                conn.execute(
                    "UPDATE dl_tasks SET status='completed', snapshot_count=?, completed_at=? WHERE id=?",
                    (len(all_keywords), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), task_id)
                )
                conn.commit()
                total_snapshots += len(all_keywords)
                completed += 1

            except Exception as e:
                logger.error(f"태스크 실패 [{period}]: {e}")
                conn.execute(
                    "UPDATE dl_tasks SET status='failed', failure_reason=? WHERE id=?",
                    (str(e)[:500], task_id)
                )
                conn.commit()
                failed += 1

            task_duration = time.time() - task_start
            task_times.append(task_duration)
            remaining = total_tasks - completed - failed
            avg_dur = sum(task_times) / len(task_times) if task_times else 2
            eta = remaining * avg_dur

            _update_run_status(run_id, {
                "status": "running", "completed_tasks": completed,
                "total_tasks": total_tasks, "current_period": period,
                "failed_tasks": failed, "total_snapshots": total_snapshots,
                "eta_seconds": round(eta, 1), "avg_task_duration": round(avg_dur, 2),
            })

        # ─── 수집 완료 → 분석 ───
        final_status = "completed" if failed < total_tasks else "failed"
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        analysis_json = None
        if final_status == "completed":
            analysis_json = await _run_analysis(conn, profile_data, profile_id, result_count)

        conn.execute("""
            UPDATE dl_runs SET status=?, completed_tasks=?, failed_tasks=?,
                total_snapshots=?, completed_at=?, analysis_result=? WHERE id=?
        """, (final_status, completed, failed, total_snapshots, now_str,
              json.dumps(analysis_json, ensure_ascii=False) if analysis_json else None, run_id))

        conn.execute("UPDATE dl_profiles SET latest_run_id=? WHERE id=?", (run_id, profile_id))
        conn.commit()

        _update_run_status(run_id, {
            "status": final_status, "completed_tasks": completed,
            "total_tasks": total_tasks, "current_period": "",
            "failed_tasks": failed, "total_snapshots": total_snapshots,
        })

        logger.info(f"수집 완료: Run {run_id} - {completed}/{total_tasks} 성공")

    except Exception as e:
        logger.error(f"수집 전체 실패: {e}")
        conn.execute("UPDATE dl_runs SET status='failed' WHERE id=?", (run_id,))
        conn.commit()
        _update_run_status(run_id, {"status": "failed", "error": str(e)})
    finally:
        conn.close()


async def _run_analysis(conn, profile_data: dict, profile_id: str, result_count: int = 20) -> dict:
    """수집 완료 후 분석 실행"""
    rows = conn.execute(
        "SELECT period, rank, keyword, click_count FROM dl_snapshots WHERE profile_id=? AND brand_excluded=0 ORDER BY period, rank",
        (profile_id,)
    ).fetchall()

    snapshots_dict = [
        {"period": dict(r)["period"], "rank": dict(r)["rank"],
         "keyword": dict(r)["keyword"], "click_count": dict(r)["click_count"]}
        for r in rows
    ]

    periods = generate_period_list()
    analysis = build_analysis(snapshots_dict, periods, result_count)

    # LLM 리포트
    try:
        llm_report = await generate_insight_report(
            category_name=profile_data.get("category_name", ""),
            hero_metrics=analysis["hero_metrics"],
            top_keywords=analysis["keyword_drilldowns"][:10],
        )
        analysis["llm_report"] = llm_report
    except Exception as e:
        logger.error(f"LLM 리포트 실패: {e}")
        analysis["llm_report"] = None

    # Pydantic → dict 변환
    return {
        "hero_metrics": analysis["hero_metrics"].model_dump(),
        "llm_report": analysis.get("llm_report"),
        "insight_cards": [c.model_dump() for c in analysis["insight_cards"]],
        "heatmap": [h.model_dump() for h in analysis["heatmap"]],
        "monthly_planner": [m.model_dump() for m in analysis["monthly_planner"]],
        "keyword_drilldowns": [k.model_dump() for k in analysis["keyword_drilldowns"]],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  API 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.get("/categories")
async def get_categories():
    categories = [{"id": k, "name": v, "depth": 1, "parent_id": None}
                  for k, v in CATEGORY_TREE.items()]
    return {"categories": categories}


@router.post("/collect")
async def collect_trends(req: CollectRequest, background_tasks: BackgroundTasks):
    """트렌드 분석 시작"""
    parts = [req.category_id, req.device or "all", req.gender or "all",
             ",".join(sorted(req.ages)) or "all", str(req.result_count), str(req.brand_exclusion)]
    slug = hashlib.md5("|".join(parts).encode()).hexdigest()[:16]

    conn = get_connection()
    try:
        # 기존 프로필 확인
        row = conn.execute("SELECT id FROM dl_profiles WHERE slug=?", (slug,)).fetchone()
        if row:
            profile_id = dict(row)["id"]
        else:
            profile_id = str(uuid.uuid4())
            conn.execute("""
                INSERT INTO dl_profiles (id, slug, category_id, category_name, category_path,
                    device, gender, ages, result_count, brand_exclusion, custom_exclusion_terms)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (profile_id, slug, req.category_id, req.category_name, req.category_path,
                  req.device, req.gender, json.dumps(req.ages), req.result_count,
                  1 if req.brand_exclusion else 0, req.custom_exclusion_terms))
            conn.commit()

        # 진행 중인 Run 확인
        active = conn.execute(
            "SELECT id, status, total_tasks FROM dl_runs WHERE profile_id=? AND status IN ('queued','running')",
            (profile_id,)
        ).fetchone()
        if active:
            a = dict(active)
            return {
                "run_id": a["id"], "profile_id": profile_id,
                "status": a["status"], "total_tasks": a["total_tasks"],
                "message": "이미 진행 중인 분석이 있습니다.",
            }

        # 기간 목록
        periods = generate_period_list()

        # 새 Run
        run_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO dl_runs (id, profile_id, status, total_tasks) VALUES (?,?,?,?)
        """, (run_id, profile_id, "queued", len(periods)))

        # 월별 태스크
        for period in periods:
            task_id = str(uuid.uuid4())
            conn.execute("""
                INSERT INTO dl_tasks (id, run_id, profile_id, period, status) VALUES (?,?,?,?,?)
            """, (task_id, run_id, profile_id, period, "pending"))

        conn.commit()

        # 백그라운드 수집 시작
        background_tasks.add_task(_start_collection, run_id, profile_id)

        return {
            "run_id": run_id, "profile_id": profile_id,
            "status": "queued", "total_tasks": len(periods),
            "message": f"분석을 시작합니다. {len(periods)}개월 데이터를 수집합니다.",
        }
    finally:
        conn.close()


@router.get("/runs/{run_id}/stream")
async def stream_run_progress(run_id: str):
    """SSE 스트림"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM dl_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run을 찾을 수 없습니다")
    finally:
        conn.close()

    return StreamingResponse(
        _sse_progress_generator(run_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    """Run 상태 조회"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM dl_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run을 찾을 수 없습니다")
        r = dict(row)
        return {
            "run_id": r["id"], "profile_id": r["profile_id"],
            "status": r["status"], "total_tasks": r["total_tasks"],
            "completed_tasks": r["completed_tasks"], "failed_tasks": r["failed_tasks"],
            "total_snapshots": r["total_snapshots"], "current_period": r["current_period"] or "",
            "started_at": r.get("started_at"), "completed_at": r.get("completed_at"),
        }
    finally:
        conn.close()


@router.get("/runs/{run_id}/analysis")
async def get_analysis(run_id: str):
    """분석 결과 조회"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM dl_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run을 찾을 수 없습니다")
        r = dict(row)
        if r["status"] != "completed":
            raise HTTPException(400, f"분석이 아직 완료되지 않았습니다 (상태: {r['status']})")
        if not r.get("analysis_result"):
            raise HTTPException(404, "분석 결과가 없습니다")

        analysis = json.loads(r["analysis_result"]) if isinstance(r["analysis_result"], str) else r["analysis_result"]
        return {"run_id": run_id, "profile_id": r["profile_id"], **analysis}
    finally:
        conn.close()


@router.get("/runs/{run_id}/snapshots")
async def get_snapshots(run_id: str, period: str = None):
    """월별 스냅샷 미리보기"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT profile_id FROM dl_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run을 찾을 수 없습니다")
        profile_id = dict(row)["profile_id"]

        # 사용 가능한 기간 목록
        periods_rows = conn.execute(
            "SELECT DISTINCT period FROM dl_snapshots WHERE profile_id=? ORDER BY period DESC",
            (profile_id,)
        ).fetchall()
        available = [dict(r)["period"] for r in periods_rows]

        if not available:
            return {"period": "", "snapshots": [], "total_periods": 0, "current_page": 0}

        target = period or available[0]
        page = available.index(target) + 1 if target in available else 1

        snaps = conn.execute(
            "SELECT period, rank, keyword, click_count, brand_excluded FROM dl_snapshots WHERE profile_id=? AND period=? AND brand_excluded=0 ORDER BY rank",
            (profile_id, target)
        ).fetchall()

        return {
            "period": target,
            "snapshots": [
                {"period": dict(s)["period"], "rank": dict(s)["rank"],
                 "keyword": dict(s)["keyword"], "click_count": dict(s)["click_count"],
                 "brand_excluded": bool(dict(s)["brand_excluded"])}
                for s in snaps
            ],
            "total_periods": len(available),
            "current_page": page,
        }
    finally:
        conn.close()


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str):
    conn = get_connection()
    try:
        row = conn.execute("SELECT status FROM dl_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run을 찾을 수 없습니다")
        if dict(row)["status"] not in ("queued", "running"):
            raise HTTPException(400, "이미 완료/취소된 작업입니다")

        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE dl_runs SET status='cancelled', completed_at=? WHERE id=?", (now_str, run_id))
        conn.commit()
        return {"message": "작업이 취소되었습니다", "run_id": run_id}
    finally:
        conn.close()


@router.delete("/runs/{run_id}")
async def delete_run(run_id: str):
    conn = get_connection()
    try:
        row = conn.execute("SELECT profile_id FROM dl_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run을 찾을 수 없습니다")
        profile_id = dict(row)["profile_id"]

        conn.execute("DELETE FROM dl_tasks WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM dl_snapshots WHERE profile_id=?", (profile_id,))
        conn.execute("DELETE FROM dl_runs WHERE id=?", (run_id,))
        conn.execute("DELETE FROM dl_profiles WHERE id=?", (profile_id,))
        conn.commit()
        return {"message": "삭제되었습니다"}
    finally:
        conn.close()


@router.get("/archive")
async def get_archive():
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT r.id as run_id, r.profile_id, r.completed_at, r.total_tasks,
                   p.category_path, p.category_name, p.result_count, p.brand_exclusion
            FROM dl_runs r
            JOIN dl_profiles p ON r.profile_id = p.id
            WHERE r.status = 'completed'
            ORDER BY r.completed_at DESC LIMIT 20
        """).fetchall()

        periods = generate_period_list()
        period_range = f"{periods[0]} ~ {periods[-1]}" if periods else ""

        items = []
        for row in rows:
            r = dict(row)
            items.append({
                "run_id": r["run_id"],
                "profile_id": r["profile_id"],
                "category_path": r["category_path"] or r["category_name"],
                "result_count": r["result_count"],
                "period_range": period_range,
                "completed_at": r["completed_at"],
                "keyword_type": "브랜드 제외" if r["brand_exclusion"] else "원본 키워드",
            })
        return items
    finally:
        conn.close()
