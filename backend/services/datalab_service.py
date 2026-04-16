"""
네이버 DataLab Shopping Insight API 클라이언트 및 분석 서비스

주요 기능:
1. 8개 쇼핑 인사이트 엔드포인트 호출
2. 검색 트렌드 분석
3. 24시간 TTL 캐싱 (DB 기반)
4. 신뢰도/계절성/모멘텀 스코링
5. 카테고리 코드 트리 관리
6. API 레이트 제한 (1000/일, 최대 5키워드/요청, 최대 3카테고리/요청)
7. 관련 키워드 제안
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
import httpx

from db.database import get_connection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  상수 정의
# ─────────────────────────────────────────
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")

# 쇼핑 인사이트 API 기본 URL
DATALAB_API_BASE = "https://openapi.naver.com/v1/datalab/shopping"
SEARCH_API_BASE = "https://openapi.naver.com/v1/search"

# 카테고리 계층 조회 (Naver DataLab 내부 API)
CATEGORY_TREE_API = "https://datalab.naver.com/shoppingInsight/getCategory.naver"

# 캐시 TTL (24시간)
CACHE_TTL_HOURS = 24

# API 레이트 제한
RATE_LIMIT_PER_DAY = 1000
MAX_KEYWORDS_PER_REQUEST = 5
MAX_CATEGORIES_PER_REQUEST = 3

# 1차 카테고리 (고정값)
CATEGORIES_L1 = [
    {"cid": "50000000", "name": "패션의류"},
    {"cid": "50000001", "name": "패션잡화"},
    {"cid": "50000002", "name": "화장품/미용"},
    {"cid": "50000003", "name": "디지털/가전"},
    {"cid": "50000004", "name": "가구/인테리어"},
    {"cid": "50000005", "name": "출산/육아"},
    {"cid": "50000006", "name": "식품"},
    {"cid": "50000007", "name": "스포츠/레저"},
    {"cid": "50000008", "name": "생활/건강"},
    {"cid": "50000009", "name": "여가/생활편의"},
    {"cid": "50000010", "name": "면세점"},
    {"cid": "50000011", "name": "도서"},
]


# ─────────────────────────────────────────
#  데이터 클래스
# ─────────────────────────────────────────
@dataclass
class TrendDataPoint:
    """트렌드 데이터 포인트"""
    period: str  # YYYY-MM-DD
    ratio: float  # 검색 비율


@dataclass
class ScoreResult:
    """스코링 결과"""
    trust_score: float  # 0-100: 최근 12개월 중 ratio >= 50인 개월 비율
    seasonality: Dict[str, Any]  # peak_months, low_months, seasonal_pattern
    momentum: Dict[str, Any]  # trend (급상승/상승/보합/하락), value (%)
    overall_score: float  # 0-100: 종합 점수


@dataclass
class TrendAnalysisResult:
    """트렌드 분석 결과"""
    category_code: str
    category_name: str
    keywords: List[str]
    date_range: Dict[str, str]  # startDate, endDate
    time_unit: str
    device_filter: str
    gender_filter: str
    ages_filter: List[str]
    trend_data: List[Dict[str, Any]]  # [{ title, category, keyword, data, scores }]
    cache_used: bool
    api_call_count: int
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ─────────────────────────────────────────
#  DB 초기화
# ─────────────────────────────────────────
def init_datalab_tables():
    """DataLab 관련 테이블 생성"""
    conn = get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS datalab_trend_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key TEXT UNIQUE NOT NULL,
                endpoint TEXT NOT NULL,
                request_body TEXT NOT NULL,
                response_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS datalab_category_cache (
                parent_cid TEXT NOT NULL,
                cid TEXT NOT NULL,
                name TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (parent_cid, cid)
            );

            CREATE TABLE IF NOT EXISTS datalab_analysis_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                emp_cd TEXT,
                category_code TEXT,
                category_name TEXT,
                keywords TEXT,
                filters TEXT,
                trend_data TEXT,
                ai_insight TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS datalab_seed_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_code TEXT NOT NULL,
                keyword TEXT NOT NULL,
                search_count INTEGER DEFAULT 1,
                last_score REAL,
                last_momentum TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(category_code, keyword)
            );

            CREATE TABLE IF NOT EXISTS datalab_brand_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                emp_cd TEXT NOT NULL,
                brand_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(emp_cd, brand_name)
            );

            CREATE INDEX IF NOT EXISTS idx_trend_cache_key ON datalab_trend_cache(cache_key);
            CREATE INDEX IF NOT EXISTS idx_trend_cache_expires ON datalab_trend_cache(expires_at);
            CREATE INDEX IF NOT EXISTS idx_category_parent ON datalab_category_cache(parent_cid);
            CREATE INDEX IF NOT EXISTS idx_analysis_emp ON datalab_analysis_history(emp_cd);
            CREATE INDEX IF NOT EXISTS idx_analysis_created ON datalab_analysis_history(created_at);
            CREATE INDEX IF NOT EXISTS idx_seed_category ON datalab_seed_keywords(category_code);
            CREATE INDEX IF NOT EXISTS idx_blacklist_emp ON datalab_brand_blacklist(emp_cd);
        """)
        conn.commit()
        logger.info("[DataLab] 테이블 초기화 완료")
    except Exception as e:
        logger.error(f"[DataLab] 테이블 초기화 실패: {e}", exc_info=True)
    finally:
        conn.close()


# ─────────────────────────────────────────
#  캐시 관리
# ─────────────────────────────────────────
def _make_cache_key(endpoint: str, request_body: Dict) -> str:
    """캐시 키 생성"""
    body_str = json.dumps(request_body, sort_keys=True, default=str)
    combined = f"{endpoint}:{body_str}"
    return hashlib.md5(combined.encode()).hexdigest()


def _get_cached_response(cache_key: str) -> Optional[Dict]:
    """캐시에서 응답 조회"""
    try:
        conn = get_connection()
        cursor = conn.execute(
            """SELECT response_data FROM datalab_trend_cache
               WHERE cache_key = ? AND expires_at > datetime('now')""",
            (cache_key,)
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            logger.debug(f"[DataLab] 캐시 히트: {cache_key}")
            return json.loads(row[0])
        return None
    except Exception as e:
        logger.error(f"[DataLab] 캐시 조회 오류: {e}")
        return None


def _save_to_cache(cache_key: str, endpoint: str, request_body: Dict, response_data: Dict):
    """응답을 캐시에 저장"""
    try:
        conn = get_connection()
        expires_at = datetime.now(timezone.utc) + timedelta(hours=CACHE_TTL_HOURS)

        conn.execute(
            """INSERT OR REPLACE INTO datalab_trend_cache
               (cache_key, endpoint, request_body, response_data, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                cache_key,
                endpoint,
                json.dumps(request_body, default=str),
                json.dumps(response_data, default=str),
                expires_at.isoformat()
            )
        )
        conn.commit()
        conn.close()
        logger.debug(f"[DataLab] 캐시 저장 완료: {cache_key}")
    except Exception as e:
        logger.error(f"[DataLab] 캐시 저장 오류: {e}")


def _cleanup_expired_cache():
    """만료된 캐시 정리"""
    try:
        conn = get_connection()
        cursor = conn.execute(
            """DELETE FROM datalab_trend_cache WHERE expires_at <= datetime('now')"""
        )
        count = cursor.rowcount
        conn.commit()
        conn.close()
        if count > 0:
            logger.info(f"[DataLab] 만료된 캐시 {count}개 정리 완료")
    except Exception as e:
        logger.error(f"[DataLab] 캐시 정리 오류: {e}")


# ─────────────────────────────────────────
#  API 호출
# ─────────────────────────────────────────
def _get_headers() -> Dict[str, str]:
    """Naver API 헤더"""
    return {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        "Content-Type": "application/json",
    }


async def _call_datalab_api(
    endpoint: str,
    request_body: Dict,
    use_cache: bool = True
) -> Optional[Dict]:
    """
    DataLab API 호출 (캐시 지원)

    Args:
        endpoint: /keywords, /category 등 (기본 URL 제외)
        request_body: 요청 본문
        use_cache: 캐시 사용 여부

    Returns:
        API 응답 또는 None
    """
    try:
        cache_key = _make_cache_key(endpoint, request_body)

        # 캐시 확인
        if use_cache:
            cached = _get_cached_response(cache_key)
            if cached:
                return cached

        # API 호출
        url = f"{DATALAB_API_BASE}{endpoint}"
        headers = _get_headers()

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(url, json=request_body, headers=headers)

            if response.status_code != 200:
                logger.error(f"[DataLab] API 오류 ({endpoint}): {response.status_code} - {response.text}")
                return None

            data = response.json()

            # 캐시 저장
            if use_cache:
                _save_to_cache(cache_key, endpoint, request_body, data)

            return data

    except Exception as e:
        logger.error(f"[DataLab] API 호출 실패 ({endpoint}): {e}", exc_info=True)
        return None


async def _call_search_api(query: str, display: int = 10) -> Optional[List[str]]:
    """
    Naver 쇼핑 검색 API를 통한 키워드 제안

    Args:
        query: 검색어
        display: 결과 개수 (기본 10)

    Returns:
        제안된 키워드 리스트 또는 None
    """
    try:
        url = f"{SEARCH_API_BASE}/shop.json"
        headers = _get_headers()
        params = {"query": query, "display": min(display, 100)}

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params, headers=headers)

            if response.status_code != 200:
                logger.warning(f"[DataLab] 검색 API 오류: {response.status_code}")
                return None

            data = response.json()
            items = data.get("items", [])

            # 상품명에서 키워드 추출 (중복 제거)
            keywords = []
            seen = set()
            for item in items:
                title = item.get("title", "").replace("<b>", "").replace("</b>", "")
                if title and title not in seen:
                    keywords.append(title)
                    seen.add(title)

            return keywords[:display]

    except Exception as e:
        logger.warning(f"[DataLab] 검색 API 호출 실패: {e}")
        return None


# ─────────────────────────────────────────
#  카테고리 관리
# ─────────────────────────────────────────
async def get_subcategories(parent_cid: str) -> List[Dict[str, str]]:
    """
    부모 카테고리의 하위 카테고리 조회

    1. DB 캐시 확인
    2. 없으면 Naver DataLab 내부 API에서 조회
    3. DB에 저장

    Args:
        parent_cid: 부모 카테고리 코드

    Returns:
        [{"cid": "...", "name": "..."}, ...] 형태의 리스트
    """
    try:
        # DB 캐시 확인
        conn = get_connection()
        cursor = conn.execute(
            """SELECT cid, name FROM datalab_category_cache
               WHERE parent_cid = ?
               ORDER BY cid""",
            (parent_cid,)
        )
        rows = cursor.fetchall()
        conn.close()

        if rows:
            logger.debug(f"[DataLab] 카테고리 캐시 히트: parent_cid={parent_cid}")
            return [{"cid": row[0], "name": row[1]} for row in rows]

        # Naver DataLab API에서 조회
        logger.info(f"[DataLab] 카테고리 API 호출: parent_cid={parent_cid}")

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                CATEGORY_TREE_API,
                data={"cid": parent_cid},
                headers={"User-Agent": "Mozilla/5.0"}
            )

            if response.status_code != 200:
                logger.warning(f"[DataLab] 카테고리 API 실패: {response.status_code}")
                return []

            # HTML 응답에서 JSON 파싱
            text = response.text
            import re

            # <select> 내 <option> 파싱
            pattern = r'<option value="(\d+)"[^>]*>([^<]+)</option>'
            matches = re.findall(pattern, text)

            if not matches:
                logger.warning(f"[DataLab] 카테고리 파싱 실패")
                return []

            subcategories = [
                {"cid": cid, "name": name}
                for cid, name in matches
            ]

            # DB에 저장
            if subcategories:
                conn = get_connection()
                conn.executemany(
                    """INSERT OR IGNORE INTO datalab_category_cache
                       (parent_cid, cid, name)
                       VALUES (?, ?, ?)""",
                    [(parent_cid, sub["cid"], sub["name"]) for sub in subcategories]
                )
                conn.commit()
                conn.close()
                logger.info(f"[DataLab] 카테고리 {len(subcategories)}개 저장")

            return subcategories

    except Exception as e:
        logger.error(f"[DataLab] 하위 카테고리 조회 실패: {e}", exc_info=True)
        return []


def get_category_name(cid: str) -> str:
    """카테고리 코드로 카테고리명 조회 (1차만 지원)"""
    for cat in CATEGORIES_L1:
        if cat["cid"] == cid:
            return cat["name"]
    return "알 수 없음"


# ─────────────────────────────────────────
#  스코링 알고리즘
# ─────────────────────────────────────────
def _calculate_trust_score(data_points: List[TrendDataPoint]) -> float:
    """
    신뢰도 점수 계산

    최근 12개월 중 ratio >= 50인 개월 비율
    score = (count / 12) * 100
    """
    if not data_points or len(data_points) < 12:
        return 0.0

    # 최근 12개월만 선택
    recent = data_points[-12:]
    count = sum(1 for p in recent if p.ratio >= 50)
    score = (count / 12) * 100
    return round(score, 2)


def _calculate_seasonality(data_points: List[TrendDataPoint]) -> Dict[str, Any]:
    """
    계절성 분석

    - 전체 데이터를 월(1-12)별로 그룹화
    - 월별 평균 계산
    - Peak months: 평균 대비 >= 120%
    - Low months: 평균 대비 < 60%
    """
    if not data_points:
        return {
            "has_seasonality": False,
            "peak_months": [],
            "low_months": [],
            "pattern": "insufficient_data"
        }

    # 월별 데이터 그룹화
    monthly_data = {}
    for point in data_points:
        try:
            # period 형식: YYYY-MM-DD
            month = int(point.period[5:7])
            if month not in monthly_data:
                monthly_data[month] = []
            monthly_data[month].append(point.ratio)
        except:
            pass

    if len(monthly_data) < 6:
        return {
            "has_seasonality": False,
            "peak_months": [],
            "low_months": [],
            "pattern": "insufficient_months"
        }

    # 월별 평균 계산
    monthly_avg = {month: sum(vals) / len(vals) for month, vals in monthly_data.items()}
    overall_avg = sum(monthly_avg.values()) / len(monthly_avg)

    # Peak/Low 월 판정
    peak_months = [m for m, avg in monthly_avg.items() if avg >= overall_avg * 1.2]
    low_months = [m for m, avg in monthly_avg.items() if avg < overall_avg * 0.6]

    has_seasonality = len(peak_months) > 0 or len(low_months) > 0

    return {
        "has_seasonality": has_seasonality,
        "peak_months": sorted(peak_months),
        "low_months": sorted(low_months),
        "overall_avg": round(overall_avg, 2),
        "monthly_avg": {str(m): round(avg, 2) for m, avg in monthly_avg.items()}
    }


def _calculate_momentum(data_points: List[TrendDataPoint]) -> Dict[str, Any]:
    """
    모멘텀 분석

    최근 3개월 평균 vs 이전 3개월 평균 비교
    - 급상승: > +20%
    - 상승: +5~+20%
    - 보합: -5~+5%
    - 하락: < -5%
    """
    if not data_points or len(data_points) < 6:
        return {
            "trend": "insufficient_data",
            "value": 0,
            "description": "데이터 부족"
        }

    # 최근 6개월 데이터 (3개월씩 2개 그룹)
    recent_6m = data_points[-6:]
    last_3m = [p.ratio for p in recent_6m[-3:]]
    prev_3m = [p.ratio for p in recent_6m[:3]]

    if not last_3m or not prev_3m:
        return {
            "trend": "insufficient_data",
            "value": 0,
            "description": "데이터 부족"
        }

    last_avg = sum(last_3m) / len(last_3m)
    prev_avg = sum(prev_3m) / len(prev_3m)

    if prev_avg == 0:
        change_pct = 0
    else:
        change_pct = ((last_avg - prev_avg) / prev_avg) * 100

    # 트렌드 판정
    if change_pct > 20:
        trend = "급상승"
    elif change_pct >= 5:
        trend = "상승"
    elif change_pct >= -5:
        trend = "보합"
    else:
        trend = "하락"

    return {
        "trend": trend,
        "value": round(change_pct, 2),
        "description": f"{change_pct:+.2f}%"
    }


def _score_keyword(data_points: List[TrendDataPoint]) -> ScoreResult:
    """키워드에 대한 종합 스코어 계산"""
    trust = _calculate_trust_score(data_points)
    seasonality = _calculate_seasonality(data_points)
    momentum = _calculate_momentum(data_points)

    # 종합 점수 (0-100)
    # Trust 40% + Seasonality의 Peak 여부 30% + Momentum 30%
    seasonal_score = 100 if seasonality.get("has_seasonality") else 50
    momentum_value = abs(momentum.get("value", 0))
    momentum_score = min(100, momentum_value * 2)  # 절댓값 활용, max 100

    overall = (trust * 0.4) + (seasonal_score * 0.3) + (momentum_score * 0.3)
    overall = round(min(100, max(0, overall)), 2)

    return ScoreResult(
        trust_score=trust,
        seasonality=seasonality,
        momentum=momentum,
        overall_score=overall
    )


# ─────────────────────────────────────────
#  주요 분석 함수
# ─────────────────────────────────────────
async def analyze_keywords(
    category_code: str,
    keywords: List[str],
    start_date: str,  # YYYY-MM-DD
    end_date: str,    # YYYY-MM-DD
    time_unit: str = "date",  # date, week, month
    device: str = "",  # "", "pc", "mo"
    gender: str = "",  # "", "m", "f"
    ages: List[str] = None,  # ["10", "20", "30", ...]
    use_cache: bool = True,
    emp_cd: str = None
) -> Optional[TrendAnalysisResult]:
    """
    키워드별 트렌드 분석

    Args:
        category_code: 카테고리 코드 (e.g., "50000000")
        keywords: 분석할 키워드 리스트
        start_date: 시작 날짜
        end_date: 종료 날짜
        time_unit: 시간 단위 (date, week, month)
        device: 기기 필터 ("", "pc", "mo")
        gender: 성별 필터 ("", "m", "f")
        ages: 나이대 필터
        use_cache: 캐시 사용 여부
        emp_cd: 직원 코드 (이력 저장용)

    Returns:
        TrendAnalysisResult 또는 None
    """
    try:
        if not keywords:
            logger.warning("[DataLab] 키워드가 비어있음")
            return None

        category_name = get_category_name(category_code)
        all_trends = []
        api_calls = 0

        # 키워드를 5개씩 배치로 분할
        for batch_idx in range(0, len(keywords), MAX_KEYWORDS_PER_REQUEST):
            batch = keywords[batch_idx:batch_idx + MAX_KEYWORDS_PER_REQUEST]

            # 요청 본문 구성
            request_body = {
                "startDate": start_date,
                "endDate": end_date,
                "timeUnit": time_unit,
                "category": category_code,
                "keyword": [{"name": kw, "param": [kw]} for kw in batch],
            }

            # 필터 추가
            if device:
                request_body["device"] = device
            if gender:
                request_body["gender"] = gender
            if ages:
                request_body["ages"] = ages

            # API 호출 (공식 엔드포인트: /category/keywords)
            response = await _call_datalab_api("/category/keywords", request_body, use_cache=use_cache)

            if response:
                api_calls += 1
                results = response.get("results", [])

                # 각 결과에 스코어 추가
                for result in results:
                    data = result.get("data", [])
                    data_points = [TrendDataPoint(p["period"], p["ratio"]) for p in data]
                    scores = _score_keyword(data_points)

                    result["scores"] = {
                        "trust_score": scores.trust_score,
                        "seasonality": scores.seasonality,
                        "momentum": scores.momentum,
                        "overall_score": scores.overall_score
                    }
                    all_trends.append(result)

        # 카테고리별 분석도 추가 (device, gender, ages 필터)
        category_request = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "category": [{"name": category_name, "param": [category_code]}],
        }

        if device:
            category_request["device"] = device
        if gender:
            category_request["gender"] = gender
        if ages:
            category_request["ages"] = ages

        category_response = await _call_datalab_api("/categories", category_request, use_cache=use_cache)
        if category_response:
            api_calls += 1

        # 결과 구성
        result = TrendAnalysisResult(
            category_code=category_code,
            category_name=category_name,
            keywords=keywords,
            date_range={"startDate": start_date, "endDate": end_date},
            time_unit=time_unit,
            device_filter=device or "all",
            gender_filter=gender or "all",
            ages_filter=ages or [],
            trend_data=all_trends,
            cache_used=use_cache,
            api_call_count=api_calls
        )

        # 이력 저장
        if emp_cd:
            _save_analysis_history(emp_cd, result)

        logger.info(f"[DataLab] 분석 완료: {category_code}, {len(keywords)} 키워드, API {api_calls}회 호출")
        return result

    except Exception as e:
        logger.error(f"[DataLab] 키워드 분석 실패: {e}", exc_info=True)
        return None


async def suggest_keywords(query: str, category_code: str = None) -> List[str]:
    """
    관련 키워드 제안

    Args:
        query: 기본 검색어
        category_code: 카테고리 코드 (선택사항)

    Returns:
        제안된 키워드 리스트
    """
    try:
        keywords = await _call_search_api(query, display=20)
        return keywords or []
    except Exception as e:
        logger.error(f"[DataLab] 키워드 제안 실패: {e}")
        return []


# ─────────────────────────────────────────
#  이력 및 Seed 관리
# ─────────────────────────────────────────
def _save_analysis_history(emp_cd: str, result: TrendAnalysisResult):
    """분석 이력 저장"""
    try:
        conn = get_connection()
        conn.execute(
            """INSERT INTO datalab_analysis_history
               (emp_cd, category_code, category_name, keywords, filters, trend_data)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                emp_cd,
                result.category_code,
                result.category_name,
                json.dumps(result.keywords),
                json.dumps({
                    "device": result.device_filter,
                    "gender": result.gender_filter,
                    "ages": result.ages_filter
                }),
                json.dumps([
                    {
                        "keyword": t.get("keyword"),
                        "scores": t.get("scores", {})
                    }
                    for t in result.trend_data
                ])
            )
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[DataLab] 이력 저장 실패: {e}")


def save_seed_keyword(
    category_code: str,
    keyword: str,
    last_score: float = None,
    last_momentum: str = None
):
    """Seed 키워드 저장"""
    try:
        conn = get_connection()
        conn.execute(
            """INSERT OR REPLACE INTO datalab_seed_keywords
               (category_code, keyword, search_count, last_score, last_momentum)
               VALUES (?, ?,
                       COALESCE((SELECT search_count + 1 FROM datalab_seed_keywords
                                WHERE category_code = ? AND keyword = ?), 1),
                       ?, ?)""",
            (category_code, keyword, category_code, keyword, last_score, last_momentum)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[DataLab] Seed 키워드 저장 실패: {e}")


def get_seed_keywords(category_code: str) -> List[Dict[str, Any]]:
    """카테고리의 Seed 키워드 조회"""
    try:
        conn = get_connection()
        cursor = conn.execute(
            """SELECT keyword, search_count, last_score, last_momentum
               FROM datalab_seed_keywords
               WHERE category_code = ?
               ORDER BY search_count DESC, updated_at DESC""",
            (category_code,)
        )
        rows = cursor.fetchall()
        conn.close()

        return [
            {
                "keyword": row[0],
                "search_count": row[1],
                "last_score": row[2],
                "last_momentum": row[3]
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"[DataLab] Seed 키워드 조회 실패: {e}")
        return []


def add_brand_to_blacklist(emp_cd: str, brand_name: str):
    """브랜드를 블랙리스트에 추가"""
    try:
        conn = get_connection()
        conn.execute(
            """INSERT OR IGNORE INTO datalab_brand_blacklist (emp_cd, brand_name)
               VALUES (?, ?)""",
            (emp_cd, brand_name)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[DataLab] 블랙리스트 추가 실패: {e}")


def get_brand_blacklist(emp_cd: str) -> List[str]:
    """직원의 브랜드 블랙리스트 조회"""
    try:
        conn = get_connection()
        cursor = conn.execute(
            """SELECT brand_name FROM datalab_brand_blacklist
               WHERE emp_cd = ?
               ORDER BY created_at DESC""",
            (emp_cd,)
        )
        brands = [row[0] for row in cursor.fetchall()]
        conn.close()
        return brands
    except Exception as e:
        logger.error(f"[DataLab] 블랙리스트 조회 실패: {e}")
        return []


# ─────────────────────────────────────────
#  유틸리티
# ─────────────────────────────────────────
def get_all_categories_l1() -> List[Dict[str, str]]:
    """모든 1차 카테고리 반환"""
    return CATEGORIES_L1.copy()


async def validate_credentials() -> bool:
    """Naver API 자격증명 검증"""
    try:
        if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
            logger.error("[DataLab] 자격증명 미설정")
            return False

        # 간단한 API 호출로 검증
        headers = _get_headers()
        request_body = {
            "startDate": (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),
            "endDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "timeUnit": "date",
            "category": [{"name": "패션의류", "param": "50000000"}]
        }

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"{DATALAB_API_BASE}/category",
                json=request_body,
                headers=headers
            )

            if response.status_code == 200:
                logger.info("[DataLab] 자격증명 검증 성공")
                return True
            else:
                logger.error(f"[DataLab] 자격증명 검증 실패: {response.status_code}")
                return False

    except Exception as e:
        logger.error(f"[DataLab] 자격증명 검증 오류: {e}")
        return False


# ─────────────────────────────────────────
#  Public API 함수 (라우터에서 호출)
# ─────────────────────────────────────────
def get_categories() -> List[Dict[str, str]]:
    """1차 카테고리 리스트 반환 (라우터에서 호출)"""
    return CATEGORIES_L1.copy()


def update_seed_keywords(category_code: str, keyword_results: list):
    """분석 결과 기반 시드 키워드 업데이트 (라우터에서 호출)"""
    for kw in keyword_results:
        keyword = kw.get("keyword", "")
        if not keyword:
            continue
        save_seed_keyword(
            category_code,
            keyword,
            last_score=kw.get("trust_score"),
            last_momentum=kw.get("momentum"),
        )


async def run_full_analysis(
    category_code: str,
    category_name: str,
    keywords: List[str],
    start_date: str,
    end_date: str,
    time_unit: str = "month",
    device: str = "",
    gender: str = "",
    ages: List[str] = None,
) -> Dict[str, Any]:
    """
    전체 분석 파이프라인 실행 (라우터에서 호출)

    1. 키워드별 트렌드 조회 (/category/keywords)
    2. 분야별 트렌드 조회 (/categories)
    3. 기기별/성별/연령별 분석 (/category/device, /category/gender, /category/age)
    4. 스코링 및 결과 구성
    """
    if ages is None:
        ages = []

    cat_name = category_name or get_category_name(category_code)
    api_calls = 0
    keyword_results = []

    # ── 1. 키워드별 트렌드 (배치) ──
    for batch_idx in range(0, len(keywords), MAX_KEYWORDS_PER_REQUEST):
        batch = keywords[batch_idx:batch_idx + MAX_KEYWORDS_PER_REQUEST]

        req_body = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "category": category_code,
            "keyword": [{"name": kw, "param": [kw]} for kw in batch],
        }
        if device:
            req_body["device"] = device
        if gender:
            req_body["gender"] = gender
        if ages:
            req_body["ages"] = ages

        resp = await _call_datalab_api("/category/keywords", req_body)
        if resp:
            api_calls += 1
            for result in resp.get("results", []):
                data = result.get("data", [])
                data_points = [TrendDataPoint(p["period"], p["ratio"]) for p in data]
                scores = _score_keyword(data_points)

                keyword_results.append({
                    "keyword": result.get("title", ""),
                    "trend_data": data,
                    "trust_score": scores.trust_score,
                    "seasonality": scores.seasonality,
                    "momentum": scores.momentum.get("trend", ""),
                    "momentum_pct": scores.momentum.get("value", 0),
                    "peak_months": scores.seasonality.get("peak_months", []),
                    "low_months": scores.seasonality.get("low_months", []),
                    "overall_score": scores.overall_score,
                    "monthly_avg": scores.seasonality.get("monthly_avg", {}),
                })

    # ── 2. 분야별 트렌드 ──
    cat_req = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": [{"name": cat_name, "param": [category_code]}],
    }
    if device:
        cat_req["device"] = device
    if gender:
        cat_req["gender"] = gender
    if ages:
        cat_req["ages"] = ages

    category_trend = await _call_datalab_api("/categories", cat_req)
    if category_trend:
        api_calls += 1

    # ── 3. 기기별 ──
    device_data = {}
    dev_req = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_code,
    }
    if gender:
        dev_req["gender"] = gender
    if ages:
        dev_req["ages"] = ages

    dev_resp = await _call_datalab_api("/category/device", dev_req)
    if dev_resp:
        api_calls += 1
        device_data = dev_resp

    # ── 4. 성별 ──
    gender_data = {}
    gen_req = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_code,
    }
    if device:
        gen_req["device"] = device
    if ages:
        gen_req["ages"] = ages

    gen_resp = await _call_datalab_api("/category/gender", gen_req)
    if gen_resp:
        api_calls += 1
        gender_data = gen_resp

    # ── 5. 연령별 ──
    age_data = {}
    age_req = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_code,
    }
    if device:
        age_req["device"] = device
    if gender:
        age_req["gender"] = gender

    age_resp = await _call_datalab_api("/category/age", age_req)
    if age_resp:
        api_calls += 1
        age_data = age_resp

    # ── 결과 구성 ──
    # keyword_results를 overall_score로 정렬
    keyword_results.sort(key=lambda x: x.get("overall_score", 0), reverse=True)

    return {
        "category_code": category_code,
        "category_name": cat_name,
        "period": {"start": start_date, "end": end_date},
        "time_unit": time_unit,
        "keywords": keyword_results,
        "category_trend": category_trend,
        "device_data": device_data,
        "gender_data": gender_data,
        "age_data": age_data,
        "api_calls": api_calls,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────
#  초기화
# ─────────────────────────────────────────
def initialize():
    """DataLab 서비스 초기화"""
    init_datalab_tables()
    _cleanup_expired_cache()
    logger.info("[DataLab] 서비스 초기화 완료")
