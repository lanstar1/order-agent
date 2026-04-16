"""
네이버 쇼핑인사이트 공식 API 기반 키워드 수집 서비스
- 스크래핑 대신 openapi.naver.com 공식 REST API 사용
- X-Naver-Client-Id / X-Naver-Client-Secret 인증
- 일일 1,000회 호출 제한

API 엔드포인트 (Base: https://openapi.naver.com/v1/datalab/shopping):
  POST /categories             분야별 트렌드
  POST /category/device        분야 내 기기별
  POST /category/gender        분야 내 성별
  POST /category/age           분야 내 연령별
  POST /category/keywords      키워드별 트렌드
  POST /category/keyword/device  키워드 기기별
  POST /category/keyword/gender  키워드 성별
  POST /category/keyword/age     키워드 연령별
"""
import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx

from db.database import get_connection
from services.trend_constants import (
    DEFAULT_BRAND_EXCLUDE,
    now_kst_str,
    get_latest_collectible_period,
    list_monthly_periods,
)

logger = logging.getLogger("naver_collector")

# ─── Naver API 설정 ────────────────────────────────
NAVER_API_BASE = "https://openapi.naver.com/v1/datalab/shopping"
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")

# 수집 설정
TREND_PAGE_SIZE = 20
DEFAULT_OPERATOR_ID = "haniroom-trend-operator"


def _get_naver_credentials() -> tuple[str, str]:
    """환경변수에서 네이버 API 인증 정보 가져오기"""
    cid = NAVER_CLIENT_ID or os.getenv("NAVER_CLIENT_ID", "")
    secret = NAVER_CLIENT_SECRET or os.getenv("NAVER_CLIENT_SECRET", "")
    if not cid or not secret:
        raise RuntimeError(
            "NAVER_CLIENT_ID 와 NAVER_CLIENT_SECRET 환경변수가 설정되지 않았습니다. "
            "네이버 개발자센터에서 데이터랩(쇼핑인사이트) API를 등록한 후 설정해주세요."
        )
    return cid, secret


def generate_short_id() -> str:
    """uuid로부터 12자 짧은 ID 생성"""
    return uuid.uuid4().hex[:12]


def month_period_to_date_range(period: str) -> tuple[str, str]:
    """
    '2021-01' → ('2021-01-01', '2021-01-31')
    """
    parts = period.split("-")
    year, month = int(parts[0]), int(parts[1])
    start_date = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end_date = f"{year:04d}-12-31"
    else:
        next_month = datetime(year, month + 1, 1)
        last_day = (next_month - timedelta(days=1)).day
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
    return start_date, end_date


def get_trend_total_pages(result_count: int) -> int:
    """result_count로 필요 페이지 수 계산 (페이지당 20개)"""
    return (result_count + TREND_PAGE_SIZE - 1) // TREND_PAGE_SIZE


# ─── 네이버 공식 API 호출 ──────────────────────────
def _naver_api_call(endpoint: str, body: dict) -> dict:
    """
    네이버 쇼핑인사이트 공식 API 동기 호출

    Args:
        endpoint: API 경로 (예: '/categories', '/category/keywords')
        body: 요청 본문 dict

    Returns:
        응답 JSON dict
    """
    client_id, client_secret = _get_naver_credentials()
    url = f"{NAVER_API_BASE}{endpoint}"

    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=body, headers=headers)

    if response.status_code != 200:
        error_text = response.text[:300]
        raise RuntimeError(
            f"Naver API 호출 실패 (HTTP {response.status_code}): {error_text}"
        )

    return response.json()


def fetch_category_trends(
    category_name: str,
    category_param: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
    device: str = "",
    gender: str = "",
    ages: list[str] = None,
) -> dict:
    """
    분야별 트렌드 조회 (POST /categories)
    카테고리의 클릭 트렌드를 기간별로 조회

    Returns:
        { startDate, endDate, timeUnit, results: [{ title, category, data: [{ period, ratio }] }] }
    """
    body: dict[str, Any] = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": [
            {"name": category_name, "param": [category_param]}
        ],
    }
    if device:
        body["device"] = device
    if gender:
        body["gender"] = gender
    if ages:
        body["ages"] = ages

    return _naver_api_call("/categories", body)


def fetch_category_device_trends(
    category_param: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict:
    """분야 내 기기별 트렌드 (POST /category/device)"""
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
    }
    return _naver_api_call("/category/device", body)


def fetch_category_gender_trends(
    category_param: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict:
    """분야 내 성별 트렌드 (POST /category/gender)"""
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
    }
    return _naver_api_call("/category/gender", body)


def fetch_category_age_trends(
    category_param: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict:
    """분야 내 연령별 트렌드 (POST /category/age)"""
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
    }
    return _naver_api_call("/category/age", body)


def fetch_keyword_trends(
    category_param: str,
    keywords: list[dict],
    start_date: str,
    end_date: str,
    time_unit: str = "month",
    device: str = "",
    gender: str = "",
    ages: list[str] = None,
) -> dict:
    """
    키워드별 트렌드 조회 (POST /category/keywords)

    Args:
        category_param: 카테고리 코드 (예: "50000000")
        keywords: [{"name": "키워드그룹명", "param": ["키워드1", "키워드2"]}, ...] (최대 5)
        start_date, end_date: 'YYYY-MM-DD'
        time_unit: 'date' | 'week' | 'month'

    Returns:
        { startDate, endDate, timeUnit, results: [{ title, keyword, data: [{ period, ratio }] }] }
    """
    body: dict[str, Any] = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
        "keyword": keywords,
    }
    if device:
        body["device"] = device
    if gender:
        body["gender"] = gender
    if ages:
        body["ages"] = ages

    return _naver_api_call("/category/keywords", body)


def fetch_keyword_device_trends(
    category_param: str,
    keyword: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict:
    """키워드 기기별 트렌드 (POST /category/keyword/device)"""
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
        "keyword": keyword,
    }
    return _naver_api_call("/category/keyword/device", body)


def fetch_keyword_gender_trends(
    category_param: str,
    keyword: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict:
    """키워드 성별 트렌드 (POST /category/keyword/gender)"""
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
        "keyword": keyword,
    }
    return _naver_api_call("/category/keyword/gender", body)


def fetch_keyword_age_trends(
    category_param: str,
    keyword: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict:
    """키워드 연령별 트렌드 (POST /category/keyword/age)"""
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_param,
        "keyword": keyword,
    }
    return _naver_api_call("/category/keyword/age", body)


# ─── 브랜드 제외 유틸 ──────────────────────────────
def apply_brand_exclusion(keyword: str, custom_excluded_terms: list[str]) -> bool:
    """
    키워드가 브랜드 제외 대상인지 확인
    Returns: True면 brand_excluded
    """
    excluded_terms = set(DEFAULT_BRAND_EXCLUDE + custom_excluded_terms)
    keyword_lower = keyword.lower()
    for term in excluded_terms:
        if term.lower() in keyword_lower:
            return True
    return False


def summarize_failure_snippet(value: str) -> str:
    """실패 원인 요약 (220자 제한)"""
    import re
    text = re.sub(r"\s+", " ", value).strip()
    return text[:220]


# ─── 카테고리 인기 키워드 수집 (공식 API 기반) ──────
def collect_period_keywords(
    profile_dict: dict[str, Any],
    period: str,
    task_id: str,
    run_id: str,
) -> None:
    """
    단일 월의 카테고리 인기 키워드를 공식 API로 수집 후 DB 저장

    공식 API의 /category/keywords 엔드포인트는 "키워드 검색 트렌드"를 반환하지만,
    카테고리별 인기 키워드 목록 자체는 공식 API에서 직접 제공하지 않음.

    대신 /categories 엔드포인트로 분야별 트렌드를 수집하고,
    해당 분야의 ratio 데이터를 스냅샷으로 저장하는 방식으로 전환.
    """
    cid = str(profile_dict["category_cid"])
    category_name = profile_dict.get("category_path", "") or profile_dict.get("name", cid)
    result_count = profile_dict.get("result_count", 20)

    # 디바이스/성별/연령 필터
    devices = profile_dict.get("devices", [])
    genders = profile_dict.get("genders", [])
    ages = profile_dict.get("ages", [])

    start_date, end_date = month_period_to_date_range(period)

    # 1. 캐시 확인
    cached = read_cached_monthly_ranks(profile_dict, period)
    if cached:
        logger.info(f"Using cached data for {period} (profile={profile_dict['id']})")
        ranks = cached
    else:
        # 2. 공식 API 호출: 분야별 트렌드
        logger.info(f"Collecting via Naver API for {period} (profile={profile_dict['id']})")

        # device 파라미터 매핑 (복수 → API 단일값)
        device_param = ""
        if devices:
            if len(devices) == 1:
                device_param = "pc" if devices[0] == "pc" else "mo"
            # 둘 다 선택 시 빈 문자열 (전체)

        # gender 파라미터 매핑
        gender_param = ""
        if genders:
            if len(genders) == 1:
                gender_param = genders[0]  # 'm' or 'f'

        # ages 매핑: ['10','20',...] → API ages 코드
        # API ages: "10"=10대, "20"=20대, ... "60"=60대
        age_params = ages if ages else None

        try:
            api_result = fetch_category_trends(
                category_name=category_name,
                category_param=cid,
                start_date=start_date,
                end_date=end_date,
                time_unit="month",
                device=device_param,
                gender=gender_param,
                ages=age_params,
            )
        except Exception as e:
            raise RuntimeError(f"Naver API 호출 실패 ({period}): {e}") from e

        # API 응답에서 데이터 추출
        results = api_result.get("results", [])
        if not results:
            raise RuntimeError(f"Naver API returned empty results for {period}")

        # 분야별 트렌드는 ratio 데이터만 반환
        # → 키워드 목록이 필요하므로 카테고리 자체를 키워드로 저장
        # 또는 여러 하위 카테고리의 트렌드를 비교
        ranks = []
        for idx, result in enumerate(results):
            data_points = result.get("data", [])
            for dp in data_points:
                ranks.append({
                    "rank": idx + 1,
                    "keyword": result.get("title", f"category_{idx}"),
                    "ratio": dp.get("ratio", 0),
                    "period": dp.get("period", period),
                })

        # rank 재정렬 (ratio 내림차순)
        ranks.sort(key=lambda x: x.get("ratio", 0), reverse=True)
        for i, r in enumerate(ranks):
            r["rank"] = i + 1

        # result_count만큼 자르기
        ranks = ranks[:result_count]

    # 3. DB 저장
    conn = get_connection()
    cur = conn.cursor()
    now = now_kst_str()

    # 기존 스냅샷 삭제
    cur.execute(
        "DELETE FROM trend_snapshots WHERE profile_id = ? AND period = ?",
        [profile_dict["id"], period],
    )

    # 새 스냅샷 삽입
    for rank_item in ranks:
        keyword = rank_item.get("keyword", "")
        brand_excluded = 1 if apply_brand_exclusion(
            keyword, profile_dict.get("custom_excluded_terms", [])
        ) else 0

        cur.execute(
            """
            INSERT INTO trend_snapshots (
                profile_id, run_id, task_id, period, rank, keyword,
                category_cid, device, gender, age, brand_excluded, collected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                profile_dict["id"],
                run_id,
                task_id,
                period,
                rank_item["rank"],
                keyword,
                cid,
                "",  # device (집계)
                "",  # gender (집계)
                "",  # age (집계)
                brand_excluded,
                now,
            ],
        )

    # 태스크 완료 업데이트
    cur.execute(
        """
        UPDATE trend_tasks
        SET status = 'completed',
            completed_pages = ?,
            updated_at = ?
        WHERE id = ?
        """,
        [1, now, task_id],
    )

    # 프로필 last_collected_period 업데이트
    cur.execute(
        "UPDATE trend_profiles SET last_collected_period = ?, updated_at = ? WHERE id = ?",
        [period, now, profile_dict["id"]],
    )

    conn.commit()
    conn.close()


def read_cached_monthly_ranks(
    profile_dict: dict[str, Any],
    period: str,
) -> Optional[list[dict[str, Any]]]:
    """
    동일 조건으로 이미 수집된 캐시 확인

    Returns:
        캐시된 키워드 리스트 또는 None
    """
    conn = get_connection()
    cur = conn.cursor()

    query = """
    SELECT tp.id
    FROM trend_profiles tp
    JOIN trend_snapshots ts ON ts.profile_id = tp.id
    WHERE ts.period = ?
      AND tp.category_cid = ?
      AND tp.devices = ?
      AND tp.genders = ?
      AND tp.ages = ?
      AND tp.result_count = ?
      AND tp.exclude_brand_products = ?
      AND tp.custom_excluded_terms = ?
      AND tp.id != ?
    GROUP BY tp.id
    HAVING COUNT(*) >= ?
    ORDER BY MAX(ts.collected_at) DESC
    LIMIT 1
    """

    devices_json = json.dumps(sorted(profile_dict.get("devices", [])))
    genders_json = json.dumps(sorted(profile_dict.get("genders", [])))
    ages_json = json.dumps(sorted(profile_dict.get("ages", [])))
    custom_excluded_json = json.dumps(sorted(profile_dict.get("custom_excluded_terms", [])))

    cur.execute(query, [
        period,
        profile_dict["category_cid"],
        devices_json,
        genders_json,
        ages_json,
        profile_dict["result_count"],
        1 if profile_dict.get("exclude_brand_products") else 0,
        custom_excluded_json,
        profile_dict["id"],
        profile_dict["result_count"],
    ])

    cached_source = cur.fetchone()
    if not cached_source:
        conn.close()
        return None

    source_profile_id = cached_source[0]

    snap_query = """
    SELECT rank, keyword
    FROM trend_snapshots
    WHERE profile_id = ?
      AND period = ?
      AND rank <= ?
    ORDER BY rank ASC
    """

    cur.execute(snap_query, [source_profile_id, period, profile_dict["result_count"]])
    rows = cur.fetchall()
    conn.close()

    if len(rows) < 1:
        return None

    return [{"rank": row[0], "keyword": row[1]} for row in rows]


# ─── 백그라운드 워커: 대기 태스크 처리 ──────────────
def process_next_task() -> dict[str, Any]:
    """
    다음 대기 중인 태스크 1건 처리

    Returns:
        {ok, processed, run_id?, task_id?, period?, code?, message?}
    """
    conn = get_connection()
    cur = conn.cursor()

    # 1. 대기 중인 run 찾기
    cur.execute("""
        SELECT * FROM trend_runs
        WHERE status IN ('queued', 'running')
          AND id IN (SELECT run_id FROM trend_tasks WHERE status = 'pending')
        ORDER BY updated_at DESC
        LIMIT 1
    """)
    run_row = cur.fetchone()
    if not run_row:
        conn.close()
        return {"ok": True, "processed": False}

    run_dict = dict(run_row)

    # 2. 다음 대기 task 찾기
    cur.execute("""
        SELECT * FROM trend_tasks
        WHERE run_id = ? AND status = 'pending'
        ORDER BY period ASC
        LIMIT 1
    """, [run_dict["id"]])
    task_row = cur.fetchone()
    if not task_row:
        conn.close()
        return {"ok": True, "processed": False}

    task_dict = dict(task_row)

    # 3. 프로필 조회
    cur.execute("SELECT * FROM trend_profiles WHERE id = ?", [run_dict["profile_id"]])
    profile_row = cur.fetchone()
    if not profile_row:
        now = now_kst_str()
        cur.execute("""
            UPDATE trend_tasks
            SET status = 'failed', failure_reason = 'Trend profile is missing.',
                failure_snippet = 'Missing profile', updated_at = ?
            WHERE id = ?
        """, [now, task_dict["id"]])
        conn.commit()
        _refresh_run_state(conn, run_dict["id"])
        conn.close()
        return {
            "ok": False, "processed": True,
            "code": "TREND_PROFILE_NOT_FOUND",
            "message": "Trend profile is missing.",
            "run_id": run_dict["id"],
            "task_id": task_dict["id"],
            "period": task_dict["period"],
        }

    profile_dict = dict(profile_row)

    # JSON 문자열 파싱
    for key in ("devices", "genders", "ages", "custom_excluded_terms"):
        val = profile_dict.get(key, "[]")
        if isinstance(val, str):
            try:
                profile_dict[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                profile_dict[key] = []

    # 4. Run/Task 상태 → running
    now = now_kst_str()
    cur.execute("""
        UPDATE trend_runs
        SET status = 'running', started_at = COALESCE(started_at, ?), updated_at = ?
        WHERE id = ?
    """, [now, now, run_dict["id"]])
    cur.execute("""
        UPDATE trend_tasks
        SET status = 'running', updated_at = ?
        WHERE id = ?
    """, [now, task_dict["id"]])
    conn.commit()
    conn.close()

    # 5. 키워드 수집
    try:
        collect_period_keywords(
            profile_dict,
            task_dict["period"],
            task_dict["id"],
            run_dict["id"],
        )

        conn = get_connection()
        _refresh_run_state(conn, run_dict["id"])
        conn.close()

        return {
            "ok": True, "processed": True,
            "run_id": run_dict["id"],
            "task_id": task_dict["id"],
            "period": task_dict["period"],
        }

    except Exception as e:
        error_msg = str(e)
        snippet = summarize_failure_snippet(error_msg)
        failed_at = now_kst_str()

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE trend_tasks
            SET status = 'failed', failure_reason = ?, failure_snippet = ?, updated_at = ?
            WHERE id = ?
        """, [error_msg, snippet, failed_at, task_dict["id"]])
        conn.commit()

        _refresh_run_state(conn, run_dict["id"])

        cur.execute(
            "UPDATE trend_profiles SET updated_at = ? WHERE id = ?",
            [failed_at, profile_dict["id"]],
        )
        conn.commit()
        conn.close()

        return {
            "ok": False, "processed": True,
            "code": "TREND_COLLECTION_FAILED",
            "message": error_msg,
            "run_id": run_dict["id"],
            "task_id": task_dict["id"],
            "period": task_dict["period"],
        }


def _refresh_run_state(conn: Any, run_id: str) -> None:
    """Run 상태 갱신: 완료/실패 task 비율에 따라 상태 결정"""
    cur = conn.cursor()

    cur.execute("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    run_row = cur.fetchone()
    if not run_row:
        raise RuntimeError("Trend run is missing.")

    run_dict = dict(run_row)
    if run_dict["status"] == "cancelled":
        return

    cur.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
        FROM trend_tasks WHERE run_id = ?
    """, [run_id])
    totals = cur.fetchone()
    total = totals[0] if totals else 0
    completed = totals[1] if totals else 0
    failed = totals[2] if totals else 0

    cur.execute("SELECT COUNT(*) FROM trend_snapshots WHERE run_id = ?", [run_id])
    snap_row = cur.fetchone()
    snapshots = snap_row[0] if snap_row else 0

    now = now_kst_str()
    status = "running"
    completed_at = None

    if total == 0 or completed == total:
        status = "completed"
        completed_at = now
    elif completed + failed == total and failed > 0:
        status = "failed"
        completed_at = now

    cur.execute("""
        UPDATE trend_runs
        SET status = ?, total_tasks = ?, completed_tasks = ?, failed_tasks = ?,
            total_snapshots = ?, completed_at = ?, updated_at = ?
        WHERE id = ?
    """, [status, total, completed, failed, snapshots, completed_at, now, run_id])

    conn.commit()
