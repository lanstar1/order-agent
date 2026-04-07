"""
네이버 데이터랩 연동 서비스
3단계: AI 키워드 생성 → 검색어트렌드 → 쇼핑인사이트

NAVER_SEARCH_ID / NAVER_SEARCH_SECRET 환경변수 사용 (MAP 감시와 동일)
"""
import os
import json
import httpx
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger("naver_datalab")

KST = timezone(timedelta(hours=9))

# ═══════════════════════════════════════════════════════
# API 상수
# ═══════════════════════════════════════════════════════
DATALAB_SEARCH_URL = "https://openapi.naver.com/v1/datalab/search"
DATALAB_SHOPPING_CATEGORY_URL = "https://openapi.naver.com/v1/datalab/shopping/categories"
DATALAB_SHOPPING_DEVICE_URL = "https://openapi.naver.com/v1/datalab/shopping/category/device"
DATALAB_SHOPPING_GENDER_URL = "https://openapi.naver.com/v1/datalab/shopping/category/gender"
DATALAB_SHOPPING_AGE_URL = "https://openapi.naver.com/v1/datalab/shopping/category/age"
DATALAB_SHOPPING_KEYWORD_URL = "https://openapi.naver.com/v1/datalab/shopping/category/keyword/age"

# 랜스타 주요 카테고리 코드 (네이버 쇼핑)
LANSTAR_CATEGORIES = {
    "네트워크장비": "50000832",     # 컴퓨터/주변기기 > 네트워크장비
    "케이블/젠더": "50000833",      # 컴퓨터/주변기기 > 케이블/젠더/컨버터
    "PC주변기기": "50000830",       # 컴퓨터/주변기기 > PC주변기기
    "모니터주변기기": "50000834",   # 컴퓨터/주변기기 > 모니터/모니터주변기기
    "컴퓨터부품": "50000803",       # 컴퓨터/주변기기 > 컴퓨터부품
    "사무기기": "50001332",         # 문구/오피스 > 사무기기
}


def _get_naver_headers() -> dict:
    """네이버 API 헤더 (MAP 감시와 동일한 키 사용)"""
    cid = os.getenv("NAVER_SEARCH_ID", "")
    csec = os.getenv("NAVER_SEARCH_SECRET", "")
    if not cid or not csec:
        raise RuntimeError("NAVER_SEARCH_ID / NAVER_SEARCH_SECRET 미설정")
    return {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
        "Content-Type": "application/json",
    }


def _now_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


# ═══════════════════════════════════════════════════════
# Stage 1: AI 키워드 자동 생성
# ═══════════════════════════════════════════════════════

async def generate_keywords_ai(products: list[dict], batch_size: int = 50) -> list[dict]:
    """
    MAP 제품 데이터에서 Claude AI로 검색 트렌드 분석용 키워드 자동 생성.
    
    Args:
        products: map_products 테이블 레코드 리스트
        batch_size: AI 호출당 처리 제품 수
        
    Returns:
        [{"product_id": int, "model_name": str, "keywords": [str, ...], "category_hint": str}, ...]
    """
    from config import ANTHROPIC_API_KEY, CLAUDE_MODEL_LIGHT
    import anthropic

    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY 미설정")

    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    all_results = []

    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        product_list_text = "\n".join([
            f"- ID:{p['id']} | 모델:{p['model_name']} | 품명:{p['product_name']} | "
            f"브랜드:{p.get('brand','LANstar')} | 특징:{p.get('features','')}"
            for p in batch
        ])

        prompt = f"""아래는 LANstar(랜스타) B2B 네트워크 장비/케이블 회사의 제품 목록입니다.
각 제품에 대해 네이버 검색 트렌드 분석에 사용할 키워드를 생성해주세요.

【규칙】
1. 각 제품당 2~5개 키워드를 생성합니다.
2. 키워드는 일반 소비자가 네이버에서 실제 검색할 법한 단어/구문이어야 합니다.
3. 키워드 유형:
   - 제품 카테고리 키워드 (예: "HDMI 케이블", "USB 허브")
   - 용도/기능 키워드 (예: "듀얼모니터 연결", "4K 모니터 케이블")
   - 브랜드+제품 키워드 (예: "랜스타 랜케이블")
4. 너무 구체적인 모델명(LS-HDMT-2M)은 제외하되, 일반적 검색어로 변환합니다.
5. category_hint에는 네트워크장비/케이블젠더/PC주변기기/모니터주변기기/컴퓨터부품/사무기기 중 가장 적합한 것을 선택합니다.

【제품 목록】
{product_list_text}

【출력 형식 - JSON 배열만 출력, 다른 텍스트 없이】
[
  {{"product_id": 1, "model_name": "LS-HDMT-2M", "keywords": ["HDMI 케이블", "HDMI 2M", "모니터 케이블"], "category_hint": "케이블젠더"}},
  ...
]"""

        try:
            resp = await client.messages.create(
                model=CLAUDE_MODEL_LIGHT,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            # JSON 추출 (```json ... ``` 래핑 처리)
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0].strip() if "```json" in text else text.split("```")[1].split("```")[0].strip()

            batch_results = json.loads(text)
            all_results.extend(batch_results)
            logger.info(f"AI 키워드 생성: batch {i//batch_size + 1}, {len(batch_results)}개 제품 처리")
        except Exception as e:
            logger.error(f"AI 키워드 생성 오류 (batch {i//batch_size + 1}): {e}")
            # 실패 시 기본 키워드 생성 (fallback)
            for p in batch:
                all_results.append({
                    "product_id": p["id"],
                    "model_name": p["model_name"],
                    "keywords": _generate_fallback_keywords(p),
                    "category_hint": "케이블젠더",
                })

        # API rate limit 방지
        await asyncio.sleep(1)

    return all_results


def _generate_fallback_keywords(product: dict) -> list[str]:
    """AI 실패 시 규칙 기반 키워드 생성"""
    model = product["model_name"]
    name = product.get("product_name", "")
    brand = product.get("brand", "LANstar")

    keywords = []
    # 브랜드 + 핵심어
    keywords.append(f"{brand} {_extract_core(name)}")

    # 일반 카테고리 키워드
    for term in ["HDMI", "USB", "DP", "DVI", "VGA", "RJ45", "랜케이블",
                  "허브", "스위치", "충전기", "컨버터", "젠더", "모니터암"]:
        if term.lower() in model.lower() or term.lower() in name.lower():
            keywords.append(term)

    # 길이/규격 추출
    import re
    length_match = re.search(r'(\d+(?:\.\d+)?)\s*[Mm]', model + " " + name)
    if length_match and keywords:
        keywords.append(f"{keywords[0]} {length_match.group(0).strip()}")

    return keywords[:5] if keywords else [name[:20]]


def _extract_core(name: str) -> str:
    """품명에서 핵심 단어 추출 (브랜드/규격 제외)"""
    import re
    # LS-, LANstar, 숫자 규격 등 제거
    cleaned = re.sub(r'(?i)(ls-\S+|lanstar|랜스타|\d+[Mm]|\d+\.\d+[Mm])', '', name).strip()
    # 앞 30자
    return cleaned[:30] if cleaned else name[:20]


# ═══════════════════════════════════════════════════════
# Stage 2: 네이버 검색어트렌드 API
# ═══════════════════════════════════════════════════════

async def fetch_search_trend(
    keyword_groups: list[dict],
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
) -> dict:
    """
    네이버 검색어트렌드 API 호출.
    
    Args:
        keyword_groups: [{"groupName": "HDMI 케이블", "keywords": ["HDMI케이블","HDMI 케이블"]}]
                        최대 5개 그룹, 각 그룹 최대 20개 키워드
        start_date: "2025-01-01" (기본: 1년 전)
        end_date: "2026-04-07" (기본: 오늘)
        time_unit: "date" | "week" | "month"
        
    Returns:
        {"startDate": ..., "endDate": ..., "timeUnit": ..., "results": [...]}
    """
    headers = _get_naver_headers()

    now = datetime.now(KST)
    if not start_date:
        start_date = _date_str(now - timedelta(days=365))
    if not end_date:
        end_date = _date_str(now)

    # 최대 5그룹 제한
    groups = keyword_groups[:5]
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "keywordGroups": [
            {
                "groupName": g["groupName"],
                "keywords": g["keywords"][:20],
            }
            for g in groups
        ],
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(DATALAB_SEARCH_URL, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"검색어트렌드 조회 성공: {len(groups)}개 그룹, {time_unit}")
            return data
    except httpx.HTTPStatusError as e:
        logger.error(f"검색어트렌드 API 오류: {e.response.status_code} - {e.response.text}")
        raise RuntimeError(f"네이버 API 오류 ({e.response.status_code}): {e.response.text}")
    except Exception as e:
        logger.error(f"검색어트렌드 요청 실패: {e}")
        raise


async def fetch_search_trend_batch(
    all_keyword_groups: list[dict],
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
) -> list[dict]:
    """
    5개 그룹 제한을 우회하여 여러 그룹을 배치 처리.
    
    Returns:
        [{"groupName": ..., "data": [{"period": "2025-01-06", "ratio": 45.2}, ...]}]
    """
    all_results = []
    for i in range(0, len(all_keyword_groups), 5):
        batch = all_keyword_groups[i:i + 5]
        try:
            data = await fetch_search_trend(batch, start_date, end_date, time_unit)
            for result in data.get("results", []):
                all_results.append(result)
        except Exception as e:
            logger.error(f"검색어트렌드 배치 {i//5 + 1} 오류: {e}")
            for g in batch:
                all_results.append({
                    "title": g["groupName"],
                    "keywords": g["keywords"],
                    "data": [],
                    "error": str(e),
                })
        # 네이버 API rate limit: 초당 10건
        await asyncio.sleep(0.3)

    return all_results


# ═══════════════════════════════════════════════════════
# Stage 3: 네이버 쇼핑인사이트 API
# ═══════════════════════════════════════════════════════

async def fetch_shopping_category_trend(
    category_code: str,
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
) -> dict:
    """쇼핑인사이트 - 분야별 트렌드"""
    headers = _get_naver_headers()
    now = datetime.now(KST)
    if not start_date:
        start_date = _date_str(now - timedelta(days=365))
    if not end_date:
        end_date = _date_str(now)

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": [{"name": category_code, "param": [category_code]}],
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(DATALAB_SHOPPING_CATEGORY_URL, headers=headers, json=body)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"쇼핑인사이트 카테고리 오류: {e.response.status_code} - {e.response.text}")
        raise RuntimeError(f"쇼핑인사이트 API 오류: {e.response.text}")


async def fetch_shopping_device_trend(
    category_code: str,
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
) -> dict:
    """쇼핑인사이트 - 기기별 (PC vs 모바일)"""
    headers = _get_naver_headers()
    now = datetime.now(KST)
    if not start_date:
        start_date = _date_str(now - timedelta(days=365))
    if not end_date:
        end_date = _date_str(now)

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_code,
        "device": "",   # 전체
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(DATALAB_SHOPPING_DEVICE_URL, headers=headers, json=body)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"쇼핑인사이트 기기별 오류: {e.response.status_code}")
        raise RuntimeError(f"기기별 트렌드 API 오류: {e.response.text}")


async def fetch_shopping_gender_trend(
    category_code: str,
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
) -> dict:
    """쇼핑인사이트 - 성별"""
    headers = _get_naver_headers()
    now = datetime.now(KST)
    if not start_date:
        start_date = _date_str(now - timedelta(days=365))
    if not end_date:
        end_date = _date_str(now)

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_code,
        "gender": "",   # 전체
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(DATALAB_SHOPPING_GENDER_URL, headers=headers, json=body)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"성별 트렌드 API 오류: {e.response.text}")


async def fetch_shopping_age_trend(
    category_code: str,
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
) -> dict:
    """쇼핑인사이트 - 연령별"""
    headers = _get_naver_headers()
    now = datetime.now(KST)
    if not start_date:
        start_date = _date_str(now - timedelta(days=365))
    if not end_date:
        end_date = _date_str(now)

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_code,
        "ages": [],   # 전체
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(DATALAB_SHOPPING_AGE_URL, headers=headers, json=body)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"연령별 트렌드 API 오류: {e.response.text}")


# ═══════════════════════════════════════════════════════
# 통합 분석: 제품별 종합 트렌드 리포트
# ═══════════════════════════════════════════════════════

async def analyze_product_trend(
    product: dict,
    keywords: list[str],
    category_code: str = None,
    days: int = 365,
) -> dict:
    """
    단일 제품에 대한 종합 트렌드 분석.
    
    Returns:
        {
            "product_id": int,
            "model_name": str,
            "search_trend": {...},       # 검색어트렌드
            "shopping_trend": {...},      # 쇼핑인사이트 카테고리
            "device_trend": {...},        # 기기별
            "gender_trend": {...},        # 성별
            "age_trend": {...},           # 연령별
            "summary": {                  # 요약 지표
                "avg_ratio": float,       # 평균 검색비율
                "trend_direction": str,   # "상승" | "하락" | "보합"
                "peak_period": str,       # 최고점 기간
                "latest_ratio": float,    # 최근 비율
            }
        }
    """
    now = datetime.now(KST)
    start_date = _date_str(now - timedelta(days=days))
    end_date = _date_str(now)

    result = {
        "product_id": product["id"],
        "model_name": product["model_name"],
        "product_name": product.get("product_name", ""),
        "keywords": keywords,
        "analyzed_at": _now_kst(),
    }

    # 1) 검색어트렌드
    try:
        keyword_group = {
            "groupName": product["model_name"],
            "keywords": keywords[:20],
        }
        search_data = await fetch_search_trend(
            [keyword_group], start_date, end_date, "week"
        )
        search_results = search_data.get("results", [])
        result["search_trend"] = search_results[0] if search_results else {}
    except Exception as e:
        result["search_trend"] = {"error": str(e)}

    # 2) 쇼핑인사이트 (카테고리 코드가 있는 경우만)
    if category_code:
        try:
            result["shopping_trend"] = await fetch_shopping_category_trend(
                category_code, start_date, end_date, "week"
            )
        except Exception as e:
            result["shopping_trend"] = {"error": str(e)}

        try:
            result["device_trend"] = await fetch_shopping_device_trend(
                category_code, start_date, end_date, "month"
            )
        except Exception as e:
            result["device_trend"] = {"error": str(e)}

        try:
            result["gender_trend"] = await fetch_shopping_gender_trend(
                category_code, start_date, end_date, "month"
            )
        except Exception as e:
            result["gender_trend"] = {"error": str(e)}

        try:
            result["age_trend"] = await fetch_shopping_age_trend(
                category_code, start_date, end_date, "month"
            )
        except Exception as e:
            result["age_trend"] = {"error": str(e)}

        await asyncio.sleep(0.5)  # rate limit
    else:
        for k in ["shopping_trend", "device_trend", "gender_trend", "age_trend"]:
            result[k] = {"error": "카테고리 코드 미지정"}

    # 3) 요약 지표 계산
    result["summary"] = _calc_summary(result.get("search_trend", {}))

    return result


def _calc_summary(search_trend: dict) -> dict:
    """검색어트렌드 데이터에서 요약 지표 계산"""
    data = search_trend.get("data", [])
    if not data:
        return {
            "avg_ratio": 0, "trend_direction": "데이터없음",
            "peak_period": "-", "latest_ratio": 0,
            "growth_rate": 0,
        }

    ratios = [d["ratio"] for d in data if d.get("ratio")]
    if not ratios:
        return {
            "avg_ratio": 0, "trend_direction": "데이터없음",
            "peak_period": "-", "latest_ratio": 0,
            "growth_rate": 0,
        }

    avg_ratio = sum(ratios) / len(ratios)
    latest_ratio = ratios[-1] if ratios else 0
    peak_idx = ratios.index(max(ratios))
    peak_period = data[peak_idx].get("period", "-") if peak_idx < len(data) else "-"

    # 트렌드 방향: 최근 4주 평균 vs 이전 4주 평균
    if len(ratios) >= 8:
        recent = sum(ratios[-4:]) / 4
        prev = sum(ratios[-8:-4]) / 4
        if prev > 0:
            growth = (recent - prev) / prev * 100
        else:
            growth = 0
        if growth > 5:
            direction = "상승"
        elif growth < -5:
            direction = "하락"
        else:
            direction = "보합"
    else:
        growth = 0
        direction = "데이터부족"

    return {
        "avg_ratio": round(avg_ratio, 2),
        "trend_direction": direction,
        "peak_period": peak_period,
        "latest_ratio": round(latest_ratio, 2),
        "growth_rate": round(growth, 1),
    }


# ═══════════════════════════════════════════════════════
# DB 저장/조회 헬퍼
# ═══════════════════════════════════════════════════════

def save_keywords_to_db(product_id: int, keywords: list[str], category_hint: str, conn=None):
    """AI 생성 키워드를 DB에 저장"""
    from db.database import get_connection
    own_conn = False
    if not conn:
        conn = get_connection()
        own_conn = True

    now = _now_kst()
    # 기존 키워드 삭제 후 재입력
    conn.execute("DELETE FROM datalab_keywords WHERE product_id = ?", (product_id,))
    for kw in keywords:
        conn.execute(
            """INSERT INTO datalab_keywords (product_id, keyword, category_hint, created_at)
               VALUES (?, ?, ?, ?)""",
            (product_id, kw.strip(), category_hint, now)
        )
    conn.commit()
    if own_conn:
        conn.close()


def save_trend_result(product_id: int, trend_type: str, data: dict, conn=None):
    """트렌드 분석 결과를 DB에 저장"""
    from db.database import get_connection
    own_conn = False
    if not conn:
        conn = get_connection()
        own_conn = True

    now = _now_kst()
    conn.execute(
        """INSERT INTO datalab_trend_results
           (product_id, trend_type, result_json, analyzed_at)
           VALUES (?, ?, ?, ?)""",
        (product_id, trend_type, json.dumps(data, ensure_ascii=False), now)
    )
    conn.commit()
    if own_conn:
        conn.close()


def get_product_keywords(product_id: int, conn=None) -> list[dict]:
    """제품의 저장된 키워드 조회"""
    from db.database import get_connection
    own_conn = False
    if not conn:
        conn = get_connection()
        own_conn = True

    rows = conn.execute(
        "SELECT * FROM datalab_keywords WHERE product_id = ? ORDER BY id",
        (product_id,)
    ).fetchall()
    result = [dict(r) for r in rows]
    if own_conn:
        conn.close()
    return result


def get_latest_trend(product_id: int, trend_type: str = None, conn=None) -> Optional[dict]:
    """제품의 최신 트렌드 결과 조회"""
    from db.database import get_connection
    own_conn = False
    if not conn:
        conn = get_connection()
        own_conn = True

    sql = "SELECT * FROM datalab_trend_results WHERE product_id = ?"
    params = [product_id]
    if trend_type:
        sql += " AND trend_type = ?"
        params.append(trend_type)
    sql += " ORDER BY analyzed_at DESC LIMIT 1"

    row = conn.execute(sql, params).fetchone()
    result = dict(row) if row else None
    if result and result.get("result_json"):
        result["result_json"] = json.loads(result["result_json"])
    if own_conn:
        conn.close()
    return result
