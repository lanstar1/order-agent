"""
네이버 데이터랩 API 라우트
3단계: AI 키워드 생성 → 검색어트렌드 → 쇼핑인사이트
"""
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
import json
import asyncio
import logging

from db.database import get_connection
from services.naver_datalab_service import (
    generate_keywords_ai,
    fetch_search_trend,
    fetch_search_trend_batch,
    fetch_shopping_category_trend,
    fetch_shopping_device_trend,
    fetch_shopping_gender_trend,
    fetch_shopping_age_trend,
    analyze_product_trend,
    save_keywords_to_db,
    save_trend_result,
    get_product_keywords,
    get_latest_trend,
    LANSTAR_CATEGORIES,
)

logger = logging.getLogger("naver_datalab_routes")
router = APIRouter(prefix="/api/datalab", tags=["Naver DataLab"])


# ── 진행률 추적 (인메모리) ──────────────────────────────
datalab_progress = {
    "running": False,
    "stage": "",         # "keywords" | "search_trend" | "shopping_insight"
    "percent": 0,
    "current_product": "",
    "total": 0,
    "done": 0,
    "message": "",
}


# ═══════════════════════════════════════════════════════
# Stage 1: AI 키워드 생성
# ═══════════════════════════════════════════════════════

class KeywordGenerateRequest(BaseModel):
    product_ids: Optional[List[int]] = None   # None이면 전체
    batch_size: int = 50

class KeywordUpdateRequest(BaseModel):
    keywords: List[str]
    category_hint: str = ""


@router.post("/keywords/generate")
async def generate_keywords(req: KeywordGenerateRequest, background_tasks: BackgroundTasks):
    """
    MAP 제품에서 AI 키워드 자동 생성 (백그라운드).
    product_ids가 없으면 전체 활성 제품 대상.
    """
    if datalab_progress["running"]:
        raise HTTPException(409, "이미 실행 중입니다")

    conn = get_connection()
    if req.product_ids:
        placeholders = ",".join(["?"] * len(req.product_ids))
        products = conn.execute(
            f"SELECT * FROM map_products WHERE id IN ({placeholders}) AND is_active=1",
            req.product_ids
        ).fetchall()
    else:
        products = conn.execute(
            "SELECT * FROM map_products WHERE is_active=1 ORDER BY model_name"
        ).fetchall()
    conn.close()

    products = [dict(p) for p in products]
    if not products:
        raise HTTPException(404, "활성 제품이 없습니다")

    background_tasks.add_task(_run_keyword_generation, products, req.batch_size)
    return {
        "message": f"{len(products)}개 제품 키워드 생성 시작",
        "product_count": len(products),
    }


async def _run_keyword_generation(products: list, batch_size: int):
    """백그라운드 키워드 생성"""
    global datalab_progress
    datalab_progress.update({
        "running": True, "stage": "keywords", "percent": 0,
        "total": len(products), "done": 0,
        "message": f"AI 키워드 생성 중... ({len(products)}개 제품)",
    })

    try:
        results = await generate_keywords_ai(products, batch_size)

        conn = get_connection()
        saved = 0
        for r in results:
            pid = r.get("product_id")
            keywords = r.get("keywords", [])
            cat_hint = r.get("category_hint", "")
            if pid and keywords:
                save_keywords_to_db(pid, keywords, cat_hint, conn)
                saved += 1
                datalab_progress["done"] = saved
                datalab_progress["percent"] = int(saved / len(results) * 100)
                datalab_progress["current_product"] = r.get("model_name", "")
        conn.close()

        datalab_progress.update({
            "running": False, "percent": 100,
            "message": f"키워드 생성 완료: {saved}개 제품",
        })
        logger.info(f"AI 키워드 생성 완료: {saved}개 제품")
    except Exception as e:
        datalab_progress.update({"running": False, "message": f"오류: {e}"})
        logger.error(f"키워드 생성 실패: {e}", exc_info=True)


@router.get("/keywords/{product_id}")
async def get_keywords(product_id: int):
    """제품별 키워드 조회"""
    keywords = get_product_keywords(product_id)
    return {"product_id": product_id, "keywords": keywords}


@router.put("/keywords/{product_id}")
async def update_keywords(product_id: int, req: KeywordUpdateRequest):
    """제품별 키워드 수동 수정"""
    save_keywords_to_db(product_id, req.keywords, req.category_hint)
    return {"message": f"키워드 {len(req.keywords)}개 저장 완료"}


@router.get("/keywords")
async def list_all_keywords(search: str = "", has_keywords: bool = False):
    """전체 키워드 현황 조회"""
    conn = get_connection()

    sql = """
        SELECT p.id, p.model_name, p.product_name, p.brand, p.map_price,
               GROUP_CONCAT(dk.keyword, ', ') as keywords,
               MAX(dk.category_hint) as category_hint,
               COUNT(dk.id) as keyword_count
        FROM map_products p
        LEFT JOIN datalab_keywords dk ON p.id = dk.product_id
        WHERE p.is_active = 1
    """
    params = []
    if search:
        sql += " AND (p.model_name LIKE ? OR p.product_name LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    sql += " GROUP BY p.id ORDER BY p.model_name"

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    result = [dict(r) for r in rows]
    if has_keywords:
        result = [r for r in result if r.get("keyword_count", 0) > 0]

    return {
        "total": len(result),
        "with_keywords": sum(1 for r in result if r.get("keyword_count", 0) > 0),
        "products": result,
    }


# ═══════════════════════════════════════════════════════
# Stage 2: 검색어트렌드
# ═══════════════════════════════════════════════════════

class SearchTrendRequest(BaseModel):
    keyword_groups: List[dict]   # [{"groupName": "...", "keywords": ["...", "..."]}]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    time_unit: str = "week"      # date | week | month


@router.post("/search-trend")
async def search_trend_api(req: SearchTrendRequest):
    """검색어트렌드 직접 조회 (최대 5그룹)"""
    if not req.keyword_groups:
        raise HTTPException(400, "키워드 그룹이 필요합니다")

    data = await fetch_search_trend(
        req.keyword_groups, req.start_date, req.end_date, req.time_unit
    )
    return data


@router.post("/search-trend/batch")
async def search_trend_batch_api(req: SearchTrendRequest):
    """검색어트렌드 배치 조회 (5그룹 이상 자동 분할)"""
    if not req.keyword_groups:
        raise HTTPException(400, "키워드 그룹이 필요합니다")

    results = await fetch_search_trend_batch(
        req.keyword_groups, req.start_date, req.end_date, req.time_unit
    )
    return {"results": results, "total_groups": len(results)}


@router.get("/search-trend/product/{product_id}")
async def search_trend_by_product(
    product_id: int,
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
):
    """제품별 검색어트렌드 (저장된 키워드 사용)"""
    keywords = get_product_keywords(product_id)
    if not keywords:
        raise HTTPException(404, "해당 제품에 저장된 키워드가 없습니다. 먼저 키워드를 생성하세요.")

    conn = get_connection()
    product = conn.execute("SELECT * FROM map_products WHERE id = ?", (product_id,)).fetchone()
    conn.close()
    if not product:
        raise HTTPException(404, "제품을 찾을 수 없습니다")
    product = dict(product)

    keyword_list = [k["keyword"] for k in keywords]
    keyword_group = {
        "groupName": product["model_name"],
        "keywords": keyword_list[:20],
    }

    data = await fetch_search_trend([keyword_group], start_date, end_date, time_unit)

    # DB에 결과 저장
    save_trend_result(product_id, "search_trend", data)

    return data


@router.post("/search-trend/compare")
async def compare_products_trend(
    product_ids: List[int],
    start_date: str = None,
    end_date: str = None,
    time_unit: str = "week",
):
    """여러 제품의 검색 트렌드 비교 (최대 5개)"""
    if len(product_ids) > 5:
        raise HTTPException(400, "최대 5개 제품까지 비교 가능합니다")

    conn = get_connection()
    keyword_groups = []
    for pid in product_ids:
        product = conn.execute("SELECT * FROM map_products WHERE id = ?", (pid,)).fetchone()
        if not product:
            continue
        product = dict(product)
        kw_rows = conn.execute(
            "SELECT keyword FROM datalab_keywords WHERE product_id = ?", (pid,)
        ).fetchall()
        kw_list = [r["keyword"] for r in kw_rows]
        if kw_list:
            keyword_groups.append({
                "groupName": product["model_name"],
                "keywords": kw_list[:20],
            })
    conn.close()

    if not keyword_groups:
        raise HTTPException(404, "비교할 키워드가 있는 제품이 없습니다")

    data = await fetch_search_trend(keyword_groups, start_date, end_date, time_unit)
    return data


# ═══════════════════════════════════════════════════════
# Stage 3: 쇼핑인사이트
# ═══════════════════════════════════════════════════════

@router.get("/shopping/categories")
async def get_category_list():
    """사용 가능한 카테고리 코드 목록"""
    return {"categories": LANSTAR_CATEGORIES}


class ShoppingTrendRequest(BaseModel):
    category_code: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    time_unit: str = "week"


@router.post("/shopping/category-trend")
async def shopping_category_trend_api(req: ShoppingTrendRequest):
    """쇼핑인사이트 - 분야별 트렌드"""
    data = await fetch_shopping_category_trend(
        req.category_code, req.start_date, req.end_date, req.time_unit
    )
    return data


@router.post("/shopping/device-trend")
async def shopping_device_trend_api(req: ShoppingTrendRequest):
    """쇼핑인사이트 - 기기별 (PC vs 모바일)"""
    data = await fetch_shopping_device_trend(
        req.category_code, req.start_date, req.end_date, req.time_unit
    )
    return data


@router.post("/shopping/gender-trend")
async def shopping_gender_trend_api(req: ShoppingTrendRequest):
    """쇼핑인사이트 - 성별"""
    data = await fetch_shopping_gender_trend(
        req.category_code, req.start_date, req.end_date, req.time_unit
    )
    return data


@router.post("/shopping/age-trend")
async def shopping_age_trend_api(req: ShoppingTrendRequest):
    """쇼핑인사이트 - 연령별"""
    data = await fetch_shopping_age_trend(
        req.category_code, req.start_date, req.end_date, req.time_unit
    )
    return data


# ═══════════════════════════════════════════════════════
# 통합 분석
# ═══════════════════════════════════════════════════════

@router.get("/analyze/{product_id}")
async def analyze_single_product(
    product_id: int,
    category_code: str = None,
    days: int = 365,
):
    """단일 제품 종합 트렌드 분석"""
    conn = get_connection()
    product = conn.execute("SELECT * FROM map_products WHERE id = ?", (product_id,)).fetchone()
    if not product:
        conn.close()
        raise HTTPException(404, "제품을 찾을 수 없습니다")
    product = dict(product)

    kw_rows = conn.execute(
        "SELECT keyword, category_hint FROM datalab_keywords WHERE product_id = ?",
        (product_id,)
    ).fetchall()
    conn.close()

    if not kw_rows:
        raise HTTPException(404, "키워드가 없습니다. 먼저 AI 키워드를 생성하세요.")

    keywords = [r["keyword"] for r in kw_rows]
    if not category_code:
        cat_hint = kw_rows[0]["category_hint"] if kw_rows else ""
        category_code = LANSTAR_CATEGORIES.get(cat_hint, "")

    result = await analyze_product_trend(product, keywords, category_code, days)

    # DB 저장
    save_trend_result(product_id, "full_analysis", result)

    return result


class BulkAnalyzeRequest(BaseModel):
    product_ids: Optional[List[int]] = None
    category_code: Optional[str] = None
    days: int = 365


@router.post("/analyze/bulk")
async def analyze_bulk(req: BulkAnalyzeRequest, background_tasks: BackgroundTasks):
    """여러 제품 일괄 트렌드 분석 (백그라운드)"""
    if datalab_progress["running"]:
        raise HTTPException(409, "이미 실행 중입니다")

    conn = get_connection()
    if req.product_ids:
        placeholders = ",".join(["?"] * len(req.product_ids))
        products = conn.execute(
            f"""SELECT p.*, GROUP_CONCAT(dk.keyword) as kw_list,
                       MAX(dk.category_hint) as cat_hint
                FROM map_products p
                JOIN datalab_keywords dk ON p.id = dk.product_id
                WHERE p.id IN ({placeholders}) AND p.is_active=1
                GROUP BY p.id""",
            req.product_ids
        ).fetchall()
    else:
        products = conn.execute(
            """SELECT p.*, GROUP_CONCAT(dk.keyword) as kw_list,
                      MAX(dk.category_hint) as cat_hint
               FROM map_products p
               JOIN datalab_keywords dk ON p.id = dk.product_id
               WHERE p.is_active=1
               GROUP BY p.id ORDER BY p.model_name"""
        ).fetchall()
    conn.close()

    products = [dict(p) for p in products]
    if not products:
        raise HTTPException(404, "키워드가 있는 활성 제품이 없습니다")

    background_tasks.add_task(_run_bulk_analysis, products, req.category_code, req.days)
    return {
        "message": f"{len(products)}개 제품 일괄 분석 시작",
        "product_count": len(products),
    }


async def _run_bulk_analysis(products: list, category_code: str, days: int):
    """백그라운드 일괄 분석"""
    global datalab_progress
    datalab_progress.update({
        "running": True, "stage": "analysis", "percent": 0,
        "total": len(products), "done": 0,
        "message": f"일괄 분석 중... ({len(products)}개 제품)",
    })

    try:
        for i, p in enumerate(products):
            keywords = p.get("kw_list", "").split(",") if p.get("kw_list") else []
            cat = category_code or LANSTAR_CATEGORIES.get(p.get("cat_hint", ""), "")

            datalab_progress.update({
                "current_product": p["model_name"],
                "done": i,
                "percent": int(i / len(products) * 100),
            })

            try:
                result = await analyze_product_trend(
                    p, keywords, cat, days
                )
                save_trend_result(p["id"], "full_analysis", result)
            except Exception as e:
                logger.error(f"분석 실패 [{p['model_name']}]: {e}")

            # Rate limit: 검색어트렌드 + 쇼핑인사이트 4종 = 5 API 호출
            await asyncio.sleep(2)

        datalab_progress.update({
            "running": False, "percent": 100, "done": len(products),
            "message": f"일괄 분석 완료: {len(products)}개 제품",
        })
    except Exception as e:
        datalab_progress.update({"running": False, "message": f"오류: {e}"})
        logger.error(f"일괄 분석 실패: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════
# 진행률 & 이력 조회
# ═══════════════════════════════════════════════════════

@router.get("/progress")
async def get_progress():
    """현재 진행률 조회"""
    return datalab_progress


@router.get("/history/{product_id}")
async def get_trend_history(product_id: int, trend_type: str = None, limit: int = 10):
    """제품별 트렌드 분석 이력"""
    conn = get_connection()
    sql = "SELECT id, product_id, trend_type, analyzed_at FROM datalab_trend_results WHERE product_id = ?"
    params = [product_id]
    if trend_type:
        sql += " AND trend_type = ?"
        params.append(trend_type)
    sql += " ORDER BY analyzed_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/history/result/{result_id}")
async def get_trend_result(result_id: int):
    """분석 결과 상세 조회"""
    conn = get_connection()
    row = conn.execute("SELECT * FROM datalab_trend_results WHERE id = ?", (result_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "결과를 찾을 수 없습니다")
    result = dict(row)
    if result.get("result_json"):
        result["result_json"] = json.loads(result["result_json"])
    return result


# ═══════════════════════════════════════════════════════
# 대시보드 요약
# ═══════════════════════════════════════════════════════

@router.get("/dashboard/summary")
async def dashboard_summary():
    """데이터랩 대시보드 요약 데이터"""
    conn = get_connection()

    # 전체 제품 수
    total = conn.execute("SELECT COUNT(*) as cnt FROM map_products WHERE is_active=1").fetchone()["cnt"]

    # 키워드 있는 제품 수
    with_kw = conn.execute(
        "SELECT COUNT(DISTINCT product_id) as cnt FROM datalab_keywords"
    ).fetchone()["cnt"]

    # 분석 완료 제품 수
    analyzed = conn.execute(
        "SELECT COUNT(DISTINCT product_id) as cnt FROM datalab_trend_results WHERE trend_type='full_analysis'"
    ).fetchone()["cnt"]

    # 총 키워드 수
    total_kw = conn.execute("SELECT COUNT(*) as cnt FROM datalab_keywords").fetchone()["cnt"]

    # 최근 분석 결과 (상승/하락/보합)
    recent = conn.execute(
        """SELECT result_json FROM datalab_trend_results
           WHERE trend_type='full_analysis'
           ORDER BY analyzed_at DESC LIMIT 50"""
    ).fetchall()

    trend_counts = {"상승": 0, "하락": 0, "보합": 0, "기타": 0}
    for r in recent:
        try:
            data = json.loads(r["result_json"])
            direction = data.get("summary", {}).get("trend_direction", "기타")
            if direction in trend_counts:
                trend_counts[direction] += 1
            else:
                trend_counts["기타"] += 1
        except:
            pass

    # 최근 분석 시간
    last_analysis = conn.execute(
        "SELECT MAX(analyzed_at) as last FROM datalab_trend_results"
    ).fetchone()

    conn.close()

    return {
        "total_products": total,
        "with_keywords": with_kw,
        "analyzed": analyzed,
        "total_keywords": total_kw,
        "trend_distribution": trend_counts,
        "last_analysis": last_analysis["last"] if last_analysis else None,
    }


@router.get("/dashboard/top-trending")
async def top_trending_products(limit: int = 10, direction: str = "상승"):
    """트렌드 상위 제품 목록"""
    conn = get_connection()
    rows = conn.execute(
        """SELECT tr.product_id, tr.result_json, tr.analyzed_at,
                  p.model_name, p.product_name, p.map_price
           FROM datalab_trend_results tr
           JOIN map_products p ON tr.product_id = p.id
           WHERE tr.trend_type = 'full_analysis'
           AND tr.id IN (
               SELECT MAX(id) FROM datalab_trend_results
               WHERE trend_type='full_analysis' GROUP BY product_id
           )
           ORDER BY tr.analyzed_at DESC"""
    ).fetchall()
    conn.close()

    results = []
    for r in rows:
        try:
            data = json.loads(r["result_json"])
            summary = data.get("summary", {})
            if summary.get("trend_direction") == direction:
                results.append({
                    "product_id": r["product_id"],
                    "model_name": r["model_name"],
                    "product_name": r["product_name"],
                    "map_price": r["map_price"],
                    "growth_rate": summary.get("growth_rate", 0),
                    "avg_ratio": summary.get("avg_ratio", 0),
                    "latest_ratio": summary.get("latest_ratio", 0),
                    "peak_period": summary.get("peak_period", "-"),
                    "analyzed_at": r["analyzed_at"],
                })
        except:
            continue

    # 성장률 기준 정렬
    results.sort(key=lambda x: x["growth_rate"], reverse=(direction == "상승"))
    return results[:limit]
