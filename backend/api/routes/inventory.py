"""
재고 조회 API 라우터
- GET  /api/inventory/search?q=검색어  — 품목 검색 후 재고 조회
- POST /api/inventory/check            — 품목코드로 직접 재고 조회
"""
import logging
import csv
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from services.erp_client import erp_client
from config import PRODUCTS_CSV

router = APIRouter(prefix="/api/inventory", tags=["inventory"])
logger = logging.getLogger(__name__)


class InventoryCheckRequest(BaseModel):
    prod_cd: str
    wh_cd: str = ""
    base_date: str = ""


def _search_products_csv(query: str, limit: int = 20) -> list:
    """products.csv에서 품목 검색 (품목코드, 품명, 모델명으로 검색)"""
    if not PRODUCTS_CSV.exists():
        logger.warning(f"[Inventory] products.csv 없음: {PRODUCTS_CSV}")
        return []

    query_lower = query.lower().strip()
    results = []
    try:
        with open(PRODUCTS_CSV, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                prod_cd = str(row.get("prod_cd", "") or "").strip()
                prod_name = str(row.get("prod_name", "") or "").strip()
                model = str(row.get("model", "") or "").strip()
                keywords = str(row.get("keywords", "") or "").strip()

                searchable = f"{prod_cd} {prod_name} {model} {keywords}".lower()
                if query_lower in searchable:
                    results.append({
                        "prod_cd": prod_cd,
                        "prod_name": prod_name,
                        "model": model,
                    })
                    if len(results) >= limit:
                        break
    except Exception as e:
        logger.error(f"[Inventory] products.csv 검색 오류: {e}")
    return results


# ─────────────────────────────────────────
#  품목 자동완성 (재고 조회 없이 빠른 검색)
# ─────────────────────────────────────────
@router.get("/autocomplete")
async def autocomplete_products(
    q: str = Query(..., min_length=1, description="검색어"),
    limit: int = Query(default=15, ge=1, le=50),
):
    """products.csv에서 품목 검색 (자동완성용, ERP 호출 없음)"""
    matches = _search_products_csv(q, limit=limit)
    return {"results": matches, "query": q}


# ─────────────────────────────────────────
#  품목 검색 → 재고 조회
# ─────────────────────────────────────────
@router.get("/search")
async def search_inventory(
    user: dict = Depends(get_current_user),
    q: str = Query(..., min_length=1, description="검색어 (품목명, 모델명, 품목코드)"),
    wh_cd: str = Query(default="", description="창고코드 (빈 값=전체)"),
    base_date: str = Query(default="", description="기준일 YYYYMMDD (빈 값=오늘)"),
):
    """품목 검색 + 각 품목의 재고 조회"""
    # 1. products.csv에서 품목 검색
    matches = _search_products_csv(q, limit=10)
    if not matches:
        return {"results": [], "message": "검색 결과가 없습니다."}

    # 2. 각 매칭 품목의 재고 조회
    results = []
    for m in matches:
        inv = await erp_client.check_inventory(
            prod_cd=m["prod_cd"],
            wh_cd=wh_cd,
            base_date=base_date,
        )
        results.append({
            "prod_cd": m["prod_cd"],
            "prod_name": m["prod_name"],
            "model": m["model"],
            "inventory": inv,
        })

    return {"results": results, "query": q}


# ─────────────────────────────────────────
#  품목코드로 직접 재고 조회
# ─────────────────────────────────────────
@router.post("/check")
async def check_inventory(req: InventoryCheckRequest, user: dict = Depends(get_current_user)):
    """품목코드로 직접 재고 조회"""
    result = await erp_client.check_inventory(
        prod_cd=req.prod_cd,
        wh_cd=req.wh_cd,
        base_date=req.base_date,
    )
    return result
