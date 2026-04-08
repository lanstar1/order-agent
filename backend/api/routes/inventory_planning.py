"""
적정재고 관리 (온라인관리품목) API 라우터
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List

from db.database import get_connection
from services.inventory_planning_service import (
    get_planning_targets, add_planning_target, update_planning_target,
    remove_planning_target, bulk_add_planning_targets,
    analyze_all_targets, analyze_single_product,
    get_daily_sales, get_pending_orders, search_products_master,
    get_all_pending_orders_map,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/inventory-planning", tags=["inventory-planning"])
KST = timezone(timedelta(hours=9))


# ─── Pydantic 모델 ──────────────────────────────────────

class TargetAdd(BaseModel):
    prod_cd: str
    model_name: str = ""
    prod_name: str = ""
    lead_time_days: int = 40
    safety_stock_days: int = 10
    moq: int = 0
    supplier_group: str = ""

class TargetUpdate(BaseModel):
    lead_time_days: Optional[int] = None
    safety_stock_days: Optional[int] = None
    moq: Optional[int] = None
    supplier_group: Optional[str] = None
    is_active: Optional[int] = None

class BulkAddRequest(BaseModel):
    items: List[TargetAdd]


# ─── 분석 (핵심) ─────────────────────────────────────────

@router.get("/analysis")
async def get_analysis():
    """전체 관리품목 적정재고 분석"""
    conn = get_connection()
    try:
        result = analyze_all_targets(conn)
        # daily_sales는 상세 조회에서만 반환 (목록에서는 제외)
        for item in result["items"]:
            item.pop("daily_sales", None)
        return result
    finally:
        conn.close()


@router.get("/analysis/{target_id}")
async def get_analysis_detail(target_id: int):
    """단일 품목 상세 분석 (일별 판매 데이터 포함)"""
    conn = get_connection()
    try:
        targets = get_planning_targets(conn, active_only=False)
        target = next((t for t in targets if t["id"] == target_id), None)
        if not target:
            raise HTTPException(404, "관리품목을 찾을 수 없습니다")
        order_map = get_all_pending_orders_map(conn)
        return analyze_single_product(conn, target, order_map)
    finally:
        conn.close()


# ─── 관리품목 CRUD ───────────────────────────────────────

@router.get("/targets")
async def list_targets(active_only: bool = True):
    conn = get_connection()
    try:
        items = get_planning_targets(conn, active_only)
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


@router.post("/targets")
async def add_target(body: TargetAdd):
    conn = get_connection()
    try:
        add_planning_target(conn, body.prod_cd, body.model_name, body.prod_name,
                           body.lead_time_days, body.safety_stock_days,
                           body.moq, body.supplier_group)
        return {"status": "ok", "prod_cd": body.prod_cd}
    finally:
        conn.close()


@router.post("/targets/bulk")
async def bulk_add_targets(body: BulkAddRequest):
    conn = get_connection()
    try:
        items = [i.dict() for i in body.items]
        added = bulk_add_planning_targets(conn, items)
        return {"status": "ok", "added": added}
    finally:
        conn.close()


@router.put("/targets/{target_id}")
async def update_target(target_id: int, body: TargetUpdate):
    conn = get_connection()
    try:
        update_planning_target(conn, target_id, **body.dict())
        return {"status": "ok"}
    finally:
        conn.close()


@router.delete("/targets/{target_id}")
async def delete_target(target_id: int):
    conn = get_connection()
    try:
        remove_planning_target(conn, target_id)
        return {"status": "ok"}
    finally:
        conn.close()


# ─── 품목 검색 (등록 시 자동완성) ─────────────────────────

@router.get("/search")
async def search_products(q: str = "", limit: int = 20):
    if not q or len(q) < 2:
        return {"items": []}
    conn = get_connection()
    try:
        results = search_products_master(conn, q, limit)
        return {"items": results}
    finally:
        conn.close()


# ─── 일별 판매 이력 ──────────────────────────────────────

@router.get("/daily-sales/{prod_cd}")
async def get_daily_sales_api(prod_cd: str, days: int = 60):
    conn = get_connection()
    try:
        daily = get_daily_sales(conn, prod_cd, days)
        return {"prod_cd": prod_cd, "days": days, "data": daily}
    finally:
        conn.close()


# ─── 오더리스트 확인 ──────────────────────────────────────

@router.get("/orders/{model_name}")
async def check_pending_orders(model_name: str):
    conn = get_connection()
    try:
        orders = get_pending_orders(conn, model_name)
        return {"model_name": model_name, "orders": orders, "has_orders": len(orders) > 0}
    finally:
        conn.close()
