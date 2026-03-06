"""
오더리스트 API 라우터
- 해외 발주 현황 조회 (Google Sheets 동기화)
"""
import logging
from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.orderlist_service import (
    sync_orderlist, get_orderlist_data, get_orderlist_tabs,
    get_orderlist_summary, get_sheet_tabs,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/orderlist", tags=["orderlist"])


@router.post("/sync")
def api_sync_orderlist(tab: str = ""):
    """오더리스트 동기화 (전체 또는 특정 탭)"""
    result = sync_orderlist(tab_title=tab)
    if not result.get("success"):
        raise HTTPException(500, result.get("error", "동기화 실패"))
    return result


@router.get("/data")
def api_get_orderlist(
    query: str = "",
    tab: str = "",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=10, le=200),
):
    """오더리스트 조회/검색"""
    return get_orderlist_data(query=query, tab=tab, page=page, page_size=page_size)


@router.get("/tabs")
def api_get_tabs():
    """동기화된 탭 목록 (건수 포함)"""
    return get_orderlist_tabs()


@router.get("/sheet-tabs")
def api_get_sheet_tabs():
    """구글시트 원본 탭 목록"""
    return get_sheet_tabs()


@router.get("/summary")
def api_get_summary():
    """오더리스트 요약 통계"""
    return get_orderlist_summary()
