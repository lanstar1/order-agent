"""
오더리스트 API 라우터
- 해외 발주 현황 조회 (Google Sheets 동기화)
"""
import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from services.orderlist_service import (
    sync_orderlist, get_orderlist_data, get_orderlist_tabs,
    get_orderlist_summary, get_sheet_tabs, autocomplete_orderlist,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/orderlist", tags=["orderlist"])


@router.post("/sync")
def api_sync_orderlist(tab: str = "", user: dict = Depends(get_current_user)):
    """오더리스트 동기화 — REST 엑셀 메일 스캔 → DB 직접 저장"""
    try:
        from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD
        from services.shipping_mail_service import scan_bor_orderlist_emails, save_bor_rest_to_db, sync_bor_orderlist_to_sheet

        if not MAIL_USER or not MAIL_PASSWORD:
            raise HTTPException(400, "메일 설정이 없습니다 (MAIL_USER, MAIL_PASSWORD)")

        # 1. Ecount 메일에서 REST 엑셀 스캔
        ol_results = scan_bor_orderlist_emails(
            MAIL_IMAP_SERVER, MAIL_USER, MAIL_PASSWORD, MAIL_IMAP_PORT, days_back=90)

        if not ol_results:
            return {"success": True, "message": "REST 파일 없음", "total_items": 0}

        # 2. 구글시트 덮어쓰기 (실패해도 계속)
        try:
            sync_bor_orderlist_to_sheet([ol_results[0]])
        except Exception:
            pass

        # 3. DB 직접 저장
        saved = save_bor_rest_to_db([ol_results[0]])

        return {
            "success": True,
            "message": f"REST → DB 직접 저장 완료",
            "total_items": saved,
            "filename": ol_results[0].get("filename", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"동기화 실패: {str(e)[:200]}")


@router.get("/autocomplete")
def api_autocomplete(
    q: str = Query(..., min_length=1, description="검색어"),
    limit: int = Query(default=15, ge=1, le=50),
):
    """오더리스트 자동완성 검색 (인증 불필요)"""
    results = autocomplete_orderlist(q, limit=limit)
    return {"results": results, "query": q}


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
