"""
자료관리 API 라우터
- 단가표 동기화 (Google Sheets → SQLite)
- 자료 검색 (단가표, KC인증서, 데이터시트)
- 소스 관리 (추가/삭제/활성화)
"""
import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, List
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from services.materials_service import (
    sync_sheet, sync_all_sheets,
    sync_drive_folder, sync_all_drive_folders,
    sync_all as sync_all_sources,
    search_materials, search_drive_documents,
    get_sync_status,
    get_drive_categories, list_drive_documents,
    get_price_sheet_vendors, get_price_sheet_tabs, get_price_sheet_data,
)
from db.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/materials", tags=["materials"])


# ─────────────────────────────────────────
#  동기화
# ─────────────────────────────────────────
@router.post("/sync")
async def sync_all(user: dict = Depends(get_current_user)):
    """모든 소스 동기화 (Sheets + Drive 폴더)"""
    result = await sync_all_sources()
    return result


@router.post("/sync/{source_id}")
async def sync_one(source_id: int, user: dict = Depends(get_current_user)):
    """특정 소스 동기화 (sheet 또는 drive_folder 자동 판별)"""
    conn = get_connection()
    source = conn.execute("SELECT source_type FROM material_sources WHERE id=?", (source_id,)).fetchone()
    conn.close()
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다.")

    if source["source_type"] == "drive_folder":
        result = await sync_drive_folder(source_id)
    else:
        result = await sync_sheet(source_id)
    return result


# ─────────────────────────────────────────
#  검색
# ─────────────────────────────────────────
@router.get("/search")
async def search(
    q: str = Query("", description="검색어"),
    vendor: str = Query("", description="제조사/유통사 필터"),
    category: str = Query("", description="카테고리 필터"),
    price_type: str = Query("", description="가격 유형 필터 (공급가, 대리점, 채널별 등)"),
    limit: int = Query(20, ge=1, le=100),
):
    """단가표 데이터 검색"""
    if not q.strip():
        raise HTTPException(status_code=400, detail="검색어(q)를 입력하세요.")
    results = search_materials(q, vendor=vendor, category=category, price_type=price_type, limit=limit)
    return {"query": q, "count": len(results), "results": results}


@router.get("/search-docs")
async def search_docs(
    q: str = Query("", description="검색어"),
    category: str = Query("", description="카테고리 필터"),
    limit: int = Query(20, ge=1, le=100),
):
    """Drive 문서 검색 (KC인증서, 데이터시트)"""
    results = search_drive_documents(q, category=category, limit=limit)
    return {"query": q, "count": len(results), "results": results}


# ─────────────────────────────────────────
#  자료검색 (Google Drive 문서 브라우저)
# ─────────────────────────────────────────
@router.get("/drive/categories")
async def drive_categories():
    """Drive 문서 카테고리 목록 (데이터시트, Fluke, KC인증서, ROHS, Test리포트, UL)"""
    return {"categories": get_drive_categories()}


@router.get("/drive/documents")
async def drive_documents(
    category: str = Query("", description="카테고리 필터"),
    q: str = Query("", description="파일명 검색어"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Drive 문서 목록 조회 (카테고리 + 검색 필터)"""
    return list_drive_documents(category=category, query=q, limit=limit, offset=offset)


# ─────────────────────────────────────────
#  단가표 조회 (Google Sheets 뷰어)
# ─────────────────────────────────────────
@router.get("/price-sheets/vendors")
async def price_sheet_vendors():
    """단가표가 있는 거래처 목록"""
    return {"vendors": get_price_sheet_vendors()}


@router.get("/price-sheets/{source_id}/tabs")
async def price_sheet_tabs(source_id: int):
    """특정 거래처의 시트 탭 목록"""
    return {"tabs": get_price_sheet_tabs(source_id)}


@router.get("/price-sheets/{source_id}")
async def price_sheet_data(
    source_id: int,
    q: str = Query("", description="검색어 (Ctrl+F)"),
    tab: str = Query("", description="시트 탭 필터"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """특정 거래처 단가표 전체 데이터"""
    return get_price_sheet_data(source_id, query=q, tab=tab, limit=limit, offset=offset)


# ─────────────────────────────────────────
#  Google API 진단
# ─────────────────────────────────────────
@router.get("/google-api-check")
async def google_api_check(user: dict = Depends(get_current_user)):
    """Google API Key 상태 진단 (Drive API, Sheets API 활성화 여부 확인)"""
    import httpx
    from config import GOOGLE_API_KEY

    results = {"google_api_key_set": bool(GOOGLE_API_KEY)}
    if not GOOGLE_API_KEY:
        results["error"] = "GOOGLE_API_KEY 환경변수가 설정되지 않았습니다."
        return results

    # 키 앞 8자리만 표시
    results["api_key_prefix"] = GOOGLE_API_KEY[:8] + "..."

    async with httpx.AsyncClient(timeout=10) as client:
        # 1) Google Drive API v3 테스트
        try:
            r = await client.get(
                "https://www.googleapis.com/drive/v3/about",
                params={"key": GOOGLE_API_KEY, "fields": "kind"},
            )
            if r.status_code == 200:
                results["drive_api"] = {"status": "OK", "enabled": True}
            else:
                body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {"raw": r.text[:300]}
                err_msg = body.get("error", {}).get("message", r.text[:300])
                results["drive_api"] = {
                    "status": "FAIL",
                    "enabled": False,
                    "http_status": r.status_code,
                    "error": err_msg,
                }
        except Exception as e:
            results["drive_api"] = {"status": "ERROR", "error": str(e)}

        # 2) Google Sheets API v4 테스트 (존재하지 않는 시트로 테스트 — 403이면 API 미활성, 404면 활성)
        try:
            r = await client.get(
                "https://sheets.googleapis.com/v4/spreadsheets/NONEXISTENT_TEST",
                params={"key": GOOGLE_API_KEY, "fields": "spreadsheetId"},
            )
            if r.status_code == 404:
                # 404 = API는 활성화됨 (시트만 못 찾은 것)
                results["sheets_api"] = {"status": "OK", "enabled": True}
            elif r.status_code == 403:
                body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {"raw": r.text[:300]}
                err_msg = body.get("error", {}).get("message", r.text[:300])
                results["sheets_api"] = {
                    "status": "FAIL",
                    "enabled": False,
                    "http_status": 403,
                    "error": err_msg,
                }
            else:
                results["sheets_api"] = {"status": "OK", "enabled": True}
        except Exception as e:
            results["sheets_api"] = {"status": "ERROR", "error": str(e)}

    # DB에서 등록된 소스 확인
    conn = get_connection()
    sources = conn.execute(
        "SELECT id, name, source_type, folder_id, sheet_id, is_active FROM material_sources"
    ).fetchall()
    conn.close()
    results["registered_sources"] = [dict(s) for s in sources]

    return results


# ─────────────────────────────────────────
#  소스 관리
# ─────────────────────────────────────────
@router.get("/sources")
async def list_sources():
    """등록된 소스 목록 + 동기화 상태"""
    return get_sync_status()


class SourceCreate(BaseModel):
    source_type: str = "sheet"
    name: str
    url: str
    sheet_id: str = ""
    folder_id: str = ""
    gid: str = "0"
    category: str = "price"
    vendor: str = ""


@router.post("/sources")
async def add_source(body: SourceCreate, user: dict = Depends(get_current_user)):
    """새 소스 추가"""
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO material_sources(source_type,name,url,sheet_id,folder_id,gid,category,vendor)
               VALUES(?,?,?,?,?,?,?,?)""",
            (body.source_type, body.name, body.url,
             body.sheet_id, body.folder_id, body.gid,
             body.category, body.vendor)
        )
        conn.commit()
        return {"success": True, "id": cur.lastrowid, "message": f"소스 '{body.name}' 추가 완료"}
    finally:
        conn.close()


@router.delete("/sources/{source_id}")
async def delete_source(source_id: int, user: dict = Depends(get_current_user)):
    """소스 삭제 (관련 데이터 포함)"""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM price_data WHERE source_id=?", (source_id,))
        conn.execute("DELETE FROM drive_documents WHERE source_id=?", (source_id,))
        conn.execute("DELETE FROM material_sources WHERE id=?", (source_id,))
        conn.commit()
        return {"success": True, "message": "소스 및 관련 데이터 삭제 완료"}
    finally:
        conn.close()


@router.put("/sources/{source_id}/toggle")
async def toggle_source(source_id: int, user: dict = Depends(get_current_user)):
    """소스 활성/비활성 토글"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT is_active FROM material_sources WHERE id=?", (source_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다.")
        new_val = 0 if row["is_active"] else 1
        conn.execute("UPDATE material_sources SET is_active=? WHERE id=?", (new_val, source_id))
        conn.commit()
        return {"success": True, "is_active": bool(new_val)}
    finally:
        conn.close()
