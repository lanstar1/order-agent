"""
PO/PI 메일 자동 등록 API 라우터
- 화이트리스트 발신자 CRUD
- 즉시 스캔 트리거
- WeChat 등 수동 업로드
- 임포트 이력 조회
"""

import logging
from pathlib import Path
import sys
from typing import Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends
from pydantic import BaseModel, EmailStr

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.database import get_connection
from services.po_mail_service import (
    list_po_senders, add_po_sender, update_po_sender, remove_po_sender,
    run_po_mail_scan_all, import_po_from_upload,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/po-mail", tags=["po-mail"])


# ─── Pydantic 모델 ─────────────────────────────────────────

class SenderAdd(BaseModel):
    email: str
    supplier_name: str = ""
    sheet_tab_prefix: str = ""


class SenderUpdate(BaseModel):
    email: Optional[str] = None
    supplier_name: Optional[str] = None
    sheet_tab_prefix: Optional[str] = None
    is_active: Optional[int] = None


# ─── 발신자 화이트리스트 CRUD ─────────────────────────────

@router.get("/senders")
async def get_senders(active_only: bool = False):
    """PO 메일 발신자 목록"""
    try:
        return {"senders": list_po_senders(active_only=active_only)}
    except Exception as e:
        logger.error(f"[PO메일] 발신자 목록 조회 실패: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.post("/senders")
async def create_sender(req: SenderAdd):
    """PO 메일 발신자 추가"""
    if not req.email or "@" not in req.email:
        raise HTTPException(400, "유효한 이메일 주소가 필요합니다.")
    try:
        sender_id = add_po_sender(req.email, req.supplier_name, req.sheet_tab_prefix)
        return {"ok": True, "id": sender_id}
    except Exception as e:
        logger.error(f"[PO메일] 발신자 추가 실패: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.patch("/senders/{sender_id}")
async def patch_sender(sender_id: int, req: SenderUpdate):
    """PO 메일 발신자 수정"""
    try:
        update_po_sender(sender_id, **req.dict(exclude_none=True))
        return {"ok": True}
    except Exception as e:
        logger.error(f"[PO메일] 발신자 수정 실패: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@router.delete("/senders/{sender_id}")
async def delete_sender(sender_id: int):
    """PO 메일 발신자 삭제"""
    try:
        remove_po_sender(sender_id)
        return {"ok": True}
    except Exception as e:
        logger.error(f"[PO메일] 발신자 삭제 실패: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# ─── 즉시 스캔 ───────────────────────────────────────────

@router.post("/scan")
async def trigger_scan(days_back: int = 90, skip_duplicates: bool = True):
    """전체 활성 발신자에 대해 PO 메일 즉시 스캔"""
    from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD

    if not MAIL_USER or not MAIL_PASSWORD:
        raise HTTPException(400, "Ecount 메일 환경변수(MAIL_USER, MAIL_PASSWORD)가 설정되지 않았습니다.")

    try:
        result = run_po_mail_scan_all(
            MAIL_IMAP_SERVER, MAIL_USER, MAIL_PASSWORD,
            MAIL_IMAP_PORT, days_back, skip_duplicates
        )
        return result
    except Exception as e:
        logger.error(f"[PO메일] 스캔 실패: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# ─── WeChat 등 수동 업로드 ───────────────────────────────

@router.post("/upload")
async def upload_po(
    file: UploadFile = File(...),
    supplier_name: str = Form(""),
    sheet_tab_prefix: str = Form("WECHAT"),
    source: str = Form("wechat"),
    sender_label: str = Form("wechat"),
):
    """
    WeChat/메신저 등으로 받은 PI/PO PDF를 수동 업로드 → AI 분석 → DB + 구글시트 저장
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "PDF 파일만 지원합니다.")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(400, "빈 파일입니다.")

    try:
        result = import_po_from_upload(
            pdf_bytes=pdf_bytes,
            filename=file.filename,
            sender_email=sender_label,
            supplier_name=supplier_name,
            sheet_tab_prefix=sheet_tab_prefix,
            source=source,
        )
        return result
    except Exception as e:
        logger.error(f"[PO수동] 업로드 처리 실패: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# ─── 임포트 이력 조회 ─────────────────────────────────────

@router.get("/imports")
async def list_imports(limit: int = 50, sender_email: str = ""):
    """PO 메일 자동 등록 이력 조회"""
    conn = get_connection()
    try:
        if sender_email:
            rows = conn.execute("""
                SELECT id, sender_email, message_uid, subject, email_date, filename,
                       po_number, pi_number, delivery_date, sheet_tab,
                       item_count, status, error_message, source, created_at
                FROM po_mail_imports
                WHERE sender_email = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, (sender_email, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT id, sender_email, message_uid, subject, email_date, filename,
                       po_number, pi_number, delivery_date, sheet_tab,
                       item_count, status, error_message, source, created_at
                FROM po_mail_imports
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return {"imports": [dict(r) for r in rows]}
    finally:
        conn.close()


@router.delete("/imports/{import_id}")
async def delete_import(import_id: int):
    """임포트 이력 + 연관 orderlist_items 삭제"""
    conn = get_connection()
    try:
        # 연관 데이터 조회
        row = conn.execute(
            "SELECT po_number, sheet_tab FROM po_mail_imports WHERE id = ?",
            (import_id,)
        ).fetchone()
        if row:
            po_number = row[0]
            sheet_tab = row[1]
            if po_number:
                conn.execute(
                    "DELETE FROM orderlist_items WHERE sheet_tab = ? AND order_no = ?",
                    (sheet_tab, po_number)
                )
        conn.execute("DELETE FROM po_mail_imports WHERE id = ?", (import_id,))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()
