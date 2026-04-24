"""
CS/RMA 접수 및 처리 API
- 불량 접수 (CS 직원)
- 물류 수령/인계 (물류 직원)
- 기술 테스트 결과 (기술 직원)
- 최종 처리 (CS 직원)
- 타임라인/이력 조회
- 파일 업로드
"""
import os
import uuid
import logging
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import APIRouter, BackgroundTasks, Depends, Query, UploadFile, File, Form, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, List
from security import get_current_user
from db.database import get_connection, now_kst

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cs", tags=["cs"])

# 파일 업로드 경로
UPLOAD_DIR = Path(__file__).parent.parent.parent.parent / "data" / "cs_files"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ── 상수 ──
CS_STATUSES = ["접수완료", "물류수령", "기술인계", "테스트완료", "처리종결"]
FINAL_ACTIONS = ["교환발송", "환불처리", "정상반송", "단순변심 반송"]
TEST_RESULTS = ["정상", "의심", "불량"]
CS_TYPES = ["반품", "교환", "A/S수리", "미출고"]
SALES_CHANNELS = ["스마트스토어", "G마켓", "옥션", "쿠팡", "컴퓨존", "오늘의집", "나비엠알오", "자사몰", "기타"]
REASON_CATEGORIES = ["파손 및 불량", "단순 변심", "주문 실수", "오배송 및 지연", "재고 부족", "기타"]
SHIPPING_COST_STATUSES = ["환불금에서 차감", "판매자에게 직접 송금", "추가결제", "무료반품", "해당없음"]


# ─── Request 모델 ───────────────────
class TicketCreate(BaseModel):
    customer_name: str
    contact_info: str
    product_name: str
    serial_number: str = ""
    defect_symptom: str
    courier: str = ""
    tracking_no: str = ""
    memo: str = ""
    sales_channel: str = ""
    order_number: str = ""
    cs_type: str = "반품"
    reason_category: str = ""
    quantity: int = 1
    shipping_cost_status: str = ""
    return_courier: str = ""
    return_tracking_no: str = ""


class StatusUpdate(BaseModel):
    memo: str = ""


class TestResultCreate(BaseModel):
    test_status: str          # 정상 / 의심 / 불량
    test_comment: str = ""


class FinalAction(BaseModel):
    action: str               # 교환발송 / 환불처리 / 정상반송
    memo: str = ""


class TicketEdit(BaseModel):
    customer_name: Optional[str] = None
    contact_info: Optional[str] = None
    product_name: Optional[str] = None
    serial_number: Optional[str] = None
    defect_symptom: Optional[str] = None
    sales_channel: Optional[str] = None
    order_number: Optional[str] = None
    cs_type: Optional[str] = None
    reason_category: Optional[str] = None
    quantity: Optional[int] = None
    shipping_cost_status: Optional[str] = None
    return_courier: Optional[str] = None
    return_tracking_no: Optional[str] = None


class BackorderCreate(BaseModel):
    sales_channel: str = ""
    order_date: str = ""
    order_number: str = ""
    recipient_name: str
    recipient_phone: str = ""
    product_name: str
    option_info: str = ""
    quantity: int = 1
    status: str = "미출고"
    memo: str = ""


# ─── 티켓 ID 생성 ───────────────────
def _generate_ticket_id() -> str:
    """고유 접수번호 생성: CS-YYYYMMDD-XXXX"""
    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST).strftime("%Y%m%d")
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM cs_tickets WHERE ticket_id LIKE ?",
            (f"CS-{today}-%",)
        ).fetchone()
        seq = (row["cnt"] if row else 0) + 1
        return f"CS-{today}-{seq:04d}"
    finally:
        conn.close()


# ─── 이력 기록 헬퍼 ───────────────────
def _log_action(conn, ticket_id: str, action_type: str, actor_cd: str, actor_name: str, detail: str = ""):
    """타임스탬프 포함 이력 기록"""
    conn.execute(
        """INSERT INTO cs_action_logs (ticket_id, action_type, actor_cd, actor_name, detail, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (ticket_id, action_type, actor_cd, actor_name, detail, now_kst())
    )


# ═══════════════════════════════════════
#  API 엔드포인트
# ═══════════════════════════════════════

# ── [1] 티켓 목록 조회 ──
@router.get("/tickets")
async def list_tickets(
    status: str = Query("", description="상태 필터"),
    search: str = Query("", description="검색 (고객명/연락처/접수번호/주문번호)"),
    channel: str = Query("", description="판매채널 필터"),
    cs_type: str = Query("", description="CS유형 필터"),
    reason: str = Query("", description="사유 필터"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
    user: dict = Depends(get_current_user),
):
    conn = get_connection()
    try:
        where_clauses = []
        params = []

        if status:
            where_clauses.append("t.current_status = ?")
            params.append(status)

        if channel:
            where_clauses.append("t.sales_channel = ?")
            params.append(channel)

        if cs_type:
            where_clauses.append("t.cs_type = ?")
            params.append(cs_type)

        if reason:
            where_clauses.append("t.reason_category = ?")
            params.append(reason)

        if search:
            search_term = f"%{search}%"
            where_clauses.append(
                "(t.ticket_id LIKE ? OR t.customer_name LIKE ? OR t.contact_info LIKE ? OR t.order_number LIKE ?)"
            )
            params.extend([search_term, search_term, search_term, search_term])

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # 전체 건수
        count_row = conn.execute(
            f"SELECT COUNT(*) as cnt FROM cs_tickets t{where_sql}", params
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        # 페이지네이션
        offset = (page - 1) * size
        rows = conn.execute(
            f"""SELECT t.*, tr.test_status, tr.test_comment
                FROM cs_tickets t
                LEFT JOIN cs_test_results tr ON t.ticket_id = tr.ticket_id
                {where_sql}
                ORDER BY t.created_at DESC
                LIMIT ? OFFSET ?""",
            params + [size, offset]
        ).fetchall()

        tickets = [dict(r) for r in rows]

        # 상태별 카운트 (대시보드 통계용)
        stats_rows = conn.execute(
            "SELECT current_status, COUNT(*) as cnt FROM cs_tickets GROUP BY current_status"
        ).fetchall()
        status_counts = {r["current_status"]: r["cnt"] for r in stats_rows}

        return {
            "tickets": tickets,
            "total": total,
            "page": page,
            "size": size,
            "status_counts": status_counts,
        }
    finally:
        conn.close()


# ── [2] 티켓 상세 조회 ──
@router.get("/tickets/{ticket_id}")
async def get_ticket(ticket_id: str, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT * FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")

        # 테스트 결과
        test_result = conn.execute(
            "SELECT * FROM cs_test_results WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()

        # 첨부파일 (file_data 바이너리 제외)
        files = conn.execute(
            "SELECT id, ticket_id, file_name, file_url, file_type, file_size, uploaded_by, created_at, drive_file_id, mime_type FROM cs_files WHERE ticket_id = ? ORDER BY created_at", (ticket_id,)
        ).fetchall()

        # 이력
        logs = conn.execute(
            "SELECT * FROM cs_action_logs WHERE ticket_id = ? ORDER BY created_at", (ticket_id,)
        ).fetchall()

        return {
            "ticket": dict(ticket),
            "test_result": dict(test_result) if test_result else None,
            "files": [dict(f) for f in files],
            "logs": [dict(l) for l in logs],
        }
    finally:
        conn.close()


# ── [3] 신규 접수 (Step 1: CS 직원) ──
@router.post("/tickets")
async def create_ticket(data: TicketCreate, user: dict = Depends(get_current_user)):
    ticket_id = _generate_ticket_id()
    now = now_kst()

    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO cs_tickets
               (ticket_id, customer_name, contact_info, product_name, serial_number,
                defect_symptom, courier, tracking_no, current_status, created_by, created_at, updated_at,
                sales_channel, order_number, cs_type, reason_category, quantity, shipping_cost_status,
                return_courier, return_tracking_no)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticket_id, data.customer_name, data.contact_info, data.product_name,
             data.serial_number, data.defect_symptom, data.courier, data.tracking_no,
             "접수완료", user["emp_cd"], now, now,
             data.sales_channel, data.order_number, data.cs_type, data.reason_category,
             data.quantity, data.shipping_cost_status, data.return_courier, data.return_tracking_no)
        )
        _log_action(conn, ticket_id, "접수완료", user["emp_cd"], user["name"],
                     data.memo or f"CS 접수: {data.product_name} - {data.defect_symptom[:50]}")
        conn.commit()

        return {"success": True, "ticket_id": ticket_id, "message": f"접수 완료 ({ticket_id})"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"접수 실패: {e}")
    finally:
        conn.close()


# ── [4] 물류 수령 (Step 2-1) ──
@router.put("/tickets/{ticket_id}/receive")
async def receive_package(ticket_id: str, data: StatusUpdate, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT current_status FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")
        if ticket["current_status"] != "접수완료":
            raise HTTPException(400, f"현재 상태({ticket['current_status']})에서는 수령 처리할 수 없습니다.")

        now = now_kst()
        conn.execute(
            "UPDATE cs_tickets SET current_status = ?, received_by = ?, received_at = ?, updated_at = ? WHERE ticket_id = ?",
            ("물류수령", user["emp_cd"], now, now, ticket_id)
        )
        _log_action(conn, ticket_id, "물류수령", user["emp_cd"], user["name"], data.memo or "택배 수령 완료")
        conn.commit()

        return {"success": True, "message": "물류 수령 완료"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [5] 기술팀 인계 (Step 2-2) ──
@router.put("/tickets/{ticket_id}/handover")
async def handover_to_tech(ticket_id: str, data: StatusUpdate, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT current_status FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")
        if ticket["current_status"] != "물류수령":
            raise HTTPException(400, f"현재 상태({ticket['current_status']})에서는 인계 처리할 수 없습니다.")

        now = now_kst()
        conn.execute(
            "UPDATE cs_tickets SET current_status = ?, handover_by = ?, handover_at = ?, updated_at = ? WHERE ticket_id = ?",
            ("기술인계", user["emp_cd"], now, now, ticket_id)
        )
        _log_action(conn, ticket_id, "기술인계", user["emp_cd"], user["name"], data.memo or "기술팀 인계 완료")
        conn.commit()

        return {"success": True, "message": "기술팀 인계 완료"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [6] 테스트 결과 등록 (Step 3) ──
@router.post("/tickets/{ticket_id}/test-result")
async def submit_test_result(ticket_id: str, data: TestResultCreate, user: dict = Depends(get_current_user)):
    if data.test_status not in TEST_RESULTS:
        raise HTTPException(400, f"유효하지 않은 테스트 상태: {data.test_status}")

    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT current_status FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")
        if ticket["current_status"] != "기술인계":
            raise HTTPException(400, f"현재 상태({ticket['current_status']})에서는 테스트 결과를 등록할 수 없습니다.")

        now = now_kst()

        # 기존 결과 삭제 후 재등록 (수정 대비)
        conn.execute("DELETE FROM cs_test_results WHERE ticket_id = ?", (ticket_id,))
        conn.execute(
            """INSERT INTO cs_test_results (ticket_id, test_status, test_comment, tested_by, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (ticket_id, data.test_status, data.test_comment, user["emp_cd"], now)
        )
        conn.execute(
            "UPDATE cs_tickets SET current_status = ?, tested_by = ?, tested_at = ?, updated_at = ? WHERE ticket_id = ?",
            ("테스트완료", user["emp_cd"], now, now, ticket_id)
        )

        emoji = {"정상": "🟢", "의심": "🟡", "불량": "🔴"}.get(data.test_status, "")
        _log_action(conn, ticket_id, "테스트완료", user["emp_cd"], user["name"],
                     f"{emoji} {data.test_status}: {data.test_comment[:100]}")
        conn.commit()

        return {"success": True, "message": "테스트 결과 등록 완료"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [7] 최종 처리 (Step 4) ──
@router.put("/tickets/{ticket_id}/resolve")
async def resolve_ticket(ticket_id: str, data: FinalAction, user: dict = Depends(get_current_user)):
    if data.action not in FINAL_ACTIONS:
        raise HTTPException(400, f"유효하지 않은 처리 방법: {data.action}")

    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT current_status FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")
        if ticket["current_status"] != "테스트완료":
            raise HTTPException(400, f"현재 상태({ticket['current_status']})에서는 최종 처리할 수 없습니다.")

        now = now_kst()
        conn.execute(
            """UPDATE cs_tickets SET current_status = ?, final_action = ?,
               resolved_by = ?, resolved_at = ?, updated_at = ?
               WHERE ticket_id = ?""",
            ("처리종결", data.action, user["emp_cd"], now, now, ticket_id)
        )
        _log_action(conn, ticket_id, "처리종결", user["emp_cd"], user["name"],
                     f"최종 처리: {data.action}" + (f" - {data.memo}" if data.memo else ""))
        conn.commit()

        return {"success": True, "message": f"티켓 종결 ({data.action})"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [7-2] 물류수령 단계에서 바로 처리종결 (단순변심 등) ──
@router.put("/tickets/{ticket_id}/quick-resolve")
async def quick_resolve(ticket_id: str, data: FinalAction, user: dict = Depends(get_current_user)):
    """물류수령 단계에서 기술인계 없이 바로 처리종결 (단순변심 반송 등)"""
    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT current_status FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")
        if ticket["current_status"] != "물류수령":
            raise HTTPException(400, f"현재 상태({ticket['current_status']})에서는 바로 종결할 수 없습니다. 물류수령 단계에서만 가능합니다.")

        now = now_kst()
        conn.execute(
            """UPDATE cs_tickets SET current_status = ?, final_action = ?,
               resolved_by = ?, resolved_at = ?, updated_at = ?
               WHERE ticket_id = ?""",
            ("처리종결", data.action, user["emp_cd"], now, now, ticket_id)
        )
        _log_action(conn, ticket_id, "처리종결", user["emp_cd"], user["name"],
                     f"즉시 종결: {data.action}" + (f" - {data.memo}" if data.memo else ""))
        conn.commit()

        return {"success": True, "message": f"티켓 즉시 종결 ({data.action})"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [8] 파일 업로드 (DB 바이너리 저장) ──
FILE_SIZE_DB_LIMIT = 5 * 1024 * 1024  # 5MB 이하만 DB BLOB, 초과 시 파일시스템


def _save_file_metadata_bg(ticket_id, original_name, file_type, total_size, emp_cd, emp_name, mime_type, disk_filename, file_blob):
    """백그라운드에서 DB 메타데이터 저장 (응답 지연 방지)"""
    try:
        conn = get_connection()
        try:
            if file_blob is not None:
                conn.execute(
                    """INSERT INTO cs_files (ticket_id, file_name, file_url, file_type, file_size, uploaded_by, created_at, mime_type, file_data, disk_filename)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ticket_id, original_name, "", file_type, total_size, emp_cd, now_kst(), mime_type, file_blob, disk_filename)
                )
            else:
                conn.execute(
                    """INSERT INTO cs_files (ticket_id, file_name, file_url, file_type, file_size, uploaded_by, created_at, mime_type, file_data, disk_filename)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                    (ticket_id, original_name, "", file_type, total_size, emp_cd, now_kst(), mime_type, disk_filename)
                )

            row = conn.execute(
                "SELECT id FROM cs_files WHERE ticket_id = ? ORDER BY id DESC LIMIT 1", (ticket_id,)
            ).fetchone()
            file_id = row["id"] if row else 0
            file_url = f"/api/cs/files/db/{file_id}"

            conn.execute("UPDATE cs_files SET file_url = ? WHERE id = ?", (file_url, file_id))
            _log_action(conn, ticket_id, "파일업로드", emp_cd, emp_name, f"{original_name} ({file_type})")
            conn.commit()
            logger.info(f"[CS] 파일 DB 저장 완료: {original_name} ({total_size} bytes) → id={file_id}")
        except Exception as e:
            conn.rollback()
            logger.error(f"[CS] 파일 DB 저장 실패: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"[CS] 파일 메타데이터 백그라운드 저장 예외: {e}")


@router.post("/tickets/{ticket_id}/upload")
async def upload_file(
    ticket_id: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = None,
):
    # 티켓 존재 확인
    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT ticket_id FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")
    finally:
        conn.close()

    # 파일 검증
    ext = Path(file.filename).suffix.lower() if file.filename else ".bin"
    video_exts = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v", ".flv", ".wmv", ".3gp", ".ts", ".mts"}
    image_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".heif", ".bmp", ".tiff", ".tif", ".svg"}
    doc_exts = {".pdf", ".xlsx", ".xls", ".csv", ".doc", ".docx", ".txt", ".zip"}
    allowed_exts = video_exts | image_exts | doc_exts
    if ext not in allowed_exts:
        raise HTTPException(400, f"허용되지 않는 파일 형식: {ext}")

    file_type = "video" if ext in video_exts else "image" if ext in image_exts else "document"

    mime_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
        ".gif": "image/gif", ".webp": "image/webp", ".heic": "image/heic",
        ".heif": "image/heif", ".bmp": "image/bmp", ".tiff": "image/tiff",
        ".tif": "image/tiff", ".svg": "image/svg+xml",
        ".mp4": "video/mp4", ".mov": "video/quicktime",
        ".avi": "video/x-msvideo", ".webm": "video/webm",
        ".mkv": "video/x-matroska", ".m4v": "video/x-m4v",
        ".flv": "video/x-flv", ".wmv": "video/x-ms-wmv",
        ".3gp": "video/3gpp", ".ts": "video/mp2t", ".mts": "video/mp2t",
        ".pdf": "application/pdf",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel", ".csv": "text/csv",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".txt": "text/plain", ".zip": "application/zip",
    }
    mime_type = mime_map.get(ext, file.content_type or "application/octet-stream")
    original_name = file.filename or f"file{ext}"
    max_size = 100 * 1024 * 1024  # 100MB

    # ── 1단계: 디스크에 직접 스트리밍 저장 ──
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    disk_filename = f"{ticket_id}_{uuid.uuid4().hex[:8]}{ext}"
    disk_path = UPLOAD_DIR / disk_filename
    total_size = 0

    try:
        with open(disk_path, "wb") as fp:
            while True:
                chunk = await file.read(256 * 1024)
                if not chunk:
                    break
                total_size += len(chunk)
                if total_size > max_size:
                    fp.close()
                    disk_path.unlink(missing_ok=True)
                    raise HTTPException(400, "파일 크기가 50MB를 초과합니다.")
                fp.write(chunk)
    except HTTPException:
        raise
    except Exception as e:
        disk_path.unlink(missing_ok=True)
        raise HTTPException(500, f"파일 저장 실패: {e}")

    # ── 2단계: 소용량은 DB BLOB 준비, 대용량은 NULL ──
    file_blob = None
    if total_size <= FILE_SIZE_DB_LIMIT:
        raw = disk_path.read_bytes()
        from db.database import USE_PG
        if USE_PG:
            import psycopg2
            file_blob = psycopg2.Binary(raw)
        else:
            file_blob = raw

    # ── 3단계: DB 메타데이터를 백그라운드로 저장 → 즉시 응답 ──
    if background_tasks is not None:
        background_tasks.add_task(
            _save_file_metadata_bg,
            ticket_id, original_name, file_type, total_size,
            user["emp_cd"], user["name"], mime_type, disk_filename, file_blob,
        )
    else:
        # fallback: 동기 저장
        _save_file_metadata_bg(
            ticket_id, original_name, file_type, total_size,
            user["emp_cd"], user["name"], mime_type, disk_filename, file_blob,
        )

    logger.info(f"[CS] 파일 디스크 저장 완료 (DB 백그라운드): {original_name} ({total_size} bytes)")
    return {"success": True, "file_name": original_name, "file_size": total_size}


# ── 파일 데이터 로딩 헬퍼 (DB BLOB 또는 디스크) ──
def _load_file_data(row) -> bytes:
    """cs_files row에서 파일 바이너리를 가져온다 (디스크 우선, DB 폴백)"""
    # 1) 디스크 파일 확인
    disk_fn = row["disk_filename"] if "disk_filename" in row.keys() else ""
    if disk_fn:
        disk_path = UPLOAD_DIR / disk_fn
        if disk_path.exists():
            return disk_path.read_bytes()
    # 2) DB BLOB
    if row["file_data"]:
        data = row["file_data"]
        return bytes(data) if not isinstance(data, bytes) else data
    return b""


# ── [9] 파일 서빙 (하이브리드: 디스크 + DB BLOB) ──
@router.get("/files/db/{file_id}")
async def serve_file_from_db(file_id: int, request: Request = None):
    """DB/디스크에 저장된 파일 서빙 (이미지/영상 미리보기용, Range 요청 지원)"""
    from fastapi.responses import Response, FileResponse
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT file_name, mime_type, file_data, disk_filename FROM cs_files WHERE id = ?", (file_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "파일을 찾을 수 없습니다.")

        mime = row["mime_type"] or "application/octet-stream"

        # 디스크 파일이 있으면 FileResponse로 효율적 서빙 (Range 자동 지원)
        disk_fn = row["disk_filename"] if "disk_filename" in row.keys() else ""
        if disk_fn:
            disk_path = UPLOAD_DIR / disk_fn
            if disk_path.exists():
                return FileResponse(
                    path=str(disk_path),
                    media_type=mime,
                    headers={"Cache-Control": "public, max-age=86400"},
                )

        # DB BLOB 서빙
        if not row["file_data"]:
            raise HTTPException(404, "파일 데이터가 없습니다.")
        data = bytes(row["file_data"]) if not isinstance(row["file_data"], bytes) else row["file_data"]
        headers = {
            "Cache-Control": "public, max-age=86400",
            "Accept-Ranges": "bytes",
            "Content-Length": str(len(data)),
        }

        # Range 요청 지원 (영상 시크/스트리밍용)
        if request and request.headers.get("range"):
            range_header = request.headers["range"]
            try:
                range_spec = range_header.replace("bytes=", "").strip()
                start_str, end_str = range_spec.split("-", 1)
                start = int(start_str) if start_str else 0
                end = int(end_str) if end_str else len(data) - 1
                end = min(end, len(data) - 1)
                chunk = data[start:end + 1]
                headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"
                headers["Content-Length"] = str(len(chunk))
                return Response(content=chunk, status_code=206, media_type=mime, headers=headers)
            except Exception:
                pass

        return Response(content=data, media_type=mime, headers=headers)
    finally:
        conn.close()


# ── [9-0] 파일 다운로드 (하이브리드) ──
@router.get("/download/{file_id}")
async def download_file(file_id: int, user: dict = Depends(get_current_user)):
    """첨부파일 다운로드"""
    from fastapi.responses import Response, FileResponse
    from urllib.parse import quote
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT file_name, mime_type, file_data, disk_filename FROM cs_files WHERE id = ?", (file_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "파일 레코드를 찾을 수 없습니다.")

        encoded_name = quote(row["file_name"])

        # 디스크 파일 우선
        disk_fn = row["disk_filename"] if "disk_filename" in row.keys() else ""
        if disk_fn:
            disk_path = UPLOAD_DIR / disk_fn
            if disk_path.exists():
                return FileResponse(
                    path=str(disk_path),
                    media_type="application/octet-stream",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_name}"},
                )

        # DB BLOB 다운로드
        if not row["file_data"]:
            raise HTTPException(404, "파일 데이터가 없습니다.")
        data = bytes(row["file_data"]) if not isinstance(row["file_data"], bytes) else row["file_data"]
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_name}"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[CS] 다운로드 오류 (file_id={file_id}): {e}")
        raise HTTPException(500, f"다운로드 실패: {e}")
    finally:
        conn.close()


# ── [9-0b] 로컬 파일 서빙 (하위호환) ──
@router.get("/files/{filename}")
async def serve_file_local(filename: str):
    """로컬에 저장된 파일 서빙 (레거시 호환)"""
    file_path = UPLOAD_DIR / filename
    if not file_path.exists():
        raise HTTPException(404, "파일을 찾을 수 없습니다.")
    from fastapi.responses import FileResponse
    return FileResponse(str(file_path))


# ── [9-1] 파일 삭제 ──
@router.delete("/files/{file_id}")
async def delete_file(file_id: int, user: dict = Depends(get_current_user)):
    """첨부파일 삭제 (DB + Google Drive/로컬 파일)"""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM cs_files WHERE id = ?", (file_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "파일을 찾을 수 없습니다.")

        ticket_id = row["ticket_id"]

        # DB 삭제 (파일 바이너리도 함께 삭제됨)
        conn.execute("DELETE FROM cs_files WHERE id = ?", (file_id,))
        _log_action(conn, ticket_id, "파일삭제", user["emp_cd"], user["name"], row["file_name"])
        conn.commit()

        return {"success": True, "message": "파일 삭제 완료"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [9-2] Google Drive 업로드 진단 ──
@router.get("/drive-check")
async def drive_upload_check(user: dict = Depends(get_current_user)):
    """Google Drive 업로드 설정 진단"""
    from config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_CS_FOLDER_ID
    result = {
        "service_account_json_set": bool(GOOGLE_SERVICE_ACCOUNT_JSON),
        "service_account_json_length": len(GOOGLE_SERVICE_ACCOUNT_JSON) if GOOGLE_SERVICE_ACCOUNT_JSON else 0,
        "cs_folder_id": GOOGLE_CS_FOLDER_ID or "(미설정)",
    }

    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            import json
            sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            result["service_account_email"] = sa_info.get("client_email", "(없음)")
            result["project_id"] = sa_info.get("project_id", "(없음)")
            result["json_parse"] = "OK"
        except Exception as e:
            result["json_parse"] = f"FAIL: {e}"
            return result

    if GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_CS_FOLDER_ID:
        try:
            from services.google_drive_service import _get_access_token
            token = await _get_access_token()
            result["access_token"] = f"{token[:20]}..." if token else "(없음)"
            result["token_status"] = "OK"
        except Exception as e:
            result["token_status"] = f"FAIL: {e}"
            return result

        # 폴더 접근 테스트
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    "https://www.googleapis.com/drive/v3/files",
                    params={
                        "q": f"'{GOOGLE_CS_FOLDER_ID}' in parents and trashed=false",
                        "fields": "files(id,name)",
                        "pageSize": 3,
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )
                if r.status_code == 200:
                    files = r.json().get("files", [])
                    result["folder_access"] = "OK"
                    result["folder_files"] = [f["name"] for f in files]
                else:
                    result["folder_access"] = f"FAIL ({r.status_code}): {r.text[:300]}"
        except Exception as e:
            result["folder_access"] = f"ERROR: {e}"

        # 테스트 파일 업로드 시도
        try:
            import httpx, json as _json
            boundary = "---test-boundary---"
            metadata = _json.dumps({"name": "_drive_test.txt", "parents": [GOOGLE_CS_FOLDER_ID]})
            test_body = (
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                f"{metadata}\r\n--{boundary}\r\nContent-Type: text/plain\r\n\r\n"
                f"drive upload test\r\n--{boundary}--\r\n"
            ).encode()
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": f"multipart/related; boundary={boundary}",
                    },
                    content=test_body,
                )
                if r.status_code == 200:
                    test_id = r.json().get("id", "")
                    result["upload_test"] = f"OK (file_id={test_id})"
                    # 테스트 파일 삭제
                    await client.delete(
                        f"https://www.googleapis.com/drive/v3/files/{test_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=5,
                    )
                else:
                    err_text = r.text[:500].replace("\n", " ")
                    result["upload_test"] = f"FAIL ({r.status_code}): {err_text}"
        except Exception as e:
            result["upload_test"] = f"ERROR: {e}"

    return result


# ── [10] 이력 조회 ──
@router.get("/tickets/{ticket_id}/logs")
async def get_ticket_logs(ticket_id: str, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        logs = conn.execute(
            "SELECT * FROM cs_action_logs WHERE ticket_id = ? ORDER BY created_at", (ticket_id,)
        ).fetchall()
        return {"logs": [dict(l) for l in logs]}
    finally:
        conn.close()


# ── [11] 메모 추가 ──
@router.post("/tickets/{ticket_id}/memo")
async def add_memo(ticket_id: str, data: StatusUpdate, user: dict = Depends(get_current_user)):
    if not data.memo:
        raise HTTPException(400, "메모 내용을 입력해주세요.")

    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT ticket_id FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")

        _log_action(conn, ticket_id, "메모", user["emp_cd"], user["name"], data.memo)
        conn.commit()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [12] 티켓 삭제 ──
@router.delete("/tickets/{ticket_id}")
async def delete_ticket(ticket_id: str, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        ticket = conn.execute(
            "SELECT ticket_id, current_status FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")

        # 관련 데이터 모두 삭제 (파일, 테스트 결과, 액션 로그)
        conn.execute("DELETE FROM cs_files WHERE ticket_id = ?", (ticket_id,))
        conn.execute("DELETE FROM cs_test_results WHERE ticket_id = ?", (ticket_id,))
        conn.execute("DELETE FROM cs_action_logs WHERE ticket_id = ?", (ticket_id,))
        conn.execute("DELETE FROM cs_tickets WHERE ticket_id = ?", (ticket_id,))
        conn.commit()

        return {"success": True, "message": f"티켓 {ticket_id} 삭제 완료"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"삭제 실패: {e}")
    finally:
        conn.close()


# ── [12-1] 티켓 수정 ──
@router.put("/tickets/{ticket_id}/edit")
async def edit_ticket(ticket_id: str, data: TicketEdit, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        ticket = conn.execute("SELECT ticket_id FROM cs_tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
        if not ticket:
            raise HTTPException(404, "티켓을 찾을 수 없습니다.")

        updates = []
        params = []
        for field in ["customer_name", "contact_info", "product_name", "serial_number",
                       "defect_symptom", "sales_channel", "order_number", "cs_type",
                       "reason_category", "quantity", "shipping_cost_status",
                       "return_courier", "return_tracking_no"]:
            val = getattr(data, field, None)
            if val is not None:
                updates.append(f"{field} = ?")
                params.append(val)

        if not updates:
            return {"success": True, "message": "변경 사항 없음"}

        updates.append("updated_at = ?")
        params.append(now_kst())
        params.append(ticket_id)

        conn.execute(f"UPDATE cs_tickets SET {', '.join(updates)} WHERE ticket_id = ?", params)
        _log_action(conn, ticket_id, "내용수정", user["emp_cd"], user["name"],
                     f"수정 항목: {', '.join(f.split(' = ')[0] for f in updates[:-1])}")
        conn.commit()
        return {"success": True, "message": "수정 완료"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"수정 실패: {e}")
    finally:
        conn.close()


# ═══════════════════════════════════════
#  미출고 관리 API
# ═══════════════════════════════════════
BACKORDER_STATUSES = ["미출고", "출고완료", "취소"]

@router.get("/backorders")
async def list_backorders(
    status: str = Query("", description="처리상태 필터"),
    channel: str = Query("", description="채널 필터"),
    search: str = Query("", description="검색"),
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=500),
    user: dict = Depends(get_current_user),
):
    conn = get_connection()
    try:
        where, params = [], []
        if status:
            where.append("status = ?"); params.append(status)
        if channel:
            where.append("sales_channel = ?"); params.append(channel)
        if search:
            st = f"%{search}%"
            where.append("(recipient_name LIKE ? OR order_number LIKE ? OR product_name LIKE ?)")
            params.extend([st, st, st])
        wsql = (" WHERE " + " AND ".join(where)) if where else ""

        total = conn.execute(f"SELECT COUNT(*) as cnt FROM cs_backorders{wsql}", params).fetchone()["cnt"]
        offset = (page - 1) * size
        rows = conn.execute(
            f"SELECT * FROM cs_backorders{wsql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [size, offset]
        ).fetchall()

        # 통계
        stats_rows = conn.execute("SELECT status, COUNT(*) as cnt FROM cs_backorders GROUP BY status").fetchall()
        status_counts = {r["status"]: r["cnt"] for r in stats_rows}
        ch_rows = conn.execute("SELECT sales_channel, COUNT(*) as cnt FROM cs_backorders WHERE status='미출고' GROUP BY sales_channel ORDER BY cnt DESC").fetchall()
        channel_counts = {r["sales_channel"]: r["cnt"] for r in ch_rows}

        return {
            "backorders": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "status_counts": status_counts,
            "channel_counts": channel_counts,
        }
    finally:
        conn.close()


@router.post("/backorders")
async def create_backorder(data: BackorderCreate, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        now = now_kst()
        conn.execute(
            """INSERT INTO cs_backorders
               (sales_channel, order_date, order_number, recipient_name, recipient_phone,
                product_name, option_info, quantity, status, memo, created_by, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (data.sales_channel, data.order_date, data.order_number, data.recipient_name,
             data.recipient_phone, data.product_name, data.option_info, data.quantity,
             data.status, data.memo, user["emp_cd"], now, now)
        )
        conn.commit()
        return {"success": True, "message": "미출고 접수 완료"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"접수 실패: {e}")
    finally:
        conn.close()


@router.put("/backorders/{bo_id}")
async def update_backorder(bo_id: int, data: BackorderCreate, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE cs_backorders SET sales_channel=?, order_date=?, order_number=?, recipient_name=?,
               recipient_phone=?, product_name=?, option_info=?, quantity=?, status=?, memo=?, updated_at=?
               WHERE id=?""",
            (data.sales_channel, data.order_date, data.order_number, data.recipient_name,
             data.recipient_phone, data.product_name, data.option_info, data.quantity,
             data.status, data.memo, now_kst(), bo_id)
        )
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@router.delete("/backorders/{bo_id}")
async def delete_backorder(bo_id: int, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM cs_backorders WHERE id = ?", (bo_id,))
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@router.put("/backorders/{bo_id}/status")
async def update_backorder_status(bo_id: int, status: str = Query(...), user: dict = Depends(get_current_user)):
    if status not in BACKORDER_STATUSES:
        raise HTTPException(400, f"유효하지 않은 상태: {status}")
    conn = get_connection()
    try:
        conn.execute("UPDATE cs_backorders SET status=?, updated_at=? WHERE id=?", (status, now_kst(), bo_id))
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [13] 대시보드 통계 ──
@router.get("/stats")
async def cs_stats(user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        # 상태별 카운트
        status_rows = conn.execute(
            "SELECT current_status, COUNT(*) as cnt FROM cs_tickets GROUP BY current_status"
        ).fetchall()
        status_counts = {r["current_status"]: r["cnt"] for r in status_rows}

        # 오늘 접수
        KST = timezone(timedelta(hours=9))
        today = datetime.now(KST).strftime("%Y-%m-%d")
        today_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM cs_tickets WHERE created_at LIKE ?",
            (f"{today}%",)
        ).fetchone()

        # 최근 7일 추이
        trend_rows = conn.execute(
            """SELECT DATE(created_at) as dt, COUNT(*) as cnt
               FROM cs_tickets
               GROUP BY DATE(created_at)
               ORDER BY dt DESC LIMIT 7"""
        ).fetchall()

        # 채널별 카운트
        channel_rows = conn.execute(
            "SELECT sales_channel, COUNT(*) as cnt FROM cs_tickets WHERE sales_channel != '' GROUP BY sales_channel ORDER BY cnt DESC"
        ).fetchall()
        channel_counts = {r["sales_channel"]: r["cnt"] for r in channel_rows}

        # 채널별 미처리(접수완료~테스트완료) 카운트
        channel_active_rows = conn.execute(
            """SELECT sales_channel, COUNT(*) as cnt FROM cs_tickets
               WHERE sales_channel != '' AND current_status != '처리종결'
               GROUP BY sales_channel ORDER BY cnt DESC"""
        ).fetchall()
        channel_active = {r["sales_channel"]: r["cnt"] for r in channel_active_rows}

        # 사유별 카운트
        reason_rows = conn.execute(
            "SELECT reason_category, COUNT(*) as cnt FROM cs_tickets WHERE reason_category != '' GROUP BY reason_category ORDER BY cnt DESC"
        ).fetchall()
        reason_counts = {r["reason_category"]: r["cnt"] for r in reason_rows}

        # CS유형별 카운트
        type_rows = conn.execute(
            "SELECT cs_type, COUNT(*) as cnt FROM cs_tickets WHERE cs_type != '' GROUP BY cs_type ORDER BY cnt DESC"
        ).fetchall()
        type_counts = {r["cs_type"]: r["cnt"] for r in type_rows}

        # 배송비 상태별 카운트
        shipping_cost_rows = conn.execute(
            "SELECT shipping_cost_status, COUNT(*) as cnt FROM cs_tickets WHERE shipping_cost_status != '' GROUP BY shipping_cost_status ORDER BY cnt DESC"
        ).fetchall()
        shipping_cost_counts = {r["shipping_cost_status"]: r["cnt"] for r in shipping_cost_rows}

        # 지연 건수 (접수 후 7일 이상 미처리)
        from db.database import USE_PG
        if USE_PG:
            overdue_sql = """SELECT COUNT(*) as cnt FROM cs_tickets
               WHERE current_status != '처리종결'
               AND created_at::timestamp < NOW() - INTERVAL '7 days'"""
        else:
            overdue_sql = """SELECT COUNT(*) as cnt FROM cs_tickets
               WHERE current_status != '처리종결'
               AND created_at < datetime('now', '-7 days', 'localtime')"""
        overdue_row = conn.execute(overdue_sql).fetchone()

        # 평균 처리일수 (종결된 건만)
        if USE_PG:
            avg_sql = """SELECT AVG(EXTRACT(EPOCH FROM (resolved_at::timestamp - created_at::timestamp)) / 86400) as avg_days
               FROM cs_tickets WHERE current_status = '처리종결' AND resolved_at IS NOT NULL AND resolved_at != ''"""
        else:
            avg_sql = """SELECT AVG(julianday(resolved_at) - julianday(created_at)) as avg_days
               FROM cs_tickets WHERE current_status = '처리종결' AND resolved_at != ''"""
        avg_row = conn.execute(avg_sql).fetchone()

        return {
            "status_counts": status_counts,
            "today_count": today_row["cnt"] if today_row else 0,
            "trend": [dict(r) for r in trend_rows],
            "total": sum(status_counts.values()),
            "channel_counts": channel_counts,
            "channel_active": channel_active,
            "reason_counts": reason_counts,
            "type_counts": type_counts,
            "shipping_cost_counts": shipping_cost_counts,
            "overdue_count": overdue_row["cnt"] if overdue_row else 0,
            "avg_resolution_days": round(avg_row["avg_days"], 1) if avg_row and avg_row["avg_days"] else 0,
        }
    finally:
        conn.close()


# ── [14] CS 옵션 목록 (프론트 드롭다운용) ──
@router.get("/options")
async def cs_options(user: dict = Depends(get_current_user)):
    return {
        "statuses": CS_STATUSES,
        "final_actions": FINAL_ACTIONS,
        "test_results": TEST_RESULTS,
        "cs_types": CS_TYPES,
        "sales_channels": SALES_CHANNELS,
        "reason_categories": REASON_CATEGORIES,
        "shipping_cost_statuses": SHIPPING_COST_STATUSES,
    }
