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
from fastapi import APIRouter, Depends, Query, UploadFile, File, Form, HTTPException
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
FINAL_ACTIONS = ["교환발송", "환불처리", "정상반송"]
TEST_RESULTS = ["정상", "의심", "불량"]


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


class StatusUpdate(BaseModel):
    memo: str = ""


class TestResultCreate(BaseModel):
    test_status: str          # 정상 / 의심 / 불량
    test_comment: str = ""


class FinalAction(BaseModel):
    action: str               # 교환발송 / 환불처리 / 정상반송
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
    search: str = Query("", description="검색 (고객명/연락처/접수번호)"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    user: dict = Depends(get_current_user),
):
    conn = get_connection()
    try:
        where_clauses = []
        params = []

        if status:
            where_clauses.append("t.current_status = ?")
            params.append(status)

        if search:
            search_term = f"%{search}%"
            where_clauses.append(
                "(t.ticket_id LIKE ? OR t.customer_name LIKE ? OR t.contact_info LIKE ?)"
            )
            params.extend([search_term, search_term, search_term])

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

        # 첨부파일
        files = conn.execute(
            "SELECT * FROM cs_files WHERE ticket_id = ? ORDER BY created_at", (ticket_id,)
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
                defect_symptom, courier, tracking_no, current_status, created_by, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticket_id, data.customer_name, data.contact_info, data.product_name,
             data.serial_number, data.defect_symptom, data.courier, data.tracking_no,
             "접수완료", user["emp_cd"], now, now)
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


# ── [8] 파일 업로드 (Google Drive 또는 로컬) ──
@router.post("/tickets/{ticket_id}/upload")
async def upload_file(
    ticket_id: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
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
    allowed_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".pdf"}
    if ext not in allowed_exts:
        raise HTTPException(400, f"허용되지 않는 파일 형식: {ext}")

    content = await file.read()
    if len(content) > 50 * 1024 * 1024:  # 50MB 제한
        raise HTTPException(400, "파일 크기가 50MB를 초과합니다.")

    file_type = "video" if ext in {".mp4", ".mov"} else "image" if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp"} else "document"

    # MIME 타입 결정
    mime_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
        ".gif": "image/gif", ".webp": "image/webp",
        ".mp4": "video/mp4", ".mov": "video/quicktime",
        ".pdf": "application/pdf",
    }
    mime_type = mime_map.get(ext, "application/octet-stream")

    file_id_short = str(uuid.uuid4())[:8]
    filename = f"{ticket_id}_{file_id_short}{ext}"
    drive_file_id = ""

    # Google Drive 업로드 시도 (설정되어 있으면)
    try:
        from services.google_drive_service import upload_to_drive, _is_configured
        if _is_configured():
            result = await upload_to_drive(
                file_content=content,
                filename=filename,
                mime_type=mime_type,
                subfolder_name=ticket_id,
            )
            file_url = result["file_url"]
            drive_file_id = result["file_id"]
            logger.info(f"[CS] 파일 Google Drive 업로드 완료: {filename} → {drive_file_id}")
        else:
            raise RuntimeError("Google Drive 미설정 — 로컬 저장으로 전환")
    except Exception as e:
        # Google Drive 실패 시 로컬 저장 (개발환경 또는 설정 전)
        logger.warning(f"[CS] Google Drive 업로드 실패, 로컬 저장: {e}")
        file_path = UPLOAD_DIR / filename
        with open(file_path, "wb") as f:
            f.write(content)
        file_url = f"/api/cs/files/{filename}"

    # DB 기록
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO cs_files (ticket_id, file_name, file_url, file_type, file_size, uploaded_by, created_at, drive_file_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticket_id, file.filename or filename, file_url, file_type, len(content), user["emp_cd"], now_kst(), drive_file_id)
        )
        _log_action(conn, ticket_id, "파일업로드", user["emp_cd"], user["name"], f"{file.filename} ({file_type})")
        conn.commit()

        return {"success": True, "file_url": file_url, "file_name": filename, "drive_file_id": drive_file_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        conn.close()


# ── [9] 파일 서빙 (로컬 폴백) ──
@router.get("/files/{filename}")
async def serve_file(filename: str):
    """로컬에 저장된 파일 서빙 (Google Drive 파일은 직접 URL 사용)"""
    file_path = UPLOAD_DIR / filename
    if not file_path.exists():
        raise HTTPException(404, "파일을 찾을 수 없습니다. (Render 재배포 시 로컬 파일은 삭제됩니다)")
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
        drive_file_id = row.get("drive_file_id", "")

        # Google Drive 파일 삭제
        if drive_file_id:
            try:
                from services.google_drive_service import delete_from_drive
                await delete_from_drive(drive_file_id)
                logger.info(f"[CS] Drive 파일 삭제: {drive_file_id}")
            except Exception as e:
                logger.warning(f"[CS] Drive 파일 삭제 실패: {e}")
        else:
            # 로컬 파일 삭제
            file_url = row.get("file_url", "")
            if file_url.startswith("/api/cs/files/"):
                local_name = file_url.split("/")[-1]
                local_path = UPLOAD_DIR / local_name
                if local_path.exists():
                    local_path.unlink()

        # DB 삭제
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


# ── [12] 대시보드 통계 ──
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

        return {
            "status_counts": status_counts,
            "today_count": today_row["cnt"] if today_row else 0,
            "trend": [dict(r) for r in trend_rows],
            "total": sum(status_counts.values()),
        }
    finally:
        conn.close()
