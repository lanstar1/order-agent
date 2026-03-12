"""
판매 에이전트 API 라우터
- xlsx 업로드 + 파싱
- AI 분석 실행 (6개 에이전트 병렬)
- 분석 결과 조회
- 분석 이력 관리
"""
import os
import uuid
import json
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import APIRouter, Depends, UploadFile, File, Form, Query, HTTPException
from typing import Optional

from security import get_current_user
from db.database import get_connection, now_kst

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sales-agent", tags=["sales-agent"])

# 업로드 디렉토리
UPLOAD_DIR = Path(__file__).parent.parent.parent.parent / "data" / "sales_uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# 진행 중인 분석 작업 상태 (메모리)
_running_jobs = {}


# ═══════════════════════════════════════════
#  xlsx 업로드 + 파싱
# ═══════════════════════════════════════════
@router.post("/upload")
async def upload_xlsx(
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
    """xlsx 파일 업로드 및 파싱"""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "xlsx 또는 xls 파일만 업로드할 수 있습니다.")

    # 파일 저장
    file_id = str(uuid.uuid4())[:8]
    safe_name = f"{file_id}_{file.filename}"
    save_path = UPLOAD_DIR / safe_name

    content = await file.read()
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 50MB를 초과합니다.")

    with open(save_path, "wb") as f:
        f.write(content)

    # 파싱
    try:
        from services.sales_agent.xlsx_parser import parse_xlsx
        parsed = parse_xlsx(str(save_path))
    except Exception as e:
        logger.error(f"[SalesAgent] xlsx 파싱 실패: {e}")
        # 파싱 실패해도 파일은 유지 (재시도 가능)
        raise HTTPException(400, f"파일 파싱 실패: {str(e)}")

    # DB에 업로드 기록 저장
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO sa_uploads (file_id, file_name, file_path, file_size,
                total_rows, total_customers, total_products, total_amount,
                period_start, period_end, uploaded_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file_id,
            file.filename,
            str(save_path),
            len(content),
            parsed["summary"]["total_rows"],
            parsed["summary"]["total_customers"],
            parsed["summary"]["total_products"],
            parsed["summary"]["total_amount"],
            parsed.get("period_start", ""),
            parsed.get("period_end", ""),
            user.get("emp_cd", ""),
            now_kst(),
        ))
        conn.commit()
    finally:
        conn.close()

    return {
        "file_id": file_id,
        "file_name": file.filename,
        "summary": parsed["summary"],
        "period_start": parsed.get("period_start", ""),
        "period_end": parsed.get("period_end", ""),
        "customers_preview": parsed["customers"][:5],
        "products_preview": parsed["products"][:5],
    }


# ═══════════════════════════════════════════
#  분석 실행
# ═══════════════════════════════════════════
@router.post("/analyze")
async def start_analysis(
    file_id: str = Form(...),
    user=Depends(get_current_user),
):
    """AI 분석 시작 (6개 에이전트 병렬 실행)"""
    # 업로드 파일 확인
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM sa_uploads WHERE file_id = ?", (file_id,)).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, "업로드된 파일을 찾을 수 없습니다.")

    # 파싱
    from services.sales_agent.xlsx_parser import parse_xlsx
    parsed = parse_xlsx(row["file_path"])

    # job 생성
    job_id = f"SA-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"

    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO sa_jobs (job_id, file_id, status, created_by, created_at, updated_at)
            VALUES (?, ?, 'running', ?, ?, ?)
        """, (job_id, file_id, user.get("emp_cd", ""), now_kst(), now_kst()))
        conn.commit()
    finally:
        conn.close()

    # 진행 상태 초기화
    _running_jobs[job_id] = {
        "status": "running",
        "agents": {k: "pending" for k in ["customer", "product", "strategy", "future", "partnership", "visualization"]},
        "progress": 0,
    }

    # 비동기로 분석 실행
    asyncio.create_task(_run_analysis_task(job_id, file_id, parsed, user))

    return {
        "job_id": job_id,
        "status": "running",
        "message": "6개 AI 에이전트 분석이 시작되었습니다.",
    }


async def _run_analysis_task(job_id: str, file_id: str, parsed: dict, user: dict):
    """백그라운드 분석 실행"""
    try:
        from services.sales_agent.orchestrator import run_analysis
        from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

        async def progress_cb(agent_key, status, pct):
            if job_id in _running_jobs:
                _running_jobs[job_id]["agents"][agent_key] = status
                done = sum(1 for s in _running_jobs[job_id]["agents"].values() if s == "done")
                _running_jobs[job_id]["progress"] = int(done / 6 * 100)

        result = await run_analysis(
            sales_data=parsed,
            api_key=ANTHROPIC_API_KEY,
            model=CLAUDE_MODEL,
            progress_callback=progress_cb,
        )

        result["job_id"] = job_id

        # 결과 DB 저장
        conn = get_connection()
        try:
            conn.execute("""
                UPDATE sa_jobs
                SET status = 'completed',
                    result_json = ?,
                    elapsed_seconds = ?,
                    updated_at = ?
                WHERE job_id = ?
            """, (
                json.dumps(result, ensure_ascii=False, default=str),
                result.get("elapsed_seconds", 0),
                now_kst(),
                job_id,
            ))
            conn.commit()
        finally:
            conn.close()

        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = "completed"
            _running_jobs[job_id]["progress"] = 100

        logger.info(f"[SalesAgent] 분석 완료: {job_id} ({result.get('elapsed_seconds', 0)}초)")

    except Exception as e:
        logger.error(f"[SalesAgent] 분석 실패: {job_id} - {e}", exc_info=True)
        conn = get_connection()
        try:
            conn.execute("""
                UPDATE sa_jobs SET status = 'failed', result_json = ?, updated_at = ?
                WHERE job_id = ?
            """, (json.dumps({"error": str(e)}, ensure_ascii=False), now_kst(), job_id))
            conn.commit()
        finally:
            conn.close()

        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = "failed"


# ═══════════════════════════════════════════
#  분석 상태 조회
# ═══════════════════════════════════════════
@router.get("/status/{job_id}")
async def get_analysis_status(job_id: str, user=Depends(get_current_user)):
    """분석 진행 상태 조회"""
    # 메모리에서 실시간 상태 확인
    if job_id in _running_jobs:
        return _running_jobs[job_id]

    # DB에서 확인
    conn = get_connection()
    try:
        row = conn.execute("SELECT status, elapsed_seconds FROM sa_jobs WHERE job_id = ?", (job_id,)).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, "분석 작업을 찾을 수 없습니다.")

    return {
        "status": row["status"],
        "progress": 100 if row["status"] == "completed" else 0,
        "elapsed_seconds": row["elapsed_seconds"],
    }


# ═══════════════════════════════════════════
#  분석 결과 조회
# ═══════════════════════════════════════════
@router.get("/result/{job_id}")
async def get_analysis_result(job_id: str, user=Depends(get_current_user)):
    """분석 결과 전체 조회"""
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT j.*, u.file_name
            FROM sa_jobs j
            LEFT JOIN sa_uploads u ON j.file_id = u.file_id
            WHERE j.job_id = ?
        """, (job_id,)).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, "분석 결과를 찾을 수 없습니다.")

    if row["status"] != "completed":
        raise HTTPException(400, f"분석이 아직 완료되지 않았습니다. (상태: {row['status']})")

    result = json.loads(row["result_json"]) if row["result_json"] else {}
    result["file_name"] = row["file_name"]

    return result


@router.get("/result/{job_id}/agent/{agent_key}")
async def get_agent_result(job_id: str, agent_key: str, user=Depends(get_current_user)):
    """개별 에이전트 분석 결과 조회"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT result_json FROM sa_jobs WHERE job_id = ?", (job_id,)).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, "분석 결과를 찾을 수 없습니다.")

    result = json.loads(row["result_json"]) if row["result_json"] else {}
    agents = result.get("agents", {})

    if agent_key not in agents:
        raise HTTPException(404, f"에이전트 '{agent_key}' 결과를 찾을 수 없습니다.")

    return agents[agent_key]


# ═══════════════════════════════════════════
#  분석 이력
# ═══════════════════════════════════════════
@router.get("/history")
async def get_analysis_history(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    user=Depends(get_current_user),
):
    """분석 이력 조회"""
    conn = get_connection()
    try:
        offset = (page - 1) * size

        total_row = conn.execute("SELECT COUNT(*) as cnt FROM sa_jobs").fetchone()
        total = total_row["cnt"] if total_row else 0

        rows = conn.execute("""
            SELECT j.job_id, j.file_id, j.status, j.elapsed_seconds,
                   j.created_by, j.created_at, u.file_name,
                   u.total_rows, u.total_customers, u.total_products, u.total_amount
            FROM sa_jobs j
            LEFT JOIN sa_uploads u ON j.file_id = u.file_id
            ORDER BY j.created_at DESC
            LIMIT ? OFFSET ?
        """, (size, offset)).fetchall()
    finally:
        conn.close()

    return {
        "jobs": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "size": size,
    }


# ═══════════════════════════════════════════
#  분석 삭제
# ═══════════════════════════════════════════
@router.delete("/jobs/{job_id}")
async def delete_job(job_id: str, user=Depends(get_current_user)):
    """분석 이력 삭제"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT file_id FROM sa_jobs WHERE job_id = ?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(404, "분석 작업을 찾을 수 없습니다.")
        conn.execute("DELETE FROM sa_jobs WHERE job_id = ?", (job_id,))
        conn.commit()
    finally:
        conn.close()

    if job_id in _running_jobs:
        del _running_jobs[job_id]

    return {"message": "삭제되었습니다."}
