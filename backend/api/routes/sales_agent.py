"""
판매 에이전트 API 라우트
- POST /upload  — xlsx 업로드 + 파싱
- POST /analyze — AI 분석 시작 (Mode A/B)
- GET  /status/{job_id} — 진행 상태
- GET  /result/{job_id} — 분석 결과
- GET  /history — 분석 이력
- GET  /customers/{file_id} — 거래처 목록 (Mode B 선택용)
- WS   /ws/{job_id} — 실시간 진행
"""
from __future__ import annotations
import asyncio, json, uuid, os, logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, WebSocket, WebSocketDisconnect
from security import get_current_user
from db.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sales-agent", tags=["sales-agent"])

_running_jobs: dict = {}
_ws_connections: dict[str, set[WebSocket]] = {}

UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "sa_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

def now_kst():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")


# ═══════════════════════════════════════════
#  xlsx 업로드 + 파싱
# ═══════════════════════════════════════════
@router.post("/upload")
async def upload_xlsx(
    file: UploadFile = File(...),
    mode: str = Form("multi"),
    target_customer_code: Optional[str] = Form(None),
    user=Depends(get_current_user),
):
    try:
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(400, "xlsx 파일만 업로드 가능합니다")

        file_id = f"SAF-{uuid.uuid4().hex[:8].upper()}"
        save_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
        content = await file.read()
        logger.info(f"[SA Upload] 파일 저장: {save_path} ({len(content)} bytes)")
        with open(save_path, "wb") as f:
            f.write(content)

        logger.info(f"[SA Upload] 파싱 시작: mode={mode}")
        from services.sales_agent.xlsx_parser import parse_xlsx
        parsed = parse_xlsx(save_path, mode=mode, target_customer_code=target_customer_code)
        logger.info(f"[SA Upload] 파싱 완료: rows={parsed.total_rows}, customers={parsed.total_customers}")

        # DB 저장
        emp_cd = user.get("emp_cd", "") if isinstance(user, dict) else ""
        conn = get_connection()
        try:
            conn.execute("""
                INSERT INTO sa_uploads (file_id, file_name, file_path, file_size,
                    total_rows, total_customers, total_products, total_amount,
                    period_start, period_end, analysis_mode, target_customer, uploaded_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (file_id, file.filename, save_path, len(content),
                  parsed.total_rows, parsed.total_customers, parsed.total_products, int(parsed.total_amount),
                  parsed.period_start, parsed.period_end, mode,
                  parsed.target_customer_name or "", emp_cd, now_kst()))
            conn.commit()
            logger.info(f"[SA Upload] DB 저장 완료: {file_id}")
        finally:
            conn.close()

        return {
            "file_id": file_id,
            "file_name": file.filename,
            "mode": mode,
            "total_rows": parsed.total_rows,
            "total_customers": parsed.total_customers,
            "total_products": parsed.total_products,
            "total_amount": parsed.total_amount,
            "customers_preview": parsed.customers[:20],
            "period_start": parsed.period_start,
            "period_end": parsed.period_end,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[SA Upload] 오류: {e}", exc_info=True)
        raise HTTPException(500, f"업로드 처리 오류: {str(e)}")


# ═══════════════════════════════════════════
#  거래처 목록 (Mode B 선택용)
# ═══════════════════════════════════════════
@router.get("/customers/{file_id}")
async def get_customers(file_id: str, user=Depends(get_current_user)):
    """업로드된 파일의 거래처 목록 반환 (Mode B에서 거래처 선택 시 사용)"""
    conn = get_connection()
    try:
        row = conn.execute("SELECT file_path FROM sa_uploads WHERE file_id = ?", (file_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, "파일을 찾을 수 없습니다")

    from services.sales_agent.xlsx_parser import parse_xlsx
    parsed = parse_xlsx(row[0], mode="multi")
    return {"customers": parsed.customers}


# ═══════════════════════════════════════════
#  AI 분석 시작
# ═══════════════════════════════════════════
@router.post("/analyze")
async def start_analysis(
    file_id: str = Form(...),
    mode: str = Form("multi"),
    target_customer_code: Optional[str] = Form(None),
    user=Depends(get_current_user),
):
    try:
        conn = get_connection()
        try:
            row = conn.execute("SELECT file_path FROM sa_uploads WHERE file_id = ?", (file_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            raise HTTPException(404, "업로드된 파일을 찾을 수 없습니다")

        file_path = row[0] if not hasattr(row, 'get') else row.get('file_path', row[0])
        logger.info(f"[SA Analyze] file_id={file_id}, file_path={file_path}, mode={mode}")

        from services.sales_agent.xlsx_parser import parse_xlsx
        parsed = parse_xlsx(file_path, mode=mode, target_customer_code=target_customer_code)
        logger.info(f"[SA Analyze] 파싱 완료: rows={parsed.total_rows}")

        job_id = f"SA-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"

        conn = get_connection()
        try:
            conn.execute("""
                INSERT INTO sa_jobs (job_id, file_id, status, analysis_mode, target_customer, created_by, created_at, updated_at)
                VALUES (?, ?, 'running', ?, ?, ?, ?, ?)
            """, (job_id, file_id, mode, parsed.target_customer_name or "",
                  user.get("emp_cd", "") if isinstance(user, dict) else "", now_kst(), now_kst()))
            conn.commit()
            logger.info(f"[SA Analyze] Job 생성 완료: {job_id}")
        finally:
            conn.close()

        # 에이전트 수 결정
        is_single = mode == "single"
        agent_keys = ["product", "strategy", "future", "partnership", "visualization"]
        if not is_single:
            agent_keys = ["customer"] + agent_keys

        _running_jobs[job_id] = {
            "status": "running",
            "agents": {k: "pending" for k in agent_keys},
            "progress": 0,
            "logs": [],
            "mode": mode,
        }

        asyncio.create_task(_run_analysis_task(job_id, file_id, parsed, user))

        return {
            "job_id": job_id,
            "status": "running",
            "mode": mode,
            "agent_count": len(agent_keys),
            "message": f"{len(agent_keys)}개 AI 에이전트 분석이 시작되었습니다.",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[SA Analyze] 오류: {e}", exc_info=True)
        raise HTTPException(500, f"분석 시작 오류: {str(e)}")


async def _run_analysis_task(job_id: str, file_id: str, parsed, user: dict):
    """백그라운드 분석 실행"""
    try:
        from services.sales_agent.orchestrator import run_analysis
        from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

        _agent_labels = {
            "_engine": "Python 엔진", "_phase": "시스템",
            "customer": "거래처 분석", "product": "품목 관리",
            "strategy": "판매전략", "future": "미래전략",
            "partnership": "파트너십", "visualization": "KPI/시각화",
        }

        async def progress_cb(agent_key, status, pct_or_msg):
            if job_id not in _running_jobs:
                return
            if agent_key not in ("_phase", "_engine"):
                _running_jobs[job_id]["agents"][agent_key] = status
            done = sum(1 for s in _running_jobs[job_id]["agents"].values() if s == "done")
            total = len(_running_jobs[job_id]["agents"])
            _running_jobs[job_id]["progress"] = int(done / total * 100)

            label = _agent_labels.get(agent_key, agent_key)
            if agent_key == "_phase":
                msg = str(pct_or_msg)
            elif status == "running":
                msg = f"{label} 에이전트가 분석을 시작합니다..."
            elif status == "done":
                msg = f"{label} 분석이 완료되었습니다."
            else:
                msg = f"{label}: {status}"

            import time as _t
            _running_jobs[job_id]["logs"].append({
                "ts": _t.time(), "agent": agent_key, "status": status, "message": msg,
            })
            await _broadcast_progress(job_id, _running_jobs[job_id])

        result = await run_analysis(
            sales_data=parsed,
            api_key=ANTHROPIC_API_KEY,
            model=CLAUDE_MODEL,
            progress_callback=progress_cb,
        )
        result["job_id"] = job_id

        conn = get_connection()
        try:
            conn.execute("""
                UPDATE sa_jobs SET status = 'completed', result_json = ?,
                    elapsed_seconds = ?, updated_at = ?
                WHERE job_id = ?
            """, (json.dumps(result, ensure_ascii=False, default=str),
                  result.get("elapsed_seconds", 0), now_kst(), job_id))
            conn.commit()
        finally:
            conn.close()

        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = "completed"
            _running_jobs[job_id]["progress"] = 100
            await _broadcast_progress(job_id, _running_jobs[job_id])

    except Exception as e:
        logger.error(f"[SalesAgent] 분석 실패: {job_id} - {e}", exc_info=True)
        conn = get_connection()
        try:
            conn.execute("UPDATE sa_jobs SET status='failed', result_json=?, updated_at=? WHERE job_id=?",
                         (json.dumps({"error": str(e)}), now_kst(), job_id))
            conn.commit()
        finally:
            conn.close()
        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = "failed"
            await _broadcast_progress(job_id, _running_jobs[job_id])


# ═══════════════════════════════════════════
#  상태 조회 / 결과 조회 / 이력
# ═══════════════════════════════════════════
@router.get("/status/{job_id}")
async def get_status(job_id: str, user=Depends(get_current_user)):
    if job_id in _running_jobs:
        return _running_jobs[job_id]
    conn = get_connection()
    try:
        row = conn.execute("SELECT status FROM sa_jobs WHERE job_id=?", (job_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404)
    return {"status": row[0], "progress": 100 if row[0] == "completed" else 0}


@router.get("/result/{job_id}")
async def get_result(job_id: str, user=Depends(get_current_user)):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT result_json, elapsed_seconds, analysis_mode FROM sa_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise HTTPException(404, "결과를 찾을 수 없습니다")
    result = json.loads(row[0])
    result["elapsed_seconds"] = row[1] or 0
    result["analysis_mode"] = row[2] or "multi"
    return result


@router.get("/history")
async def get_history(size: int = 20, user=Depends(get_current_user)):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT j.job_id, j.file_id, j.status, j.analysis_mode, j.target_customer,
                   j.elapsed_seconds, j.created_at, u.file_name
            FROM sa_jobs j LEFT JOIN sa_uploads u ON j.file_id = u.file_id
            ORDER BY j.created_at DESC LIMIT ?
        """, (size,)).fetchall()
    finally:
        conn.close()
    return {"history": [
        {"job_id": r[0], "file_id": r[1], "status": r[2], "mode": r[3],
         "target_customer": r[4], "elapsed": r[5], "created_at": r[6], "file_name": r[7]}
        for r in rows
    ]}


# ═══════════════════════════════════════════
#  WebSocket 실시간 진행
# ═══════════════════════════════════════════
@router.websocket("/ws/{job_id}")
async def ws_progress(ws: WebSocket, job_id: str):
    await ws.accept()
    _ws_connections.setdefault(job_id, set()).add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        _ws_connections.get(job_id, set()).discard(ws)


async def _broadcast_progress(job_id: str, data: dict):
    conns = _ws_connections.get(job_id, set()).copy()
    msg = json.dumps(data, default=str)
    for ws in conns:
        try:
            await ws.send_text(msg)
        except Exception:
            _ws_connections.get(job_id, set()).discard(ws)
