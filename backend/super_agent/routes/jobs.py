"""
Super Agent API 라우터 — Job 관리 + WebSocket
DB 기반 저장 + 사용자별 격리
"""
import uuid
import asyncio
import logging
import os
import shutil
from datetime import datetime
from typing import Optional
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse, FileResponse

from super_agent.core.orchestrator import orchestrator
from super_agent.core.websocket_manager import ws_manager
from super_agent.models.schemas import JobResponse, JobListResponse
from super_agent.db.sa_tables import save_job, update_job, get_job, list_jobs_by_user, delete_job_db
from db.database import get_connection
from security import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/super-agent", tags=["Super Agent"])

# 업로드 디렉토리
UPLOAD_DIR = Path(__file__).parent.parent.parent.parent / "data" / "sa_uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# 실행 중인 Job 상태 (WebSocket용 인메모리 캐시 — DB 보조용)
_running_jobs: dict = {}


# ─── Job 생성 (프롬프트 + 선택적 파일 업로드) ───
@router.post("/jobs", response_model=JobResponse)
async def create_job(
    prompt: str = Form(..., description="사용자 자연어 요청"),
    deliverable_type: Optional[str] = Form("report", description="결과물 유형"),
    file: Optional[UploadFile] = File(None, description="분석할 파일"),
    user: dict = Depends(get_current_user),
):
    """새 Super Agent Job 생성 및 실행"""
    job_id = str(uuid.uuid4())[:12]
    user_id = user["emp_cd"]

    # 파일 저장
    file_path = None
    if file and file.filename:
        safe_name = f"{job_id}_{file.filename}"
        file_path = str(UPLOAD_DIR / safe_name)
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        logger.info(f"[Job] 파일 업로드: {safe_name} ({len(content)} bytes)")

    # DB에 Job 저장
    job_data = {
        "job_id": job_id,
        "status": "queued",
        "prompt": prompt,
        "deliverable_type": deliverable_type,
        "file_path": file_path or "",
        "created_by": user_id,
        "created_at": datetime.now().isoformat(),
        "result": None,
        "title": prompt[:50],
    }

    conn = get_connection()
    try:
        save_job(conn, job_data)
    finally:
        conn.close()

    # 인메모리 캐시 (WebSocket + 비동기 실행용)
    _running_jobs[job_id] = job_data

    # 비동기 실행 시작
    asyncio.create_task(_run_job_async(job_id, prompt, file_path, deliverable_type))

    return JobResponse(
        job_id=job_id,
        status="queued",
        deliverable_type=deliverable_type,
        title=prompt[:50],
        created_at=job_data["created_at"],
    )


async def _run_job_async(
    job_id: str,
    prompt: str,
    file_path: Optional[str],
    deliverable_type: str,
):
    """백그라운드 Job 실행"""
    try:
        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = "running"

        # DB 상태 업데이트
        conn = get_connection()
        try:
            update_job(conn, job_id, {"status": "running"})
        finally:
            conn.close()

        result = await orchestrator.run_job(
            job_id=job_id,
            user_prompt=prompt,
            file_path=file_path,
            deliverable_type=deliverable_type,
        )

        status = result.get("status", "completed")
        classification = result.get("classification", {})
        artifact = result.get("artifact", {})
        cost = result.get("cost_summary", {})

        # DB 업데이트
        conn = get_connection()
        try:
            update_job(conn, job_id, {
                "status": status,
                "result": result,
                "title": classification.get("title", prompt[:50]),
                "job_type": classification.get("job_type", "freeform"),
                "artifact_path": artifact.get("file_path", ""),
                "artifact_name": artifact.get("file_name", ""),
                "total_cost": cost.get("total_cost", 0),
                "total_tokens": cost.get("total_tokens", 0),
                "elapsed_ms": cost.get("elapsed_ms", 0),
                "result_summary": result.get("synthesis", {}).get("summary", ""),
            })
        finally:
            conn.close()

        # 인메모리 캐시 업데이트
        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = status
            _running_jobs[job_id]["result"] = result

    except Exception as e:
        logger.error(f"[Job] {job_id} 실행 실패: {e}", exc_info=True)

        conn = get_connection()
        try:
            update_job(conn, job_id, {
                "status": "failed",
                "error_message": str(e),
                "result": {"error": str(e)},
            })
        finally:
            conn.close()

        if job_id in _running_jobs:
            _running_jobs[job_id]["status"] = "failed"
            _running_jobs[job_id]["result"] = {"error": str(e)}

        await ws_manager.send_error(job_id, str(e))


def _get_job_data(job_id: str, user_id: str) -> dict:
    """Job 데이터 조회 (인메모리 캐시 우선, DB fallback)"""
    # 실행 중인 Job은 인메모리에서 최신 상태 확인
    if job_id in _running_jobs:
        cached = _running_jobs[job_id]
        if cached.get("created_by") == user_id:
            return cached

    # DB에서 조회
    conn = get_connection()
    try:
        job = get_job(conn, job_id, user_id)
    finally:
        conn.close()

    return job


# ─── Job 상태 조회 ───
@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """Job 상태 및 결과 조회"""
    job = _get_job_data(job_id, user["emp_cd"])
    if not job:
        raise HTTPException(404, "Job을 찾을 수 없습니다")

    result = job.get("result") or {}
    classification = result.get("classification", {})
    progress = result.get("cost_summary") if result.get("cost_summary") else None

    return JobResponse(
        job_id=job_id,
        status=job.get("status", "unknown"),
        job_type=classification.get("job_type") or job.get("job_type"),
        deliverable_type=job.get("deliverable_type"),
        title=classification.get("title") or job.get("title", ""),
        progress=progress,
        current_summary=result.get("synthesis", {}).get("summary"),
        created_at=job.get("created_at"),
    )


# ─── Job 결과 상세 ───
@router.get("/jobs/{job_id}/result")
async def get_job_result(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """Job 실행 결과 상세"""
    job = _get_job_data(job_id, user["emp_cd"])
    if not job:
        raise HTTPException(404, "Job을 찾을 수 없습니다")

    result = job.get("result")
    if not result:
        return {"job_id": job_id, "status": job.get("status"), "message": "아직 결과가 없습니다"}

    return {
        "job_id": job_id,
        "status": job.get("status"),
        "classification": result.get("classification"),
        "plan_summary": result.get("plan_summary"),
        "execution_result": result.get("execution_result"),
        "synthesis": result.get("synthesis"),
        "artifact": result.get("artifact"),
        "cost_summary": result.get("cost_summary"),
    }


# ─── Job 목록 ───
@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_current_user),
):
    """사용자별 Job 목록 조회"""
    conn = get_connection()
    try:
        data = list_jobs_by_user(conn, user["emp_cd"], limit, offset)
    finally:
        conn.close()

    items = []
    for j in data["items"]:
        result = j.get("result") or {}
        cls_ = result.get("classification", {})
        items.append(
            JobResponse(
                job_id=j.get("job_id", ""),
                status=j.get("status", "unknown"),
                job_type=cls_.get("job_type") or j.get("job_type"),
                deliverable_type=j.get("deliverable_type"),
                title=cls_.get("title") or j.get("title", ""),
                created_at=j.get("created_at"),
            )
        )
    return JobListResponse(items=items, total=data["total"])


# ─── 아티팩트 다운로드 ───
@router.get("/jobs/{job_id}/download")
async def download_artifact(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """생성된 문서 파일 다운로드 (소유자만)"""
    job = _get_job_data(job_id, user["emp_cd"])
    if not job:
        raise HTTPException(404, "Job을 찾을 수 없습니다")

    result = job.get("result")
    if not result or not result.get("artifact"):
        raise HTTPException(404, "아티팩트가 아직 생성되지 않았습니다")

    artifact = result["artifact"]
    file_path = artifact.get("file_path") or job.get("artifact_path")
    if not file_path or not Path(file_path).exists():
        raise HTTPException(404, "파일을 찾을 수 없습니다 (재배포 후 파일이 초기화되었을 수 있습니다)")

    file_name = artifact.get("file_name") or job.get("artifact_name", "report.md")
    media_types = {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pdf": "application/pdf",
        "markdown": "text/markdown",
        "md": "text/markdown",
    }
    fmt = artifact.get("format", "markdown")
    media_type = media_types.get(fmt, "application/octet-stream")

    return FileResponse(
        path=file_path,
        filename=file_name,
        media_type=media_type,
    )


# ─── 합성 텍스트 (미리보기) ───
@router.get("/jobs/{job_id}/preview")
async def preview_result(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """실행 결과 텍스트 미리보기"""
    job = _get_job_data(job_id, user["emp_cd"])
    if not job:
        raise HTTPException(404, "Job을 찾을 수 없습니다")

    result = job.get("result")
    if not result:
        return {"job_id": job_id, "status": job.get("status"), "preview": ""}

    synthesis = result.get("synthesis", {})
    if synthesis.get("parsed_report"):
        return {
            "job_id": job_id,
            "status": job.get("status"),
            "preview_type": "structured",
            "data": synthesis["parsed_report"],
        }

    return {
        "job_id": job_id,
        "status": job.get("status"),
        "preview_type": "text",
        "data": {
            "summary": synthesis.get("summary", ""),
            "content": synthesis.get("text_content", "")[:5000],
        },
    }


# ─── WebSocket (실시간 진행상황) ───
@router.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """Job 진행상황 실시간 스트리밍"""
    await ws_manager.connect(websocket, job_id)
    try:
        # 현재 상태 전송 (인메모리 캐시에서)
        job = _running_jobs.get(job_id)
        if job:
            await ws_manager.send_progress(
                job_id, job["status"],
                f"연결됨 - 현재 상태: {job['status']}",
                0 if job["status"] == "queued" else 100 if job["status"] == "completed" else 50,
            )

        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket, job_id)
    except Exception as e:
        logger.debug(f"[WS] 연결 종료: {job_id} - {e}")
        await ws_manager.disconnect(websocket, job_id)


# ─── 템플릿 목록 ───
@router.get("/templates")
async def list_templates(category: Optional[str] = Query(None)):
    """사용 가능한 분석 템플릿 목록"""
    from super_agent.agents.templates import get_templates, get_categories
    templates = get_templates(category)
    return {
        "templates": templates,
        "categories": get_categories(),
    }


# ─── 템플릿으로 Job 생성 ───
@router.post("/templates/{template_id}/run")
async def run_template(
    template_id: str,
    file: Optional[UploadFile] = File(None),
    user: dict = Depends(get_current_user),
):
    """템플릿 기반 Job 실행"""
    from super_agent.agents.templates import get_template_by_id

    template = get_template_by_id(template_id)
    if not template:
        raise HTTPException(404, f"템플릿 '{template_id}'을 찾을 수 없습니다")

    if template.get("requires_file") and not file:
        raise HTTPException(400, "이 템플릿은 파일 업로드가 필요합니다")

    job_id = str(uuid.uuid4())[:12]
    user_id = user["emp_cd"]

    file_path = None
    if file and file.filename:
        safe_name = f"{job_id}_{file.filename}"
        file_path = str(UPLOAD_DIR / safe_name)
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)

    job_data = {
        "job_id": job_id,
        "status": "queued",
        "prompt": template["prompt"],
        "deliverable_type": template["deliverable_type"],
        "file_path": file_path or "",
        "created_by": user_id,
        "created_at": datetime.now().isoformat(),
        "result": None,
        "title": template["title"],
    }

    conn = get_connection()
    try:
        save_job(conn, job_data)
    finally:
        conn.close()

    _running_jobs[job_id] = job_data

    asyncio.create_task(
        _run_job_async(job_id, template["prompt"], file_path, template["deliverable_type"])
    )

    return JobResponse(
        job_id=job_id,
        status="queued",
        deliverable_type=template["deliverable_type"],
        title=template["title"],
        created_at=job_data["created_at"],
    )


# ─── 퀵 분석 (파일 업로드 → 즉시 분석, Job 생성 없이) ───
@router.post("/quick-analyze")
async def quick_analyze(
    prompt: str = Form("이 데이터를 분석해주세요"),
    file: UploadFile = File(..., description="분석할 파일"),
    user: dict = Depends(get_current_user),
):
    """파일 업로드 즉시 분석 (간단 모드)"""
    from super_agent.tools.file_parser import parse_file as _parse
    from super_agent.core.intent_classifier import classify_intent as _classify

    temp_name = f"quick_{uuid.uuid4().hex[:8]}_{file.filename}"
    temp_path = str(UPLOAD_DIR / temp_name)
    with open(temp_path, "wb") as f:
        f.write(await file.read())

    file_data = _parse(temp_path)

    file_info = {
        "file_name": file.filename,
        "type": file_data.get("type"),
        "row_count": file_data.get("row_count", 0),
        "columns": file_data.get("columns", []),
    }
    classification = await _classify(prompt, has_files=True, file_info=file_info)

    return {
        "file_info": file_info,
        "classification": classification,
        "data_preview": file_data.get("data_preview", [])[:5],
        "column_stats": file_data.get("column_stats", {}),
    }


# ─── 비용/통계 API ───
@router.get("/stats")
async def get_stats(user: dict = Depends(get_current_user)):
    """사용자별 사용량 통계"""
    from super_agent.tools.cost_tracker import get_cost_summary, check_budget

    summary = get_cost_summary(days=30)
    budget = check_budget()

    # DB에서 사용자별 통계
    conn = get_connection()
    try:
        data = list_jobs_by_user(conn, user["emp_cd"], limit=1000, offset=0)
    finally:
        conn.close()

    status_counts = {}
    for j in data["items"]:
        s = j.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    return {
        "cost_summary": summary,
        "budget": budget,
        "job_stats": {
            "total": data["total"],
            "by_status": status_counts,
        },
    }


# ─── Job 삭제 ───
@router.delete("/jobs/{job_id}")
async def delete_job(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """Job 삭제 (소유자만)"""
    # 실행 중인 Job 확인
    if job_id in _running_jobs and _running_jobs[job_id].get("status") == "running":
        raise HTTPException(400, "실행 중인 Job은 삭제할 수 없습니다")

    conn = get_connection()
    try:
        deleted = delete_job_db(conn, job_id, user["emp_cd"])
    finally:
        conn.close()

    if not deleted:
        raise HTTPException(404, "Job을 찾을 수 없습니다")

    # 인메모리 캐시에서도 제거
    _running_jobs.pop(job_id, None)

    return {"message": "삭제 완료", "job_id": job_id}
