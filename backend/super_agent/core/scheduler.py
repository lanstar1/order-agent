"""
Super Agent 스케줄러 — 주기적 자동 분석 실행
예: 매주 월요일 9시 매출 분석 보고서 자동 생성
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# 인메모리 예약 작업 (Phase 4에서 DB)
_scheduled_tasks: List[Dict[str, Any]] = []


def register_scheduled_analysis(
    name: str,
    template_id: str,
    schedule: str = "weekly",  # daily, weekly, monthly
    enabled: bool = True,
) -> Dict[str, Any]:
    """주기적 분석 등록"""
    task = {
        "id": f"sched_{len(_scheduled_tasks) + 1}",
        "name": name,
        "template_id": template_id,
        "schedule": schedule,
        "enabled": enabled,
        "created_at": datetime.now().isoformat(),
        "last_run": None,
        "next_run": None,
    }
    _scheduled_tasks.append(task)
    logger.info(f"[SA-Scheduler] 등록: {name} ({schedule})")
    return task


def list_scheduled_tasks() -> List[Dict[str, Any]]:
    """등록된 예약 작업 목록"""
    return _scheduled_tasks


def remove_scheduled_task(task_id: str) -> bool:
    """예약 작업 삭제"""
    global _scheduled_tasks
    before = len(_scheduled_tasks)
    _scheduled_tasks = [t for t in _scheduled_tasks if t["id"] != task_id]
    return len(_scheduled_tasks) < before


async def run_scheduled_task(task_id: str) -> Dict[str, Any]:
    """예약 작업 즉시 실행"""
    task = next((t for t in _scheduled_tasks if t["id"] == task_id), None)
    if not task:
        return {"error": "예약 작업을 찾을 수 없습니다"}

    try:
        from super_agent.agents.templates import get_template_by_id
        from super_agent.core.orchestrator import orchestrator
        import uuid

        template = get_template_by_id(task["template_id"])
        if not template:
            return {"error": f"템플릿 '{task['template_id']}'을 찾을 수 없습니다"}

        job_id = f"sched_{uuid.uuid4().hex[:8]}"
        result = await orchestrator.run_job(
            job_id=job_id,
            user_prompt=template["prompt"],
            deliverable_type=template["deliverable_type"],
        )

        task["last_run"] = datetime.now().isoformat()
        return {"success": True, "job_id": job_id, "status": result.get("status")}

    except Exception as e:
        logger.error(f"[SA-Scheduler] 실행 실패: {e}")
        return {"error": str(e)}
