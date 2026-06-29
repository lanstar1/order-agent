"""
MAP Monitor Scheduler
정기 수집 + 상시감시 스케줄 관리
기존 scheduler_service.py의 APScheduler 패턴과 동일하게 구현
"""
import json
import logging
import asyncio
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger("map_scheduler")

# 스케줄러 상태 (기존 패턴과 동일)
_map_scheduler_state = {
    "scheduler": None,
    "jobs": [],
}


def setup_map_scheduler():
    """MAP 감시 스케줄러 초기화 - 기존 택배 스케줄러와 공존"""
    from db.database import get_connection

    # 기존 택배 스케줄러의 APScheduler 인스턴스를 공유
    from services.scheduler_service import _scheduler_state
    scheduler = _scheduler_state.get("scheduler")

    if not scheduler:
        logger.warning("기존 스케줄러 인스턴스 없음, MAP 스케줄러 건너뜀")
        return

    _map_scheduler_state["scheduler"] = scheduler

    # 설정 로드
    conn = get_connection()
    row = conn.execute("SELECT schedules, watch_interval_hours FROM map_settings WHERE id = 1").fetchone()
    conn.close()

    if not row:
        schedules = ["00:00", "12:00"]
        watch_interval = 2
    else:
        s_str = row["schedules"] if hasattr(row, '__getitem__') else row[0]
        schedules = json.loads(s_str) if isinstance(s_str, str) else (s_str or ["00:00", "12:00"])
        watch_interval = (row["watch_interval_hours"] if hasattr(row, '__getitem__') else row[1]) or 2

    # 기존 MAP 잡 제거
    for job in scheduler.get_jobs():
        if job.id.startswith("map_"):
            job.remove()

    # 1) 정기 수집 스케줄 등록
    for t in schedules:
        try:
            h, m = t.split(":")
            job_id = f"map_sched_{h}_{m}"
            scheduler.add_job(
                _run_scheduled,
                CronTrigger(hour=int(h), minute=int(m), timezone="Asia/Seoul"),
                id=job_id, replace_existing=True,
                name=f"MAP 정기수집 {t}",
            )
            logger.info(f"MAP 정기 수집 등록: {t} KST")
        except Exception as e:
            logger.error(f"MAP 스케줄 등록 실패 [{t}]: {e}")

    # 2) 상시감시 스케줄 등록
    scheduler.add_job(
        _run_watch,
        IntervalTrigger(hours=watch_interval),
        id="map_watch", replace_existing=True,
        name=f"MAP 상시감시 ({watch_interval}h 간격)",
    )
    logger.info(f"MAP 상시감시 등록: {watch_interval}시간 간격")


def _run_scheduled():
    """정기 수집 (동기 래퍼)"""
    logger.info("=== MAP 정기 수집 시작 ===")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        from services.map_collector_service import run_price_collection
        result = loop.run_until_complete(run_price_collection(collection_type="scheduled"))
        logger.info(f"MAP 정기 수집 완료: {result.get('message', '')}")
    except Exception as e:
        logger.error(f"MAP 정기 수집 오류: {e}")
    finally:
        loop.close()


def _run_watch():
    """상시감시 수집 (is_watched=1 제품만)"""
    logger.info("=== MAP 상시감시 시작 ===")
    try:
        from db.database import get_connection
        conn = get_connection()
        rows = conn.execute("SELECT id FROM map_products WHERE is_watched=1 AND is_active=1").fetchall()
        watched_ids = [r["id"] if hasattr(r, '__getitem__') else r[0] for r in rows]
        conn.close()

        if not watched_ids:
            logger.info("상시감시 대상 없음, 건너뜀")
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        from services.map_collector_service import run_price_collection
        result = loop.run_until_complete(
            run_price_collection(product_ids=watched_ids, collection_type="watch"))
        logger.info(f"MAP 상시감시 완료: {result.get('message', '')}")
    except Exception as e:
        logger.error(f"MAP 상시감시 오류: {e}")
    finally:
        loop.close()


def reload_map_schedule():
    """설정 변경 시 스케줄 재로드"""
    logger.info("MAP 스케줄 재로드...")
    setup_map_scheduler()


def get_scheduler_status():
    """스케줄러 상태"""
    scheduler = _map_scheduler_state.get("scheduler")
    if not scheduler:
        return {"running": False, "jobs": []}
    jobs = []
    for job in scheduler.get_jobs():
        if job.id.startswith("map_"):
            jobs.append({
                "id": job.id, "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None,
                "trigger": str(job.trigger),
            })
    return {"running": scheduler.running, "jobs": jobs}
