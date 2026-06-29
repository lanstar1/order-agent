"""
자료 자동 동기화 스케줄러 (매일 KST HH:MM)

대상:
- 단가표 소스 (Google Sheets) 동기화
- 자료검색 (Google Drive 폴더) 동기화
- 오더리스트 (Google Sheets) 동기화

설정은 app_settings 테이블에 저장:
- auto_sync_hour   (기본 8)
- auto_sync_minute (기본 0)
- auto_sync_enabled ('1' / '0', 기본 '1')

스케줄러 인스턴스는 services.scheduler_service._scheduler_state["scheduler"] 를 공유한다.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
JOB_ID = "data_sync_daily"

DEFAULT_HOUR = 8
DEFAULT_MINUTE = 0

# 실행 상태 (UI에서 last_run/last_result 조회용)
_state = {
    "last_run": None,     # ISO KST
    "last_result": None,  # dict
}


# ─────────────────────────────────────────
#  설정 조회/저장
# ─────────────────────────────────────────
def _get_setting(key: str, default: str) -> str:
    try:
        from db.database import get_connection
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT value FROM app_settings WHERE key=?", (key,)
            ).fetchone()
            return row["value"] if row else default
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"[data-sync] 설정 조회 실패({key}): {e}")
        return default


def _set_setting(key: str, value: str):
    from db.database import get_connection
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES(?, ?, datetime('now','localtime'))
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def get_schedule_config() -> dict:
    """현재 스케줄 설정 조회"""
    hour = int(_get_setting("auto_sync_hour", str(DEFAULT_HOUR)))
    minute = int(_get_setting("auto_sync_minute", str(DEFAULT_MINUTE)))
    enabled = _get_setting("auto_sync_enabled", "1") == "1"
    return {"hour": hour, "minute": minute, "enabled": enabled}


def save_schedule_config(hour: int, minute: int, enabled: bool) -> None:
    _set_setting("auto_sync_hour", str(hour))
    _set_setting("auto_sync_minute", str(minute))
    _set_setting("auto_sync_enabled", "1" if enabled else "0")


# ─────────────────────────────────────────
#  실제 동기화 실행
# ─────────────────────────────────────────
async def run_data_sync() -> dict:
    """단가표/자료검색/오더리스트 일괄 동기화 실행"""
    started_at = datetime.now(KST)
    result: dict = {
        "started_at": started_at.isoformat(),
        "materials": None,
        "orderlist": None,
        "errors": [],
    }

    # ── 단가표 + 자료검색 (sheets + drive) ──
    try:
        from services.materials_service import sync_all as sync_all_sources
        mat = await sync_all_sources()
        result["materials"] = mat
        sheets = (mat or {}).get("sheets", {}) or {}
        drive = (mat or {}).get("drive", {}) or {}
        logger.info(
            f"[data-sync] 자료 동기화 완료: "
            f"시트 {sheets.get('success_count',0)}/{sheets.get('total_sources',0)}"
            f"({sheets.get('total_rows',0)}행), "
            f"Drive {drive.get('success_count',0)}/{drive.get('total_sources',0)}"
            f"({drive.get('total_files',0)}파일)"
        )
    except Exception as e:
        logger.error(f"[data-sync] 자료 동기화 오류: {e}", exc_info=True)
        result["errors"].append(f"materials: {e}")

    # ── 오더리스트 ──
    try:
        from services.orderlist_service import sync_orderlist
        # sync_orderlist 는 동기 함수 → 이벤트 루프 블로킹 방지용으로 스레드에서 실행
        ol = await asyncio.to_thread(sync_orderlist)
        result["orderlist"] = ol
        if ol and ol.get("success"):
            logger.info(
                f"[data-sync] 오더리스트 동기화 완료: {ol.get('total_items', 0)}건"
            )
        else:
            msg = (ol or {}).get("error", "알 수 없음")
            logger.warning(f"[data-sync] 오더리스트 실패: {msg}")
    except Exception as e:
        logger.error(f"[data-sync] 오더리스트 오류: {e}", exc_info=True)
        result["errors"].append(f"orderlist: {e}")

    finished_at = datetime.now(KST)
    result["finished_at"] = finished_at.isoformat()
    result["duration_sec"] = int((finished_at - started_at).total_seconds())

    _state["last_run"] = started_at.isoformat()
    _state["last_result"] = result
    return result


def _sync_job():
    """APScheduler 에서 호출되는 동기 래퍼"""
    try:
        from services.scheduler_service import _main_loop
        if _main_loop and _main_loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(run_data_sync(), _main_loop)
            try:
                fut.result(timeout=1800)  # 최대 30분
            except Exception as e:
                logger.error(f"[data-sync] 실행 오류: {e}", exc_info=True)
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(run_data_sync())
            finally:
                loop.close()
    except Exception as e:
        logger.error(f"[data-sync] _sync_job 오류: {e}", exc_info=True)


# ─────────────────────────────────────────
#  스케줄 관리
# ─────────────────────────────────────────
def _get_scheduler():
    from services.scheduler_service import _scheduler_state
    return _scheduler_state.get("scheduler")


def apply_schedule(hour: Optional[int] = None, minute: Optional[int] = None,
                   enabled: Optional[bool] = None) -> dict:
    """
    스케줄 설정을 적용 (인자 생략 시 DB에서 읽음).
    기존 job 이 있으면 제거 후 새 트리거로 등록.
    """
    cfg = get_schedule_config()
    if hour is not None:
        cfg["hour"] = hour
    if minute is not None:
        cfg["minute"] = minute
    if enabled is not None:
        cfg["enabled"] = enabled

    scheduler = _get_scheduler()
    if not scheduler:
        logger.warning("[data-sync] 공유 스케줄러 인스턴스 없음 → 스케줄 미등록")
        return {**cfg, "next_run": None, "registered": False}

    # 기존 잡 제거
    try:
        scheduler.remove_job(JOB_ID)
    except Exception:
        pass

    if not cfg["enabled"]:
        logger.info("[data-sync] 비활성화됨 → 스케줄 미등록")
        return {**cfg, "next_run": None, "registered": False}

    from apscheduler.triggers.cron import CronTrigger
    trigger = CronTrigger(
        hour=cfg["hour"], minute=cfg["minute"], timezone="Asia/Seoul"
    )
    scheduler.add_job(
        _sync_job,
        trigger,
        id=JOB_ID,
        name=f"자료/오더리스트 동기화 (매일 {cfg['hour']:02d}:{cfg['minute']:02d} KST)",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    job = scheduler.get_job(JOB_ID)
    next_run = job.next_run_time.astimezone(KST).isoformat() if job and job.next_run_time else None
    logger.info(
        f"[data-sync] 스케줄 등록: 매일 {cfg['hour']:02d}:{cfg['minute']:02d} KST (next={next_run})"
    )
    return {**cfg, "next_run": next_run, "registered": True}


def get_status() -> dict:
    """UI 용 상태 조회"""
    cfg = get_schedule_config()
    scheduler = _get_scheduler()
    next_run = None
    registered = False
    if scheduler:
        job = scheduler.get_job(JOB_ID)
        if job:
            registered = True
            if job.next_run_time:
                next_run = job.next_run_time.astimezone(KST).isoformat()
    return {
        **cfg,
        "next_run": next_run,
        "registered": registered,
        "last_run": _state["last_run"],
        "last_result": _state["last_result"],
    }


async def trigger_now() -> dict:
    """즉시 실행 (API 핸들러용)"""
    return await run_data_sync()


# ─────────────────────────────────────────
#  서버 시작 시 catch-up
# ─────────────────────────────────────────
async def check_and_run_on_startup(stale_hours: int = 12):
    """
    서버 시작 직후, 마지막 동기화가 stale_hours 시간 이상 지났으면 즉시 실행.
    (Render 무료 플랜이 밤새 잠들어 8시 스케줄이 건너뛰어졌을 때 대비)
    """
    await asyncio.sleep(8)  # DB/스케줄러 준비 대기

    cfg = get_schedule_config()
    if not cfg["enabled"]:
        logger.info("[data-sync] startup catch-up: 비활성화 상태 → 스킵")
        return

    try:
        from db.database import get_connection
        conn = get_connection()
        try:
            mat_row = conn.execute(
                "SELECT MIN(last_synced) as oldest FROM material_sources "
                "WHERE last_synced != '' AND is_active=1"
            ).fetchone()
            ol_row = conn.execute(
                "SELECT MAX(synced_at) as ts FROM orderlist_sync_log"
            ).fetchone()
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"[data-sync] startup 조회 실패: {e}")
        return

    def _hours_since(ts) -> Optional[float]:
        if not ts:
            return None
        try:
            dt = datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S")
            return (datetime.now() - dt).total_seconds() / 3600
        except Exception:
            return None

    mat_oldest = mat_row["oldest"] if mat_row else None
    ol_last = ol_row["ts"] if ol_row else None
    mat_hours = _hours_since(mat_oldest)
    ol_hours = _hours_since(ol_last)

    stale_mat = (mat_hours is None) or (mat_hours >= stale_hours)
    stale_ol = (ol_hours is None) or (ol_hours >= stale_hours)

    logger.info(
        f"[data-sync] startup 상태 점검: "
        f"materials oldest={mat_oldest} ({mat_hours}h ago), "
        f"orderlist last={ol_last} ({ol_hours}h ago), "
        f"stale_threshold={stale_hours}h"
    )

    if stale_mat or stale_ol:
        logger.info("[data-sync] startup catch-up: 동기화 실행")
        try:
            await run_data_sync()
        except Exception as e:
            logger.error(f"[data-sync] startup catch-up 오류: {e}", exc_info=True)
    else:
        logger.info("[data-sync] startup catch-up: 최근 동기화됨 → 스킵")
