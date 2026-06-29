"""
SmartLogen 자동 동기화 스케줄러
- 1시간마다 SmartLogen에서 발송 내역 자동 가져오기 (최근 3일, 전체 창고)
- 서버 시작 시 마지막 동기화가 2시간 이상 지났으면 즉시 실행
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

# 메인 이벤트 루프 참조 (startup에서 설정)
_main_loop = None

# 스케줄러 상태
_scheduler_state = {
    "enabled": True,
    "last_run": None,
    "last_result": None,
    "next_run": None,
    "scheduler": None,
}


def get_scheduler_status() -> dict:
    """스케줄러 상태 반환"""
    state = dict(_scheduler_state)
    state.pop("scheduler", None)
    return state


async def run_auto_fetch(days: int = 3) -> dict:
    """SmartLogen 자동 가져오기 실행"""
    from services.smart_logen_client import fetch_shipments, save_fetched_to_db
    from db.database import get_connection

    now_kst = datetime.now(KST)
    logger.info(f"[스케줄러] SmartLogen 자동 가져오기 시작 (KST: {now_kst.strftime('%Y-%m-%d %H:%M')})")

    result = {"success": False, "fetched": 0, "saved": 0, "time": now_kst.isoformat()}

    try:
        # 전체 창고 조회 (최근 days일)
        records = await fetch_shipments(warehouse="", from_date="", to_date="", days=days)

        if records:
            conn = get_connection()
            try:
                saved = save_fetched_to_db(records, conn)
            finally:
                conn.close()
            result.update({"success": True, "fetched": len(records), "saved": saved})
            logger.info(f"[스케줄러] 완료: {len(records)}건 조회, {saved}건 저장")
        else:
            result.update({"success": True, "fetched": 0, "saved": 0})
            logger.info("[스케줄러] 조회된 데이터 없음")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"[스케줄러] SmartLogen 자동 가져오기 오류: {e}", exc_info=True)

    _scheduler_state["last_run"] = now_kst.isoformat()
    _scheduler_state["last_result"] = result
    _scheduler_state["next_run"] = _get_next_run_time()
    return result


def _sync_job():
    """APScheduler에서 호출되는 동기 함수 (메인 이벤트 루프에 코루틴 예약)"""
    global _main_loop
    try:
        if _main_loop and _main_loop.is_running():
            # 메인 asyncio 루프에 안전하게 코루틴 예약 (thread-safe)
            future = asyncio.run_coroutine_threadsafe(run_auto_fetch(days=3), _main_loop)
            try:
                future.result(timeout=120)  # 2분 타임아웃
            except Exception as e:
                logger.error(f"[스케줄러] 자동 동기화 실행 오류: {e}")
        else:
            # 루프 없으면 새로 생성
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(run_auto_fetch(days=3))
            finally:
                loop.close()
    except Exception as e:
        logger.error(f"[스케줄러] _sync_job 오류: {e}", exc_info=True)


def start_scheduler():
    """APScheduler 시작 - 1시간마다 실행"""
    global _main_loop
    try:
        # 메인 이벤트 루프 캡처
        try:
            _main_loop = asyncio.get_running_loop()
        except RuntimeError:
            _main_loop = asyncio.get_event_loop()

        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger

        scheduler = BackgroundScheduler()

        # 1시간마다 실행 (최근 3일, 전체 창고)
        trigger = IntervalTrigger(hours=1)
        scheduler.add_job(_sync_job, trigger, id="smartlogen_hourly_sync", replace_existing=True)

        scheduler.start()
        _scheduler_state["scheduler"] = scheduler
        _scheduler_state["next_run"] = _get_next_run_time()

        logger.info("[스케줄러] APScheduler 시작 - 1시간 간격 SmartLogen 자동 동기화 (최근 3일, 전체 창고)")
        return True

    except Exception as e:
        logger.error(f"[스케줄러] APScheduler 시작 실패: {e}", exc_info=True)
        return False


def stop_scheduler():
    """스케줄러 중지"""
    scheduler = _scheduler_state.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)
        _scheduler_state["scheduler"] = None
        logger.info("[스케줄러] APScheduler 중지됨")


def _get_next_run_time() -> str:
    """다음 실행 시각 계산 (KST 기준) - 1시간 후"""
    now_kst = datetime.now(KST)
    next_run = now_kst + timedelta(hours=1)
    return next_run.replace(second=0, microsecond=0).isoformat()


async def check_and_run_on_startup():
    """
    서버 시작 시 마지막 동기화가 2시간 이상 지났으면 즉시 실행.
    Render 무료 플랜에서 서버가 꺼졌다 켜질 때를 대비.
    """
    await asyncio.sleep(8)  # DB 초기화 완료 대기

    try:
        from db.database import get_connection
        conn = get_connection()

        # shipments 테이블의 최근 created_at 확인 (updated_at 컬럼 없음)
        row = conn.execute(
            "SELECT MAX(created_at) as last_update FROM shipments"
        ).fetchone()
        conn.close()

        last_update = row[0] if row else None
        need_sync = True

        if last_update:
            try:
                last_dt = datetime.strptime(str(last_update)[:19], "%Y-%m-%d %H:%M:%S")
                hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                logger.info(f"[스케줄러] 택배 마지막 등록: {last_update} ({hours_ago:.1f}시간 전)")
                if hours_ago < 2:
                    need_sync = False
                    logger.info("[스케줄러] 최근 동기화됨 → 시작 시 자동 가져오기 스킵")
            except (ValueError, TypeError) as e:
                logger.warning(f"[스케줄러] 날짜 파싱 실패: {e}")

        if need_sync:
            logger.info("[스케줄러] 2시간 이상 동기화 안됨 → 자동 가져오기 실행")
            await run_auto_fetch(days=3)

    except Exception as e:
        logger.error(f"[스케줄러] 시작 시 자동 동기화 체크 오류: {e}", exc_info=True)
