"""Content Factory — 예약 발행 스케줄러 (매분 체크)"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from db.database import get_connection

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


async def check_and_publish_scheduled():
    conn = get_connection()
    try:
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute("""
            SELECT id, platform FROM content_items
            WHERE status = 'scheduled' AND scheduled_at IS NOT NULL AND scheduled_at <= ?
            ORDER BY scheduled_at ASC LIMIT 5
        """, (now_kst,)).fetchall()

        if not rows:
            return

        from services.content_service import publish_content
        for row in rows:
            row = dict(row)
            try:
                result = await publish_content(row["id"], row["platform"])
                logger.info(f"[예약발행] ID={row['id']} → {row['platform']} 완료")
            except Exception as e:
                logger.error(f"[예약발행] ID={row['id']} 실패: {e}")
                conn.execute("UPDATE content_items SET status='publish_error',updated_at=datetime('now','localtime') WHERE id=?", (row["id"],))
                conn.commit()
    finally:
        conn.close()


def run_publish_check():
    """APScheduler용 동기 래퍼"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(check_and_publish_scheduled())
        else:
            asyncio.run(check_and_publish_scheduled())
    except Exception as e:
        logger.error(f"[스케줄러] 예약 발행 체크 실패: {e}")
