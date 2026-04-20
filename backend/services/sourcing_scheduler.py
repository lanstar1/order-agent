"""Queue-driven sourcing scheduler.

Intended to be registered on the existing order-agent APScheduler instance:

    from services.sourcing_scheduler import register as register_sourcing
    scheduler = BackgroundScheduler()
    register_sourcing(scheduler, get_conn, interval_hours=3)
    scheduler.start()

Core contract: every tick picks up ONE pending/stale video and runs the next
step of its pipeline. This keeps the per-tick work bounded regardless of
backlog size.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

try:
    from apscheduler.triggers.interval import IntervalTrigger  # type: ignore
except ImportError:  # pragma: no cover
    IntervalTrigger = None  # library absent during unit tests


GetConnFn = Callable[[], object]


MAX_RETRY = 3


def pick_next_video(conn) -> Optional[dict]:
    """Find the next work item. Priority:
    1. processed_status='pending' (never processed).
    2. processed_status='failed' AND retry_count < MAX_RETRY
       AND next_retry_at <= now.
    Returns dict(id, internal_step, retry_count) or None.
    """
    now = datetime.now(timezone.utc).isoformat()
    row = conn.execute(
        """SELECT id, internal_step, retry_count FROM youtube_videos
           WHERE processed_status='pending'
           ORDER BY created_at ASC LIMIT 1""",
    ).fetchone()
    if row:
        return {"id": row[0], "internal_step": row[1], "retry_count": row[2]}
    row = conn.execute(
        """SELECT id, internal_step, retry_count FROM youtube_videos
           WHERE processed_status='failed'
             AND retry_count < ?
             AND (next_retry_at IS NULL OR next_retry_at <= ?)
           ORDER BY next_retry_at ASC LIMIT 1""",
        (MAX_RETRY, now),
    ).fetchone()
    if row:
        return {"id": row[0], "internal_step": row[1], "retry_count": row[2]}
    return None


def schedule_retry(conn, video_id: int, *, backoff_hours: int = 3) -> None:
    now = datetime.now(timezone.utc)
    next_at = (now + timedelta(hours=backoff_hours)).isoformat()
    conn.execute(
        """UPDATE youtube_videos
           SET processed_status='failed', retry_count=retry_count+1,
               next_retry_at=?
           WHERE id=?""",
        (next_at, video_id),
    )
    conn.commit()


def mark_done(conn, video_id: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE youtube_videos
           SET processed_status='done', processed_at=?,
               internal_step='done'
           WHERE id=?""",
        (now, video_id),
    )
    conn.commit()


def set_step(conn, video_id: int, step: str) -> None:
    conn.execute(
        "UPDATE youtube_videos SET internal_step=?, processed_status='in_progress' WHERE id=?",
        (step, video_id),
    )
    conn.commit()


# --------------------------------------------------------------------------- #
# APScheduler registration
# --------------------------------------------------------------------------- #


def register(scheduler, get_conn: GetConnFn, *, interval_hours: int = 3,
             tick_fn: Optional[Callable[[], None]] = None) -> None:
    """Register the sourcing tick on the given APScheduler instance."""
    if IntervalTrigger is None:
        raise RuntimeError("apscheduler not installed")
    trigger = IntervalTrigger(hours=interval_hours, timezone="Asia/Seoul")
    scheduler.add_job(
        tick_fn or (lambda: _default_tick(get_conn)),
        trigger=trigger, id="sourcing_tick", replace_existing=True,
    )


def _default_tick(get_conn: GetConnFn) -> None:
    """Single-step worker. The actual work (transcript, correction, extract,
    analyze) is plugged by the route layer — for the library this is a stub."""
    conn = get_conn()
    task = pick_next_video(conn)
    if not task:
        return
    # The real implementation dispatches by internal_step; tests cover this
    # via dependency injection.
