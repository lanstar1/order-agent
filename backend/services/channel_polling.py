"""Channel polling — YouTube Data API로 실제 영상 수집.

3가지 진입점:
1. ``resolve_channel_metadata(conn, channel_row_id)`` — 핸들 → 실제 channelId,
   title, subscriber_count 등 메타데이터 백필.
2. ``poll_channel_now(conn, channel_row_id)`` — 최근 업로드 영상 10개 수집.
   스케줄러 틱마다 호출되는 경량 경로 (쿼터 ~3u per channel).
3. ``poll_channel_period(conn, channel_row_id, start, end)`` — 사용자가 지정한
   날짜 범위의 영상을 search.list로 수집 (쿼터 100u + N).

모두 동기 함수로 구현해서 FastAPI route에서 직접 호출 가능하게 함.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .youtube_client import (
    YouTubeClient, ChannelSnapshot, VideoMetrics,
    uploads_playlist_id,
)


logger = logging.getLogger(__name__)


# --------------------------------------------------------------- #
# DTOs
# --------------------------------------------------------------- #


@dataclass
class PollResult:
    channel_id: int                 # DB row id
    new_video_count: int
    updated_channel_title: Optional[str]
    api_calls: int                  # 대략적 쿼터 소진 건수
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "new_video_count": self.new_video_count,
            "updated_channel_title": self.updated_channel_title,
            "api_calls": self.api_calls,
            "error": self.error,
        }


# --------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------- #


class PollingError(RuntimeError):
    pass


def _get_client() -> YouTubeClient:
    key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if not key:
        raise PollingError("YOUTUBE_API_KEY 환경변수가 설정되지 않았습니다.")
    return YouTubeClient(key)


def _load_channel_row(conn, channel_row_id: int) -> dict:
    row = conn.execute(
        """SELECT id, channel_id, channel_handle, channel_title, category,
                  last_polled_at
           FROM youtube_channels WHERE id=?""",
        (channel_row_id,),
    ).fetchone()
    if not row:
        raise PollingError(f"채널을 찾을 수 없습니다 (id={channel_row_id})")
    return {
        "id": row[0], "channel_id": row[1], "channel_handle": row[2],
        "channel_title": row[3], "category": row[4], "last_polled_at": row[5],
    }


def _ensure_real_channel_id(conn, channel_row: dict, yt: YouTubeClient) -> tuple[str, Optional[str]]:
    """Ensure the row has a real UCxxx channel_id + return (channel_id, updated_title).

    - Already UCxxxx → return as-is, no API call.
    - Starts with ``__provisional__`` or is an ``@handle`` → call
      ``channels.list?forHandle`` to resolve.
    """
    cid = channel_row["channel_id"] or ""
    handle = channel_row["channel_handle"] or ""

    if cid and cid.startswith("UC") and len(cid) == 24:
        return cid, None

    # Resolve via forHandle
    hdl = handle or (cid if cid.startswith("@") else "")
    if not hdl and cid.startswith("__provisional__"):
        # Fall back to the stored provisional value
        hdl = cid.removeprefix("__provisional__")

    if not hdl:
        raise PollingError(
            f"channel_id도 핸들도 확보할 수 없음: row={channel_row['id']}"
        )

    snap = yt.resolve_by_handle(hdl)
    if not snap or not snap.channel_id:
        raise PollingError(f"YouTube에서 핸들 {hdl!r} 을 찾을 수 없습니다.")

    # Persist the real channelId + title + subscriber_count
    conn.execute(
        """UPDATE youtube_channels
           SET channel_id=?, channel_title=?, subscriber_count=?
           WHERE id=?""",
        (snap.channel_id, snap.title, snap.subscriber_count, channel_row["id"]),
    )
    conn.commit()
    return snap.channel_id, snap.title


def _insert_video_if_new(conn, *, channel_row_id: int, video_id: str,
                         title: str, published_at: str,
                         thumbnail_url: str = "",
                         video_type: str = "normal") -> bool:
    """INSERT OR IGNORE semantics — returns True if newly inserted."""
    # Check if exists
    existing = conn.execute(
        "SELECT id FROM youtube_videos WHERE video_id=?", (video_id,),
    ).fetchone()
    if existing:
        return False
    conn.execute(
        """INSERT INTO youtube_videos
           (channel_id, video_id, title, published_at,
            video_type, thumbnail_url, processed_status)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')""",
        (channel_row_id, video_id, title, published_at,
         video_type, thumbnail_url),
    )
    return True


def _classify_video_type(title: str, duration_sec: Optional[int]) -> str:
    """Heuristic: Shorts if <60s, live if title has 'LIVE', else normal."""
    if duration_sec is not None and 0 < duration_sec < 60:
        return "short"
    t_upper = (title or "").upper()
    if "LIVE" in t_upper or "실시간" in title:
        return "live"
    return "normal"


# --------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------- #


def resolve_channel_metadata(conn, channel_row_id: int) -> PollResult:
    """API 호출 없이 가능한 경우 skip. 핸들만 있으면 forHandle로 resolve."""
    row = _load_channel_row(conn, channel_row_id)
    yt = _get_client()
    _, updated_title = _ensure_real_channel_id(conn, row, yt)
    return PollResult(
        channel_id=channel_row_id, new_video_count=0,
        updated_channel_title=updated_title, api_calls=1,
    )


def poll_channel_now(conn, channel_row_id: int, *, max_videos: int = 10) -> PollResult:
    """Fetch latest ``max_videos`` uploads and INSERT new ones.

    Cost: ~3 quota units per channel (channels.list + playlistItems + videos).
    """
    row = _load_channel_row(conn, channel_row_id)
    yt = _get_client()

    # 1. Resolve to real channelId (1 API call if needed)
    cid, updated_title = _ensure_real_channel_id(conn, row, yt)
    api_calls = 1 if updated_title else 0

    # 2. Get recent uploads via playlistItems (1u)
    playlist = uploads_playlist_id(cid)
    try:
        recent_ids = yt.list_recent_video_ids(playlist, max_results=max_videos)
        api_calls += 1
    except Exception as exc:
        raise PollingError(f"playlistItems.list 실패: {exc}") from exc

    if not recent_ids:
        conn.execute(
            "UPDATE youtube_channels SET last_polled_at=CURRENT_TIMESTAMP WHERE id=?",
            (channel_row_id,),
        )
        conn.commit()
        return PollResult(
            channel_id=channel_row_id, new_video_count=0,
            updated_channel_title=updated_title, api_calls=api_calls,
        )

    # 3. Hydrate titles, published_at via videos.list (1u)
    try:
        metrics = yt.get_video_metrics(recent_ids)
        api_calls += 1
    except Exception as exc:
        raise PollingError(f"videos.list 실패: {exc}") from exc

    # 4. INSERT new ones
    new_count = 0
    for m in metrics:
        vtype = _classify_video_type(m.title, None)
        if _insert_video_if_new(
            conn,
            channel_row_id=channel_row_id,
            video_id=m.video_id,
            title=m.title,
            published_at=m.published_at,
            video_type=vtype,
        ):
            new_count += 1

    # 5. Mark last_polled_at
    conn.execute(
        "UPDATE youtube_channels SET last_polled_at=CURRENT_TIMESTAMP WHERE id=?",
        (channel_row_id,),
    )
    conn.commit()

    return PollResult(
        channel_id=channel_row_id, new_video_count=new_count,
        updated_channel_title=updated_title, api_calls=api_calls,
    )


def _to_rfc3339(yyyy_mm_dd: str, *, end_of_day: bool = False) -> str:
    """``2026-04-20`` → ``2026-04-20T23:59:59Z`` when end_of_day else 00:00:00Z."""
    # Validate
    datetime.strptime(yyyy_mm_dd, "%Y-%m-%d")
    tpart = "T23:59:59Z" if end_of_day else "T00:00:00Z"
    return yyyy_mm_dd + tpart


def poll_channel_period(
    conn, channel_row_id: int, *,
    start_date: str, end_date: str,
    max_videos: int = 50,
) -> PollResult:
    """Fetch videos uploaded within [start_date, end_date] inclusive.

    Costs ~100 quota units per call (search.list). Use sparingly — this is
    the expensive path, meant for one-off back-fills.
    """
    row = _load_channel_row(conn, channel_row_id)
    yt = _get_client()

    cid, updated_title = _ensure_real_channel_id(conn, row, yt)
    api_calls = 1 if updated_title else 0

    try:
        pub_after = _to_rfc3339(start_date, end_of_day=False)
        pub_before = _to_rfc3339(end_date, end_of_day=True)
    except ValueError as exc:
        raise PollingError(f"날짜 형식 오류 (YYYY-MM-DD): {exc}") from exc

    try:
        items = yt.search_videos_by_channel(
            cid,
            published_after=pub_after,
            published_before=pub_before,
            max_results=max_videos,
        )
        api_calls += 1  # search.list itself is 100u but we count as 1 call
    except Exception as exc:
        raise PollingError(f"search.list 실패: {exc}") from exc

    new_count = 0
    for it in items:
        vtype = _classify_video_type(it.get("title", ""), None)
        if _insert_video_if_new(
            conn,
            channel_row_id=channel_row_id,
            video_id=it["videoId"],
            title=it.get("title", ""),
            published_at=it.get("publishedAt", ""),
            thumbnail_url=it.get("thumbnail", ""),
            video_type=vtype,
        ):
            new_count += 1

    conn.execute(
        "UPDATE youtube_channels SET last_polled_at=CURRENT_TIMESTAMP WHERE id=?",
        (channel_row_id,),
    )
    conn.commit()

    return PollResult(
        channel_id=channel_row_id, new_video_count=new_count,
        updated_channel_title=updated_title, api_calls=api_calls,
    )
