"""Influencer candidate extraction pipeline.

Flow:
1. For each product keyword, YouTube search → channel ids
2. Batch-fetch channel stats (channels.list)
3. Batch-fetch last 10 video metrics per channel (playlistItems + videos.list)
4. Apply filters: subscriber band 10k-500k, activity ≥10%, ER ≥ 0.5%,
   sponsored_ratio ≤ 0.5.
5. Deduplicate against the `influencers` master table; update metrics if stale.
6. Return list of CandidateMatch objects (not yet persisted).

Network cost estimate per product:
  - keywords=3 → 3 × search.list (100u) = 300u
  - channels.list = 1u
  - playlistItems.list per channel = 1u
  - videos.list per channel = 1u
  For 10 channels: ~320u. Comfortably fits the 10,000-unit daily quota when
  run on a weekly batch.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from .youtube_client import (
    YouTubeClient, ChannelSnapshot, VideoMetrics, uploads_playlist_id,
)


@dataclass
class CandidateFilterConfig:
    min_subscribers: int = 10_000
    max_subscribers: int = 500_000
    min_activity_pct: float = 10.0     # avg_views / subs
    min_engagement_pct: float = 0.5    # comments / views
    max_sponsored_ratio: float = 0.5
    min_recent_videos: int = 5

    def passes(self, snap: ChannelSnapshot) -> tuple[bool, str]:
        if snap.subscriber_count < self.min_subscribers:
            return False, "subs too low"
        if snap.subscriber_count > self.max_subscribers:
            return False, "subs too high"
        if len(snap.recent_videos) < self.min_recent_videos:
            return False, "not enough recent videos"
        if snap.activity_ratio < self.min_activity_pct:
            return False, f"activity {snap.activity_ratio}% < {self.min_activity_pct}%"
        if snap.engagement_rate < self.min_engagement_pct:
            return False, f"ER {snap.engagement_rate}% < {self.min_engagement_pct}%"
        if snap.sponsored_ratio > self.max_sponsored_ratio:
            return False, f"sponsored {snap.sponsored_ratio:.0%}"
        return True, "ok"


@dataclass
class CandidateMatch:
    platform: str
    channel_snapshot: ChannelSnapshot
    quality_score: int
    match_score: float
    excluded: bool = False
    exclusion_reason: Optional[str] = None


def _recent_window_iso(days: int = 180) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_quality_score(snap: ChannelSnapshot) -> int:
    """0-100 blended score: subs log-scaled + activity + engagement."""
    import math
    subs_score = min(35, int(math.log10(max(1, snap.subscriber_count)) * 7))
    activity_score = min(35, int(snap.activity_ratio))
    er_score = min(30, int(snap.engagement_rate * 20))
    return max(0, min(100, subs_score + activity_score + er_score))


def find_candidates(
    *,
    youtube: YouTubeClient,
    keywords: list[str],
    config: Optional[CandidateFilterConfig] = None,
    published_after_days: int = 180,
    max_channels_per_keyword: int = 10,
) -> list[CandidateMatch]:
    """Run the full pipeline. Uses `published_after` to bias towards active
    channels (last 6 months)."""
    cfg = config or CandidateFilterConfig()
    published_after = _recent_window_iso(published_after_days)

    seen_channels: dict[str, ChannelSnapshot] = {}

    # Step 1 + 2: search → channels.list
    for kw in keywords[:5]:
        try:
            channel_ids = youtube.search_channels(
                kw, max_results=max_channels_per_keyword,
                published_after=published_after,
            )
        except Exception:
            continue
        new_ids = [cid for cid in channel_ids if cid not in seen_channels]
        if not new_ids:
            continue
        try:
            snapshots = youtube.get_channels(new_ids)
        except Exception:
            continue
        for s in snapshots:
            seen_channels[s.channel_id] = s

    # Step 3: recent video metrics per channel
    for cid, snap in seen_channels.items():
        try:
            vids = youtube.list_recent_video_ids(uploads_playlist_id(cid), max_results=10)
            if vids:
                snap.recent_videos = youtube.get_video_metrics(vids)
        except Exception:
            pass

    # Step 4: filter + score
    results: list[CandidateMatch] = []
    for snap in seen_channels.values():
        ok, reason = cfg.passes(snap)
        quality = compute_quality_score(snap) if ok else 0
        match_score = 0.0
        if ok:
            # Simple relevance heuristic: keyword appears in channel title/description
            text = (snap.title + " " + (snap.description or "")).lower()
            hits = sum(1 for kw in keywords if kw.lower() in text)
            match_score = round(hits / max(1, len(keywords)), 2)
        results.append(CandidateMatch(
            platform="youtube",
            channel_snapshot=snap,
            quality_score=quality,
            match_score=match_score,
            excluded=not ok,
            exclusion_reason=None if ok else reason,
        ))

    # Rank: accepted first by quality_score desc
    results.sort(key=lambda c: (c.excluded, -c.quality_score, -c.match_score))
    return results


# --------------------------------------------------------------------- #
# Persistence: upsert into `influencers` + insert `product_influencer_matches`
# --------------------------------------------------------------------- #


def upsert_influencer(conn, snap: ChannelSnapshot, contact_email: Optional[str] = None) -> int:
    """Insert or update the influencers master row. Returns id."""
    cur = conn.cursor()
    row = cur.execute(
        "SELECT id FROM influencers WHERE platform=? AND handle=?",
        ("youtube", snap.handle or snap.channel_id),
    ).fetchone()
    if row:
        inf_id = row[0]
        cur.execute(
            """UPDATE influencers SET
                display_name=?, follower_count=?, avg_views=?,
                engagement_rate=?, last_metrics_update=CURRENT_TIMESTAMP,
                main_categories=?
               WHERE id=?""",
            (
                snap.title, snap.subscriber_count, snap.avg_views,
                snap.engagement_rate,
                json.dumps([], ensure_ascii=False),
                inf_id,
            ),
        )
    else:
        cur.execute(
            """INSERT INTO influencers
               (platform, handle, profile_url, display_name,
                follower_count, avg_views, engagement_rate,
                main_categories, contact_email, last_metrics_update)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (
                "youtube",
                snap.handle or snap.channel_id,
                f"https://www.youtube.com/channel/{snap.channel_id}",
                snap.title,
                snap.subscriber_count,
                snap.avg_views,
                snap.engagement_rate,
                json.dumps([], ensure_ascii=False),
                contact_email,
            ),
        )
        inf_id = cur.lastrowid
    conn.commit()
    return inf_id


def persist_matches(
    conn, product_id: int, candidates: list[CandidateMatch],
) -> list[int]:
    """Upsert each candidate into influencers + create a match row per product."""
    ids: list[int] = []
    cur = conn.cursor()
    for c in candidates:
        inf_id = upsert_influencer(conn, c.channel_snapshot)
        existing = cur.execute(
            "SELECT id FROM product_influencer_matches WHERE product_id=? AND influencer_id=?",
            (product_id, inf_id),
        ).fetchone()
        if existing:
            cur.execute(
                """UPDATE product_influencer_matches SET
                    quality_score=?, match_score=?, is_excluded=?, exclusion_reason=?
                   WHERE id=?""",
                (c.quality_score, c.match_score,
                 bool(c.excluded), c.exclusion_reason, existing[0]),
            )
            ids.append(existing[0])
        else:
            cur.execute(
                """INSERT INTO product_influencer_matches
                   (product_id, influencer_id, quality_score, match_score,
                    is_excluded, exclusion_reason)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (product_id, inf_id, c.quality_score, c.match_score,
                 bool(c.excluded), c.exclusion_reason),
            )
            ids.append(cur.lastrowid)
    conn.commit()
    return ids
