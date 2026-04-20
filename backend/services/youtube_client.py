"""YouTube Data API v3 thin client (search.list, channels.list, videos.list).

Kept intentionally thin — pass an `http_fetcher` in tests. Quota costs are
documented inline.
"""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional, Iterable


API_BASE = "https://www.googleapis.com/youtube/v3"


@dataclass
class VideoMetrics:
    video_id: str
    title: str = ""
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    published_at: str = ""


@dataclass
class ChannelSnapshot:
    channel_id: str
    title: str = ""
    handle: str = ""
    subscriber_count: int = 0
    video_count: int = 0
    description: str = ""
    # metrics from the latest 10 videos
    recent_videos: list[VideoMetrics] = field(default_factory=list)

    @property
    def avg_views(self) -> int:
        if not self.recent_videos:
            return 0
        return sum(v.view_count for v in self.recent_videos) // len(self.recent_videos)

    @property
    def avg_comments(self) -> int:
        if not self.recent_videos:
            return 0
        return sum(v.comment_count for v in self.recent_videos) // len(self.recent_videos)

    @property
    def engagement_rate(self) -> float:
        """Rough proxy: comments / views averaged over recent videos."""
        if not self.recent_videos or self.avg_views == 0:
            return 0.0
        return round(self.avg_comments / self.avg_views * 100, 3)

    @property
    def activity_ratio(self) -> float:
        """avg_views / subscriber_count (in percent)."""
        if not self.subscriber_count:
            return 0.0
        return round(self.avg_views / self.subscriber_count * 100, 2)

    @property
    def sponsored_ratio(self) -> float:
        """Fraction of recent videos whose title/description contains paid-content markers."""
        if not self.recent_videos:
            return 0.0
        markers = ("협찬", "유료광고 포함", "광고 포함", "#ad", "sponsored", "PPL")
        hits = 0
        for v in self.recent_videos:
            hay = (v.title or "").lower()
            if any(m.lower() in hay for m in markers):
                hits += 1
        return hits / len(self.recent_videos)


class YouTubeClient:
    """Thin wrapper — every method takes ~1-100 quota units."""

    def __init__(
        self, api_key: str, *, http_fetcher: Optional[callable] = None,
    ):
        self.api_key = api_key
        self._fetch = http_fetcher or self._default_fetcher

    def _default_fetcher(self, url: str) -> dict:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _get(self, path: str, params: dict) -> dict:
        params = {**params, "key": self.api_key}
        qs = urllib.parse.urlencode(params)
        return self._fetch(f"{API_BASE}/{path}?{qs}")

    # ---- search.list (100u) ------------------------------------------- #
    def search_channels(
        self, query: str, *, max_results: int = 10,
        order: str = "relevance", region_code: str = "KR",
        published_after: Optional[str] = None,
    ) -> list[str]:
        """Return a list of channelIds matching a keyword search.

        NOTE: 100 quota units. Use sparingly.
        """
        params = {
            "part": "snippet", "type": "channel",
            "q": query, "maxResults": max_results, "order": order,
            "regionCode": region_code,
        }
        if published_after:
            params["publishedAfter"] = published_after
        data = self._get("search", params)
        return [it["snippet"]["channelId"]
                for it in data.get("items", [])
                if "channelId" in it.get("snippet", {})]

    # ---- channels.list (1u) ------------------------------------------- #
    def get_channels(self, channel_ids: Iterable[str]) -> list[ChannelSnapshot]:
        ids = [cid for cid in channel_ids if cid]
        if not ids:
            return []
        data = self._get("channels", {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(ids[:50]),
        })
        out: list[ChannelSnapshot] = []
        for it in data.get("items", []):
            stats = it.get("statistics") or {}
            sn = it.get("snippet") or {}
            out.append(ChannelSnapshot(
                channel_id=it.get("id", ""),
                title=sn.get("title", ""),
                handle=(sn.get("customUrl") or ""),
                subscriber_count=int(stats.get("subscriberCount") or 0),
                video_count=int(stats.get("videoCount") or 0),
                description=sn.get("description", ""),
            ))
        return out

    # ---- playlistItems.list (1u) -------------------------------------- #
    def list_recent_video_ids(self, uploads_playlist_id: str, *, max_results: int = 10) -> list[str]:
        data = self._get("playlistItems", {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": max_results,
        })
        return [it["contentDetails"]["videoId"] for it in data.get("items", [])]

    # ---- videos.list (1u) --------------------------------------------- #
    def get_video_metrics(self, video_ids: Iterable[str]) -> list[VideoMetrics]:
        ids = [v for v in video_ids if v]
        if not ids:
            return []
        data = self._get("videos", {
            "part": "snippet,statistics",
            "id": ",".join(ids[:50]),
        })
        out: list[VideoMetrics] = []
        for it in data.get("items", []):
            st = it.get("statistics") or {}
            sn = it.get("snippet") or {}
            out.append(VideoMetrics(
                video_id=it.get("id", ""),
                title=sn.get("title", ""),
                view_count=int(st.get("viewCount") or 0),
                like_count=int(st.get("likeCount") or 0),
                comment_count=int(st.get("commentCount") or 0),
                published_at=sn.get("publishedAt", ""),
            ))
        return out


# --------------------------------------------------------------------- #
# Uploads playlist helper: UC…… channel id → UU…… uploads playlist id
# --------------------------------------------------------------------- #

def uploads_playlist_id(channel_id: str) -> str:
    """YouTube convention: uploads playlist id is channel id with 'UC' → 'UU'."""
    if channel_id.startswith("UC"):
        return "UU" + channel_id[2:]
    return channel_id
