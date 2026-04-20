"""Channel polling orchestration tests (no network)."""
from __future__ import annotations

import os
import sqlite3
from unittest.mock import patch

import pytest

from db.sourcing_schema import init_sourcing_tables
from services import channel_polling as polling
from services.youtube_client import ChannelSnapshot, VideoMetrics


# ─────────────────────────────────────────────────────────── #
# Fake YouTube client
# ─────────────────────────────────────────────────────────── #


class FakeYT:
    def __init__(self):
        self.calls = {"resolve": 0, "playlist": 0, "videos": 0, "search_period": 0}

    def resolve_by_handle(self, handle):
        self.calls["resolve"] += 1
        return ChannelSnapshot(
            channel_id="UCabcdefghijklmnopqrstuv",
            title="알뜰직구",
            handle=handle,
            subscriber_count=42_000,
            video_count=120,
        )

    def list_recent_video_ids(self, playlist_id, *, max_results=10):
        self.calls["playlist"] += 1
        return [f"vid{i:02d}_aaaa" for i in range(3)]

    def get_video_metrics(self, video_ids):
        self.calls["videos"] += 1
        out = []
        for i, vid in enumerate(video_ids):
            out.append(VideoMetrics(
                video_id=vid,
                title=f"알리 BEST {i+1}",
                view_count=5000 + i*1000,
                comment_count=50 + i*10,
                published_at=f"2026-04-{15+i:02d}T12:00:00Z",
            ))
        return out

    def search_videos_by_channel(self, channel_id, **kw):
        self.calls["search_period"] += 1
        return [
            {"videoId": "periodA_aaaa", "title": "기간 내 영상 A",
             "publishedAt": "2026-04-10T00:00:00Z", "thumbnail": ""},
            {"videoId": "periodB_bbbb", "title": "기간 내 영상 B",
             "publishedAt": "2026-04-11T00:00:00Z", "thumbnail": ""},
        ]


# ─────────────────────────────────────────────────────────── #
# Fixtures
# ─────────────────────────────────────────────────────────── #


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_sourcing_tables(c, dialect="sqlite")
    return c


def _seed_provisional_channel(conn, handle="@알뜰직구"):
    """채널 등록 직후 상태 시뮬 — handle만 있고 real UCxxx 아직 없음."""
    cur = conn.execute(
        "INSERT INTO youtube_channels (channel_id, channel_handle, category) VALUES (?, ?, ?)",
        (handle, handle, "알리추천"),
    )
    conn.commit()
    return cur.lastrowid


# ─────────────────────────────────────────────────────────── #
# Tests
# ─────────────────────────────────────────────────────────── #


def test_poll_channel_now_resolves_handle_and_inserts_videos(conn):
    row_id = _seed_provisional_channel(conn)
    fake = FakeYT()
    with patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key"}), \
         patch("services.channel_polling._get_client", return_value=fake):
        result = polling.poll_channel_now(conn, row_id)
    assert result.new_video_count == 3
    assert result.updated_channel_title == "알뜰직구"
    # Channel row got real channelId + title
    row = conn.execute(
        "SELECT channel_id, channel_title, subscriber_count, last_polled_at "
        "FROM youtube_channels WHERE id=?", (row_id,)
    ).fetchone()
    assert row[0] == "UCabcdefghijklmnopqrstuv"
    assert row[1] == "알뜰직구"
    assert row[2] == 42_000
    assert row[3] is not None
    # Videos inserted
    vids = conn.execute(
        "SELECT video_id, title, processed_status FROM youtube_videos "
        "WHERE channel_id=? ORDER BY id", (row_id,)
    ).fetchall()
    assert len(vids) == 3
    for v in vids:
        assert v[0].startswith("vid")
        assert v[2] == "pending"


def test_poll_channel_now_is_idempotent(conn):
    """Second call of poll_channel_now with the same fake videos must NOT
    insert duplicates."""
    row_id = _seed_provisional_channel(conn)
    fake = FakeYT()
    with patch("services.channel_polling._get_client", return_value=fake):
        polling.poll_channel_now(conn, row_id)
        result2 = polling.poll_channel_now(conn, row_id)
    assert result2.new_video_count == 0  # already present
    cnt = conn.execute(
        "SELECT COUNT(*) FROM youtube_videos WHERE channel_id=?", (row_id,)
    ).fetchone()[0]
    assert cnt == 3


def test_poll_channel_period_fetches_and_inserts(conn):
    row_id = _seed_provisional_channel(conn)
    fake = FakeYT()
    with patch("services.channel_polling._get_client", return_value=fake):
        result = polling.poll_channel_period(
            conn, row_id, start_date="2026-04-10", end_date="2026-04-15",
        )
    assert result.new_video_count == 2
    assert fake.calls["search_period"] == 1
    rows = conn.execute(
        "SELECT video_id FROM youtube_videos WHERE channel_id=?", (row_id,)
    ).fetchall()
    assert {r[0] for r in rows} == {"periodA_aaaa", "periodB_bbbb"}


def test_poll_channel_period_rejects_bad_date():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    row_id = _seed_provisional_channel(conn)
    fake = FakeYT()
    with patch("services.channel_polling._get_client", return_value=fake):
        with pytest.raises(polling.PollingError) as e:
            polling.poll_channel_period(
                conn, row_id, start_date="2026/04/10", end_date="2026-04-15",
            )
    assert "날짜 형식" in str(e.value)


def test_poll_requires_api_key(conn):
    row_id = _seed_provisional_channel(conn)
    # No YOUTUBE_API_KEY in env
    with patch.dict(os.environ, {"YOUTUBE_API_KEY": ""}, clear=False):
        # Remove any ambient key
        os.environ.pop("YOUTUBE_API_KEY", None)
        with pytest.raises(polling.PollingError) as e:
            polling.poll_channel_now(conn, row_id)
    assert "YOUTUBE_API_KEY" in str(e.value)


def test_resolve_channel_metadata_skips_when_already_resolved(conn):
    """이미 UCxxx로 해결된 채널은 추가 API 호출 안 함."""
    cur = conn.execute(
        """INSERT INTO youtube_channels (channel_id, channel_handle, channel_title)
           VALUES ('UCabcdefghijklmnopqrstuv', '@알뜰직구', '알뜰직구')"""
    )
    conn.commit()
    row_id = cur.lastrowid
    fake = FakeYT()
    with patch("services.channel_polling._get_client", return_value=fake):
        result = polling.resolve_channel_metadata(conn, row_id)
    assert result.updated_channel_title is None  # no change needed
    assert fake.calls["resolve"] == 0


def test_classify_video_type_shorts():
    assert polling._classify_video_type("리뷰 제목", duration_sec=45) == "short"
    assert polling._classify_video_type("리뷰 제목", duration_sec=300) == "normal"
    assert polling._classify_video_type("LIVE 방송 중", None) == "live"
    assert polling._classify_video_type("실시간 알리 쇼핑", None) == "live"
    assert polling._classify_video_type("일반 리뷰", None) == "normal"


def test_to_rfc3339_conversion():
    assert polling._to_rfc3339("2026-04-10") == "2026-04-10T00:00:00Z"
    assert polling._to_rfc3339("2026-04-10", end_of_day=True) == "2026-04-10T23:59:59Z"
    with pytest.raises(ValueError):
        polling._to_rfc3339("2026/04/10")
