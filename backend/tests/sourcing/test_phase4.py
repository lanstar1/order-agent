"""Phase 4 — YouTube client, influencer finder, pricing, outreach drafts."""
from __future__ import annotations

import json
import sqlite3

import pytest

from db.sourcing_schema import init_sourcing_tables
from services.youtube_client import (
    YouTubeClient, ChannelSnapshot, VideoMetrics, uploads_playlist_id,
)
from services import influencer_finder as inf_find
from services import influencer_pricing as pricing
from services import outreach_service as outreach


# -------------------------------------------------------- #
# YouTube client helpers
# -------------------------------------------------------- #


def test_uploads_playlist_id_converts_uc_to_uu():
    assert uploads_playlist_id("UCabcdefghijklmnopqrstuv") == "UUabcdefghijklmnopqrstuv"


def test_channel_snapshot_metrics():
    snap = ChannelSnapshot(
        channel_id="UC1", title="t", subscriber_count=50_000,
        recent_videos=[
            VideoMetrics("v1", title="리뷰", view_count=10_000, comment_count=120),
            VideoMetrics("v2", title="[협찬] 리뷰", view_count=8_000, comment_count=80),
            VideoMetrics("v3", title="언박싱", view_count=5_000, comment_count=40),
        ],
    )
    assert snap.avg_views == 7_666
    assert snap.engagement_rate > 0.0
    # activity = 7666/50000*100 ≈ 15.33 %
    assert 14 < snap.activity_ratio < 16
    # 1/3 sponsored
    assert round(snap.sponsored_ratio, 2) == 0.33


def test_search_channels_prefers_id_channelId_per_official_spec():
    """Regression guard: youtube/api-samples uses item.id.channelId; we must
    honour that even if a snippet.channelId alias is absent."""
    resp = {"items": [
        {"id": {"kind": "youtube#channel", "channelId": "UC_AAA"},
         "snippet": {"title": "official shape"}},
        # Legacy fallback: only snippet.channelId present
        {"snippet": {"channelId": "UC_BBB", "title": "fallback shape"}},
    ]}
    yt = YouTubeClient("KEY", http_fetcher=lambda url: resp)
    assert yt.search_channels("차박") == ["UC_AAA", "UC_BBB"]


def test_youtube_client_search_and_get_channels_with_fake_fetcher():
    # Mimic the canonical YouTube Data API v3 response shape
    # (per github.com/youtube/api-samples python/search.py).
    # `id.channelId` is the canonical field for type=channel searches.
    responses = {
        "search": {"items": [
            {"id": {"kind": "youtube#channel", "channelId": "UC111"},
             "snippet": {"title": "ch1"}},
            {"id": {"kind": "youtube#channel", "channelId": "UC222"},
             "snippet": {"title": "ch2"}},
        ]},
        "channels": {"items": [
            {"id": "UC111", "snippet": {"title": "ch1", "description": "차박"},
             "statistics": {"subscriberCount": "50000", "videoCount": "200"}},
            {"id": "UC222", "snippet": {"title": "ch2", "description": "리뷰"},
             "statistics": {"subscriberCount": "80000", "videoCount": "150"}},
        ]},
    }
    def fake_fetch(url):
        if "/search" in url:
            return responses["search"]
        if "/channels" in url:
            return responses["channels"]
        return {"items": []}

    yt = YouTubeClient("KEY", http_fetcher=fake_fetch)
    ids = yt.search_channels("차박")
    assert ids == ["UC111", "UC222"]
    snaps = yt.get_channels(ids)
    assert len(snaps) == 2
    assert snaps[0].subscriber_count == 50_000


# -------------------------------------------------------- #
# Influencer finder filters + scoring
# -------------------------------------------------------- #


def _mk_snap(cid: str, *, subs: int, views: int, comments: int,
              sponsored: int = 0, n_videos: int = 10,
              title: str = "차박 리뷰") -> ChannelSnapshot:
    vids = []
    for i in range(n_videos):
        is_sponsored = i < sponsored
        # Per-channel unique video id — avoids cross-channel matching in tests
        vids.append(VideoMetrics(
            video_id=f"{cid}_v{i}",
            title=(f"[협찬] {title}" if is_sponsored else title),
            view_count=views,
            comment_count=comments,
        ))
    return ChannelSnapshot(
        channel_id=cid, title=title, subscriber_count=subs,
        recent_videos=vids, description=title,
    )


def test_filter_rejects_low_subs():
    snap = _mk_snap("UC_small", subs=5_000, views=1000, comments=10)
    ok, _ = inf_find.CandidateFilterConfig().passes(snap)
    assert not ok


def test_filter_rejects_too_many_sponsored():
    # sponsored 6/10 = 60% > 50% default
    snap = _mk_snap("UC_spn", subs=50_000, views=8_000, comments=80, sponsored=6)
    ok, reason = inf_find.CandidateFilterConfig().passes(snap)
    assert not ok
    assert "sponsored" in reason


def test_filter_accepts_healthy_channel():
    snap = _mk_snap("UC_ok", subs=50_000, views=8_000, comments=80, sponsored=1)
    ok, _ = inf_find.CandidateFilterConfig().passes(snap)
    assert ok


def test_find_candidates_ranks_by_quality():
    # Build a fake YouTubeClient subclass with scripted behavior
    class FakeYT:
        def __init__(self):
            self.snaps = {
                "UC_a": _mk_snap("UC_a", subs=30_000, views=5_000, comments=30, sponsored=0),
                "UC_b": _mk_snap("UC_b", subs=200_000, views=50_000, comments=500, sponsored=1, title="차박 리뷰"),
            }
        def search_channels(self, *a, **kw): return list(self.snaps.keys())
        def get_channels(self, ids):        return [self.snaps[i] for i in ids]
        def list_recent_video_ids(self, playlist_id, max_results=10):
            cid = playlist_id.replace("UU", "UC")
            return [v.video_id for v in self.snaps[cid].recent_videos]
        def get_video_metrics(self, vids):
            return [v for s in self.snaps.values() for v in s.recent_videos if v.video_id in vids]

    results = inf_find.find_candidates(
        youtube=FakeYT(), keywords=["차박"],
    )
    # Both channels should pass filters and UC_b (bigger) should rank first
    accepted = [r for r in results if not r.excluded]
    assert len(accepted) == 2
    assert accepted[0].channel_snapshot.channel_id == "UC_b"


def test_persist_matches_upserts_influencers():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute(
        "INSERT INTO sourced_products (id, video_id, position, product_name, category) "
        "VALUES (1, 1, 1, 'p', 'c')"
    )
    conn.commit()

    snap = _mk_snap("UCxxx", subs=50_000, views=8_000, comments=80)
    cand = inf_find.CandidateMatch(
        platform="youtube", channel_snapshot=snap,
        quality_score=80, match_score=0.8,
    )
    ids1 = inf_find.persist_matches(conn, product_id=1, candidates=[cand])
    ids2 = inf_find.persist_matches(conn, product_id=1, candidates=[cand])
    # second run is upsert → same match id, influencers table has exactly 1 row
    assert ids1 == ids2
    n = conn.execute("SELECT COUNT(*) FROM influencers").fetchone()[0]
    assert n == 1


# -------------------------------------------------------- #
# Pricing engine
# -------------------------------------------------------- #


def test_estimate_quote_basic_youtube():
    q = pricing.estimate_quote(
        platform="youtube", avg_views=10_000, engagement_rate_pct=7.0,
        content_format="integrated_review", category="camping",
    )
    # base = 10000 * 15000 / 1000 = 150000
    # fmt=1.5, er_bonus=1.3 (>=5), cat=1.1 → 150000*1.5*1.3*1.1 = 321750
    assert 300_000 < q.raw_quote_krw < 350_000
    assert q.low_krw < q.raw_quote_krw < q.high_krw


def test_estimate_quote_high_er_bonus():
    q = pricing.estimate_quote(
        platform="youtube", avg_views=10_000, engagement_rate_pct=12.0,
        content_format="integrated_review", category="tech",
    )
    assert q.engagement_bonus == 1.6


def test_estimate_quote_low_er_no_bonus():
    q = pricing.estimate_quote(
        platform="instagram", avg_views=10_000, engagement_rate_pct=1.0,
        content_format="ppl", category="daily",
    )
    assert q.engagement_bonus == 1.0
    assert q.cpm_rate == 10_000  # instagram_reels default for non-feed


# -------------------------------------------------------- #
# Outreach drafts
# -------------------------------------------------------- #


def _fake_draft_synth(_prompt, ctx):
    off = ctx["offer_kind"]
    ch = ctx["channel_kind"]
    if ch == "email" and off == "gift":
        return ({
            "subject": "[제품 협찬 문의] 샤오미 다기능 손전등 제공 가능할까요?",
            "body": "안녕하세요. ...수신거부는 이메일 답신으로 부탁드립니다. #광고",
            "product_proposal": {
                "qty": 2, "color": "black", "accessories": ["USB-C 케이블"],
                "shipping_note": "리뷰용 2개", "estimated_cost_krw": 27000,
            },
        }, {"provider": "fake", "model": "draft"})
    if ch == "email" and off == "paid":
        return ({
            "subject": "[유료 협찬 제안] 오프로드 채널 — 통합 리뷰 의뢰",
            "body": "협찬비는 채널 단가표 회신 부탁드립니다. 수신거부 가능. #광고",
            "product_proposal": {
                "qty": 3, "color": "black", "accessories": [],
                "shipping_note": "리뷰용", "estimated_cost_krw": 40500,
            },
        }, {"provider": "fake", "model": "draft"})
    # instagram DM
    return ({
        "subject": None,
        "body": "안녕하세요! 제품 한번 테스트해 보실래요? 샘플 2개 제공 가능합니다 🙂 #광고",
        "product_proposal": {
            "qty": 2, "color": "black", "accessories": [],
            "shipping_note": "DM 초안", "estimated_cost_krw": 27000,
        },
    }, {"provider": "fake", "model": "draft"})


def test_generate_draft_email_gift():
    d = outreach.generate_draft(
        match_id=1,
        influencer={"handle": "@chabak", "platform": "youtube",
                    "follower_count": 38200, "avg_views": 11500},
        product={"name": "샤오미 다기능 손전등"},
        offer_kind="gift", channel_kind="email",
        sender={"name": "(주)랜스타", "contact_email": "kyu@lanstar.co.kr"},
        synth_fn=_fake_draft_synth,
    )
    assert d.subject and "협찬" in d.subject
    assert "수신거부" in d.body
    assert "#광고" in d.body


def test_generate_draft_paid_omits_price_in_body():
    quote = pricing.estimate_quote(
        platform="youtube", avg_views=45_000, engagement_rate_pct=5.0,
        content_format="integrated_review", category="auto",
    )
    d = outreach.generate_draft(
        match_id=2,
        influencer={"handle": "@offroadkim", "platform": "youtube",
                    "follower_count": 165_000, "avg_views": 45_000},
        product={"name": "샤오미 다기능 손전등"},
        offer_kind="paid", channel_kind="email",
        sender={"name": "(주)랜스타", "contact_email": "kyu@lanstar.co.kr"},
        estimated_quote={
            "raw_quote_krw": quote.raw_quote_krw,
            "low_krw": quote.low_krw, "high_krw": quote.high_krw,
        },
        synth_fn=_fake_draft_synth,
    )
    # Must not leak the number in the body
    assert str(quote.raw_quote_krw) not in d.body
    assert "단가표" in d.body


def test_generate_draft_instagram_dm_no_subject():
    d = outreach.generate_draft(
        match_id=3,
        influencer={"handle": "@camping.mom", "platform": "instagram",
                    "follower_count": 52_000, "avg_views": 1800},
        product={"name": "샤오미 다기능 손전등"},
        offer_kind="gift", channel_kind="instagram_dm",
        sender={"name": "(주)랜스타", "contact_email": "kyu@lanstar.co.kr"},
        synth_fn=_fake_draft_synth,
    )
    assert d.subject is None
    assert "#광고" in d.body


def test_build_mailto_url_encodes_subject_and_body():
    url = outreach.build_mailto_url(
        "c@example.com", "제안 | 안전 + 가성비", "안녕하세요\n본문입니다."
    )
    assert url.startswith("mailto:c%40example.com")
    assert "subject=" in url
    assert "body=" in url


def test_copy_payload_shapes_differ_by_channel():
    d_email = outreach.OutreachDraft(
        match_id=1, channel_kind="email", offer_kind="gift",
        subject="hi", body="body", product_proposal={},
    )
    d_dm = outreach.OutreachDraft(
        match_id=1, channel_kind="instagram_dm", offer_kind="gift",
        subject=None, body="dm body", product_proposal={},
    )
    p1 = outreach.copy_payload(d_email)
    p2 = outreach.copy_payload(d_dm)
    assert "제목: hi" in p1["clipboard_text"]
    assert "제목" not in p2["clipboard_text"]


def test_persist_draft_and_status_transitions():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    # minimal FK targets
    conn.execute(
        "INSERT INTO sourced_products (id, video_id, position, product_name, category) "
        "VALUES (1, 1, 1, 'p', 'c')"
    )
    conn.execute(
        "INSERT INTO influencers (id, platform, handle, profile_url) "
        "VALUES (1, 'youtube', '@x', 'https://yt/x')"
    )
    conn.execute(
        "INSERT INTO product_influencer_matches (id, product_id, influencer_id) "
        "VALUES (1, 1, 1)"
    )
    conn.commit()

    d = outreach.generate_draft(
        match_id=1,
        influencer={"handle": "@x", "platform": "youtube"},
        product={"name": "p"},
        offer_kind="gift", channel_kind="email",
        sender={"name": "s", "contact_email": "a@b"},
        synth_fn=_fake_draft_synth,
    )
    did = outreach.persist_draft(conn, d)
    status0 = conn.execute(
        "SELECT status FROM outreach_drafts WHERE id=?", (did,)
    ).fetchone()[0]
    assert status0 == "draft"

    outreach.mark_copied(conn, did)
    status1 = conn.execute(
        "SELECT status FROM outreach_drafts WHERE id=?", (did,)
    ).fetchone()[0]
    assert status1 == "copied"

    outreach.update_status(conn, did, "replied", "긍정적 답변")
    row = conn.execute(
        "SELECT status, manual_response_note FROM outreach_drafts WHERE id=?",
        (did,),
    ).fetchone()
    assert row[0] == "replied"
    assert "긍정적" in row[1]


def test_update_status_rejects_invalid_value():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    with pytest.raises(ValueError):
        outreach.update_status(conn, 1, "totally_bogus")
