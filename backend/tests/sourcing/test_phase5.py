"""Phase 5 — feedback loop, scheduler queue, alerts, integration tests."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta

import pytest

from db.sourcing_schema import init_sourcing_tables
from services import feedback_service as fb
from services import alerts_service as al
from services import sourcing_scheduler as sch
from services.llm_logger import LLMCallRecord, log_llm_call


# ---------------------------------------------------------------- #
# Feedback loop
# ---------------------------------------------------------------- #


def _seed_product(conn, pid: int, **kw):
    defaults = dict(video_id=1, position=1, product_name=f"p{pid}", category="c",
                     sourcing_status="new", revenue_krw_30d=0, sales_count_30d=0)
    defaults.update(kw)
    cols = ", ".join(["id"] + list(defaults.keys()))
    ph = ", ".join("?" * (1 + len(defaults)))
    conn.execute(
        f"INSERT INTO sourced_products ({cols}) VALUES ({ph})",
        [pid, *defaults.values()],
    )
    conn.commit()


def test_mark_purchased_sets_status_and_code():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    _seed_product(conn, 1)
    fb.mark_purchased(conn, product_id=1, erp_item_code="ABC-001")
    row = conn.execute(
        "SELECT sourcing_status, erp_item_code FROM sourced_products WHERE id=1"
    ).fetchone()
    assert row[0] == "purchased"
    assert row[1] == "ABC-001"


def test_refresh_sales_pulls_and_persists():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    _seed_product(conn, 1)
    fb.mark_purchased(conn, 1, "SKU-777")

    def fake_erp(code):
        assert code == "SKU-777"
        return {"sales_count_30d": 50, "revenue_krw_30d": 1_200_000, "return_rate_30d": 0.04}

    upd = fb.refresh_sales(conn, product_id=1, fetch_fn=fake_erp)
    assert upd.revenue_krw_30d == 1_200_000
    row = conn.execute(
        "SELECT sales_count_30d, revenue_krw_30d, return_rate_30d FROM sourced_products WHERE id=1"
    ).fetchone()
    assert row[0] == 50 and row[1] == 1_200_000


def test_sourcing_hit_rate_and_score_vs_outcome():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    # 3 purchased products, 2 above threshold
    _seed_product(conn, 1, sourcing_status="purchased", revenue_krw_30d=2_000_000)
    _seed_product(conn, 2, sourcing_status="purchased", revenue_krw_30d=500_000)
    _seed_product(conn, 3, sourcing_status="purchased", revenue_krw_30d=3_000_000)

    # attach market_research rows
    conn.execute(
        "INSERT INTO market_research (product_id, version, is_latest, market_size_score) "
        "VALUES (1, 1, 1, 5), (2, 1, 1, 2), (3, 1, 1, 5)"
    )
    conn.commit()

    hr = fb.sourcing_hit_rate(conn)
    assert hr["total_purchased"] == 3
    assert hr["hits"] == 2
    assert hr["hit_rate"] == pytest.approx(2 / 3, abs=0.01)

    bd = fb.score_vs_outcome(conn)
    assert len(bd) == 2
    # score 5 first (DESC)
    assert bd[0]["market_size_score"] == 5
    assert bd[0]["samples"] == 2


# ---------------------------------------------------------------- #
# Scheduler queue
# ---------------------------------------------------------------- #


def test_pick_next_video_prefers_pending():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    now = datetime.now(timezone.utc).isoformat()
    earlier = (datetime.now(timezone.utc) - timedelta(hours=10)).isoformat()
    conn.executemany(
        "INSERT INTO youtube_videos (video_id, processed_status, retry_count, next_retry_at, created_at) VALUES (?, ?, ?, ?, ?)",
        [
            ("vid-failed", "failed", 1, earlier, earlier),
            ("vid-pending", "pending", 0, None, now),
        ],
    )
    conn.commit()
    task = sch.pick_next_video(conn)
    assert task is not None
    # pending comes first even if failed is retry-ready
    r = conn.execute("SELECT video_id FROM youtube_videos WHERE id=?", (task["id"],)).fetchone()
    assert r[0] == "vid-pending"


def test_pick_next_video_waits_for_retry_backoff():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    future = (datetime.now(timezone.utc) + timedelta(hours=5)).isoformat()
    conn.execute(
        "INSERT INTO youtube_videos (video_id, processed_status, retry_count, next_retry_at) VALUES (?, ?, ?, ?)",
        ("vid1", "failed", 1, future),
    )
    conn.commit()
    task = sch.pick_next_video(conn)
    assert task is None  # still within backoff


def test_schedule_retry_increments_and_caps():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute(
        "INSERT INTO youtube_videos (id, video_id, processed_status, retry_count) VALUES (1, 'v', 'in_progress', 0)"
    )
    conn.commit()

    for _ in range(sch.MAX_RETRY):
        sch.schedule_retry(conn, video_id=1)
    row = conn.execute(
        "SELECT processed_status, retry_count FROM youtube_videos WHERE id=1"
    ).fetchone()
    assert row[0] == "failed"
    assert row[1] == sch.MAX_RETRY
    # Further picks should NOT return this video
    assert sch.pick_next_video(conn) is None


def test_mark_done_transition():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute(
        "INSERT INTO youtube_videos (id, video_id, processed_status) VALUES (1, 'v', 'in_progress')"
    )
    conn.commit()
    sch.mark_done(conn, 1)
    row = conn.execute(
        "SELECT processed_status, internal_step, processed_at FROM youtube_videos WHERE id=1"
    ).fetchone()
    assert row[0] == "done"
    assert row[1] == "done"
    assert row[2] is not None


# ---------------------------------------------------------------- #
# Alerts
# ---------------------------------------------------------------- #


def test_llm_failure_rate_with_mixed_results():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    for ok in [True]*15 + [False]*5:
        log_llm_call(conn, LLMCallRecord(
            service="s", provider="p", model="m", prompt_version="v",
            input_tokens=10, output_tokens=10, latency_ms=1, success=ok,
        ))
    stats = al.llm_failure_rate(conn, window_hours=24)
    assert stats["total"] == 20
    assert stats["failed"] == 5
    assert stats["rate"] == 0.25


def test_maybe_alert_high_llm_failure_threshold():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    # below min_samples → no alert
    for ok in [False]*5:
        log_llm_call(conn, LLMCallRecord(
            service="s", provider="p", model="m", prompt_version="v",
            input_tokens=0, output_tokens=0, latency_ms=1, success=ok,
        ))
    assert al.maybe_alert_high_llm_failure(conn) is None

    # push to 25 total w/ 25% failure rate → should trigger
    for ok in [True]*15 + [False]*5:
        log_llm_call(conn, LLMCallRecord(
            service="s", provider="p", model="m", prompt_version="v",
            input_tokens=0, output_tokens=0, latency_ms=1, success=ok,
        ))
    alert = al.maybe_alert_high_llm_failure(conn)
    assert alert is not None
    assert "실패율" in alert.title


def test_alert_payload_slack_text_formatting():
    alert = al.AlertPayload(
        level="error", title="YouTube quota exceeded",
        summary="일일 quota 초과",
        context={"used": 10200, "quota": 10000},
    )
    text = alert.as_slack_text()
    assert ":rotating_light:" in text
    assert "YouTube quota exceeded" in text
    assert '"used"' in text


def test_send_slack_no_webhook_returns_false():
    alert = al.AlertPayload(level="info", title="t", summary="s")
    ok = al.send_slack(alert, webhook_url=None)
    assert ok is False


def test_send_slack_with_webhook_invokes_poster():
    sent = {}
    def poster(url, payload):
        sent["url"] = url; sent["payload"] = payload
    alert = al.AlertPayload(level="warn", title="hi", summary="body")
    ok = al.send_slack(alert, webhook_url="https://hooks.slack/test", poster_fn=poster)
    assert ok is True
    assert "text" in sent["payload"]
    assert sent["url"].startswith("https://")


# ---------------------------------------------------------------- #
# Integration smoke test: Phase 1 → 3 pipeline on pilot transcript
# ---------------------------------------------------------------- #


def test_end_to_end_pipeline_on_pilot_transcript():
    """Tie it all together: transcript → correction → split → extract → market
    research insert → marketing asset insert. Uses fake LLMs throughout."""
    from services import transcript_service as ts
    from services import transcript_corrector as tc
    from services import product_extractor as px
    from services import market_analyzer as ma
    from services import marketing_generator as mg

    raw = (
        "첫 번째 제품은 샤오미 다기능 손전등입니다. "
        "철루맨 조명 3,100m마 배터리, 체련된 디자인. "
        "두 번째 제품은 오토바이 헬멧 카메라 인터콤입니다. "
        "실소파 라이더용 가성비 제품."
    )
    # Stage 1: light dedup (already clean)
    cleaned = ts.dedupe_sliding_window(raw)

    # Stage 2: LLM correction (fake rule-based)
    corr = tc.correct_transcript(cleaned, llm_fn=tc.fake_rule_based_llm)
    assert "1,000루멘" in corr.corrected
    assert "세련된" in corr.corrected

    # Stage 3: split + extract
    outcome = px.extract_products(corr.corrected, llm_fn=px.fake_keyword_extractor)
    assert len(outcome.products) == 2

    # Stage 4: DB insert + market research + marketing asset
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute(
        "INSERT INTO youtube_videos (id, video_id, processed_status) VALUES (1, 'gZPdX8NRv24', 'in_progress')"
    )
    import json
    for rec in outcome.products:
        conn.execute(
            """INSERT INTO sourced_products
               (video_id, position, product_name, brand, category, subcategory,
                key_features, search_keywords_kr, target_persona)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                rec["position"], rec["product_name"], rec.get("brand"),
                rec["category"], rec.get("subcategory"),
                json.dumps(rec.get("key_features") or []),
                json.dumps(rec.get("search_keywords_kr") or []),
                json.dumps(rec.get("target_persona") or {}, ensure_ascii=False),
            ),
        )
    conn.commit()

    # Run market research on product 1
    def fake_synth(_p, _ctx):
        return (
            {"market_size_score": 4, "competition_score": 3,
             "blue_ocean_signal": "x", "opportunity_summary": "y",
             "recommended_price_range_krw": {"low": 10000, "mid": 15000, "high": 20000},
             "target_persona_refined": {}, "positioning_statement": "",
             "risk_factors": []},
            {"provider": "fake", "model": "s", "input_tokens": 100, "output_tokens": 100},
        )
    product_row = conn.execute(
        "SELECT id, product_name, brand, category, search_keywords_kr FROM sourced_products WHERE id=1"
    ).fetchone()
    product_dict = {
        "id": product_row[0], "product_name": product_row[1],
        "brand": product_row[2], "category": product_row[3],
        "search_keywords_kr": json.loads(product_row[4] or "[]"),
    }
    mr = ma.run_analysis(product=product_dict, synth_fn=fake_synth)
    ma.persist_research(conn, mr)

    # Run marketing b2c
    def fake_b2c(prompt, ctx):
        return (
            {"title": "테스트 제품",
             "hero_headline": "h",
             "painpoint_scenarios": ["a","b","c"],
             "selling_points": [{"problem":"p","solution":"s","benefit":"b","visual_hint":"v"}]*5,
             "faq": {"defensive":[{"q":"q","a":"a"}]*4, "conversion":[{"q":"q","a":"a"}]*4},
             "seo_keywords": {"main":["m1","m2","m3"], "sub":[f"s{i}" for i in range(10)], "hashtags":["#1"]},
             "bundle_proposals": [
                 {"tier":"bait","price_krw":10000,"spec":"s","angle":"a"},
                 {"tier":"main","price_krw":15000,"spec":"s","angle":"a"},
                 {"tier":"crosssell","price_krw":22000,"spec":"s","angle":"a"},
             ]},
            {"provider":"fake","model":"b2c"},
        )
    asset = mg.generate_asset(
        "b2c", product=product_dict,
        market={"recommended_price_range_krw": mr.recommended_price_range_krw,
                "positioning_statement": "pos"},
        synth_fn=fake_b2c,
    )
    mg.persist_asset(conn, product_id=1, asset=asset)

    # Assertions: each table has the expected row counts
    assert conn.execute("SELECT COUNT(*) FROM sourced_products").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM market_research WHERE product_id=1").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM market_research WHERE product_id=1 AND is_latest=TRUE"
    ).fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM marketing_assets WHERE product_id=1").fetchone()[0] == 1
