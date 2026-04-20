"""Phase 1 unit tests.

Run:
    PYTHONPATH=/sessions/nice-eloquent-mayer/sourcing python -m pytest tests/ -v
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from db.sourcing_schema import init_sourcing_tables, sourcing_ddl
from services.youtube_url import parse_youtube_input
from services import transcript_service as ts
from services import transcript_corrector as tc
from services import product_extractor as px
from services.llm_logger import (
    LLMCallRecord,
    compute_cost_usd,
    log_llm_call,
)


# ------------------------------------------------------------------ #
# DB schema
# ------------------------------------------------------------------ #


def test_sourcing_ddl_sqlite_executes_cleanly():
    ddl = sourcing_ddl("sqlite")
    assert len(ddl) == 10
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    cur = conn.cursor()
    tables = {r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    expected = {
        "youtube_channels", "youtube_videos", "sourced_products",
        "market_research", "marketing_assets", "influencers",
        "product_influencer_matches", "outreach_drafts",
        "persona_labels", "llm_call_logs",
    }
    assert expected.issubset(tables)


def test_sourcing_ddl_is_idempotent():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    init_sourcing_tables(conn, dialect="sqlite")  # must not raise


# ------------------------------------------------------------------ #
# URL parser
# ------------------------------------------------------------------ #


@pytest.mark.parametrize(
    "raw, expected_kind, expected_value",
    [
        ("https://www.youtube.com/watch?v=gZPdX8NRv24",    "video",      "gZPdX8NRv24"),
        ("https://youtu.be/gZPdX8NRv24",                    "video",      "gZPdX8NRv24"),
        ("https://www.youtube.com/shorts/abcdefghijk",      "video",      "abcdefghijk"),
        ("https://www.youtube.com/@ali_gadget/videos",      "handle",     "@ali_gadget"),
        # Unicode (Hangul) handle — raw and percent-encoded forms must both parse
        ("https://www.youtube.com/@알뜰직구",                 "handle",     "@알뜰직구"),
        ("https://www.youtube.com/@%EC%95%8C%EB%9C%B0%EC%A7%81%EA%B5%AC",
                                                            "handle",     "@알뜰직구"),
        ("@알뜰직구",                                         "handle",     "@알뜰직구"),
        ("https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv", "channel_id", "UCabcdefghijklmnopqrstuv"),
        ("@best_reviews",                                   "handle",     "@best_reviews"),
        ("UCabcdefghijklmnopqrstuv",                        "channel_id", "UCabcdefghijklmnopqrstuv"),
        ("gZPdX8NRv24",                                     "video",      "gZPdX8NRv24"),
        ("https://www.youtube.com/c/LegacyName",            "custom",     "LegacyName"),
        ("",                                                "unknown",    ""),
        ("ftp://nope",                                      "unknown",    ""),
    ],
)
def test_parse_youtube_input(raw, expected_kind, expected_value):
    result = parse_youtube_input(raw)
    assert result.kind == expected_kind
    assert result.value == expected_value


# ------------------------------------------------------------------ #
# Transcript SRT parse + sliding-window dedup
# ------------------------------------------------------------------ #


SRT_SAMPLE = """1
00:00:00,000 --> 00:00:03,000
알리 꿀템 모음 알리 꿀템 모음

2
00:00:03,000 --> 00:00:06,000
알리 꿀템 모음입니다 알리 꿀템 모음입니다

3
00:00:06,000 --> 00:00:10,000
첫 번째 제품은 손전등입니다

4
00:00:10,000 --> 00:00:14,000
[음악] 1000루멘 조명입니다 1000루멘 조명입니다
"""


def test_parse_srt_extracts_segments():
    segs = ts.parse_srt(SRT_SAMPLE)
    assert len(segs) == 4
    assert segs[0].start_sec == 0
    assert segs[0].end_sec == 3
    assert "[음악]" not in segs[3].text


def test_dedupe_sliding_window_collapses_repeats():
    txt = "알리 꿀템 모음 알리 꿀템 모음 입니다 알리 꿀템 모음 입니다"
    out = ts.dedupe_sliding_window(txt)
    # The tail of 'out' ("알리 꿀템 모음") is never appended twice in a row.
    assert out.count("알리 꿀템 모음") <= 2
    assert len(out) < len(txt)


def test_clean_transcript_roundtrip_on_pilot_sample():
    """Pilot file is ALREADY dedup'd from a previous run. Verify idempotence:
    a second dedup must not explode the text or lose critical substrings."""
    pilot_raw = Path(
        "/sessions/nice-eloquent-mayer/mnt/outputs/pilot/01_transcript_raw.txt"
    )
    if not pilot_raw.exists():
        pytest.skip("pilot transcript not available")
    raw = pilot_raw.read_text(encoding="utf-8")
    out = ts.dedupe_sliding_window(raw)
    # Idempotent on already-deduped text.
    assert len(out) <= len(raw)
    assert "첫 번째 제품" in out
    assert "열 번째 제품" in out


# ------------------------------------------------------------------ #
# Transcript corrector (change-ratio guard + chunking)
# ------------------------------------------------------------------ #


def test_compute_change_ratio_identical():
    assert tc.compute_change_ratio("hello", "hello") == 0.0


def test_compute_change_ratio_completely_different():
    r = tc.compute_change_ratio("hello world", "foobar baz")
    assert r > 0.5


def test_chunk_text_preserves_content():
    txt = "a" * 10_000
    chunks = tc.chunk_text(txt, chunk_chars=4000, overlap_chars=100)
    assert all(len(c) <= 4000 for c in chunks)
    # first chunk starts at 0
    assert chunks[0].startswith("a")
    # chunks overlap by 100
    assert chunks[0][-100:] == chunks[1][:100]


def test_correct_transcript_with_fake_llm_fixes_known_typos():
    raw = "첫 번째 제품은 철루맨 조명 3,100m마 배터리, 체련된 디자인."
    res = tc.correct_transcript(raw, llm_fn=tc.fake_rule_based_llm)
    assert "1,000루멘" in res.corrected
    assert "3,100mAh" in res.corrected
    assert "세련된" in res.corrected
    assert not res.used_fallback
    assert res.ratio < 0.3


def test_correction_fallback_on_excessive_change():
    def runaway(_prompt: str, _text: str) -> tuple[str, dict]:
        return ("전혀 다른 문장으로 바꿔버렸어요.", {"provider": "fake", "model": "bad"})
    raw = "이 제품은 정말 좋아요."
    res = tc.correct_transcript(raw, llm_fn=runaway)
    assert res.used_fallback is True
    assert res.corrected == raw
    assert res.needs_human_review is True


def test_correction_detects_brand_overrides():
    """Long enough raw so the change-ratio stays below the guard and the
    correction is actually applied (not fallback)."""
    def brand_swap(_p: str, text: str) -> tuple[str, dict]:
        return text.replace("유트라 4", "Ultra4[?]"), {"provider": "fake", "model": "bs"}
    raw = (
        "오늘 소개할 제품은 유트라 4의 점프 스타터입니다. 2000암어 피크 전류로 "
        "대형 엔진도 시동 걸 수 있고, 영하 20도에서도 안정적으로 작동합니다. "
        "전용 공기 압축기가 내장되어 타이어 공기압도 충전할 수 있는데요. "
        "차량 트렁크에 하나쯤 구비해 둬야 할 필수 아이템입니다."
    )
    res = tc.correct_transcript(raw, llm_fn=brand_swap)
    assert not res.used_fallback, f"unexpected fallback, ratio={res.ratio:.3f}"
    assert any(b["new"].startswith("Ultra4") for b in res.brand_overrides)
    assert res.needs_human_review is True


# ------------------------------------------------------------------ #
# Product extractor
# ------------------------------------------------------------------ #


def test_split_products_by_markers():
    text = ("첫 번째 제품은 손전등입니다 이 제품은 최고입니다 "
            "두 번째 제품은 인터콤입니다 성능이 좋습니다 "
            "세 번째 제품은 짐벌입니다")
    paras = px.split_products(text)
    assert [p.position for p in paras] == [1, 2, 3]
    assert "손전등" in paras[0].text
    assert "짐벌" in paras[2].text


def test_split_products_with_segments_assigns_timing():
    text = "첫 번째 제품은 손전등 두 번째 제품은 인터콤"
    segs = [
        ts.Segment(0, 10, "첫 번째 제품은 손전등"),
        ts.Segment(10, 20, "두 번째 제품은 인터콤"),
    ]
    paras = px.split_products(text, segments=segs)
    assert paras[0].start_sec == 0
    assert paras[0].end_sec == 10
    assert paras[1].start_sec == 10


def test_validate_persona_label():
    assert px.validate_persona_label("30~50대 차박 캠핑러") is True
    assert px.validate_persona_label("30대 중년") is False
    assert px.validate_persona_label("") is False


def test_persona_dict_load_approved():
    pd = px.PersonaDict.load()
    assert pd.is_approved("30~50대 차박 캠핑러")
    assert not pd.is_approved("99~00대 외계인")


def test_extract_products_returns_multiple_records():
    pilot_corrected = (
        "첫 번째 제품은 손전등입니다 1000루멘 "
        "두 번째 제품은 오토바이 헬멧 카메라 인터콤 "
        "세 번째 제품은 스마트폰 3축 짐벌입니다 "
        "네 번째 제품은 전동 바디 마사지기 "
    )
    outcome = px.extract_products(pilot_corrected, llm_fn=px.fake_keyword_extractor)
    assert len(outcome.products) == 4
    positions = [p["position"] for p in outcome.products]
    assert positions == [1, 2, 3, 4]
    names = [p["product_name"] for p in outcome.products]
    assert all(names)
    for p in outcome.products:
        assert px.validate_persona_label(p["target_persona"]["label"])


def test_extract_products_captures_failures_gracefully():
    # 5th product keyword that the fake extractor won't recognize
    text = (
        "첫 번째 제품은 손전등 "
        "두 번째 제품은 외계인 물건"
    )
    outcome = px.extract_products(text, llm_fn=px.fake_keyword_extractor)
    # product 1 succeeds; product 2 returns missing field → goes to failures
    assert len(outcome.products) == 1
    assert len(outcome.failures) == 1
    assert outcome.failures[0]["position"] == 2


# ------------------------------------------------------------------ #
# LLM logger
# ------------------------------------------------------------------ #


def test_compute_cost_usd_known_model():
    cost = compute_cost_usd("openai", "gpt-4o-mini", 1_000_000, 1_000_000)
    assert cost == pytest.approx(0.15 + 0.60, rel=1e-6)


def test_compute_cost_usd_unknown_model_returns_zero():
    assert compute_cost_usd("unknown", "ghost-1", 1000, 1000) == 0.0


def test_log_llm_call_inserts_row():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    rec = LLMCallRecord(
        service="correct_transcript",
        provider="openai",
        model="gpt-4o-mini",
        prompt_version="correct_transcript@v1",
        input_tokens=1500,
        output_tokens=1400,
        latency_ms=1234,
        related_entity="video:1",
    )
    new_id = log_llm_call(conn, rec)
    row = conn.execute(
        "SELECT service, provider, cost_usd FROM llm_call_logs WHERE id=?",
        (new_id,),
    ).fetchone()
    assert row[0] == "correct_transcript"
    assert row[1] == "openai"
    assert row[2] > 0
