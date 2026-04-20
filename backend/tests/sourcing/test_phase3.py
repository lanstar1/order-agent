"""Phase 3 — marketing asset generator tests."""
from __future__ import annotations

import json
import sqlite3

import pytest

from db.sourcing_schema import init_sourcing_tables
from services import marketing_generator as mg


SAMPLE_PRODUCT = {
    "id": 1,
    "product_name": "샤오미 다기능 손전등",
    "brand": "Xiaomi",
    "category": "차량용품",
    "key_features": ["1000루멘", "안전벨트 커터", "자석 부착"],
    "specs": {"luminance_lm": 1000, "battery_mah": 3100},
    "target_persona": {"label": "30~50대 차박 캠핑러"},
}
SAMPLE_MARKET = {
    "recommended_price_range_krw": {"low": 19900, "mid": 24900, "high": 32900},
    "opportunity_summary": "여름·추석 시즌 수요 강함",
    "positioning_statement": "차량 트렁크에 하나면 되는 안전 올인원",
}


# --------------------------------------------------------------- #
# Claim guard
# --------------------------------------------------------------- #


def test_scan_forbidden_claims_flags_unbacked_claim():
    text = "본 제품은 KC 인증 완료, IP54 방수 등급, 12개월 무상 A/S 제공"
    hits = mg.scan_forbidden_claims(text, allowed_specs={})
    assert any("KC" in h for h in hits)
    assert any("IP" in h for h in hits)
    assert any("A/S" in h or "AS" in h for h in hits)


def test_scrub_forbidden_claims_replaces_text():
    text = "KC 인증 완료 제품이며 IP54 방수에 12개월 무상 A/S"
    scrubbed, hits = mg.scrub_forbidden_claims(text, allowed_specs={})
    assert "KC 인증 완료" not in scrubbed
    assert "[확인 필요]" in scrubbed
    assert len(hits) >= 3


def test_scrub_keeps_backed_claim():
    # If "KC 인증 완료" appears in specs, don't scrub it.
    text = "KC 인증 완료 제품"
    scrubbed, hits = mg.scrub_forbidden_claims(
        text, allowed_specs={"cert": "KC 인증 완료"}
    )
    assert scrubbed == text
    assert hits == []


# --------------------------------------------------------------- #
# B2C generator
# --------------------------------------------------------------- #


def _fake_b2c_synth(_prompt, ctx):
    # Return minimally-valid payload with intentional bad claims to test scrub
    return (
        {
            "title": "차량 트렁크 1개 필수템 — 손전등",
            "hero_headline": "KC 인증 완료 1000루멘 손전등",
            "painpoint_scenarios": ["시골길 펑크", "사고 탈출", "캠핑 야간"],
            "selling_points": [
                {"problem": "약한 불빛", "solution": "1000루멘",
                 "benefit": "멀리 보임", "visual_hint": "야간 비교 GIF"},
                {"problem": "벨트 걸림", "solution": "세라믹 커터",
                 "benefit": "3초 탈출", "visual_hint": "슬로우모션"},
                {"problem": "양손 점유", "solution": "자석 부착",
                 "benefit": "핸즈프리", "visual_hint": "정비 사진"},
                {"problem": "짧은 배터리", "solution": "3100mAh",
                 "benefit": "90시간", "visual_hint": "타임랩스"},
                {"problem": "2차 사고", "solution": "적색 경고등",
                 "benefit": "피탐지", "visual_hint": "야간 경고"},
            ],
            "faq": {
                "defensive": [{"q": f"Q{i}", "a": f"A{i}"} for i in range(4)],
                "conversion": [{"q": f"Q{i}", "a": f"A{i}"} for i in range(4)],
            },
            "seo_keywords": {
                "main": ["차량용 LED 손전등", "자동차 비상등", "차박 손전등"],
                "sub": [f"kw{i}" for i in range(10)],
                "hashtags": ["#차박", "#캠핑필수"],
            },
            "bundle_proposals": [
                {"tier": "bait",      "price_krw": 19900, "spec": "단품", "angle": "최저가"},
                {"tier": "main",      "price_krw": 35900, "spec": "2개 세트", "angle": "10% 할인"},
                {"tier": "crosssell", "price_krw": 42900, "spec": "세트+파우치", "angle": "풀세트"},
            ],
        },
        {"provider": "fake", "model": "b2c", "input_tokens": 1000, "output_tokens": 1200},
    )


def test_generate_asset_b2c_scrubs_unverified_kc_claim():
    asset = mg.generate_asset(
        "b2c", product=SAMPLE_PRODUCT, market=SAMPLE_MARKET, synth_fn=_fake_b2c_synth
    )
    assert "KC 인증 완료" not in asset.body_markdown
    assert "[확인 필요]" in asset.body_markdown
    assert asset.needs_human_review is True
    assert any("scrubbed" in r for r in asset.review_reasons)


def test_generate_asset_b2c_bundle_price_guard():
    def bad_bundle_synth(_p, ctx):
        payload, meta = _fake_b2c_synth(_p, ctx)
        # crosssell priced lower than recommended high → must trigger issue
        payload["bundle_proposals"][2]["price_krw"] = 25000
        return payload, meta

    asset = mg.generate_asset(
        "b2c", product=SAMPLE_PRODUCT, market=SAMPLE_MARKET, synth_fn=bad_bundle_synth
    )
    assert asset.needs_human_review is True
    assert any("crosssell" in r for r in asset.review_reasons)


# --------------------------------------------------------------- #
# B2B generator
# --------------------------------------------------------------- #


def _fake_b2b_synth(_prompt, ctx):
    return (
        {
            "one_pager": {
                "product_overview": "차량 비상용 5-in-1 손전등",
                "specs_table": [
                    {"label": "Luminance", "value": "1000 lm"},
                    {"label": "Battery",   "value": "3100 mAh"},
                ],
                "supply_terms": {
                    "msrp_krw": 29900, "wholesale_krw": 13500,
                    "margin_pct_min": 30, "margin_pct_max": 35,
                    "moq": 50, "open_window": "3개월 한시 독점",
                    "settlement": "월 정산",
                },
                "promotion_opening": ["마케팅비 100만원 지원", "샘플 5개 무상"],
                "sales_rationale": [{"metric": "영상 조회수", "value": "45만"}],
            },
            "cold_mail": {
                "subject": "[제안] 마진 30% 보장, 30~50대 캠핑러 타깃 손전등 입점 제안 (MOQ 50)",
                "body": "안녕하세요, (주)랜스타 신제품 소싱팀입니다. ..."
            },
        },
        {"provider": "fake", "model": "b2b", "input_tokens": 1500, "output_tokens": 1200},
    )


def test_generate_asset_b2b_has_expected_sections():
    asset = mg.generate_asset(
        "b2b", product=SAMPLE_PRODUCT, market=SAMPLE_MARKET, synth_fn=_fake_b2b_synth
    )
    assert "공급 조건" in asset.body_markdown
    assert "MSRP" in asset.body_markdown
    assert asset.title.startswith("[제안]")


# --------------------------------------------------------------- #
# Influencer generator
# --------------------------------------------------------------- #


def _fake_influencer_synth(_prompt, ctx):
    return (
        {
            "channel_mix": {"mega_macro_ratio": 1, "micro_ratio": 3, "nano_ratio": 6,
                             "notes": "마이크로 비중 높게"},
            "format_guide": {
                "youtube": "스펙 심층 리뷰 6~10분",
                "shortform": "언박싱 + 전후 비교 7~15초",
                "blog": "2주 내돈내산",
            },
            "viral_assets": {
                "shortform_script_30s": {
                    "hook_0_3s": "스마트폰 플래시로는 안 됩니다",
                    "problem_3_10s": "5분 만에 방전",
                    "solution_10_23s": "1000루멘 자석 부착",
                    "cta_23_30s": "2만 9천원 링크 확인",
                },
                "cardnews_6": [
                    {"idx": i, "copy": f"cut{i}", "visual": f"visual{i}"}
                    for i in range(1, 7)
                ],
                "community_hooks": {
                    "mom_cafe":  "애들 태우고 외곽 갈 때 불안해요",
                    "boba_dream": "차박 선배님들 Q",
                    "dc_gallery": "3만원짜리 손전등 ㅋㅋ",
                },
            },
            "ad_disclosure_notice": "협찬 시 #광고 표기 필수",
        },
        {"provider": "fake", "model": "inf", "input_tokens": 1200, "output_tokens": 1400},
    )


def test_generate_asset_influencer_renders_all_sections():
    asset = mg.generate_asset(
        "influencer", product=SAMPLE_PRODUCT, market=SAMPLE_MARKET,
        synth_fn=_fake_influencer_synth,
    )
    assert "채널 믹스" in asset.body_markdown
    assert "30초 숏폼 스크립트" in asset.body_markdown
    assert "카드뉴스" in asset.body_markdown


# --------------------------------------------------------------- #
# generate_all_kinds + persistence
# --------------------------------------------------------------- #


def test_generate_all_kinds_returns_3_assets():
    def route(prompt, ctx):
        # Route by inspecting the prompt header words
        if "B2C" in prompt or "bundle_proposals" in prompt:
            return _fake_b2c_synth(prompt, ctx)
        if "one_pager" in prompt:
            return _fake_b2b_synth(prompt, ctx)
        return _fake_influencer_synth(prompt, ctx)
    assets = mg.generate_all_kinds(
        product=SAMPLE_PRODUCT, market=SAMPLE_MARKET, synth_fn=route
    )
    assert {a.kind for a in assets} == {"b2c", "b2b", "influencer"}
    for a in assets:
        assert a.body_markdown


def test_persist_asset_inserts_row():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute(
        "INSERT INTO sourced_products (id, video_id, position, product_name, category) "
        "VALUES (1, 1, 1, 'p', 'c')"
    )
    conn.commit()
    asset = mg.generate_asset(
        "b2c", product={**SAMPLE_PRODUCT, "id": 1},
        market=SAMPLE_MARKET, synth_fn=_fake_b2c_synth
    )
    new_id = mg.persist_asset(conn, product_id=1, asset=asset)
    row = conn.execute(
        "SELECT kind, title, prompt_version FROM marketing_assets WHERE id=?",
        (new_id,),
    ).fetchone()
    assert row[0] == "b2c"
    assert row[2] == "marketing_b2c@v1"
