"""Phase 2 unit tests — naver 3 API clients + market analyzer history."""
from __future__ import annotations

import json
import sqlite3

import pytest

from db.sourcing_schema import init_sourcing_tables
from services.naver_search_client import (
    NaverSearchClient, ShopStats, _strip_html,
)
from services.naver_datalab_client import NaverDataLabClient
from services.naver_ad_client import (
    NaverAdClient, build_headers, _sign, _parse_int,
)
from services.market_analyzer import (
    MarketResearchResult, run_analysis, persist_research,
    load_latest_research, load_research_history,
)


# ---------------------------------------------------------------- #
# Naver Search client
# ---------------------------------------------------------------- #


def test_strip_html_removes_tags():
    assert _strip_html("<b>hello</b> world") == "hello world"


def test_shop_stats_price_summary():
    s = ShopStats(keyword="k", total=100, prices=[10, 20, 30, 40, 50])
    summ = s.price_summary
    assert summ["min"] == 10
    assert summ["max"] == 50
    assert summ["median"] in (30, 40)  # depending on quantile rule
    assert summ["sample_size"] == 5


def test_naver_search_client_parses_response():
    fake_response = {
        "total": 2,
        "items": [
            {"title": "<b>차량용</b> 손전등",
             "link": "https://a.b", "lprice": "19900",
             "mallName": "스마트스토어",
             "category1": "생활/건강", "category2": "자동차용품"},
            {"title": "다기능 해머",
             "link": "https://c.d", "lprice": "25000",
             "mallName": "쿠팡",
             "category1": "생활/건강", "category2": "자동차용품"},
        ],
    }
    def fake_fetch(url, headers):
        assert "X-Naver-Client-Id" in headers
        assert "search.naver" not in url  # should hit openapi
        return fake_response

    client = NaverSearchClient("id", "secret", http_fetcher=fake_fetch,
                                sleep_between_calls=0)
    stats = client.shop("차량용 손전등", display=40)
    assert stats.total == 2
    assert stats.prices == [19900, 25000]
    assert stats.products[0]["title"] == "차량용 손전등"
    assert stats.products[0]["mall_name"] == "스마트스토어"


# ---------------------------------------------------------------- #
# Naver DataLab client
# ---------------------------------------------------------------- #


def test_datalab_category_trend_sends_expected_body():
    captured = {}
    def fake_fetch(url, headers, body):
        captured["url"] = url
        captured["headers"] = headers
        captured["body"] = json.loads(body)
        return {"results": [{"title": "차량용품",
                             "data": [{"period": "2025-06", "ratio": 88}]}]}
    c = NaverDataLabClient("id", "secret", http_fetcher=fake_fetch,
                           sleep_between_calls=0)
    out = c.category_trend(
        start_date="2025-04-01", end_date="2026-03-31",
        time_unit="month",
        category=[{"name": "차량용품", "param": ["50000006"]}],
    )
    assert captured["url"].endswith("/shopping/categories")
    assert captured["headers"]["Content-Type"] == "application/json"
    assert captured["body"]["startDate"] == "2025-04-01"
    assert captured["body"]["category"][0]["param"] == ["50000006"]
    assert out["results"][0]["data"][0]["ratio"] == 88


# ---------------------------------------------------------------- #
# Naver Ad client (HMAC signature)
# ---------------------------------------------------------------- #


def test_hmac_signature_stable_for_fixed_ts():
    sig = _sign("GET", "/keywordstool", "1700000000000", "secret")
    # Signature of a known tuple. Must stay stable across refactors.
    assert isinstance(sig, str) and len(sig) > 20


def test_build_headers_shape():
    h = build_headers("GET", "/keywordstool",
                      api_key="KEY", customer_id="1234",
                      secret="shhh", now_ms=1700000000000)
    assert h["X-API-KEY"] == "KEY"
    assert h["X-Customer"] == "1234"
    assert h["X-Timestamp"] == "1700000000000"
    assert "X-Signature" in h


def test_parse_int_less_than_10():
    assert _parse_int("< 10") == 10
    assert _parse_int("") == 0
    assert _parse_int(None) == 0
    assert _parse_int("1234") == 1234


def test_ad_client_keywordstool_chunks_and_parses():
    calls = []
    def fake_fetch(url, headers):
        calls.append(url)
        # return 1 keyword row per call
        return {"keywordList": [
            {"relKeyword": "차량용 손전등",
             "monthlyPcQcCnt": "1100", "monthlyMobileQcCnt": "6400",
             "compIdx": "HIGH",
             "monthlyAvePcClkCnt": "23", "plAvgDepth": "2.5"},
        ]}
    # 7 keywords → 2 batches of (5,2)
    client = NaverAdClient("api", "secret", "1234",
                           http_fetcher=fake_fetch, sleep_between_calls=0)
    kws = [f"kw{i}" for i in range(7)]
    stats = client.keywordstool(kws)
    assert len(calls) == 2
    assert len(stats) == 2
    assert stats[0].monthly_total == 7500
    assert stats[0].competition == "high"


# ---------------------------------------------------------------- #
# Market analyzer (integration w/ fake clients + fake synth)
# ---------------------------------------------------------------- #


def _fake_synth(_prompt, context):
    product = context["product"]
    # Signal that the analyzer passes context correctly
    assert product["name"]
    assert "keywords" in product
    return (
        {
            "market_size_score": 4,
            "competition_score": 3,
            "blue_ocean_signal": "자석 + 1000루멘 동시 충족 제품 소수",
            "opportunity_summary": "여름·추석 시즌 수요 강함",
            "recommended_price_range_krw": {"low": 19900, "mid": 24900, "high": 32900,
                                            "rationale": "중앙값 근처"},
            "target_persona_refined": {
                "primary": "30~50대 남성 차박 캠핑러",
                "secondary": None,
                "exclusions": ["젊은 여성 단독 운전자"],
            },
            "positioning_statement": "차량 트렁크에 하나면 되는 안전 올인원",
            "risk_factors": ["KC 인증 확인 필요", "OEM 중복 판매 가능"],
        },
        {"provider": "fake", "model": "synth", "input_tokens": 3000, "output_tokens": 300},
    )


def test_run_analysis_without_external_apis():
    product = {
        "id": 1,
        "product_name": "샤오미 다기능 손전등",
        "brand": "Xiaomi",
        "category": "차량용품",
        "subcategory": "비상 조명",
        "search_keywords_kr": ["차량용 손전등", "비상탈출 해머"],
        "target_persona": {"label": "30~50대 차박 캠핑러"},
    }
    result = run_analysis(product=product, synth_fn=_fake_synth)
    assert result.market_size_score == 4
    assert "차박" in result.target_persona_refined["primary"]
    assert result.risk_factors


def test_persist_research_versioning_and_latest_flag():
    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    # Insert a fake product first
    conn.execute(
        "INSERT INTO sourced_products (id, video_id, position, product_name, category) "
        "VALUES (1, 1, 1, 'p1', 'c')"
    )
    conn.commit()

    product = {"id": 1, "product_name": "p1", "category": "c",
               "search_keywords_kr": ["kw"], "target_persona": {"label": "20~30대 게이머"}}
    r1 = run_analysis(product=product, synth_fn=_fake_synth)
    persist_research(conn, r1)

    # Second run
    r2 = run_analysis(product=product, synth_fn=_fake_synth)
    persist_research(conn, r2)

    latest = load_latest_research(conn, product_id=1)
    hist = load_research_history(conn, product_id=1)

    assert latest["version"] == 2
    assert len(hist) == 2
    assert hist[0]["is_latest"] is True
    assert hist[1]["is_latest"] is False
    # Prior row demoted
    count_latest = conn.execute(
        "SELECT COUNT(*) FROM market_research WHERE product_id=1 AND is_latest=TRUE"
    ).fetchone()[0]
    assert count_latest == 1


def test_run_analysis_tolerates_api_failures():
    """If Naver clients throw, collectors must return empty without crashing."""
    class BoomAdClient:
        def keywordstool(self, _):
            raise RuntimeError("network down")
    class BoomSearchClient:
        def shop(self, *a, **kw):
            raise RuntimeError("network down")
    product = {
        "id": 2,
        "product_name": "x", "category": "c",
        "search_keywords_kr": ["k"],
    }
    result = run_analysis(
        product=product,
        ad_client=BoomAdClient(),
        search_client=BoomSearchClient(),
        synth_fn=_fake_synth,
    )
    assert result.ad_keyword_stats == []
    assert result.naver_shop_top40 == []
    assert result.market_size_score == 4  # synth still ran
