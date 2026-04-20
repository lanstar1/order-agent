"""Market-research orchestration + history management.

Pipeline per product:
1. Naver keyword-stats (search-ads API): monthly volumes + competition
2. Naver Search (shop): top-40 products, price distribution
3. Naver DataLab shopping-insight: category trend + demographics
4. Claude synthesis: market_size_score, competition_score, blue_ocean_signal,
   recommended_price_range_krw, target_persona_refined, risk_factors
5. Persist as a new row in `market_research` with version = prev.max + 1.
   Previous rows flipped to is_latest=false.

All external APIs are optional: set them to None to skip. The Claude call is
the one thing that must be provided (or replaced by `fake_synth_fn` in tests).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Iterable

from .naver_ad_client import NaverAdClient, KeywordStat
from .naver_datalab_client import NaverDataLabClient
from .naver_search_client import NaverSearchClient, ShopStats


PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "analyze_market.txt"


@dataclass
class MarketResearchResult:
    product_id: int
    version: int
    ad_keyword_stats: list[dict] = field(default_factory=list)
    datalab_category_trend: dict = field(default_factory=dict)
    naver_shop_top40: list[dict] = field(default_factory=list)
    market_size_score: int = 0
    competition_score: int = 0
    blue_ocean_signal: str = ""
    opportunity_summary: str = ""
    recommended_price_range_krw: dict = field(default_factory=dict)
    target_persona_refined: dict = field(default_factory=dict)
    positioning_statement: str = ""
    risk_factors: list[str] = field(default_factory=list)
    prompt_version: str = "analyze_market@v1"
    llm_raw_output: str = ""
    llm_meta: dict = field(default_factory=dict)


SynthFn = Callable[[str, dict], tuple[dict, dict]]
"""(system_prompt, input_context) -> (synthesis_dict, llm_meta)"""


# --------------------------------------------------------------------------- #
# Data collectors (robust to None clients)
# --------------------------------------------------------------------------- #


def collect_ad_stats(
    ad_client: Optional[NaverAdClient], keywords: list[str]
) -> list[KeywordStat]:
    if not ad_client or not keywords:
        return []
    try:
        return ad_client.keywordstool(keywords)
    except Exception:
        return []


def collect_shop_stats(
    search_client: Optional[NaverSearchClient], primary_keyword: str
) -> Optional[ShopStats]:
    if not search_client or not primary_keyword:
        return None
    try:
        return search_client.shop(primary_keyword, display=40)
    except Exception:
        return None


def collect_datalab_trend(
    datalab_client: Optional[NaverDataLabClient],
    *,
    category_param: Optional[list[str]],
    category_name: str,
    start_date: str,
    end_date: str,
) -> dict:
    if not datalab_client or not category_param:
        return {}
    try:
        return datalab_client.category_trend(
            start_date=start_date,
            end_date=end_date,
            time_unit="month",
            category=[{"name": category_name, "param": category_param}],
        )
    except Exception:
        return {}


# --------------------------------------------------------------------------- #
# Synthesis (Claude or fake)
# --------------------------------------------------------------------------- #


def _load_prompt() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return (
            "Return a JSON object with market_size_score, competition_score, "
            "blue_ocean_signal, opportunity_summary, recommended_price_range_krw, "
            "target_persona_refined, positioning_statement, risk_factors."
        )


def run_analysis(
    *,
    product: dict,
    ad_client: Optional[NaverAdClient] = None,
    search_client: Optional[NaverSearchClient] = None,
    datalab_client: Optional[NaverDataLabClient] = None,
    synth_fn: SynthFn,
    datalab_category_param: Optional[list[str]] = None,
    datalab_range: tuple[str, str] = ("2025-04-01", "2026-03-31"),
) -> MarketResearchResult:
    keywords = (product.get("search_keywords_kr") or [])[:5]
    ad_stats = collect_ad_stats(ad_client, keywords)
    shop_stats = collect_shop_stats(search_client, keywords[0] if keywords else "")
    datalab = collect_datalab_trend(
        datalab_client,
        category_param=datalab_category_param,
        category_name=product.get("category", ""),
        start_date=datalab_range[0],
        end_date=datalab_range[1],
    )

    system_prompt = _load_prompt()
    context = {
        "product": {
            "name": product.get("product_name"),
            "brand": product.get("brand"),
            "category": product.get("category"),
            "subcategory": product.get("subcategory"),
            "target_persona": product.get("target_persona"),
            "keywords": keywords,
        },
        "naver_ad_stats": [_stat_to_dict(s) for s in ad_stats],
        "naver_shop_summary": shop_stats.price_summary if shop_stats else {},
        "naver_shop_top_samples":
            shop_stats.products[:5] if shop_stats else [],
        "datalab_trend": datalab,
    }

    synth, meta = synth_fn(system_prompt, context)

    return MarketResearchResult(
        product_id=product.get("id", 0),
        version=0,  # filled in by persist_research
        ad_keyword_stats=[_stat_to_dict(s) for s in ad_stats],
        datalab_category_trend=datalab,
        naver_shop_top40=(shop_stats.products if shop_stats else []),
        market_size_score=int(synth.get("market_size_score", 0) or 0),
        competition_score=int(synth.get("competition_score", 0) or 0),
        blue_ocean_signal=str(synth.get("blue_ocean_signal", "") or ""),
        opportunity_summary=str(synth.get("opportunity_summary", "") or ""),
        recommended_price_range_krw=synth.get("recommended_price_range_krw") or {},
        target_persona_refined=synth.get("target_persona_refined") or {},
        positioning_statement=str(synth.get("positioning_statement", "") or ""),
        risk_factors=list(synth.get("risk_factors") or []),
        llm_raw_output=json.dumps(synth, ensure_ascii=False),
        llm_meta=meta or {},
    )


def _stat_to_dict(s: KeywordStat) -> dict:
    return {
        "keyword": s.keyword,
        "monthly_pc": s.monthly_pc,
        "monthly_mobile": s.monthly_mobile,
        "monthly_total": s.monthly_total,
        "competition": s.competition,
    }


# --------------------------------------------------------------------------- #
# History persistence (version bumping + latest flag)
# --------------------------------------------------------------------------- #


def persist_research(conn, result: MarketResearchResult) -> int:
    """Insert the result as a new row, flipping previous rows of the same
    product to is_latest=0. Returns the new row id.

    Uses ``conn.execute()`` (not cursor) per order-agent convention so the
    PostgreSQL wrapper applies ``_sql_to_pg`` translation.
    """
    # Find max version for this product
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) FROM market_research WHERE product_id=?",
        (result.product_id,),
    ).fetchone()
    prev_max = int(row[0] if row else 0)
    new_version = prev_max + 1
    # Flip is_latest=FALSE on previous rows (TRUE/FALSE 리터럴로 SQLite/PG 양쪽 호환)
    conn.execute(
        "UPDATE market_research SET is_latest=FALSE WHERE product_id=?",
        (result.product_id,),
    )
    cur = conn.execute(
        """INSERT INTO market_research (
            product_id, version, is_latest,
            ad_keyword_stats, datalab_category_trend, naver_shop_top40,
            market_size_score, competition_score,
            blue_ocean_signal, opportunity_summary,
            recommended_price_range_krw, target_persona_refined,
            positioning_statement, risk_factors,
            prompt_version, llm_raw_output
        ) VALUES (?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            result.product_id,
            new_version,
            json.dumps(result.ad_keyword_stats, ensure_ascii=False),
            json.dumps(result.datalab_category_trend, ensure_ascii=False),
            json.dumps(result.naver_shop_top40, ensure_ascii=False),
            result.market_size_score,
            result.competition_score,
            result.blue_ocean_signal,
            result.opportunity_summary,
            json.dumps(result.recommended_price_range_krw, ensure_ascii=False),
            json.dumps(result.target_persona_refined, ensure_ascii=False),
            result.positioning_statement,
            json.dumps(result.risk_factors, ensure_ascii=False),
            result.prompt_version,
            result.llm_raw_output,
        ),
    )
    conn.commit()
    result.version = new_version
    return cur.lastrowid


def _loads_any(value, default):
    """SQLite(TEXT) vs PostgreSQL(JSONB) 양쪽 호환 JSON 파싱.

    - PostgreSQL JSONB: psycopg2 가 이미 dict/list 로 반환 → 그대로 사용
    - SQLite TEXT: 문자열 → json.loads
    - NULL / 빈 값 → default
    """
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def load_latest_research(conn, product_id: int) -> Optional[dict]:
    row = conn.execute(
        """SELECT id, version, market_size_score, competition_score,
                  blue_ocean_signal, opportunity_summary,
                  recommended_price_range_krw, risk_factors, created_at
           FROM market_research
           WHERE product_id=? AND is_latest=TRUE""",
        (product_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "version": row[1],
        "market_size_score": row[2],
        "competition_score": row[3],
        "blue_ocean_signal": row[4],
        "opportunity_summary": row[5],
        "recommended_price_range_krw": _loads_any(row[6], {}),
        "risk_factors": _loads_any(row[7], []),
        "created_at": row[8],
    }


def load_research_history(conn, product_id: int) -> list[dict]:
    rows = conn.execute(
        """SELECT version, market_size_score, competition_score, is_latest, created_at
           FROM market_research WHERE product_id=? ORDER BY version DESC""",
        (product_id,),
    ).fetchall()
    return [
        {
            "version": r[0],
            "market_size_score": r[1],
            "competition_score": r[2],
            "is_latest": bool(r[3]),
            "created_at": r[4],
        }
        for r in rows
    ]
