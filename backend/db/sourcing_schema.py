"""Sourcing module DB schema.

Adds 10 new tables to the existing order-agent database. Supports both SQLite
(development/tests) and PostgreSQL (Render production).

Usage
-----
from db.sourcing_schema import init_sourcing_tables

with get_connection() as conn:
    init_sourcing_tables(conn, dialect="sqlite")
"""
from __future__ import annotations

from typing import Literal

Dialect = Literal["sqlite", "postgres"]


def _type_mapping(dialect: Dialect) -> dict[str, str]:
    """Translate generic types to dialect-specific SQL types."""
    if dialect == "postgres":
        return {
            "SERIAL": "SERIAL",
            "JSON": "JSONB",
            "TS_DEFAULT": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "BOOL_TRUE": "BOOLEAN DEFAULT TRUE",
            "BOOL_FALSE": "BOOLEAN DEFAULT FALSE",
        }
    return {
        "SERIAL": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "JSON": "TEXT",  # sqlite stores JSON as TEXT
        "TS_DEFAULT": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "BOOL_TRUE": "INTEGER DEFAULT 1",
        "BOOL_FALSE": "INTEGER DEFAULT 0",
    }


def sourcing_ddl(dialect: Dialect = "sqlite") -> list[str]:
    """Return the list of CREATE TABLE statements."""
    t = _type_mapping(dialect)
    serial = t["SERIAL"]
    # SQLite requires `INTEGER PRIMARY KEY AUTOINCREMENT` to stand alone;
    # PostgreSQL allows `id SERIAL PRIMARY KEY`.
    if dialect == "postgres":
        pk = f"id {serial} PRIMARY KEY"
    else:
        pk = f"id {serial}"
    jsn = t["JSON"]
    ts = t["TS_DEFAULT"]
    btrue = t["BOOL_TRUE"]
    bfalse = t["BOOL_FALSE"]

    return [
        # ① 채널
        f"""CREATE TABLE IF NOT EXISTS youtube_channels (
            {pk},
            channel_id TEXT UNIQUE NOT NULL,
            channel_handle TEXT,
            channel_title TEXT,
            subscriber_count INTEGER,
            category TEXT,
            polling_mode TEXT DEFAULT 'auto',
            last_polled_at TIMESTAMP,
            enabled {btrue},
            created_at {ts}
        )""",
        # ② 영상
        f"""CREATE TABLE IF NOT EXISTS youtube_videos (
            {pk},
            channel_id INTEGER,
            video_id TEXT UNIQUE NOT NULL,
            title TEXT,
            published_at TIMESTAMP,
            duration_seconds INTEGER,
            video_type TEXT DEFAULT 'normal',
            thumbnail_url TEXT,
            transcript_raw TEXT,
            transcript_corrected TEXT,
            transcript_segments {jsn},
            correction_model TEXT,
            correction_tokens INTEGER,
            correction_ratio REAL,
            needs_human_review {bfalse},
            processed_status TEXT DEFAULT 'pending',
            internal_step TEXT,
            retry_count INTEGER DEFAULT 0,
            next_retry_at TIMESTAMP,
            error_reason TEXT,
            processed_at TIMESTAMP,
            created_at {ts},
            FOREIGN KEY (channel_id) REFERENCES youtube_channels(id)
        )""",
        # ③ 제품
        f"""CREATE TABLE IF NOT EXISTS sourced_products (
            {pk},
            video_id INTEGER,
            position INTEGER,
            product_name TEXT NOT NULL,
            brand TEXT,
            brand_confidence TEXT,
            category TEXT,
            subcategory TEXT,
            key_features {jsn},
            specs {jsn},
            price_range_usd {jsn},
            target_use_case {jsn},
            search_keywords_kr {jsn},
            target_persona {jsn},
            start_sec INTEGER,
            end_sec INTEGER,
            thumbnail_url TEXT,
            sourcing_status TEXT DEFAULT 'new',
            sourcing_note TEXT,
            purchased_at TIMESTAMP,
            listed_at TIMESTAMP,
            erp_item_code TEXT,
            sales_count_30d INTEGER DEFAULT 0,
            revenue_krw_30d INTEGER DEFAULT 0,
            return_rate_30d REAL,
            created_at {ts},
            FOREIGN KEY (video_id) REFERENCES youtube_videos(id) ON DELETE CASCADE
        )""",
        # ④ 시장성 (이력 관리)
        f"""CREATE TABLE IF NOT EXISTS market_research (
            {pk},
            product_id INTEGER,
            version INTEGER DEFAULT 1,
            is_latest {btrue},
            ad_keyword_stats {jsn},
            datalab_category_trend {jsn},
            naver_shop_top40 {jsn},
            market_size_score INTEGER,
            competition_score INTEGER,
            blue_ocean_signal TEXT,
            opportunity_summary TEXT,
            recommended_price_range_krw {jsn},
            target_persona_refined {jsn},
            positioning_statement TEXT,
            risk_factors {jsn},
            prompt_version TEXT,
            llm_raw_output TEXT,
            created_at {ts},
            UNIQUE (product_id, version),
            FOREIGN KEY (product_id) REFERENCES sourced_products(id) ON DELETE SET NULL
        )""",
        # ⑤ 마케팅 자료
        f"""CREATE TABLE IF NOT EXISTS marketing_assets (
            {pk},
            product_id INTEGER,
            kind TEXT NOT NULL,
            title TEXT,
            body_markdown TEXT,
            metadata {jsn},
            prompt_version TEXT,
            created_at {ts},
            FOREIGN KEY (product_id) REFERENCES sourced_products(id) ON DELETE SET NULL
        )""",
        # ⑥ 인플루언서 마스터
        f"""CREATE TABLE IF NOT EXISTS influencers (
            {pk},
            platform TEXT NOT NULL,
            handle TEXT NOT NULL,
            profile_url TEXT NOT NULL,
            display_name TEXT,
            follower_count INTEGER,
            avg_views INTEGER,
            engagement_rate REAL,
            main_categories {jsn},
            contact_email TEXT,
            actual_quotes {jsn},
            last_metrics_update TIMESTAMP,
            created_at {ts},
            UNIQUE (platform, handle)
        )""",
        # ⑦ 제품-인플루언서 매칭
        f"""CREATE TABLE IF NOT EXISTS product_influencer_matches (
            {pk},
            product_id INTEGER,
            influencer_id INTEGER,
            estimated_quote_krw INTEGER,
            quote_breakdown {jsn},
            quality_score INTEGER,
            match_score REAL,
            is_excluded {bfalse},
            exclusion_reason TEXT,
            created_at {ts},
            UNIQUE (product_id, influencer_id),
            FOREIGN KEY (product_id) REFERENCES sourced_products(id) ON DELETE CASCADE,
            FOREIGN KEY (influencer_id) REFERENCES influencers(id) ON DELETE CASCADE
        )""",
        # ⑧ 컨택 초안 (발송 없음)
        f"""CREATE TABLE IF NOT EXISTS outreach_drafts (
            {pk},
            match_id INTEGER,
            channel_kind TEXT NOT NULL,
            offer_kind TEXT NOT NULL,
            subject TEXT,
            message_body TEXT,
            product_proposal {jsn},
            status TEXT DEFAULT 'draft',
            copied_at TIMESTAMP,
            manual_response_note TEXT,
            prompt_version TEXT,
            created_at {ts},
            FOREIGN KEY (match_id) REFERENCES product_influencer_matches(id) ON DELETE CASCADE
        )""",
        # ⑨ 페르소나 라벨 화이트리스트
        f"""CREATE TABLE IF NOT EXISTS persona_labels (
            {pk},
            label TEXT UNIQUE NOT NULL,
            age_min INTEGER,
            age_max INTEGER,
            gender TEXT,
            lifestyle_tags {jsn},
            status TEXT DEFAULT 'pending',
            approved_by TEXT,
            approved_at TIMESTAMP,
            created_at {ts}
        )""",
        # ⑩ LLM 호출 로그
        f"""CREATE TABLE IF NOT EXISTS llm_call_logs (
            {pk},
            called_at {ts},
            service TEXT,
            provider TEXT,
            model TEXT,
            prompt_version TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            latency_ms INTEGER,
            success {btrue},
            error_message TEXT,
            related_entity TEXT,
            cost_usd REAL
        )""",
    ]


def init_sourcing_tables(conn, dialect: Dialect = "sqlite") -> None:
    """Execute every CREATE TABLE statement. Idempotent."""
    cur = conn.cursor()
    for ddl in sourcing_ddl(dialect):
        cur.execute(ddl)
    conn.commit()
