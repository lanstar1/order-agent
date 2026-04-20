"""FastAPI routes for the sourcing module (order-agent integration).

Wired to the order-agent project conventions:
- `from db.database import get_connection`
- `from security import get_current_user`
- `from services.X import ...`
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from db.database import get_connection, USE_PG
from security import get_current_user

from db.sourcing_schema import init_sourcing_tables
from services.youtube_url import parse_youtube_input
from services import transcript_service as ts
from services import transcript_corrector as tc
from services import product_extractor as px
from services import market_analyzer as ma
from services import marketing_generator as mg
from services import influencer_finder as inf_find
from services import influencer_pricing as pricing
from services import outreach_service as outreach
from services import feedback_service as fb
from services import sourcing_scheduler as sched
from services.llm_logger import LLMCallRecord, log_llm_call


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sourcing", tags=["sourcing"])


def _dialect() -> str:
    return "postgres" if USE_PG else "sqlite"


def _conn():
    """Acquire a DB connection + ensure sourcing tables exist.

    init_sourcing_tables is idempotent (IF NOT EXISTS). Calling it on every
    request is wasteful; we memoize the "already-initialised" flag per
    process.
    """
    conn = get_connection()
    if not getattr(_conn, "_inited", False):
        try:
            init_sourcing_tables(conn, dialect=_dialect())
            _conn._inited = True  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[sourcing] init_sourcing_tables failed: {exc}")
    return conn


# --------------------------------------------------------------- #
# Pydantic bodies
# --------------------------------------------------------------- #


class ChannelCreate(BaseModel):
    url_or_id: str
    category: Optional[str] = None
    polling_mode: str = "auto"


class ChannelPollPeriod(BaseModel):
    start: str
    end: str


class ProductStatusUpdate(BaseModel):
    sourcing_status: str
    sourcing_note: Optional[str] = None


class MarketingRequest(BaseModel):
    kind: str = Field(..., pattern="^(b2c|b2b|influencer)$")


class OutreachStatusUpdate(BaseModel):
    status: str
    note: Optional[str] = None


class PurchaseMark(BaseModel):
    erp_item_code: str


# --------------------------------------------------------------- #
# Channels
# --------------------------------------------------------------- #


@router.post("/channels")
def create_channel(body: ChannelCreate, user=Depends(get_current_user)):
    parsed = parse_youtube_input(body.url_or_id)
    if not parsed:
        raise HTTPException(400, f"지원하지 않는 입력 형식: {body.url_or_id!r}")
    conn = _conn()
    raw_handle = None
    channel_id_val = None
    if parsed.kind == "handle":
        raw_handle = parsed.value
        channel_id_val = parsed.value  # provisional — YouTube API will resolve
    elif parsed.kind == "channel_id":
        channel_id_val = parsed.value
    else:
        channel_id_val = f"__provisional__{parsed.value}"
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO youtube_channels (channel_id, channel_handle, category, polling_mode) VALUES (?, ?, ?, ?)",
        (channel_id_val, raw_handle, body.category, body.polling_mode),
    )
    conn.commit()
    return {"id": cur.lastrowid, "parsed_kind": parsed.kind, "parsed_value": parsed.value}


@router.get("/channels")
def list_channels(user=Depends(get_current_user)):
    conn = _conn()
    rows = conn.execute(
        """SELECT id, channel_id, channel_handle, channel_title, category,
                  polling_mode, enabled, last_polled_at
           FROM youtube_channels WHERE enabled=1 ORDER BY id DESC"""
    ).fetchall()
    cols = ["id", "channel_id", "channel_handle", "channel_title", "category",
            "polling_mode", "enabled", "last_polled_at"]
    return [dict(zip(cols, r)) for r in rows]


@router.delete("/channels/{cid}")
def soft_delete_channel(cid: int, user=Depends(get_current_user)):
    _conn().execute("UPDATE youtube_channels SET enabled=0 WHERE id=?", (cid,))
    _conn().commit()
    return {"ok": True}


@router.post("/channels/{cid}/poll")
def trigger_poll(cid: int, user=Depends(get_current_user)):
    """Queue a poll job — actual YouTube Data API fetch is handled by the
    scheduler loop. This endpoint only flips an internal marker."""
    conn = _conn()
    conn.execute(
        "UPDATE youtube_channels SET last_polled_at=NULL WHERE id=?", (cid,)
    )
    conn.commit()
    return {"ok": True, "message": "다음 스케줄러 틱에 폴링됩니다"}


@router.post("/channels/{cid}/poll-period")
def trigger_poll_period(cid: int, body: ChannelPollPeriod,
                        user=Depends(get_current_user)):
    # Period-polling is implemented by the scheduler; we persist intent in a
    # dedicated column or activity log in production. For now return ack.
    return {"ok": True, "channel_id": cid, "start": body.start, "end": body.end}


# --------------------------------------------------------------- #
# Videos
# --------------------------------------------------------------- #


@router.get("/videos")
def list_videos(channel_id: Optional[int] = None, status: Optional[str] = None,
                user=Depends(get_current_user)):
    conn = _conn()
    sql = ("SELECT id, channel_id, video_id, title, video_type, "
           "processed_status, internal_step, retry_count, error_reason, created_at "
           "FROM youtube_videos WHERE 1=1")
    args: list[Any] = []
    if channel_id:
        sql += " AND channel_id=?"; args.append(channel_id)
    if status:
        sql += " AND processed_status=?"; args.append(status)
    sql += " ORDER BY id DESC LIMIT 200"
    rows = conn.execute(sql, args).fetchall()
    cols = ["id", "channel_id", "video_id", "title", "video_type",
            "processed_status", "internal_step", "retry_count",
            "error_reason", "created_at"]
    return [dict(zip(cols, r)) for r in rows]


# --------------------------------------------------------------- #
# Products
# --------------------------------------------------------------- #


@router.get("/products")
def list_products(user=Depends(get_current_user)):
    conn = _conn()
    rows = conn.execute(
        """SELECT id, product_name, brand, category, subcategory,
                  sourcing_status, target_persona, thumbnail_url, created_at
           FROM sourced_products ORDER BY id DESC LIMIT 200"""
    ).fetchall()
    cols = ["id", "product_name", "brand", "category", "subcategory",
            "sourcing_status", "target_persona", "thumbnail_url", "created_at"]
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        if d["target_persona"]:
            try: d["target_persona"] = json.loads(d["target_persona"])
            except Exception: pass
        out.append(d)
    return out


@router.patch("/products/{pid}")
def update_product(pid: int, body: ProductStatusUpdate,
                   user=Depends(get_current_user)):
    conn = _conn()
    conn.execute(
        "UPDATE sourced_products SET sourcing_status=?, sourcing_note=? WHERE id=?",
        (body.sourcing_status, body.sourcing_note, pid),
    )
    conn.commit()
    return {"ok": True}


@router.post("/products/{pid}/mark-purchased")
def mark_purchased(pid: int, body: PurchaseMark,
                   user=Depends(get_current_user)):
    fb.mark_purchased(_conn(), product_id=pid, erp_item_code=body.erp_item_code)
    return {"ok": True}


# --------------------------------------------------------------- #
# Market research
# --------------------------------------------------------------- #


@router.get("/products/{pid}/market-latest")
def latest_research(pid: int, user=Depends(get_current_user)):
    return ma.load_latest_research(_conn(), product_id=pid) or {}


@router.get("/products/{pid}/market-history")
def history(pid: int, user=Depends(get_current_user)):
    return ma.load_research_history(_conn(), product_id=pid)


# --------------------------------------------------------------- #
# Marketing
# --------------------------------------------------------------- #


@router.get("/products/{pid}/marketing")
def list_assets(pid: int, user=Depends(get_current_user)):
    conn = _conn()
    rows = conn.execute(
        "SELECT id, kind, title, created_at FROM marketing_assets "
        "WHERE product_id=? ORDER BY id DESC", (pid,),
    ).fetchall()
    return [dict(zip(["id", "kind", "title", "created_at"], r)) for r in rows]


@router.get("/marketing/{aid}")
def get_asset(aid: int, user=Depends(get_current_user)):
    conn = _conn()
    row = conn.execute(
        "SELECT id, product_id, kind, title, body_markdown, metadata, prompt_version, created_at "
        "FROM marketing_assets WHERE id=?", (aid,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "자료를 찾을 수 없습니다")
    cols = ["id", "product_id", "kind", "title", "body_markdown",
            "metadata", "prompt_version", "created_at"]
    d = dict(zip(cols, row))
    if d["metadata"]:
        try: d["metadata"] = json.loads(d["metadata"])
        except Exception: pass
    return d


# --------------------------------------------------------------- #
# Influencer matches + outreach drafts
# --------------------------------------------------------------- #


@router.get("/products/{pid}/matches")
def list_matches(pid: int, user=Depends(get_current_user)):
    conn = _conn()
    rows = conn.execute(
        """SELECT m.id, m.influencer_id, m.estimated_quote_krw,
                  m.quality_score, m.match_score, m.is_excluded,
                  i.platform, i.handle, i.display_name,
                  i.follower_count, i.avg_views, i.engagement_rate,
                  i.contact_email
           FROM product_influencer_matches m
           JOIN influencers i ON i.id=m.influencer_id
           WHERE m.product_id=? ORDER BY m.is_excluded, m.quality_score DESC""",
        (pid,),
    ).fetchall()
    cols = ["id", "influencer_id", "estimated_quote_krw", "quality_score",
            "match_score", "is_excluded", "platform", "handle",
            "display_name", "follower_count", "avg_views",
            "engagement_rate", "contact_email"]
    return [dict(zip(cols, r)) for r in rows]


@router.get("/outreach-drafts")
def list_drafts(user=Depends(get_current_user)):
    conn = _conn()
    rows = conn.execute(
        """SELECT id, match_id, channel_kind, offer_kind, subject, message_body,
                  status, copied_at, created_at
           FROM outreach_drafts ORDER BY id DESC LIMIT 200"""
    ).fetchall()
    cols = ["id", "match_id", "channel_kind", "offer_kind", "subject",
            "message_body", "status", "copied_at", "created_at"]
    return [dict(zip(cols, r)) for r in rows]


@router.post("/outreach-drafts/{did}/mark-copied")
def mark_copied(did: int, user=Depends(get_current_user)):
    outreach.mark_copied(_conn(), draft_id=did)
    return {"ok": True}


@router.patch("/outreach-drafts/{did}")
def update_draft_status(did: int, body: OutreachStatusUpdate,
                        user=Depends(get_current_user)):
    outreach.update_status(_conn(), did, body.status, body.note)
    return {"ok": True}


# --------------------------------------------------------------- #
# Dashboard + LLM logs
# --------------------------------------------------------------- #


@router.get("/dashboard")
def dashboard(user=Depends(get_current_user)):
    conn = _conn()
    daily_videos = conn.execute(
        "SELECT COUNT(*) FROM youtube_videos WHERE date(created_at)=date('now')"
    ).fetchone()[0]
    new_products = conn.execute(
        "SELECT COUNT(*) FROM sourced_products WHERE date(created_at)=date('now')"
    ).fetchone()[0]
    pending_videos = conn.execute(
        "SELECT COUNT(*) FROM youtube_videos WHERE processed_status='pending'"
    ).fetchone()[0]
    failed_videos = conn.execute(
        "SELECT COUNT(*) FROM youtube_videos WHERE processed_status='failed'"
    ).fetchone()[0]
    outreach_drafts = conn.execute(
        "SELECT COUNT(*) FROM outreach_drafts WHERE status IN ('draft','copied','sent','replied')"
    ).fetchone()[0]
    return {
        "today_videos": daily_videos,
        "today_products": new_products,
        "pending_videos": pending_videos,
        "failed_videos": failed_videos,
        "active_outreach_drafts": outreach_drafts,
        "hit_rate": fb.sourcing_hit_rate(conn),
        "score_vs_outcome": fb.score_vs_outcome(conn),
    }


@router.get("/llm-logs")
def recent_llm_calls(limit: int = 100, user=Depends(get_current_user)):
    conn = _conn()
    rows = conn.execute(
        """SELECT id, called_at, service, provider, model, prompt_version,
                  input_tokens, output_tokens, latency_ms, success,
                  cost_usd, related_entity
           FROM llm_call_logs ORDER BY id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    cols = ["id", "called_at", "service", "provider", "model",
            "prompt_version", "input_tokens", "output_tokens",
            "latency_ms", "success", "cost_usd", "related_entity"]
    return [dict(zip(cols, r)) for r in rows]
