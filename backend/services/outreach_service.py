"""Outreach draft generator — NEVER sends anything.

Provides:
- `generate_draft`: Claude/fake synth
- `build_mailto_url`: RFC 6068 for email drafts
- `copy_payload`: object ready for clipboard-copy UI
- `mark_copied`: DB status transition draft → copied
- `mark_sent_manually`: sent timestamp when user confirms they sent it
"""
from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional


PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "outreach_draft.txt"


@dataclass
class OutreachDraft:
    match_id: int
    channel_kind: str              # email / instagram_dm
    offer_kind: str                # gift / paid
    subject: Optional[str]
    body: str
    product_proposal: dict
    prompt_version: str = "outreach_draft@v1"
    llm_meta: dict = field(default_factory=dict)


DraftFn = Callable[[str, dict], tuple[dict, dict]]


def _load_prompt() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "Draft a polite Korean outreach message."


def generate_draft(
    *,
    match_id: int,
    influencer: dict,
    product: dict,
    offer_kind: str,
    channel_kind: str,
    sender: dict,
    estimated_quote: Optional[dict] = None,
    synth_fn: DraftFn,
) -> OutreachDraft:
    if offer_kind not in ("gift", "paid"):
        raise ValueError("offer_kind must be 'gift' or 'paid'")
    if channel_kind not in ("email", "instagram_dm"):
        raise ValueError("channel_kind must be 'email' or 'instagram_dm'")

    context = {
        "influencer": influencer,
        "product": product,
        "offer_kind": offer_kind,
        "channel_kind": channel_kind,
        "sender": sender,
        "estimated_quote": estimated_quote or {},
    }
    payload, meta = synth_fn(_load_prompt(), context)

    # Defensive normalization
    subject = payload.get("subject") if channel_kind == "email" else None
    body = (payload.get("body") or "").strip()
    product_proposal = payload.get("product_proposal") or {}

    return OutreachDraft(
        match_id=match_id,
        channel_kind=channel_kind,
        offer_kind=offer_kind,
        subject=subject,
        body=body,
        product_proposal=product_proposal,
        llm_meta=meta or {},
    )


# --------------------------------------------------------------- #
# Clipboard / mailto helpers
# --------------------------------------------------------------- #


def build_mailto_url(to: str, subject: str, body: str) -> str:
    """RFC 6068 mailto: URL. Browsers open the default mail client with
    recipient / subject / body prefilled."""
    params = {"subject": subject or "", "body": body or ""}
    q = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    return f"mailto:{urllib.parse.quote(to or '')}?{q}"


def copy_payload(draft: OutreachDraft) -> dict:
    """Return dict the UI puts into `navigator.clipboard.writeText`."""
    if draft.channel_kind == "email":
        text = ""
        if draft.subject:
            text += f"제목: {draft.subject}\n\n"
        text += draft.body
        return {"clipboard_text": text, "kind": "email"}
    # Instagram DM — no subject line; just body
    return {"clipboard_text": draft.body, "kind": "instagram_dm"}


# --------------------------------------------------------------- #
# DB persistence + status transitions (no sending)
# --------------------------------------------------------------- #


def persist_draft(conn, draft: OutreachDraft) -> int:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO outreach_drafts
           (match_id, channel_kind, offer_kind, subject, message_body,
            product_proposal, status, prompt_version)
           VALUES (?, ?, ?, ?, ?, ?, 'draft', ?)""",
        (
            draft.match_id, draft.channel_kind, draft.offer_kind,
            draft.subject, draft.body,
            json.dumps(draft.product_proposal, ensure_ascii=False),
            draft.prompt_version,
        ),
    )
    conn.commit()
    return cur.lastrowid


def mark_copied(conn, draft_id: int) -> None:
    conn.execute(
        "UPDATE outreach_drafts SET status='copied', copied_at=CURRENT_TIMESTAMP "
        "WHERE id=?", (draft_id,),
    )
    conn.commit()


def mark_sent_manually(conn, draft_id: int, note: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE outreach_drafts SET status='sent', manual_response_note=? "
        "WHERE id=?", (note, draft_id),
    )
    conn.commit()


def update_status(conn, draft_id: int, status: str, note: Optional[str] = None) -> None:
    allowed = {"draft", "copied", "sent", "replied", "agreed",
               "published", "settled", "declined"}
    if status not in allowed:
        raise ValueError(f"invalid outreach status: {status}")
    conn.execute(
        "UPDATE outreach_drafts SET status=?, manual_response_note=COALESCE(?, manual_response_note) "
        "WHERE id=?", (status, note, draft_id),
    )
    conn.commit()
