"""Alerts service — detect failures and notify via Slack / email.

Triggers:
- Scheduler job exception
- YouTube Data API quota breach
- LLM call failure rate > 20% in last 24 h
- Marketing asset marked `needs_human_review` with unverifiable claims

Delivery channels:
- Slack webhook (optional)
- Email via ECOUNT SMTP (leverages the existing `ecount-mail-access` skill —
  the calling code in order-agent already knows how to send; we only produce
  the `AlertPayload`).
"""
from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class AlertPayload:
    level: str                         # info / warn / error
    title: str
    summary: str
    context: dict = field(default_factory=dict)
    channel: str = "both"              # slack / email / both

    def as_slack_text(self) -> str:
        icons = {"info": ":information_source:", "warn": ":warning:", "error": ":rotating_light:"}
        i = icons.get(self.level, ":bell:")
        lines = [f"{i} *{self.title}*", self.summary]
        if self.context:
            lines.append("```\n" + json.dumps(self.context, ensure_ascii=False, indent=2) + "\n```")
        return "\n".join(lines)

    def as_email_body(self) -> str:
        lines = [f"[{self.level.upper()}] {self.title}", "", self.summary]
        if self.context:
            lines.extend(["", "컨텍스트:", json.dumps(self.context, ensure_ascii=False, indent=2)])
        return "\n".join(lines)


PosterFn = Callable[[str, dict], None]
"""(url, json_payload) -> None"""


def _default_poster(url: str, payload: dict) -> None:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as _:
        pass


def send_slack(
    alert: AlertPayload, *, webhook_url: Optional[str] = None,
    poster_fn: Optional[PosterFn] = None,
) -> bool:
    url = webhook_url or os.environ.get("SOURCING_SLACK_WEBHOOK")
    if not url:
        return False
    (poster_fn or _default_poster)(url, {"text": alert.as_slack_text()})
    return True


# ---- LLM failure-rate detector ---------------------------------- #


def llm_failure_rate(conn, window_hours: int = 24) -> dict:
    """Return % failed LLM calls over the last N hours."""
    # success 컬럼은 PostgreSQL에선 BOOLEAN, SQLite에선 INTEGER.
    # NOT success 는 둘 다 호환되며 가장 이식성 높음.
    row = conn.execute(
        """SELECT COUNT(*) AS total,
                  SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failed
           FROM llm_call_logs
           WHERE called_at >= datetime('now', ?)""",
        (f"-{window_hours} hours",),
    ).fetchone()
    total = int(row[0] or 0)
    failed = int(row[1] or 0)
    rate = (failed / total) if total else 0.0
    return {"total": total, "failed": failed, "rate": round(rate, 3)}


def maybe_alert_high_llm_failure(
    conn, *, threshold: float = 0.2, min_samples: int = 20,
) -> Optional[AlertPayload]:
    stats = llm_failure_rate(conn)
    if stats["total"] < min_samples:
        return None
    if stats["rate"] < threshold:
        return None
    return AlertPayload(
        level="warn",
        title="LLM 호출 실패율 경고",
        summary=f"최근 24시간 LLM 실패율 {stats['rate']*100:.1f}%",
        context=stats,
    )
