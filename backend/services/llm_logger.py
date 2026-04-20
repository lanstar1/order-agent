"""Unified LLM call logger — writes rows into `llm_call_logs`.

Cost table is approximate; keep the numbers here so billing surprises appear
in one commit instead of scattered across callers.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


COST_PER_1M_TOKENS_USD = {
    ("openai", "gpt-4o-mini"): {"input": 0.15, "output": 0.60},
    ("openai", "gpt-4o"): {"input": 2.50, "output": 10.00},
    ("anthropic", "claude-haiku-4.5"): {"input": 0.80, "output": 4.00},
    ("anthropic", "claude-sonnet-4.6"): {"input": 3.00, "output": 15.00},
    ("google", "gemini-1.5-flash"): {"input": 0.075, "output": 0.30},
    ("fake", "rule-based"): {"input": 0.0, "output": 0.0},
    ("fake", "kw"): {"input": 0.0, "output": 0.0},
}


@dataclass
class LLMCallRecord:
    service: str
    provider: str
    model: str
    prompt_version: str
    input_tokens: int
    output_tokens: int
    latency_ms: int
    success: bool = True
    error_message: Optional[str] = None
    related_entity: Optional[str] = None
    cost_usd: float = 0.0


def compute_cost_usd(provider: str, model: str, input_tokens: int, output_tokens: int) -> float:
    table = COST_PER_1M_TOKENS_USD.get((provider.lower(), model.lower()))
    if not table:
        return 0.0
    return (
        input_tokens * table["input"] / 1_000_000.0
        + output_tokens * table["output"] / 1_000_000.0
    )


def log_llm_call(conn, record: LLMCallRecord) -> int:
    """Insert a row into llm_call_logs. Returns new id."""
    if record.cost_usd == 0.0:
        record.cost_usd = compute_cost_usd(
            record.provider, record.model, record.input_tokens, record.output_tokens
        )
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO llm_call_logs (
            service, provider, model, prompt_version,
            input_tokens, output_tokens, latency_ms,
            success, error_message, related_entity, cost_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            record.service,
            record.provider,
            record.model,
            record.prompt_version,
            record.input_tokens,
            record.output_tokens,
            record.latency_ms,
            1 if record.success else 0,
            record.error_message,
            record.related_entity,
            record.cost_usd,
        ),
    )
    conn.commit()
    return cur.lastrowid
