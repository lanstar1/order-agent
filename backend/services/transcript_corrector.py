"""Two-stage transcript correction (LLM pass + change-ratio guard).

Default provider: OpenAI gpt-4o-mini.
Alternate providers are pluggable via a callable `llm_fn(prompt, text) -> (text, meta)`.

Core guarantees:
- Always returns a non-empty string: falls back to `raw_text` when the LLM
  output changes more than `max_change_ratio` of the original.
- Emits an event payload (`CorrectionResult`) ready for `llm_call_logs`.
"""
from __future__ import annotations

import difflib
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional


PROMPT_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1] / "prompts" / "correct_transcript.txt"
)


@dataclass
class CorrectionResult:
    corrected: str
    ratio: float
    used_fallback: bool
    needs_human_review: bool
    provider: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: int = 0
    prompt_version: str = "correct_transcript@v1"
    error: Optional[str] = None
    brand_overrides: list[dict] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Pure helpers (no network) — used heavily in tests
# --------------------------------------------------------------------------- #


def compute_change_ratio(raw: str, corrected: str) -> float:
    """1 - SequenceMatcher.ratio() on character sequences.

    0.0 = identical, 1.0 = completely different.
    """
    if not raw:
        return 1.0 if corrected else 0.0
    if not corrected:
        return 1.0
    ratio = difflib.SequenceMatcher(a=raw, b=corrected, autojunk=False).ratio()
    return max(0.0, min(1.0, 1.0 - ratio))


def chunk_text(text: str, chunk_chars: int = 4000, overlap_chars: int = 100) -> list[str]:
    """Deterministic char-based chunker with overlap."""
    if len(text) <= chunk_chars:
        return [text]
    chunks: list[str] = []
    i = 0
    while i < len(text):
        chunks.append(text[i : i + chunk_chars])
        if i + chunk_chars >= len(text):
            break
        i += chunk_chars - overlap_chars
    return chunks


def stitch_chunks(chunks: list[str], overlap_chars: int = 100) -> str:
    """Inverse of chunk_text — merges overlapping tails heuristically."""
    if not chunks:
        return ""
    out = chunks[0]
    for nxt in chunks[1:]:
        # find best overlap up to overlap_chars
        max_probe = min(overlap_chars, len(out), len(nxt))
        joined = False
        for k in range(max_probe, 20, -10):
            if out[-k:] == nxt[:k]:
                out = out + nxt[k:]
                joined = True
                break
        if not joined:
            out = out + " " + nxt
    return out


def detect_brand_overrides(raw: str, corrected: str) -> list[dict]:
    """Very lightweight detector: surfaces Korean-to-English brand swaps.

    Finds tokens in `corrected` containing Latin letters that do NOT appear
    verbatim in `raw`. Useful for the human-review queue.
    """
    latin_re = __import__("re").compile(r"[A-Za-z][A-Za-z0-9]{1,20}")
    hits: list[dict] = []
    seen: set[str] = set()
    for token in latin_re.findall(corrected):
        if token in raw or token.lower() in raw.lower() or token in seen:
            continue
        seen.add(token)
        hits.append({"new": token, "source": "corrected"})
    return hits


# --------------------------------------------------------------------------- #
# Provider-agnostic runner
# --------------------------------------------------------------------------- #


def _load_prompt_template() -> str:
    try:
        return PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return (
            "You are a Korean transcript cleanup assistant.\n"
            "Fix obvious typos, punctuation, and drop [음악]/>> markers.\n"
            "Do not change meaning. Mark uncertain brand names as [?]."
        )


LLMCorrectFn = Callable[[str, str], tuple[str, dict]]
"""Signature: (system_prompt, user_text) -> (corrected_text, meta)

`meta` keys expected: provider, model, input_tokens, output_tokens.
"""


def correct_transcript(
    raw_text: str,
    *,
    llm_fn: Optional[LLMCorrectFn] = None,
    max_change_ratio: float = 0.3,
    chunk_chars: int = 4000,
) -> CorrectionResult:
    """Run the two-stage correction.

    If `llm_fn` is None, uses OpenAI gpt-4o-mini via env OPENAI_API_KEY.
    """
    if llm_fn is None:
        llm_fn = _default_openai_llm_fn()

    system_prompt = _load_prompt_template()
    start = time.monotonic()

    try:
        chunks = chunk_text(raw_text, chunk_chars=chunk_chars)
        corrected_chunks: list[str] = []
        total_in = total_out = 0
        provider = model = ""
        for chunk in chunks:
            out, meta = llm_fn(system_prompt, chunk)
            corrected_chunks.append(out)
            total_in += int(meta.get("input_tokens", 0))
            total_out += int(meta.get("output_tokens", 0))
            provider = meta.get("provider", provider)
            model = meta.get("model", model)
        corrected = stitch_chunks(corrected_chunks)
    except Exception as exc:  # noqa: BLE001
        return CorrectionResult(
            corrected=raw_text,
            ratio=0.0,
            used_fallback=True,
            needs_human_review=True,
            provider="",
            model="",
            latency_ms=int((time.monotonic() - start) * 1000),
            error=str(exc),
        )

    ratio = compute_change_ratio(raw_text, corrected)
    used_fallback = ratio > max_change_ratio
    final = raw_text if used_fallback else corrected
    overrides = detect_brand_overrides(raw_text, final)

    return CorrectionResult(
        corrected=final,
        ratio=ratio,
        used_fallback=used_fallback,
        needs_human_review=used_fallback or bool(overrides),
        provider=provider,
        model=model,
        input_tokens=total_in,
        output_tokens=total_out,
        latency_ms=int((time.monotonic() - start) * 1000),
        brand_overrides=overrides,
    )


# --------------------------------------------------------------------------- #
# OpenAI default impl (lazy import)
# --------------------------------------------------------------------------- #


def _default_openai_llm_fn() -> LLMCorrectFn:
    """Return a function that calls OpenAI gpt-4o-mini. Raises at call time
    (not import time) so tests can stay network-free."""

    def _call(system_prompt: str, text: str) -> tuple[str, dict]:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY not set")
        from openai import OpenAI  # type: ignore[import-not-found]

        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.1,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
        )
        content = resp.choices[0].message.content or ""
        usage = getattr(resp, "usage", None)
        meta = {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "input_tokens": getattr(usage, "prompt_tokens", 0) if usage else 0,
            "output_tokens": getattr(usage, "completion_tokens", 0) if usage else 0,
        }
        return content, meta

    return _call


# --------------------------------------------------------------------------- #
# Utility for tests: deterministic fake LLM
# --------------------------------------------------------------------------- #


def fake_rule_based_llm(system_prompt: str, text: str) -> tuple[str, dict]:
    """Stand-in for the real LLM in tests. Applies a small handcrafted
    replacement table. Does NOT read the prompt."""
    replacements = {
        "철루맨": "1,000루멘",
        "3,100m마": "3,100mAh",
        "굉절음": "풍절음",
        "실소파": "실속파",
        "시엔성": "시인성",
        "체련된": "세련된",
        "얘기치": "예기치",
        "금막링": "근막 링",
        "트위원": "2-in-1",
        "진동 면접": "진동 면적",
        "신부 조직": "심부 조직",
        "렌치료": "렌치류",
        "필요도": "피로도",
        "서브오퍼": "서브우퍼",
        "경고한": "견고한",
        "경고함": "견고함",
        "내치 잠금": "래치 잠금",
        "화중": "하중",
        "설교되어": "설계되어",
        "분세력": "분쇄력",
        "보완선": "보안성",
        "인문용": "입문용",
        "육종 스테이니스": "6중 스테인리스",
        "타입 포트": "C타입 포트",
        "고아질": "고화질",
        "키럴리스": "키리스",
        "안정어": "안정화",
        "오인원": "올인원",
        "이원부터": "이완부터",
    }
    result = text
    for bad, good in replacements.items():
        result = result.replace(bad, good)
    # drop music/jumper tokens
    import re as _re
    result = _re.sub(r"\[음악\]|>>", " ", result)
    result = _re.sub(r"\s+", " ", result).strip()
    return result, {
        "provider": "fake",
        "model": "rule-based",
        "input_tokens": len(text),
        "output_tokens": len(result),
    }
