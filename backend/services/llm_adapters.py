"""실제 LLM API 호출 어댑터 — sourcing 파이프라인 전용.

각 services/* 모듈은 fake `llm_fn` 시그니처로 테스트 가능하게 설계됨.
이 파일은 그 시그니처를 만족하는 실제 OpenAI / Anthropic 어댑터를 제공.

시그니처 4종 (전부 `(system_prompt, user_input) -> (result, meta)` 형태):
- ``openai_correct_fn`` → transcript_corrector.LLMCorrectFn: user=str → result=str
- ``claude_extract_fn`` → product_extractor.ExtractLLMFn:  user=str → result=dict
- ``claude_synth_fn``   → market_analyzer.SynthFn:          user=dict → result=dict
- ``claude_marketing_fn`` → marketing_generator.MarketingFn: user=dict → result=dict
- ``claude_outreach_fn`` → outreach_service.DraftFn:        user=dict → result=dict
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────── #
# 공용 유틸
# ───────────────────────────────────────────────────────────── #


class LLMAdapterError(RuntimeError):
    pass


def _require_env(var: str) -> str:
    v = os.environ.get(var, "").strip()
    if not v:
        raise LLMAdapterError(f"{var} 환경변수가 설정되지 않았습니다.")
    return v


def _parse_json_loose(text: str) -> dict:
    """LLM 응답에서 JSON 객체를 추출한다.

    - ```json ... ``` 블록에 감싸진 경우
    - 앞뒤에 텍스트가 붙은 경우
    - 중괄호로 시작하는 첫 JSON 덩어리
    모두 대응.
    """
    if not text:
        raise LLMAdapterError("LLM 응답이 비어있습니다")
    # Strip markdown code fences
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    # First try whole string
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    # Grab widest {...} block
    m = re.search(r"\{[\s\S]*\}", s)
    if not m:
        raise LLMAdapterError(f"JSON 응답 파싱 실패: {s[:200]!r}")
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError as exc:
        raise LLMAdapterError(f"JSON 파싱 실패: {exc}: {m.group(0)[:200]!r}") from exc


# ───────────────────────────────────────────────────────────── #
# OpenAI — 자막 보정용 (gpt-4o-mini, 저렴·빠름)
# ───────────────────────────────────────────────────────────── #


def openai_correct_fn(system_prompt: str, text: str) -> tuple[str, dict]:
    """transcript_corrector.correct_transcript 에 주입하는 어댑터.

    Returns (corrected_text, meta).
    """
    _require_env("OPENAI_API_KEY")
    from openai import OpenAI

    client = OpenAI()
    t0 = time.monotonic()
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.1,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
    )
    latency_ms = int((time.monotonic() - t0) * 1000)
    content = (resp.choices[0].message.content or "").strip()
    usage = getattr(resp, "usage", None)
    meta = {
        "provider": "openai",
        "model": "gpt-4o-mini",
        "input_tokens": getattr(usage, "prompt_tokens", 0) if usage else 0,
        "output_tokens": getattr(usage, "completion_tokens", 0) if usage else 0,
        "latency_ms": latency_ms,
    }
    return content, meta


# ───────────────────────────────────────────────────────────── #
# Anthropic Claude — 구조화·분석·자료·초안
# ───────────────────────────────────────────────────────────── #


def _claude_json_call(
    system_prompt: str,
    user_input: Any,
    *,
    model: str,
    max_tokens: int = 4096,
) -> tuple[dict, dict]:
    """Claude를 호출해 JSON 객체를 반환하는 공용 함수."""
    _require_env("ANTHROPIC_API_KEY")
    import anthropic

    client = anthropic.Anthropic()
    user_text = (
        json.dumps(user_input, ensure_ascii=False)
        if isinstance(user_input, (dict, list))
        else str(user_input)
    )

    t0 = time.monotonic()
    resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_text}],
    )
    latency_ms = int((time.monotonic() - t0) * 1000)

    # Extract text from content blocks
    parts = []
    for blk in resp.content:
        if getattr(blk, "type", "") == "text":
            parts.append(blk.text)
    raw_text = "\n".join(parts)
    data = _parse_json_loose(raw_text)

    usage = getattr(resp, "usage", None)
    meta = {
        "provider": "anthropic",
        "model": model,
        "input_tokens": getattr(usage, "input_tokens", 0) if usage else 0,
        "output_tokens": getattr(usage, "output_tokens", 0) if usage else 0,
        "latency_ms": latency_ms,
        "raw_text": raw_text,
    }
    return data, meta


# Default models per use-case — 쉬운 과업은 Haiku, 고품질 필요는 Sonnet
CLAUDE_HAIKU = "claude-haiku-4-5-20251001"
CLAUDE_SONNET = "claude-sonnet-4-6"


def claude_extract_fn(system_prompt: str, paragraph_text: str) -> tuple[dict, dict]:
    """product_extractor.ExtractLLMFn — 단락별 제품 JSON 추출 (Haiku)."""
    return _claude_json_call(
        system_prompt, paragraph_text, model=CLAUDE_HAIKU, max_tokens=2048,
    )


def claude_synth_fn(system_prompt: str, context: dict) -> tuple[dict, dict]:
    """market_analyzer.SynthFn — 시장성 종합 분석 (Sonnet)."""
    return _claude_json_call(
        system_prompt, context, model=CLAUDE_SONNET, max_tokens=4096,
    )


def claude_marketing_fn(system_prompt: str, context: dict) -> tuple[dict, dict]:
    """marketing_generator.MarketingFn — B2C/B2B/인플루언서 자료 생성 (Sonnet)."""
    return _claude_json_call(
        system_prompt, context, model=CLAUDE_SONNET, max_tokens=8192,
    )


def claude_outreach_fn(system_prompt: str, context: dict) -> tuple[dict, dict]:
    """outreach_service.DraftFn — 컨택 초안 생성 (Sonnet)."""
    return _claude_json_call(
        system_prompt, context, model=CLAUDE_SONNET, max_tokens=2048,
    )
