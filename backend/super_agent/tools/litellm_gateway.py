"""
LiteLLM 멀티 LLM 게이트웨이
- Claude, GPT-4, Gemini 등 복수 모델 통합
- 비용 추적, fallback, rate limit 대응
"""
import os
import time
import json
import logging
import asyncio
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# LiteLLM 사용 가능 여부 체크
try:
    import litellm
    litellm.set_verbose = False
    LITELLM_AVAILABLE = True
except ImportError:
    LITELLM_AVAILABLE = False
    logger.warning("[LLM] litellm 미설치 - anthropic SDK 직접 사용")

# Anthropic SDK fallback
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# ─── 모델 레지스트리 ───
MODEL_REGISTRY = {
    "claude-sonnet": {
        "litellm_key": "anthropic/claude-sonnet-4-5-20250929",
        "anthropic_key": "claude-sonnet-4-5-20250929",
        "cost_input_1m": 3.0,
        "cost_output_1m": 15.0,
        "tier": "reasoning",
    },
    "claude-haiku": {
        "litellm_key": "anthropic/claude-haiku-4-5-20251001",
        "anthropic_key": "claude-haiku-4-5-20251001",
        "cost_input_1m": 0.80,
        "cost_output_1m": 4.0,
        "tier": "fast",
    },
    "gpt-4o": {
        "litellm_key": "gpt-4o",
        "cost_input_1m": 2.5,
        "cost_output_1m": 10.0,
        "tier": "reasoning",
    },
    "gpt-4o-mini": {
        "litellm_key": "gpt-4o-mini",
        "cost_input_1m": 0.15,
        "cost_output_1m": 0.60,
        "tier": "fast",
    },
    "gemini-flash": {
        "litellm_key": "gemini/gemini-2.0-flash",
        "cost_input_1m": 0.10,
        "cost_output_1m": 0.40,
        "tier": "data",
    },
}

# ─── 라우팅 정책 ───
ROUTING_POLICY = {
    "reasoning": "claude-sonnet",
    "data_processing": "gemini-flash",
    "web_research": "gpt-4o-mini",
    "document_writing": "claude-sonnet",
    "fast_classification": "claude-haiku",
    "verification": "gpt-4o",
    "code": "claude-sonnet",
}

FALLBACK_CHAIN = {
    "claude-sonnet": ["gpt-4o", "gemini-flash"],
    "claude-haiku": ["gpt-4o-mini", "gemini-flash"],
    "gpt-4o": ["claude-sonnet", "gemini-flash"],
    "gpt-4o-mini": ["claude-haiku", "gemini-flash"],
    "gemini-flash": ["claude-haiku", "gpt-4o-mini"],
}


def get_model_for_task(task_type: str) -> str:
    """작업 유형에 맞는 최적 모델 반환"""
    return ROUTING_POLICY.get(task_type, "claude-sonnet")


async def call_llm(
    model_key: str,
    messages: List[Dict[str, str]],
    system_prompt: Optional[str] = None,
    max_tokens: int = 4096,
    temperature: float = 0.3,
    response_format: Optional[str] = None,
) -> Dict[str, Any]:
    """
    LLM 호출 (LiteLLM 또는 Anthropic SDK)
    Returns: {content, model, tokens_input, tokens_output, cost, latency_ms}
    """
    start_time = time.time()
    model_info = MODEL_REGISTRY.get(model_key, MODEL_REGISTRY["claude-sonnet"])

    # 시도할 모델 순서 (primary + fallbacks)
    models_to_try = [model_key] + FALLBACK_CHAIN.get(model_key, [])

    last_error = None
    for try_model_key in models_to_try:
        try_info = MODEL_REGISTRY.get(try_model_key, model_info)

        # 각 모델당 최대 2회 재시도 (지수 백오프)
        for attempt in range(2):
            try:
                result = await _call_single_model(
                    try_model_key, try_info, messages, system_prompt,
                    max_tokens, temperature, response_format
                )
                result["latency_ms"] = int((time.time() - start_time) * 1000)

                # 비용 로깅
                try:
                    from super_agent.tools.cost_tracker import log_cost
                    log_cost(
                        job_id="",
                        model=try_model_key,
                        tokens_input=result.get("tokens_input", 0),
                        tokens_output=result.get("tokens_output", 0),
                        cost=result.get("cost", 0),
                    )
                except Exception:
                    pass

                return result
            except Exception as e:
                last_error = e
                if attempt == 0:
                    logger.warning(f"[LLM] {try_model_key} 1차 실패: {e}, 재시도")
                    await asyncio.sleep(1)  # 1초 대기 후 재시도
                else:
                    logger.warning(f"[LLM] {try_model_key} 2차 실패: {e}, 다음 모델")

    # 모든 모델 실패
    raise Exception(f"모든 LLM 호출 실패. 마지막 에러: {last_error}")


async def _call_single_model(
    model_key: str,
    model_info: dict,
    messages: List[Dict[str, str]],
    system_prompt: Optional[str],
    max_tokens: int,
    temperature: float,
    response_format: Optional[str],
) -> Dict[str, Any]:
    """단일 모델 호출"""

    # LiteLLM 사용 가능하면 LiteLLM으로
    if LITELLM_AVAILABLE:
        return await _call_via_litellm(
            model_key, model_info, messages, system_prompt,
            max_tokens, temperature, response_format
        )

    # Anthropic SDK 직접 사용 (Claude 모델만)
    if ANTHROPIC_AVAILABLE and model_key.startswith("claude"):
        return await _call_via_anthropic(
            model_key, model_info, messages, system_prompt,
            max_tokens, temperature
        )

    raise Exception(f"LLM 호출 불가: litellm 미설치, {model_key}은 anthropic SDK로 호출 불가")


async def _call_via_litellm(
    model_key, model_info, messages, system_prompt,
    max_tokens, temperature, response_format
) -> Dict[str, Any]:
    """LiteLLM을 통한 호출"""
    litellm_model = model_info["litellm_key"]

    kwargs = {
        "model": litellm_model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    if system_prompt:
        # system 메시지를 맨 앞에 추가
        kwargs["messages"] = [{"role": "system", "content": system_prompt}] + messages

    if response_format == "json":
        kwargs["response_format"] = {"type": "json_object"}

    response = await asyncio.to_thread(litellm.completion, **kwargs)

    content = response.choices[0].message.content
    usage = response.usage
    tokens_in = usage.prompt_tokens if usage else 0
    tokens_out = usage.completion_tokens if usage else 0

    cost = (tokens_in / 1_000_000 * model_info["cost_input_1m"] +
            tokens_out / 1_000_000 * model_info["cost_output_1m"])

    return {
        "content": content,
        "model": model_key,
        "tokens_input": tokens_in,
        "tokens_output": tokens_out,
        "cost": round(cost, 6),
        "latency_ms": 0,
    }


async def _call_via_anthropic(
    model_key, model_info, messages, system_prompt,
    max_tokens, temperature
) -> Dict[str, Any]:
    """Anthropic SDK 직접 호출"""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise Exception("ANTHROPIC_API_KEY 미설정")

    client = anthropic.Anthropic(api_key=api_key)
    anthropic_model = model_info.get("anthropic_key", model_info.get("litellm_key", ""))

    kwargs = {
        "model": anthropic_model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if system_prompt:
        kwargs["system"] = system_prompt

    response = await asyncio.to_thread(client.messages.create, **kwargs)

    content = response.content[0].text if response.content else ""
    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens

    cost = (tokens_in / 1_000_000 * model_info["cost_input_1m"] +
            tokens_out / 1_000_000 * model_info["cost_output_1m"])

    return {
        "content": content,
        "model": model_key,
        "tokens_input": tokens_in,
        "tokens_output": tokens_out,
        "cost": round(cost, 6),
        "latency_ms": 0,
    }
