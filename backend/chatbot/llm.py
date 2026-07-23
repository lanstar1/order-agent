#!/usr/bin/env python3
"""LLM 클라이언트 — 선택적 보조. 없으면 조용히 규칙 기반으로 폴백한다.

원칙
  · 키가 없거나 패키지가 없거나 API 가 죽어도 **예외를 절대 올리지 않는다**. None 을 돌려준다.
  · 챗봇 전체가 LLM 없이 동작하는 것이 기본 동작이고, LLM 은 문장 다듬기 보조다.
  · **인증 자료 정보를 프롬프트에 넣지 않는다**(절대규칙). cert 경로 응답은 policy.py 가
    강제한 문구 그대로 나가야 하며, LLM 이 요약하면 O*·자재묶음·스캔·만료 주의문구가 사라진다.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from . import config

_CLIENT: Any = None
_TRIED = False

#: 절대 어겨선 안 되는 지침. 사내 영업·CS 가 그대로 고객에게 복사한다는 전제로 쓴다.
SYSTEM_PROMPT = """당신은 랜스타(라인업시스템) 사내 영업·CS 담당자를 돕는 상담 보조입니다.

규칙:
1. 답변은 반드시 "랜스타입니다." 로 시작합니다.
2. 주어진 [근거] 안에 있는 내용만 사용합니다. 근거에 없는 사양·수치·호환성을 지어내지 마십시오.
3. 인증(RoHS/CE/UL/KC/FCC/ISO 등)·시험성적서·인증서 보유 여부는 **어떤 경우에도 언급하지
   마십시오**. 이 경로에는 인증 정보가 주어지지 않습니다. 인증 문의는 별도 조회 대상입니다.
4. [제품분석] 근거는 제품 이미지를 보고 만든 AI 추론이라 실제와 다를 수 있습니다.
   단정하지 말고 "참고 정보" 성격으로 전달하십시오.
5. [상담이력] 근거는 2023~2025년 과거 상담 기록입니다. 재고·배송·가격은 현재와 다릅니다.
6. 근거가 부족하면 지어내지 말고 "정확한 확인을 위해 담당자 확인이 필요합니다" 라고 답하십시오.
7. 존댓말, 3~6문장, 불필요한 인사말 반복 없이 간결하게.
"""


def available() -> bool:
    """지금 이 프로세스에서 LLM 호출이 가능한가."""
    return _client() is not None


def _client() -> Any:
    global _CLIENT, _TRIED
    if _CLIENT is not None:
        return _CLIENT
    if _TRIED:
        return None
    _TRIED = True
    key = os.environ.get(config.LLM_API_KEY_ENV)
    if not key:
        return None
    try:
        import anthropic                                   # noqa: PLC0415
        _CLIENT = anthropic.Anthropic(api_key=key, timeout=config.LLM_TIMEOUT)
    except Exception:                                      # 패키지 없음/버전 불일치/기타
        _CLIENT = None
    return _CLIENT


def reset() -> None:
    """테스트용 — 캐시된 클라이언트를 버린다."""
    global _CLIENT, _TRIED
    _CLIENT, _TRIED = None, False


def complete(system: str, messages: List[Dict[str, str]],
             max_tokens: int = config.LLM_MAX_TOKENS) -> Optional[str]:
    """텍스트 한 덩어리를 돌려주거나 None. 어떤 예외도 밖으로 내보내지 않는다."""
    cli = _client()
    if cli is None:
        return None
    try:
        resp = cli.messages.create(
            model=config.LLM_MODEL,
            max_tokens=max_tokens,
            system=system or SYSTEM_PROMPT,
            messages=messages,
        )
        parts = [getattr(b, 'text', '') for b in getattr(resp, 'content', []) or []]
        out = ''.join(p for p in parts if p).strip()
        return out or None
    except Exception:
        return None


def output_is_safe(text: str) -> bool:
    """LLM 출력에 인증 주장이 섞였으면 버린다(규칙 기반으로 폴백).

    프롬프트로 금지해도 근거 텍스트에 남은 어휘를 따라 쓰는 일이 있으므로 출력단에서 한 번 더 막는다.
    """
    return bool(text) and not config.CERT_TOKEN_RX.search(text)


def polish(user_message: str, evidence: str,
           extra_rules: str = '') -> Optional[str]:
    """근거 블록을 사람 문장으로 다듬는다. 실패·불안전하면 None."""
    if not evidence.strip():
        return None
    sys_prompt = SYSTEM_PROMPT + (f'\n추가 지침:\n{extra_rules}\n' if extra_rules else '')
    out = complete(sys_prompt, [{'role': 'user',
                                 'content': f'[문의]\n{user_message}\n\n[근거]\n{evidence}'}])
    if out and not output_is_safe(out):
        return None
    return out
