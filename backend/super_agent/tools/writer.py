"""
고품질 문서 작성 도구 — OpenAI GPT-4o
한국어 비즈니스 문서 특화
"""
import os
import logging
import asyncio
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def write_document(
    topic: str,
    style: str = "formal",
    format: str = "report",
    context: str = "",
    max_tokens: int = 4096,
) -> "ToolResult":
    """GPT-4o로 고품질 문서 작성"""
    from super_agent.tools.tool_registry import ToolResult

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        # OpenAI 없으면 Claude fallback
        return await _write_with_claude(topic, style, format, context, max_tokens)

    style_map = {
        "formal": "격식체 비즈니스 문서",
        "casual": "친근한 설명 문서",
        "technical": "기술 보고서",
        "marketing": "마케팅/홍보 문서",
        "executive": "경영진 보고서 (간결하고 핵심 위주)",
    }
    style_desc = style_map.get(style, style)

    format_map = {
        "report": "분석 보고서 형식 (제목, 요약, 본문 섹션, 결론, 액션아이템)",
        "brief": "브리핑 문서 (1-2페이지 핵심 요약)",
        "email": "비즈니스 이메일 형식",
        "proposal": "제안서 형식 (배경, 목적, 방안, 기대효과, 일정)",
        "article": "아티클/블로그 형식",
    }
    format_desc = format_map.get(format, format)

    system_prompt = f"""당신은 한국어 비즈니스 문서 전문 작성자입니다.
스타일: {style_desc}
형식: {format_desc}

규칙:
- 한국어로 작성하되, 전문 용어는 영어 병기 가능
- 구체적 수치와 데이터를 포함하여 설득력 있게 작성
- 마크다운 형식으로 구조화
- 불필요한 상투적 문구 배제, 핵심에 집중"""

    user_msg = topic
    if context:
        user_msg = f"### 참고 자료\n{context}\n\n### 작성 요청\n{topic}"

    import httpx
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                "max_tokens": max_tokens,
                "temperature": 0.4,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    content = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})
    cost = (usage.get("prompt_tokens", 0) / 1_000_000 * 2.5 +
            usage.get("completion_tokens", 0) / 1_000_000 * 10.0)

    return ToolResult(
        success=True,
        data=content,
        cost=cost,
        metadata={"model": "gpt-4o", "tokens": usage.get("total_tokens", 0)},
    )


async def _write_with_claude(topic, style, format, context, max_tokens):
    """OpenAI 없을 때 Claude fallback"""
    from super_agent.tools.tool_registry import ToolResult
    from super_agent.tools.litellm_gateway import call_llm

    user_msg = f"다음 주제로 {style} 스타일의 {format} 문서를 한국어로 작성하세요:\n{topic}"
    if context:
        user_msg = f"참고자료:\n{context}\n\n{user_msg}"

    result = await call_llm(
        model_key="claude-sonnet",
        messages=[{"role": "user", "content": user_msg}],
        system_prompt="당신은 한국어 비즈니스 문서 전문 작성자입니다. 마크다운 형식으로 구조화하여 작성하세요.",
        max_tokens=max_tokens,
    )
    return ToolResult(
        success=True,
        data=result["content"],
        cost=result.get("cost", 0),
        metadata={"model": result.get("model", "claude-sonnet")},
    )


def register_writer_tool(registry):
    from super_agent.tools.tool_registry import ToolDefinition
    registry.register(ToolDefinition(
        name="write_document",
        description="고품질 비즈니스 문서 작성. 보고서, 브리핑, 이메일, 제안서 등",
        parameters={
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "작성할 문서 주제/내용"},
                "style": {"type": "string", "description": "문체 (formal/casual/technical/marketing/executive)"},
                "format": {"type": "string", "description": "형식 (report/brief/email/proposal/article)"},
                "context": {"type": "string", "description": "참고 자료/데이터 (선택)"},
            },
            "required": ["topic"],
        },
        execute_fn=write_document,
        category="generation",
    ))
