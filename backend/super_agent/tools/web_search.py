"""
웹 검색 도구 — Perplexity API (주), Tavily API (fallback)
"""
import os
import logging
import asyncio
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def _search_perplexity(query: str, max_results: int = 5) -> Dict[str, Any]:
    """Perplexity sonar 모델로 웹검색"""
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        raise ValueError("PERPLEXITY_API_KEY 미설정")

    import httpx
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "sonar",
                "messages": [
                    {"role": "system", "content": "정확하고 최신 정보를 한국어로 제공하세요. 출처를 반드시 포함하세요."},
                    {"role": "user", "content": query},
                ],
                "max_tokens": 2048,
                "return_citations": True,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    content = data["choices"][0]["message"]["content"]
    citations = data.get("citations", [])
    usage = data.get("usage", {})

    cost = (usage.get("prompt_tokens", 0) / 1_000_000 * 1.0 +
            usage.get("completion_tokens", 0) / 1_000_000 * 1.0)

    return {
        "content": content,
        "citations": citations[:max_results],
        "model": "perplexity/sonar",
        "cost": cost,
    }


async def _search_tavily(query: str, max_results: int = 5) -> Dict[str, Any]:
    """Tavily API fallback 웹검색"""
    api_key = os.getenv("TAVILY_API_KEY", "")
    if not api_key:
        raise ValueError("TAVILY_API_KEY 미설정")

    import httpx
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.tavily.com/search",
            json={
                "api_key": api_key,
                "query": query,
                "search_depth": "advanced",
                "max_results": max_results,
                "include_answer": True,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    answer = data.get("answer", "")
    results = data.get("results", [])
    sources = [{"title": r.get("title", ""), "url": r.get("url", ""), "snippet": r.get("content", "")[:200]}
               for r in results[:max_results]]

    content = answer + "\n\n### 참고 자료\n"
    for s in sources:
        content += f"- [{s['title']}]({s['url']}): {s['snippet']}\n"

    return {"content": content, "citations": [s["url"] for s in sources], "model": "tavily", "cost": 0.01}


async def web_search(query: str, max_results: int = 5) -> "ToolResult":
    """웹 검색 실행 (Perplexity → Tavily fallback)"""
    from super_agent.tools.tool_registry import ToolResult

    perplexity_err = None
    try:
        result = await _search_perplexity(query, max_results)
        return ToolResult(
            success=True,
            data=result["content"],
            cost=result["cost"],
            metadata={"citations": result["citations"], "model": result["model"]},
        )
    except Exception as e:
        perplexity_err = str(e)
        logger.warning(f"[WebSearch] Perplexity 실패: {e}, Tavily 시도")

    try:
        result = await _search_tavily(query, max_results)
        return ToolResult(
            success=True,
            data=result["content"],
            cost=result["cost"],
            metadata={"citations": result["citations"], "model": result["model"]},
        )
    except Exception as e2:
        logger.error(f"[WebSearch] Tavily도 실패: {e2}")
        return ToolResult(success=False, error=f"웹검색 실패: Perplexity({perplexity_err}), Tavily({e2})")


def register_web_search_tool(registry):
    from super_agent.tools.tool_registry import ToolDefinition
    registry.register(ToolDefinition(
        name="web_search",
        description="실시간 웹 검색. 최신 정보, 시장 동향, 뉴스, 경쟁사 분석 등에 사용",
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "검색 쿼리 (한국어 또는 영어)"},
                "max_results": {"type": "integer", "description": "최대 결과 수 (기본 5)"},
            },
            "required": ["query"],
        },
        execute_fn=web_search,
        category="search",
    ))
