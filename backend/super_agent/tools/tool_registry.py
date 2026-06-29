"""
Tool Registry — 도구 인터페이스 + 등록/실행 관리
ReAct Agent Loop에서 사용하는 도구들을 관리한다.
"""
import logging
import time
from typing import Dict, Any, List, Optional, Callable, Awaitable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ToolResult:
    """도구 실행 결과 표준 포맷"""
    success: bool
    data: Any = None
    error: str = ""
    cost: float = 0.0
    latency_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_text(self) -> str:
        if not self.success:
            return f"[오류] {self.error}"
        if isinstance(self.data, str):
            return self.data
        if isinstance(self.data, dict):
            return str(self.data)
        return str(self.data)


@dataclass
class ToolDefinition:
    """도구 정의"""
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema 형태
    execute_fn: Callable[..., Awaitable[ToolResult]]
    category: str = "general"  # search, generation, analysis, data, utility


class ToolRegistry:
    """도구 등록소 — 싱글턴"""

    def __init__(self):
        self._tools: Dict[str, ToolDefinition] = {}

    def register(self, tool: ToolDefinition):
        self._tools[tool.name] = tool
        logger.info(f"[ToolRegistry] 도구 등록: {tool.name} ({tool.category})")

    def get(self, name: str) -> Optional[ToolDefinition]:
        return self._tools.get(name)

    def list_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "parameters": t.parameters,
                "category": t.category,
            }
            for t in self._tools.values()
        ]

    def get_tools_prompt(self) -> str:
        """Agent Loop에 전달할 도구 목록 프롬프트"""
        lines = ["사용 가능한 도구:"]
        for t in self._tools.values():
            params = ", ".join(
                f"{k}: {v.get('description', v.get('type', ''))}"
                for k, v in t.parameters.get("properties", {}).items()
            )
            required = t.parameters.get("required", [])
            lines.append(f"- {t.name}: {t.description}")
            lines.append(f"  파라미터: {params}")
            if required:
                lines.append(f"  필수: {', '.join(required)}")
        return "\n".join(lines)

    async def execute(self, name: str, params: Dict[str, Any]) -> ToolResult:
        """도구 실행"""
        tool = self._tools.get(name)
        if not tool:
            return ToolResult(success=False, error=f"도구 '{name}'을 찾을 수 없습니다")

        start = time.time()
        try:
            result = await tool.execute_fn(**params)
            result.latency_ms = int((time.time() - start) * 1000)
            logger.info(f"[Tool] {name} 실행 완료 ({result.latency_ms}ms, cost=${result.cost:.4f})")
            return result
        except Exception as e:
            latency = int((time.time() - start) * 1000)
            logger.error(f"[Tool] {name} 실행 실패 ({latency}ms): {e}")
            return ToolResult(success=False, error=str(e), latency_ms=latency)


# 싱글턴 인스턴스
registry = ToolRegistry()


def init_all_tools():
    """모든 도구를 레지스트리에 등록"""
    from super_agent.tools.web_search import register_web_search_tool
    from super_agent.tools.image_gen import register_image_gen_tool
    from super_agent.tools.writer import register_writer_tool
    from super_agent.tools.code_executor import register_code_executor_tool
    from super_agent.tools.erp_connector import register_erp_tools

    register_web_search_tool(registry)
    register_image_gen_tool(registry)
    register_writer_tool(registry)
    register_code_executor_tool(registry)
    register_erp_tools(registry)

    logger.info(f"[ToolRegistry] 전체 {len(registry._tools)}개 도구 등록 완료")
