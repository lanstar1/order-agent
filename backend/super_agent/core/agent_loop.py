"""
ReAct Agent Loop — Think → Act → Observe 반복 엔진
각 SubTask별로 도구를 실제 호출하며 결과를 수집한다.
"""
import json
import logging
import re
import time
from typing import Dict, Any, List, Optional, Callable, Awaitable

from super_agent.tools.tool_registry import registry, ToolResult

logger = logging.getLogger(__name__)

MAX_ITERATIONS = 10

REACT_SYSTEM_PROMPT = """당신은 업무 자동화 AI 에이전트입니다.
주어진 작업을 완수하기 위해 도구를 사용하세요.

## 응답 형식

매 턴마다 아래 형식으로 응답하세요:

<think>현재 상황 분석 및 다음 행동 계획</think>

<tool_call>
{{"name": "도구이름", "params": {{"key": "value"}}}}
</tool_call>

또는 작업이 완료되면:

<final_answer>
최종 결과를 여기에 작성
</final_answer>

## 규칙
1. 한 번에 하나의 도구만 호출하세요
2. 도구 결과를 관찰한 후 다음 행동을 결정하세요
3. 충분한 정보가 모이면 final_answer로 마무리하세요
4. 한국어로 응답하세요
5. 도구 호출이 실패하면 다른 방법을 시도하세요

{tools_prompt}
"""


async def run_agent_loop(
    task_objective: str,
    context: str = "",
    file_data: Optional[Dict[str, Any]] = None,
    model_key: str = "claude-sonnet",
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]:
    """
    ReAct Agent Loop 실행
    Returns: {answer, tool_calls: [{tool, params, result}], total_cost, iterations}
    """
    from super_agent.tools.litellm_gateway import call_llm

    # Agent Loop 오케스트레이터는 반드시 Claude 사용 (XML 형식 준수 필요)
    # GPT/Gemini는 도구(writer, 검색 등)로만 사용
    if model_key not in ("claude-sonnet", "claude-haiku"):
        model_key = "claude-haiku"

    tools_prompt = registry.get_tools_prompt()
    system = REACT_SYSTEM_PROMPT.format(tools_prompt=tools_prompt)

    # 초기 메시지 구성
    user_content = f"## 작업 목표\n{task_objective}"
    if context:
        user_content += f"\n\n## 참고 컨텍스트\n{context}"
    if file_data:
        preview = file_data.get("full_text", "")[:5000] or str(file_data.get("data_preview", []))[:3000]
        cols = file_data.get("columns", [])
        stats = file_data.get("column_stats", {})
        user_content += f"\n\n## 첨부 데이터\n- 형식: {file_data.get('type', 'unknown')}"
        user_content += f"\n- 행수: {file_data.get('row_count', 0)}"
        if cols:
            user_content += f"\n- 컬럼: {', '.join(cols[:20])}"
        if preview:
            user_content += f"\n- 미리보기:\n```\n{preview[:2000]}\n```"

    messages = [{"role": "user", "content": user_content}]

    tool_calls_log = []
    total_cost = 0.0
    final_answer = None
    _tool_fail_counts: Dict[str, int] = {}  # 도구별 실패 횟수 추적

    for iteration in range(MAX_ITERATIONS):
        if progress_callback:
            await progress_callback(f"Agent 반복 {iteration + 1}/{MAX_ITERATIONS}")

        # LLM 호출
        try:
            llm_result = await call_llm(
                model_key=model_key,
                messages=messages,
                system_prompt=system,
                max_tokens=4096,
                temperature=0.2,
            )
        except Exception as e:
            logger.error(f"[AgentLoop] LLM 호출 실패: {e}")
            break

        total_cost += llm_result.get("cost", 0)
        response_text = llm_result.get("content", "")

        # final_answer 체크
        final_match = re.search(r"<final_answer>(.*?)</final_answer>", response_text, re.DOTALL)
        if final_match:
            final_answer = final_match.group(1).strip()
            logger.info(f"[AgentLoop] 최종 답변 도출 (반복 {iteration + 1}회)")
            break

        # tool_call 파싱 (여러 형식 지원)
        tool_match = re.search(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", response_text, re.DOTALL)
        # JSON 블록 fallback (```json ... ```)
        if not tool_match:
            tool_match = re.search(r'```json\s*(\{"name".*?\})\s*```', response_text, re.DOTALL)
        if tool_match:
            try:
                tool_req = json.loads(tool_match.group(1))
                tool_name = tool_req.get("name", "")
                tool_params = tool_req.get("params", tool_req.get("arguments", tool_req.get("parameters", {})))
                if not tool_name:
                    raise ValueError("도구 이름 없음")

                logger.info(f"[AgentLoop] 도구 호출: {tool_name}({list(tool_params.keys())})")

                if progress_callback:
                    await progress_callback(f"도구 실행: {tool_name}")

                # 같은 도구 연속 실패 시 포기
                if _tool_fail_counts.get(tool_name, 0) >= 2:
                    logger.warning(f"[AgentLoop] {tool_name} 2회 이상 실패, 도구 없이 진행")
                    messages.append({"role": "assistant", "content": response_text})
                    messages.append({"role": "user", "content": f"<observation>\n{tool_name} 도구를 사용할 수 없습니다. 이 도구 없이 현재까지 수집된 정보로 최종 답변을 작성하세요.\n</observation>"})
                    continue

                # 도구 실행
                tool_result = await registry.execute(tool_name, tool_params)
                total_cost += tool_result.cost

                if not tool_result.success:
                    _tool_fail_counts[tool_name] = _tool_fail_counts.get(tool_name, 0) + 1

                tool_calls_log.append({
                    "iteration": iteration + 1,
                    "tool": tool_name,
                    "params": tool_params,
                    "success": tool_result.success,
                    "result_preview": tool_result.to_text()[:500],
                    "cost": tool_result.cost,
                    "latency_ms": tool_result.latency_ms,
                })

                # 대화에 assistant + tool 결과 추가
                messages.append({"role": "assistant", "content": response_text})
                observation = f"<observation>\n{tool_result.to_text()[:8000]}\n</observation>"
                messages.append({"role": "user", "content": observation})

            except json.JSONDecodeError as e:
                logger.warning(f"[AgentLoop] tool_call JSON 파싱 실패: {e}")
                messages.append({"role": "assistant", "content": response_text})
                messages.append({"role": "user", "content": "<observation>\n도구 호출 형식이 잘못되었습니다. JSON 형식을 확인하세요.\n</observation>"})
        else:
            # 도구 호출도 final_answer도 없으면 — 텍스트 응답으로 취급
            final_answer = response_text
            # think 태그 정리
            final_answer = re.sub(r"<think>.*?</think>", "", final_answer, flags=re.DOTALL).strip()
            if final_answer:
                break
            # 빈 응답이면 계속
            messages.append({"role": "assistant", "content": response_text})
            messages.append({"role": "user", "content": "작업을 계속하세요. 도구를 사용하거나 최종 답변을 제출하세요."})

    if not final_answer:
        final_answer = "작업을 완료하지 못했습니다. 최대 반복 횟수에 도달했습니다."

    return {
        "answer": final_answer,
        "tool_calls": tool_calls_log,
        "total_cost": round(total_cost, 6),
        "iterations": min(iteration + 1, MAX_ITERATIONS) if 'iteration' in dir() else 0,
    }
