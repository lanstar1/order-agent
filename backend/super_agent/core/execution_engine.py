"""
실행 엔진 — DAG 기반 병렬 실행, 의존성 관리
"""
import json
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional, Callable
from collections import defaultdict

from super_agent.models.schemas import SubTask, ExecutionPlan
from super_agent.tools.litellm_gateway import call_llm

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """DAG 기반 태스크 병렬 실행 엔진"""

    def __init__(self, max_concurrent: int = 6):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.task_results: Dict[str, Dict[str, Any]] = {}
        self.progress_callback: Optional[Callable] = None

    def set_progress_callback(self, callback: Callable):
        """WebSocket 진행상황 콜백 설정"""
        self.progress_callback = callback

    async def execute_plan(
        self,
        plan: ExecutionPlan,
        file_data: Optional[Dict] = None,
        user_prompt: str = "",
    ) -> Dict[str, Any]:
        """
        실행 계획의 모든 태스크를 DAG 순서대로 실행
        Returns: {tasks: {task_key: result}, total_cost, total_tokens, elapsed_ms}
        """
        start_time = time.time()
        self.task_results = {}
        total_cost = 0
        total_tokens = 0

        # DAG 레이어 분리 (Topological Sort)
        layers = self._topological_layers(plan.subtasks)
        total_tasks = len(plan.subtasks)
        completed_count = 0

        for layer_idx, layer in enumerate(layers):
            logger.info(f"[Engine] Layer {layer_idx + 1}/{len(layers)}: {[t.task_key for t in layer]}")

            # 레이어 내 태스크 병렬 실행
            tasks = []
            for subtask in layer:
                tasks.append(self._execute_task(subtask, file_data, user_prompt, plan))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for subtask, result in zip(layer, results):
                if isinstance(result, Exception):
                    self.task_results[subtask.task_key] = {
                        "status": "failed",
                        "error": str(result),
                        "content": "",
                    }
                    logger.error(f"[Engine] {subtask.task_key} 실패: {result}")
                else:
                    self.task_results[subtask.task_key] = result
                    total_cost += result.get("cost", 0)
                    total_tokens += result.get("tokens_input", 0) + result.get("tokens_output", 0)

                completed_count += 1

                # 진행상황 콜백
                if self.progress_callback:
                    pct = int(completed_count / total_tasks * 100)
                    status = "succeeded" if not isinstance(result, Exception) else "failed"
                    await self.progress_callback(
                        task_id=subtask.task_id,
                        task_key=subtask.task_key,
                        status=status,
                        progress_pct=pct,
                        message=f"{subtask.title} {'완료' if status == 'succeeded' else '실패'}",
                    )

        elapsed_ms = int((time.time() - start_time) * 1000)

        return {
            "tasks": self.task_results,
            "total_cost": round(total_cost, 6),
            "total_tokens": total_tokens,
            "elapsed_ms": elapsed_ms,
        }

    async def _execute_task(
        self,
        subtask: SubTask,
        file_data: Optional[Dict],
        user_prompt: str,
        plan: ExecutionPlan,
    ) -> Dict[str, Any]:
        """단일 태스크 실행"""
        async with self.semaphore:
            start = time.time()

            if self.progress_callback:
                await self.progress_callback(
                    task_id=subtask.task_id,
                    task_key=subtask.task_key,
                    status="running",
                    message=f"{subtask.title} 실행 중...",
                )

            # 컨텍스트 구성
            context = self._build_context(subtask, file_data, user_prompt)

            # 의존성 결과 포함
            dep_results = {}
            for dep_key in subtask.depends_on:
                if dep_key in self.task_results:
                    dep_results[dep_key] = self.task_results[dep_key].get("content", "")

            if dep_results:
                context += "\n\n## 이전 작업 결과\n"
                for key, val in dep_results.items():
                    context += f"\n### {key}\n{val[:3000]}\n"

            # LLM 호출
            system_prompt = self._get_system_prompt(subtask)

            result = await call_llm(
                model_key=subtask.preferred_llm,
                messages=[{"role": "user", "content": context}],
                system_prompt=system_prompt,
                max_tokens=4096,
                temperature=0.3,
            )

            elapsed = int((time.time() - start) * 1000)

            return {
                "status": "succeeded",
                "content": result["content"],
                "model": result["model"],
                "tokens_input": result["tokens_input"],
                "tokens_output": result["tokens_output"],
                "cost": result["cost"],
                "latency_ms": elapsed,
            }

    def _build_context(self, subtask: SubTask, file_data: Optional[Dict], user_prompt: str) -> str:
        """태스크 실행 컨텍스트 구성"""
        parts = [
            f"## 작업 목표\n{subtask.objective}",
            f"\n## 원본 사용자 요청\n{user_prompt}",
        ]

        if file_data and "file_analysis" in subtask.required_tools:
            parts.append("\n## 업로드된 데이터")
            if file_data.get("columns"):
                parts.append(f"컬럼: {', '.join(file_data['columns'])}")
            if file_data.get("row_count"):
                parts.append(f"총 {file_data['row_count']}행")
            if file_data.get("column_stats"):
                parts.append("\n컬럼 통계:")
                for col, stat in file_data["column_stats"].items():
                    if stat.get("is_numeric"):
                        parts.append(f"  {col}: 합계={stat.get('sum',0):,.0f}, 평균={stat.get('avg',0):,.1f}, 최소={stat.get('min',0):,.0f}, 최대={stat.get('max',0):,.0f}")
                    else:
                        parts.append(f"  {col}: {stat.get('unique',0)}개 고유값, 샘플={stat.get('sample_values', [])[:3]}")
            if file_data.get("data_preview"):
                preview_text = json.dumps(file_data["data_preview"][:10], ensure_ascii=False, indent=2)
                parts.append(f"\n데이터 미리보기 (처음 10행):\n{preview_text}")
            if file_data.get("full_text"):
                parts.append(f"\n전체 데이터 (최대 30,000자):\n{file_data['full_text'][:30000]}")

        return '\n'.join(parts)

    def _get_system_prompt(self, subtask: SubTask) -> str:
        """태스크 유형별 시스템 프롬프트"""
        prompts = {
            "research": "당신은 전문 리서치 에이전트입니다. 주어진 주제에 대해 체계적으로 조사하고 핵심 인사이트를 구조화하세요. 한국어로 응답하세요.",
            "analysis": "당신은 데이터 분석 전문가입니다. 주어진 데이터를 깊이 분석하고, 추이/패턴/이상치/원인을 찾아내세요. 숫자에 근거한 구체적 분석을 제공하세요. 한국어로 응답하세요.",
            "calculation": "당신은 수치 계산 전문가입니다. 정확한 계산과 통계 분석을 수행하세요. 모든 계산 과정을 명시하세요. 한국어로 응답하세요.",
            "composition": """당신은 보고서 작성 전문가입니다. 이전 작업 결과를 종합하여 완성도 높은 문서를 작성하세요.
반드시 아래 JSON 형식으로 응답하세요:
```json
{
  "title": "보고서 제목",
  "executive_summary": "핵심 요약 (3-5줄)",
  "sections": [
    {"heading": "섹션 제목", "body": "본문 내용"},
    {"heading": "섹션 제목", "body": ["불릿 항목1", "불릿 항목2"]}
  ],
  "tables": [
    {"title": "표 제목", "headers": ["컬럼1", "컬럼2"], "rows": [["값1", "값2"]]}
  ],
  "action_items": ["액션1", "액션2"],
  "references": ["출처1"]
}
```""",
            "verification": "당신은 품질 검증 전문가입니다. 주어진 내용의 사실 정확성, 숫자 일관성, 논리적 타당성을 검증하세요. 한국어로 응답하세요.",
        }
        return prompts.get(subtask.task_kind, prompts["analysis"])

    def _topological_layers(self, subtasks: List[SubTask]) -> List[List[SubTask]]:
        """DAG를 레이어별로 분리 (Topological Sort)"""
        task_map = {t.task_key: t for t in subtasks}
        in_degree = defaultdict(int)
        dependents = defaultdict(list)

        for t in subtasks:
            in_degree[t.task_key] = len(t.depends_on)
            for dep in t.depends_on:
                dependents[dep].append(t.task_key)

        layers = []
        remaining = set(t.task_key for t in subtasks)

        while remaining:
            # in_degree가 0인 태스크들 = 현재 레이어
            layer_keys = [k for k in remaining if in_degree[k] == 0]
            if not layer_keys:
                # 순환 의존성 → 나머지 모두 현재 레이어에
                layer_keys = list(remaining)

            layer = [task_map[k] for k in layer_keys]
            layers.append(layer)

            for k in layer_keys:
                remaining.discard(k)
                for dep_key in dependents[k]:
                    in_degree[dep_key] -= 1

        return layers
