"""
작업 계획 수립기 — 요청을 DAG 형태의 SubTask로 분해
"""
import json
import logging
import uuid
from typing import Dict, Any, List, Optional

from super_agent.tools.litellm_gateway import call_llm
from super_agent.models.schemas import SubTask, ExecutionPlan

logger = logging.getLogger(__name__)

PLANNER_PROMPT = """당신은 업무 자동화 시스템의 작업 계획 수립 AI입니다.
사용자 요청을 분석하여 병렬 실행 가능한 하위 작업(SubTask)으로 분해하세요.

## 규칙
1. 각 SubTask는 독립적이거나 명시적 의존성만 가져야 합니다
2. 병렬 가능한 작업은 depends_on을 비워두세요
3. 최종 조립/문서 생성 작업은 모든 분석 작업에 의존해야 합니다
4. task_kind: research(조사), analysis(분석), calculation(계산), composition(문서작성), verification(검증)
5. 실용적이고 구체적인 작업으로 분해하세요

## 사용 가능한 도구 (실제 실행 가능)
- web_search: Perplexity AI 실시간 웹 검색 (시장조사, 경쟁사 분석, 최신 동향)
- erp_query: ERP 매출/재고/거래처/배송 데이터 조회
- execute_code: Python 코드 실행 (pandas/matplotlib 차트 생성, 통계 분석)
- write_document: GPT-4o 고품질 문서 작성 (보고서, 제안서, 이메일)
- image_gen: AI 이미지 생성 (제품 이미지, 인포그래픽, 홍보 이미지)
- file_analysis: 업로드된 파일 데이터 분석

## 도메인별 추천 태스크 구성
- sales_analysis: data_extraction → trend_analysis + segment_analysis → compose_report
- client_analysis: customer_segmentation → health_scoring + churn_detection → compose_report
- market_research: industry_research + competitor_analysis → market_synthesis → compose_report
- meeting_prep: data_preparation + key_message_extraction → presentation_composition
- inventory_analysis: inventory_classification → demand_forecast + risk_analysis → compose_report
- pricing_analysis: margin_analysis + price_comparison → pricing_strategy → compose_report
- executive_report: kpi_summary + issue_scan → executive_synthesis → compose_report

## 응답 형식 (JSON만 출력)
{
  "summary": "계획 요약 (한국어)",
  "tasks": [
    {
      "task_key": "data_analysis",
      "task_kind": "analysis",
      "title": "매출 데이터 분석",
      "objective": "업로드된 매출 데이터에서 추이와 이상치를 분석한다",
      "preferred_llm": "claude-sonnet",
      "required_tools": ["file_analysis", "calculation"],
      "depends_on": [],
      "timeout_sec": 60
    }
  ],
  "estimated_total_cost": 0.05
}"""


async def create_plan(
    job_id: str,
    user_prompt: str,
    classification: Dict[str, Any],
    file_info: Optional[Dict] = None,
) -> ExecutionPlan:
    """실행 계획 수립"""
    context_parts = [
        f"사용자 요청: {user_prompt}",
        f"분류된 업무 유형: {classification.get('job_type', 'freeform')}",
        f"결과물 유형: {classification.get('deliverable_type', 'report')}",
        f"복잡도: {classification.get('complexity', 3)}",
    ]

    if file_info:
        context_parts.append(f"\n첨부 파일 정보:")
        context_parts.append(f"- 파일명: {file_info.get('file_name', '')}")
        context_parts.append(f"- 유형: {file_info.get('type', '')}")
        context_parts.append(f"- 행수: {file_info.get('row_count', 0)}")
        if file_info.get('columns'):
            context_parts.append(f"- 컬럼: {', '.join(file_info['columns'][:30])}")
        if file_info.get('column_stats'):
            stats_summary = []
            for col, stat in list(file_info['column_stats'].items())[:10]:
                if stat.get('is_numeric'):
                    stats_summary.append(f"  {col}: 합계={stat.get('sum',0):,.0f}, 평균={stat.get('avg',0):,.0f}")
                else:
                    stats_summary.append(f"  {col}: {stat.get('unique',0)}개 고유값")
            context_parts.append("- 컬럼 통계:\n" + '\n'.join(stats_summary))

    user_message = '\n'.join(context_parts)

    try:
        result = await call_llm(
            model_key="claude-sonnet",
            messages=[{"role": "user", "content": user_message}],
            system_prompt=PLANNER_PROMPT,
            max_tokens=2000,
            temperature=0.2,
        )

        content = result["content"].strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        plan_data = json.loads(content)

        # SubTask 객체 생성
        subtasks = []
        for i, t in enumerate(plan_data.get("tasks", [])):
            task = SubTask(
                task_id=str(uuid.uuid4())[:8],
                task_key=t.get("task_key", f"task_{i}"),
                task_kind=t.get("task_kind", "analysis"),
                title=t.get("title", f"작업 {i+1}"),
                objective=t.get("objective", ""),
                preferred_llm=t.get("preferred_llm", "claude-sonnet"),
                fallback_llm=t.get("fallback_llm", "gpt-4o"),
                required_tools=t.get("required_tools", []),
                depends_on=t.get("depends_on", []),
                timeout_sec=t.get("timeout_sec", 60),
            )
            subtasks.append(task)

        plan = ExecutionPlan(
            job_id=job_id,
            intent=classification.get("job_type", "freeform"),
            complexity=classification.get("complexity", 3),
            subtasks=subtasks,
            deliverable_type=classification.get("deliverable_type", "report"),
            estimated_cost=plan_data.get("estimated_total_cost", 0),
        )

        logger.info(f"[Planner] 계획 수립 완료: {len(subtasks)}개 태스크")
        return plan

    except Exception as e:
        logger.error(f"[Planner] 계획 수립 실패: {e}")
        # 폴백: 단일 태스크 계획
        fallback_task = SubTask(
            task_id=str(uuid.uuid4())[:8],
            task_key="single_analysis",
            task_kind="analysis",
            title="통합 분석",
            objective=user_prompt,
            preferred_llm="claude-sonnet",
            required_tools=["file_analysis"] if file_info else [],
            depends_on=[],
        )
        compose_task = SubTask(
            task_id=str(uuid.uuid4())[:8],
            task_key="compose_output",
            task_kind="composition",
            title="결과물 생성",
            objective="분석 결과를 문서로 작성",
            preferred_llm="claude-sonnet",
            required_tools=["doc_gen"],
            depends_on=["single_analysis"],
        )
        return ExecutionPlan(
            job_id=job_id,
            intent=classification.get("job_type", "freeform"),
            complexity=classification.get("complexity", 3),
            subtasks=[fallback_task, compose_task],
            deliverable_type=classification.get("deliverable_type", "report"),
        )
