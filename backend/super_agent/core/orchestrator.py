"""
오케스트레이터 — Super Agent 메인 파이프라인
Intent → Plan → Execute → Synthesize → Document
"""
import json
import uuid
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime

from super_agent.core.intent_classifier import classify_intent
from super_agent.core.task_planner import create_plan
from super_agent.core.execution_engine import ExecutionEngine
from super_agent.core.websocket_manager import ws_manager
from super_agent.core.cross_verifier import verify_results, quick_number_check
from super_agent.tools.file_parser import parse_file
from super_agent.tools.doc_builder import build_document
from super_agent.models.schemas import ExecutionPlan

logger = logging.getLogger(__name__)


class SuperAgentOrchestrator:
    """
    Super Agent 메인 오케스트레이터
    6-Stage Pipeline: 분류 → 계획 → 라우팅 → 실행 → 합성 → 문서생성
    """

    def __init__(self):
        self.engine = ExecutionEngine(max_concurrent=6)

    async def run_job(
        self,
        job_id: str,
        user_prompt: str,
        file_path: Optional[str] = None,
        deliverable_type: Optional[str] = None,
        constraints: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        전체 파이프라인 실행
        Returns: {
            status, classification, plan_summary,
            execution_result, synthesis, artifact, cost_summary
        }
        """
        start_time = time.time()
        result = {
            "job_id": job_id,
            "status": "running",
            "stages": {},
        }

        try:
            # ─── Stage 1: 파일 파싱 (업로드 있을 경우) ───
            file_data = None
            file_info = None
            if file_path:
                await ws_manager.send_progress(
                    job_id, "running", "파일 분석 중...", 5
                )
                file_data = parse_file(file_path)
                if file_data.get("error"):
                    logger.warning(f"[Orchestrator] 파일 파싱 경고: {file_data['error']}")
                file_info = {
                    "file_name": file_path.split("/")[-1] if "/" in file_path else file_path,
                    "type": file_data.get("type", "unknown"),
                    "row_count": file_data.get("row_count", 0),
                    "columns": file_data.get("columns", []),
                    "column_stats": file_data.get("column_stats", {}),
                }
                result["stages"]["file_parse"] = {
                    "status": "completed",
                    "type": file_data.get("type"),
                    "row_count": file_data.get("row_count", 0),
                    "columns": file_data.get("columns", [])[:20],
                }

            # ─── Stage 2: 의도 분류 ───
            await ws_manager.send_progress(
                job_id, "running", "요청 분석 중...", 10
            )
            classification = await classify_intent(
                user_prompt=user_prompt,
                has_files=file_path is not None,
                file_info=file_info,
            )
            result["classification"] = classification
            result["stages"]["intent"] = {"status": "completed", "data": classification}

            # deliverable_type 오버라이드
            if deliverable_type:
                classification["deliverable_type"] = deliverable_type

            logger.info(
                f"[Orchestrator] 분류: {classification.get('job_type')} / "
                f"{classification.get('deliverable_type')} / 복잡도 {classification.get('complexity')}"
            )

            # ─── Stage 3: 작업 계획 수립 ───
            await ws_manager.send_progress(
                job_id, "running", "작업 계획 수립 중...", 20
            )
            plan = await create_plan(
                job_id=job_id,
                user_prompt=user_prompt,
                classification=classification,
                file_info=file_info,
            )
            result["plan_summary"] = {
                "task_count": len(plan.subtasks),
                "tasks": [
                    {"key": t.task_key, "title": t.title, "kind": t.task_kind}
                    for t in plan.subtasks
                ],
            }
            result["stages"]["planning"] = {"status": "completed"}

            logger.info(f"[Orchestrator] 계획: {len(plan.subtasks)}개 태스크")

            # ─── Stage 4: DAG 실행 ───
            await ws_manager.send_progress(
                job_id, "running", "태스크 실행 중...", 30
            )

            # 도메인 프롬프트 설정
            self.engine.job_type = classification.get("job_type", "freeform")

            # 진행상황 콜백 설정
            async def progress_callback(**kwargs):
                task_key = kwargs.get("task_key", "")
                status = kwargs.get("status", "")
                pct = kwargs.get("progress_pct", 0)
                msg = kwargs.get("message", "")
                # 30~80% 범위로 매핑
                mapped_pct = 30 + int(pct * 0.5)
                await ws_manager.send_task_update(
                    job_id=job_id,
                    task_id=kwargs.get("task_id", ""),
                    task_key=task_key,
                    status=status,
                    progress_pct=mapped_pct,
                    message=msg,
                )

            self.engine.set_progress_callback(progress_callback)
            execution_result = await self.engine.execute_plan(
                plan=plan,
                file_data=file_data,
                user_prompt=user_prompt,
            )
            result["execution_result"] = {
                "elapsed_ms": execution_result.get("elapsed_ms"),
                "total_cost": execution_result.get("total_cost"),
                "total_tokens": execution_result.get("total_tokens"),
                "task_count": len(execution_result.get("tasks", {})),
            }
            result["stages"]["execution"] = {"status": "completed"}

            # ─── Stage 5: 결과 합성 ───
            await ws_manager.send_progress(
                job_id, "running", "결과 종합 중...", 85
            )

            synthesis = self._synthesize_results(execution_result, plan)
            result["synthesis"] = synthesis
            result["stages"]["synthesis"] = {"status": "completed"}

            # ─── Stage 5.5: 교차 검증 ───
            await ws_manager.send_progress(
                job_id, "running", "결과 검증 중...", 88
            )
            try:
                verification = await verify_results(
                    synthesis=synthesis,
                    original_prompt=user_prompt,
                    file_data=file_data,
                )
                result["verification"] = verification
                if not verification.get("passed"):
                    logger.warning(
                        f"[Orchestrator] 검증 미달: {verification.get('overall_score', 0):.1f}/5, "
                        f"이슈: {verification.get('issues', [])}"
                    )
            except Exception as ve:
                logger.warning(f"[Orchestrator] 검증 스킵: {ve}")
                result["verification"] = {"passed": True, "skipped": True}

            # ─── Stage 6: 문서 생성 ───
            await ws_manager.send_progress(
                job_id, "running", "문서 생성 중...", 90
            )

            artifact_type = classification.get("deliverable_type", "report")
            doc_content = self._prepare_doc_content(synthesis, classification)

            artifact = build_document(
                artifact_type=artifact_type,
                content=doc_content,
                filename_prefix=classification.get("job_type", "report"),
            )
            result["artifact"] = artifact
            result["stages"]["document"] = {"status": "completed"}

            # ─── 완료 ───
            elapsed = int((time.time() - start_time) * 1000)
            result["status"] = "completed"
            result["cost_summary"] = {
                "intent_cost": classification.get("_llm_cost", 0),
                "execution_cost": execution_result.get("total_cost", 0),
                "total_cost": round(
                    classification.get("_llm_cost", 0)
                    + execution_result.get("total_cost", 0),
                    6,
                ),
                "total_tokens": (
                    classification.get("_llm_tokens", 0)
                    + execution_result.get("total_tokens", 0)
                ),
                "elapsed_ms": elapsed,
            }

            await ws_manager.send_completed(job_id, {
                "artifact": artifact,
                "cost_summary": result["cost_summary"],
            })

            logger.info(
                f"[Orchestrator] 완료: {elapsed}ms, "
                f"비용 ${result['cost_summary']['total_cost']:.4f}"
            )

            return result

        except Exception as e:
            logger.error(f"[Orchestrator] 파이프라인 오류: {e}", exc_info=True)
            result["status"] = "failed"
            result["error"] = str(e)
            await ws_manager.send_error(job_id, str(e))
            return result

    def _synthesize_results(
        self, execution_result: Dict, plan: ExecutionPlan
    ) -> Dict[str, Any]:
        """실행 결과를 종합"""
        tasks = execution_result.get("tasks", {})
        synthesis = {
            "summary": "",
            "sections": [],
            "raw_outputs": {},
        }

        # composition 태스크 결과가 있으면 그것을 메인 콘텐츠로 사용
        compose_content = None
        for key, result in tasks.items():
            if result.get("status") == "succeeded":
                synthesis["raw_outputs"][key] = result.get("content", "")[:5000]

                # composition 타입 태스크 찾기
                for subtask in plan.subtasks:
                    if subtask.task_key == key and subtask.task_kind == "composition":
                        compose_content = result.get("content", "")
                        break

        if compose_content:
            # JSON 파싱 시도
            try:
                parsed = json.loads(compose_content)
                synthesis["parsed_report"] = parsed
                synthesis["summary"] = parsed.get("executive_summary", "")
            except (json.JSONDecodeError, TypeError):
                # JSON이 아닌 경우 텍스트로 처리
                synthesis["summary"] = compose_content[:500]
                synthesis["text_content"] = compose_content
        else:
            # composition이 없으면 모든 결과를 합침
            all_contents = []
            for key, result in tasks.items():
                if result.get("status") == "succeeded" and result.get("content"):
                    all_contents.append(f"## {key}\n{result['content']}")
            synthesis["text_content"] = "\n\n".join(all_contents)
            synthesis["summary"] = "분석이 완료되었습니다."

        return synthesis

    def _prepare_doc_content(
        self, synthesis: Dict, classification: Dict
    ) -> Dict[str, Any]:
        """문서 빌더용 콘텐츠 구조 준비"""

        # parsed_report가 있으면 그대로 사용
        if synthesis.get("parsed_report"):
            return synthesis["parsed_report"]

        # 텍스트 결과를 문서 구조로 변환
        title = classification.get("title", "분석 보고서")
        content = {
            "title": title,
            "executive_summary": synthesis.get("summary", ""),
            "sections": [],
            "tables": [],
            "action_items": [],
        }

        # raw_outputs를 섹션으로 변환
        for key, text in synthesis.get("raw_outputs", {}).items():
            content["sections"].append({
                "heading": key.replace("_", " ").title(),
                "body": text[:3000],
            })

        return content


# 싱글턴
orchestrator = SuperAgentOrchestrator()
