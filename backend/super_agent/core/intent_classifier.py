"""
의도 분류기 — 사용자 요청을 분석하여 job_type, deliverable_type, complexity 결정
Claude Haiku로 빠르게 분류 (~100ms 목표)
"""
import json
import logging
from typing import Dict, Any, Optional

from super_agent.tools.litellm_gateway import call_llm

logger = logging.getLogger(__name__)

CLASSIFICATION_PROMPT = """당신은 사내 업무 요청을 분류하는 AI입니다.
사용자의 요청을 분석하여 아래 JSON 형식으로 정확히 응답하세요.

## 분류 기준

### job_type (업무 유형)
- sales_analysis: 매출 분석, 매출 추이, 채널별 성과, 매출 보고서
- client_analysis: 거래처 분석, 거래처 건강도, 이탈 탐지, 영업 전략
- market_research: 시장 조사, 경쟁사 분석, 트렌드, 시장 동향
- meeting_prep: 미팅 자료, 브리핑, 프레젠테이션, 발표 준비
- content_creation: 콘텐츠 생성, 블로그, 상세페이지, 마케팅 자료
- inventory_analysis: 재고 분석, 발주 참고, 품절 위험, ABC 분석
- pricing_analysis: 가격 분석, 마진, 채널별 가격, 수익성
- cs_analysis: CS 분석, 반품, 불량, 고객 불만
- executive_report: 경영 보고서, 임원 보고, 월간 브리핑
- freeform: 위 분류에 해당하지 않는 일반 요청

### deliverable_type (결과물 유형)
- report: 보고서 (docx)
- slides: 프레젠테이션 (pptx)
- sheet: 데이터 표 (xlsx)
- brief: 간단 요약 (1페이지)
- email: 이메일 초안
- json: 구조화된 데이터

### complexity (1-5)
- 1: 단순 조회/분류
- 2: 단일 소스 분석
- 3: 복수 소스 결합 분석
- 4: 심층 분석 + 문서 생성
- 5: 멀티 에이전트 필요한 복합 작업

## 응답 형식 (JSON만 출력)
{
  "job_type": "...",
  "deliverable_type": "...",
  "complexity": 3,
  "title": "작업 제목 (한국어, 20자 이내)",
  "key_entities": ["추출된 핵심 키워드"],
  "data_sources_needed": ["erp", "web_search", "uploaded_file" 등],
  "confidence": 0.95
}"""


async def classify_intent(
    user_prompt: str,
    has_files: bool = False,
    file_info: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    사용자 요청의 의도를 분류
    Returns: {job_type, deliverable_type, complexity, title, ...}
    """
    context_parts = [f"사용자 요청: {user_prompt}"]
    if has_files and file_info:
        context_parts.append(f"첨부 파일: {file_info.get('file_name', '')}")
        context_parts.append(f"파일 유형: {file_info.get('type', '')}")
        if file_info.get('columns'):
            context_parts.append(f"데이터 컬럼: {', '.join(file_info['columns'][:20])}")
        if file_info.get('row_count'):
            context_parts.append(f"데이터 행수: {file_info['row_count']}")

    user_message = '\n'.join(context_parts)

    try:
        result = await call_llm(
            model_key="claude-haiku",
            messages=[{"role": "user", "content": user_message}],
            system_prompt=CLASSIFICATION_PROMPT,
            max_tokens=500,
            temperature=0.1,
        )

        content = result["content"].strip()
        # JSON 추출
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        classification = json.loads(content)

        # 파일이 있으면 deliverable_type 자동 조정
        if has_files and classification.get("deliverable_type") == "brief":
            classification["deliverable_type"] = "report"

        classification["_llm_cost"] = result.get("cost", 0)
        classification["_llm_tokens"] = result.get("tokens_input", 0) + result.get("tokens_output", 0)

        logger.info(f"[Intent] 분류 완료: {classification.get('job_type')} / {classification.get('title')}")
        return classification

    except Exception as e:
        logger.error(f"[Intent] 분류 실패: {e}")
        # 폴백 분류
        return {
            "job_type": "freeform",
            "deliverable_type": "report",
            "complexity": 3,
            "title": user_prompt[:20],
            "key_entities": [],
            "data_sources_needed": ["uploaded_file"] if has_files else [],
            "confidence": 0.3,
            "_llm_cost": 0,
            "_llm_tokens": 0,
        }
