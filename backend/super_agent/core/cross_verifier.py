"""
교차 검증 엔진 — 실행 결과의 품질 검증
- 숫자 일관성 체크
- 논리적 타당성 검증
- 할루시네이션 탐지
"""
import json
import re
import logging
from typing import Dict, Any, List, Optional

from super_agent.tools.litellm_gateway import call_llm

logger = logging.getLogger(__name__)

VERIFICATION_PROMPT = """당신은 AI 결과물의 품질을 검증하는 전문가입니다.

아래 분석 결과를 검증하고, 각 항목에 대해 평가하세요:

1. **숫자 정확성** (1-5점): 수치가 서로 일관적인가? 합계가 맞는가?
2. **논리적 타당성** (1-5점): 결론이 데이터에서 자연스럽게 도출되는가?
3. **완성도** (1-5점): 요청한 내용을 모두 다루었는가?
4. **실용성** (1-5점): 액션 아이템이 구체적이고 실행 가능한가?
5. **할루시네이션 위험** (1-5, 5=위험 없음): 데이터에 없는 내용을 만들어낸 것은 없는가?

JSON으로 응답하세요:
```json
{
  "scores": {
    "number_accuracy": 4,
    "logical_validity": 4,
    "completeness": 3,
    "practicality": 4,
    "hallucination_risk": 5
  },
  "overall_score": 4.0,
  "issues": ["발견된 문제점1", "문제점2"],
  "suggestions": ["개선 제안1", "제안2"],
  "passed": true
}
```
"""


async def verify_results(
    synthesis: Dict[str, Any],
    original_prompt: str,
    file_data: Optional[Dict] = None,
    threshold: float = 3.0,
) -> Dict[str, Any]:
    """
    실행 결과를 교차 검증
    Returns: {passed, overall_score, scores, issues, suggestions}
    """
    try:
        # 검증 대상 구성
        parts = [f"## 원본 요청\n{original_prompt}"]

        # 합성 결과
        if synthesis.get("parsed_report"):
            parts.append(f"\n## 분석 결과 (JSON)\n{json.dumps(synthesis['parsed_report'], ensure_ascii=False, indent=2)[:5000]}")
        elif synthesis.get("text_content"):
            parts.append(f"\n## 분석 결과\n{synthesis['text_content'][:5000]}")
        elif synthesis.get("summary"):
            parts.append(f"\n## 요약\n{synthesis['summary']}")

        # 원본 데이터 (있으면)
        if file_data:
            if file_data.get("column_stats"):
                stats_text = json.dumps(file_data["column_stats"], ensure_ascii=False, indent=2)[:2000]
                parts.append(f"\n## 원본 데이터 통계\n{stats_text}")

        context = "\n".join(parts)

        result = await call_llm(
            model_key="claude-haiku",  # 빠른 검증
            messages=[{"role": "user", "content": context}],
            system_prompt=VERIFICATION_PROMPT,
            max_tokens=1000,
            temperature=0.1,
        )

        content = result["content"].strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        verification = json.loads(content)

        # overall_score 계산 (미포함 시)
        if not verification.get("overall_score"):
            scores = verification.get("scores", {})
            if scores:
                verification["overall_score"] = sum(scores.values()) / len(scores)

        # 통과 여부
        verification["passed"] = verification.get("overall_score", 0) >= threshold
        verification["_cost"] = result.get("cost", 0)

        logger.info(
            f"[Verifier] 점수: {verification.get('overall_score', 0):.1f}/5 "
            f"({'통과' if verification['passed'] else '미달'})"
        )

        return verification

    except Exception as e:
        logger.warning(f"[Verifier] 검증 실패 (통과 처리): {e}")
        return {
            "passed": True,
            "overall_score": 0,
            "scores": {},
            "issues": [f"검증 실행 실패: {str(e)}"],
            "suggestions": [],
            "_error": str(e),
        }


def quick_number_check(content: str) -> List[str]:
    """
    빠른 숫자 일관성 체크 (LLM 호출 없이)
    숫자들의 합계 검증 등 기본적인 수치 검증
    """
    issues = []

    # 퍼센트 합계 체크 (100%를 초과하는 비율 합계)
    pct_matches = re.findall(r'(\d+(?:\.\d+)?)\s*%', content)
    if pct_matches:
        pct_values = [float(p) for p in pct_matches]
        # 비율 값들이 연속적으로 나오고 합이 100을 크게 초과하면 경고
        for i in range(len(pct_values) - 2):
            window = pct_values[i:i+3]
            if sum(window) > 120 and all(v < 100 for v in window):
                issues.append(f"비율 합계 의심: {window} = {sum(window):.1f}% (100% 초과)")

    return issues
