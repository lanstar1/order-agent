"""
데이터랩 AI 인사이트 서비스 - Claude를 활용한 소싱 의사결정 지원
"""
import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


async def generate_datalab_insight(analysis_data: dict) -> dict:
    """
    트렌드 분석 결과를 기반으로 Claude AI 인사이트를 생성합니다.

    analysis_data should contain:
    - category_name: str
    - period: {start, end}
    - keywords: list of keyword analysis results with scores
    - device_data, gender_data, age_data (optional demographic breakdowns)

    Returns structured insight JSON.
    """
    try:
        import anthropic

        # Use config model or default
        model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
        # Prefer Haiku for cost efficiency
        insight_model = "claude-haiku-4-20250414"

        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

        # Build the analysis summary for Claude
        current_month = datetime.now().strftime("%Y년 %m월")

        keyword_summaries = []
        for kw in analysis_data.get("keywords", []):
            summary = {
                "keyword": kw.get("keyword", ""),
                "trust_score": kw.get("trust_score", 0),
                "momentum": kw.get("momentum", ""),
                "momentum_pct": kw.get("momentum_pct", 0),
                "peak_months": kw.get("peak_months", []),
                "low_months": kw.get("low_months", []),
                "recent_trend": kw.get("trend_data", [])[-6:] if kw.get("trend_data") else [],
            }
            keyword_summaries.append(summary)

        prompt = f"""당신은 이커머스 제품 소싱 전문 분석가입니다.
네이버 쇼핑인사이트 데이터를 기반으로 실행 가능한 비즈니스 인사이트를 제공해주세요.

## 분석 조건
- 카테고리: {analysis_data.get('category_name', '미지정')}
- 분석 기간: {analysis_data.get('period', {}).get('start', '')} ~ {analysis_data.get('period', {}).get('end', '')}
- 현재: {current_month}

## 키워드 분석 결과
{json.dumps(keyword_summaries, ensure_ascii=False, indent=2)}

## 인구통계 데이터
- 기기별: {json.dumps(analysis_data.get('device_data', {}), ensure_ascii=False)}
- 성별: {json.dumps(analysis_data.get('gender_data', {}), ensure_ascii=False)}
- 연령별: {json.dumps(analysis_data.get('age_data', {}), ensure_ascii=False)}

## 요청사항
위 데이터를 분석하여 아래 JSON 형식으로 응답해주세요. 반드시 JSON만 출력하세요.

{{
  "executive_summary": "전체 시장 상황 요약 (2-3문장, 한국어)",
  "hot_keywords": [
    {{"keyword": "키워드명", "reason": "추천 사유 (1문장)", "action": "구체적 실행 제안"}}
  ],
  "caution_keywords": [
    {{"keyword": "키워드명", "reason": "주의 사유 (1문장)", "risk_level": "high/medium/low"}}
  ],
  "seasonal_advice": "현재 월 기준 소싱 타이밍 조언 (2-3문장)",
  "target_insight": "성별/연령 타겟팅 제안 (1-2문장)",
  "action_items": [
    "구체적 실행 항목 1",
    "구체적 실행 항목 2",
    "구체적 실행 항목 3"
  ]
}}"""

        response = client.messages.create(
            model=insight_model,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse Claude's response
        raw_text = response.content[0].text.strip()

        # Try to extract JSON from response
        # Sometimes Claude wraps JSON in markdown code blocks
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()

        try:
            insight = json.loads(raw_text)
        except json.JSONDecodeError:
            logger.warning(f"[DataLab AI] JSON 파싱 실패, 원본 텍스트 반환")
            insight = {
                "executive_summary": raw_text[:500],
                "hot_keywords": [],
                "caution_keywords": [],
                "seasonal_advice": "",
                "target_insight": "",
                "action_items": [],
                "raw_response": raw_text,
            }

        # Add metadata
        insight["_meta"] = {
            "model": insight_model,
            "generated_at": datetime.now().isoformat(),
            "input_keywords": len(keyword_summaries),
        }

        return insight

    except ImportError:
        logger.error("[DataLab AI] anthropic 라이브러리 없음")
        return {"error": "AI 서비스를 사용할 수 없습니다. anthropic 라이브러리를 설치해주세요."}
    except Exception as e:
        logger.error(f"[DataLab AI] 인사이트 생성 실패: {e}", exc_info=True)
        return {"error": f"AI 인사이트 생성 중 오류가 발생했습니다: {str(e)}"}
