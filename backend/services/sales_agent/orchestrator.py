"""
판매 에이전트 오케스트레이터
- Phase 0: Python 엔진 (RFM, ABC, CLV 등) 즉시 실행
- Phase 1: Claude 1차 병렬 분석 (Mode A: 4개, Mode B: 3개)
- Phase 2: Claude 2차 분석 (전략+파트너십, 1차 결과 참조)
"""
from __future__ import annotations
import asyncio, time, json, logging
from typing import Optional, Callable

from .schemas import SalesData, AnalysisResult, AnalysisMode
from .engines import (
    calculate_rfm, calculate_abc, calculate_forecast,
    calculate_safety_stock, calculate_clv_acv, calculate_trend_matching,
)

logger = logging.getLogger(__name__)


async def run_analysis(
    sales_data: SalesData,
    api_key: str,
    model: str,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """2단계 분석 실행"""
    start_time = time.time()
    is_single = sales_data.analysis_mode == AnalysisMode.SINGLE
    txs = sales_data.transactions
    data_summary = _build_data_summary(sales_data)

    # ── Phase 0: Python 엔진 정량 분석 ──
    if progress_callback:
        await progress_callback("_phase", "info", "Phase 0: 정량 분석 엔진을 실행합니다")
        await progress_callback("_engine", "running", 0)

    engine_results = {}
    try:
        if not is_single:
            engine_results["rfm"] = calculate_rfm(txs)
        engine_results["abc"] = calculate_abc(txs)
        engine_results["forecast"] = calculate_forecast(txs)
        engine_results["safety_stock"] = calculate_safety_stock(txs)
        engine_results["clv"] = calculate_clv_acv(txs)
        engine_results["trends"] = calculate_trend_matching(txs, sales_data.customers)
        logger.info("[SalesAgent] Python 엔진 분석 완료")
    except Exception as e:
        logger.error(f"[SalesAgent] Python 엔진 오류: {e}")

    if progress_callback:
        await progress_callback("_engine", "done", 100)

    engine_context = _build_engine_context(engine_results)
    results = {}

    # ── Phase 1: Claude 1차 병렬 분석 ──
    if is_single:
        # Mode B: customer 에이전트 스킵, 나머지 3개 병렬
        phase1_agents = ["product", "future", "visualization"]
        if progress_callback:
            await progress_callback("_phase", "info",
                f"Phase 1: Mode B(단일 거래처) — 3개 에이전트 병렬 분석")
    else:
        # Mode A: 4개 병렬
        phase1_agents = ["customer", "product", "future", "visualization"]
        if progress_callback:
            await progress_callback("_phase", "info",
                "Phase 1: Mode A(다중 거래처) — 4개 에이전트 병렬 분석")

    async def _run_agent(agent_key: str, extra_context: str = ""):
        if progress_callback:
            await progress_callback(agent_key, "running", 0)
        prompt = _get_agent_prompt(agent_key, data_summary, engine_context,
                                   extra_context, is_single, sales_data)
        agent_result = await _call_claude(api_key, model, prompt, agent_key)
        results[agent_key] = agent_result
        if progress_callback:
            await progress_callback(agent_key, "done", 100)
        return agent_result

    tasks1 = [_run_agent(k) for k in phase1_agents]
    await asyncio.gather(*tasks1, return_exceptions=True)

    # ── Phase 2: Claude 2차 분석 (전략+파트너십) ──
    if progress_callback:
        await progress_callback("_phase", "info",
            "Phase 2: 1차 결과를 기반으로 전략/파트너십 심층 분석")

    p2_ctx_strategy = _build_phase2_context(results, engine_results, "strategy")
    p2_ctx_partnership = _build_phase2_context(results, engine_results, "partnership")

    tasks2 = [
        _run_agent("strategy", p2_ctx_strategy),
        _run_agent("partnership", p2_ctx_partnership),
    ]
    await asyncio.gather(*tasks2, return_exceptions=True)

    elapsed = time.time() - start_time
    logger.info(f"[SalesAgent] 전체 분석 완료: {elapsed:.1f}초")

    return {
        "analysis_mode": sales_data.analysis_mode.value,
        "target_customer": sales_data.target_customer_name or "",
        "engine_results": engine_results,
        "agent_results": {k: v for k, v in results.items()},
        "elapsed_seconds": elapsed,
        "period_start": sales_data.period_start,
        "period_end": sales_data.period_end,
    }


def _build_data_summary(data: SalesData) -> str:
    """분석 데이터 요약"""
    mode_str = "단일 거래처 심층 분석(Mode B)" if data.analysis_mode == AnalysisMode.SINGLE else "다중 거래처 비교 분석(Mode A)"
    target = f"\n대상 거래처: {data.target_customer_name}" if data.target_customer_name else ""
    lines = [
        f"분석 모드: {mode_str}{target}",
        f"기간: {data.period_start} ~ {data.period_end}",
        f"거래 건수: {data.total_rows}, 거래처: {data.total_customers}개, 품목: {data.total_products}개",
        f"총 매출액: {data.total_amount:,}원",
        "",
        "거래처별 매출 TOP 10:",
    ]
    from collections import defaultdict
    cust_amt = defaultdict(int)
    for tx in data.transactions[:5000]:
        cn = tx.get("customer_name", "")
        amt = int(float(str(tx.get("total_amount", tx.get("supply_price", 0)) or 0).replace(",", "")))
        if cn:
            cust_amt[cn] += amt
    for i, (cn, amt) in enumerate(sorted(cust_amt.items(), key=lambda x: -x[1])[:10], 1):
        lines.append(f"  {i}. {cn}: {amt:,}원")

    lines.append("\n품목별 매출 TOP 10:")
    prod_amt = defaultdict(int)
    for tx in data.transactions[:5000]:
        pn = tx.get("product_name", "")
        amt = int(float(str(tx.get("total_amount", tx.get("supply_price", 0)) or 0).replace(",", "")))
        if pn:
            prod_amt[pn] += amt
    for i, (pn, amt) in enumerate(sorted(prod_amt.items(), key=lambda x: -x[1])[:10], 1):
        lines.append(f"  {i}. {pn}: {amt:,}원")

    return "\n".join(lines)


def _build_engine_context(engine_results: dict) -> str:
    """Python 엔진 결과 요약"""
    lines = ["=== Python 정량 분석 결과 ==="]

    rfm = engine_results.get("rfm")
    if rfm and rfm.get("segments"):
        lines.append("\n[RFM 세그먼트]")
        for seg, info in rfm["segments"].items():
            lines.append(f"  {seg}: {info['count']}개 거래처, 매출 {info['total_monetary']:,}원")
        if rfm.get("churn_risk"):
            lines.append(f"  ⚠ 이탈 위험: {len(rfm['churn_risk'])}개 거래처")

    abc = engine_results.get("abc")
    if abc and abc.get("grade_summary"):
        lines.append("\n[ABC 분류]")
        for g, info in abc["grade_summary"].items():
            lines.append(f"  {g}등급: {info['count']}개 품목 ({info['pct']}%)")

    clv = engine_results.get("clv")
    if clv and clv.get("tier_summary"):
        lines.append("\n[CLV 티어]")
        for tier, info in clv["tier_summary"].items():
            if info["count"] > 0:
                lines.append(f"  {tier}: {info['count']}개, CLV합계 {info['total_clv']:,}원")

    trends = engine_results.get("trends")
    if trends and trends.get("trends"):
        lines.append("\n[트렌드 매칭]")
        for t in trends["trends"]:
            lines.append(f"  {t['trend_name']}: score={t['score']}, 기회={t['opportunity_level']}")

    return "\n".join(lines)


def _build_phase2_context(results: dict, engine_results: dict, agent_key: str) -> str:
    """Phase 2 에이전트에 전달할 1차 분석 결과 요약"""
    lines = ["\n=== Phase 1 분석 결과 참조 ==="]
    for k, v in results.items():
        if isinstance(v, dict):
            summary = v.get("summary", "")[:300]
            lines.append(f"\n[{k} 에이전트] {summary}")
            recs = v.get("recommendations", [])
            if recs:
                for r in recs[:3]:
                    lines.append(f"  - {r}")
    return "\n".join(lines)


def _get_agent_prompt(agent_key: str, data_summary: str, engine_context: str,
                      extra_context: str, is_single: bool, sales_data: SalesData) -> str:
    """에이전트별 프롬프트 생성"""
    mode_desc = "단일 거래처 심층 분석" if is_single else "다중 거래처 비교 분석"
    base = f"""당신은 B2B IT 장비 판매팀의 AI 분석 에이전트입니다.
분석 모드: {mode_desc}

{data_summary}

{engine_context}
{extra_context}

반드시 아래 JSON 형식으로만 응답하세요 (마크다운 코드블록 없이 순수 JSON):
"""

    prompts = {
        "customer": base + """{
  "summary": "거래처 분석 요약 (2-3문장)",
  "segments": [{"segment": "Champions|Loyal|At Risk|...", "count": 0, "strategy": "전략", "kpi": "목표 KPI"}],
  "top_customers": [{"name": "거래처명", "revenue": 0, "growth": "+15%", "risk": "낮음|보통|높음"}],
  "recommendations": ["추천1", "추천2", "추천3"]
}""",
        "product": base + """{
  "summary": "품목 분석 요약",
  "abc_insights": "ABC 분류 기반 인사이트",
  "top_products": [{"name": "품목명", "grade": "A|B|C", "revenue": 0, "trend": "상승|유지|하락"}],
  "cross_sell": [{"source": "기존품목", "target": "추천품목", "reason": "사유"}],
  "recommendations": ["추천1", "추천2"]
}""",
        "strategy": base + """{
  "summary": "판매전략 요약",
  "customer_strategies": [{"customer_name": "거래처", "tier": "Tier 1|2|3", "segment": "세그먼트", "strategy": "맞춤전략", "expected_growth": "+20%"}],
  "cross_sell_plans": [{"customer": "거래처", "current": "현재품목", "recommended": "추천품목", "revenue_potential": 0}],
  "action_items": [{"priority": "높음|보통", "action": "실행항목", "timeline": "1개월", "owner": "담당"}],
  "recommendations": ["추천1", "추천2"]
}""",
        "future": base + """{
  "summary": "미래전략 요약",
  "trend_strategies": [{"trend": "트렌드명", "opportunity_level": "높음|보통|낮음", "strategy": "대응전략", "target_customers": ["거래처1"], "actions": ["실행1"], "timeline": "Q1 2026"}],
  "scenarios": {"conservative": {"description": "보수적", "revenue": 0}, "moderate": {"description": "중립", "revenue": 0}, "aggressive": {"description": "공격적", "revenue": 0}},
  "recommendations": ["추천1", "추천2"]
}""",
        "partnership": base + """{
  "summary": "파트너십 전략 요약",
  "squad_formation": [{"tier": "Tier 1|2|3", "count": 0, "structure": "팀구성", "customers": ["거래처"]}],
  "abm_targets": [{"customer": "거래처", "score": 85, "clv": 0, "growth_potential": "높음|보통|낮음", "reason": "사유"}],
  "relationship_programs": [{"program": "프로그램명", "target": "대상", "timeline": "기한", "expected_impact": "기대효과"}],
  "recommendations": ["추천1", "추천2"]
}""",
        "visualization": base + """{
  "summary": "KPI 대시보드 요약",
  "kpis": [{"label": "KPI명", "value": 0, "unit": "원|건|%", "change": "+5%"}],
  "monthly_revenue": [{"month": "2026-01", "amount": 0}],
  "monthly_count": [{"month": "2026-01", "count": 0}],
  "category_breakdown": [{"category": "카테고리", "amount": 0, "pct": 0}],
  "recommendations": ["추천1", "추천2"]
}""",
    }
    return prompts.get(agent_key, base + '{"summary": "분석 완료", "recommendations": []}')


async def _call_claude(api_key: str, model: str, prompt: str, agent_key: str) -> dict:
    """Claude API 호출"""
    import anthropic

    client = anthropic.AsyncAnthropic(api_key=api_key)
    try:
        response = await client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        # JSON 추출
        if "```" in text:
            import re
            m = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
            if m:
                text = m.group(1).strip()
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"[{agent_key}] JSON 파싱 실패, 원문 저장: {e}")
        return {"summary": text[:500] if text else "JSON 파싱 실패", "recommendations": [], "_raw": text[:2000]}
    except Exception as e:
        logger.error(f"[{agent_key}] Claude 호출 실패: {e}")
        return {"summary": f"Claude 호출 오류: {str(e)}", "recommendations": []}
