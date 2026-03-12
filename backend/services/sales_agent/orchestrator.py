"""
판매 에이전트 오케스트레이터 v2
- Phase 1: Python 엔진으로 정량 분석 (RFM, ABC, 수요예측, CLV, 트렌드매칭)
- Phase 2: Claude AI로 전략 해석 + 에이전트 간 연동 (전략/파트너십이 다른 에이전트 결과 참조)
- 6개 에이전트 병렬/순차 실행
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional

import httpx

from services.sales_agent.engines import (
    calculate_rfm,
    calculate_abc,
    calculate_forecast,
    calculate_safety_stock,
    calculate_clv_acv,
    calculate_trend_matching,
)

logger = logging.getLogger(__name__)

# ── 6개 에이전트 정의 ──
AGENTS = {
    "customer": {
        "name": "거래처 분석",
        "icon": "👥",
        "description": "RFM 분석, 거래처 세분화, 이탈 위험 감지",
    },
    "product": {
        "name": "품목 관리",
        "icon": "📦",
        "description": "ABC 등급 분류, 수요 예측, 안전재고 산출",
    },
    "strategy": {
        "name": "판매전략",
        "icon": "🎯",
        "description": "거래처별 맞춤 전략, 교차판매, HaaS 전환 제안",
    },
    "future": {
        "name": "미래전략",
        "icon": "🔮",
        "description": "트렌드 매칭, 3년 성장 시나리오",
    },
    "partnership": {
        "name": "파트너십",
        "icon": "🤝",
        "description": "CLV/ACV 산출, 어카운트 스쿼드, ABM 타겟",
    },
    "visualization": {
        "name": "KPI/시각화",
        "icon": "📊",
        "description": "KPI 대시보드 데이터, 차트 데이터 생성",
    },
}


def _build_data_summary(sales_data: dict) -> str:
    """분석 데이터를 프롬프트에 포함할 요약 텍스트로 변환"""
    txs = sales_data.get("transactions", [])
    customers = sales_data.get("customers", [])
    products = sales_data.get("products", [])

    cust_totals = {}
    cust_freq = {}
    cust_last = {}
    for tx in txs:
        cn = tx.get("customer_name", "")
        cust_totals[cn] = cust_totals.get(cn, 0) + tx.get("total_amount", 0)
        cust_freq[cn] = cust_freq.get(cn, 0) + 1
        td = tx.get("transaction_date", "")
        if td and (cn not in cust_last or td > cust_last[cn]):
            cust_last[cn] = td

    prod_totals = {}
    for tx in txs:
        pn = tx.get("product_name", "")
        prod_totals[pn] = prod_totals.get(pn, 0) + tx.get("total_amount", 0)

    cat_totals = {}
    for tx in txs:
        cat = tx.get("category", "미분류") or "미분류"
        cat_totals[cat] = cat_totals.get(cat, 0) + tx.get("total_amount", 0)

    monthly = {}
    for tx in txs:
        td = tx.get("transaction_date", "")
        if len(td) >= 7:
            mon = td[:7]
            monthly[mon] = monthly.get(mon, 0) + tx.get("total_amount", 0)

    parts = []
    parts.append(f"## 데이터 개요")
    parts.append(f"- 분석 기간: {sales_data.get('period_start', '')} ~ {sales_data.get('period_end', '')}")
    parts.append(f"- 총 거래 건수: {len(txs):,}건")
    parts.append(f"- 총 거래처: {len(customers)}개, 총 품목: {len(products)}개")
    parts.append(f"- 총 매출액: {sum(t.get('total_amount', 0) for t in txs):,}원")

    parts.append(f"\n## 거래처별 매출 (상위 20)")
    for cn, amt in sorted(cust_totals.items(), key=lambda x: -x[1])[:20]:
        parts.append(f"- {cn}: {amt:,}원 (거래 {cust_freq.get(cn,0)}회, 최근 {cust_last.get(cn,'')})")

    parts.append(f"\n## 품목별 매출 (상위 20)")
    for pn, amt in sorted(prod_totals.items(), key=lambda x: -x[1])[:20]:
        parts.append(f"- {pn}: {amt:,}원")

    parts.append(f"\n## 카테고리별 매출")
    for cat, amt in sorted(cat_totals.items(), key=lambda x: -x[1]):
        parts.append(f"- {cat}: {amt:,}원")

    parts.append(f"\n## 월별 매출 추이")
    for mon, amt in sorted(monthly.items()):
        parts.append(f"- {mon}: {amt:,}원")

    custs_with_info = [c for c in customers if c.get("industry") or c.get("company_size")]
    if custs_with_info:
        parts.append(f"\n## 거래처 추가 정보")
        for c in custs_with_info[:20]:
            p = [c.get("customer_name", "")]
            if c.get("industry"): p.append(f"업종:{c['industry']}")
            if c.get("company_size"): p.append(f"규모:{c['company_size']}")
            if c.get("region"): p.append(f"지역:{c['region']}")
            parts.append(f"- {', '.join(p)}")

    return "\n".join(parts)


def _build_engine_context(engine_results: dict) -> str:
    """Python 엔진 결과를 Claude 프롬프트용 컨텍스트로 변환"""
    parts = []

    # RFM 결과
    rfm = engine_results.get("rfm", {})
    if rfm.get("segments"):
        parts.append("\n## [Python 엔진] RFM 분석 결과")
        for seg, info in rfm["segments"].items():
            parts.append(f"- {seg}: {info['count']}개 거래처, 총 {info['total_amount']:,}원")
        if rfm.get("churn_risk"):
            parts.append(f"- 이탈 위험 거래처: {len(rfm['churn_risk'])}개")
            for cr in rfm["churn_risk"][:5]:
                parts.append(f"  · {cr['customer_name']} ({cr['risk_level']}): {cr['reason']}")

    # ABC 결과
    abc = engine_results.get("abc", {})
    if abc.get("grade_summary"):
        parts.append("\n## [Python 엔진] ABC 등급 분류")
        for g, info in abc["grade_summary"].items():
            parts.append(f"- {g}등급: {info['count']}개 품목, {info['amount']:,}원 ({info['pct']}%)")

    # 수요예측
    forecast = engine_results.get("forecast", {})
    if forecast.get("forecast"):
        parts.append(f"\n## [Python 엔진] 수요 예측 (상위 5품목)")
        for f in forecast["forecast"][:5]:
            parts.append(f"- {f['product_name']}: 월평균 {f['current_monthly_avg']:,}원, 트렌드 {f['trend']}, 3개월 예측 {f['forecast_3m']}")

    # 안전재고
    safety = engine_results.get("safety_stock", [])
    if safety:
        parts.append(f"\n## [Python 엔진] 안전재고/ROP (상위 5품목)")
        for s in safety[:5]:
            parts.append(f"- {s['product_name']}: 일평균 {s['daily_avg_qty']}개, 안전재고 {s['safety_stock']}개, ROP {s['rop']}개")

    # CLV/ACV
    clv = engine_results.get("clv", {})
    if clv.get("tier_summary"):
        parts.append("\n## [Python 엔진] CLV/ACV 티어")
        for tier, info in clv["tier_summary"].items():
            parts.append(f"- {tier}: {info['count']}개 거래처, CLV 합계 {info['total_clv']:,}원")

    # 트렌드 매칭
    trends = engine_results.get("trends", {})
    if trends.get("growth_scenarios"):
        gs = trends["growth_scenarios"]
        parts.append(f"\n## [Python 엔진] 성장 시나리오")
        parts.append(f"- 현재 연매출: {gs['current_annual']:,}원")
        parts.append(f"- 보수적(10%): 3년 후 {gs['conservative']['year3']:,}원")
        parts.append(f"- 기본(20%): 3년 후 {gs['moderate']['year3']:,}원")
        parts.append(f"- 공격적(35%): 3년 후 {gs['aggressive']['year3']:,}원")

    return "\n".join(parts)


def _get_agent_prompt_v2(agent_key: str, data_summary: str, engine_context: str, phase2_context: str = "") -> str:
    """에이전트별 분석 프롬프트 (v2: Python 엔진 결과 + Phase 2 연동 컨텍스트)"""

    base = f"""너는 B2B IT 인프라 장비(서버, 스위치, 라우터, 방화벽, 스토리지 등) 영업팀의 전문 AI 분석가야.
아래 판매 데이터와 Python 엔진의 정량 분석 결과를 기반으로, 전략적 해석과 실행 가능한 제안을 JSON으로 반환해줘.

{data_summary}
{engine_context}
{phase2_context}
"""

    prompts = {
        "customer": f"""{base}

## 너의 역할: 거래처 분석 에이전트
Python 엔진이 이미 RFM 점수와 세그먼트를 산출했어. 너는 이 결과를 해석하고 전략을 제안해줘:

1. 각 세그먼트별 맞춤 영업 전략 (Champions/Loyal/Potential/At Risk/Hibernating)
2. 이탈 위험 거래처에 대한 구체적 리텐션 액션플랜
3. 기업학적 세분화 인사이트 (업종, 규모, 거래유형별 특성)
4. 전체 고객 포트폴리오 건강도 평가

반드시 아래 JSON으로 응답:
```json
{{
  "summary": "한줄 요약",
  "segment_strategies": [
    {{"segment": "Champions", "count": 0, "strategy": "구체적 전략", "actions": ["액션1", "액션2"], "kpi": "측정 지표"}}
  ],
  "churn_actions": [
    {{"customer_name": "...", "risk_level": "높음|주의", "action_plan": "...", "timeline": "..."}}
  ],
  "firmographic_insights": [
    {{"dimension": "업종별|규모별|유형별", "insight": "...", "recommendation": "..."}}
  ],
  "portfolio_health": {{"score": 75, "assessment": "...", "improvement_areas": ["..."]}},
  "recommendations": ["핵심 권고사항 1", "핵심 권고사항 2", "핵심 권고사항 3"]
}}
```""",

        "product": f"""{base}

## 너의 역할: 품목 관리 에이전트
Python 엔진이 ABC 등급, 수요예측, 안전재고를 산출했어. 너는 해석과 관리 전략을 제안해줘:

1. ABC 등급별 관리 전략 (A=주간 점검, B=격주, C=월간)
2. 수요 예측 결과 해석 및 재고 운영 전략 (VMI/JIT/MEIO 중 적합 방식)
3. 안전재고 운영 권고 및 발주점(ROP) 활용법
4. 품목 포트폴리오 최적화 제안

반드시 아래 JSON으로 응답:
```json
{{
  "summary": "한줄 요약",
  "grade_strategies": [
    {{"grade": "A", "strategy": "구체적 관리 전략", "check_cycle": "주간", "inventory_model": "VMI|JIT|MEIO", "actions": ["..."]}}
  ],
  "forecast_insights": [
    {{"product_name": "...", "trend": "증가|유지|감소", "interpretation": "...", "action": "..."}}
  ],
  "inventory_recommendations": [
    {{"product_name": "...", "current_issue": "...", "recommendation": "...", "expected_benefit": "..."}}
  ],
  "portfolio_optimization": {{"assessment": "...", "actions": ["..."]}},
  "recommendations": ["핵심 권고사항 1", "핵심 권고사항 2"]
}}
```""",

        "strategy": f"""{base}

## 너의 역할: 판매전략 에이전트
Python 엔진의 RFM/ABC/CLV/트렌드 분석 결과를 종합하여 실행 가능한 판매전략을 수립해줘:

1. 거래처별 맞춤 영업 전략 (세그먼트+티어 기반)
2. 교차판매/상향판매 기회 (구매 패턴 분석)
3. HaaS(Hardware as a Service) 전환 후보 및 MRR 추정
4. 파이프라인 분석 (매출 집중도, 리스크)

반드시 아래 JSON으로 응답:
```json
{{
  "summary": "한줄 요약",
  "customer_strategies": [
    {{"customer_name": "...", "tier": "VIP|성장|일반", "segment": "...", "current_amount": 0, "strategy": "...", "actions": ["..."], "expected_growth": "..."}}
  ],
  "cross_sell": [
    {{"customer_name": "...", "current_products": ["..."], "recommended": ["..."], "reason": "...", "expected_revenue": 0}}
  ],
  "haas_candidates": [
    {{"customer_name": "...", "current_purchase_type": "단발", "monthly_subscription": 0, "annual_mrr": 0, "reason": "...", "roi_for_customer": "..."}}
  ],
  "pipeline": {{
    "total_revenue": 0,
    "top3_concentration": 0.0,
    "monthly_trend": "증가|유지|감소",
    "risk_factors": ["..."],
    "velocity_assessment": "..."
  }},
  "recommendations": ["핵심 권고사항 1", "핵심 권고사항 2", "핵심 권고사항 3"]
}}
```""",

        "future": f"""{base}

## 너의 역할: 미래전략 에이전트
Python 엔진의 트렌드 매칭과 성장 시나리오를 기반으로 중장기 전략을 수립해줘:

### IT 인프라 메가트렌드 (참조):
1. 프라이빗 AI 인프라 - 연 35% 성장
2. 엣지 컴퓨팅 - 연 28% 성장
3. 제로 트러스트 보안 - 연 22% 성장
4. SBOM/공급망 보안 - 연 40% 성장
5. HaaS 구독 모델 - 연 30% 성장

분석 요구사항:
1. 거래처-트렌드 매칭 결과 해석 및 영업 기회 전략
2. 신규 시장 진입 전략 (미진출 영역)
3. 3년 성장 시나리오별 실행 전략
4. 기술 역량 강화 로드맵

반드시 아래 JSON으로 응답:
```json
{{
  "summary": "한줄 요약",
  "trend_strategies": [
    {{"trend": "...", "opportunity_level": "높음|보통|낮음", "strategy": "...", "target_customers": ["..."], "actions": ["..."], "timeline": "..."}}
  ],
  "market_entry": [
    {{"market": "...", "entry_strategy": "...", "required_capabilities": ["..."], "expected_timeline": "...", "estimated_revenue": 0}}
  ],
  "scenario_strategies": {{
    "conservative": {{"focus": "...", "key_actions": ["..."]}},
    "moderate": {{"focus": "...", "key_actions": ["..."]}},
    "aggressive": {{"focus": "...", "key_actions": ["..."]}}
  }},
  "capability_roadmap": [
    {{"quarter": "Q1", "focus_area": "...", "actions": ["..."]}}
  ],
  "recommendations": ["핵심 권고사항 1", "핵심 권고사항 2"]
}}
```""",

        "partnership": f"""{base}

## 너의 역할: 파트너십 에이전트
Python 엔진의 CLV/ACV/트렌드 분석을 기반으로 파트너십 전략을 수립해줘:

1. 어카운트 스쿼드 편성 제안 (Tier별 인력 배치)
2. ABM 2.0 타겟 선정 (CLV + 성장 잠재력 + 전략적 가치)
3. 관계 강화 프로그램 (Vendor→Partner→Strategic 단계)
4. 디지털 마케팅 전략 (웨비나, 영상 브리핑 등)

반드시 아래 JSON으로 응답:
```json
{{
  "summary": "한줄 요약",
  "squad_formation": [
    {{"tier": "Tier 1", "count": 0, "structure": "전담 AM + SE + CS", "customers": ["..."]}}
  ],
  "abm_targets": [
    {{"rank": 1, "customer_name": "...", "score": 0, "clv": 0, "growth_potential": "높음|보통", "strategic_value": "...", "reason": "..."}}
  ],
  "relationship_programs": [
    {{"customer_name": "...", "current_level": "Vendor|Partner|Strategic", "target_level": "...", "actions": ["..."], "timeline": "..."}}
  ],
  "digital_marketing": [
    {{"program": "...", "target_audience": "...", "frequency": "...", "expected_outcome": "..."}}
  ],
  "recommendations": ["핵심 권고사항 1", "핵심 권고사항 2"]
}}
```""",

        "visualization": f"""{base}

## 너의 역할: KPI/시각화 에이전트
데이터를 종합하여 KPI와 차트 데이터를 생성해줘:

1. 핵심 KPI 카드 4개 (총 매출, 거래처 수, 품목 수, 평균 거래단가)
2. 차트 데이터 (월별 매출, Top 거래처, 카테고리 비중, 월별 건수)
3. 전체 트렌드 요약 및 핵심 인사이트

반드시 아래 JSON으로 응답:
```json
{{
  "summary": "한줄 요약",
  "kpi_cards": [
    {{"label": "총 매출액", "value": 0, "unit": "원", "change": "+5.2%", "trend": "up|down|flat"}},
    {{"label": "거래처 수", "value": 0, "unit": "개", "change": "", "trend": "flat"}},
    {{"label": "품목 수", "value": 0, "unit": "개", "change": "", "trend": "flat"}},
    {{"label": "평균 거래단가", "value": 0, "unit": "원", "change": "", "trend": "flat"}}
  ],
  "charts": {{
    "monthly_revenue": [{{"month": "2025-01", "amount": 0}}],
    "top_customers": [{{"name": "...", "amount": 0}}],
    "category_share": [{{"category": "...", "amount": 0, "pct": 0.0}}],
    "monthly_count": [{{"month": "2025-01", "count": 0}}]
  }},
  "trend_summary": "전체적인 매출 트렌드 요약...",
  "key_insights": ["인사이트 1", "인사이트 2", "인사이트 3"],
  "recommendations": ["핵심 권고사항 1"]
}}
```""",
    }
    return prompts.get(agent_key, "")


async def _call_claude(api_key: str, model: str, prompt: str, agent_name: str) -> dict:
    """Claude API 호출"""
    start_time = time.time()
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": 8000,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
    }

    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()

        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block["text"]

        elapsed = time.time() - start_time
        logger.info(f"[SalesAgent/{agent_name}] Claude 응답 수신 ({elapsed:.1f}s)")

        result = _extract_json(text)
        if result:
            result["_elapsed_seconds"] = round(elapsed, 1)
            return result
        else:
            return {
                "summary": f"[{agent_name}] 분석 완료 (텍스트 형식)",
                "raw_text": text[:2000],
                "_elapsed_seconds": round(elapsed, 1),
                "recommendations": [],
            }
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"[SalesAgent/{agent_name}] Claude API 오류: {e}")
        return {
            "summary": f"[{agent_name}] 분석 실패: {str(e)}",
            "error": str(e),
            "_elapsed_seconds": round(elapsed, 1),
            "recommendations": [],
        }


def _extract_json(text: str) -> Optional[dict]:
    """Claude 응답에서 JSON 추출"""
    import re
    json_match = re.search(r'```json\s*\n(.*?)\n\s*```', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
    brace_start = text.find("{")
    if brace_start >= 0:
        brace_end = text.rfind("}")
        if brace_end > brace_start:
            try:
                return json.loads(text[brace_start:brace_end + 1])
            except json.JSONDecodeError:
                pass
    return None


async def run_analysis(
    sales_data: dict,
    api_key: str,
    model: str,
    progress_callback=None,
) -> dict:
    """
    2단계 분석 실행:
    Phase 1: Python 엔진 정량 분석 (즉시) + ①②④⑤⑥ Claude 병렬
    Phase 2: ③전략 + ⑤파트너십 Claude 재분석 (Phase 1 결과 참조)
    """
    start_time = time.time()
    txs = sales_data.get("transactions", [])
    data_summary = _build_data_summary(sales_data)

    # ── Phase 0: Python 엔진 정량 분석 (즉시, <1초) ──
    if progress_callback:
        await progress_callback("_engine", "running", 0)

    engine_results = {}
    try:
        engine_results["rfm"] = calculate_rfm(txs)
        engine_results["abc"] = calculate_abc(txs)
        engine_results["forecast"] = calculate_forecast(txs)
        engine_results["safety_stock"] = calculate_safety_stock(txs)
        engine_results["clv"] = calculate_clv_acv(txs)
        engine_results["trends"] = calculate_trend_matching(txs, sales_data.get("customers", []))
        logger.info("[SalesAgent] Python 엔진 분석 완료")
    except Exception as e:
        logger.error(f"[SalesAgent] Python 엔진 오류: {e}")

    if progress_callback:
        await progress_callback("_engine", "done", 100)

    engine_context = _build_engine_context(engine_results)
    results = {}

    # ── Phase 1: Claude 1차 병렬 분석 (①②④⑥) ──
    phase1_agents = ["customer", "product", "future", "visualization"]

    async def _run_agent(agent_key: str, extra_context: str = ""):
        if progress_callback:
            await progress_callback(agent_key, "running", 0)
        prompt = _get_agent_prompt_v2(agent_key, data_summary, engine_context, extra_context)
        agent_result = await _call_claude(api_key, model, prompt, agent_key)
        results[agent_key] = agent_result
        if progress_callback:
            await progress_callback(agent_key, "done", 100)
        return agent_result

    tasks1 = [_run_agent(k) for k in phase1_agents]
    await asyncio.gather(*tasks1, return_exceptions=True)

    # ── Phase 2: Claude 2차 분석 (③전략 + ⑤파트너십) — Phase 1 결과 참조 ──
    phase2_context_strategy = _build_phase2_context_for_strategy(results, engine_results)
    phase2_context_partnership = _build_phase2_context_for_partnership(results, engine_results)

    tasks2 = [
        _run_agent("strategy", phase2_context_strategy),
        _run_agent("partnership", phase2_context_partnership),
    ]
    await asyncio.gather(*tasks2, return_exceptions=True)

    elapsed = time.time() - start_time

    # ── 종합 결과 구성 ──
    agent_keys = list(AGENTS.keys())
    analysis_result = {
        "job_id": "",
        "status": "completed",
        "elapsed_seconds": round(elapsed, 1),
        "created_at": datetime.now().isoformat(),
        "sales_data_summary": sales_data.get("summary", {}),
        "period": {
            "start": sales_data.get("period_start", ""),
            "end": sales_data.get("period_end", ""),
        },
        "engine_results": {
            "rfm": engine_results.get("rfm", {}),
            "abc": engine_results.get("abc", {}),
            "forecast": engine_results.get("forecast", {}),
            "safety_stock": engine_results.get("safety_stock", []),
            "clv": engine_results.get("clv", {}),
            "trends": engine_results.get("trends", {}),
        },
        "agents": {},
    }

    for key in agent_keys:
        agent_info = AGENTS[key]
        agent_res = results.get(key, {})
        analysis_result["agents"][key] = {
            "name": agent_info["name"],
            "icon": agent_info["icon"],
            "description": agent_info["description"],
            "status": "error" if "error" in agent_res else "completed",
            "result": agent_res,
        }

    # 전체 권고사항 종합
    all_recs = []
    for key in agent_keys:
        agent_res = results.get(key, {})
        recs = agent_res.get("recommendations", [])
        if recs:
            agent_name = AGENTS[key]["name"]
            for rec in recs[:3]:
                all_recs.append(f"[{agent_name}] {rec}")
    analysis_result["top_recommendations"] = all_recs[:15]

    logger.info(f"[SalesAgent] 전체 분석 완료: {elapsed:.1f}초 (Phase 1+2), {len(agent_keys)}개 에이전트")
    return analysis_result


def _build_phase2_context_for_strategy(phase1_results: dict, engine_results: dict) -> str:
    """전략 에이전트용 Phase 2 컨텍스트 (①거래처 + ②품목 결과 참조)"""
    parts = ["\n## [Phase 2 연동] 다른 에이전트 분석 결과 참조"]

    # ① 거래처 분석 결과
    cust = phase1_results.get("customer", {})
    if cust.get("segment_strategies"):
        parts.append("\n### 거래처 분석 에이전트 결과:")
        for ss in cust["segment_strategies"][:5]:
            parts.append(f"- {ss.get('segment', '')}: {ss.get('strategy', '')}")
    if cust.get("churn_actions"):
        parts.append(f"- 이탈 위험 거래처 {len(cust['churn_actions'])}개 감지됨")

    # ② 품목 분석 결과
    prod = phase1_results.get("product", {})
    if prod.get("grade_strategies"):
        parts.append("\n### 품목 관리 에이전트 결과:")
        for gs in prod["grade_strategies"][:3]:
            parts.append(f"- {gs.get('grade', '')}등급: {gs.get('strategy', '')}")

    # 안전재고 결과
    safety = engine_results.get("safety_stock", [])
    if safety:
        parts.append(f"- 안전재고 산출 품목: {len(safety)}개")

    parts.append("\n위 결과를 반드시 참조하여 거래처별 전략을 구체화해줘. 세그먼트별 전략과 품목 등급을 연계한 교차판매 기회를 도출해.")
    return "\n".join(parts)


def _build_phase2_context_for_partnership(phase1_results: dict, engine_results: dict) -> str:
    """파트너십 에이전트용 Phase 2 컨텍스트 (④미래 + CLV 결과 참조)"""
    parts = ["\n## [Phase 2 연동] 다른 에이전트 분석 결과 참조"]

    # ④ 미래전략 결과
    future = phase1_results.get("future", {})
    if future.get("trend_strategies"):
        parts.append("\n### 미래전략 에이전트 결과:")
        for ts in future["trend_strategies"][:5]:
            parts.append(f"- {ts.get('trend', '')}: {ts.get('opportunity_level', '')} 기회, {ts.get('strategy', '')[:50]}")

    # 트렌드 매칭
    trends = engine_results.get("trends", {})
    if trends.get("trend_matching"):
        parts.append(f"\n### 트렌드 매칭 결과:")
        for tm in trends["trend_matching"][:5]:
            top = tm.get("top_trends", [])
            trend_names = [t["trend"] for t in top[:2]] if top else []
            parts.append(f"- {tm['customer_name']}: {', '.join(trend_names)} (기회가치 {tm.get('opportunity_value', 0):,}원)")

    parts.append("\n위 결과를 반드시 참조하여 ABM 스코어에 성장 잠재력(트렌드 매칭)을 반영하고, 어카운트 스쿼드 편성에 미래 전략 기회를 연계해줘.")
    return "\n".join(parts)
