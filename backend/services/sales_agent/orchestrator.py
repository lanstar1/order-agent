"""
판매 에이전트 오케스트레이터
- 6개 AI 에이전트를 병렬 실행
- Claude API로 각 에이전트의 분석 프롬프트 실행
- 결과를 종합하여 최종 리포트 생성
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional

import httpx

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

    # 거래처별 매출 집계
    cust_totals = {}
    for tx in txs:
        cn = tx.get("customer_name", "")
        cust_totals[cn] = cust_totals.get(cn, 0) + tx.get("total_amount", 0)

    # 품목별 매출 집계
    prod_totals = {}
    for tx in txs:
        pn = tx.get("product_name", "")
        prod_totals[pn] = prod_totals.get(pn, 0) + tx.get("total_amount", 0)

    # 거래처별 거래 횟수
    cust_freq = {}
    for tx in txs:
        cn = tx.get("customer_name", "")
        cust_freq[cn] = cust_freq.get(cn, 0) + 1

    # 거래처별 최근 거래일
    cust_last = {}
    for tx in txs:
        cn = tx.get("customer_name", "")
        td = tx.get("transaction_date", "")
        if td and (cn not in cust_last or td > cust_last[cn]):
            cust_last[cn] = td

    # 카테고리별 매출
    cat_totals = {}
    for tx in txs:
        cat = tx.get("category", "미분류") or "미분류"
        cat_totals[cat] = cat_totals.get(cat, 0) + tx.get("total_amount", 0)

    # 월별 매출 추이
    monthly = {}
    for tx in txs:
        td = tx.get("transaction_date", "")
        if len(td) >= 7:
            mon = td[:7]
            monthly[mon] = monthly.get(mon, 0) + tx.get("total_amount", 0)

    summary_parts = []
    summary_parts.append(f"## 데이터 개요")
    summary_parts.append(f"- 분석 기간: {sales_data.get('period_start', '')} ~ {sales_data.get('period_end', '')}")
    summary_parts.append(f"- 총 거래 건수: {len(txs):,}건")
    summary_parts.append(f"- 총 거래처: {len(customers)}개")
    summary_parts.append(f"- 총 품목: {len(products)}개")
    summary_parts.append(f"- 총 매출액: {sum(t.get('total_amount', 0) for t in txs):,}원")

    summary_parts.append(f"\n## 거래처별 매출 (상위 20)")
    for cn, amt in sorted(cust_totals.items(), key=lambda x: -x[1])[:20]:
        freq = cust_freq.get(cn, 0)
        last = cust_last.get(cn, "")
        summary_parts.append(f"- {cn}: {amt:,}원 (거래 {freq}회, 최근 {last})")

    summary_parts.append(f"\n## 품목별 매출 (상위 20)")
    for pn, amt in sorted(prod_totals.items(), key=lambda x: -x[1])[:20]:
        summary_parts.append(f"- {pn}: {amt:,}원")

    summary_parts.append(f"\n## 카테고리별 매출")
    for cat, amt in sorted(cat_totals.items(), key=lambda x: -x[1]):
        summary_parts.append(f"- {cat}: {amt:,}원")

    summary_parts.append(f"\n## 월별 매출 추이")
    for mon, amt in sorted(monthly.items()):
        summary_parts.append(f"- {mon}: {amt:,}원")

    # 거래처 마스터 정보 (있으면)
    custs_with_info = [c for c in customers if c.get("industry") or c.get("company_size")]
    if custs_with_info:
        summary_parts.append(f"\n## 거래처 추가 정보")
        for c in custs_with_info[:20]:
            parts = [c.get("customer_name", "")]
            if c.get("industry"):
                parts.append(f"업종:{c['industry']}")
            if c.get("company_size"):
                parts.append(f"규모:{c['company_size']}")
            if c.get("region"):
                parts.append(f"지역:{c['region']}")
            if c.get("contract_type"):
                parts.append(f"계약:{c['contract_type']}")
            summary_parts.append(f"- {', '.join(parts)}")

    return "\n".join(summary_parts)


def _get_agent_prompt(agent_key: str, data_summary: str) -> str:
    """에이전트별 분석 프롬프트 생성"""

    base_context = f"""너는 B2B IT 인프라 장비(서버, 스위치, 라우터, 방화벽, 스토리지 등) 영업팀의 전문 AI 분석가야.
아래 판매 데이터를 분석하여 결과를 JSON 형식으로 반환해줘.

{data_summary}
"""

    prompts = {
        "customer": f"""{base_context}

## 분석 역할: 거래처 분석 에이전트
다음을 분석해줘:

### 1. RFM 분석
각 거래처에 대해:
- Recency(R): 마지막 거래일로부터 경과일 → 1~5등급 (5=최근)
- Frequency(F): 분석 기간 내 거래 횟수 → 1~5등급 (5=빈번)
- Monetary(M): 총 거래 금액 → 1~5등급 (5=고액)
- 5분위수 방식으로 등급 분류

### 2. 세그먼트 분류
RFM 점수 기반 5개 세그먼트:
- Champions (R≥4,F≥4,M≥4): VIP 고객
- Loyal (R≥3,F≥3,M≥3): 충성 고객
- Potential (R≥3,F≤3): 성장 잠재
- At Risk (R≤2,F≥3): 이탈 위험
- Hibernating (R≤2,F≤2): 휴면 고객

### 3. 이탈 위험 감지
- 최근 거래가 없고 이전 대비 금액 감소한 거래처 식별

### 4. 세그먼트별 영업 전략 제안

반드시 아래 JSON 형식으로 응답:
```json
{{
  "summary": "한줄 요약",
  "rfm_results": [
    {{"customer_name": "...", "customer_code": "...", "R": 5, "F": 4, "M": 5, "score": 545, "segment": "Champions", "total_amount": 0, "frequency": 0, "last_date": ""}}
  ],
  "segments": {{
    "Champions": {{"count": 0, "total_amount": 0, "customers": ["..."]}},
    "Loyal": {{"count": 0, "total_amount": 0, "customers": ["..."]}},
    "Potential": {{"count": 0, "total_amount": 0, "customers": ["..."]}},
    "At Risk": {{"count": 0, "total_amount": 0, "customers": ["..."]}},
    "Hibernating": {{"count": 0, "total_amount": 0, "customers": ["..."]}}
  }},
  "churn_risk": [
    {{"customer_name": "...", "risk_level": "높음|주의", "reason": "..."}}
  ],
  "strategies": [
    {{"segment": "...", "strategy": "...", "actions": ["..."]}}
  ],
  "recommendations": ["핵심 권고사항 1", "핵심 권고사항 2"]
}}
```""",

        "product": f"""{base_context}

## 분석 역할: 품목 관리 에이전트
다음을 분석해줘:

### 1. ABC 등급 분류 (매출액 기준 파레토)
- A등급: 상위 20% 품목 (전체 매출의 약 80%)
- B등급: 다음 30% 품목
- C등급: 나머지 50% 품목

### 2. 수요 예측 (향후 3개월)
- 월별 판매 추이 기반 이동평균 예측
- 성장률/감소율 반영

### 3. 품목별 관리 전략 제안

반드시 아래 JSON 형식으로 응답:
```json
{{
  "summary": "한줄 요약",
  "abc_results": [
    {{"product_name": "...", "product_code": "...", "category": "...", "total_amount": 0, "total_qty": 0, "cumulative_pct": 0.0, "grade": "A|B|C"}}
  ],
  "grade_summary": {{
    "A": {{"count": 0, "amount": 0, "pct": 0.0}},
    "B": {{"count": 0, "amount": 0, "pct": 0.0}},
    "C": {{"count": 0, "amount": 0, "pct": 0.0}}
  }},
  "forecast": [
    {{"product_name": "...", "current_monthly_avg": 0, "forecast_3m": [0, 0, 0], "trend": "증가|유지|감소"}}
  ],
  "management_strategies": [
    {{"grade": "A", "strategy": "...", "actions": ["..."]}}
  ],
  "recommendations": ["핵심 권고사항 1"]
}}
```""",

        "strategy": f"""{base_context}

## 분석 역할: 판매전략 에이전트
다음을 분석해줘:

### 1. 거래처별 맞춤 영업 전략
매출 규모와 거래 빈도 기반으로 거래처별 전략 제안

### 2. 교차판매/상향판매 기회
- 거래처가 구매한 품목 분석 → 연관 품목 제안
- 서버 구매 → 스토리지/UPS 제안 등

### 3. HaaS(Hardware as a Service) 전환 제안
단발 구매 중심 거래처에 구독형 전환 가능성

### 4. 파이프라인 분석
- 월별 매출 추이, 거래처 집중도

반드시 아래 JSON 형식으로 응답:
```json
{{
  "summary": "한줄 요약",
  "customer_strategies": [
    {{"customer_name": "...", "tier": "VIP|성장|일반", "current_amount": 0, "strategy": "...", "actions": ["..."], "expected_growth": "..."}}
  ],
  "cross_sell": [
    {{"customer_name": "...", "current_products": ["..."], "recommended": ["..."], "reason": "...", "expected_revenue": 0}}
  ],
  "haas_candidates": [
    {{"customer_name": "...", "current_purchase": "...", "monthly_subscription": 0, "reason": "..."}}
  ],
  "pipeline": {{
    "total_revenue": 0,
    "top_customer_concentration": 0.0,
    "monthly_trend": "증가|유지|감소",
    "risk_factors": ["..."]
  }},
  "recommendations": ["핵심 권고사항 1"]
}}
```""",

        "future": f"""{base_context}

## 분석 역할: 미래전략 에이전트

### IT 인프라 시장 메가트렌드:
1. 프라이빗 AI 인프라 (GPU 서버, 100G 네트워크, NVMe 스토리지) - 연 35% 성장
2. 엣지 컴퓨팅 (소형 서버, IoT, 마이크로 DC) - 연 28% 성장
3. 제로 트러스트 보안 (NGFW, NAC, ZTNA, SASE) - 연 22% 성장
4. SBOM/공급망 보안 - 연 40% 성장
5. HaaS 구독 모델 (CAPEX→OPEX) - 연 30% 성장

### 분석 요구사항:
1. 거래처-트렌드 매칭: 구매 이력 기반으로 어떤 트렌드 기회가 있는지
2. 신규 시장 기회: 현재 미진출 영역
3. 3년 성장 시나리오 (보수적 10%, 기본 20%, 공격적 35%)

반드시 아래 JSON 형식으로 응답:
```json
{{
  "summary": "한줄 요약",
  "trend_matching": [
    {{"customer_name": "...", "top_trends": [{{"trend": "...", "score": 0, "reason": "..."}}], "opportunity_value": 0}}
  ],
  "new_opportunities": [
    {{"trend": "...", "target_customers": ["..."], "estimated_market": 0, "entry_difficulty": "낮음|보통|높음"}}
  ],
  "growth_scenarios": {{
    "current_annual": 0,
    "conservative": {{"rate": 0.10, "year1": 0, "year2": 0, "year3": 0}},
    "moderate": {{"rate": 0.20, "year1": 0, "year2": 0, "year3": 0}},
    "aggressive": {{"rate": 0.35, "year1": 0, "year2": 0, "year3": 0}}
  }},
  "recommendations": ["핵심 권고사항 1"]
}}
```""",

        "partnership": f"""{base_context}

## 분석 역할: 파트너십 에이전트
다음을 분석해줘:

### 1. CLV(고객 생애 가치) / ACV(연간 계약 가치) 산출
- CLV = 평균 거래 금액 × 연간 빈도 × 예상 거래 기간(3년)
- 거래처별 CLV/ACV 랭킹

### 2. 거래처 티어 분류
- Tier 1 (상위 10%): 전담 AM + SE + CS
- Tier 2 (상위 30%): 담당 AM + 공유 SE
- Tier 3 (나머지): 인사이드 세일즈

### 3. ABM 타겟 선정 (Top 20)
### 4. 관계 강화 프로그램 제안

반드시 아래 JSON 형식으로 응답:
```json
{{
  "summary": "한줄 요약",
  "clv_results": [
    {{"customer_name": "...", "customer_code": "...", "annual_amount": 0, "frequency": 0, "clv": 0, "acv": 0, "tier": "Tier 1|Tier 2|Tier 3"}}
  ],
  "tier_summary": {{
    "Tier 1": {{"count": 0, "total_clv": 0, "customers": ["..."]}},
    "Tier 2": {{"count": 0, "total_clv": 0, "customers": ["..."]}},
    "Tier 3": {{"count": 0, "total_clv": 0, "customers": ["..."]}}
  }},
  "abm_targets": [
    {{"rank": 1, "customer_name": "...", "score": 0, "reason": "..."}}
  ],
  "relationship_programs": [
    {{"customer_name": "...", "current_level": "Vendor|Partner|Strategic", "target_level": "...", "actions": ["..."]}}
  ],
  "recommendations": ["핵심 권고사항 1"]
}}
```""",

        "visualization": f"""{base_context}

## 분석 역할: KPI/시각화 에이전트
다음 KPI를 산출하고 차트 데이터를 생성해줘:

### 1. 핵심 KPI 카드 (4개)
- 총 매출액, 거래처 수, 품목 수, 평균 거래 단가

### 2. 차트 데이터 생성
- 월별 매출 추이 (AreaChart 데이터)
- 거래처 Top 10 매출 (BarChart 데이터)
- 카테고리별 매출 비중 (PieChart 데이터)
- 거래처 매출 vs 빈도 산점도 (ScatterChart 데이터)
- 월별 거래 건수 추이 (LineChart 데이터)

### 3. 트렌드 요약

반드시 아래 JSON 형식으로 응답:
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
    "customer_scatter": [{{"name": "...", "amount": 0, "frequency": 0}}],
    "monthly_count": [{{"month": "2025-01", "count": 0}}]
  }},
  "trend_summary": "전체적인 매출 트렌드 요약...",
  "recommendations": ["핵심 권고사항 1"]
}}
```""",
    }

    return prompts.get(agent_key, "")


async def _call_claude(api_key: str, model: str, prompt: str, agent_name: str) -> dict:
    """Claude API 호출하여 에이전트 분석 실행"""
    start_time = time.time()

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    body = {
        "model": model,
        "max_tokens": 8000,
        "messages": [
            {"role": "user", "content": prompt}
        ],
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

        # 응답 텍스트 추출
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block["text"]

        elapsed = time.time() - start_time
        logger.info(f"[SalesAgent/{agent_name}] Claude 응답 수신 ({elapsed:.1f}s)")

        # JSON 파싱 시도
        result = _extract_json(text)
        if result:
            result["_raw_response"] = text[:500]  # 디버깅용 일부 보존
            result["_elapsed_seconds"] = round(elapsed, 1)
            return result
        else:
            # JSON 파싱 실패 시 텍스트 그대로 반환
            return {
                "summary": f"[{agent_name}] 분석 완료 (텍스트 형식)",
                "raw_text": text,
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
    # ```json ... ``` 블록 추출
    import re
    json_match = re.search(r'```json\s*\n(.*?)\n\s*```', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # 그냥 JSON 파싱 시도
    # { 로 시작하는 부분 찾기
    brace_start = text.find("{")
    if brace_start >= 0:
        # 마지막 } 찾기
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
    6개 에이전트를 병렬 실행하여 분석을 수행한다.

    Args:
        sales_data: parse_xlsx() 결과
        api_key: Anthropic API key
        model: Claude model name
        progress_callback: async callable(agent_key, status, pct) 진행 상태 콜백

    Returns:
        종합 분석 결과 dict
    """
    data_summary = _build_data_summary(sales_data)
    start_time = time.time()
    results = {}

    async def _run_agent(agent_key: str):
        if progress_callback:
            await progress_callback(agent_key, "running", 0)

        prompt = _get_agent_prompt(agent_key, data_summary)
        agent_result = await _call_claude(api_key, model, prompt, agent_key)
        results[agent_key] = agent_result

        if progress_callback:
            await progress_callback(agent_key, "done", 100)

        return agent_result

    # 6개 에이전트 병렬 실행
    agent_keys = list(AGENTS.keys())
    tasks = [_run_agent(k) for k in agent_keys]
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start_time

    # ── 종합 결과 구성 ──
    analysis_result = {
        "job_id": "",  # caller가 설정
        "status": "completed",
        "elapsed_seconds": round(elapsed, 1),
        "created_at": datetime.now().isoformat(),
        "sales_data_summary": sales_data.get("summary", {}),
        "period": {
            "start": sales_data.get("period_start", ""),
            "end": sales_data.get("period_end", ""),
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
            for rec in recs[:3]:  # 에이전트당 최대 3개
                all_recs.append(f"[{agent_name}] {rec}")
    analysis_result["top_recommendations"] = all_recs[:15]

    logger.info(f"[SalesAgent] 전체 분석 완료: {elapsed:.1f}초, {len(agent_keys)}개 에이전트")

    return analysis_result
