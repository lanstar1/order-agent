"""
비용 모니터링 및 사용량 통계
- Job별 LLM 비용 추적
- 일별/월별 사용량 집계
- 예산 초과 경고
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# 인메모리 비용 트래커 (Phase 4에서 DB 전환)
_cost_log: List[Dict] = []


def log_cost(
    job_id: str,
    model: str,
    tokens_input: int,
    tokens_output: int,
    cost: float,
    task_key: str = "",
):
    """비용 기록"""
    _cost_log.append({
        "timestamp": datetime.now().isoformat(),
        "job_id": job_id,
        "model": model,
        "tokens_input": tokens_input,
        "tokens_output": tokens_output,
        "cost": cost,
        "task_key": task_key,
    })


def get_cost_summary(days: int = 30) -> Dict[str, Any]:
    """기간별 비용 요약"""
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff.isoformat()

    recent = [e for e in _cost_log if e["timestamp"] >= cutoff_str]

    total_cost = sum(e["cost"] for e in recent)
    total_tokens = sum(e["tokens_input"] + e["tokens_output"] for e in recent)
    job_count = len(set(e["job_id"] for e in recent))

    # 모델별 집계
    model_costs = {}
    for e in recent:
        m = e["model"]
        if m not in model_costs:
            model_costs[m] = {"cost": 0, "tokens": 0, "calls": 0}
        model_costs[m]["cost"] += e["cost"]
        model_costs[m]["tokens"] += e["tokens_input"] + e["tokens_output"]
        model_costs[m]["calls"] += 1

    # 일별 추이
    daily = {}
    for e in recent:
        day = e["timestamp"][:10]
        if day not in daily:
            daily[day] = {"cost": 0, "jobs": set(), "tokens": 0}
        daily[day]["cost"] += e["cost"]
        daily[day]["jobs"].add(e["job_id"])
        daily[day]["tokens"] += e["tokens_input"] + e["tokens_output"]

    daily_trend = [
        {"date": d, "cost": round(v["cost"], 4), "jobs": len(v["jobs"]), "tokens": v["tokens"]}
        for d, v in sorted(daily.items())
    ]

    return {
        "period_days": days,
        "total_cost": round(total_cost, 4),
        "total_tokens": total_tokens,
        "total_jobs": job_count,
        "total_calls": len(recent),
        "avg_cost_per_job": round(total_cost / max(job_count, 1), 4),
        "model_breakdown": model_costs,
        "daily_trend": daily_trend[-30:],
    }


def check_budget(monthly_budget: float = 10.0) -> Dict[str, Any]:
    """월간 예산 체크"""
    summary = get_cost_summary(days=30)
    used = summary["total_cost"]
    remaining = monthly_budget - used
    pct_used = (used / monthly_budget * 100) if monthly_budget > 0 else 0

    return {
        "budget": monthly_budget,
        "used": round(used, 4),
        "remaining": round(remaining, 4),
        "pct_used": round(pct_used, 1),
        "warning": pct_used > 80,
        "exceeded": pct_used > 100,
    }
