"""
AI 품질 대시보드 API
- 토큰 사용량, STP율, 매칭 정확도 통계
- 거래처별 성능 비교
"""
import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from security import get_current_user
from services.ai_metrics import get_dashboard_stats, get_dynamic_threshold

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats")
async def dashboard_stats(
    days: int = Query(30, ge=1, le=365),
    user: dict = Depends(get_current_user),
):
    """AI 품질 대시보드 통계 (최근 N일)"""
    try:
        return get_dashboard_stats(days=days)
    except Exception as e:
        logger.error(f"[Dashboard] 통계 조회 실패: {e}", exc_info=True)
        # 빈 데이터 반환 (프론트엔드가 정상 렌더링 가능)
        return {
            "period_days": days,
            "total_calls": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "estimated_cost_usd": 0,
            "stp_rate": {"total_lines": 0, "auto_matched": 0, "stp_pct": 0},
            "auto_training_total": 0,
            "token_usage": {"total_tokens": 0, "cost_estimate_usd": 0, "by_model": {}},
            "matching": {"total_lines": 0, "auto_matched": 0, "stp_rate": 0, "total_orders": 0},
            "auto_training": {"sessions_count": 0},
            "customer_stats": [],
            "daily_trend": [],
            "_error": str(e),
        }


@router.get("/threshold/{cust_code}")
async def customer_threshold(
    cust_code: str,
    user: dict = Depends(get_current_user),
):
    """거래처별 동적 신뢰도 임계값 조회"""
    try:
        from config import CONFIDENCE_THRESHOLD
        threshold = get_dynamic_threshold(cust_code, CONFIDENCE_THRESHOLD)
        return {
            "cust_code": cust_code,
            "base_threshold": CONFIDENCE_THRESHOLD,
            "dynamic_threshold": threshold,
        }
    except Exception as e:
        logger.error(f"[Dashboard] 임계값 조회 실패: {e}", exc_info=True)
        raise HTTPException(500, f"임계값 조회 실패: {str(e)}")
