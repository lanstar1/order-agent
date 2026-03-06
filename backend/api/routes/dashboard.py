"""
AI 품질 대시보드 API
- 토큰 사용량, STP율, 매칭 정확도 통계
- 거래처별 성능 비교
"""
import logging
from fastapi import APIRouter, Depends, Query
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
    return get_dashboard_stats(days=days)


@router.get("/threshold/{cust_code}")
async def customer_threshold(
    cust_code: str,
    user: dict = Depends(get_current_user),
):
    """거래처별 동적 신뢰도 임계값 조회"""
    from config import CONFIDENCE_THRESHOLD
    threshold = get_dynamic_threshold(cust_code, CONFIDENCE_THRESHOLD)
    return {
        "cust_code": cust_code,
        "base_threshold": CONFIDENCE_THRESHOLD,
        "dynamic_threshold": threshold,
    }
