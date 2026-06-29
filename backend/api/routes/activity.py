"""
활동 로그 API 라우터 (관리자 전용)
- GET  /api/activity/logs      : 활동 로그 조회
- GET  /api/activity/summary   : 활동 요약 통계
- GET  /api/activity/employees : 활동 기록된 직원 목록
"""
import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user
from services.activity_service import get_activity_logs, get_activity_summary
from db.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/activity", tags=["activity"])

ADMIN_EMP_CDS = {"28", "01"}


def _require_admin(user: dict):
    """관리자 권한 확인"""
    if user.get("emp_cd") not in ADMIN_EMP_CDS:
        raise HTTPException(403, "관리자만 접근할 수 있습니다.")


@router.get("/logs")
def api_get_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=10, le=200),
    emp_cd: str = "",
    action: str = "",
    date_from: str = "",
    date_to: str = "",
    user: dict = Depends(get_current_user),
):
    """활동 로그 조회 (관리자 전용)"""
    _require_admin(user)
    return get_activity_logs(
        page=page, page_size=page_size,
        emp_cd=emp_cd, action=action,
        date_from=date_from, date_to=date_to,
    )


@router.get("/summary")
def api_get_summary(user: dict = Depends(get_current_user)):
    """활동 요약 통계 (관리자 전용)"""
    _require_admin(user)
    return get_activity_summary()


@router.get("/employees")
def api_get_activity_employees(user: dict = Depends(get_current_user)):
    """활동 기록이 있는 직원 목록"""
    _require_admin(user)
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT emp_cd, emp_name
        FROM activity_log
        ORDER BY emp_name
    """).fetchall()
    conn.close()
    return {"employees": [dict(r) for r in rows]}
