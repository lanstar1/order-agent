"""
판매현황 분석 API 라우터
"""
import logging
from fastapi import APIRouter, Depends, Query, UploadFile, File, BackgroundTasks
from security import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sales", tags=["Sales Analytics"])


def _get_svc():
    from services.sales_analytics_service import SalesAnalyticsService
    return SalesAnalyticsService()


# ══════════════════════════════════════════════
#  수집
# ══════════════════════════════════════════════

@router.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    content = await file.read()
    svc = _get_svc()
    result = await svc.import_csv_bytes(content)
    return result


@router.post("/fetch-ecount")
async def fetch_ecount(
    bg: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    svc = _get_svc()
    bg.add_task(svc.auto_fetch_from_ecount)
    return {"status": "started", "message": "이카운트 자동 수집이 백그라운드에서 시작되었습니다"}


@router.get("/fetch-status")
async def fetch_status(user: dict = Depends(get_current_user)):
    svc = _get_svc()
    return await svc.get_fetch_status()


@router.get("/scheduler/status")
async def scheduler_status(user: dict = Depends(get_current_user)):
    svc = _get_svc()
    return await svc.get_scheduler_status()


@router.post("/scheduler/run-now")
async def scheduler_run_now(
    bg: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    svc = _get_svc()
    bg.add_task(svc.auto_fetch_from_ecount)
    return {"status": "started"}


# ══════════════════════════════════════════════
#  일별 거래처 전일대비 판매 현황
# ══════════════════════════════════════════════

@router.get("/daily-compare")
async def get_daily_compare(
    date: str = Query(None, description="조회 날짜 (YYYY-MM-DD), 기본값: 오늘"),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_daily_compare(date)


# ══════════════════════════════════════════════
#  뷰1: 전체 대시보드
# ══════════════════════════════════════════════

@router.get("/summary")
async def get_summary(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_summary(date_from, date_to)


@router.get("/monthly-trend")
async def get_monthly_trend(
    months: int = Query(6), customer_name: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_monthly_trend(months, customer_name)


@router.get("/daily-trend")
async def get_daily_trend(
    date_from: str = Query(None), date_to: str = Query(None),
    customer_name: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_daily_trend(date_from, date_to, customer_name)


@router.get("/customers/ranking")
async def get_customer_ranking(
    date_from: str = Query(None), date_to: str = Query(None),
    limit: int = Query(15), group: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_customer_ranking(date_from, date_to, limit, group)


@router.get("/customers/list")
async def get_customer_list(user: dict = Depends(get_current_user)):
    return await _get_svc().get_customer_list()


@router.get("/customers/detail")
async def get_customer_detail(
    customer_name: str = Query(...),
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_customer_detail(customer_name, date_from, date_to)


@router.get("/customers/compare")
async def get_customer_compare(
    names: str = Query(...),
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    name_list = [n.strip() for n in names.split(",") if n.strip()]
    return await _get_svc().get_customer_compare(name_list, date_from, date_to)


@router.get("/customers/groups")
async def get_customer_groups(user: dict = Depends(get_current_user)):
    return await _get_svc().get_customer_groups()


@router.get("/products/ranking")
async def get_product_ranking(
    date_from: str = Query(None), date_to: str = Query(None),
    limit: int = Query(20), customer_name: str = Query(None),
    item_group: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_product_ranking(date_from, date_to, limit, customer_name, item_group)


@router.get("/products/groups")
async def get_product_groups(user: dict = Depends(get_current_user)):
    return await _get_svc().get_product_groups()


@router.get("/products/detail")
async def get_product_detail(
    item_code: str = Query(...),
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_product_detail(item_code, date_from, date_to)


# ══════════════════════════════════════════════
#  뷰2: 이익률 분석
# ══════════════════════════════════════════════

@router.get("/profit/analysis")
async def get_profit_analysis(
    date_from: str = Query(None), date_to: str = Query(None),
    group: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_profit_analysis(date_from, date_to, group)


@router.get("/profit/heatmap")
async def get_profit_heatmap(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_profit_heatmap(date_from, date_to)


# ══════════════════════════════════════════════
#  뷰3: 거래처 건강도
# ══════════════════════════════════════════════

@router.get("/health/customers")
async def get_customer_health(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_customer_health(date_from, date_to)


@router.get("/health/growth")
async def get_customer_growth(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_customer_growth(date_from, date_to)


# ══════════════════════════════════════════════
#  뷰4: 재고 위험도
# ══════════════════════════════════════════════

@router.get("/inventory/risk")
async def get_inventory_risk(
    months_back: int = Query(1),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_inventory_risk(months_back)


@router.get("/inventory/summary")
async def get_inventory_summary(user: dict = Depends(get_current_user)):
    return await _get_svc().get_inventory_summary()


# ══════════════════════════════════════════════
#  뷰5: 담당자 성과
# ══════════════════════════════════════════════

@router.get("/staff/performance")
async def get_staff_performance(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_staff_performance(date_from, date_to)


# ══════════════════════════════════════════════
#  뷰6: 반품·이슈 관리
# ══════════════════════════════════════════════

@router.get("/returns/analysis")
async def get_returns_analysis(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_returns_analysis(date_from, date_to)


# ══════════════════════════════════════════════
#  뷰7: 단가 일관성
# ══════════════════════════════════════════════

@router.get("/price/inconsistency")
async def get_price_inconsistency(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_price_inconsistency(date_from, date_to)


@router.get("/price/standards")
async def get_price_standards(user: dict = Depends(get_current_user)):
    return await _get_svc().get_price_standards()


@router.post("/price/standards")
async def set_price_standard(
    body: dict,
    user: dict = Depends(get_current_user),
):
    return await _get_svc().set_price_standard(
        body["item_code"], body.get("customer_name", ""),
        body["standard_price"], body.get("tolerance_pct", 10.0),
    )


# ══════════════════════════════════════════════
#  뷰8: 창고×채널
# ══════════════════════════════════════════════

@router.get("/warehouse/channel")
async def get_warehouse_channel(
    date_from: str = Query(None), date_to: str = Query(None),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_warehouse_channel(date_from, date_to)


# ══════════════════════════════════════════════
#  에이전트
# ══════════════════════════════════════════════

@router.get("/agents/alerts")
async def get_alerts(
    is_read: bool = Query(None), limit: int = Query(50),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_alerts(is_read, limit)


@router.post("/agents/alerts/{alert_id}/read")
async def mark_alert_read(
    alert_id: int,
    user: dict = Depends(get_current_user),
):
    return await _get_svc().mark_alert_read(alert_id)


@router.post("/agents/run")
async def run_agents(
    bg: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    svc = _get_svc()
    bg.add_task(svc.run_all_agents)
    return {"status": "started", "message": "에이전트 3종 실행 시작"}


@router.get("/agents/ai-analysis")
async def get_ai_analysis(
    customer_name: str = Query(...),
    user: dict = Depends(get_current_user),
):
    return await _get_svc().get_ai_customer_analysis(customer_name)
