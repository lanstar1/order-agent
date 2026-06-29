"""
지마켓/옥션 ESM 주문 자동화 API 라우트
주문수집 → ERP 판매입력 → 발송처리
"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List

from fastapi import APIRouter, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from config import (
    ESM_MASTER_ID, ESM_SECRET_KEY, ESM_SELLER_ID_G, ESM_SELLER_ID_A,
    ESM_API_BASE,
    GMARKET_CUST_CODE, GMARKET_EMP_CODE, GMARKET_WH_CODE,
    ERP_COM_CODE, ERP_USER_ID, ERP_ZONE, ERP_API_KEY,
)
from services.gmarket_client import (
    ESMClient, DELIVERY_COMPANIES,
    SITE_GMARKET, SITE_AUCTION,
    ORDER_STATUS_PAID, ORDER_STATUS_READY,
)

KST = ZoneInfo("Asia/Seoul")
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gmarket", tags=["Gmarket/Auction"])

# ESM 클라이언트 싱글턴
_esm: ESMClient | None = None


def _get_esm() -> ESMClient:
    global _esm
    if _esm is None:
        _esm = ESMClient(
            master_id=ESM_MASTER_ID,
            secret_key=ESM_SECRET_KEY,
            seller_id_g=ESM_SELLER_ID_G,
            seller_id_a=ESM_SELLER_ID_A,
            base_url=ESM_API_BASE,
        )
    return _esm


# ─── 연결 테스트 ───
@router.get("/test-connection")
async def test_connection():
    """ESM API 및 ERP 연결 테스트"""
    results = {}

    # ESM API 테스트
    esm = _get_esm()
    esm_result = await esm.test_connection()
    results["esm"] = esm_result

    # ERP 테스트
    if ERP_COM_CODE and ERP_API_KEY:
        try:
            from services.erp_client import ERPClient
            erp = ERPClient()
            session = await erp.ensure_session()
            results["erp"] = {"ok": bool(session)}
        except Exception as e:
            results["erp"] = {"ok": False, "error": str(e)}
    else:
        results["erp"] = {"ok": False, "error": "ERP 설정 없음"}

    return results


# ─── 주문 수집 ───
@router.get("/orders")
async def get_orders(
    site: int = Query(SITE_GMARKET, description="1=옥션, 2=지마켓"),
    status: int = Query(ORDER_STATUS_PAID, description="1=결제완료, 2=배송준비중, 3=배송중"),
    from_date: str = Query("", description="시작일 (YYYY-MM-DD)"),
    to_date: str = Query("", description="종료일 (YYYY-MM-DD)"),
):
    """지마켓/옥션 주문 목록 조회"""
    esm = _get_esm()
    if not esm.master_id:
        return JSONResponse(
            status_code=400,
            content={"detail": "ESM API 키가 설정되지 않았습니다. 환경변수를 확인하세요."},
        )

    try:
        orders = await esm.fetch_orders(
            site_type=site,
            order_status=status,
            from_date=from_date,
            to_date=to_date,
        )
        return {"orders": orders, "total": len(orders), "site": site, "status": status}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})
    except Exception as e:
        logger.error(f"[지마켓] 주문조회 오류: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ─── 주문 확인 (결제완료 → 배송준비중) ───
class OrderConfirmRequest(BaseModel):
    order_nos: List[str]


@router.post("/confirm-orders")
async def confirm_orders(req: OrderConfirmRequest):
    """선택한 주문을 '배송준비중' 상태로 변경"""
    esm = _get_esm()
    results = []
    for order_no in req.order_nos:
        try:
            data = await esm.confirm_order(order_no)
            results.append({"order_no": order_no, "success": True, "data": data})
        except Exception as e:
            results.append({"order_no": order_no, "success": False, "error": str(e)})

    success = sum(1 for r in results if r["success"])
    return {"results": results, "total": len(results), "success_count": success}


# ─── ERP 판매전표 등록 ───
class ERPSendRequest(BaseModel):
    orders: list  # 주문 데이터 리스트
    io_date: str = ""  # 전표일자 (YYYY-MM-DD)


@router.post("/send-to-erp")
async def send_to_erp(req: ERPSendRequest):
    """주문 데이터를 ERP 판매전표로 등록"""
    if not ERP_COM_CODE or not ERP_API_KEY:
        return JSONResponse(status_code=400, content={"detail": "ERP 설정이 없습니다."})

    io_date = req.io_date or datetime.now(KST).strftime("%Y-%m-%d")

    try:
        from services.erp_client import ERPClient
        erp = ERPClient()
        session_id = await erp.ensure_session()
        if not session_id:
            return JSONResponse(status_code=500, content={"detail": "ERP 세션 획득 실패"})

        results = []
        for order in req.orders:
            try:
                # ERP 판매전표 생성
                sale_data = {
                    "IO_DATE": io_date,
                    "CUST_CD": GMARKET_CUST_CODE,
                    "EMP_CD": GMARKET_EMP_CODE,
                    "WH_CD": GMARKET_WH_CODE,
                    "PROD_CD": order.get("item_code", ""),
                    "QTY": order.get("quantity", 1),
                    "PRICE": order.get("price", 0),
                    "SUPPLY_AMT": order.get("price", 0) * order.get("quantity", 1),
                    "REMARKS1": f"지마켓#{order.get('order_no', '')}",
                }
                # TODO: 실제 ERP API 호출 추가
                results.append({
                    "order_no": order.get("order_no", ""),
                    "success": True,
                    "sale_data": sale_data,
                })
            except Exception as e:
                results.append({
                    "order_no": order.get("order_no", ""),
                    "success": False,
                    "error": str(e),
                })

        success = sum(1 for r in results if r["success"])
        return {"results": results, "total": len(results), "success_count": success}
    except Exception as e:
        logger.error(f"[지마켓] ERP 전송 오류: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ─── 발송 처리 ───
class ShipRequest(BaseModel):
    shipments: list  # [{"order_no": "...", "delivery_company": "로젠택배", "invoice_no": "..."}, ...]


@router.post("/ship")
async def ship_orders(req: ShipRequest):
    """발송 처리 (송장번호 입력)"""
    esm = _get_esm()
    results = []
    for s in req.shipments:
        company = s.get("delivery_company", "")
        company_code = DELIVERY_COMPANIES.get(company, s.get("delivery_company_code", ""))
        if not company_code:
            results.append({"order_no": s.get("order_no"), "success": False, "error": f"택배사코드 없음: {company}"})
            continue

        try:
            data = await esm.ship_order(
                order_no=s["order_no"],
                delivery_company_code=company_code,
                invoice_no=s["invoice_no"],
            )
            results.append({"order_no": s["order_no"], "success": True, "data": data})
        except Exception as e:
            results.append({"order_no": s["order_no"], "success": False, "error": str(e)})

    success = sum(1 for r in results if r["success"])
    return {"results": results, "total": len(results), "success_count": success}


# ─── 택배사 목록 ───
@router.get("/delivery-companies")
async def delivery_companies():
    """사용 가능한 택배사 목록"""
    return {"companies": [{"name": k, "code": v} for k, v in DELIVERY_COMPANIES.items()]}


# ─── 설정 상태 ───
@router.get("/config-status")
async def config_status():
    """현재 설정 상태 반환 (API 키 존재 여부 등)"""
    return {
        "esm_configured": bool(ESM_MASTER_ID and ESM_SECRET_KEY),
        "esm_master_id": ESM_MASTER_ID[:4] + "****" if ESM_MASTER_ID else "",
        "seller_gmarket": bool(ESM_SELLER_ID_G),
        "seller_auction": bool(ESM_SELLER_ID_A),
        "erp_configured": bool(ERP_COM_CODE and ERP_API_KEY),
        "gmarket_cust_code": GMARKET_CUST_CODE,
        "gmarket_wh_code": GMARKET_WH_CODE,
    }
