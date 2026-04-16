"""
쿠팡 OPEN API 주문 자동화 라우트
발주서조회 → 상품준비중 → ERP 판매입력 → 송장업로드(발송처리)
"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List

from fastapi import APIRouter, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from config import (
    COUPANG_ACCESS_KEY, COUPANG_SECRET_KEY, COUPANG_VENDOR_ID,
    COUPANG_API_BASE,
    COUPANG_CUST_CODE, COUPANG_EMP_CODE, COUPANG_WH_CODE,
    ERP_COM_CODE, ERP_USER_ID, ERP_ZONE, ERP_API_KEY,
)
from services.coupang_client import (
    CoupangClient, DELIVERY_COMPANIES,
    ORDER_STATUS_ACCEPT, ORDER_STATUS_INSTRUCT,
    ORDER_STATUS_DEPARTURE, ORDER_STATUS_DELIVERING,
)

KST = ZoneInfo("Asia/Seoul")
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/coupang", tags=["Coupang"])

# 쿠팡 클라이언트 싱글턴
_coupang: CoupangClient | None = None


def _get_coupang() -> CoupangClient:
    global _coupang
    if _coupang is None:
        _coupang = CoupangClient(
            access_key=COUPANG_ACCESS_KEY,
            secret_key=COUPANG_SECRET_KEY,
            vendor_id=COUPANG_VENDOR_ID,
            base_url=COUPANG_API_BASE,
        )
    return _coupang


# ─── 연결 테스트 ───
@router.get("/test-connection")
async def test_connection():
    """쿠팡 API 및 ERP 연결 테스트"""
    results = {}

    # 쿠팡 API 테스트
    cp = _get_coupang()
    cp_result = await cp.test_connection()
    results["coupang"] = cp_result

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


# ─── 발주서 목록 조회 (주문수집) ───
@router.get("/orders")
async def get_orders(
    status: str = Query(ORDER_STATUS_ACCEPT, description="ACCEPT/INSTRUCT/DEPARTURE/DELIVERING"),
    from_date: str = Query("", description="시작일 (YYYY-MM-DD)"),
    to_date: str = Query("", description="종료일 (YYYY-MM-DD)"),
):
    """쿠팡 발주서 목록 조회"""
    cp = _get_coupang()
    if not cp.access_key:
        return JSONResponse(
            status_code=400,
            content={"detail": "쿠팡 API 키가 설정되지 않았습니다. 환경변수를 확인하세요."},
        )

    try:
        orders = await cp.fetch_orders(
            status=status,
            from_date=from_date,
            to_date=to_date,
        )
        return {"orders": orders, "total": len(orders), "status": status}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})
    except Exception as e:
        logger.error(f"[쿠팡] 주문조회 오류: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ─── 상품준비중 처리 (발주확인) ───
class ConfirmRequest(BaseModel):
    shipment_box_ids: List[int]


@router.post("/confirm-orders")
async def confirm_orders(req: ConfirmRequest):
    """선택한 발주서를 '상품준비중' 상태로 변경"""
    cp = _get_coupang()
    try:
        # 50개씩 나눠서 처리
        all_results = []
        ids = req.shipment_box_ids
        for i in range(0, len(ids), 50):
            batch = ids[i:i+50]
            result = await cp.confirm_orders(batch)
            all_results.append(result)

        return {"results": all_results, "total": len(ids)}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})
    except Exception as e:
        logger.error(f"[쿠팡] 상품준비중 처리 오류: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": str(e)})


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
                # 주문 아이템 개별 처리
                order_items = order.get("orderItems", [])
                for item in order_items:
                    sale_data = {
                        "IO_DATE": io_date,
                        "CUST_CD": COUPANG_CUST_CODE,
                        "EMP_CD": COUPANG_EMP_CODE,
                        "WH_CD": COUPANG_WH_CODE,
                        "PROD_CD": item.get("externalVendorSku", ""),
                        "QTY": item.get("shippingCount", 1),
                        "PRICE": item.get("salesPrice", {}).get("units", 0),
                        "SUPPLY_AMT": item.get("orderPrice", {}).get("units", 0),
                        "REMARKS1": f"쿠팡#{order.get('orderId', '')}",
                    }
                    results.append({
                        "order_id": order.get("orderId", ""),
                        "vendor_item_id": item.get("vendorItemId", ""),
                        "success": True,
                        "sale_data": sale_data,
                    })
            except Exception as e:
                results.append({
                    "order_id": order.get("orderId", ""),
                    "success": False,
                    "error": str(e),
                })

        success = sum(1 for r in results if r.get("success"))
        return {"results": results, "total": len(results), "success_count": success}
    except Exception as e:
        logger.error(f"[쿠팡] ERP 전송 오류: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ─── 송장업로드 (발송처리) ───
class ShipRequest(BaseModel):
    shipments: list  # [{"shipment_box_id": ..., "order_id": ..., "vendor_item_id": ..., "delivery_company": "CJ대한통운", "invoice_number": "..."}, ...]


@router.post("/ship")
async def ship_orders(req: ShipRequest):
    """송장업로드 처리 (발송처리)"""
    cp = _get_coupang()
    results = []
    for s in req.shipments:
        company = s.get("delivery_company", "")
        company_code = DELIVERY_COMPANIES.get(company, s.get("delivery_company_code", ""))
        if not company_code:
            results.append({
                "shipment_box_id": s.get("shipment_box_id"),
                "success": False,
                "error": f"택배사코드 없음: {company}",
            })
            continue

        try:
            invoice_data = [{
                "shipmentBoxId": s["shipment_box_id"],
                "orderId": s["order_id"],
                "vendorItemId": s["vendor_item_id"],
                "deliveryCompanyCode": company_code,
                "invoiceNumber": s["invoice_number"],
                "splitShipping": s.get("split_shipping", False),
                "preSplitShipped": s.get("pre_split_shipped", False),
                "estimatedShippingDate": s.get("estimated_shipping_date", ""),
            }]
            data = await cp.upload_invoice(invoice_data)
            results.append({
                "shipment_box_id": s["shipment_box_id"],
                "success": True,
                "data": data,
            })
        except Exception as e:
            results.append({
                "shipment_box_id": s.get("shipment_box_id"),
                "success": False,
                "error": str(e),
            })

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
    """현재 설정 상태 반환"""
    return {
        "coupang_configured": bool(COUPANG_ACCESS_KEY and COUPANG_SECRET_KEY),
        "coupang_access_key": COUPANG_ACCESS_KEY[:4] + "****" if COUPANG_ACCESS_KEY else "",
        "vendor_id": COUPANG_VENDOR_ID,
        "erp_configured": bool(ERP_COM_CODE and ERP_API_KEY),
        "coupang_cust_code": COUPANG_CUST_CODE,
        "coupang_wh_code": COUPANG_WH_CODE,
    }
