"""
AICC REST API 라우터
"""
import os
import httpx
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from services.aicc_data_loader import data_loader
from services.aicc_session_manager import session_manager
from security import get_current_user

router = APIRouter()


class AdminMessageBody(BaseModel):
    content: str


@router.get("/models")
async def get_models(q: str = ""):
    """드롭다운 자동완성용 모델 목록"""
    if q and len(q.strip()) >= 2:
        return data_loader.search_models(q.strip(), limit=15)
    # q 없으면 전체 반환 (초기 로드용)
    return data_loader.dropdown_models


@router.get("/sessions")
async def get_sessions(current_user=Depends(get_current_user)):
    """관리자: 전체 세션 목록"""
    return {"sessions": session_manager.all_serialized()}


@router.get("/sessions/{session_id}")
async def get_session(session_id: str, current_user=Depends(get_current_user)):
    """관리자: 세션 상세"""
    s = session_manager.get(session_id)
    if not s:
        raise HTTPException(404, "세션 없음")
    return session_manager.serialize(s)


@router.post("/sessions/{session_id}/intervene")
async def intervene(session_id: str, current_user=Depends(get_current_user)):
    """관리자 개입"""
    session_manager.intervene(session_id)
    await session_manager.send_customer(session_id, {
        "type": "admin_joined",
        "content": "담당자가 연결되었습니다. 직접 안내해 드리겠습니다."
    })
    return {"ok": True}


@router.post("/sessions/{session_id}/close")
async def close_session(session_id: str, current_user=Depends(get_current_user)):
    """세션 종료"""
    session_manager.close(session_id)
    await session_manager.send_customer(session_id, {
        "type": "session_closed",
        "content": "상담이 종료되었습니다. 감사합니다."
    })
    return {"ok": True}


@router.post("/sessions/{session_id}/admin-message")
async def admin_message(
    session_id: str,
    body: AdminMessageBody,
    current_user=Depends(get_current_user)
):
    """관리자 메시지 (WebSocket 미연결 시 REST 폴백)"""
    session_manager.add_message(session_id, "admin", body.content)
    await session_manager.send_customer(session_id, {
        "type": "admin_message",
        "content": body.content
    })
    return {"ok": True}


@router.get("/inventory/{model_name}")
async def get_inventory(model_name: str):
    """ERP 재고조회 — check_inventory() 호출 후 창고별 재고 반환"""
    erp_code = data_loader.get_erp_code(model_name)
    if not erp_code:
        return {"ok": False, "message": "해당 제품의 ERP 코드를 찾을 수 없습니다."}
    try:
        from services.erp_client import ERPClient
        erp = ERPClient()
        # check_inventory: 전체 창고 조회 (wh_cd="" → 모든 창고)
        result = await erp.check_inventory(prod_cd=erp_code, wh_cd="")
        if not result.get("success"):
            return {"ok": False, "message": result.get("error", "재고 조회 실패")}

        # 창고코드/창고명으로 용산·김포 분류
        yongsan = 0
        gimpo = 0
        other = 0
        for item in result.get("data", []):
            qty = int(item.get("qty", 0))
            wh = (item.get("wh_name", "") + item.get("wh_cd", "")).upper()
            if "용산" in wh or "YONGSAN" in wh or item.get("wh_cd") == "10":
                yongsan += qty
            elif "김포" in wh or "GIMPO" in wh or item.get("wh_cd") == "20":
                gimpo += qty
            else:
                other += qty

        total = yongsan + gimpo + other
        return {
            "ok": True,
            "model_name": model_name,
            "erp_code": erp_code,
            "yongsan": yongsan,
            "gimpo": gimpo,
            "total": total,
        }
    except Exception as e:
        import traceback
        print(f"[AICC Inventory] 오류: {e}\n{traceback.format_exc()}")
        return {"ok": False, "message": "재고 조회 중 오류가 발생했습니다. 전화(02-717-3386) 문의 바랍니다."}


# ── 주문상태코드 → 한글 ──────────────────────────────────────

ORDER_STATUS = {
    "o1": "입금대기",  "o2": "결제완료",  "o3": "상품준비중",
    "g1": "구매발주",  "g2": "구매발주",  "g3": "상품입고",
    "d1": "배송중",    "d2": "배송완료",  "p1": "구매확정",
    "r1": "환불접수",  "r2": "환불완료",
    "b1": "반품접수",  "b2": "반품완료",
    "e1": "교환접수",  "e2": "교환완료",
    "c1": "취소요청",  "c2": "취소완료",
}


@router.get("/orders")
async def get_customer_orders(phone: str):
    """
    고객 휴대폰번호로 최근 90일 주문 조회
    고도몰 Open API (관리자 Key 서버에서만 사용)
    """
    if not phone or len(phone.strip()) < 9:
        return {"orders": [], "message": "전화번호 오류"}

    end_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")

    payload = {
        "mallId": os.getenv("GODOMALL_MALL_ID"),
        "authorizationKey": os.getenv("GODOMALL_API_KEY"),
        "searchType": "orderCellPhone",
        "searchKeyword": phone.strip(),
        "startDate": start_date,
        "endDate": end_date,
        "dateType": "order",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            res = await http_client.post(
                "https://openhub.godo.co.kr/godomall5/order/Order_Search.php",
                data=payload
            )
        result = res.json()
    except Exception as e:
        return {"orders": [], "message": f"API 오류: {e}"}

    if str(result.get("code")) != "000":
        return {"orders": [], "message": f"조회 실패: {result.get('msg', '')}"}

    orders = []
    for order in result.get("order_data", []):
        order_no = order.get("orderNo", "")
        order_date = order.get("orderDate", "")[:10]
        settle_price = order.get("settlePrice", "")

        goods_list = order.get("orderGoodsData", [])
        goods_items = []
        primary_status = ""
        primary_status_text = ""

        for goods in goods_list:
            inv_no = goods.get("invoiceNo", "")
            status = goods.get("orderStatus", "")
            status_text = ORDER_STATUS.get(status, status)
            if not primary_status:
                primary_status = status
                primary_status_text = status_text
            goods_items.append({
                "goods_name": goods.get("goodsNm", ""),
                "order_status": status,
                "order_status_text": status_text,
                "invoice_company": goods.get("invoiceCompany", ""),
                "invoice_no": inv_no,
                "delivery_dt": (goods.get("deliveryDt") or "")[:10],
                "delivery_complete_dt": (goods.get("deliveryCompleteDt") or "")[:10],
                "tracking_url": f"https://www.ilogen.com/m/personal/trace/{inv_no}" if inv_no else "",
            })

        first_name = goods_items[0]["goods_name"] if goods_items else ""
        summary = first_name if len(goods_items) <= 1 else f"{first_name} 외 {len(goods_items)-1}건"

        orders.append({
            "order_no": order_no,
            "order_date": order_date,
            "settle_price": settle_price,
            "goods_summary": summary,
            "order_status": primary_status,
            "order_status_text": primary_status_text,
            "goods": goods_items,
        })

    orders.sort(key=lambda x: x["order_date"], reverse=True)
    return {"orders": orders, "total": len(orders)}
