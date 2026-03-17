"""
AICC REST API 라우터
"""
import os
import uuid
import base64
import httpx
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from services.aicc_data_loader import data_loader
from services.aicc_session_manager import session_manager
from security import get_current_user

router = APIRouter()


class AdminMessageBody(BaseModel):
    content: str


class ImageUploadBody(BaseModel):
    session_id: str
    image: str          # data:image/jpeg;base64,...
    file_name: str = "image.jpg"


ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}


@router.post("/upload")
async def upload_image(body: ImageUploadBody):
    """고객 이미지 업로드 — base64로 세션에 저장"""
    # 세션 확인
    s = None
    for sess in session_manager.sessions.values():
        if body.session_id in (sess.get("session_id", ""),):
            s = sess
            break
    # session_id가 프론트에서 보낸 client-side ID일 수 있으므로 유연하게 처리
    if not s:
        # 모든 세션을 확인하여 customer_ws가 연결된 세션 찾기
        for sess in session_manager.sessions.values():
            if sess.get("status") == "active":
                s = sess
                break
    if not s:
        return {"ok": False, "message": "세션을 찾을 수 없습니다."}

    # data URL 파싱: data:image/jpeg;base64,/9j/4AAQ...
    if not body.image.startswith("data:image/"):
        return {"ok": False, "message": "잘못된 이미지 형식입니다."}

    try:
        header, b64_data = body.image.split(",", 1)
        media_type = header.split(":")[1].split(";")[0]  # image/jpeg
    except (ValueError, IndexError):
        return {"ok": False, "message": "이미지 데이터 파싱 실패"}

    if media_type not in ALLOWED_IMAGE_TYPES:
        return {"ok": False, "message": f"지원하지 않는 이미지 형식: {media_type}"}

    # 크기 확인 (base64 디코딩 전 대략 체크: base64는 원본의 ~1.33배)
    if len(b64_data) > 7 * 1024 * 1024:  # ~5MB 원본
        return {"ok": False, "message": "이미지 크기가 너무 큽니다. (최대 5MB)"}

    image_id = str(uuid.uuid4())[:8]
    s["images"][image_id] = {
        "media_type": media_type,
        "base64_data": b64_data,
        "file_name": body.file_name,
    }

    return {"ok": True, "image_id": image_id, "session_id": s["session_id"]}


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


def _xml_text(el, tag: str) -> str:
    """XML 엘리먼트에서 태그 텍스트 추출 (None-safe)"""
    child = el.find(tag)
    return (child.text or "").strip() if child is not None else ""


@router.get("/orders")
async def get_customer_orders(memNo: str = "", phone: str = ""):
    """
    고도몰 Open API 주문조회
    - memNo(회원번호) 또는 phone(휴대폰번호)으로 필터링
    - API 제한: 최대 30일 단위 조회
    - 응답: XML → 파싱 후 JSON 반환
    """
    if not memNo and (not phone or len(phone.replace("-", "").strip()) < 9):
        return {"orders": [], "message": "회원 정보가 필요합니다. 로그인 후 이용해 주세요."}

    partner_key = os.getenv("GODOMALL_PARTNER_KEY", os.getenv("GODOMALL_MALL_ID", ""))
    user_key = os.getenv("GODOMALL_USER_KEY", os.getenv("GODOMALL_API_KEY", ""))

    end_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    payload = {
        "partner_key": partner_key,
        "key": user_key,
        "dateType": "order",
        "startDate": start_date,
        "endDate": end_date,
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as http_client:
            res = await http_client.post(
                "https://openhub.godo.co.kr/godomall5/order/Order_Search.php",
                data=payload,
            )
        root = ET.fromstring(res.text)
    except Exception as e:
        print(f"[AICC Orders] API 오류: {e}")
        return {"orders": [], "message": f"주문 조회 중 오류가 발생했습니다."}

    code = _xml_text(root, ".//code")
    if code != "000":
        msg = _xml_text(root, ".//msg")
        return {"orders": [], "message": f"조회 실패: {msg}"}

    # ── 전체 주문 데이터 파싱 + memNo/phone 필터링 ──
    phone_clean = phone.replace("-", "").strip() if phone else ""
    all_order_els = root.findall(".//order_data")
    orders = []

    for order_el in all_order_els:
        # memNo 필터
        if memNo:
            order_memNo = _xml_text(order_el, "memNo")
            if order_memNo != memNo:
                continue

        # phone 필터 (memNo 없을 때)
        if not memNo and phone_clean:
            info_el = order_el.find("orderInfoData")
            if info_el is not None:
                order_phone = _xml_text(info_el, "orderCellPhone").replace("-", "")
                receiver_phone = _xml_text(info_el, "receiverCellPhone").replace("-", "")
                if phone_clean not in (order_phone, receiver_phone):
                    continue
            else:
                continue

        order_no = _xml_text(order_el, "orderNo")
        order_date = _xml_text(order_el, "orderDate")[:10]
        settle_price = _xml_text(order_el, "settlePrice")
        order_status_top = _xml_text(order_el, "orderStatus")

        # orderGoodsData 파싱
        goods_els = order_el.findall("orderGoodsData")
        goods_items = []
        primary_status = ""
        primary_status_text = ""

        for g in goods_els:
            inv_no = _xml_text(g, "invoiceNo")
            status = _xml_text(g, "orderStatus")
            status_text = ORDER_STATUS.get(status, status)
            if not primary_status:
                primary_status = status
                primary_status_text = status_text
            goods_items.append({
                "goods_name": _xml_text(g, "goodsNm"),
                "goods_image": _xml_text(g, "listImageData"),
                "order_status": status,
                "order_status_text": status_text,
                "invoice_company": _xml_text(g, "invoiceCompany"),
                "invoice_no": inv_no,
                "delivery_dt": _xml_text(g, "deliveryDt")[:10],
                "delivery_complete_dt": _xml_text(g, "deliveryCompleteDt")[:10],
                "tracking_url": f"https://www.ilogen.com/m/personal/trace/{inv_no}" if inv_no else "",
            })

        if not primary_status:
            primary_status = order_status_top
            primary_status_text = ORDER_STATUS.get(order_status_top, order_status_top)

        first_name = goods_items[0]["goods_name"] if goods_items else _xml_text(order_el, "orderGoodsNm")
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

        if len(orders) >= 10:
            break

    orders.sort(key=lambda x: x["order_date"], reverse=True)
    return {"orders": orders, "total": len(orders)}
