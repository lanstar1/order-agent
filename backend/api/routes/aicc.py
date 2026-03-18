"""
AICC REST API 라우터
"""
import json
import os
import uuid
import base64
import httpx
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel
from services.aicc_data_loader import data_loader
from services.aicc_session_manager import session_manager
from services import aicc_db
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
    """관리자: 현재 활성 세션 목록 (인메모리만 — 깜빡임 방지)"""
    return {"sessions": session_manager.all_serialized()}


@router.get("/sessions/{session_id}")
async def get_session(session_id: str, current_user=Depends(get_current_user)):
    """관리자: 세션 상세 (인메모리 or DB 조회)"""
    s = session_manager.get(session_id)
    if s:
        return session_manager.serialize(s)

    # 인메모리에 없으면 DB에서 조회
    db_sessions = aicc_db.get_all_sessions(limit=500)
    db_session = next((ds for ds in db_sessions if ds["id"] == session_id), None)
    if not db_session:
        raise HTTPException(404, "세션 없음")

    # DB에서 메시지도 가져오기
    db_messages = aicc_db.get_session_messages(session_id)
    return {
        "session_id": db_session["id"],
        "customer_name": db_session.get("customer_name", ""),
        "selected_model": db_session.get("selected_model", ""),
        "erp_code": db_session.get("erp_code", ""),
        "selected_menu": db_session.get("selected_menu", ""),
        "status": db_session.get("status", "closed"),
        "is_admin_intervened": False,
        "messages": [{"role": m["role"], "content": m["content"], "timestamp": m.get("created_at", "")} for m in db_messages],
        "created_at": db_session.get("created_at", ""),
        "updated_at": db_session.get("updated_at", ""),
        "from_db": True,
    }


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

        # 디버깅: 실제 ERP 응답 데이터 출력
        for item in result.get("data", []):
            print(f"[AICC Inventory DEBUG] wh_cd='{item.get('wh_cd')}', wh_name='{item.get('wh_name')}', qty={item.get('qty')}")

        # 창고코드/창고명으로 용산·김포 분류
        yongsan = 0
        gimpo = 0
        other = 0
        for item in result.get("data", []):
            qty = int(float(item.get("qty", 0)))
            wh_cd = str(item.get("wh_cd", "")).strip()
            wh_name = str(item.get("wh_name", "")).strip()
            if "용산" in wh_name or wh_cd == "10":
                yongsan += qty
            elif "김포" in wh_name or wh_cd == "20":
                gimpo += qty
            else:
                other += qty

        total = yongsan + gimpo + other
        print(f"[AICC Inventory DEBUG] 결과: 용산={yongsan}, 김포={gimpo}, 기타={other}, 총={total}")
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


# ── 제품 지식 DB 관리 API ────────────────────────────────

class ProductKnowledgeBody(BaseModel):
    model_name: str
    category: str = ""
    data: Dict[str, Any]


class BulkProductKnowledgeBody(BaseModel):
    products: Dict[str, Dict[str, Any]]


@router.post("/knowledge")
async def upsert_knowledge(
    body: ProductKnowledgeBody,
    current_user=Depends(get_current_user)
):
    """제품 지식 단건 등록/수정"""
    aicc_db.upsert_product_knowledge(body.model_name, body.category, body.data)
    return {"ok": True, "model_name": body.model_name}


@router.post("/knowledge/bulk")
async def bulk_upload_knowledge(
    body: BulkProductKnowledgeBody,
    current_user=Depends(get_current_user),
):
    """
    제품 지식 일괄 등록 — JSON body
    형식: { "products": { "LS-ANDOOR-S": { "카테고리": "도어락", ... }, ... } }
    """
    count = aicc_db.bulk_upsert_product_knowledge(body.products)
    return {"ok": True, "count": count, "message": f"{count}개 제품 지식 등록 완료"}


@router.post("/knowledge/upload")
async def upload_knowledge_file(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user),
):
    """
    제품 지식 JSON 파일 업로드
    파일 형식: { "LS-ANDOOR-S": { "카테고리": "도어락", ... }, ... }
    """
    content = await file.read()
    try:
        products = json.loads(content.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "JSON 파일 파싱 실패")

    if not isinstance(products, dict):
        raise HTTPException(400, "JSON 최상위는 객체여야 합니다 (모델명: {데이터})")

    count = aicc_db.bulk_upsert_product_knowledge(products)
    return {"ok": True, "count": count, "message": f"{count}개 제품 지식 등록 완료"}


@router.get("/knowledge")
async def list_knowledge(current_user=Depends(get_current_user)):
    """전체 제품 지식 목록"""
    items = aicc_db.get_all_product_knowledge()
    return {"items": items, "total": len(items)}


@router.get("/knowledge-count")
async def knowledge_count():
    """제품 지식 DB 총 개수 (인증 불필요, 디버깅용)"""
    items = aicc_db.get_all_product_knowledge()
    return {"total": len(items)}


@router.get("/knowledge/{model_name}")
async def get_knowledge(model_name: str, current_user=Depends(get_current_user)):
    """특정 제품 지식 상세"""
    item = aicc_db.get_product_knowledge(model_name)
    if not item:
        raise HTTPException(404, "해당 제품 지식이 없습니다")
    return item


@router.get("/knowledge-check/{model_name}")
async def check_knowledge(model_name: str):
    """제품 지식 존재 여부 확인 (인증 불필요)"""
    item = aicc_db.get_product_knowledge(model_name)
    if not item:
        return {"exists": False, "model_name": model_name}
    return {"exists": True, "model_name": model_name, "keys": list(item.get("data", {}).keys())}


@router.delete("/knowledge/{model_name}")
async def delete_knowledge(model_name: str, current_user=Depends(get_current_user)):
    """제품 지식 삭제"""
    aicc_db.delete_product_knowledge(model_name)
    return {"ok": True}


# ── 채팅 이력 조회 API ───────────────────────────────────

@router.get("/history/sessions")
async def get_chat_history_sessions(current_user=Depends(get_current_user)):
    """저장된 전체 AICC 채팅 세션 목록"""
    sessions = aicc_db.get_all_sessions(limit=200)
    return {"sessions": sessions, "total": len(sessions)}


@router.get("/history/sessions/{session_id}")
async def get_chat_history_messages(
    session_id: str,
    current_user=Depends(get_current_user)
):
    """특정 세션의 전체 채팅 메시지"""
    messages = aicc_db.get_session_messages(session_id)
    return {"session_id": session_id, "messages": messages, "total": len(messages)}


# ── 미답변 관리 API ───────────────────────────────────

@router.get("/unanswered")
async def get_unanswered_list(
    resolved: bool = False,
    current_user=Depends(get_current_user)
):
    """미답변 목록 조회"""
    items = aicc_db.get_unanswered(resolved=resolved)
    return {"items": items, "total": len(items)}


@router.get("/unanswered/count")
async def get_unanswered_count(current_user=Depends(get_current_user)):
    """미해결 미답변 수"""
    return {"count": aicc_db.count_unanswered()}


@router.post("/unanswered/{item_id}/resolve")
async def resolve_unanswered(
    item_id: int,
    current_user=Depends(get_current_user)
):
    """미답변 해결 처리"""
    aicc_db.resolve_unanswered(item_id)
    return {"ok": True}


class AddKnowledgeFromUnanswered(BaseModel):
    model_name: str
    key: str       # 예: "손잡이_들뜸_해결"
    value: str     # 예: "사각봉을 35mm용으로 교체하세요"


@router.post("/unanswered/{item_id}/add-knowledge")
async def add_knowledge_from_unanswered(
    item_id: int,
    body: AddKnowledgeFromUnanswered,
    current_user=Depends(get_current_user)
):
    """미답변에서 직접 제품 지식 DB에 항목 추가"""
    existing = aicc_db.get_product_knowledge(body.model_name)
    if existing:
        data = existing["data"]
    else:
        data = {"카테고리": ""}

    # 기존 데이터에 새 키 추가
    if body.key in data and isinstance(data[body.key], list):
        data[body.key].append(body.value)
    elif body.key in data and isinstance(data[body.key], str):
        data[body.key] = [data[body.key], body.value]
    else:
        data[body.key] = body.value

    aicc_db.upsert_product_knowledge(body.model_name, data.get("카테고리", ""), data)
    aicc_db.resolve_unanswered(item_id, admin_note=f"DB 추가: {body.key}")
    return {"ok": True, "model_name": body.model_name, "key": body.key}


class DirectKnowledgeAdd(BaseModel):
    model_name: str
    key: str       # 예: "FAQ", "비밀번호등록"
    value: str     # 단일 텍스트 또는 줄바꿈 구분 리스트


@router.post("/knowledge/add-direct")
async def add_knowledge_direct(
    body: DirectKnowledgeAdd,
    current_user=Depends(get_current_user)
):
    """관리자가 직접 제품 지식 DB에 항목 추가 (미답변 연동 없음)"""
    # 모델명이 실제 존재하는지 확인
    found = data_loader.search_models(body.model_name, limit=1)
    if not found or found[0]["model_name"] != body.model_name:
        # 정확히 일치하지 않아도 DB에는 추가 가능 (이미 등록된 제품일 수 있음)
        pass

    existing = aicc_db.get_product_knowledge(body.model_name)
    if existing:
        data = existing["data"]
        category = existing.get("category", data.get("카테고리", ""))
    else:
        data = {"카테고리": ""}
        category = ""

    # 줄바꿈이 있으면 리스트로 변환
    lines = [l.strip() for l in body.value.split("\n") if l.strip()]
    new_value = lines if len(lines) > 1 else body.value.strip()

    # 기존 데이터에 추가/병합
    if body.key in data and isinstance(data[body.key], list):
        if isinstance(new_value, list):
            data[body.key].extend(new_value)
        else:
            data[body.key].append(new_value)
    elif body.key in data and isinstance(data[body.key], str) and data[body.key]:
        if isinstance(new_value, list):
            data[body.key] = [data[body.key]] + new_value
        else:
            data[body.key] = [data[body.key], new_value]
    else:
        data[body.key] = new_value

    aicc_db.upsert_product_knowledge(body.model_name, category, data)
    return {"ok": True, "model_name": body.model_name, "key": body.key}
