"""
스마트스토어 주문 자동화 API 라우트
네이버 주문수집 → ERP 판매입력 → 로젠택배 등록 → 발송처리
"""
import re
import json
import asyncio
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Query, HTTPException, Body, UploadFile, File

KST = ZoneInfo("Asia/Seoul")

from config import (
    SMARTSTORE_CUST_CODE, SMARTSTORE_EMP_CODE, SMARTSTORE_WH_CODE,
    SMARTSTORE_PRODUCT_MAP_PATH, SMARTSTORE_MODEL_MAP_PATH,
    SMARTSTORE_OPTION_MAP_PATH, SMARTSTORE_ADDON_MAP_PATH,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/smartstore", tags=["SmartStore"])

# 모델코드 정규식
MODEL_CODE_RE = re.compile(r"(LS[PNE]?-[\w\-]+|ZOT-[\w\-]+)", re.IGNORECASE)
EXCLUDE_KEYWORDS = ["허브랙", "서버랙", "캐비넷"]

# 시트1: 메인상품 — 상품번호 → ERP품목코드 (옵션 없는 상품)
_product_map: dict = {}
# 시트2: 옵션상품 — 상품번호 → ERP품목코드 (자동추출 오버라이드)
_option_override_map: dict = {}
# 시트3: 추가상품 — 상품번호 → ERP품목코드
_addon_map: dict = {}
# 모델명: 상품번호 → 모델명 (로젠 송장용, 전 시트 공용)
_model_map: dict = {}
# 역방향: 모델명 → ERP품목코드 (옵션 텍스트에서 모델명 추출 시 ERP코드로 변환)
_model_to_erp_map: dict = {}


def _load_json(path) -> dict:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_json(path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_product_map():
    global _product_map, _option_override_map, _addon_map, _model_map, _model_to_erp_map
    _product_map        = _load_json(SMARTSTORE_PRODUCT_MAP_PATH)
    _option_override_map = _load_json(SMARTSTORE_OPTION_MAP_PATH)
    _addon_map          = _load_json(SMARTSTORE_ADDON_MAP_PATH)
    _model_map          = _load_json(SMARTSTORE_MODEL_MAP_PATH)
    # 역방향 맵 빌드: 모델명 → ERP품목코드 (전 시트 포함)
    _model_to_erp_map = {}
    for pno, model in _model_map.items():
        if model:
            if pno in _product_map:
                _model_to_erp_map[model] = _product_map[pno]
            elif pno in _option_override_map:
                _model_to_erp_map[model] = _option_override_map[pno]
            elif pno in _addon_map:
                _model_to_erp_map[model] = _addon_map[pno]
    logger.info(
        f"[SS] 매핑 로드 — 메인:{len(_product_map)} 옵션:{len(_option_override_map)} "
        f"추가:{len(_addon_map)} 모델역방향:{len(_model_to_erp_map)}"
    )


def _save_product_map():
    _save_json(SMARTSTORE_PRODUCT_MAP_PATH, _product_map)
    _save_json(SMARTSTORE_OPTION_MAP_PATH,  _option_override_map)
    _save_json(SMARTSTORE_ADDON_MAP_PATH,   _addon_map)
    _save_json(SMARTSTORE_MODEL_MAP_PATH,   _model_map)


_load_product_map()


def _extract_erp_code_from_option(option_text: str) -> Optional[str]:
    """
    옵션 텍스트에서 ERP 품목코드 추출.
    우선순위:
      1) 마지막 (코드) — 정상/비정상 괄호 모두 처리
      2) 콜론 뒤 코드
    유효 코드 기준: 영문·숫자로 시작, 공백 없음
    """
    if not option_text:
        return None

    text = option_text.strip()

    # '/ 배송방법:' 이후 제거 (예: "LS-750HS / 배송방법: 경동택배 (원하는 배송지 도착)")
    if " / " in text:
        text = text.split(" / ")[0].strip()

    def is_valid_code(s: str) -> bool:
        # 영문·숫자 시작, 공백 없음, 최소 3자 이상 (단일 문자 오탐 방지)
        return bool(s and " " not in s and len(s) >= 3 and re.match(r"[A-Za-z0-9]", s))

    # 패턴 1: 마지막 (코드) — 닫힌 괄호
    m = re.search(r"\(([^()]*(?:\([^)]*\))[^()]*|[^()]+)\)\s*$", text)
    if m:
        candidate = m.group(1).strip()
        if is_valid_code(candidate):
            return candidate

    # 패턴 1-b: 닫히지 않은 괄호 (예: (LS-ADOOR(B) )
    m2 = re.search(r"\(([A-Za-z0-9][A-Za-z0-9\-\(\)\.]*)\s*$", text)
    if m2:
        candidate = m2.group(1).strip()
        if is_valid_code(candidate):
            return candidate

    # 패턴 2: 콜론 뒤 코드
    if ":" in text:
        after_colon = text.rsplit(":", 1)[1].strip()
        if is_valid_code(after_colon):
            return after_colon

    # 패턴 3: 옵션 텍스트 전체가 코드 (예: "LS-420HM", "LS-UHS2SR", "LS-WPCOP-C6")
    if is_valid_code(text):
        return text

    return None


def _match_item_code(order: dict) -> Optional[str]:
    """
    ERP 품목코드 결정 우선순위:
      1) 시트2 오버라이드 (옵션상품, 사용자 수동 지정)
      2) 옵션 텍스트 자동 추출
      3) 시트1 (메인상품, 상품번호 기준)
      4) 시트3 (추가상품, 상품번호 기준)
    """
    option_text = (order.get("optionInfo", "") or "").strip()
    product_no  = str(order.get("productNo", "") or order.get("productId", "") or "")

    if option_text:
        # 1) 시트2 오버라이드
        if product_no and product_no in _option_override_map:
            code = _option_override_map[product_no]
            logger.info(f"[SS] 옵션오버라이드(시트2): {product_no} → {code}")
            return code
        # 2) 자동 추출 (모델명 추출 시 역방향 맵으로 ERP품목코드 변환)
        code = _extract_erp_code_from_option(option_text)
        if code:
            erp_code = _model_to_erp_map.get(code, code)
            logger.info(f"[SS] 옵션자동추출: '{option_text[:40]}' → 모델:{code} → ERP:{erp_code}")
            return erp_code

    # 3) 시트1 메인상품
    if product_no and product_no in _product_map:
        code = _product_map[product_no]
        logger.info(f"[SS] 메인상품(시트1): {product_no} → {code}")
        return code

    # 4) 시트3 추가상품
    if product_no and product_no in _addon_map:
        code = _addon_map[product_no]
        logger.info(f"[SS] 추가상품(시트3): {product_no} → {code}")
        return code

    seller_code = (order.get("sellerProductCode", "") or "").strip()
    logger.warning(
        f"[SS] 매칭 실패: productNo={product_no}, option='{option_text[:30]}', "
        f"sellerCode={seller_code}, name={order.get('productName','')[:40]}"
    )
    return None


def _is_excluded(order: dict) -> bool:
    # 중첩 구조(_rawOrders: {productOrder: {...}})와 평탄화 구조(그룹 내부 dict) 모두 지원
    po = order.get("productOrder") or {}
    name   = po.get("productName", "")   or order.get("productName", "")   or ""
    option = po.get("productOption", "") or po.get("optionInfo", "") or \
             order.get("optionInfo", "") or order.get("productOption", "") or ""
    combined = (name + " " + option).lower()
    return any(kw in combined for kw in EXCLUDE_KEYWORDS)


def _build_goods_nm(orders_in_group: list[dict]) -> str:
    model_qty = {}
    for o in orders_in_group:
        code = _match_item_code(o) or "UNKNOWN"
        qty = int(o.get("quantity", 1) or 1)
        model_qty[code] = model_qty.get(code, 0) + qty
    return ", ".join(f"{c}({q})" for c, q in model_qty.items())


@router.get("/token-test")
async def token_test():
    import httpx
    # 서버 outbound IP 확인 (네이버 IP 화이트리스트 등록용)
    server_ip = None
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get("https://api.ipify.org?format=json")
            server_ip = r.json().get("ip")
    except Exception:
        pass

    try:
        from services.naver_client import naver_client
        token = await naver_client.get_token()
        return {"success": True, "token_prefix": token[:20] + "..." if token else None, "server_ip": server_ip}
    except Exception as e:
        return {"success": False, "error": str(e), "server_ip": server_ip}


@router.get("/orders")
async def fetch_orders(
    date_from: Optional[str] = Query(None, description="시작일 YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="종료일 YYYY-MM-DD"),
    order_type: str = Query("NEW_BEFORE", description="NEW_BEFORE|NEW_AFTER|DELIVERING"),
):
    try:
        from services.naver_client import naver_client
        orders = await naver_client.fetch_orders(
            date_from=date_from, date_to=date_to, order_type=order_type,
        )
        return {"success": True, "orders": orders, "count": len(orders)}
    except Exception as e:
        logger.error(f"[SS] 주문수집 오류: {e}", exc_info=True)
        return {"success": False, "error": str(e), "orders": []}


@router.post("/send-erp")
async def send_erp_only(
    selected_orders: list[dict] = Body(...),
):
    """ERP 판매전표만 전송 (로젠 미포함)"""
    from services.erp_client_ss import ERPClientSS

    if not selected_orders:
        return {"success": True, "message": "선택된 주문이 없습니다.", "lines": 0}

    order_groups = {}
    unmatched_items = []

    order_shipping: dict = {}   # orderId → 배송비 금액
    for o in selected_orders:
        od = o.get("order", {})
        po = o.get("productOrder", {})
        oid = od.get("orderId", "")
        poid = po.get("productOrderId", "")
        if not oid or not poid:
            continue
        if oid not in order_groups:
            order_groups[oid] = []
            # 배송비: productOrder.shippingFee 우선, 없으면 order.shippingFee
            fee = float(po.get("shippingFee", 0) or od.get("shippingFee", 0) or 0)
            order_shipping[oid] = fee
        product_id = str(po.get("productId", "") or po.get("productNo", "") or "")
        seller_code = po.get("sellerProductCode", "") or ""
        order_groups[oid].append({
            "orderId": oid, "productOrderId": poid,
            "productName": po.get("productName", ""),
            "productNo": product_id, "productId": product_id,
            "sellerProductCode": seller_code,
            "optionInfo": po.get("productOption", "") or seller_code,
            "quantity": po.get("quantity", 1),
            "settlementAmount": po.get("expectedSettlementAmount", 0) or po.get("totalPaymentAmount", 0),
        })

    DELIVERY_PROD_CD = "DEL-매출배002"
    erp_lines = []

    for oid, group in order_groups.items():
        for o in group:
            # 직접 선택해서 ERP 전송하는 경우 제외 키워드 무시 (사용자 명시적 선택 우선)
            code = _match_item_code(o)
            qty = int(o.get("quantity", 1) or 1)
            settle = float(o.get("settlementAmount", 0) or 0)
            if code:
                erp_lines.append({"prod_cd": code, "qty": qty, "price": round(settle / qty, 2) if qty else 0})
            else:
                unmatched_items.append({
                    "orderId": oid,
                    "productOrderId": o.get("productOrderId", ""),
                    "productNo": o.get("productNo", "") or o.get("productId", ""),
                    "productName": o.get("productName", ""),
                    "optionInfo": o.get("optionInfo", ""),
                    "quantity": qty, "settlementAmount": settle,
                })

    # 배송비: 금액별로 묶어서 (같은 금액 → 수량 합산, 다른 금액 → 별도 라인)
    from collections import defaultdict
    delivery_by_fee: dict = defaultdict(int)
    for oid in order_groups:
        fee = order_shipping.get(oid, 0)
        delivery_by_fee[fee] += 1
    for fee_amount, count in delivery_by_fee.items():
        erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": count, "price": int(fee_amount)})

    if not erp_lines:
        return {"success": False, "error": "ERP 전송 대상 없음", "unmatched_items": unmatched_items}

    if not SMARTSTORE_CUST_CODE:
        return {"success": False, "error": "SMARTSTORE_CUST_CODE 미설정"}

    # 발주확인 대상 productOrderId 수집
    all_po_ids = []
    for o in selected_orders:
        po = o.get("productOrder", {})
        poid = po.get("productOrderId", "")
        if poid:
            all_po_ids.append(poid)

    try:
        erp = ERPClientSS()
        await erp.ensure_session()
        r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, SMARTSTORE_WH_CODE, SMARTSTORE_EMP_CODE)
        delivery_count = len(delivery_by_fee)
        r["lines"] = len(erp_lines)
        r["erp_matched"] = len(erp_lines) - delivery_count
        r["erp_unmatched"] = len(unmatched_items)
        r["unmatched_items"] = unmatched_items

        # ERP 전송 성공 시 네이버 발주확인 처리 → "신규주문(발주 후)"로 이동
        if r.get("success") and all_po_ids:
            from services.naver_client import naver_client
            confirm_result = await naver_client.confirm_orders(all_po_ids)
            r["confirm"] = confirm_result
            logger.info(f"[SS] 발주확인: {confirm_result.get('confirmed', 0)}건")

        return r
    except Exception as e:
        logger.error(f"[SS] ERP 전송 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/excluded-send-erp")
async def excluded_send_erp(
    selected_orders: list[dict] = Body(...),
):
    """제외 키워드 주문 → ERP 판매전표 전송 (비고사항에 경동택배선불/착불 자동 기입)"""
    from services.erp_client_ss import ERPClientSS
    from collections import defaultdict

    if not selected_orders:
        return {"success": False, "error": "선택된 주문이 없습니다."}

    # 제외 키워드 포함 주문만 필터
    excluded_orders = []
    for o in selected_orders:
        if _is_excluded(o):
            excluded_orders.append(o)

    if not excluded_orders:
        return {"success": False, "error": "제외 키워드(허브랙/서버랙/캐비넷) 주문이 없습니다."}

    # orderId 기준 그룹화
    order_groups: dict = {}
    order_shipping: dict = {}
    order_feetype: dict = {}
    order_addr: dict = {}    # orderId → 배송지 정보 (비고사항 합산용)
    for o in excluded_orders:
        od = o.get("order") or {}
        po = o.get("productOrder") or {}
        oid = od.get("orderId", "") or po.get("orderId", "")
        poid = po.get("productOrderId", "")
        if not oid or not poid:
            continue
        if oid not in order_groups:
            order_groups[oid] = []
            fee = float(po.get("shippingFee", 0) or od.get("shippingFee", 0) or 0)
            order_shipping[oid] = fee
            fee_type = str(po.get("shippingFeeType") or od.get("shippingFeeType") or "")
            order_feetype[oid] = fee_type
            # 배송지 정보 수집
            addr = po.get("shippingAddress") or {}
            rcv       = addr.get("name", "") or ""
            tel       = addr.get("tel2", "") or addr.get("tel1", "") or ""
            full_addr = ((addr.get("baseAddress", "") or "") + " " + (addr.get("detailedAddress", "") or "")).strip()
            cust_msg  = (po.get("shippingMemo") or po.get("deliveryMemo") or
                         po.get("deliveryMessage") or od.get("shippingMemo") or
                         od.get("deliveryMemo") or "")
            order_addr[oid] = {"rcv": rcv, "tel": tel, "addr": full_addr, "msg": cust_msg}
        product_id = str(po.get("productId", "") or po.get("productNo", "") or "")
        seller_code = po.get("sellerProductCode", "") or ""
        order_groups[oid].append({
            "orderId": oid, "productOrderId": poid,
            "productName": po.get("productName", ""),
            "productNo": product_id, "productId": product_id,
            "sellerProductCode": seller_code,
            "optionInfo": po.get("productOption", "") or seller_code,
            "quantity": po.get("quantity", 1),
            "settlementAmount": po.get("expectedSettlementAmount", 0) or po.get("totalPaymentAmount", 0),
        })

    DELIVERY_PROD_CD = "DEL-매출배002"
    erp_lines = []
    unmatched_items = []

    for oid, group in order_groups.items():
        # 선불/착불 판단
        fee_type = order_feetype.get(oid, "")
        if "착불" in fee_type or fee_type.upper() in ("COLLECT", "COD"):
            delivery_type = "경동택배착불"
        else:
            delivery_type = "경동택배선불"

        # 비고사항: 경동택배선불/착불 / 전표제외 / 수령인 / 연락처 / 주소 / 배송메세지
        ai = order_addr.get(oid, {})
        parts = [f"{delivery_type} / 전표제외"]
        if ai.get("rcv"):   parts.append(ai["rcv"])
        if ai.get("tel"):   parts.append(ai["tel"])
        if ai.get("addr"):  parts.append(ai["addr"])
        if ai.get("msg"):   parts.append(ai["msg"])
        remark = " / ".join(parts)

        for o in group:
            code = _match_item_code(o)
            qty = int(o.get("quantity", 1) or 1)
            settle = float(o.get("settlementAmount", 0) or 0)
            if code:
                erp_lines.append({"prod_cd": code, "qty": qty,
                                   "price": round(settle / qty, 2) if qty else 0,
                                   "remark": remark})
            else:
                unmatched_items.append({
                    "orderId": oid,
                    "productOrderId": o.get("productOrderId", ""),
                    "productNo": o.get("productNo", "") or o.get("productId", ""),
                    "productName": o.get("productName", ""),
                    "optionInfo": o.get("optionInfo", ""),
                    "quantity": qty, "settlementAmount": settle,
                })

    # 배송비 라인 (경동택배 배송비) — 비고사항 동일하게 포함
    # (모든 라인에 CHAR5가 있어야 Ecount가 마지막 라인으로 덮어쓰지 않음)
    first_remark = erp_lines[0]["remark"] if erp_lines and erp_lines[0].get("remark") else ""
    delivery_by_fee: dict = defaultdict(int)
    for oid in order_groups:
        fee = order_shipping.get(oid, 0)
        delivery_by_fee[fee] += 1
    for fee_amount, count in delivery_by_fee.items():
        erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": count, "price": int(fee_amount),
                           "remark": first_remark})

    if not erp_lines:
        return {"success": False, "error": "ERP 전송 대상 없음", "unmatched_items": unmatched_items}

    if not SMARTSTORE_CUST_CODE:
        return {"success": False, "error": "SMARTSTORE_CUST_CODE 미설정"}

    # 발주확인 대상 productOrderId 수집
    all_po_ids = []
    for o in excluded_orders:
        po = o.get("productOrder") or {}
        poid = po.get("productOrderId", "")
        if poid:
            all_po_ids.append(poid)

    try:
        erp = ERPClientSS()
        await erp.ensure_session()
        r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, SMARTSTORE_WH_CODE, SMARTSTORE_EMP_CODE)
        r["lines"] = len(erp_lines)
        r["erp_matched"] = len(erp_lines)
        r["erp_unmatched"] = len(unmatched_items)
        r["unmatched_items"] = unmatched_items
        logger.info(f"[SS] 경동택배 ERP 전송: {len(erp_lines)}건, 미매칭: {len(unmatched_items)}건")

        # ERP 전송 성공 시 네이버 발주확인 처리 → "신규주문(발주 후)"로 이동
        if r.get("success") and all_po_ids:
            from services.naver_client import naver_client
            confirm_result = await naver_client.confirm_orders(all_po_ids)
            r["confirm"] = confirm_result
            logger.info(f"[SS] 경동 발주확인: {confirm_result.get('confirmed', 0)}건")

        return r
    except Exception as e:
        logger.error(f"[SS] 경동택배 ERP 전송 오류: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/excluded-export-excel")
async def excluded_export_excel(
    selected_orders: list[dict] = Body(...),
):
    """제외 키워드 주문 → 경동택배 전표용 엑셀 다운로드"""
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from fastapi.responses import StreamingResponse

    # 제외 키워드 포함 주문만 필터 (orderId 기준 그룹)
    excluded_oids: set = set()
    for o in selected_orders:
        po = o.get("productOrder") or {}
        combined = ((po.get("productName", "") or "") + " " + (po.get("productOption", "") or "")).lower()
        if any(kw in combined for kw in EXCLUDE_KEYWORDS):
            od = o.get("order") or {}
            oid = od.get("orderId", "") or po.get("orderId", "")
            if oid:
                excluded_oids.add(oid)
    logger.info(f"[SS] excluded-export-excel: 제외 주문 {len(excluded_oids)}개 orderId={excluded_oids}")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "경동택배전표"

    headers = ["비고사항", "수령인", "연락처", "주소", "배송메세지"]
    hdr_fill = PatternFill("solid", fgColor="C00000")
    hdr_font = Font(bold=True, color="FFFFFF")
    for ci, h in enumerate(headers, 1):
        c = ws.cell(1, ci, h)
        c.fill = hdr_fill; c.font = hdr_font; c.alignment = Alignment(horizontal="center")

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 12
    ws.column_dimensions["C"].width = 16
    ws.column_dimensions["D"].width = 50
    ws.column_dimensions["E"].width = 25

    seen: set = set()
    row = 2
    for o in selected_orders:
        try:
            od = o.get("order") or {}
            po = o.get("productOrder") or {}
            oid = od.get("orderId", "") or po.get("orderId", "")
            if not oid or oid not in excluded_oids or oid in seen:
                continue
            seen.add(oid)
            addr      = po.get("shippingAddress") or {}
            rcv       = addr.get("name", "") or ""
            tel       = addr.get("tel2", "") or addr.get("tel1", "") or ""
            full_addr = ((addr.get("baseAddress", "") or "") + " " + (addr.get("detailedAddress", "") or "")).strip()
            cust_msg  = (po.get("shippingMemo") or po.get("deliveryMemo") or
                         po.get("deliveryMessage") or od.get("shippingMemo") or
                         od.get("deliveryMemo") or "")

            fee_type = str(po.get("shippingFeeType") or od.get("shippingFeeType") or "")
            if "착불" in fee_type or fee_type.upper() in ("COLLECT", "COD"):
                delivery_type = "경동택배착불"
            else:
                delivery_type = "경동택배선불"

            ws.cell(row, 1, f"{delivery_type} / 전표제외")
            ws.cell(row, 2, rcv)
            ws.cell(row, 3, tel)
            ws.cell(row, 4, full_addr)
            ws.cell(row, 5, cust_msg)
            row += 1
        except Exception as ex:
            logger.error(f"[SS] excluded-export-excel 행 처리 오류: {ex}", exc_info=True)
            continue

    if row == 2:
        raise HTTPException(status_code=404, detail="제외 키워드(허브랙/서버랙/캐비넷) 주문이 없습니다.")

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    from urllib.parse import quote
    filename = f"경동택배전표_{datetime.now(KST).strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"}
    )


@router.post("/logen-export-excel")
async def logen_export_excel(
    selected_orders: list[dict] = Body(...),
):
    """선택 주문을 로젠 전송용 엑셀로 다운로드.
    컬럼: 주문번호 | 상품주문번호 | 수령인 | 연락처 | 주소 | 상품명 | 수량
    """
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from fastapi.responses import StreamingResponse

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "엑셀파일첫행-제목있음"

    # 로젠 구시스템양식 (A타입) - 사용자 설정 컬럼 순서
    # A:수하인명 B:수하인주소1 C:수하인전화 D:수하인휴대폰
    # E:택배수량 F:택배운임 G:운임구분 H:물품명 I:주문번호(→반환파일 S열 매칭용)
    # J:제주운임구분 K:배송메세지
    headers = ["수하인명", "수하인주소1", "수하인전화", "수하인휴대폰",
               "택배수량", "택배운임", "운임구분", "물품명", "주문번호",
               None, "배송메세지"]
    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    hdr_font = Font(bold=True, color="FFFFFF")
    for ci, h in enumerate(headers, 1):
        if h:
            c = ws.cell(1, ci, h)
            c.fill = hdr_fill; c.font = hdr_font; c.alignment = Alignment(horizontal="center")

    ws.column_dimensions["A"].width = 14
    ws.column_dimensions["B"].width = 50
    ws.column_dimensions["C"].width = 16
    ws.column_dimensions["D"].width = 16
    ws.column_dimensions["E"].width = 8
    ws.column_dimensions["F"].width = 10
    ws.column_dimensions["G"].width = 10
    ws.column_dimensions["H"].width = 30
    ws.column_dimensions["I"].width = 22
    ws.column_dimensions["K"].width = 25

    # orderId 기준으로 그룹화
    groups: dict = {}
    for o in selected_orders:
        od = o.get("order", {})
        po = o.get("productOrder", {})
        oid = od.get("orderId", "")
        if not oid:
            continue
        if oid not in groups:
            groups[oid] = {"od": od, "po": po, "items": []}
        groups[oid]["items"].append(po)

    row = 2
    for oid, g in groups.items():
        od = g["od"]
        po = g["po"]   # 첫 번째 productOrder (수령인 정보용)
        addr     = po.get("shippingAddress", {})
        rcv      = addr.get("name", "")
        tel_home = addr.get("tel1", "")
        tel_cell = addr.get("tel2", "") or addr.get("tel1", "")
        full_addr = ((addr.get("baseAddress","") or "") + " " + (addr.get("detailedAddress","") or "")).strip()
        ship_fee = int(float(po.get("shippingFee", 0) or od.get("shippingFee", 0) or 0))
        fare_tp  = "010"

        # 품목명: 모델명(or ERP코드) + 수량 요약
        model_qty: dict = {}
        total_qty = 0
        first_poid = ""
        cust_msg = ""
        for item_po in g["items"]:
            if not first_poid:
                first_poid = item_po.get("productOrderId", "")
                cust_msg = (item_po.get("shippingMemo", "") or
                            item_po.get("deliveryMemo", "") or
                            item_po.get("deliveryMessage", "") or
                            od.get("shippingMemo", "") or
                            od.get("deliveryMemo", "") or "")
            product_id = str(item_po.get("productId", "") or item_po.get("productNo", "") or "")
            # 모델명 우선, 없으면 ERP코드, 없으면 상품명
            model = _model_map.get(product_id, "")
            if not model:
                model = _product_map.get(product_id, "") or item_po.get("productName", "")[:20]
            qty = int(item_po.get("quantity", 1) or 1)
            model_qty[model] = model_qty.get(model, 0) + qty
            total_qty += qty

        goods = ", ".join(f"{m} x{q}" for m, q in model_qty.items())[:50]

        ws.cell(row, 1,  rcv)         # A: 수하인명
        ws.cell(row, 2,  full_addr)   # B: 수하인주소1
        ws.cell(row, 3,  tel_home)    # C: 수하인전화
        ws.cell(row, 4,  tel_cell)    # D: 수하인휴대폰
        ws.cell(row, 5,  1)            # E: 택배수량 (박스 수량, 항상 1)
        ws.cell(row, 6,  ship_fee)    # F: 택배운임
        ws.cell(row, 7,  fare_tp)     # G: 운임구분
        ws.cell(row, 8,  goods)       # H: 물품명 (모델명+수량)
        ws.cell(row, 9,  first_poid)  # I: 주문번호 → 반환파일 S열(index 18)로 매칭
        jeju = "선착불" if "제주" in full_addr else None
        ws.cell(row, 10, jeju)        # J: 제주운임구분 (제주 주소면 선착불 자동)
        ws.cell(row, 11, cust_msg)    # K: 배송메세지 (고객 요청사항)
        row += 1

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    filename = f"logen_{datetime.now(KST).strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"}
    )


@router.post("/logen-dispatch-excel")
async def logen_dispatch_excel(
    file: UploadFile = File(...),
):
    """송장번호 기입된 엑셀 업로드 → 네이버 발송처리.
    H열(8번째)에 송장번호, A열(1번째)에 주문번호, B열(2번째)에 상품주문번호
    """
    import io
    import openpyxl
    from services.naver_client import naver_client

    content = await file.read()
    wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
    ws = wb.active

    dispatch_list = []
    skipped = []
    # 로젠 반환 파일 구조:
    #   1행: 타이틀, 2행: 헤더, 3행: 서브헤더, 4행~: 데이터
    #   D열(index 3): 운송장번호
    #   S열(index 18): 주문번호 → 다운로드 시 I열에 삽입한 productOrderId
    for row in ws.iter_rows(min_row=4, values_only=True):
        if not any(row):
            continue
        tracking = str(row[3]  or "").strip()   # D열: 운송장번호
        poid     = str(row[18] or "").strip()   # S열: 주문번호(=productOrderId)
        if not tracking or not poid or tracking == "None" or poid == "None":
            skipped.append(poid or str(row[6] or ""))
            continue
        dispatch_list.append({
            "productOrderId": poid,
            "deliveryCompanyCode": "LOGEN",
            "trackingNumber": tracking,
        })

    if not dispatch_list:
        return {"success": False, "error": f"송장번호가 입력된 행이 없습니다. (빈 행: {len(skipped)}개)"}

    try:
        result = await naver_client.dispatch_orders(dispatch_list)
        result["dispatched_count"] = len(dispatch_list)
        result["skipped_count"] = len(skipped)
        logger.info(f"[SS] 엑셀발송처리: {len(dispatch_list)}건, 결과={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[SS] 엑셀발송처리 오류: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/dispatch-manual")
async def dispatch_manual(
    body: dict = Body(...),
):
    """수기 송장번호로 네이버 발송처리.
    body: { "items": [{"productOrderId": "...", "trackingNumber": "..."}, ...] }
    """
    from services.naver_client import naver_client
    items = body.get("items", [])
    if not items:
        return {"success": False, "error": "송장 데이터가 없습니다."}

    dispatch_list = [
        {"productOrderId": it["productOrderId"], "deliveryCompanyCode": "LOGEN", "trackingNumber": it["trackingNumber"]}
        for it in items if it.get("productOrderId") and it.get("trackingNumber")
    ]
    if not dispatch_list:
        return {"success": False, "error": "유효한 송장번호가 없습니다."}

    try:
        result = await naver_client.dispatch_orders(dispatch_list)
        result["dispatched_count"] = len(dispatch_list)
        logger.info(f"[SS] 수기발송처리: {len(dispatch_list)}건, 결과={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[SS] 수기발송처리 오류: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/register-logen")
async def register_logen_only(
    warehouse: str = Query(..., pattern="^(gimpo|yongsan)$"),
    selected_orders: list[dict] = Body(...),
):
    """로젠택배 등록 + 발주확인 + 발송처리 (ERP 미포함)"""
    from services.naver_client import naver_client
    from services.ilogen_client import register_orders, get_sender

    if not selected_orders:
        return {"success": True, "message": "선택된 주문이 없습니다."}

    order_groups = {}
    all_po_ids = []

    for o in selected_orders:
        od = o.get("order", {})
        po = o.get("productOrder", {})
        oid = od.get("orderId", "")
        poid = po.get("productOrderId", "")
        if not oid or not poid:
            continue
        all_po_ids.append(poid)
        if oid not in order_groups:
            order_groups[oid] = []
        product_id = str(po.get("productId", "") or po.get("productNo", "") or "")
        seller_code = po.get("sellerProductCode", "") or ""
        order_groups[oid].append({
            "orderId": oid, "productOrderId": poid,
            "productName": po.get("productName", ""),
            "productNo": product_id, "productId": product_id,
            "sellerProductCode": seller_code,
            "optionInfo": po.get("productOption", "") or seller_code,
            "quantity": po.get("quantity", 1),
            "deliveryFeeType": po.get("shippingFeeType", ""),
            "rcvName": po.get("shippingAddress", {}).get("name", ""),
            "rcvTel": po.get("shippingAddress", {}).get("tel1", ""),
            "rcvAddr": (po.get("shippingAddress", {}).get("baseAddress", "") + " " + po.get("shippingAddress", {}).get("detailedAddress", "")).strip(),
        })

    sender = get_sender(warehouse)
    ilogen_orders = []
    oid_to_idx = {}

    for oid, group in order_groups.items():
        first = group[0]
        fare_code = "020" if "착불" in str(first.get("deliveryFeeType", "")) else "030"
        ilogen_orders.append({
            "snd_name": sender["name"], "snd_tel": sender["tel"], "snd_addr": sender["addr"],
            "rcv_name": first["rcvName"], "rcv_tel": first["rcvTel"], "rcv_addr": first["rcvAddr"],
            "fare_code": fare_code, "goods_nm": _build_goods_nm(group),
        })
        oid_to_idx[oid] = len(ilogen_orders) - 1

    try:
        logen_res = await register_orders(warehouse, ilogen_orders)
        tns = logen_res.get("tracking_numbers", [])
        logen_ok = logen_res.get("success", False) and len(tns) > 0

        confirm_result = {"confirmed": 0, "message": "로젠 등록 실패로 보류"}
        dispatch_result = {"dispatched": 0, "message": "로젠 등록 실패로 보류"}

        if logen_ok:
            confirm_result = await naver_client.confirm_orders(all_po_ids)
            if confirm_result.get("confirmed", 0) > 0:
                oid_slip = {}
                for tn in tns:
                    for oid, idx in oid_to_idx.items():
                        if idx == tn["index"]:
                            oid_slip[oid] = tn["slip_no"]
                            break
                dispatch_list = []
                for oid, group in order_groups.items():
                    slip = oid_slip.get(oid)
                    if not slip:
                        continue
                    for o in group:
                        dispatch_list.append({"productOrderId": o["productOrderId"], "deliveryCompanyCode": "LOGEN", "trackingNumber": slip})
                dispatch_result = await naver_client.dispatch_orders(dispatch_list) if dispatch_list else {"success": True, "message": "대상 없음"}

        return {
            "success": logen_ok,
            "logen": logen_res,
            "confirm": confirm_result,
            "dispatch": dispatch_result,
            "tracking_count": len(tns),
            "total_orders": len(all_po_ids),
        }
    except Exception as e:
        logger.error(f"[SS] 로젠등록 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/auto-register-logen")
async def auto_register_logen(
    warehouse: str = Query(..., pattern="^(gimpo|yongsan)$"),
    selected_orders: list[dict] = Body(...),
):
    from services.naver_client import naver_client
    from services.ilogen_client import register_orders, get_sender
    from services.erp_client_ss import ERPClientSS

    result = {"step1_confirm": None, "step2_erp": None, "step2_logen": None, "step3_dispatch": None, "summary": {}}

    try:
        if not selected_orders:
            return {"success": True, "message": "선택된 주문이 없습니다.", **result}

        order_groups = {}
        all_po_ids = []

        order_shipping: dict = {}   # orderId → 배송비 금액
        for o in selected_orders:
            od = o.get("order", {})
            po = o.get("productOrder", {})
            oid = od.get("orderId", "")
            poid = po.get("productOrderId", "")
            if not oid or not poid:
                continue
            all_po_ids.append(poid)
            if oid not in order_groups:
                order_groups[oid] = []
                fee = float(po.get("shippingFee", 0) or od.get("shippingFee", 0) or 0)
                order_shipping[oid] = fee
            product_id = str(po.get("productId", "") or po.get("productNo", "") or "")
            seller_code = po.get("sellerProductCode", "") or ""
            order_groups[oid].append({
                "orderId": oid, "productOrderId": poid,
                "productName": po.get("productName", ""),
                "productNo": product_id, "productId": product_id,
                "sellerProductCode": seller_code,
                "optionInfo": po.get("productOption", "") or seller_code,
                "quantity": po.get("quantity", 1),
                "settlementAmount": po.get("expectedSettlementAmount", 0) or po.get("totalPaymentAmount", 0),
                "deliveryFeeType": po.get("shippingFeeType", ""),
                "rcvName": po.get("shippingAddress", {}).get("name", ""),
                "rcvTel": po.get("shippingAddress", {}).get("tel1", ""),
                "rcvAddr": (po.get("shippingAddress", {}).get("baseAddress", "") + " " + po.get("shippingAddress", {}).get("detailedAddress", "")).strip(),
            })

        logger.info(f"[SS] 선택 주문: {len(all_po_ids)}건, {len(order_groups)}그룹")

        # ERP 라인 구성
        DELIVERY_PROD_CD = "DEL-매출배002"
        erp_lines = []
        unmatched_items = []

        for oid, group in order_groups.items():
            for o in group:
                if _is_excluded(o):
                    logger.info(f"[SS] 제외 키워드 필터: {o.get('productName','')[:40]}")
                    continue
                code = _match_item_code(o)
                qty = int(o.get("quantity", 1) or 1)
                settle = float(o.get("settlementAmount", 0) or 0)
                if code:
                    erp_lines.append({"prod_cd": code, "qty": qty, "price": round(settle / qty, 2) if qty else 0})
                else:
                    unmatched_items.append({
                        "orderId": oid,
                        "productOrderId": o.get("productOrderId", ""),
                        "productNo": o.get("productNo", "") or o.get("productId", ""),
                        "productName": o.get("productName", ""),
                        "optionInfo": o.get("optionInfo", ""),
                        "quantity": qty, "settlementAmount": settle,
                        "deliveryMethod": o.get("deliveryFeeType", ""),
                        "rcvName": o.get("rcvName", ""),
                    })

        # 배송비: 금액별로 묶어서 (같은 금액 → 수량 합산, 다른 금액 → 별도 라인)
        from collections import defaultdict
        delivery_by_fee: dict = defaultdict(int)
        for oid in order_groups:
            fee = order_shipping.get(oid, 0)
            delivery_by_fee[fee] += 1
        for fee_amount, count in delivery_by_fee.items():
            erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": count, "price": int(fee_amount)})

        sender = get_sender(warehouse)
        ilogen_orders = []
        oid_to_idx = {}

        for oid, group in order_groups.items():
            first = group[0]
            fare_code = "020" if "착불" in str(first.get("deliveryFeeType", "")) else "030"
            ilogen_orders.append({
                "snd_name": sender["name"], "snd_tel": sender["tel"], "snd_addr": sender["addr"],
                "rcv_name": first["rcvName"], "rcv_tel": first["rcvTel"], "rcv_addr": first["rcvAddr"],
                "fare_code": fare_code, "goods_nm": _build_goods_nm(group),
            })
            oid_to_idx[oid] = len(ilogen_orders) - 1

        async def _do_erp():
            if not erp_lines:
                return {"success": True, "lines": 0, "message": "ERP 입력 대상 없음"}
            if not SMARTSTORE_CUST_CODE:
                return {"success": False, "lines": len(erp_lines), "error": "SMARTSTORE_CUST_CODE 미설정"}
            erp = ERPClientSS()
            await erp.ensure_session()
            r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, SMARTSTORE_WH_CODE, SMARTSTORE_EMP_CODE)
            r["lines"] = len(erp_lines)
            r["sent_prod_codes"] = [l["prod_cd"] for l in erp_lines]
            return r

        async def _do_logen():
            if not ilogen_orders:
                return {"success": True, "tracking_numbers": []}
            return await register_orders(warehouse, ilogen_orders)

        erp_res, logen_res = await asyncio.gather(_do_erp(), _do_logen())
        result["step1_erp"] = erp_res
        result["step1_logen"] = logen_res

        tns = logen_res.get("tracking_numbers", [])
        erp_ok = erp_res.get("success", False)
        logen_ok = logen_res.get("success", False) and len(tns) > 0

        if erp_ok and logen_ok:
            confirm_result = await naver_client.confirm_orders(all_po_ids)
            result["step2_confirm"] = confirm_result
        else:
            reasons = []
            if not erp_ok: reasons.append("ERP 판매입력 실패")
            if not logen_ok: reasons.append("로젠 송장발급 실패")
            result["step2_confirm"] = {"confirmed": 0, "message": f"발주확인 보류 ({', '.join(reasons)})"}

        if tns and erp_ok and result["step2_confirm"].get("confirmed", 0) > 0:
            oid_slip = {}
            for tn in tns:
                for oid, idx in oid_to_idx.items():
                    if idx == tn["index"]:
                        oid_slip[oid] = tn["slip_no"]
                        break
            dispatch_list = []
            for oid, group in order_groups.items():
                slip = oid_slip.get(oid)
                if not slip: continue
                for o in group:
                    dispatch_list.append({"productOrderId": o["productOrderId"], "deliveryCompanyCode": "LOGEN", "trackingNumber": slip})
            result["step3_dispatch"] = await naver_client.dispatch_orders(dispatch_list) if dispatch_list else {"success": True, "message": "대상 없음"}
        else:
            skip_reason = "ERP/로젠 미완료" if not (erp_ok and logen_ok) else "운송장 없음"
            result["step3_dispatch"] = {"dispatched": 0, "message": f"발송처리 보류 ({skip_reason})"}

        result["unmatched_items"] = unmatched_items
        result["summary"] = {
            "total_orders": len(all_po_ids), "total_groups": len(order_groups),
            "erp_matched": len(erp_lines) - (1 if delivery_count > 0 else 0),
            "erp_unmatched": len(unmatched_items),
            "erp_delivery_count": delivery_count,
            "erp_lines": len(erp_lines),
            "logen_registered": len(ilogen_orders),
            "tracking_numbers": len(tns),
        }
        result["success"] = True
        return result

    except Exception as e:
        logger.error(f"[SS] 자동등록 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reload-product-map")
async def reload_product_map():
    _load_product_map()
    return {"success": True, "count": len(_product_map)}


# ═══════════════════════════════════════════
# 매핑 관리 API
# ═══════════════════════════════════════════

@router.get("/product-map")
async def get_product_map(
    search: Optional[str] = Query(None),
):
    items = []
    for prod_no, erp_code in _product_map.items():
        model = _model_map.get(prod_no, "")
        if search:
            q = search.lower()
            if q not in prod_no.lower() and q not in erp_code.lower() and q not in model.lower():
                continue
        items.append({"productNo": prod_no, "erpCode": erp_code, "model": model})
    return {"success": True, "items": items, "total": len(_product_map), "filtered": len(items)}


@router.post("/product-map")
async def add_product_map(entry: dict = Body(...)):
    prod_no = str(entry.get("productNo", "")).strip()
    erp_code = str(entry.get("erpCode", "")).strip()
    model = str(entry.get("model", "")).strip()

    if not prod_no or not erp_code:
        return {"success": False, "error": "상품번호와 품목코드는 필수입니다."}

    is_new = prod_no not in _product_map
    _product_map[prod_no] = erp_code
    if model:
        _model_map[prod_no] = model
    _save_product_map()

    action = "추가" if is_new else "수정"
    logger.info(f"[SS] 매핑 {action}: {prod_no} → ERP:{erp_code}, 모델:{model}")
    return {"success": True, "action": action, "productNo": prod_no, "erpCode": erp_code, "model": model,
            "total": len(_product_map)}


@router.delete("/product-map/{product_no}")
async def delete_product_map(product_no: str):
    if product_no not in _product_map:
        return {"success": False, "error": f"상품번호 {product_no} 매핑이 없습니다."}
    erp_code = _product_map.pop(product_no)
    _model_map.pop(product_no, None)
    _save_product_map()
    logger.info(f"[SS] 매핑 삭제: {product_no} (was {erp_code})")
    return {"success": True, "deleted": product_no, "total": len(_product_map)}


# ═══════════════════════════════════════════
# Excel 업로드/다운로드 API
# ═══════════════════════════════════════════

def _make_header(ws, headers: list, fill_color: str):
    """공통 헤더 스타일 적용"""
    from openpyxl.styles import Font, PatternFill, Alignment
    fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center")


def _find_data_start(ws) -> int:
    """헤더행(상품번호 포함) 다음 행 반환"""
    for r in range(1, min(5, ws.max_row + 1)):
        if "상품번호" in str(ws.cell(r, 1).value or ""):
            return r + 1
    return 2  # 헤더 없으면 2행부터


def _read_sheet_2col(ws) -> tuple[dict, dict]:
    """상품번호|ERP품목코드|모델명 시트 읽기 → (map, model_map)"""
    data_start = _find_data_start(ws)
    prod_map, model_map = {}, {}
    for r in range(data_start, ws.max_row + 1):
        pno  = str(ws.cell(r, 1).value or "").strip()
        code = str(ws.cell(r, 2).value or "").strip()
        mdl  = str(ws.cell(r, 3).value or "").strip()
        if pno and code and pno != "None" and code != "None":
            prod_map[pno] = code
            if mdl and mdl != "None":
                model_map[pno] = mdl
    return prod_map, model_map


@router.get("/product-map/export-excel")
async def export_product_map_excel():
    """현재 매핑 전체를 3시트 Excel로 다운로드
       시트1: 메인상품 / 시트2: 옵션상품(오버라이드) / 시트3: 추가상품
    """
    import io
    from fastapi.responses import StreamingResponse
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl 미설치")

    wb = openpyxl.Workbook()

    # ── 시트1: 메인상품 ──────────────────────────
    ws1 = wb.active
    ws1.title = "1_메인상품"
    _make_header(ws1, ["상품번호", "ERP품목코드", "모델명(로젠송장용)"], "1F4E79")
    ws1.column_dimensions["A"].width = 20
    ws1.column_dimensions["B"].width = 30
    ws1.column_dimensions["C"].width = 30
    for i, (pno, code) in enumerate(_product_map.items(), start=2):
        ws1.cell(i, 1, pno); ws1.cell(i, 2, code); ws1.cell(i, 3, _model_map.get(pno, ""))

    # ── 시트2: 옵션상품 ──────────────────────────
    ws2 = wb.create_sheet("2_옵션상품")
    _make_header(ws2, ["상품번호", "옵션텍스트(참고용)", "자동추출코드(참고용)", "ERP품목코드(비우면자동)", "모델명(로젠송장용)"], "375623")
    ws2.column_dimensions["A"].width = 20
    ws2.column_dimensions["B"].width = 45
    ws2.column_dimensions["C"].width = 25
    ws2.column_dimensions["D"].width = 30
    ws2.column_dimensions["E"].width = 30
    for i, (pno, code) in enumerate(_option_override_map.items(), start=2):
        ws2.cell(i, 1, pno); ws2.cell(i, 4, code); ws2.cell(i, 5, _model_map.get(pno, ""))

    # ── 시트3: 추가상품 ──────────────────────────
    ws3 = wb.create_sheet("3_추가상품")
    _make_header(ws3, ["상품번호", "ERP품목코드", "모델명(로젠송장용)"], "7B3F00")
    ws3.column_dimensions["A"].width = 20
    ws3.column_dimensions["B"].width = 30
    ws3.column_dimensions["C"].width = 30
    for i, (pno, code) in enumerate(_addon_map.items(), start=2):
        ws3.cell(i, 1, pno); ws3.cell(i, 2, code); ws3.cell(i, 3, _model_map.get(pno, ""))

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    logger.info(f"[SS] Excel 내보내기 — 메인:{len(_product_map)} 옵션:{len(_option_override_map)} 추가:{len(_addon_map)}")
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=smartstore_product_map.xlsx"},
    )


@router.post("/product-map/import-excel")
async def import_product_map_excel(file: bytes = Body(..., media_type="application/octet-stream")):
    """3시트 Excel 업로드 → 전체 매핑 갱신
       시트1: 메인상품 / 시트2: 옵션상품 / 시트3: 추가상품
    """
    import io
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl 미설치")

    try:
        wb = openpyxl.load_workbook(io.BytesIO(file), data_only=True)

        new_product_map, new_model_map, new_option_map, new_addon_map = {}, {}, {}, {}

        # 시트1: 메인상품 (컬럼: 상품번호|ERP품목코드|모델명)
        if len(wb.sheetnames) >= 1:
            m, mdl = _read_sheet_2col(wb.worksheets[0])
            new_product_map.update(m); new_model_map.update(mdl)

        # 시트2: 옵션상품 (컬럼: 상품번호|옵션텍스트|자동추출코드|ERP품목코드|모델명)
        if len(wb.sheetnames) >= 2:
            ws2 = wb.worksheets[1]
            data_start = _find_data_start(ws2)
            for r in range(data_start, ws2.max_row + 1):
                pno  = str(ws2.cell(r, 1).value or "").strip()
                code = str(ws2.cell(r, 4).value or "").strip()  # D열: ERP품목코드
                mdl  = str(ws2.cell(r, 5).value or "").strip()  # E열: 모델명
                if pno and code and pno != "None" and code != "None":
                    new_option_map[pno] = code
                    if mdl and mdl != "None":
                        new_model_map[pno] = mdl

        # 시트3: 추가상품 (컬럼: 상품번호|ERP품목코드|모델명)
        if len(wb.sheetnames) >= 3:
            m, mdl = _read_sheet_2col(wb.worksheets[2])
            new_addon_map.update(m); new_model_map.update(mdl)

        total = len(new_product_map) + len(new_option_map) + len(new_addon_map)
        if total == 0:
            return {"success": False, "error": "유효한 데이터가 없습니다."}

        global _product_map, _option_override_map, _addon_map, _model_map, _model_to_erp_map
        _product_map         = new_product_map
        _option_override_map = new_option_map
        _addon_map           = new_addon_map
        _model_map           = new_model_map
        _save_product_map()
        _load_product_map()   # 역방향 맵 재빌드

        logger.info(f"[SS] Excel 가져오기 — 메인:{len(_product_map)} 옵션:{len(_option_override_map)} 추가:{len(_addon_map)}")
        return {
            "success": True,
            "sheet1_main": len(_product_map),
            "sheet2_option": len(_option_override_map),
            "sheet3_addon": len(_addon_map),
            "message": f"메인 {len(_product_map)}건 / 옵션 {len(_option_override_map)}건 / 추가상품 {len(_addon_map)}건 갱신 완료",
        }

    except Exception as e:
        logger.error(f"[SS] Excel 가져오기 오류: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"파일 파싱 오류: {e}")


@router.post("/product-map/fetch-options-excel")
async def fetch_options_excel(body: dict = Body(...)):
    """상품번호 목록으로 네이버 API 조회 → 옵션/추가상품 매핑 작업용 Excel 다운로드"""
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    from fastapi.responses import StreamingResponse
    from services.naver_client import naver_client

    product_nos = body.get("productNos", [])
    if not product_nos:
        raise HTTPException(status_code=400, detail="productNos 필요")

    items = await naver_client.fetch_products_with_options(product_nos)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "옵션_추가상품_매핑"

    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    hdr_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    yel_fill = PatternFill("solid", fgColor="FFF2CC")
    data_font = Font(name="Arial", size=10)

    cols = ["구분", "상품번호", "상품명", "옵션텍스트", "판매자코드(참고)", "재고", "ERP품목코드(입력)", "모델명(입력)"]
    for ci, col in enumerate(cols, 1):
        c = ws.cell(1, ci, col)
        c.fill = hdr_fill; c.font = hdr_font
        c.alignment = Alignment(horizontal="center")

    widths = [10, 18, 40, 40, 20, 8, 25, 20]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    for ri, item in enumerate(items, 2):
        # 현재 매핑 여부 확인
        pno = item["productNo"]
        existing_erp = _product_map.get(pno, "") or _option_override_map.get(pno, "")
        existing_model = _model_map.get(pno, "")

        vals = [
            item["type"], pno, item["productName"], item["optionText"],
            item["sellerCode"], item["stock"],
            existing_erp,   # 기존 매핑 있으면 미리 채워줌
            existing_model,
        ]
        for ci, val in enumerate(vals, 1):
            c = ws.cell(ri, ci, val)
            c.font = data_font
            if ci in (7, 8):
                c.fill = yel_fill

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    logger.info(f"[SS] 옵션조회 Excel: {len(items)}개 항목")
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=smartstore_options.xlsx"},
    )


@router.get("/product-map/debug-option/{product_no}")
async def debug_channel_product(product_no: str):
    """채널상품 API 응답 구조 확인용 (디버그)"""
    from services.naver_client import naver_client
    headers = await naver_client._headers()
    import httpx
    from config import NAVER_COMMERCE_URL
    
    # 여러 엔드포인트 시도
    endpoints = [
        f"{NAVER_COMMERCE_URL}/external/v1/channel-products/{product_no}",
        f"{NAVER_COMMERCE_URL}/external/v2/channel-products/{product_no}",
    ]
    results = {}
    async with httpx.AsyncClient(timeout=15) as client:
        for url in endpoints:
            try:
                r = await client.get(url, headers=headers)
                results[url] = {"status": r.status_code, "body": r.json()}
            except Exception as e:
                results[url] = {"error": str(e)}
    return results
