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
from fastapi import APIRouter, Query, HTTPException, Body

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
    combined = ((order.get("productName","") or "") + " " + (order.get("optionInfo","") or "")).lower()
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

    for o in selected_orders:
        od = o.get("order", {})
        po = o.get("productOrder", {})
        oid = od.get("orderId", "")
        poid = po.get("productOrderId", "")
        if not oid or not poid:
            continue
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
            "settlementAmount": po.get("expectedSettlementAmount", 0) or po.get("totalPaymentAmount", 0),
        })

    DELIVERY_PROD_CD = "DEL-매출배002"
    erp_lines = []

    for oid, group in order_groups.items():
        for o in group:
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

    delivery_count = len(order_groups)
    if delivery_count > 0:
        erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": delivery_count, "price": 0})

    if not erp_lines:
        return {"success": False, "error": "ERP 전송 대상 없음", "unmatched_items": unmatched_items}

    if not SMARTSTORE_CUST_CODE:
        return {"success": False, "error": "SMARTSTORE_CUST_CODE 미설정"}

    try:
        erp = ERPClientSS()
        await erp.ensure_session()
        r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, SMARTSTORE_WH_CODE, SMARTSTORE_EMP_CODE)
        r["lines"] = len(erp_lines)
        r["erp_matched"] = len(erp_lines) - (1 if delivery_count > 0 else 0)
        r["erp_unmatched"] = len(unmatched_items)
        r["unmatched_items"] = unmatched_items
        return r
    except Exception as e:
        logger.error(f"[SS] ERP 전송 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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

        delivery_count = len(order_groups)
        if delivery_count > 0:
            erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": delivery_count, "price": 0})

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
