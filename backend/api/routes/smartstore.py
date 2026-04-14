"""
ì¤ë§í¸ì¤í ì´ ì£¼ë¬¸ ìëí API ë¼ì°í¸
ë¤ì´ë² ì£¼ë¬¸ìì§ â ERP íë§¤ìë ¥ â ë¡ì  íë°° ë±ë¡ â ë°ì¡ì²ë¦¬
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
from pydantic import BaseModel

KST = ZoneInfo("Asia/Seoul")

from config import (
    SMARTSTORE_CUST_CODE, SMARTSTORE_EMP_CODE, SMARTSTORE_WH_CODE,
    SMARTSTORE_PRODUCT_MAP_PATH, SMARTSTORE_MODEL_MAP_PATH,
    SMARTSTORE_OPTION_MAP_PATH, SMARTSTORE_ADDON_MAP_PATH,
    SMARTSTORE_OPTION_TEXT_MAP_PATH, SMARTSTORE_ADDON_TEXT_MAP_PATH,
    SMARTSTORE_CODE_ALIAS_MAP_PATH,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/smartstore", tags=["SmartStore"])

# ëª¨ë¸ì½ë ì ê·ì
MODEL_CODE_RE = re.compile(r"(LS[PNE]?-[\w\-]+|ZOT-[\w\-]+)", re.IGNORECASE)
EXCLUDE_KEYWORDS = ["íë¸ë", "ìë²ë", "ìºë¹ë·"]

# ìí¸1: ë©ì¸ìí â ìíë²í¸ â ERPíëª©ì½ë (ìµì ìë ìí)
_product_map: dict = {}
# ìí¸2: ìµììí â ìíë²í¸ â ERPíëª©ì½ë (ìëì¶ì¶ ì¤ë²ë¼ì´ë)
_option_override_map: dict = {}
# ìí¸3: ì¶ê°ìí â ìíë²í¸ â ERPíëª©ì½ë
_addon_map: dict = {}
# ëª¨ë¸ëª: ìíë²í¸ â ëª¨ë¸ëª (ë¡ì   ì¡ì¥ì©, ì  ìí¸ ê³µì©)
_model_map: dict = {}
# ì­ë°©í¥: ëª¨ë¸ëª â ERPíëª©ì½ë (ìµì íì¤í¸ìì ëª¨ë¸ëª ì¶ì¶ ì ERPì½ëë¡ ë³í)
_model_to_erp_map: dict = {}
# ìµìíì¤í¸ ì§ì ë§¤í: "ìíë²í¸|ìµìê°" â ERPíëª©ì½ë
_option_text_map: dict = {}
# ì¶ê°ìííì¤í¸ ì§ì ë§¤í: "ìíë²í¸|ì¶ê°ìíê°" â ERPíëª©ì½ë
_addon_text_map: dict = {}
# ì¶ì¶ì½ë ë³ì¹­ë§µ: HDSVAL-XXX ë± â ERPíëª©ì½ë
_code_alias_map: dict = {}


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
    global _option_text_map, _addon_text_map, _code_alias_map
    _product_map        = _load_json(SMARTSTORE_PRODUCT_MAP_PATH)
    _option_override_map = _load_json(SMARTSTORE_OPTION_MAP_PATH)
    _addon_map          = _load_json(SMARTSTORE_ADDON_MAP_PATH)
    _model_map          = _load_json(SMARTSTORE_MODEL_MAP_PATH)
    _option_text_map    = _load_json(SMARTSTORE_OPTION_TEXT_MAP_PATH)
    _addon_text_map     = _load_json(SMARTSTORE_ADDON_TEXT_MAP_PATH)
    _code_alias_map     = _load_json(SMARTSTORE_CODE_ALIAS_MAP_PATH)
    # ì­ë°©í¥ ë§µ ë¹ë: ëª¨ë¸ëª â ERPíëª©ì½ë (ì  ìí¸ í¬í¨)
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
        f"[SS] ë§¤í ë¡ë â ë©ì¸:{len(_product_map)} ìµì:{len(_option_override_map)} "
        f"ì¶ê°:{len(_addon_map)} ëª¨ë¸ì­ë°©í¥:{len(_model_to_erp_map)} "
        f"ìµìíì¤í¸:{len(_option_text_map)} ì¶ê°ìííì¤í¸:{len(_addon_text_map)} ì½ëë³ì¹­:{len(_code_alias_map)}"
    )


def _save_product_map():
    _save_json(SMARTSTORE_PRODUCT_MAP_PATH, _product_map)
    _save_json(SMARTSTORE_OPTION_MAP_PATH,  _option_override_map)
    _save_json(SMARTSTORE_ADDON_MAP_PATH,   _addon_map)
    _save_json(SMARTSTORE_MODEL_MAP_PATH,   _model_map)


_load_product_map()


def _extract_erp_code_from_option(option_text: str) -> Optional[str]:
    """
    ìµì íì¤í¸ìì ERP íëª©ì½ë ì¶ì¶.
    ì°ì ìì:
      1) ë§ì§ë§ (ì½ë) ëë [ì½ë] â ì ì/ë¹ì ì ê´í¸ ëª¨ë ì²ë¦¬
      2) ì½ë¡  ë¤ ì½ë
      3) ìµì ì ì²´ê° ì½ë
      4) LS/LSP/LSN/LST/ZOT ë¡ ììíë ì½ëê° íì¤í¸ ë´ í¬í¨
    ì í¨ ì½ë ê¸°ì¤: ìë¬¸Â·ì«ìë¡ ìì, ê³µë°± ìì
    """
    if not option_text:
        return None

    text = option_text.strip()

    # '/ ë°°ì¡ë°©ë²:' ì´í ì ê±° (ì: "LS-750HS / ë°°ì¡ë°©ë²: ê²½ëíë°° (ìíë ë°°ì¡ì§ ëì°©)")
    if " / " in text:
        text = text.split(" / ")[0].strip()

    def is_valid_code(s: str) -> bool:
        # ìë¬¸Â·ì«ì ìì, ê³µë°± ìì, ìµì 3ì ì´ì (ë¨ì¼ ë¬¸ì ì¤í ë°©ì§)
        return bool(s and " " not in s and len(s) >= 3 and re.match(r"[A-Za-z0-9]", s))

    # í¨í´ 1: ë§ì§ë§ (ì½ë) â ë«í ê´í¸
    m = re.search(r"\(([^()]*(?:\([^)]*\))[^()]*|[^()]+)\)\s*$", text)
    if m:
        candidate = m.group(1).strip()
        if is_valid_code(candidate):
            return candidate

    # í¨í´ 1-b: ë«íì§ ìì ê´í¸ (ì: (LS-ADOOR(B) )
    m2 = re.search(r"\(([A-Za-z0-9][A-Za-z0-9\-\(\)\.]*)\s*$", text)
    if m2:
        candidate = m2.group(1).strip()
        if is_valid_code(candidate):
            return candidate

    # í¨í´ 1-c: ëê´í¸ [ì½ë] (ì: "0.5M [LS-5UTPD-0.5MG]", "ìë²í­ [HDSVAL-615]")
    m3 = re.search(r"\[([A-Za-z0-9][A-Za-z0-9\-\.]*)\]", text)
    if m3:
        candidate = m3.group(1).strip()
        if is_valid_code(candidate):
            return candidate

    # í¨í´ 2: ì½ë¡  ë¤ ì½ë
    if ":" in text:
        after_colon = text.rsplit(":", 1)[1].strip()
        if is_valid_code(after_colon):
            return after_colon

    # í¨í´ 3: ìµì íì¤í¸ ì ì²´ê° ì½ë (ì: "LS-420HM", "LS-UHS2SR", "LS-WPCOP-C6")
    if is_valid_code(text):
        return text

    # í¨í´ 4: LS/LSP/LSN/ZOT ë¡ ììíë ì½ëê° íì¤í¸ ë´ í¬í¨
    # (ì: "1. LS-U61MH", "0.5M LS-HF7005", "ëª¨ëí° 4ê° ì°ê²°(LS-UCHD4) ë¦¬í¼ì í")
    # LS- íìì²ë¼ ì ëì´ ë¤ìì íì´íì´ ì¤ë ê²½ì°ë í¬í¨
    m4 = re.search(r'\b((?:LS[PNT]?|ZOT)[A-Za-z0-9\-\.]{2,})', text, re.IGNORECASE)
    if m4:
        candidate = m4.group(1).rstrip('-.')
        if is_valid_code(candidate) and len(candidate) >= 4:
            return candidate

    return None


def _match_item_code(order: dict) -> Optional[str]:
    """
    ERP íëª©ì½ë ê²°ì  ì°ì ìì:
      1) ìí¸2 ì¤ë²ë¼ì´ë (ìµììí, ì¬ì©ì ìë ì§ì )
      2a) ìµìíì¤í¸ ì§ì ë§¤í ("ìíë²í¸|ìµìê°" â ERPì½ë)
      2b) ìµì íì¤í¸ ìë ì¶ì¶ â ì½ëë³ì¹­ë§µ â ëª¨ë¸ì­ë°©í¥ë§µ
      3) ìí¸1 (ë©ì¸ìí, ìíë²í¸ ê¸°ì¤)
      4a) ì¶ê°ìííì¤í¸ ì§ì ë§¤í ("ìíë²í¸|ì¶ê°ìíê°" â ERPì½ë)
      4b) ìí¸3 (ì¶ê°ìí, ìíë²í¸ ê¸°ì¤)
    """
    option_text = (order.get("optionInfo", "") or "").strip()
    addon_text  = (order.get("addProductInfo", "") or "").strip()
    product_no  = str(order.get("productNo", "") or order.get("productId", "") or "")

    if option_text:
        # 1) ìí¸2 ì¤ë²ë¼ì´ë (ìíë²í¸ â ERP)
        if product_no and product_no in _option_override_map:
            code = _option_override_map[product_no]
            logger.info(f"[SS] ìµìì¤ë²ë¼ì´ë(ìí¸2): {product_no} â {code}")
            return code
        # 2a) ìµìíì¤í¸ ì§ì ë§¤í (ìíë²í¸|ìµìê° â ERP)
        opt_key = f"{product_no}|{option_text}"
        if opt_key in _option_text_map:
            code = _option_text_map[opt_key]
            logger.info(f"[SS] ìµìíì¤í¸ì§ì ë§¤í: '{option_text[:40]}' â {code}")
            return code
        # 2a-2) ì¶ê°ìíë§µììë optionInfoë¡ ê²ì (ì¶ê°ìíì´ productOptionì¼ë¡ ëì´ì¤ë ê²½ì°)
        if product_no and opt_key in _addon_text_map:
            code = _addon_text_map[opt_key]
            logger.info(f"[SS] ì¶ê°ìííì¤í¸(optionInfoê²½ì ): '{option_text[:40]}' â {code}")
            return code
        # 2b) ìë ì¶ì¶ â ì½ëë³ì¹­ë§µ â ëª¨ë¸ì­ë°©í¥ë§µ
        code = _extract_erp_code_from_option(option_text)
        if code:
            erp_code = _code_alias_map.get(code) or _model_to_erp_map.get(code, code)
            logger.info(f"[SS] ìµììëì¶ì¶: '{option_text[:40]}' â ì½ë:{code} â ERP:{erp_code}")
            return erp_code

    # 3) ìí¸1 ë©ì¸ìí
    if product_no and product_no in _product_map:
        code = _product_map[product_no]
        logger.info(f"[SS] ë©ì¸ìí(ìí¸1): {product_no} â {code}")
        return code

    # 4a) ì¶ê°ìííì¤í¸ ì§ì ë§¤í
    if addon_text and product_no:
        addon_key = f"{product_no}|{addon_text}"
        if addon_key in _addon_text_map:
            code = _addon_text_map[addon_key]
            logger.info(f"[SS] ì¶ê°ìííì¤í¸ì§ì ë§¤í: '{addon_text[:40]}' â {code}")
            return code
        # ì¶ê°ìíë ì½ë ìëì¶ì¶ ìë
        code = _extract_erp_code_from_option(addon_text)
        if code:
            erp_code = _code_alias_map.get(code) or _model_to_erp_map.get(code, code)
            logger.info(f"[SS] ì¶ê°ìíìëì¶ì¶: '{addon_text[:40]}' â ì½ë:{code} â ERP:{erp_code}")
            return erp_code

    # 4b) ìí¸3 ì¶ê°ìí (ìíë²í¸ ê¸°ì¤)
    if product_no and product_no in _addon_map:
        code = _addon_map[product_no]
        logger.info(f"[SS] ì¶ê°ìí(ìí¸3): {product_no} â {code}")
        return code

    seller_code = (order.get("sellerProductCode", "") or "").strip()
    logger.warning(
        f"[SS] ë§¤ì¹­ ì¤í¨: productNo={product_no}, option='{option_text[:30]}', "
        f"sellerCode={seller_code}, name={order.get('productName','')[:40]}"
    )
    return None


def _is_excluded(order: dict) -> bool:
    # ì¤ì²© êµ¬ì¡°(_rawOrders: {productOrder: {...}})ì ííí êµ¬ì¡°(ê·¸ë£¹ ë´ë¶ dict) ëª¨ë ì§ì
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
    # ìë² outbound IP íì¸ (ë¤ì´ë² IP íì´í¸ë¦¬ì¤í¸ ë±ë¡ì©)
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
    date_from: Optional[str] = Query(None, description="ììì¼ YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="ì¢ë£ì¼ YYYY-MM-DD"),
    order_type: str = Query("NEW_BEFORE", description="NEW_BEFORE|NEW_AFTER|DELIVERING"),
):
    try:
        from services.naver_client import naver_client
        orders = await naver_client.fetch_orders(
            date_from=date_from, date_to=date_to, order_type=order_type,
        )
        return {"success": True, "orders": orders, "count": len(orders)}
    except Exception as e:
        logger.error(f"[SS] ì£¼ë¬¸ìì§ ì¤ë¥: {e}", exc_info=True)
        return {"success": False, "error": str(e), "orders": []}


class SendErpRequest(BaseModel):
    orders: list[dict]
    emp_cd: str = ""
    wh_cd: str = ""  # "10"=ì©ì°, "30"=íµì§, ""=ê¸°ë³¸ê°

@router.post("/send-erp")
async def send_erp_only(
    req: SendErpRequest,
):
    """ERP íë§¤ì íë§ ì ì¡ (ë¡ì   ë¯¸í¬í¨)"""
    selected_orders = req.orders
    _emp_cd = req.emp_cd or SMARTSTORE_EMP_CODE
    _wh_cd = req.wh_cd if req.wh_cd else SMARTSTORE_WH_CODE
    from services.erp_client_ss import ERPClientSS

    if not selected_orders:
        return {"success": True, "message": "ì íë ì£¼ë¬¸ì´ ììµëë¤.", "lines": 0}

    order_groups = {}
    unmatched_items = []

    order_shipping: dict = {}   # orderId â ë°°ì¡ë¹ ê¸ì¡
    for o in selected_orders:
        od = o.get("order", {})
        po = o.get("productOrder", {})
        oid = od.get("orderId", "")
        poid = po.get("productOrderId", "")
        if not oid or not poid:
            continue
        if oid not in order_groups:
            order_groups[oid] = []
            # ë°°ì¡ë¹: productOrder.deliveryFeeAmount
            fee = float(po.get("deliveryFeeAmount", 0) or 0)
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
            "rcvName": po.get("shippingAddress", {}).get("name", ""),
        })

    DELIVERY_PROD_CD = "DEL-ë§¤ì¶ë°°002"
    erp_lines = []

    for oid, group in order_groups.items():
        for o in group:
            # ì§ì  ì íí´ì ERP ì ì¡íë ê²½ì° ì ì¸ í¤ìë ë¬´ì (ì¬ì©ì ëªìì  ì í ì°ì )
            code = _match_item_code(o)
            qty = int(o.get("quantity", 1) or 1)
            settle = float(o.get("settlementAmount", 0) or 0)
            if code:
                erp_lines.append({"prod_cd": code, "prod_name": o.get("productName", ""), "qty": qty, "price": round(settle / qty, 2) if qty else 0, "rcv_name": o.get("rcvName", "")})
            else:
                unmatched_items.append({
                    "orderId": oid,
                    "productOrderId": o.get("productOrderId", ""),
                    "productNo": o.get("productNo", "") or o.get("productId", ""),
                    "productName": o.get("productName", ""),
                    "optionInfo": o.get("optionInfo", ""),
                    "quantity": qty, "settlementAmount": settle,
                })

    # ë°°ì¡ë¹: ê¸ì¡ë³ë¡ ë¬¶ì´ì (ê°ì ê¸ì¡ â ìë í©ì°, ë¤ë¥¸ ê¸ì¡ â ë³ë ë¼ì¸)
    from collections import defaultdict
    delivery_by_fee: dict = defaultdict(int)
    for oid in order_groups:
        fee = order_shipping.get(oid, 0)
        delivery_by_fee[fee] += 1
    for fee_amount, count in delivery_by_fee.items():
        erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": count, "price": int(fee_amount), "rcv_name": order_groups[oid][0].get("rcvName", "") if order_groups.get(oid) else ""})

    if not erp_lines:
        return {"success": False, "error": "ERP ì ì¡ ëì ìì", "unmatched_items": unmatched_items}

    if not SMARTSTORE_CUST_CODE:
        return {"success": False, "error": "SMARTSTORE_CUST_CODE ë¯¸ì¤ì "}

    # ë°ì£¼íì¸ ëì productOrderId ìì§
    all_po_ids = []
    for o in selected_orders:
        po = o.get("productOrder", {})
        poid = po.get("productOrderId", "")
        if poid:
            all_po_ids.append(poid)

    try:
        erp = ERPClientSS()
        await erp.ensure_session()
        r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, _wh_cd, _emp_cd)
        delivery_count = len(delivery_by_fee)
        r["lines"] = len(erp_lines)
        r["erp_matched"] = len(erp_lines) - delivery_count
        r["erp_unmatched"] = len(unmatched_items)
        r["unmatched_items"] = unmatched_items

        # ERP ì ì¡ ì±ê³µ ì ë¤ì´ë² ë°ì£¼íì¸ ì²ë¦¬ â "ì ê·ì£¼ë¬¸(ë°ì£¼ í)"ë¡ ì´ë
        if r.get("success") and all_po_ids:
            from services.naver_client import naver_client
            confirm_result = await naver_client.confirm_orders(all_po_ids)
            r["confirm"] = confirm_result
            logger.info(f"[SS] ë°ì£¼íì¸: {confirm_result.get('confirmed', 0)}ê±´")

        return r
    except Exception as e:
        logger.error(f"[SS] ERP ì ì¡ ì¤ë¥: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


class ExcludedSendErpRequest(BaseModel):
    orders: list[dict]
    emp_cd: str = ""

@router.post("/excluded-send-erp")
async def excluded_send_erp(
    req: ExcludedSendErpRequest,
):
    """ì ì¸ í¤ìë ì£¼ë¬¸ â ERP íë§¤ì í ì ì¡ (ë¹ê³ ì¬í­ì ê²½ëíë°°ì ë¶/ì°©ë¶ ìë ê¸°ì)"""
    selected_orders = req.orders
    _emp_cd = req.emp_cd or SMARTSTORE_EMP_CODE
    from services.erp_client_ss import ERPClientSS
    from collections import defaultdict

    if not selected_orders:
        return {"success": False, "error": "ì íë ì£¼ë¬¸ì´ ììµëë¤."}

    # ì ì¸ í¤ìë í¬í¨ ì£¼ë¬¸ë§ íí°
    excluded_orders = []
    for o in selected_orders:
        if _is_excluded(o):
            excluded_orders.append(o)

    if not excluded_orders:
        return {"success": False, "error": "ì ì¸ í¤ìë(íë¸ë/ìë²ë/ìºë¹ë·) ì£¼ë¬¸ì´ ììµëë¤."}

    # orderId ê¸°ì¤ ê·¸ë£¹í
    order_groups: dict = {}
    order_shipping: dict = {}
    order_feetype: dict = {}
    order_addr: dict = {}    # orderId â ë°°ì¡ì§ ì ë³´ (ë¹ê³ ì¬í­ í©ì°ì©)
    for o in excluded_orders:
        od = o.get("order") or {}
        po = o.get("productOrder") or {}
        oid = od.get("orderId", "") or po.get("orderId", "")
        poid = po.get("productOrderId", "")
        if not oid or not poid:
            continue
        if oid not in order_groups:
            order_groups[oid] = []
            fee = float(po.get("deliveryFeeAmount", 0) or 0)
            order_shipping[oid] = fee
            fee_type = str(po.get("shippingFeeType") or od.get("shippingFeeType") or "")
            order_feetype[oid] = fee_type
            # ë°°ì¡ì§ ì ë³´ ìì§
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

    DELIVERY_PROD_CD = "DEL-ë§¤ì¶ë°°002"
    erp_lines = []
    unmatched_items = []

    for oid, group in order_groups.items():
        # ì ë¶/ì°©ë¶ íë¨
        fee_type = order_feetype.get(oid, "")
        if "ì°©ë¶" in fee_type or fee_type.upper() in ("COLLECT", "COD"):
            delivery_type = "ê²½ëíë°°ì°©ë¶"
        else:
            delivery_type = "ê²½ëíë°°ì ë¶"

        # ë¹ê³ ì¬í­: ê²½ëíë°°ì ë¶/ì°©ë¶ / ì íì ì¸ / ìë ¹ì¸ / ì°ë½ì² / ì£¼ì / ë°°ì¡ë©ì¸ì§
        ai = order_addr.get(oid, {})
        parts = [f"{delivery_type} / ì íì ì¸"]
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

    # ë°°ì¡ë¹ ë¼ì¸ (ê²½ëíë°° ë°°ì¡ë¹) â ë¹ê³ ì¬í­ ëì¼íê² í¬í¨
    # (ëª¨ë  ë¼ì¸ì CHAR5ê° ìì´ì¼ Ecountê° ë§ì§ë§ ë¼ì¸ì¼ë¡ ë®ì´ì°ì§ ìì)
    first_remark = erp_lines[0]["remark"] if erp_lines and erp_lines[0].get("remark") else ""
    delivery_by_fee: dict = defaultdict(int)
    for oid in order_groups:
        fee = order_shipping.get(oid, 0)
        delivery_by_fee[fee] += 1
    for fee_amount, count in delivery_by_fee.items():
        erp_lines.append({"prod_cd": DELIVERY_PROD_CD, "qty": count, "price": int(fee_amount),
                           "remark": first_remark})

    if not erp_lines:
        return {"success": False, "error": "ERP ì ì¡ ëì ìì", "unmatched_items": unmatched_items}

    if not SMARTSTORE_CUST_CODE:
        return {"success": False, "error": "SMARTSTORE_CUST_CODE ë¯¸ì¤ì "}

    # ë°ì£¼íì¸ ëì productOrderId ìì§
    all_po_ids = []
    for o in excluded_orders:
        po = o.get("productOrder") or {}
        poid = po.get("productOrderId", "")
        if poid:
            all_po_ids.append(poid)

    try:
        erp = ERPClientSS()
        await erp.ensure_session()
        r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, SMARTSTORE_WH_CODE, _emp_cd)
        r["lines"] = len(erp_lines)
        r["erp_matched"] = len(erp_lines)
        r["erp_unmatched"] = len(unmatched_items)
        r["unmatched_items"] = unmatched_items
        logger.info(f"[SS] ê²½ëíë°° ERP ì ì¡: {len(erp_lines)}ê±´, ë¯¸ë§¤ì¹­: {len(unmatched_items)}ê±´")

        # ERP ì ì¡ ì±ê³µ ì ë¤ì´ë² ë°ì£¼íì¸ ì²ë¦¬ â "ì ê·ì£¼ë¬¸(ë°ì£¼ í)"ë¡ ì´ë
        if r.get("success") and all_po_ids:
            from services.naver_client import naver_client
            confirm_result = await naver_client.confirm_orders(all_po_ids)
            r["confirm"] = confirm_result
            logger.info(f"[SS] ê²½ë ë°ì£¼íì¸: {confirm_result.get('confirmed', 0)}ê±´")

        return r
    except Exception as e:
        logger.error(f"[SS] ê²½ëíë°° ERP ì ì¡ ì¤ë¥: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/excluded-export-excel")
async def excluded_export_excel(
    selected_orders: list[dict] = Body(...),
):
    """ì ì¸ í¤ìë ì£¼ë¬¸ â ê²½ëíë°° ì íì© ìì ë¤ì´ë¡ë"""
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from fastapi.responses import StreamingResponse

    # ì ì¸ í¤ìë í¬í¨ ì£¼ë¬¸ë§ íí° (orderId ê¸°ì¤ ê·¸ë£¹)
    excluded_oids: set = set()
    for o in selected_orders:
        po = o.get("productOrder") or {}
        combined = ((po.get("productName", "") or "") + " " + (po.get("productOption", "") or "")).lower()
        if any(kw in combined for kw in EXCLUDE_KEYWORDS):
            od = o.get("order") or {}
            oid = od.get("orderId", "") or po.get("orderId", "")
            if oid:
                excluded_oids.add(oid)
    logger.info(f"[SS] excluded-export-excel: ì ì¸ ì£¼ë¬¸ {len(excluded_oids)}ê° orderId={excluded_oids}")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "ê²½ëíë°°ì í"

    headers = ["ë¹ê³ ì¬í­", "ìë ¹ì¸", "ì°ë½ì²", "ì£¼ì", "ë°°ì¡ë©ì¸ì§"]
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
            if "ì°©ë¶" in fee_type or fee_type.upper() in ("COLLECT", "COD"):
                delivery_type = "ê²½ëíë°°ì°©ë¶"
            else:
                delivery_type = "ê²½ëíë°°ì ë¶"

            ws.cell(row, 1, f"{delivery_type} / ì íì ì¸")
            ws.cell(row, 2, rcv)
            ws.cell(row, 3, tel)
            ws.cell(row, 4, full_addr)
            ws.cell(row, 5, cust_msg)
            row += 1
        except Exception as ex:
            logger.error(f"[SS] excluded-export-excel í ì²ë¦¬ ì¤ë¥: {ex}", exc_info=True)
            continue

    if row == 2:
        raise HTTPException(status_code=404, detail="ì ì¸ í¤ìë(íë¸ë/ìë²ë/ìºë¹ë·) ì£¼ë¬¸ì´ ììµëë¤.")

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    from urllib.parse import quote
    filename = f"ê²½ëíë°°ì í_{datetime.now(KST).strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"}
    )


@router.post("/logen-export-excel")
async def logen_export_excel(
    selected_orders: list[dict] = Body(...),
):
    """ì í ì£¼ë¬¸ì ë¡ì   ì ì¡ì© ììë¡ ë¤ì´ë¡ë.
    ì»¬ë¼: ì£¼ë¬¸ë²í¸ | ìíì£¼ë¬¸ë²í¸ | ìë ¹ì¸ | ì°ë½ì² | ì£¼ì | ìíëª | ìë
    """
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from fastapi.responses import StreamingResponse

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "ììíì¼ì²«í-ì ëª©ìì"

    # ë¡ì   êµ¬ìì¤íìì (Aíì) - ì¬ì©ì ì¤ì  ì»¬ë¼ ìì
    # A:ìíì¸ëª B:ìíì¸ì£¼ì1 C:ìíì¸ì í D:ìíì¸í´ëí°
    # E:íë°°ìë F:íë°°ì´ì G:ì´ìêµ¬ë¶ H:ë¬¼íëª I:ì£¼ë¬¸ë²í¸(âë°ííì¼ Sì´ ë§¤ì¹­ì©)
    # J:ì ì£¼ì´ìêµ¬ë¶ K:ë°°ì¡ë©ì¸ì§
    headers = ["ìíì¸ëª", "ìíì¸ì£¼ì1", "ìíì¸ì í", "ìíì¸í´ëí°",
               "íë°°ìë", "íë°°ì´ì", "ì´ìêµ¬ë¶", "ë¬¼íëª", "ì£¼ë¬¸ë²í¸",
               None, "ë°°ì¡ë©ì¸ì§"]
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

    # orderId ê¸°ì¤ì¼ë¡ ê·¸ë£¹í
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
        po = g["po"]   # ì²« ë²ì§¸ productOrder (ìë ¹ì¸ ì ë³´ì©)
        addr     = po.get("shippingAddress", {})
        rcv      = addr.get("name", "")
        tel_home = addr.get("tel1", "")
        tel_cell = addr.get("tel2", "") or addr.get("tel1", "")
        full_addr = ((addr.get("baseAddress","") or "") + " " + (addr.get("detailedAddress","") or "")).strip()
        ship_fee = int(float(po.get("deliveryFeeAmount", 0) or 0))
        fare_tp  = "010"

        # íëª©ëª: ëª¨ë¸ëª(or ERPì½ë) + ìë ìì½
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
            # ëª¨ë¸ëª ì°ì , ìì¼ë©´ ERPì½ë, ìì¼ë©´ ìíëª
            model = _model_map.get(product_id, "")
            if not model:
                model = _product_map.get(product_id, "") or item_po.get("productName", "")[:20]
            qty = int(item_po.get("quantity", 1) or 1)
            model_qty[model] = model_qty.get(model, 0) + qty
            total_qty += qty

        goods = ", ".join(f"{m} x{q}" for m, q in model_qty.items())[:50]

        ws.cell(row, 1,  rcv)         # A: ìíì¸ëª
        ws.cell(row, 2,  full_addr)   # B: ìíì¸ì£¼ì1
        ws.cell(row, 3,  tel_home)    # C: ìíì¸ì í
        ws.cell(row, 4,  tel_cell)    # D: ìíì¸í´ëí°
        ws.cell(row, 5,  1)            # E: íë°°ìë (ë°ì¤ ìë, í­ì 1)
        ws.cell(row, 6,  ship_fee)    # F: íë°°ì´ì
        ws.cell(row, 7,  fare_tp)     # G: ì´ìêµ¬ë¶
        ws.cell(row, 8,  goods)       # H: ë¬¼íëª (ëª¨ë¸ëª+ìë)
        ws.cell(row, 9,  first_poid)  # I: ì£¼ë¬¸ë²í¸ â ë°ííì¼ Sì´(index 18)ë¡ ë§¤ì¹­
        jeju = "ì ì°©ë¶" if "ì ì£¼" in full_addr else None
        ws.cell(row, 10, jeju)        # J: ì ì£¼ì´ìêµ¬ë¶ (ì ì£¼ ì£¼ìë©´ ì ì°©ë¶ ìë)
        ws.cell(row, 11, cust_msg)    # K: ë°°ì¡ë©ì¸ì§ (ê³ ê° ìì²­ì¬í­)
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
    carrier: str = Form("LOGEN"),
):
    """ì¡ì¥ë²í¸ ê¸°ìë ìì ìë¡ë â ë¤ì´ë² ë°ì¡ì²ë¦¬.
    Hì´(8ë²ì§¸)ì ì¡ì¥ë²í¸, Aì´(1ë²ì§¸)ì ì£¼ë¬¸ë²í¸, Bì´(2ë²ì§¸)ì ìíì£¼ë¬¸ë²í¸
    """
    import io
    import openpyxl
    from services.naver_client import naver_client

    content = await file.read()
    wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
    ws = wb.active

    dispatch_list = []
    skipped = []
    # ë¡ì   ë°í íì¼ êµ¬ì¡°:
    #   1í: íì´í, 2í: í¤ë, 3í: ìë¸í¤ë, 4í~: ë°ì´í°
    #   Dì´(index 3): ì´ì¡ì¥ë²í¸
    #   Sì´(index 18): ì£¼ë¬¸ë²í¸ â ë¤ì´ë¡ë ì Iì´ì ì½ìí productOrderId
    for row in ws.iter_rows(min_row=4, values_only=True):
        if not any(row):
            continue
        tracking = str(row[3]  or "").strip()   # Dì´: ì´ì¡ì¥ë²í¸
        poid     = str(row[18] or "").strip()   # Sì´: ì£¼ë¬¸ë²í¸(=productOrderId)
        if not tracking or not poid or tracking == "None" or poid == "None":
            skipped.append(poid or str(row[6] or ""))
            continue
        dispatch_list.append({
            "productOrderId": poid,
            "deliveryCompanyCode": carrier,
            "trackingNumber": tracking,
        })

    if not dispatch_list:
        return {"success": False, "error": f"ì¡ì¥ë²í¸ê° ìë ¥ë íì´ ììµëë¤. (ë¹ í: {len(skipped)}ê°)"}

    try:
        result = await naver_client.dispatch_orders(dispatch_list)
        result["dispatched_count"] = len(dispatch_list)
        result["skipped_count"] = len(skipped)
        logger.info(f"[SS] ììë°ì¡ì²ë¦¬: {len(dispatch_list)}ê±´, ê²°ê³¼={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[SS] ììë°ì¡ì²ë¦¬ ì¤ë¥: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/dispatch-manual")
async def dispatch_manual(
    body: dict = Body(...),
):
    """ìê¸° ì¡ì¥ë²í¸ë¡ ë¤ì´ë² ë°ì¡ì²ë¦¬.
    body: { "items": [{"productOrderId": "...", "trackingNumber": "..."}, ...] }
    """
    from services.naver_client import naver_client
    items = body.get("items", [])
    if not items:
        return {"success": False, "error": "ì¡ì¥ ë°ì´í°ê° ììµëë¤."}

    dispatch_list = [
        {"productOrderId": it["productOrderId"], "deliveryCompanyCode": "LOGEN", "trackingNumber": it["trackingNumber"]}
        for it in items if it.get("productOrderId") and it.get("trackingNumber")
    ]
    if not dispatch_list:
        return {"success": False, "error": "ì í¨í ì¡ì¥ë²í¸ê° ììµëë¤."}

    try:
        result = await naver_client.dispatch_orders(dispatch_list)
        result["dispatched_count"] = len(dispatch_list)
        logger.info(f"[SS] ìê¸°ë°ì¡ì²ë¦¬: {len(dispatch_list)}ê±´, ê²°ê³¼={result}")
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"[SS] ìê¸°ë°ì¡ì²ë¦¬ ì¤ë¥: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/register-logen")
async def register_logen_only(
    warehouse: str = Query(..., pattern="^(gimpo|yongsan)$"),
    selected_orders: list[dict] = Body(...),
):
    """ë¡ì  íë°° ë±ë¡ + ë°ì£¼íì¸ + ë°ì¡ì²ë¦¬ (ERP ë¯¸í¬í¨)"""
    from services.naver_client import naver_client
    from services.ilogen_client import register_orders, get_sender

    if not selected_orders:
        return {"success": True, "message": "ì íë ì£¼ë¬¸ì´ ììµëë¤."}

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
        fare_code = "020" if "ì°©ë¶" in str(first.get("deliveryFeeType", "")) else "030"
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

        confirm_result = {"confirmed": 0, "message": "ë¡ì   ë±ë¡ ì¤í¨ë¡ ë³´ë¥"}
        dispatch_result = {"dispatched": 0, "message": "ë¡ì   ë±ë¡ ì¤í¨ë¡ ë³´ë¥"}

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
                dispatch_result = await naver_client.dispatch_orders(dispatch_list) if dispatch_list else {"success": True, "message": "ëì ìì"}

        return {
            "success": logen_ok,
            "logen": logen_res,
            "confirm": confirm_result,
            "dispatch": dispatch_result,
            "tracking_count": len(tns),
            "total_orders": len(all_po_ids),
        }
    except Exception as e:
        logger.error(f"[SS] ë¡ì  ë±ë¡ ì¤ë¥: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


class AutoRegisterLogenRequest(BaseModel):
    orders: list[dict]
    emp_cd: str = ""

@router.post("/auto-register-logen")
async def auto_register_logen(
    warehouse: str = Query(..., pattern="^(gimpo|yongsan)$"),
    req: AutoRegisterLogenRequest = Body(...),
):
    selected_orders = req.orders
    _emp_cd = req.emp_cd or SMARTSTORE_EMP_CODE
    from services.naver_client import naver_client
    from services.ilogen_client import register_orders, get_sender
    from services.erp_client_ss import ERPClientSS

    result = {"step1_confirm": None, "step2_erp": None, "step2_logen": None, "step3_dispatch": None, "summary": {}}

    try:
        if not selected_orders:
            return {"success": True, "message": "ì íë ì£¼ë¬¸ì´ ììµëë¤.", **result}

        order_groups = {}
        all_po_ids = []

        order_shipping: dict = {}   # orderId â ë°°ì¡ë¹ ê¸ì¡
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
                fee = float(po.get("deliveryFeeAmount", 0) or 0)
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

        logger.info(f"[SS] ì í ì£¼ë¬¸: {len(all_po_ids)}ê±´, {len(order_groups)}ê·¸ë£¹")

        # ERP ë¼ì¸ êµ¬ì±
        DELIVERY_PROD_CD = "DEL-ë§¤ì¶ë°°002"
        erp_lines = []
        unmatched_items = []

        for oid, group in order_groups.items():
            for o in group:
                if _is_excluded(o):
                    logger.info(f"[SS] ì ì¸ í¤ìë íí°: {o.get('productName','')[:40]}")
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

        # ë°°ì¡ë¹: ê¸ì¡ë³ë¡ ë¬¶ì´ì (ê°ì ê¸ì¡ â ìë í©ì°, ë¤ë¥¸ ê¸ì¡ â ë³ë ë¼ì¸)
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
            fare_code = "020" if "ì°©ë¶" in str(first.get("deliveryFeeType", "")) else "030"
            ilogen_orders.append({
                "snd_name": sender["name"], "snd_tel": sender["tel"], "snd_addr": sender["addr"],
                "rcv_name": first["rcvName"], "rcv_tel": first["rcvTel"], "rcv_addr": first["rcvAddr"],
                "fare_code": fare_code, "goods_nm": _build_goods_nm(group),
            })
            oid_to_idx[oid] = len(ilogen_orders) - 1

        async def _do_erp():
            if not erp_lines:
                return {"success": True, "lines": 0, "message": "ERP ìë ¥ ëì ìì"}
            if not SMARTSTORE_CUST_CODE:
                return {"success": False, "lines": len(erp_lines), "error": "SMARTSTORE_CUST_CODE ë¯¸ì¤ì "}
            erp = ERPClientSS()
            await erp.ensure_session()
            r = await erp.save_sale(SMARTSTORE_CUST_CODE, erp_lines, SMARTSTORE_WH_CODE, _emp_cd)
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
            if not erp_ok: reasons.append("ERP íë§¤ìë ¥ ì¤í¨")
            if not logen_ok: reasons.append("ë¡ì   ì¡ì¥ë°ê¸ ì¤í¨")
            result["step2_confirm"] = {"confirmed": 0, "message": f"ë°ì£¼íì¸ ë³´ë¥ ({', '.join(reasons)})"}

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
            result["step3_dispatch"] = await naver_client.dispatch_orders(dispatch_list) if dispatch_list else {"success": True, "message": "ëì ìì"}
        else:
            skip_reason = "ERP/ë¡ì   ë¯¸ìë£" if not (erp_ok and logen_ok) else "ì´ì¡ì¥ ìì"
            result["step3_dispatch"] = {"dispatched": 0, "message": f"ë°ì¡ì²ë¦¬ ë³´ë¥ ({skip_reason})"}

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
        logger.error(f"[SS] ìëë±ë¡ ì¤ë¥: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# âââââââââââââââââââââââââââââââââââââââââââ
# ì¬ê³ íí© ì¡°í
# âââââââââââââââââââââââââââââââââââââââââââ

@router.post("/inventory")
async def get_inventory(body: dict = Body(...)):
    """
    ERP ì¬ê³ íí© ì¡°í (ì°½ê³ ë³).
    body: { "orders": [...] }
    ì©ì°(10), íµì§(30) ì°½ê³  ì¬ê³ ë¥¼ ê°ê° ì¡°ííì¬ ë°í.
    """
    from services.erp_client_ss import ERPClientSS

    try:
        orders = body.get("orders", [])
        if not orders:
            return {"success": True, "inventory": {}, "message": "ì£¼ë¬¸ ìì"}

        prod_codes = set()
        order_erp_map = {}
        for o in orders:
            po = o.get("productOrder", {})
            poid = po.get("productOrderId", "")
            product_id = str(po.get("productId", "") or po.get("productNo", "") or "")
            seller_code = po.get("sellerProductCode", "") or ""
            option_info = po.get("productOption", "") or seller_code

            item = {
                "productNo": product_id,
                "productId": product_id,
                "sellerProductCode": seller_code,
                "optionInfo": option_info,
                "productName": po.get("productName", ""),
            }
            code = _match_item_code(item)
            if code:
                prod_codes.add(code)
                if poid:
                    order_erp_map[poid] = code

        if not prod_codes:
            return {"success": True, "inventory": {"yongsan": {}, "tongjin": {}}, "order_erp_map": {}, "message": "ë§¤ì¹­ë íëª©ì½ë ìì"}

        erp = ERPClientSS()
        await erp.ensure_session()
        # ECOUNT API PROD_CD íí°ê° ì½¤ë§ êµ¬ë¶ì ì§ìíì§ ìì¼ë¯ë¡
        # ì ì²´ ì¬ê³ ë¥¼ ì¡°íí í Pythonìì íí°ë§
        result = await erp.get_inventory_by_warehouses(prod_codes=None)

        # íìí íëª©ì½ëë§ íí°ë§
        if result.get("success") and result.get("inventory"):
            inv = result["inventory"]
            filtered_yongsan = {k: v for k, v in inv.get("yongsan", {}).items() if k in prod_codes}
            filtered_tongjin = {k: v for k, v in inv.get("tongjin", {}).items() if k in prod_codes}
            result["inventory"] = {"yongsan": filtered_yongsan, "tongjin": filtered_tongjin}

        result["order_erp_map"] = order_erp_map

        return result
    except Exception as e:
        logger.error(f"[SS] ì¬ê³ ì¡°í ì¤ë¥: {e}", exc_info=True)
        return {"success": False, "error": str(e), "inventory": {}}



@router.get("/inventory-debug")
async def inventory_debug():
    """ëë²ê·¸: PROD_CD ìì´ ì¬ê³  ì ì²´ ì¡°í (ìµë ëªê±´ ë°íëëì§ íì¸)"""
    from services.erp_client_ss import ERPClientSS
    try:
        erp = ERPClientSS()
        await erp.ensure_session()
        r10 = await erp.get_inventory_balance(prod_codes=None, wh_cd="10")
        r30 = await erp.get_inventory_balance(prod_codes=None, wh_cd="30")
        return {
            "success": True,
            "yongsan_count": r10.get("total", 0),
            "yongsan_sample": dict(list(r10.get("inventory", {}).items())[:5]),
            "tongjin_count": r30.get("total", 0),
            "tongjin_sample": dict(list(r30.get("inventory", {}).items())[:5]),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/reload-product-map")
async def reload_product_map():
    _load_product_map()
    return {"success": True, "count": len(_product_map)}


# âââââââââââââââââââââââââââââââââââââââââââ
# ë§¤í ê´ë¦¬ API
# âââââââââââââââââââââââââââââââââââââââââââ

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
        return {"success": False, "error": "ìíë²í¸ì íëª©ì½ëë íììëë¤."}

    is_new = prod_no not in _product_map
    _product_map[prod_no] = erp_code
    if model:
        _model_map[prod_no] = model
    _save_product_map()

    action = "ì¶ê°" if is_new else "ìì "
    logger.info(f"[SS] ë§¤í {action}: {prod_no} â ERP:{erp_code}, ëª¨ë¸:{model}")
    return {"success": True, "action": action, "productNo": prod_no, "erpCode": erp_code, "model": model,
            "total": len(_product_map)}


@router.delete("/product-map/{product_no}")
async def delete_product_map(product_no: str):
    if product_no not in _product_map:
        return {"success": False, "error": f"ìíë²í¸ {product_no} ë§¤íì´ ììµëë¤."}
    erp_code = _product_map.pop(product_no)
    _model_map.pop(product_no, None)
    _save_product_map()
    logger.info(f"[SS] ë§¤í ì­ì : {product_no} (was {erp_code})")
    return {"success": True, "deleted": product_no, "total": len(_product_map)}


# âââââââââââââââââââââââââââââââââââââââââââ
# Excel ìë¡ë/ë¤ì´ë¡ë API
# âââââââââââââââââââââââââââââââââââââââââââ

def _make_header(ws, headers: list, fill_color: str):
    """ê³µíµ í¤ë ì¤íì¼ ì ì©"""
    from openpyxl.styles import Font, PatternFill, Alignment
    fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center")


def _find_data_start(ws) -> int:
    """í¤ëí(ìíë²í¸ í¬í¨) ë¤ì í ë°í"""
    for r in range(1, min(5, ws.max_row + 1)):
        if "ìíë²í¸" in str(ws.cell(r, 1).value or ""):
            return r + 1
    return 2  # í¤ë ìì¼ë©´ 2íë¶í°


def _read_sheet_2col(ws) -> tuple[dict, dict]:
    """ìíë²í¸|ERPíëª©ì½ë|ëª¨ë¸ëª ìí¸ ì½ê¸° â (map, model_map)"""
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
    """íì¬ ë§¤í ì ì²´ë¥¼ 3ìí¸ Excelë¡ ë¤ì´ë¡ë
       ìí¸1: ë©ì¸ìí / ìí¸2: ìµììí(ì¤ë²ë¼ì´ë) / ìí¸3: ì¶ê°ìí
    """
    import io
    from fastapi.responses import StreamingResponse
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl ë¯¸ì¤ì¹")

    wb = openpyxl.Workbook()

    # ââ ìí¸1: ë©ì¸ìí ââââââââââââââââââââââââââ
    ws1 = wb.active
    ws1.title = "1_ë©ì¸ìí"
    _make_header(ws1, ["ìíë²í¸", "ERPíëª©ì½ë", "ëª¨ë¸ëª(ë¡ì  ì¡ì¥ì©)"], "1F4E79")
    ws1.column_dimensions["A"].width = 20
    ws1.column_dimensions["B"].width = 30
    ws1.column_dimensions["C"].width = 30
    for i, (pno, code) in enumerate(_product_map.items(), start=2):
        ws1.cell(i, 1, pno); ws1.cell(i, 2, code); ws1.cell(i, 3, _model_map.get(pno, ""))

    # ââ ìí¸2: ìµììí ââââââââââââââââââââââââââ
    ws2 = wb.create_sheet("2_ìµììí")
    _make_header(ws2, ["ìíë²í¸", "ìµìíì¤í¸(ì°¸ê³ ì©)", "ìëì¶ì¶ì½ë(ì°¸ê³ ì©)", "ERPíëª©ì½ë(ë¹ì°ë©´ìë)", "ëª¨ë¸ëª(ë¡ì  ì¡ì¥ì©)"], "375623")
    ws2.column_dimensions["A"].width = 20
    ws2.column_dimensions["B"].width = 45
    ws2.column_dimensions["C"].width = 25
    ws2.column_dimensions["D"].width = 30
    ws2.column_dimensions["E"].width = 30
    for i, (pno, code) in enumerate(_option_override_map.items(), start=2):
        ws2.cell(i, 1, pno); ws2.cell(i, 4, code); ws2.cell(i, 5, _model_map.get(pno, ""))

    # ââ ìí¸3: ì¶ê°ìí ââââââââââââââââââââââââââ
    ws3 = wb.create_sheet("3_ì¶ê°ìí")
    _make_header(ws3, ["ìíë²í¸", "ERPíëª©ì½ë", "ëª¨ë¸ëª(ë¡ì  ì¡ì¥ì©)"], "7B3F00")
    ws3.column_dimensions["A"].width = 20
    ws3.column_dimensions["B"].width = 30
    ws3.column_dimensions["C"].width = 30
    for i, (pno, code) in enumerate(_addon_map.items(), start=2):
        ws3.cell(i, 1, pno); ws3.cell(i, 2, code); ws3.cell(i, 3, _model_map.get(pno, ""))

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    logger.info(f"[SS] Excel ë´ë³´ë´ê¸° â ë©ì¸:{len(_product_map)} ìµì:{len(_option_override_map)} ì¶ê°:{len(_addon_map)}")
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=smartstore_product_map.xlsx"},
    )


@router.post("/product-map/import-excel")
async def import_product_map_excel(file: bytes = Body(..., media_type="application/octet-stream")):
    """3ìí¸ Excel ìë¡ë â ì ì²´ ë§¤í ê°±ì 
       ìí¸1: ë©ì¸ìí / ìí¸2: ìµììí / ìí¸3: ì¶ê°ìí
    """
    import io
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl ë¯¸ì¤ì¹")

    try:
        wb = openpyxl.load_workbook(io.BytesIO(file), data_only=True)

        new_product_map, new_model_map, new_option_map, new_addon_map = {}, {}, {}, {}

        # ìí¸1: ë©ì¸ìí (ì»¬ë¼: ìíë²í¸|ERPíëª©ì½ë|ëª¨ë¸ëª)
        if len(wb.sheetnames) >= 1:
            m, mdl = _read_sheet_2col(wb.worksheets[0])
            new_product_map.update(m); new_model_map.update(mdl)

        # ìí¸2: ìµììí (ì»¬ë¼: ìíë²í¸|ìµìíì¤í¸|ìëì¶ì¶ì½ë|ERPíëª©ì½ë|ëª¨ë¸ëª)
        if len(wb.sheetnames) >= 2:
            ws2 = wb.worksheets[1]
            data_start = _find_data_start(ws2)
            for r in range(data_start, ws2.max_row + 1):
                pno  = str(ws2.cell(r, 1).value or "").strip()
                code = str(ws2.cell(r, 4).value or "").strip()  # Dì´: ERPíëª©ì½ë
                mdl  = str(ws2.cell(r, 5).value or "").strip()  # Eì´: ëª¨ë¸ëª
                if pno and code and pno != "None" and code != "None":
                    new_option_map[pno] = code
                    if mdl and mdl != "None":
                        new_model_map[pno] = mdl

        # ìí¸3: ì¶ê°ìí (ì»¬ë¼: ìíë²í¸|ERPíëª©ì½ë|ëª¨ë¸ëª)
        if len(wb.sheetnames) >= 3:
            m, mdl = _read_sheet_2col(wb.worksheets[2])
            new_addon_map.update(m); new_model_map.update(mdl)

        total = len(new_product_map) + len(new_option_map) + len(new_addon_map)
        if total == 0:
            return {"success": False, "error": "ì í¨í ë°ì´í°ê° ììµëë¤."}

        global _product_map, _option_override_map, _addon_map, _model_map, _model_to_erp_map
        _product_map         = new_product_map
        _option_override_map = new_option_map
        _addon_map           = new_addon_map
        _model_map           = new_model_map
        _save_product_map()
        _load_product_map()   # ì­ë°©í¥ ë§µ ì¬ë¹ë

        logger.info(f"[SS] Excel ê°ì ¸ì¤ê¸° â ë©ì¸:{len(_product_map)} ìµì:{len(_option_override_map)} ì¶ê°:{len(_addon_map)}")
        return {
            "success": True,
            "sheet1_main": len(_product_map),
            "sheet2_option": len(_option_override_map),
            "sheet3_addon": len(_addon_map),
            "message": f"ë©ì¸ {len(_product_map)}ê±´ / ìµì {len(_option_override_map)}ê±´ / ì¶ê°ìí {len(_addon_map)}ê±´ ê°±ì  ìë£",
        }

    except Exception as e:
        logger.error(f"[SS] Excel ê°ì ¸ì¤ê¸° ì¤ë¥: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"íì¼ íì± ì¤ë¥: {e}")


@router.post("/product-map/fetch-options-excel")
async def fetch_options_excel(body: dict = Body(...)):
    """ìíë²í¸ ëª©ë¡ì¼ë¡ ë¤ì´ë² API ì¡°í â ìµì/ì¶ê°ìí ë§¤í ììì© Excel ë¤ì´ë¡ë"""
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    from fastapi.responses import StreamingResponse
    from services.naver_client import naver_client

    product_nos = body.get("productNos", [])
    if not product_nos:
        raise HTTPException(status_code=400, detail="productNos íì")

    items = await naver_client.fetch_products_with_options(product_nos)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "ìµì_ì¶ê°ìí_ë§¤í"

    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    hdr_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    yel_fill = PatternFill("solid", fgColor="FFF2CC")
    data_font = Font(name="Arial", size=10)

    cols = ["êµ¬ë¶", "ìíë²í¸", "ìíëª", "ìµìíì¤í¸", "íë§¤ìì½ë(ì°¸ê³ )", "ì¬ê³ ", "ERPíëª©ì½ë(ìë ¥)", "ëª¨ë¸ëª(ìë ¥)"]
    for ci, col in enumerate(cols, 1):
        c = ws.cell(1, ci, col)
        c.fill = hdr_fill; c.font = hdr_font
        c.alignment = Alignment(horizontal="center")

    widths = [10, 18, 40, 40, 20, 8, 25, 20]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    for ri, item in enumerate(items, 2):
        # íì¬ ë§¤í ì¬ë¶ íì¸
        pno = item["productNo"]
        existing_erp = _product_map.get(pno, "") or _option_override_map.get(pno, "")
        existing_model = _model_map.get(pno, "")

        vals = [
            item["type"], pno, item["productName"], item["optionText"],
            item["sellerCode"], item["stock"],
            existing_erp,   # ê¸°ì¡´ ë§¤í ìì¼ë©´ ë¯¸ë¦¬ ì±ìì¤
            existing_model,
        ]
        for ci, val in enumerate(vals, 1):
            c = ws.cell(ri, ci, val)
            c.font = data_font
            if ci in (7, 8):
                c.fill = yel_fill

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    logger.info(f"[SS] ìµìì¡°í Excel: {len(items)}ê° í­ëª©")
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=smartstore_options.xlsx"},
    )


@router.get("/product-map/debug-option/{product_no}")
async def debug_channel_product(product_no: str, naver_no: str = ""):
    """ì±ëìí API ìëµ êµ¬ì¡° íì¸ì© (ëë²ê·¸)
    product_no = ìíë²í¸(ì¤ë§í¸ì¤í ì´) = ììíë²í¸
    naver_no   = ë¤ì´ë²ì¼íìíë²í¸    = ì±ëìíë²í¸
    """
    from services.naver_client import naver_client
    headers = await naver_client._headers()
    import httpx
    from config import NAVER_COMMERCE_URL

    endpoints = {
        # ììí ì¡°í (v2) â ìíë²í¸(ì¤ë§í¸ì¤í ì´) ì¬ì©
        "v2_ììí": f"{NAVER_COMMERCE_URL}/external/v2/products/origin-products/{product_no}",
        # ì±ëìí ì¡°í (v2) â ìíë²í¸ë¡ ìë
        "v2_ì±ë_ììíë²í¸": f"{NAVER_COMMERCE_URL}/external/v2/channel-products/{product_no}",
    }
    if naver_no:
        # ì±ëìí ì¡°í (v2) â ë¤ì´ë²ì¼íìíë²í¸ë¡ ìë
        endpoints["v2_ì±ë_ì¼íë²í¸"] = f"{NAVER_COMMERCE_URL}/external/v2/channel-products/{naver_no}"

    results = {}
    async with httpx.AsyncClient(timeout=15) as client:
        for key, url in endpoints.items():
            try:
                r = await client.get(url, headers=headers)
                results[key] = {"status": r.status_code, "url": url, "body": r.json()}
            except Exception as e:
                results[key] = {"error": str(e), "url": url}
    return results
