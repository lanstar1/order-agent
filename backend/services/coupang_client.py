"""쿠팡 윙 OPEN API 클라이언트 — 반품/교환 클레임 수집 (CS 자동접수용).

- HMAC-SHA256 서명 인증 (IP 화이트리스트 없음 → Render에서 직접 호출 가능)
- 키는 환경변수(COUPANG_*) 우선, 없으면 설정 UI에 저장된 값(app_settings) 폴백
- fetch_claims(days): 반품(returnRequests) + 교환(exchangeRequests)을 수집해
  네이버 클레임과 동일한 스키마의 dict 리스트로 정규화 (channel="쿠팡")
"""
import os
import time
import hmac
import hashlib
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

HOST = "https://api-gateway.coupang.com"
KST = timezone(timedelta(hours=9))

# 쿠팡 반품 수거상태(receiptStatus) → 수거완료(물류수령 전환 대상) 판정
COUPANG_COLLECTED_STATUSES = {
    "VENDOR_WAREHOUSE_CONFIRM",  # 입고완료(판매자 창고 확인)
    "RETURNS_COMPLETED",          # 반품완료
    "EXCHANGE_DELIVERY_COMPLETED",  # 교환 배송완료
    "SUCCESS",                    # 교환 완료
}

# 쿠팡 반품/교환 사유코드 → CS 사유 분류
COUPANG_REASON_CATEGORY = {
    "CHANGEMIND": "단순 변심",
    "DONOTNEED": "단순 변심",
    "DIFFERENTOPT": "주문 실수",
    "WRONGORDER": "주문 실수",
    "DELIVERYDELAY": "오배송 및 지연",
    "MISDELIVERY": "오배송 및 지연",
    "DAMAGED": "파손 및 불량",
    "DEFECTIVE": "파손 및 불량",
    "BROKEN": "파손 및 불량",
    "FAULTYITEM": "파손 및 불량",
}


def _get_keys():
    access = os.getenv("COUPANG_ACCESS_KEY", "")
    secret = os.getenv("COUPANG_SECRET_KEY", "")
    vendor = os.getenv("COUPANG_VENDOR_ID", "")
    if not (access and secret and vendor):
        try:
            from api.routes.settings import get_llm_setting
            access = access or get_llm_setting("api_coupang_access", "")
            secret = secret or get_llm_setting("api_coupang_secret", "")
            vendor = vendor or get_llm_setting("api_coupang_vendor", "")
        except Exception:
            pass
    return access, secret, vendor


def _auth(method, path, query, access, secret):
    dt = time.strftime("%y%m%dT%H%M%SZ", time.gmtime())
    sig = hmac.new(secret.encode(), (dt + method + path + query).encode(), hashlib.sha256).hexdigest()
    return f"CEA algorithm=HmacSHA256, access-key={access}, signed-date={dt}, signature={sig}"


def _call(method, path, query, access, secret, max_retries=4):
    url = HOST + path + (("?" + query) if query else "")
    for attempt in range(max_retries):
        req = urllib.request.Request(url, method=method)
        req.add_header("Authorization", _auth(method, path, query, access, secret))
        req.add_header("Content-Type", "application/json;charset=UTF-8")
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                return r.status, json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            txt = e.read().decode("utf-8", "replace")
            if e.code == 429 or e.code >= 500:
                time.sleep(2 ** attempt)
                continue
            return e.code, txt
        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == max_retries - 1:
                return -1, str(e)
    return 429, "max retries exceeded"


def _norm_return(rec: dict) -> dict:
    items = rec.get("returnItems") or []
    first = items[0] if items else {}
    name = first.get("vendorItemName") or first.get("sellerProductName") or "-"
    qty = sum(int(i.get("cancelCount") or 0) for i in items) or 1
    dtos = rec.get("returnDeliveryDtos") or []
    dto = dtos[0] if dtos else {}
    charge = rec.get("returnShippingCharge") or 0
    fault = rec.get("faultByType") or ""
    if fault == "VENDOR":
        ship = "무료반품"
    elif charge and abs(int(charge)) > 0:
        ship = "환불금에서 차감"
    else:
        ship = ""
    reason_text = rec.get("reasonCodeText") or rec.get("cancelReasonCategory2") or rec.get("cancelReason") or ""
    return {
        "channel": "쿠팡",
        "product_order_id": "CP-" + str(rec.get("receiptId") or ""),
        "order_id": str(rec.get("orderId") or ""),
        "claim_type": "반품",
        "claim_status": rec.get("receiptStatus") or "",
        "customer_name": rec.get("requesterName") or "",
        "contact_info": rec.get("requesterPhoneNumber") or rec.get("requesterRealPhoneNumber") or "",
        "product_name": name,
        "product_option": "",
        "quantity": qty,
        "reason_code": rec.get("reasonCode") or "",
        "reason_category": COUPANG_REASON_CATEGORY.get(rec.get("reasonCode") or "", "기타"),
        "detailed_reason": reason_text,
        "claim_request_date": rec.get("createdAt") or "",
        "collect_courier": dto.get("deliveryCompanyCode") or "",
        "collect_tracking_no": dto.get("deliveryInvoiceNo") or "",
        "shipping_cost_status": ship,
    }


def _norm_exchange(rec: dict) -> dict:
    # 교환 응답은 반품과 유사하나 일부 필드명이 다를 수 있어 방어적으로 매핑
    items = rec.get("exchangeItems") or rec.get("returnItems") or []
    first = items[0] if items else {}
    name = first.get("vendorItemName") or first.get("sellerProductName") or "-"
    qty = sum(int(i.get("quantity") or i.get("cancelCount") or 0) for i in items) or 1
    dtos = rec.get("returnDeliveryDtos") or rec.get("collectDeliveryDtos") or []
    dto = dtos[0] if dtos else {}
    reason_code = rec.get("reasonCode") or ""
    reason_text = rec.get("reasonCodeText") or rec.get("reason") or ""
    return {
        "channel": "쿠팡",
        "product_order_id": "CPEX-" + str(rec.get("exchangeId") or rec.get("receiptId") or ""),
        "order_id": str(rec.get("orderId") or ""),
        "claim_type": "교환",
        "claim_status": rec.get("status") or rec.get("receiptStatus") or "",
        "customer_name": rec.get("requesterName") or "",
        "contact_info": rec.get("requesterPhoneNumber") or "",
        "product_name": name,
        "product_option": "",
        "quantity": qty,
        "reason_code": reason_code,
        "reason_category": COUPANG_REASON_CATEGORY.get(reason_code, "기타"),
        "detailed_reason": reason_text,
        "claim_request_date": rec.get("createdAt") or "",
        "collect_courier": dto.get("deliveryCompanyCode") or "",
        "collect_tracking_no": dto.get("deliveryInvoiceNo") or "",
        "shipping_cost_status": "",
    }


def fetch_claims(days: int = 14) -> list:
    """쿠팡 반품 + 교환 클레임 수집 → 정규화 dict 리스트. 키 미설정/오류 시 []"""
    access, secret, vendor = _get_keys()
    if not (access and secret and vendor):
        logger.warning("[Coupang] API 키 미설정 (COUPANG_ACCESS_KEY/SECRET_KEY/VENDOR_ID)")
        return []

    now = datetime.now(KST)
    claims = {}

    # ── 반품 (날짜 YYYY-MM-DD, status 필수 — 상태별 스윕 후 receiptId 중복제거) ──
    rpath = f"/v2/providers/openapi/apis/api/v4/vendors/{vendor}/returnRequests"
    frm_d = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    to_d = now.strftime("%Y-%m-%d")
    for st in ("RU", "CC", "PR", "UC"):
        q = f"createdAtFrom={frm_d}&createdAtTo={to_d}&status={st}&maxPerPage=50"
        code, body = _call("GET", rpath, q, access, secret)
        if code != 200 or not isinstance(body, dict):
            logger.warning(f"[Coupang] 반품 조회 실패 status={st}: {code} {str(body)[:150]}")
            continue
        for rec in body.get("data") or []:
            c = _norm_return(rec)
            if c["product_order_id"] != "CP-":
                claims[c["product_order_id"]] = c
        time.sleep(0.3)

    # ── 교환 (datetime, 기간 ≤7일 제한 → 7일 청크) ──
    epath = f"/v2/providers/openapi/apis/api/v4/vendors/{vendor}/exchangeRequests"
    chunk_start = now - timedelta(days=days)
    while chunk_start < now:
        chunk_end = min(chunk_start + timedelta(days=6, hours=23), now)
        f = chunk_start.strftime("%Y-%m-%dT%H:%M:%S")
        t = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        for st in ("RECEIPT", "PROGRESS", "SUCCESS"):
            q = f"createdAtFrom={f}&createdAtTo={t}&status={st}&maxPerPage=50"
            code, body = _call("GET", epath, q, access, secret)
            if code != 200 or not isinstance(body, dict):
                continue
            for rec in body.get("data") or []:
                c = _norm_exchange(rec)
                if c["product_order_id"] not in ("CPEX-",):
                    claims[c["product_order_id"]] = c
            time.sleep(0.3)
        chunk_start = chunk_end + timedelta(seconds=1)

    result = list(claims.values())
    logger.info(f"[Coupang] 클레임 {len(result)}건 수집 (기간 {days}일)")
    return result
