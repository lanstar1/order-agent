"""
네이버 커머스 API 클라이언트
"""
import os
import time
import base64
import logging
import httpx
from typing import Optional
from datetime import datetime, timedelta, timezone

try:
    import bcrypt
except ImportError:
    bcrypt = None
    logging.getLogger(__name__).error(
        "[Naver] bcrypt 미설치! pip install bcrypt --break-system-packages"
    )

from config import NAVER_CLIENT_ID, NAVER_CLIENT_SECRET, NAVER_COMMERCE_URL

logger = logging.getLogger(__name__)


def _credentials() -> tuple[str, str]:
    """커머스 API 자격증명.

    설정 페이지에서 저장하면 NAVER_COMMERCE_CLIENT_ID/SECRET 환경변수로
    런타임 주입되므로 호출 시점에 읽는다. 미설정 시 구 변수(NAVER_CLIENT_ID,
    NAS .env 호환)로 폴백.
    """
    cid = os.getenv("NAVER_COMMERCE_CLIENT_ID", "") or NAVER_CLIENT_ID
    secret = os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "") or NAVER_CLIENT_SECRET
    return cid, secret


class NaverCommerceClient:
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expires: float = 0
        self._token_client_id: str = ""

    async def get_token(self) -> str:
        client_id, client_secret = _credentials()
        if self._token and time.time() < self._token_expires and self._token_client_id == client_id:
            return self._token

        if not client_id or not client_secret:
            raise RuntimeError("네이버 커머스 API 키 미설정 (설정 → API 키 → 네이버 커머스 Client ID/Secret)")

        if bcrypt is None:
            raise RuntimeError("bcrypt 패키지 미설치. pip install bcrypt")

        timestamp = str(int(time.time() * 1000))
        pwd = f"{client_id}_{timestamp}"
        hashed = bcrypt.hashpw(pwd.encode('utf-8'), client_secret.encode('utf-8'))
        sign = base64.b64encode(hashed).decode('utf-8')

        url = f"{NAVER_COMMERCE_URL}/external/v1/oauth2/token"
        params = {
            "client_id": client_id,
            "timestamp": timestamp,
            "client_secret_sign": sign,
            "grant_type": "client_credentials",
            "type": "SELF",
        }

        async with httpx.AsyncClient() as client:
            r = await client.post(
                url, data=params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            body = r.json()
            logger.info(f"[Naver] 토큰 응답: status={r.status_code}")

            if r.status_code != 200:
                error_msg = body.get("error_description") or body.get("message") or str(body)
                raise RuntimeError(f"토큰 발급 실패 ({r.status_code}): {error_msg}")

        self._token = body.get("access_token")
        if not self._token:
            raise RuntimeError(f"access_token 없음: {body}")

        expires_in = body.get("expires_in", 1800)
        self._token_expires = time.time() + expires_in - 300
        self._token_client_id = client_id
        logger.info(f"[Naver] 토큰 발급 완료 (expires_in={expires_in}s)")
        return self._token

    async def _headers(self) -> dict:
        token = await self.get_token()
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async def fetch_orders(
        self,
        date_from: str = None,
        date_to: str = None,
        order_type: str = "NEW_BEFORE",
    ) -> list[dict]:
        """
        주문 수집.
        order_type:
          - NEW_BEFORE : 신규주문 (발주 전)
          - NEW_AFTER  : 신규주문 (발주 후)
          - DELIVERING : 배송중 (발송처리 완료)
        """
        headers = await self._headers()
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST)

        if date_from:
            from_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=KST)
        else:
            from_dt = now - timedelta(days=3)

        if date_to:
            to_dt = datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=KST)
        else:
            to_dt = now

        if order_type in ("NEW_BEFORE", "NEW_AFTER"):
            last_changed_type = "PAYED"
        elif order_type == "DELIVERING":
            last_changed_type = "DISPATCHED"
        else:
            last_changed_type = "PAYED"

        url = f"{NAVER_COMMERCE_URL}/external/v1/pay-order/seller/product-orders/last-changed-statuses"

        # 24시간 단위로 분할 (API 제한)
        time_chunks = []
        chunk_start = from_dt
        while chunk_start < to_dt:
            chunk_end = min(chunk_start + timedelta(hours=23, minutes=59, seconds=59), to_dt)
            time_chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(seconds=1)

        logger.info(f"[Naver] 주문수집 type={order_type}, 기간={from_dt.date()}~{to_dt.date()}, {len(time_chunks)}개 구간")

        all_orders = []
        try:
            product_order_ids = []
            for c_from, c_to in time_chunks:
                params = {
                    "lastChangedFrom": c_from.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"),
                    "lastChangedTo": c_to.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"),
                    "lastChangedType": last_changed_type,
                }
                async with httpx.AsyncClient(timeout=30) as client:
                    r = await client.get(url, headers=headers, params=params)
                    r.raise_for_status()
                    body = r.json()

                last_changed = body.get("data", {}).get("lastChangeStatuses", [])
                chunk_ids = [i["productOrderId"] for i in last_changed if i.get("productOrderId")]
                logger.info(f"[Naver] 구간 {c_from.date()} ~ {c_to.date()}: {len(chunk_ids)}건")
                product_order_ids.extend(chunk_ids)

            product_order_ids = list(dict.fromkeys(product_order_ids))

            if not product_order_ids:
                logger.info("[Naver] 해당 조건 주문 없음")
                return []

            logger.info(f"[Naver] {last_changed_type} 주문 총 {len(product_order_ids)}건 발견")

            for i in range(0, len(product_order_ids), 50):
                batch = product_order_ids[i:i+50]
                detail_url = f"{NAVER_COMMERCE_URL}/external/v1/pay-order/seller/product-orders/query"
                async with httpx.AsyncClient(timeout=30) as client:
                    r2 = await client.post(detail_url, headers=headers, json={"productOrderIds": batch})
                    if r2.status_code == 200:
                        data = r2.json().get("data", [])
                        if data and not all_orders:
                            first = data[0]
                            logger.info(f"[Naver] 응답 첫 건 최상위 키: {list(first.keys())}")
                        for po in data:
                            all_orders.append(po)

            if order_type in ("NEW_BEFORE", "NEW_AFTER"):
                filtered = []
                for o in all_orders:
                    po_obj = o.get("productOrder", o)
                    place_status = po_obj.get("placeOrderStatus", "")
                    if order_type == "NEW_BEFORE" and place_status != "OK":
                        filtered.append(o)
                    elif order_type == "NEW_AFTER" and place_status == "OK":
                        filtered.append(o)
                logger.info(f"[Naver] {order_type} 필터링: {len(all_orders)}건 → {len(filtered)}건")
                return filtered

            logger.info(f"[Naver] 주문 상세 {len(all_orders)}건 수집 완료")
            return all_orders
        except Exception as e:
            logger.error(f"[Naver] 주문 수집 오류: {e}", exc_info=True)
            return []

    async def confirm_orders(self, product_order_ids: list[str]) -> dict:
        if not product_order_ids:
            return {"success": True, "confirmed": 0, "failed": 0}

        headers = await self._headers()
        url = f"{NAVER_COMMERCE_URL}/external/v1/pay-order/seller/product-orders/confirm"
        results = {"success": True, "confirmed": 0, "failed": 0, "errors": []}

        for i in range(0, len(product_order_ids), 30):
            batch = product_order_ids[i:i+30]
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    r = await client.post(url, headers=headers, json={"productOrderIds": batch})
                    body = r.json()
                if r.status_code == 200:
                    results["confirmed"] += len(body.get("data", {}).get("successProductOrderIds", []))
                    fails = body.get("data", {}).get("failProductOrderInfos", [])
                    results["failed"] += len(fails)
                    results["errors"].extend(fails)
                else:
                    results["failed"] += len(batch)
            except Exception as e:
                logger.error(f"[Naver] 발주확인 오류: {e}")
                results["failed"] += len(batch)

        results["success"] = results["failed"] == 0
        logger.info(f"[Naver] 발주확인: 성공={results['confirmed']}, 실패={results['failed']}")
        return results

    async def dispatch_orders(self, dispatch_list: list[dict]) -> dict:
        if not dispatch_list:
            return {"success": True, "dispatched": 0, "failed": 0}

        headers = await self._headers()
        url = f"{NAVER_COMMERCE_URL}/external/v1/pay-order/seller/product-orders/ship"
        results = {"success": True, "dispatched": 0, "failed": 0, "errors": []}

        for i in range(0, len(dispatch_list), 30):
            batch = dispatch_list[i:i+30]
            items = [{
                "productOrderId": it["productOrderId"],
                "deliveryMethod": "DELIVERY",
                "deliveryCompanyCode": it.get("deliveryCompanyCode", "LOGEN"),
                "trackingNumber": it["trackingNumber"],
            } for it in batch]

            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    r = await client.post(url, headers=headers, json={"dispatchProductOrders": items})
                    body = r.json()
                if r.status_code == 200:
                    results["dispatched"] += len(body.get("data", {}).get("successProductOrderIds", []))
                    fails = body.get("data", {}).get("failProductOrderInfos", [])
                    results["failed"] += len(fails)
                    results["errors"].extend(fails)
                else:
                    results["failed"] += len(batch)
            except Exception as e:
                logger.error(f"[Naver] 발송처리 오류: {e}")
                results["failed"] += len(batch)

        results["success"] = results["failed"] == 0
        logger.info(f"[Naver] 발송처리: 성공={results['dispatched']}, 실패={results['failed']}")
        return results

    async def fetch_claims(self, days: int = 7) -> list[dict]:
        """반품/교환 클레임 수집 (lastChangedType=CLAIM_REQUESTED 기준).

        취소(CANCEL) 등은 제외하고 RETURN/EXCHANGE만 정규화하여 반환한다.
        """
        headers = await self._headers()
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST)
        from_dt = now - timedelta(days=days)

        url = f"{NAVER_COMMERCE_URL}/external/v1/pay-order/seller/product-orders/last-changed-statuses"

        # 24시간 단위로 분할 (API 제한)
        time_chunks = []
        chunk_start = from_dt
        while chunk_start < now:
            chunk_end = min(chunk_start + timedelta(hours=23, minutes=59, seconds=59), now)
            time_chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(seconds=1)

        import asyncio

        async def request_throttled(method, req_url, **kwargs):
            """레이트리밋 대응 — 호출 간 0.4초 간격 + 429 시 지수 백오프 재시도"""
            await asyncio.sleep(0.4)
            for attempt in range(1, 5):
                async with httpx.AsyncClient(timeout=30) as client:
                    r = await client.request(method, req_url, headers=headers, **kwargs)
                if r.status_code != 429:
                    return r
                wait = 1.5 * attempt
                logger.warning(f"[Naver] 429 레이트리밋 — {wait:.1f}초 대기 후 재시도 ({attempt}/4)")
                await asyncio.sleep(wait)
            return r

        try:
            product_order_ids = []
            for c_from, c_to in time_chunks:
                params = {
                    "lastChangedFrom": c_from.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"),
                    "lastChangedTo": c_to.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"),
                    "lastChangedType": "CLAIM_REQUESTED",
                }
                # 구간당 300건 초과 시 more 페이징
                for _ in range(5):
                    r = await request_throttled("GET", url, params=params)
                    r.raise_for_status()
                    data = r.json().get("data", {}) or {}
                    items = data.get("lastChangeStatuses", []) or []
                    product_order_ids.extend(i["productOrderId"] for i in items if i.get("productOrderId"))
                    more = data.get("more") or {}
                    if not more.get("moreSequence"):
                        break
                    params = {**params, "lastChangedFrom": more.get("moreFrom"), "moreSequence": more["moreSequence"]}

            product_order_ids = list(dict.fromkeys(product_order_ids))
            if not product_order_ids:
                logger.info(f"[Naver] 클레임 없음 (기간 {days}일)")
                return []

            claims = []
            detail_url = f"{NAVER_COMMERCE_URL}/external/v1/pay-order/seller/product-orders/query"
            for i in range(0, len(product_order_ids), 50):
                batch = product_order_ids[i:i+50]
                r2 = await request_throttled("POST", detail_url, json={"productOrderIds": batch})
                if r2.status_code != 200:
                    logger.warning(f"[Naver] 클레임 상세 조회 실패 ({r2.status_code}): {r2.text[:200]}")
                    continue
                for item in r2.json().get("data", []) or []:
                    claim = self._normalize_claim(item)
                    if claim:
                        claims.append(claim)

            logger.info(f"[Naver] 반품/교환 클레임 {len(claims)}건 수집 (기간 {days}일, 전체 클레임 {len(product_order_ids)}건)")
            return claims
        except Exception as e:
            logger.error(f"[Naver] 클레임 수집 오류: {e}", exc_info=True)
            raise

    @staticmethod
    def _normalize_claim(item: dict) -> Optional[dict]:
        """product-orders/query 응답 1건 → CS 접수용 클레임 dict (반품/교환 외 None)"""
        po = item.get("productOrder", {}) or {}
        order = item.get("order", {}) or {}
        claim_type = po.get("claimType", "")
        if claim_type not in ("RETURN", "EXCHANGE"):
            return None
        detail = (item.get("return") if claim_type == "RETURN" else item.get("exchange")) or {}
        addr = po.get("shippingAddress", {}) or {}
        return {
            "product_order_id": po.get("productOrderId") or "",
            "order_id": order.get("orderId") or "",
            "claim_type": "반품" if claim_type == "RETURN" else "교환",
            "claim_status": detail.get("claimStatus") or po.get("claimStatus") or "",
            "customer_name": order.get("ordererName") or addr.get("name") or "",
            "contact_info": order.get("ordererTel") or addr.get("tel1") or addr.get("tel2") or "",
            "product_name": po.get("productName") or "",
            "product_option": po.get("productOption") or "",
            "quantity": po.get("quantity") or 1,
            "reason_code": detail.get("returnReason") or detail.get("exchangeReason") or "",
            "detailed_reason": detail.get("returnDetailedReason") or detail.get("exchangeDetailedReason") or "",
            "claim_request_date": detail.get("claimRequestDate") or "",
            "collect_courier": detail.get("collectDeliveryCompany") or "",
            "collect_tracking_no": detail.get("collectTrackingNumber") or "",
            # 반품 배송비 처리(이미 한글: '환불금에서 차감'/'판매자에게 직접 송금' 등). 청구 없으면 빈 값.
            "shipping_cost_status": detail.get("claimDeliveryFeePayMethod") or "",
        }


naver_client = NaverCommerceClient()
