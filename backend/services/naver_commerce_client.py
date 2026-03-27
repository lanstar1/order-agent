"""
네이버 커머스 API 클라이언트
- OAuth2 토큰 발급 (HMAC-BCrypt 전자서명)
- 주문 수집 (발주 미확인 주문 조회)
- 발송 처리 (송장번호 입력)
"""
import logging
import time
import hashlib
import hmac
import base64
import bcrypt
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

NAVER_API_BASE = "https://api.commerce.naver.com/external"


class NaverCommerceClient:
    """네이버 커머스 API 클라이언트"""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._token_expires: float = 0

    # ─── 전자서명 생성 ───
    def _make_signature(self, timestamp: int) -> str:
        """BCrypt 기반 전자서명 생성"""
        password = f"{self.client_id}_{timestamp}"
        hashed = bcrypt.hashpw(
            password.encode("utf-8"),
            self.client_secret.encode("utf-8")
        )
        return base64.b64encode(hashed).decode("utf-8")

    # ─── 토큰 발급 ───
    async def _get_token(self) -> str:
        """OAuth2 토큰 발급 (캐싱, 만료 30분 전 갱신)"""
        now = time.time()
        if self._token and now < self._token_expires - 1800:
            return self._token

        timestamp = int(now * 1000)
        sign = self._make_signature(timestamp)

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{NAVER_API_BASE}/v1/oauth2/token",
                data={
                    "client_id": self.client_id,
                    "timestamp": timestamp,
                    "grant_type": "client_credentials",
                    "client_secret_sign": sign,
                    "type": "SELF",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.error(f"[NaverCommerce] 토큰 발급 실패: {resp.status_code} {resp.text}")
                raise Exception(f"토큰 발급 실패: {resp.status_code}")
            data = resp.json()
            self._token = data["access_token"]
            self._token_expires = now + data.get("expires_in", 10800)
            logger.info("[NaverCommerce] 토큰 발급 성공")
            return self._token

    # ─── 인증 헤더 ───
    async def _headers(self) -> dict:
        token = await self._get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ─── 주문 수집 (발송대기 주문 조회) ───
    async def fetch_new_orders(
        self,
        from_dt: str,
        to_dt: Optional[str] = None,
        page_size: int = 300,
        status: str = "PAYED",
    ) -> list[str]:
        """
        주문 상태별 상품주문번호 조회
        from_dt: ISO 형식 (예: 2026-02-13T00:00:00.000+09:00)
        status: PAYED, DELIVERING, DELIVERED, PURCHASE_DECIDED 등
        """
        headers = await self._headers()

        # 상태에 따라 rangeType 설정
        range_type_map = {
            "PAYED": "PAYED_DATETIME",
            "DELIVERING": "PAYED_DATETIME",
            "DELIVERED": "PAYED_DATETIME",
            "PURCHASE_DECIDED": "PAYED_DATETIME",
        }
        range_type = range_type_map.get(status, "PAYED_DATETIME")

        params = {
            "from": from_dt,
            "rangeType": range_type,
            "productOrderStatuses": status,
            "pageSize": page_size,
            "page": 1,
        }
        if to_dt:
            params["to"] = to_dt

        all_order_ids = []
        async with httpx.AsyncClient() as client:
            while True:
                resp = await client.get(
                    f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
                if resp.status_code != 200:
                    logger.error(f"[NaverCommerce] 주문 조회 실패: {resp.status_code} {resp.text}")
                    raise Exception(f"주문 조회 실패: {resp.status_code}")

                body = resp.json()
                logger.info(f"[NaverCommerce] 주문 조회 응답 키: {list(body.keys()) if isinstance(body, dict) else type(body)}")

                # 응답: {"data": {"count": N, "productOrderIds": [...]}} 또는 {"data": [...]}
                data = body.get("data", {})
                if isinstance(data, dict):
                    items = data.get("productOrderIds", [])
                    total_count = data.get("count", 0)
                    logger.info(f"[NaverCommerce] 주문 조회: count={total_count}, ids={len(items)}건")
                elif isinstance(data, list):
                    items = data
                else:
                    items = []

                all_order_ids.extend(items)

                # 페이징 처리
                if len(items) < page_size:
                    break
                params["page"] = params.get("page", 1) + 1

        logger.info(f"[NaverCommerce] 상품주문번호 총 {len(all_order_ids)}건 수집")
        return all_order_ids

    # ─── 변경 주문 조회 ───
    async def fetch_changed_orders(
        self,
        last_changed_from: str,
        last_changed_to: Optional[str] = None,
        limit_count: int = 300,
    ) -> list[dict]:
        """변경된 상품 주문 내역 조회"""
        headers = await self._headers()
        params = {
            "lastChangedFrom": last_changed_from,
            "limitCount": limit_count,
        }
        if last_changed_to:
            params["lastChangedTo"] = last_changed_to

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders/last-changed-statuses",
                headers=headers,
                params=params,
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"[NaverCommerce] 변경 주문 조회 실패: {resp.status_code}")
                raise Exception(f"변경 주문 조회 실패: {resp.status_code}")
            return resp.json().get("data", [])

    # ─── 발주 확인 ───
    async def confirm_orders(self, product_order_ids: list[str]) -> dict:
        """발주 확인 처리 (최대 30건)"""
        headers = await self._headers()
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders/confirm",
                headers=headers,
                json={"productOrderIds": product_order_ids},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.error(f"[NaverCommerce] 발주 확인 실패: {resp.status_code} {resp.text}")
                raise Exception(f"발주 확인 실패: {resp.status_code}")
            logger.info(f"[NaverCommerce] 발주 확인 {len(product_order_ids)}건 완료")
            return resp.json()

    # ─── 발송 처리 (송장번호 입력) ───
    async def dispatch_orders(
        self,
        dispatches: list[dict],
    ) -> dict:
        """
        발송 처리 (송장번호 입력, 최대 30건)
        dispatches: [{
            "productOrderId": "...",
            "deliveryMethod": "DELIVERY",
            "deliveryCompanyCode": "KGB",  # 로젠택배
            "trackingNumber": "918...",
        }, ...]
        """
        headers = await self._headers()
        payload = {
            "dispatchProductOrders": [
                {
                    "productOrderId": d["productOrderId"],
                    "deliveryMethod": d.get("deliveryMethod", "DELIVERY"),
                    "deliveryCompanyCode": d.get("deliveryCompanyCode", "KGB"),
                    "trackingNumber": d["trackingNumber"],
                }
                for d in dispatches
            ]
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders/dispatch",
                headers=headers,
                json=payload,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.error(f"[NaverCommerce] 발송 처리 실패: {resp.status_code} {resp.text}")
                raise Exception(f"발송 처리 실패: {resp.status_code}")
            logger.info(f"[NaverCommerce] 발송 처리 {len(dispatches)}건 완료")
            return resp.json()

    # ─── 주문번호로 상품주문번호 목록 조회 ───
    async def get_product_order_ids(self, order_id: str) -> list[str]:
        """주문번호로 상품주문번호 목록 조회"""
        headers = await self._headers()
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{NAVER_API_BASE}/v1/pay-order/seller/orders/{order_id}/product-order-ids",
                headers=headers,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.error(f"[NaverCommerce] 상품주문번호 조회 실패: {resp.status_code}")
                raise Exception(f"상품주문번호 조회 실패: {resp.status_code}")
            return resp.json().get("data", [])
