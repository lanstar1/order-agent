"""
지마켓/옥션 ESM Trading API 클라이언트
- JWT(HS256) 인증
- 주문수집 (결제완료/배송준비중 주문 조회)
- 주문확인 (배송준비중으로 상태변경)
- 발송처리 (송장번호 입력)
"""
import time
import json
import base64
import hmac
import hashlib
import logging
from typing import Optional, List, Dict, Any

import httpx

logger = logging.getLogger(__name__)

# 택배사 코드 (ESM 기준)
DELIVERY_COMPANIES = {
    "경동택배": "10016",
    "로젠택배": "10003",
    "CJ대한통운": "10013",
    "한진택배": "10007",
    "롯데택배": "10008",
    "우체국택배": "10001",
}

# 사이트타입
SITE_GMARKET = 2
SITE_AUCTION = 1

# 주문상태
ORDER_STATUS_PAID = 1       # 결제완료
ORDER_STATUS_READY = 2      # 배송준비중
ORDER_STATUS_SHIPPING = 3   # 배송중


class ESMClient:
    """ESM Trading API 클라이언트 (지마켓/옥션)"""

    def __init__(self, master_id: str, secret_key: str,
                 seller_id_g: str = "", seller_id_a: str = "",
                 base_url: str = "https://sa2.esmplus.com/api"):
        self.master_id = master_id
        self.secret_key = secret_key
        self.seller_id_g = seller_id_g   # 지마켓 판매자 ID
        self.seller_id_a = seller_id_a   # 옥션 판매자 ID
        self.base_url = base_url.rstrip("/")
        self._token: Optional[str] = None
        self._token_expires: float = 0

    # ─── JWT 토큰 생성 ───
    def _generate_jwt(self) -> str:
        """HS256 JWT 토큰 생성"""
        now = int(time.time())

        # Header
        header = {
            "alg": "HS256",
            "typ": "JWT",
            "kid": self.master_id,
        }

        # Payload - ssi: 판매자 ID (A:옥션ID, G:지마켓ID)
        ssi_parts = []
        if self.seller_id_a:
            ssi_parts.append(f"A:{self.seller_id_a}")
        if self.seller_id_g:
            ssi_parts.append(f"G:{self.seller_id_g}")

        payload = {
            "iss": "www.esmplus.com",
            "sub": "sell",
            "aud": "sa.esmplus.com",
            "iat": now,
            "ssi": ",".join(ssi_parts),
        }

        # Encode
        def _b64url(data: bytes) -> str:
            return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

        header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
        payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())
        message = f"{header_b64}.{payload_b64}"

        # Signature
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        signature_b64 = _b64url(signature)

        token = f"{message}.{signature_b64}"
        self._token = token
        self._token_expires = now + 3600  # 1시간
        return token

    def _get_token(self) -> str:
        """JWT 토큰 반환 (만료 시 재생성)"""
        now = time.time()
        if self._token and now < self._token_expires - 300:
            return self._token
        return self._generate_jwt()

    def _auth_headers(self) -> dict:
        """인증 헤더"""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # ─── 연결 테스트 ───
    async def test_connection(self) -> dict:
        """API 연결 테스트"""
        if not self.master_id or not self.secret_key:
            return {"ok": False, "error": "ESM API 키가 설정되지 않았습니다."}
        try:
            # 간단한 주문 조회로 테스트
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"{self.base_url}/Order/RequestOrders",
                    headers=self._auth_headers(),
                    json={
                        "siteType": SITE_GMARKET,
                        "orderStatus": ORDER_STATUS_PAID,
                        "fromDate": "2025-01-01",
                        "toDate": "2025-01-01",
                    },
                )
            if resp.status_code == 200:
                return {"ok": True}
            elif resp.status_code == 401:
                return {"ok": False, "error": "인증 실패 - API 키를 확인하세요."}
            else:
                return {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── 주문 조회 ───
    async def fetch_orders(
        self,
        site_type: int = SITE_GMARKET,
        order_status: int = ORDER_STATUS_PAID,
        from_date: str = "",
        to_date: str = "",
    ) -> List[Dict[str, Any]]:
        """주문 목록 조회"""
        if not from_date or not to_date:
            from datetime import datetime, timedelta
            today = datetime.now()
            if not to_date:
                to_date = today.strftime("%Y-%m-%d")
            if not from_date:
                from_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")

        body = {
            "siteType": site_type,
            "orderStatus": order_status,
            "fromDate": from_date,
            "toDate": to_date,
        }

        logger.info(f"[지마켓] 주문조회 요청: {body}")

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.base_url}/Order/RequestOrders",
                headers=self._auth_headers(),
                json=body,
            )

        if resp.status_code != 200:
            raise ValueError(f"주문조회 실패 (HTTP {resp.status_code}): {resp.text[:300]}")

        data = resp.json()
        orders = data if isinstance(data, list) else data.get("data", data.get("orders", []))
        logger.info(f"[지마켓] 주문조회 결과: {len(orders)}건")
        return orders

    # ─── 주문 확인 (결제완료 → 배송준비중) ───
    async def confirm_order(self, order_no: str) -> dict:
        """주문 확인 처리 (배송준비중으로 변경)"""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self.base_url}/Order/OrderCheck/{order_no}",
                headers=self._auth_headers(),
            )

        if resp.status_code != 200:
            raise ValueError(f"주문확인 실패 (HTTP {resp.status_code}): {resp.text[:300]}")

        return resp.json()

    # ─── 발송 처리 ───
    async def ship_order(
        self,
        order_no: str,
        delivery_company_code: str,
        invoice_no: str,
        shipping_date: str = "",
    ) -> dict:
        """발송 처리 (송장번호 입력)"""
        if not shipping_date:
            from datetime import datetime
            shipping_date = datetime.now().strftime("%Y-%m-%d")

        body = {
            "orderNo": order_no,
            "ShippingDate": shipping_date,
            "DeliveryCompanyCode": delivery_company_code,
            "InvoiceNo": invoice_no,
        }

        logger.info(f"[지마켓] 발송처리 요청: {body}")

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self.base_url}/Delivery/ShippingInfo",
                headers=self._auth_headers(),
                json=body,
            )

        if resp.status_code != 200:
            raise ValueError(f"발송처리 실패 (HTTP {resp.status_code}): {resp.text[:300]}")

        return resp.json()

    # ─── 일괄 발송 처리 ───
    async def ship_orders_bulk(
        self,
        shipments: List[Dict[str, str]],
    ) -> List[dict]:
        """일괄 발송 처리
        shipments: [{"order_no": "...", "delivery_company_code": "...", "invoice_no": "..."}, ...]
        """
        results = []
        for s in shipments:
            try:
                result = await self.ship_order(
                    order_no=s["order_no"],
                    delivery_company_code=s["delivery_company_code"],
                    invoice_no=s["invoice_no"],
                    shipping_date=s.get("shipping_date", ""),
                )
                results.append({"order_no": s["order_no"], "success": True, "data": result})
            except Exception as e:
                results.append({"order_no": s["order_no"], "success": False, "error": str(e)})
        return results
