"""
쿠팡 OPEN API 클라이언트
- HMAC-SHA256 인증
- 발주서 목록 조회 (주문수집)
- 상품준비중 처리 (발주확인)
- 송장업로드 처리 (발송처리)
"""
import os
import time
import hmac
import hashlib
import urllib.parse
import logging
from typing import Optional, List, Dict, Any

import httpx

logger = logging.getLogger(__name__)

# 쿠팡 택배사 코드
DELIVERY_COMPANIES = {
    "CJ대한통운": "CJGLS",
    "로젠택배": "LOGEN",
    "한진택배": "HANJIN",
    "롯데택배": "LOTTE",
    "우체국택배": "EPOST",
    "경동택배": "KDEXP",
    "대신택배": "DAESIN",
    "일양로지스": "ILYANG",
    "합동택배": "HDEXP",
    "건영택배": "KUNYOUNG",
    "업체직접배송(DIRECT)": "DIRECT",
}

# 발주서 상태
ORDER_STATUS_ACCEPT = "ACCEPT"           # 결제완료
ORDER_STATUS_INSTRUCT = "INSTRUCT"       # 상품준비중
ORDER_STATUS_DEPARTURE = "DEPARTURE"     # 배송지시
ORDER_STATUS_DELIVERING = "DELIVERING"   # 배송중
ORDER_STATUS_FINAL = "FINAL_DELIVERY"    # 배송완료


class CoupangClient:
    """쿠팡 OPEN API 클라이언트 (HMAC-SHA256 인증)"""

    def __init__(self, access_key: str, secret_key: str,
                 vendor_id: str = "",
                 base_url: str = "https://api-gateway.coupang.com"):
        self.access_key = access_key
        self.secret_key = secret_key
        self.vendor_id = vendor_id
        self.base_url = base_url.rstrip("/")

    # ─── HMAC-SHA256 서명 생성 ───
    def _generate_signature(self, method: str, path: str, query: str = "") -> dict:
        """HMAC-SHA256 서명 헤더 생성

        Coupang OPEN API 인증 형식:
        Authorization: CEA algorithm=HmacSHA256, access-key={accesskey}, signed-date={datetime}, signature={signature}
        message = datetime + method + path + query
        datetime = yymmddTHHMMSSZ (GMT)
        """
        # GMT 시간으로 datetime 생성
        os.environ['TZ'] = 'GMT+0'
        try:
            time.tzset()
        except AttributeError:
            pass  # Windows에서는 tzset이 없음

        datetime_str = time.strftime('%y%m%d') + 'T' + time.strftime('%H%M%S') + 'Z'

        # 메시지 조합
        message = datetime_str + method.upper() + path + query

        # HMAC-SHA256 서명
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

        authorization = (
            f"CEA algorithm=HmacSHA256, "
            f"access-key={self.access_key}, "
            f"signed-date={datetime_str}, "
            f"signature={signature}"
        )

        return {
            "Authorization": authorization,
            "Content-Type": "application/json;charset=UTF-8",
        }

    # ─── 연결 테스트 ───
    async def test_connection(self) -> dict:
        """API 연결 테스트"""
        if not self.access_key or not self.secret_key:
            return {"ok": False, "error": "쿠팡 API 키가 설정되지 않았습니다."}
        if not self.vendor_id:
            return {"ok": False, "error": "쿠팡 판매자 ID(vendorId)가 설정되지 않았습니다."}

        try:
            # 간단한 주문 조회로 테스트 (오늘~오늘)
            from datetime import datetime, timezone, timedelta
            kst = timezone(timedelta(hours=9))
            today = datetime.now(kst).strftime("%Y-%m-%d")
            path = f"/v2/providers/openapi/apis/api/v5/vendors/{self.vendor_id}/ordersheets"
            query_params = {
                "createdAtFrom": f"{today}+09:00",
                "createdAtTo": f"{today}+09:00",
                "status": ORDER_STATUS_ACCEPT,
                "maxPerPage": "1",
            }
            query_string = urllib.parse.urlencode(query_params)

            headers = self._generate_signature("GET", path, query_string)
            url = f"{self.base_url}{path}?{query_string}"

            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, headers=headers)

            if resp.status_code == 200:
                return {"ok": True}
            elif resp.status_code == 401:
                return {"ok": False, "error": "인증 실패 - API 키를 확인하세요."}
            else:
                return {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ─── 발주서 목록 조회 (주문수집) ───
    async def fetch_orders(
        self,
        status: str = ORDER_STATUS_ACCEPT,
        from_date: str = "",
        to_date: str = "",
        max_per_page: int = 50,
    ) -> List[Dict[str, Any]]:
        """발주서 목록 조회 (일단위 페이징)

        Args:
            status: ACCEPT, INSTRUCT, DEPARTURE, DELIVERING, FINAL_DELIVERY
            from_date: 시작일 (YYYY-MM-DD)
            to_date: 종료일 (YYYY-MM-DD)
            max_per_page: 페이지당 최대 건수 (기본 50)
        """
        if not from_date or not to_date:
            from datetime import datetime, timedelta, timezone
            kst = timezone(timedelta(hours=9))
            now = datetime.now(kst)
            if not to_date:
                to_date = now.strftime("%Y-%m-%d")
            if not from_date:
                from_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")

        path = f"/v2/providers/openapi/apis/api/v5/vendors/{self.vendor_id}/ordersheets"

        all_orders = []
        next_token = ""

        while True:
            query_params = {
                "createdAtFrom": f"{from_date}+09:00",
                "createdAtTo": f"{to_date}+09:00",
                "status": status,
                "maxPerPage": str(max_per_page),
            }
            if next_token:
                query_params["nextToken"] = next_token

            query_string = urllib.parse.urlencode(query_params)
            headers = self._generate_signature("GET", path, query_string)
            url = f"{self.base_url}{path}?{query_string}"

            logger.info(f"[쿠팡] 주문조회 요청: status={status}, from={from_date}, to={to_date}")

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, headers=headers)

            if resp.status_code != 200:
                raise ValueError(f"주문조회 실패 (HTTP {resp.status_code}): {resp.text[:300]}")

            data = resp.json()
            code = data.get("code")
            if code and str(code) != "200" and code != 200:
                raise ValueError(f"주문조회 오류: {data.get('message', str(data))}")

            orders = data.get("data", [])
            if not orders:
                break

            all_orders.extend(orders)

            # nextToken으로 다음 페이지 체크
            next_token = data.get("nextToken", "")
            if not next_token:
                break

        logger.info(f"[쿠팡] 주문조회 결과: {len(all_orders)}건")
        return all_orders

    # ─── 상품준비중 처리 (발주확인) ───
    async def confirm_orders(self, shipment_box_ids: List[int]) -> dict:
        """결제완료 → 상품준비중 상태로 변경

        Args:
            shipment_box_ids: 묶음배송번호 목록 (최대 50개)
        """
        if len(shipment_box_ids) > 50:
            raise ValueError("shipmentBoxIds는 최대 50개까지 요청 가능합니다.")

        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/acknowledgement"
        body = {
            "vendorId": self.vendor_id,
            "shipmentBoxIds": shipment_box_ids,
        }

        headers = self._generate_signature("PUT", path, "")
        url = f"{self.base_url}{path}"

        logger.info(f"[쿠팡] 상품준비중 처리 요청: {len(shipment_box_ids)}건")

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.put(url, headers=headers, json=body)

        if resp.status_code != 200:
            raise ValueError(f"상품준비중 처리 실패 (HTTP {resp.status_code}): {resp.text[:300]}")

        result = resp.json()
        logger.info(f"[쿠팡] 상품준비중 처리 결과: {result}")
        return result

    # ─── 송장업로드 처리 (발송처리) ───
    async def upload_invoice(self, invoices: List[Dict[str, Any]]) -> dict:
        """송장번호를 업로드하여 배송지시 상태로 변경

        Args:
            invoices: 송장 정보 리스트
                [{
                    "shipmentBoxId": 12345,
                    "orderId": 67890,
                    "vendorItemId": 11111,
                    "deliveryCompanyCode": "CJGLS",
                    "invoiceNumber": "1234567890",
                    "splitShipping": False,
                    "preSplitShipped": False,
                    "estimatedShippingDate": ""
                }, ...]
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/orders/invoices"
        body = {
            "vendorId": self.vendor_id,
            "orderSheetInvoiceApplyDtos": invoices,
        }

        headers = self._generate_signature("POST", path, "")
        url = f"{self.base_url}{path}"

        logger.info(f"[쿠팡] 송장업로드 요청: {len(invoices)}건")

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, json=body)

        if resp.status_code != 200:
            raise ValueError(f"송장업로드 실패 (HTTP {resp.status_code}): {resp.text[:300]}")

        result = resp.json()
        logger.info(f"[쿠팡] 송장업로드 결과: {result}")
        return result

    # ─── 일괄 송장업로드 ───
    async def upload_invoices_bulk(
        self,
        shipments: List[Dict[str, Any]],
    ) -> List[dict]:
        """일괄 송장업로드 (개별 처리)

        shipments: [{"shipment_box_id": ..., "order_id": ..., "vendor_item_id": ...,
                     "delivery_company_code": "CJGLS", "invoice_number": "..."}, ...]
        """
        results = []
        for s in shipments:
            try:
                invoice_data = [{
                    "shipmentBoxId": s["shipment_box_id"],
                    "orderId": s["order_id"],
                    "vendorItemId": s["vendor_item_id"],
                    "deliveryCompanyCode": s["delivery_company_code"],
                    "invoiceNumber": s["invoice_number"],
                    "splitShipping": s.get("split_shipping", False),
                    "preSplitShipped": s.get("pre_split_shipped", False),
                    "estimatedShippingDate": s.get("estimated_shipping_date", ""),
                }]
                result = await self.upload_invoice(invoice_data)
                results.append({
                    "shipment_box_id": s["shipment_box_id"],
                    "success": True,
                    "data": result,
                })
            except Exception as e:
                results.append({
                    "shipment_box_id": s["shipment_box_id"],
                    "success": False,
                    "error": str(e),
                })
        return results
