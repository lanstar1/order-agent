"""ECOUNT ERP OAPI 클라이언트 (리베이트 전용)

Zone API → Login API → SaveSale API 3단계 인증 패턴.
동기 httpx 클라이언트 사용 (리베이트 전표 순차 처리용).
"""
import time
import logging
import httpx

logger = logging.getLogger(__name__)


class RebateERPClient:
    """이카운트 ERP Open API V2 클라이언트 (리베이트 전용)."""

    ZONE_URL = "https://oapi.ecount.com/OAPI/V2/Zone"
    LOGIN_URL_TPL = "https://oapi{zone}.ecount.com/OAPI/V2/OAPILogin"
    SAVE_SALE_URL_TPL = "https://oapi{zone}.ecount.com/OAPI/V2/Sale/SaveSale"

    def __init__(self, com_code: str, user_id: str, api_key: str, zone: str = ""):
        self.com_code = com_code
        self.user_id = user_id
        self.api_key = api_key
        self.zone = zone
        self.session_id = ""
        self._session_expires = 0
        self._client = httpx.Client(timeout=30)

    def _ensure_zone(self):
        if self.zone:
            return
        resp = self._client.post(self.ZONE_URL, json={"COM_CODE": self.com_code})
        data = resp.json()
        if data.get("Status") == "200":
            result = data.get("Data", {}).get("ZONE", "")
            if result:
                self.zone = result
                logger.info(f"ERP Zone 확인: {self.zone}")
                return
        raise ConnectionError(f"ERP Zone API 실패: {data}")

    def _login(self):
        self._ensure_zone()
        url = self.LOGIN_URL_TPL.format(zone=self.zone)
        resp = self._client.post(url, json={
            "COM_CODE": self.com_code,
            "USER_ID": self.user_id,
            "API_CERT_KEY": self.api_key,
            "LAN_TYPE": "ko-KR",
            "ZONE": self.zone,
        })
        data = resp.json()
        if data.get("Status") == "200":
            result = data.get("Data", {})
            self.session_id = result.get("Datas", {}).get("SESSION_ID", "")
            self._session_expires = time.time() + 3000
            logger.info("ERP 로그인 성공 (리베이트)")
            return
        raise ConnectionError(f"ERP Login API 실패: {data}")

    def _ensure_session(self):
        if not self.session_id or time.time() > self._session_expires:
            self._login()

    def save_sale(self, sale_data: dict) -> dict:
        self._ensure_session()
        url = f"{self.SAVE_SALE_URL_TPL.format(zone=self.zone)}?SESSION_ID={self.session_id}"
        resp = self._client.post(url, json=sale_data)
        data = resp.json()
        if data.get("Status") == "401":
            logger.warning("ERP 세션 만료, 재로그인 시도")
            self._login()
            url = f"{self.SAVE_SALE_URL_TPL.format(zone=self.zone)}?SESSION_ID={self.session_id}"
            resp = self._client.post(url, json=sale_data)
            data = resp.json()
        return data

    def create_rebate_slip(self, io_date, customer_code, customer_name, rebate_amount,
                           emp_cd="", wh_cd="10", prod_cd="판매장려금할인006",
                           prod_des="리베이트", remarks="", io_type="1Z") -> dict:
        price = -abs(rebate_amount)
        bulk_data = {
            "UPLOAD_SER_NO": "1",
            "IO_DATE": io_date,
            "IO_TYPE": io_type,
            "CUST": customer_code,
            "CUST_DES": customer_name,
            "WH_CD": wh_cd,
            "PROD_CD": prod_cd,
            "PROD_DES": prod_des,
            "QTY": "1",
            "PRICE": str(price),
            "SUPPLY_AMT": str(price),
            "VAT_AMT": "0",
            "REMARKS": remarks,
        }
        if emp_cd:
            bulk_data["EMP_CD"] = emp_cd
        sale_data = {"SaleList": [{"BulkDatas": bulk_data}]}
        return self.save_sale(sale_data)

    def close(self):
        self._client.close()
