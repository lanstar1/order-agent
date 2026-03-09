"""
ECOUNT ERP Open API V2 클라이언트
Zone → Login → SaveSale 순서로 호출

올바른 API 엔드포인트:
  Zone:     POST https://oapi.ecount.com/OAPI/V2/Zone
  Login:    POST https://oapi{ZONE}.ecount.com/OAPI/V2/OAPILogin
  SaveSale: POST https://oapi{ZONE}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}
"""
import httpx
import asyncio
import logging
from typing import Optional
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ERP_COM_CODE, ERP_USER_ID, ERP_ZONE, ERP_API_KEY, ERP_WH_CD, ERP_EMP_CD

logger = logging.getLogger(__name__)


class ERPClient:
    """ECOUNT ERP API 클라이언트 (세션 캐싱 포함)"""

    def __init__(self):
        self._session_id: Optional[str] = None
        self._zone:       Optional[str] = None   # 예: "CD", "A", "B" ...

    # ─────────────────────────────────────────
    #  Step 1: Zone API
    #  POST https://oapi.ecount.com/OAPI/V2/Zone
    #  Body: {"COM_CODE": "..."}
    # ─────────────────────────────────────────
    async def _get_zone(self, client: httpx.AsyncClient) -> str:
        url = "https://oapi.ecount.com/OAPI/V2/Zone"
        payload = {"COM_CODE": ERP_COM_CODE}

        r = await client.post(url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        logger.debug(f"[ERP Zone] 응답: {data}")

        # Status 필드 확인 ("200" 이어야 성공)
        if str(data.get("Status", "")) != "200":
            raise RuntimeError(f"Zone API 실패: {data}")

        zone = data.get("Data", {}).get("ZONE") or ERP_ZONE
        self._zone = zone
        logger.info(f"[ERP Zone] 획득: {zone}")
        return zone

    # ─────────────────────────────────────────
    #  Step 2: Login API
    #  POST https://oapi{ZONE}.ecount.com/OAPI/V2/OAPILogin
    # ─────────────────────────────────────────
    async def _login(self, client: httpx.AsyncClient) -> str:
        zone = (self._zone or ERP_ZONE).lower()   # 소문자로 (예: CD → cd)
        url = f"https://oapi{zone}.ecount.com/OAPI/V2/OAPILogin"
        payload = {
            "COM_CODE":     ERP_COM_CODE,
            "USER_ID":      ERP_USER_ID,
            "API_CERT_KEY": ERP_API_KEY,
            "LAN_TYPE":     "ko-KR",
            "ZONE":         (self._zone or ERP_ZONE).upper(),
        }

        r = await client.post(url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        logger.debug(f"[ERP Login] 응답: {data}")

        if str(data.get("Status", "")) != "200":
            raise RuntimeError(f"Login API 실패: {data}")

        # SESSION_ID 위치: Data → Datas → SESSION_ID
        try:
            self._session_id = data["Data"]["Datas"]["SESSION_ID"]
        except (KeyError, TypeError):
            # 일부 버전은 Data 바로 아래 있을 수도 있음
            self._session_id = (
                data.get("Data", {}).get("Datas", {}).get("SESSION_ID")
                or data.get("Data", {}).get("SESSION_ID")
            )
        if not self._session_id:
            raise RuntimeError(f"SESSION_ID를 찾을 수 없습니다. 응답: {data}")

        logger.info(f"[ERP Login] 세션 획득 완료")
        return self._session_id

    # ─────────────────────────────────────────
    #  공개 메서드: 세션 확보 (Zone + Login)
    # ─────────────────────────────────────────
    async def ensure_session(self) -> str:
        async with httpx.AsyncClient() as client:
            await self._get_zone(client)
            await self._login(client)
        return self._session_id

    # ─────────────────────────────────────────
    #  GetBasicProductsList: 품목 출고단가 조회
    #  POST https://oapi{ZONE}.ecount.com/OAPI/V2/InventoryBasic/GetBasicProductsList
    # ─────────────────────────────────────────
    async def get_product_prices(self, prod_cds: list) -> dict:
        """
        ECOUNT GetBasicProductsList API로 품목 출고단가(OUT_PRICE) 조회
        Returns: {prod_cd: float}
        """
        if not prod_cds:
            return {}

        if not self._session_id:
            await self.ensure_session()

        zone = (self._zone or ERP_ZONE).lower()
        url = (
            f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBasic/GetBasicProductsList"
            f"?SESSION_ID={self._session_id}"
        )

        # 여러 품목코드를 ∬ (\u222c) 구분자로 결합
        prod_cd_str = "\u222c".join(prod_cds)
        payload = {"PROD_CD": prod_cd_str}

        async def _fetch(u, p):
            async with httpx.AsyncClient() as client:
                r = await client.post(u, json=p, timeout=15)
                r.raise_for_status()
                return r.json()

        try:
            data = await _fetch(url, payload)
            logger.info(
                f"[ERP GetBasicProductsList] Status={data.get('Status')}, "
                f"조회품목수={len(prod_cds)}"
            )
            logger.debug(f"[ERP GetBasicProductsList] 응답: {data}")

            # 세션 만료 시 재로그인 후 1회 재시도
            if str(data.get("Status", "")) in ("301", "302"):
                logger.warning("[ERP] 세션 만료 - 재로그인 후 단가 재조회")
                await self.ensure_session()
                zone = (self._zone or ERP_ZONE).lower()
                url = (
                    f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBasic/GetBasicProductsList"
                    f"?SESSION_ID={self._session_id}"
                )
                data = await _fetch(url, payload)

            if str(data.get("Status", "")) != "200":
                logger.warning(f"[ERP GetBasicProductsList] 실패: {data}")
                return {}

            # Data.Result 배열에서 OUT_PRICE 추출
            result_list = data.get("Data", {}).get("Result", [])
            prices = {}
            for item in result_list:
                cd = str(item.get("PROD_CD", "") or "").strip()
                raw_price = str(item.get("OUT_PRICE", "0") or "0").replace(",", "")
                try:
                    price_val = float(raw_price)
                except (ValueError, TypeError):
                    price_val = 0.0
                if cd:
                    prices[cd] = price_val

            logger.info(f"[ERP GetBasicProductsList] {len(prices)}개 품목 단가 반환")
            return prices

        except Exception as e:
            logger.error(f"[ERP GetBasicProductsList] 오류: {e}", exc_info=True)
            return {}

    # ─────────────────────────────────────────
    #  재고조회: ViewInventoryBalanceStatusByLocation
    #  POST https://oapi{ZONE}.ecount.com/OAPI/V2/InventoryBalance/ViewInventoryBalanceStatusByLocation
    # ─────────────────────────────────────────
    async def check_inventory(
        self,
        prod_cd: str,
        wh_cd: str = "",
        base_date: str = "",
    ) -> dict:
        """
        ECOUNT 창고별재고현황 API로 특정 품목의 재고 조회
        Args:
            prod_cd: 품목코드
            wh_cd: 창고코드 (빈 값이면 전체 창고)
            base_date: 조회 기준일 YYYYMMDD (빈 값이면 오늘)
        Returns: {"success": bool, "data": [...], "total_qty": float, "error": ...}
        """
        if not prod_cd:
            return {"success": False, "error": "품목코드가 필요합니다.", "data": [], "total_qty": 0}

        if not self._session_id:
            await self.ensure_session()

        # 기준일 기본값: 오늘
        if not base_date:
            from datetime import datetime
            base_date = datetime.now().strftime("%Y%m%d")

        zone = (self._zone or ERP_ZONE).lower()
        url = (
            f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/"
            f"ViewInventoryBalanceStatusByLocation?SESSION_ID={self._session_id}"
        )

        payload = {
            "PROD_CD": prod_cd,
            "WH_CD": wh_cd,
            "BASE_DATE": base_date,
        }

        async def _fetch(u, p):
            async with httpx.AsyncClient() as c:
                r = await c.post(u, json=p, timeout=15)
                r.raise_for_status()
                return r.json()

        try:
            data = await _fetch(url, payload)
            logger.info(
                f"[ERP Inventory] Status={data.get('Status')}, PROD_CD={prod_cd}"
            )

            # 세션 만료 시 재로그인 후 1회 재시도
            if str(data.get("Status", "")) in ("301", "302"):
                logger.warning("[ERP] 세션 만료 - 재로그인 후 재고 재조회")
                await self.ensure_session()
                zone = (self._zone or ERP_ZONE).lower()
                url = (
                    f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/"
                    f"ViewInventoryBalanceStatusByLocation?SESSION_ID={self._session_id}"
                )
                data = await _fetch(url, payload)

            if str(data.get("Status", "")) != "200":
                err_msg = "재고조회 실패"
                error_obj = data.get("Error") or {}
                if isinstance(error_obj, dict) and error_obj.get("Message"):
                    err_msg = error_obj["Message"]
                elif data.get("Errors"):
                    errors = data["Errors"]
                    if isinstance(errors, list) and errors:
                        err_msg = errors[0].get("Message", err_msg)
                logger.warning(f"[ERP Inventory] 실패: {data}")
                return {"success": False, "error": err_msg, "data": [], "total_qty": 0}

            result_list = data.get("Data", {}).get("Result", [])
            total_cnt = data.get("Data", {}).get("TotalCnt", 0)

            # 결과 정리
            items = []
            total_qty = 0.0
            for item in result_list:
                qty_raw = str(item.get("BAL_QTY", "0") or "0")
                try:
                    qty = float(qty_raw)
                except (ValueError, TypeError):
                    qty = 0.0
                total_qty += qty
                items.append({
                    "wh_cd": item.get("WH_CD", ""),
                    "wh_name": item.get("WH_DES", ""),
                    "prod_cd": item.get("PROD_CD", ""),
                    "prod_name": item.get("PROD_DES", ""),
                    "prod_size": item.get("PROD_SIZE_DES", ""),
                    "qty": qty,
                })

            logger.info(f"[ERP Inventory] {len(items)}개 창고, 총 재고: {total_qty}")
            return {
                "success": True,
                "data": items,
                "total_qty": total_qty,
                "total_cnt": total_cnt,
                "base_date": base_date,
            }

        except Exception as e:
            logger.error(f"[ERP Inventory] 오류: {e}", exc_info=True)
            return {"success": False, "error": str(e), "data": [], "total_qty": 0}

    # ─────────────────────────────────────────
    #  거래처 목록 조회: GetListCustomerBySearch
    #  POST https://oapi{ZONE}.ecount.com/OAPI/V2/AccountBasic/GetListCustomerBySearch
    # ─────────────────────────────────────────
    async def get_customer_list(self, page: int = 1, per_page: int = 500) -> dict:
        """
        ECOUNT 거래처 목록 조회
        Returns: {"success": bool, "customers": [{"cust_code": ..., "cust_name": ...}], "total": int}
        """
        if not self._session_id:
            await self.ensure_session()

        zone = (self._zone or ERP_ZONE).lower()
        url = (
            f"https://oapi{zone}.ecount.com/OAPI/V2/AccountBasic/GetListCustomerBySearch"
            f"?SESSION_ID={self._session_id}"
        )

        payload = {
            "PAGE_NO": str(page),
            "PER_PAGE_CNT": str(per_page),
        }

        async def _fetch(u, p):
            async with httpx.AsyncClient() as c:
                r = await c.post(u, json=p, timeout=30)
                r.raise_for_status()
                return r.json()

        try:
            data = await _fetch(url, payload)
            logger.info(f"[ERP CustomerList] Status={data.get('Status')}, Page={page}")

            # 세션 만료 시 재로그인 후 재시도
            if str(data.get("Status", "")) in ("301", "302"):
                logger.warning("[ERP] 세션 만료 - 재로그인 후 거래처 재조회")
                await self.ensure_session()
                zone = (self._zone or ERP_ZONE).lower()
                url = (
                    f"https://oapi{zone}.ecount.com/OAPI/V2/AccountBasic/GetListCustomerBySearch"
                    f"?SESSION_ID={self._session_id}"
                )
                data = await _fetch(url, payload)

            if str(data.get("Status", "")) != "200":
                logger.warning(f"[ERP CustomerList] 실패: {data}")
                return {"success": False, "customers": [], "total": 0, "error": str(data)}

            result_list = data.get("Data", {}).get("Result", [])
            total_cnt = data.get("Data", {}).get("TotalCnt", 0)

            customers = []
            for item in result_list:
                cust_code = str(item.get("CUST_CD", "") or item.get("CUST", "") or "").strip()
                cust_name = str(item.get("CUST_DES", "") or item.get("CUST_NAME", "") or "").strip()
                if cust_code and cust_name:
                    customers.append({
                        "cust_code": cust_code,
                        "cust_name": cust_name,
                    })

            logger.info(f"[ERP CustomerList] {len(customers)}개 거래처 반환 (총 {total_cnt})")
            return {"success": True, "customers": customers, "total": total_cnt}

        except Exception as e:
            logger.error(f"[ERP CustomerList] 오류: {e}", exc_info=True)
            return {"success": False, "customers": [], "total": 0, "error": str(e)}

    # ─────────────────────────────────────────
    #  Step 3: SaveSale (판매 전표 저장)
    #  POST https://oapi{ZONE}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}
    #  Body: {"SaleList": {"BulkDatas": [...]}}
    # ─────────────────────────────────────────
    async def save_sale(
        self,
        cust_code:  str,
        lines:      list,    # [{"prod_cd": str, "qty": float, "unit": str, "price": float}]
        upload_ser: str = "1",
        wh_cd:      str = "",
        emp_cd:     str = "",   # 로그인한 담당자 코드
    ) -> dict:
        """
        판매 전표를 ERP에 저장합니다.
        lines 예시: [{"prod_cd": "A001", "qty": 10, "unit": "EA"}]
        """
        if not self._session_id:
            await self.ensure_session()

        wh = wh_cd or ERP_WH_CD
        zone = (self._zone or ERP_ZONE).lower()

        # SESSION_ID는 URL 쿼리 파라미터로 전달
        url = f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={self._session_id}"

        # UPLOAD_SER_NO: ECOUNT SMALLINT(4,0) → 최대 4자리 정수 문자열
        ser_no = str(int(upload_ser))[-4:] if upload_ser else "1"

        # SaleList: 각 라인 = {"BulkDatas": {...}} 하나씩, 같은 UPLOAD_SER_NO로 묶어서 한 전표 처리
        sale_list = []
        for line in lines:
            # QTY: 정수면 정수로, 소수이면 소수점 유지 (ECOUNT는 문자열로 전송)
            qty_val = line["qty"]
            if qty_val is not None:
                qty_val = float(qty_val)
                qty_str = str(int(qty_val)) if qty_val == int(qty_val) else str(qty_val)
            else:
                qty_str = "0"

            bulk = {
                "UPLOAD_SER_NO": ser_no,
                "CUST":          cust_code,
                "PROD_CD":       line["prod_cd"],
                "QTY":           qty_str,
                "WH_CD":         wh,
            }
            # 담당자 코드: 로그인한 사용자 우선, 없으면 .env 기본값
            effective_emp = emp_cd or ERP_EMP_CD
            if effective_emp:
                bulk["EMP_CD"] = effective_emp
            # 단위 (있을 때만)
            unit = line.get("unit", "")
            if unit:
                bulk["UNIT"] = unit
            # 단가 및 공급가 (price > 0 일 때만)
            price_val = line.get("price") or 0
            try:
                price_val = float(price_val)
            except (ValueError, TypeError):
                price_val = 0.0
            if price_val > 0:
                qty_f = float(qty_val) if qty_val is not None else 0.0
                supply = round(price_val * qty_f, 2)
                # 정수면 정수 문자열로
                bulk["PRICE"]       = str(int(price_val)) if price_val == int(price_val) else str(price_val)
                bulk["SUPPLY_AMT"]  = str(int(supply))    if supply    == int(supply)    else str(supply)

            sale_list.append({"BulkDatas": bulk})

        payload = {
            "SaleList": sale_list
        }

        logger.info(f"[ERP SaveSale] URL: {url}")
        logger.debug(f"[ERP SaveSale] 페이로드: {payload}")

        # 재시도 로직 (세션 만료 시 재로그인)
        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=15)
                    r.raise_for_status()
                    data = r.json()

                logger.info(f"[ERP SaveSale] 응답: {data}")

                if str(data.get("Status", "")) == "200":
                    return {"success": True, "data": data}

                # 세션 만료 코드 (301, 302 등)
                if str(data.get("Status", "")) in ("301", "302"):
                    logger.warning("[ERP] 세션 만료, 재로그인 시도")
                    await self.ensure_session()
                    zone = (self._zone or ERP_ZONE).lower()
                    url = f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={self._session_id}"
                    continue

                err = data.get("Error") or data.get("error") or data
                logger.error(f"[ERP SaveSale] 실패 응답: {data}")
                return {"success": False, "error": err}

            except httpx.HTTPStatusError as e:
                logger.error(f"[ERP] HTTP 상태 오류 (시도 {attempt+1}): {e.response.status_code} {e.response.text}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
            except httpx.HTTPError as e:
                logger.error(f"[ERP] HTTP 오류 (시도 {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

        return {"success": False, "error": "최대 재시도 횟수 초과"}

    # ─────────────────────────────────────────
    #  Step 4: SaveQuotation (견적서 저장)
    #  POST https://oapi{ZONE}.ecount.com/OAPI/V2/Quotation/SaveQuotation?SESSION_ID={session_id}
    #  Body: {"QuotationList": [{"BulkDatas": {...}}]}
    # ─────────────────────────────────────────
    async def save_quotation(
        self,
        cust_code:  str,
        lines:      list,    # [{"prod_cd": str, "qty": float, "unit": str, "price": float}]
        upload_ser: str = "1",
        wh_cd:      str = "",
        emp_cd:     str = "",
        doc_no:     str = "",   # 견적No.
    ) -> dict:
        """
        견적서를 ERP에 저장합니다.
        lines 예시: [{"prod_cd": "A001", "qty": 10, "unit": "EA"}]
        """
        if not self._session_id:
            await self.ensure_session()

        wh = wh_cd or ERP_WH_CD
        zone = (self._zone or ERP_ZONE).lower()

        url = f"https://oapi{zone}.ecount.com/OAPI/V2/Quotation/SaveQuotation?SESSION_ID={self._session_id}"

        ser_no = str(int(upload_ser))[-4:] if upload_ser else "1"

        quotation_list = []
        for line in lines:
            qty_val = line["qty"]
            if qty_val is not None:
                qty_val = float(qty_val)
                qty_str = str(int(qty_val)) if qty_val == int(qty_val) else str(qty_val)
            else:
                qty_str = "0"

            bulk = {
                "UPLOAD_SER_NO": ser_no,
                "CUST":          cust_code,
                "PROD_CD":       line["prod_cd"],
                "QTY":           qty_str,
                "WH_CD":         wh,
            }
            # 담당자 코드
            effective_emp = emp_cd or ERP_EMP_CD
            if effective_emp:
                bulk["EMP_CD"] = effective_emp
            # 단위
            unit = line.get("unit", "")
            if unit:
                bulk["UNIT"] = unit
            # 견적No.
            if doc_no:
                bulk["DOC_NO"] = doc_no
            # 단가 및 공급가
            price_val = line.get("price") or 0
            try:
                price_val = float(price_val)
            except (ValueError, TypeError):
                price_val = 0.0
            if price_val > 0:
                qty_f = float(qty_val) if qty_val is not None else 0.0
                supply = round(price_val * qty_f, 2)
                bulk["PRICE"]       = str(int(price_val)) if price_val == int(price_val) else str(price_val)
                bulk["SUPPLY_AMT"]  = str(int(supply))    if supply    == int(supply)    else str(supply)

            quotation_list.append({"BulkDatas": bulk})

        payload = {
            "QuotationList": quotation_list
        }

        logger.info(f"[ERP SaveQuotation] URL: {url}")
        logger.debug(f"[ERP SaveQuotation] 페이로드: {payload}")

        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=15)
                    r.raise_for_status()
                    data = r.json()

                logger.info(f"[ERP SaveQuotation] 응답: {data}")

                if str(data.get("Status", "")) == "200":
                    return {"success": True, "data": data}

                if str(data.get("Status", "")) in ("301", "302"):
                    logger.warning("[ERP] 세션 만료, 재로그인 시도")
                    await self.ensure_session()
                    zone = (self._zone or ERP_ZONE).lower()
                    url = f"https://oapi{zone}.ecount.com/OAPI/V2/Quotation/SaveQuotation?SESSION_ID={self._session_id}"
                    continue

                err = data.get("Error") or data.get("error") or data
                logger.error(f"[ERP SaveQuotation] 실패 응답: {data}")
                return {"success": False, "error": err}

            except httpx.HTTPStatusError as e:
                logger.error(f"[ERP] HTTP 상태 오류 (시도 {attempt+1}): {e.response.status_code} {e.response.text}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
            except httpx.HTTPError as e:
                logger.error(f"[ERP] HTTP 오류 (시도 {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

        return {"success": False, "error": "최대 재시도 횟수 초과"}

    # 하위 호환용 (기존 코드에서 호출하는 경우)
    async def save_sale_order(self, **kwargs):
        return await self.save_quotation(**kwargs)

    # ─────────────────────────────────────────
    #  세션 초기화 (로그아웃/재연결 시)
    # ─────────────────────────────────────────
    def reset(self):
        self._session_id = None
        self._zone = None


# 싱글톤
erp_client = ERPClient()
