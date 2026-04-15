"""
ECOUNT ERP API ÃÂÃÂ­ÃÂÃÂÃÂÃÂ´ÃÂÃÂ«ÃÂÃÂÃÂÃÂ¼ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ´ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¸ (ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ«ÃÂÃÂ§ÃÂÃÂÃÂÃÂ­ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ´ ÃÂÃÂ¬ÃÂÃÂ ÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ© ÃÂÃÂªÃÂÃÂ²ÃÂÃÂ½ÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ²ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ ÃÂÃÂ)
ÃÂÃÂªÃÂÃÂ¸ÃÂÃÂ°ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ´ erp_client.pyÃÂÃÂ¬ÃÂÃÂÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ¶ÃÂÃÂÃÂÃÂ«ÃÂÃÂ¦ÃÂÃÂ¬ÃÂÃÂ­ÃÂÃÂÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ«ÃÂÃÂ§ÃÂÃÂÃÂÃÂ­ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ´ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ±ÃÂÃÂ«ÃÂÃÂ¡ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ ÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¼ÃÂÃÂ«ÃÂÃÂ¡ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ©
"""
import json
import logging
import httpx
import asyncio
from datetime import datetime, timezone, timedelta

from config import ERP_COM_CODE, ERP_USER_ID, ERP_ZONE, ERP_API_KEY

logger = logging.getLogger(__name__)


class ERPClientSS:
    def __init__(self):
        self._session_id = None
        self._zone = None

    async def ensure_session(self):
        async with httpx.AsyncClient() as client:
            r = await client.post("https://oapi.ecount.com/OAPI/V2/Zone", json={"COM_CODE": ERP_COM_CODE}, timeout=10)
            data = r.json()
            if str(data.get("Status")) != "200":
                raise RuntimeError(f"Zone ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¨: {data}")
            self._zone = data.get("Data", {}).get("ZONE") or ERP_ZONE

            zone = self._zone.lower()
            r = await client.post(f"https://oapi{zone}.ecount.com/OAPI/V2/OAPILogin", json={
                "COM_CODE": ERP_COM_CODE, "USER_ID": ERP_USER_ID,
                "API_CERT_KEY": ERP_API_KEY, "LAN_TYPE": "ko-KR",
                "ZONE": self._zone.upper(),
            }, timeout=10)
            data = r.json()
            if str(data.get("Status")) != "200":
                raise RuntimeError(f"Login ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¨: {data}")
            self._session_id = data["Data"]["Datas"]["SESSION_ID"]
            logger.info("[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ ÃÂÃÂ­ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂ£ÃÂÃÂ")
        return self._session_id

    async def save_sale(self, cust_code, lines, wh_cd="30", emp_cd=""):
        if not self._session_id:
            await self.ensure_session()

        zone = self._zone.lower()
        url = f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={self._session_id}"

        KST = timezone(timedelta(hours=9))
        io_date = datetime.now(KST).strftime("%Y%m%d")

        sale_list = []
        for idx, line in enumerate(lines, start=1):
            qty = float(line["qty"])
            qty_str = str(int(qty)) if qty == int(qty) else str(qty)
            bulk = {
                "UPLOAD_SER_NO": str(line.get("ser_no", "1")),
                "IO_DATE": io_date,
                "CUST": cust_code,
                "PROD_CD": line["prod_cd"],
                "PROD_DES": line.get("prod_name", ""),
                "QTY": qty_str,
                "WH_CD": wh_cd,
            }
            rcv = line.get("rcv_name", "")
            remark = line.get("remark", "")
            if remark:
                bulk["U_MEMO5"] = remark
                bulk["DES"] = remark
            elif rcv:
                bulk["DES"] = rcv
            # 적요2(P_REMARKS2): productOrderId 저장 (경동 자동발송처리용)
            p_remarks2 = line.get("p_remarks2", "")
            if p_remarks2:
                bulk["P_REMARKS2"] = str(p_remarks2)[:100]
            if emp_cd:
                bulk["EMP_CD"] = emp_cd
            price = float(line.get("price", 0) or 0)
            if price > 0:
                supply = round(price * qty, 2)
                bulk["PRICE"] = str(int(round(price)))
                bulk["SUPPLY_AMT"] = str(int(round(supply)))
            sale_list.append({"BulkDatas": bulk})

        payload = {"SaleList": sale_list}
        # 디버그: 전송 데이터 로그
        for i, sl in enumerate(sale_list):
            bd = sl.get("BulkDatas", {})
            logger.info(f"[ERP-SS] Line {i+1}: SER_NO={bd.get('UPLOAD_SER_NO')}, PROD_CD={bd.get('PROD_CD')}, QTY={bd.get('QTY')}, PRICE={bd.get('PRICE','0')}, DES={bd.get('DES','')[:50]}, U_MEMO5={bd.get('U_MEMO5','')[:50]}")

        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=15)
                    data = r.json()
                logger.info(f"[ERP-SS] SaveSale ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂµ: Status={data.get('Status')}")
                if str(data.get("Status")) == "200":
                    inner = data.get("Data", {})
                    if isinstance(inner, dict):
                        success_cnt = inner.get("SuccessCnt", 0)
                        fail_cnt = inner.get("FailCnt", 0)
                        results = inner.get("ResultDetails", [])
                        slip_nos = inner.get("SlipNos", [])

                        fail_details = []
                        for rd in (results or []):
                            if not rd.get("IsSuccess"):
                                fail_details.append({
                                    "line": rd.get("Line"),
                                    "error": rd.get("TotalError", ""),
                                    "fields": rd.get("Errors", []),
                                })

                        logger.info(f"[ERP-SS] SaveSale ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ±ÃÂÃÂªÃÂÃÂ³ÃÂÃÂµ: {success_cnt}ÃÂÃÂªÃÂÃÂ±ÃÂÃÂ´, ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¨: {fail_cnt}ÃÂÃÂªÃÂÃÂ±ÃÂÃÂ´, ÃÂÃÂ¬ÃÂÃÂ ÃÂÃÂÃÂÃÂ­ÃÂÃÂÃÂÃÂ: {slip_nos}")
                        is_success = success_cnt > 0 and fail_cnt == 0
                        err_summary = ""
                        if fail_cnt > 0 and fail_details:
                            err_summary = "; ".join(d.get("error","") for d in fail_details if d.get("error"))
                        result = {
                            "success": is_success,
                            "data": data,
                            "detail": {
                                "success_count": success_cnt,
                                "fail_count": fail_cnt,
                                "slip_nos": slip_nos,
                                "errors": fail_details,
                            },
                        }
                        if not is_success:
                            result["error"] = f"ERP 전송 실패 ({fail_cnt}건): {err_summary}" if err_summary else f"ERP 전송 실패 ({fail_cnt}건)"
                        return result
                    return {"success": True, "data": data}
                if str(data.get("Status")) in ("301", "302"):
                    logger.warning(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ§ÃÂÃÂÃÂÃÂ«ÃÂÃÂ£ÃÂÃÂ, ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ«ÃÂÃÂ¡ÃÂÃÂÃÂÃÂªÃÂÃÂ·ÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¸ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ")
                    await self.ensure_session()
                    zone = self._zone.lower()
                    url = f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={self._session_id}"
                    continue
                logger.error(f"[ERP-SS] SaveSale ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¨: Status={data.get('Status')}")
                err_msg = data.get("Message") or data.get("Error") or str(data.get("Status", ""))
                logger.error(f"[ERP-SS] SaveSale 응답 데이터: {data}")
                return {"success": False, "error": f"ERP 오류 (Status {data.get('Status')}): {err_msg}"}
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {"success": False, "error": str(e)}
        return {"success": False, "error": "ÃÂÃÂ¬ÃÂÃÂµÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ´ÃÂÃÂÃÂÃÂªÃÂÃÂ³ÃÂÃÂ¼"}

    async def get_inventory_balance(self, prod_codes: list[str] = None, wh_cd: str = "") -> dict:
        """
        ECOUNT ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ­ÃÂÃÂÃÂÃÂÃÂÃÂ­ÃÂÃÂÃÂÃÂ© ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ API
        prod_codes: ÃÂÃÂ­ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂªÃÂÃÂ©ÃÂÃÂ¬ÃÂÃÂ½ÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ¦ÃÂÃÂ¬ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¸ (NoneÃÂÃÂ¬ÃÂÃÂÃÂÃÂ´ÃÂÃÂ«ÃÂÃÂ©ÃÂÃÂ´ ÃÂÃÂ¬ÃÂÃÂ ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ²ÃÂÃÂ´ ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ)
        wh_cd: ÃÂÃÂ¬ÃÂÃÂ°ÃÂÃÂ½ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ½ÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ (ÃÂÃÂ«ÃÂÃÂ¹ÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ¬ÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ´ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ´ÃÂÃÂ«ÃÂÃÂ©ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ²ÃÂÃÂ´ ÃÂÃÂ¬ÃÂÃÂ°ÃÂÃÂ½ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ )
        Returns: {"success": True, "inventory": {ÃÂÃÂ­ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂªÃÂÃÂ©ÃÂÃÂ¬ÃÂÃÂ½ÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ: ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ}}
        """
        if not self._session_id:
            await self.ensure_session()

        zone = self._zone.lower()
        url = f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/GetListInventoryBalanceStatus?SESSION_ID={self._session_id}"

        KST = timezone(timedelta(hours=9))
        base_date = datetime.now(KST).strftime("%Y%m%d")

        prod_cd_str = ",".join(prod_codes) if prod_codes else ""
        logger.info(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ¬ÃÂÃÂ²ÃÂÃÂ­: WH={wh_cd}, PROD_CD={prod_cd_str[:200]}, BASE_DATE={base_date}")
        payload = {
            "BASE_DATE": base_date,
            "WH_CD": wh_cd,
            "PROD_CD": prod_cd_str,
            "PAGE_NUM": 1,
            "PER_PAGE_CNT": 5000,
        }

        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=15)
                    data = r.json()

                if str(data.get("Status")) == "200":
                    inner = data.get("Data", {}) if isinstance(data.get("Data"), dict) else {}
                    # ECOUNT API returns 'Result' key for data rows
                    rows = inner.get("Result", []) or inner.get("ÃÂÃÂªÃÂÃÂ²ÃÂÃÂ°ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ¼", []) or inner.get("Datas", []) or []
                    total_cnt = inner.get("TotalCnt", 0)
                    logger.info(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ API WH={wh_cd}: TotalCnt={total_cnt}, rows={len(rows)}, keys={list(inner.keys())}")
                    if rows and len(rows) > 0:
                        logger.info(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ  ÃÂÃÂ¬ÃÂÃÂ²ÃÂÃÂ«ÃÂÃÂ«ÃÂÃÂ²ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ§ÃÂÃÂ¸ row keys: {list(rows[0].keys()) if isinstance(rows[0], dict) else 'not dict'}")
                    inventory = {}
                    for row in rows:
                        prod_cd = row.get("PROD_CD", "") or row.get("ÃÂÃÂ­ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂªÃÂÃÂ©ÃÂÃÂ¬ÃÂÃÂ½ÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ", "")
                        # BAL_QTY = ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ (ÃÂÃÂªÃÂÃÂ¸ÃÂÃÂ°ÃÂÃÂ«ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ), also try Korean key
                        bal_qty = row.get("BAL_QTY") or row.get("ÃÂÃÂªÃÂÃÂ¸ÃÂÃÂ°ÃÂÃÂ«ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ") or row.get("ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ") or 0
                        try:
                            bal_qty = int(float(bal_qty))
                        except (ValueError, TypeError):
                            bal_qty = 0
                        if prod_cd:
                            inventory[prod_cd] = bal_qty
                    logger.info(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂ£ÃÂÃÂ WH={wh_cd}: {len(inventory)}ÃÂÃÂªÃÂÃÂ±ÃÂÃÂ´")
                    return {"success": True, "inventory": inventory, "total": len(inventory)}

                if str(data.get("Status")) in ("301", "302"):
                    logger.warning("[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ§ÃÂÃÂÃÂÃÂ«ÃÂÃÂ£ÃÂÃÂ, ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ«ÃÂÃÂ¡ÃÂÃÂÃÂÃÂªÃÂÃÂ·ÃÂÃÂ¸ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¸")
                    await self.ensure_session()
                    zone = self._zone.lower()
                    url = f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/GetListInventoryBalanceStatus?SESSION_ID={self._session_id}"
                    continue

                logger.error(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¨: Status={data.get('Status')}, Data={data}")
                return {"success": False, "error": f"API ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ«ÃÂÃÂ¥ÃÂÃÂ: Status {data.get('Status')}"}
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {"success": False, "error": str(e)}
        return {"success": False, "error": "ÃÂÃÂ¬ÃÂÃÂµÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂ«ÃÂÃÂÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂ´ÃÂÃÂÃÂÃÂªÃÂÃÂ³ÃÂÃÂ¼"}

    async def get_sales_list(self, from_date: str = "", to_date: str = "",
                             cust_code: str = "", page: int = 1, per_page: int = 500) -> dict:
        """ECOUNT 판매현황 조회 (U_MEMO5, P_REMARKS2 포함)
        Args:
            from_date: 시작일 YYYYMMDD
            to_date: 종료일 YYYYMMDD
            cust_code: 거래처코드 (빈 값이면 전체)
        Returns: {"success": bool, "items": [...], "total": int}
        """
        if not self._session_id:
            await self.ensure_session()

        zone = self._zone.lower()
        url = (f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/GetListSaleBySearch"
               f"?SESSION_ID={self._session_id}")

        payload = {"PAGE_NO": str(page), "PER_PAGE_CNT": str(per_page)}
        if from_date:
            payload["FROM_DATE"] = from_date
        if to_date:
            payload["TO_DATE"] = to_date
        if cust_code:
            payload["CUST"] = cust_code

        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=30)
                    data = r.json()

                if str(data.get("Status")) == "200":
                    result_list = data.get("Data", {}).get("Result", [])
                    total_cnt = data.get("Data", {}).get("TotalCnt", 0)
                    items = []
                    for item in result_list:
                        items.append({
                            "date": item.get("IO_DATE", ""),
                            "slip_no": item.get("SLIP_NO", ""),
                            "cust_code": item.get("CUST", ""),
                            "cust_name": item.get("CUST_DES", ""),
                            "prod_cd": item.get("PROD_CD", ""),
                            "prod_name": item.get("PROD_DES", ""),
                            "qty": item.get("QTY", 0),
                            "price": item.get("PRICE", 0),
                            "supply_amt": item.get("SUPPLY_AMT", 0),
                            "wh_cd": item.get("WH_CD", ""),
                            "u_memo5": item.get("U_MEMO5", ""),       # 비고사항 (경동택배 정보+송장)
                            "p_remarks2": item.get("P_REMARKS2", ""),  # 적요2 (productOrderId)
                            "des": item.get("DES", ""),                # 적요
                            "remarks1": item.get("REMARKS1", ""),
                            "_raw": item,
                        })
                    logger.info(f"[ERP-SS] 판매현황 조회: {len(items)}건 (총 {total_cnt})")
                    return {"success": True, "items": items, "total": total_cnt}

                if str(data.get("Status")) in ("301", "302"):
                    logger.warning("[ERP-SS] 판매현황 세션만료, 재로그인")
                    await self.ensure_session()
                    zone = self._zone.lower()
                    url = (f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/GetListSaleBySearch"
                           f"?SESSION_ID={self._session_id}")
                    continue

                logger.error(f"[ERP-SS] 판매현황 조회 실패: {data}")
                return {"success": False, "items": [], "total": 0, "error": f"Status {data.get('Status')}"}
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {"success": False, "items": [], "total": 0, "error": str(e)}
        return {"success": False, "items": [], "total": 0, "error": "재시도 초과"}

    async def get_inventory_by_warehouses(self, prod_codes: list[str] = None) -> dict:
        """ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ°(10), ÃÂÃÂ­ÃÂÃÂÃÂÃÂµÃÂÃÂ¬ÃÂÃÂ§ÃÂÃÂ(30) ÃÂÃÂ¬ÃÂÃÂ°ÃÂÃÂ½ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ  ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ¥ÃÂÃÂ¼ ÃÂÃÂ«ÃÂÃÂ³ÃÂÃÂÃÂÃÂ«ÃÂÃÂ ÃÂÃÂ¬ ÃÂÃÂ¬ÃÂÃÂ¡ÃÂÃÂ°ÃÂÃÂ­ÃÂÃÂÃÂÃÂ"""
        async def _fetch_wh(wh_cd):
            return await self.get_inventory_balance(prod_codes=prod_codes, wh_cd=wh_cd)

        r10, r30 = await asyncio.gather(_fetch_wh("10"), _fetch_wh("30"))
        yongsan = r10.get("inventory", {}) if r10.get("success") else {}
        tongjin = r30.get("inventory", {}) if r30.get("success") else {}
        total = len(set(list(yongsan.keys()) + list(tongjin.keys())))
        logger.info(f"[ERP-SS] ÃÂÃÂ¬ÃÂÃÂ°ÃÂÃÂ½ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ«ÃÂÃÂ³ÃÂÃÂ ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ¬ÃÂÃÂªÃÂÃÂ³ÃÂÃÂ : ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¬ÃÂÃÂÃÂÃÂ°={len(yongsan)}ÃÂÃÂªÃÂÃÂ±ÃÂÃÂ´, ÃÂÃÂ­ÃÂÃÂÃÂÃÂµÃÂÃÂ¬ÃÂÃÂ§ÃÂÃÂ={len(tongjin)}ÃÂÃÂªÃÂÃÂ±ÃÂÃÂ´")
        return {
            "success": True,
            "inventory": {"yongsan": yongsan, "tongjin": tongjin},
            "total": total,
        }
