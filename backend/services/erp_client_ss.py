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
