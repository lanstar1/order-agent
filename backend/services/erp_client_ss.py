"""
ECOUNT ERP API 클라이언트 (스마트스토어 전용 경량 버전)
기존 erp_client.py와 분리하여 스마트스토어 자동등록 전용으로 사용
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
                raise RuntimeError(f"Zone 실패: {data}")
            self._zone = data.get("Data", {}).get("ZONE") or ERP_ZONE

            zone = self._zone.lower()
            r = await client.post(f"https://oapi{zone}.ecount.com/OAPI/V2/OAPILogin", json={
                "COM_CODE": ERP_COM_CODE, "USER_ID": ERP_USER_ID,
                "API_CERT_KEY": ERP_API_KEY, "LAN_TYPE": "ko-KR",
                "ZONE": self._zone.upper(),
            }, timeout=10)
            data = r.json()
            if str(data.get("Status")) != "200":
                raise RuntimeError(f"Login 실패: {data}")
            self._session_id = data["Data"]["Datas"]["SESSION_ID"]
            logger.info("[ERP-SS] 세션 획득 완료")
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
                "UPLOAD_SER_NO": str(idx),
                "IO_DATE": io_date,
                "CUST": cust_code,
                "PROD_CD": line["prod_cd"],
                "QTY": qty_str,
                "WH_CD": wh_cd,
            }
            if emp_cd:
                bulk["EMP_CD"] = emp_cd
            price = float(line.get("price", 0) or 0)
            if price > 0:
                supply = round(price * qty, 2)
                bulk["PRICE"] = str(int(price)) if price == int(price) else str(price)
                bulk["SUPPLY_AMT"] = str(int(supply)) if supply == int(supply) else str(supply)
            sale_list.append({"BulkDatas": bulk})

        payload = {"SaleList": sale_list}

        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=15)
                    data = r.json()
                logger.info(f"[ERP-SS] SaveSale 응답: Status={data.get('Status')}")
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

                        logger.info(f"[ERP-SS] SaveSale 성공: {success_cnt}건, 실패: {fail_cnt}건, 전표: {slip_nos}")
                        is_success = success_cnt > 0 and fail_cnt == 0
                        return {
                            "success": is_success,
                            "data": data,
                            "detail": {
                                "success_count": success_cnt,
                                "fail_count": fail_cnt,
                                "slip_nos": slip_nos,
                                "errors": fail_details,
                            },
                        }
                    return {"success": True, "data": data}
                if str(data.get("Status")) in ("301", "302"):
                    logger.warning(f"[ERP-SS] 세션 만료, 재로그인 시도")
                    await self.ensure_session()
                    zone = self._zone.lower()
                    url = f"https://oapi{zone}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={self._session_id}"
                    continue
                logger.error(f"[ERP-SS] SaveSale 실패: Status={data.get('Status')}")
                return {"success": False, "error": data}
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {"success": False, "error": str(e)}
        return {"success": False, "error": "최대 재시도 초과"}

    async def get_inventory_balance(self, prod_codes: list[str] = None, wh_cd: str = "") -> dict:
        """
        ECOUNT 재고현황 조회 API
        prod_codes: 품목코드 리스트 (None이면 전체 조회)
        wh_cd: 창고코드 (빈 문자열이면 전체 창고)
        Returns: {"success": True, "inventory": {품목코드: 재고수량}}
        """
        if not self._session_id:
            await self.ensure_session()

        zone = self._zone.lower()
        url = f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/GetListInventoryBalanceStatus?SESSION_ID={self._session_id}"

        KST = timezone(timedelta(hours=9))
        base_date = datetime.now(KST).strftime("%Y%m%d")

        prod_cd_str = ",".join(prod_codes) if prod_codes else ""
        logger.info(f"[ERP-SS] 재고조회 요청: WH={wh_cd}, PROD_CD={prod_cd_str[:200]}, BASE_DATE={base_date}")
        payload = {
            "BASE_DATE": base_date,
            "WH_CD": wh_cd,
            "PROD_CD": prod_cd_str,
        }

        for attempt in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(url, json=payload, timeout=15)
                    data = r.json()

                if str(data.get("Status")) == "200":
                    inner = data.get("Data", {}) if isinstance(data.get("Data"), dict) else {}
                    # ECOUNT API returns '결과' (Korean) instead of 'Datas'
                    rows = inner.get("결과", []) or inner.get("Datas", []) or []
                    total_cnt = inner.get("TotalCnt", 0)
                    logger.info(f"[ERP-SS] 재고API WH={wh_cd}: TotalCnt={total_cnt}, rows={len(rows)}, keys={list(inner.keys())}")
                    if rows and len(rows) > 0:
                        logger.info(f"[ERP-SS] 재고 첫번째 row keys: {list(rows[0].keys()) if isinstance(rows[0], dict) else 'not dict'}")
                    inventory = {}
                    for row in rows:
                        prod_cd = row.get("PROD_CD", "") or row.get("품목코드", "")
                        # BAL_QTY = 재고수량 (기말재고), also try Korean key
                        bal_qty = row.get("BAL_QTY") or row.get("기말재고") or row.get("재고수량") or 0
                        try:
                            bal_qty = int(float(bal_qty))
                        except (ValueError, TypeError):
                            bal_qty = 0
                        if prod_cd:
                            inventory[prod_cd] = bal_qty
                    logger.info(f"[ERP-SS] 재고조회 완료 WH={wh_cd}: {len(inventory)}건")
                    return {"success": True, "inventory": inventory, "total": len(inventory)}

                if str(data.get("Status")) in ("301", "302"):
                    logger.warning("[ERP-SS] 재고조회 세션 만료, 재로그인")
                    await self.ensure_session()
                    zone = self._zone.lower()
                    url = f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/GetListInventoryBalanceStatus?SESSION_ID={self._session_id}"
                    continue

                logger.error(f"[ERP-SS] 재고조회 실패: Status={data.get('Status')}, Data={data}")
                return {"success": False, "error": f"API 오류: Status {data.get('Status')}"}
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {"success": False, "error": str(e)}
        return {"success": False, "error": "최대 재시도 초과"}

    async def get_inventory_by_warehouses(self, prod_codes: list[str] = None) -> dict:
        """용산(10), 통진(30) 창고 재고를 병렬 조회"""
        async def _fetch_wh(wh_cd):
            return await self.get_inventory_balance(prod_codes=prod_codes, wh_cd=wh_cd)

        r10, r30 = await asyncio.gather(_fetch_wh("10"), _fetch_wh("30"))
        yongsan = r10.get("inventory", {}) if r10.get("success") else {}
        tongjin = r30.get("inventory", {}) if r30.get("success") else {}
        total = len(set(list(yongsan.keys()) + list(tongjin.keys())))
        logger.info(f"[ERP-SS] 창고별 재고: 용산={len(yongsan)}건, 통진={len(tongjin)}건")
        return {
            "success": True,
            "inventory": {"yongsan": yongsan, "tongjin": tongjin},
            "total": total,
        }
