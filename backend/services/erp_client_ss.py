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
