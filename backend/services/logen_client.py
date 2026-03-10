"""
로젠택배 OpenAPI 클라이언트
- 화물추적 (운송장번호 기반)
- 송장등록 (slipPrintM) → 자체 DB 저장
"""
import os
import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)

# ─── 로젠 API 설정 ───────────────────────────
LOGEN_BASE_URL = "https://openapi.ilogen.com/lrm02b-edi/edi"

# 계정 정보 (용산 / 김포)
ACCOUNTS = {
    "용산": {
        "userId": os.getenv("LOGEN_YONGSAN_ID", ""),
        "password": os.getenv("LOGEN_YONGSAN_PW", ""),
    },
    "김포": {
        "userId": os.getenv("LOGEN_GIMPO_ID", ""),
        "password": os.getenv("LOGEN_GIMPO_PW", ""),
    },
}


async def track_shipments(slip_nos: list[str], user_id: str = "") -> dict:
    """
    운송장번호로 화물추적 (inquiryCargoTrackingMulti)
    user_id가 없으면 용산 계정으로 조회
    """
    if not user_id:
        user_id = ACCOUNTS["용산"]["userId"]

    payload = {
        "userId": user_id,
        "data": [{"slipNo": s.strip()} for s in slip_nos if s.strip()],
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{LOGEN_BASE_URL}/inquiryCargoTrackingMulti",
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"[Logen] 화물추적 오류: {e}")
        return {"sttsCd": "FAIL", "sttsMsg": str(e), "data": []}


async def track_shipments_both(slip_nos: list[str]) -> list[dict]:
    """
    양쪽 계정(용산/김포) 모두로 화물추적 후 결과 병합.
    어느 계정에서 결과가 오든 모두 반환.
    """
    results = []
    for wh_name, acct in ACCOUNTS.items():
        if not acct["userId"]:
            continue
        try:
            data = await track_shipments(slip_nos, acct["userId"])
            if data.get("data"):
                for item in data["data"]:
                    item["_warehouse"] = wh_name
                    results.append(item)
        except Exception as e:
            logger.error(f"[Logen] {wh_name} 추적 오류: {e}")
    return results


async def register_slip(warehouse: str, slip_data: dict) -> dict:
    """
    송장 출력 주문 정보 등록 (slipPrintM)
    slip_data: slipPrintM API 스펙에 맞는 데이터
    """
    acct = ACCOUNTS.get(warehouse, ACCOUNTS["용산"])
    if not acct["userId"]:
        return {"sttsCd": "FAIL", "sttsMsg": "계정 정보 없음"}

    payload = {
        "userId": acct["userId"],
        "data": [slip_data],
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{LOGEN_BASE_URL}/slipPrintM",
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"[Logen] 송장등록 오류: {e}")
        return {"sttsCd": "FAIL", "sttsMsg": str(e)}


def get_account_info(warehouse: str) -> Optional[dict]:
    """계정 정보 반환"""
    return ACCOUNTS.get(warehouse)
