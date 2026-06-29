"""
로젠택배 OpenAPI 클라이언트
- 화물추적 (운송장번호 기반)
- 송장등록 (slipPrintM) → 자체 DB 저장
"""
import os
import re
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


def _normalize_slip_no(slip_no: str) -> str:
    """운송장번호에서 하이픈/공백 제거하여 숫자만 반환"""
    return re.sub(r'\D', '', slip_no.strip())


async def track_shipments(slip_nos: list[str], user_id: str = "") -> dict:
    """
    운송장번호로 화물추적 (inquiryCargoTrackingMulti)
    user_id가 없으면 용산 계정으로 조회
    """
    if not user_id:
        user_id = ACCOUNTS["용산"]["userId"]

    # 운송장번호 정규화 (숫자만)
    cleaned = [_normalize_slip_no(s) for s in slip_nos if s.strip()]
    cleaned = [s for s in cleaned if len(s) >= 10]

    if not cleaned:
        logger.warning("[Logen] 유효한 운송장번호 없음")
        return {"sttsCd": "FAIL", "sttsMsg": "유효한 운송장번호 없음", "data": []}

    payload = {
        "userId": user_id,
        "data": [{"slipNo": s} for s in cleaned],
    }

    logger.info(f"[Logen] 화물추적 요청: userId={user_id}, slipNos={cleaned}")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{LOGEN_BASE_URL}/inquiryCargoTrackingMulti",
                json=payload,
            )
            resp.raise_for_status()
            result = resp.json()

            logger.info(f"[Logen] 화물추적 응답: sttsCd={result.get('sttsCd')}, "
                       f"sttsMsg={result.get('sttsMsg')}, "
                       f"data_count={len(result.get('data', []))}")

            # 각 항목의 결과 로깅
            for item in result.get("data", []):
                slip = item.get("slipNo", "")
                rc = item.get("resultCd", "")
                d1_cnt = len(item.get("data1", []))
                logger.info(f"[Logen]   slipNo={slip}, resultCd={rc}, scans={d1_cnt}")

            return result
    except httpx.TimeoutException:
        logger.error(f"[Logen] 화물추적 타임아웃: slipNos={cleaned}")
        return {"sttsCd": "FAIL", "sttsMsg": "API 타임아웃 (30초)", "data": []}
    except Exception as e:
        logger.error(f"[Logen] 화물추적 오류: {e}")
        return {"sttsCd": "FAIL", "sttsMsg": str(e), "data": []}


async def track_shipments_both(slip_nos: list[str]) -> list[dict]:
    """
    양쪽 계정(용산/김포) 모두로 화물추적 후 결과 병합.
    어느 계정에서 결과가 오든 모두 반환.
    중복 제거: 같은 운송장번호는 한 번만 포함.
    """
    results = []
    seen_slips = set()

    for wh_name, acct in ACCOUNTS.items():
        if not acct["userId"]:
            logger.warning(f"[Logen] {wh_name} 계정 userId 미설정")
            continue
        try:
            data = await track_shipments(slip_nos, acct["userId"])
            if data.get("data"):
                for item in data["data"]:
                    slip = item.get("slipNo", "")
                    # 중복 방지 (이미 다른 계정에서 조회 성공한 건은 스킵)
                    if slip in seen_slips:
                        continue
                    item["_warehouse"] = wh_name
                    results.append(item)
                    if item.get("resultCd") == "TRUE":
                        seen_slips.add(slip)
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
