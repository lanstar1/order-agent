"""
iLOGEN OpenAPI 클라이언트 — 라인업시스템(주)

REST API 방식으로 로젠택배 주문등록 / 송장번호 조회 수행.
- 인증: secretKey 헤더
- IP 화이트리스트 필요 (iLogen 오픈등록에서 등록)
- 운임 공식: dlvFare = BOX_FARE × qty
"""
import uuid
import logging
import httpx
from datetime import datetime, timezone, timedelta

from config import (
    ILOGEN_SECRET_KEY, ILOGEN_USER_ID, ILOGEN_CUST_CD,
    ILOGEN_BASE_URL, ILOGEN_BOX_FARE, ILOGEN_FARE_TY,
    ILOGEN_SND_NM, ILOGEN_SND_ADDR, ILOGEN_SND_TEL, ILOGEN_SND_CELL,
)

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

HEADERS = {
    "secretKey": ILOGEN_SECRET_KEY,
    "Content-Type": "application/json",
}


def _gen_fix_take_no() -> str:
    """고유 주문번호: R + YYMMDDHHmmSS + 6자리 hex"""
    return f"R{datetime.now(KST).strftime('%y%m%d%H%M%S')}{uuid.uuid4().hex[:6].upper()}"


async def check_contract() -> dict:
    """계약정보 조회 — 연결 & 인증 테스트용"""
    url = f"{ILOGEN_BASE_URL}/lrm02b-edi/edi/contractTotalInfo"
    payload = {"userId": ILOGEN_USER_ID, "data": [{"custCd": ILOGEN_CUST_CD}]}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, json=payload, headers=HEADERS)
        r.raise_for_status()
        return r.json()


async def register_orders(orders: list[dict]) -> dict:
    """
    주문 일괄 등록.

    orders: list of dict, 각 항목에 필요한 필드:
      - rcv_name: 수하인명
      - rcv_addr: 수하인 주소
      - rcv_tel: 수하인 연락처
      - qty: 박스 수량 (기본 1)
      - goods_nm: 물품명 (선택)
      - goods_amt: 물품금액 (선택)
      - in_qty: 내품수량 (선택, 기본=qty)
      - snd_msg: 배송메시지 (선택)
      - fix_take_no: 주문번호 (없으면 자동생성)

    Returns: {
      success: bool,
      registered: [{fix_take_no, result_cd, result_msg}, ...],
      stts_cd, stts_msg,
      error: str (실패 시)
    }
    """
    if not orders:
        return {"success": True, "registered": [], "stts_msg": "등록할 주문 없음"}

    if not ILOGEN_SECRET_KEY:
        return {"success": False, "registered": [], "error": "ILOGEN_SECRET_KEY 환경변수 미설정"}

    url = f"{ILOGEN_BASE_URL}/lrm02b-edi/edi/registerOrderData"
    today = datetime.now(KST).strftime("%Y%m%d")

    data_list = []
    for o in orders:
        qty = int(o.get("qty", 1)) or 1
        fix_take_no = o.get("fix_take_no") or _gen_fix_take_no()
        o["_fix_take_no"] = fix_take_no  # 추후 송장조회용으로 저장

        data_list.append({
            "custCd": ILOGEN_CUST_CD,
            "takeDt": today,
            "fixTakeNo": fix_take_no,
            # 송하인 (고정)
            "sndCustNm": ILOGEN_SND_NM,
            "sndCustAddr": ILOGEN_SND_ADDR,
            "sndTelNo": ILOGEN_SND_TEL,
            "sndCellNo": ILOGEN_SND_CELL,
            # 수하인 (가변)
            "rcvCustNm": o.get("rcv_name", ""),
            "rcvCustAddr": o.get("rcv_addr", ""),
            "rcvTelNo": o.get("rcv_tel", ""),
            "rcvCellNo": o.get("rcv_tel", ""),
            # 운임
            "fareTy": ILOGEN_FARE_TY,
            "qty": qty,
            "dlvFare": ILOGEN_BOX_FARE * qty,
            "extraFare": 0,
            # 물품
            "goodsNm": o.get("goods_nm", ""),
            "goodsAmt": int(o.get("goods_amt", 0) or 0),
            "inQty": int(o.get("in_qty", qty) or qty),
            "sndMsg": o.get("snd_msg", ""),
            "mrgYn": "N",
        })

    payload = {"userId": ILOGEN_USER_ID, "data": data_list}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            logger.info(f"[iLOGEN] 주문등록 {len(data_list)}건 → {url}")
            r = await client.post(url, json=payload, headers=HEADERS)
            r.raise_for_status()
            resp = r.json()

        stts_cd = resp.get("sttsCd", "")
        stts_msg = resp.get("sttsMsg", "")
        registered = resp.get("data", [])
        success = stts_cd in ("SUCCESS", "PARTIAL SUCCESS")

        logger.info(f"[iLOGEN] 등록 결과: {stts_cd} — {stts_msg}")
        return {
            "success": success,
            "registered": registered,
            "stts_cd": stts_cd,
            "stts_msg": stts_msg,
        }
    except httpx.ConnectTimeout:
        logger.error("[iLOGEN] 연결 타임아웃 — IP 화이트리스트 확인 필요")
        return {"success": False, "registered": [], "error": "로젠 API 연결 타임아웃 (IP 화이트리스트 미등록 가능)"}
    except Exception as e:
        logger.error(f"[iLOGEN] 주문등록 오류: {e}", exc_info=True)
        return {"success": False, "registered": [], "error": str(e)}


async def query_slip_numbers(fix_take_nos: list[str]) -> dict:
    """
    송장번호 조회.

    Returns: {
      success: bool,
      slips: [{fix_take_no, slip_nos: [str], del_yn: [str]}, ...],
      stts_cd, stts_msg,
      error: str (실패 시)
    }
    """
    if not fix_take_nos:
        return {"success": True, "slips": [], "stts_msg": "조회 대상 없음"}

    url = f"{ILOGEN_BASE_URL}/lrm02b-edi/edi/inquirySlipNoMulti"
    payload = {
        "userId": ILOGEN_USER_ID,
        "data": [{"custCd": ILOGEN_CUST_CD, "fixTakeNo": no} for no in fix_take_nos],
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            logger.info(f"[iLOGEN] 송장조회 {len(fix_take_nos)}건")
            r = await client.post(url, json=payload, headers=HEADERS)
            r.raise_for_status()
            resp = r.json()

        stts_cd = resp.get("sttsCd", "")
        stts_msg = resp.get("sttsMsg", "")
        data = resp.get("data", [])

        slips = []
        for item in data:
            slip_nos = []
            for s in item.get("data1", []):
                if s.get("delYn", "N") != "Y" and s.get("slipNo"):
                    slip_nos.append(s["slipNo"])
            slips.append({
                "fix_take_no": item.get("fixTakeNo", ""),
                "slip_nos": slip_nos,
                "result_cd": item.get("resultCd", ""),
            })

        logger.info(f"[iLOGEN] 송장조회 결과: {stts_cd} — 총 {sum(len(s['slip_nos']) for s in slips)}건 발급")
        return {
            "success": stts_cd in ("SUCCESS", "PARTIAL SUCCESS"),
            "slips": slips,
            "stts_cd": stts_cd,
            "stts_msg": stts_msg,
        }
    except Exception as e:
        logger.error(f"[iLOGEN] 송장조회 오류: {e}", exc_info=True)
        return {"success": False, "slips": [], "error": str(e)}


async def register_and_get_slips(orders: list[dict]) -> dict:
    """
    주문등록 + 송장번호 조회 통합.

    Returns: {
      success: bool,
      results: [{fix_take_no, rcv_name, slip_nos: [str], register_ok: bool}, ...],
      total_orders, total_slips,
      error: str (실패 시)
    }
    """
    # 1) 주문 등록
    reg_result = await register_orders(orders)
    if not reg_result["success"]:
        return {
            "success": False,
            "results": [],
            "total_orders": len(orders),
            "total_slips": 0,
            "error": reg_result.get("error", reg_result.get("stts_msg", "등록 실패")),
        }

    # 2) 송장번호 조회
    fix_take_nos = [o["_fix_take_no"] for o in orders if o.get("_fix_take_no")]
    if not fix_take_nos:
        return {
            "success": True,
            "results": [],
            "total_orders": len(orders),
            "total_slips": 0,
            "error": "주문번호 없음",
        }

    slip_result = await query_slip_numbers(fix_take_nos)

    # 3) 결과 매핑
    slip_map = {s["fix_take_no"]: s["slip_nos"] for s in slip_result.get("slips", [])}
    results = []
    total_slips = 0
    for o in orders:
        ftn = o.get("_fix_take_no", "")
        sns = slip_map.get(ftn, [])
        total_slips += len(sns)
        results.append({
            "fix_take_no": ftn,
            "rcv_name": o.get("rcv_name", ""),
            "slip_nos": sns,
            "register_ok": True,
        })

    return {
        "success": True,
        "results": results,
        "total_orders": len(orders),
        "total_slips": total_slips,
        "register_result": reg_result,
        "slip_result": slip_result,
    }
