"""
iLogen 웹포털 자동등록 클라이언트

플로우: login(AES-128-CBC) → checkExcelData → saveExcelData → getOrdSeq → getTransData
"""
import base64
import logging
import httpx
from typing import Optional
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding
from cryptography.hazmat.backends import default_backend

from config import (
    LOGEN_GIMPO_ID, LOGEN_GIMPO_PW, LOGEN_GIMPO_PW_PREV,
    LOGEN_YONGSAN_ID, LOGEN_YONGSAN_PW, LOGEN_YONGSAN_PW_PREV,
    SENDER_GIMPO_NAME, SENDER_GIMPO_TEL, SENDER_GIMPO_ADDR,
    SENDER_YONGSAN_NAME, SENDER_YONGSAN_TEL, SENDER_YONGSAN_ADDR,
)

logger = logging.getLogger(__name__)

ILOGEN_BASE = "https://logis.ilogen.com"
AES_KEY = b"A9f$2kLm!zQx7@1B"
AES_IV  = b"V#8d*P0w$eR6!nTq"

ACCOUNTS = {
    "gimpo":  {"userId": LOGEN_GIMPO_ID,  "passwords": [LOGEN_GIMPO_PW, LOGEN_GIMPO_PW_PREV]},
    "yongsan": {"userId": LOGEN_YONGSAN_ID, "passwords": [LOGEN_YONGSAN_PW, LOGEN_YONGSAN_PW_PREV]},
}

SENDER = {
    "gimpo":  {"name": SENDER_GIMPO_NAME,  "tel": SENDER_GIMPO_TEL,  "addr": SENDER_GIMPO_ADDR},
    "yongsan": {"name": SENDER_YONGSAN_NAME, "tel": SENDER_YONGSAN_TEL, "addr": SENDER_YONGSAN_ADDR},
}


def _aes_encrypt(plaintext: str) -> str:
    padder = sym_padding.PKCS7(128).padder()
    padded = padder.update(plaintext.encode("utf-8")) + padder.finalize()
    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=default_backend())
    enc = cipher.encryptor()
    ct = enc.update(padded) + enc.finalize()
    return base64.b64encode(ct).decode("utf-8")


def _orders_to_excel_data(orders: list[dict]) -> str:
    rows = []
    for o in orders:
        row = "\t".join([
            o.get("snd_name", ""), o.get("snd_tel", ""), o.get("snd_addr", ""),
            o.get("rcv_name", ""), o.get("rcv_tel", ""), o.get("rcv_addr", ""),
            o.get("fare_code", "030"), o.get("goods_nm", ""),
        ])
        rows.append(row)
    return "\n".join(rows)


async def register_orders(warehouse: str, orders: list[dict]) -> dict:
    if not orders:
        return {"success": True, "tracking_numbers": [], "message": "등록할 주문 없음"}

    acct = ACCOUNTS.get(warehouse)
    if not acct or not acct["userId"]:
        return {"success": False, "tracking_numbers": [], "error": f"{warehouse} 계정 미설정"}

    passwords = [pw for pw in acct.get("passwords", []) if pw]
    if not passwords:
        return {"success": False, "tracking_numbers": [], "error": f"{warehouse} 비밀번호 미설정"}

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            jwt_token = None
            for i, pw in enumerate(passwords):
                label = "현재" if i == 0 else "이전"
                logger.info(f"[iLogen] {warehouse} 로그인 시도 ({label} 비밀번호)")
                jwt_token = await _login(client, acct["userId"], pw)
                if jwt_token:
                    if i > 0:
                        logger.warning(f"[iLogen] {warehouse} 이전 비밀번호로 로그인 성공!")
                    break
                logger.warning(f"[iLogen] {warehouse} {label} 비밀번호 실패")
            if not jwt_token:
                return {"success": False, "tracking_numbers": [], "error": f"iLogen 로그인 실패"}

            headers = {"Authorization": f"Bearer {jwt_token}", "Content-Type": "application/json"}
            excel_data = _orders_to_excel_data(orders)

            r = await client.post(f"{ILOGEN_BASE}/api/order/checkExcelData",
                                  headers=headers, json={"excelData": excel_data, "rowCount": len(orders)})
            check = r.json()
            logger.info(f"[iLogen] check: {check.get('resultCode')}")
            if check.get("resultCode") != "0000":
                return {"success": False, "tracking_numbers": [], "error": f"검증실패: {check.get('resultMsg','')}"}

            r = await client.post(f"{ILOGEN_BASE}/api/order/saveExcelData",
                                  headers=headers, json={"excelData": excel_data, "rowCount": len(orders)})
            save = r.json()
            logger.info(f"[iLogen] save: {save.get('resultCode')}")
            if save.get("resultCode") != "0000":
                return {"success": False, "tracking_numbers": [], "error": f"저장실패: {save.get('resultMsg','')}"}

            KST = timezone(timedelta(hours=9))
            today = datetime.now(KST).strftime("%Y%m%d")
            r = await client.post(f"{ILOGEN_BASE}/api/order/getOrdSeq",
                                  headers=headers, json={"ordDate": today})
            seq = r.json()
            ord_seq_list = seq.get("data", [])

            r = await client.post(f"{ILOGEN_BASE}/api/order/getTransData",
                                  headers=headers, json={"ordSeqList": [s["ordSeq"] for s in ord_seq_list if s.get("ordSeq")]})
            trans = r.json()

            tracking_numbers = []
            for idx, td in enumerate(trans.get("data", [])):
                slip_no = td.get("slipNo", "")
                if slip_no:
                    tracking_numbers.append({"index": idx, "slip_no": slip_no, "rcv_name": td.get("rcvName","")})

            logger.info(f"[iLogen] 운송장 {len(tracking_numbers)}건 채번")
            return {"success": True, "tracking_numbers": tracking_numbers,
                    "total_registered": len(orders), "total_tracking": len(tracking_numbers)}

    except Exception as e:
        logger.error(f"[iLogen] 오류: {e}", exc_info=True)
        return {"success": False, "tracking_numbers": [], "error": str(e)}


async def _login(client: httpx.AsyncClient, user_id: str, password: str) -> Optional[str]:
    try:
        encrypted_id = _aes_encrypt(user_id)
        encrypted_pw = _aes_encrypt(password)
        r = await client.post(f"{ILOGEN_BASE}/api/auth/login",
                              json={"userId": encrypted_id, "userPw": encrypted_pw}, timeout=15)
        if r.status_code != 200:
            logger.error(f"[iLogen] 로그인 HTTP {r.status_code}")
            return None
        body = r.json()
        if body.get("resultCode") != "0000":
            logger.error(f"[iLogen] 로그인 실패: {body.get('resultMsg','')}")
            return None
        token = body.get("data", {}).get("token") or body.get("token")
        logger.info("[iLogen] 로그인 성공")
        return token
    except Exception as e:
        logger.error(f"[iLogen] 로그인 오류: {e}")
        return None


def get_sender(warehouse: str) -> dict:
    return SENDER.get(warehouse, SENDER["gimpo"])
