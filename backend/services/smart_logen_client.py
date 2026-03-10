"""
SmartLogen 웹 포털 클라이언트
- smart.ilogen.com:8080 로그인 후 주문(발송) 내역 조회
- OrderSelect API 호출 → SEED 복호화 → 파싱
- 발송 내역을 자체 DB에 자동 저장

OrderSelect 응답 필드 (67개, Ξ 구분):
[0]  tradeCd       거래처코드
[1]  tradeName     거래처명
[7]  takeDt        접수일자 YYYYMMDD
[8]  status        상태 (배송완료/집하완료/배송출발/접수/송장출력)
[9]  sndSlipNo     보내는분 연락처(?)
[10] sndName       보내는분 이름
[11] sndTel        보내는분 전화
[15] sndAddr1      보내는분 주소1
[16] sndAddr2      보내는분 주소2
[24] rcvName       받는분 이름
[25] rcvTel1       받는분 전화1
[26] rcvTel2       받는분 전화2
[27] rcvZip        받는분 우편번호
[29] rcvAddr1      받는분 주소1
[30] rcvAddr2      받는분 주소2
[36] sndDt         발송일자
[37] dlvDt         배송완료일자
[42] qty           수량
[55] slipNo        운송장번호 (918-xxxx-xxxx)
"""
import os
import logging
import subprocess
import tempfile
import urllib.request
import urllib.parse
import http.cookiejar
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── SmartLogen 설정 ───────────────────────────
SMART_LOGEN_BASE = "http://smart.ilogen.com:8080/SmartLogen"

# 계정 정보 (용산 / 김포) — 로젠 OpenAPI와 동일 계정 사용
SMART_ACCOUNTS = {
    "용산": {
        "userId": os.getenv("LOGEN_YONGSAN_ID", ""),
        "password": os.getenv("LOGEN_YONGSAN_PW", ""),
        "branchCd": "227",
    },
    "김포": {
        "userId": os.getenv("LOGEN_GIMPO_ID", ""),
        "password": os.getenv("LOGEN_GIMPO_PW", ""),
        "branchCd": "348",
    },
}

# SEED 복호화를 위한 Node.js 스크립트
SEED_JS_PATH = str(Path(__file__).parent / "seedForNode.js")


def _decrypt_seed(encrypted_text: str) -> str:
    """Node.js를 통한 SEED 복호화 (대용량 데이터는 파일 경유)"""
    if not encrypted_text or len(encrypted_text) < 20:
        return ""

    try:
        # 대용량 데이터는 파일로 전달 (OS 명령줄 길이 제한 회피)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tf:
            tf.write(encrypted_text)
            temp_path = tf.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as of:
            out_path = of.name

        js_code = f"""
const fs = require('fs');
const vm = require('vm');
const seedCode = fs.readFileSync('{SEED_JS_PATH}', 'utf8');
const sandbox = {{ Math: Math, undefined: undefined }};
vm.createContext(sandbox);
vm.runInContext(seedCode, sandbox);
const CryptoJS = sandbox.CryptoJS;
const encrypted = fs.readFileSync('{temp_path}', 'utf8').trim();
function string_to_utf8_hex_string(str) {{
    let hex = '';
    for (let i = 0; i < str.length; i++) hex += str.charCodeAt(i).toString(16).padStart(2, '0');
    return hex;
}}
const keyText = "ILOGEN.COMGCSEED";
const keyHex = string_to_utf8_hex_string(keyText);
const key = CryptoJS.enc.Hex.parse(keyHex);
const decrypted = CryptoJS.SEED.decrypt(encrypted, key, {{ iv: keyText }});
fs.writeFileSync('{out_path}', decrypted.toString(CryptoJS.enc.Utf8));
"""

        result = subprocess.run(
            ["node", "-e", js_code],
            capture_output=True, text=True, timeout=30,
        )

        if result.returncode != 0:
            logger.error(f"[SmartLogen] SEED 복호화 오류: {result.stderr[:200]}")
            return ""

        with open(out_path, 'r') as f:
            decrypted = f.read()

        return decrypted

    except Exception as e:
        logger.error(f"[SmartLogen] SEED 복호화 예외: {e}")
        return ""
    finally:
        # 임시 파일 정리
        try:
            os.unlink(temp_path)
        except:
            pass
        try:
            os.unlink(out_path)
        except:
            pass


def _login(warehouse: str) -> Optional[tuple]:
    """
    SmartLogen 로그인 후 (opener, login_parts) 반환
    login_parts: Ξ 구분 로그인 응답 배열
    """
    acct = SMART_ACCOUNTS.get(warehouse)
    if not acct or not acct["userId"]:
        logger.warning(f"[SmartLogen] {warehouse} 계정 정보 없음")
        return None

    try:
        cj = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

        login_data = urllib.parse.urlencode({
            "userid": acct["userId"],
            "userpw": acct["password"],
            "SessionTime": "7200000",
        }).encode()

        resp = opener.open(f"{SMART_LOGEN_BASE}/UserLogin", login_data, timeout=15)
        login_result = resp.read().decode("utf-8")

        if not login_result.startswith("TRUE"):
            logger.error(f"[SmartLogen] {warehouse} 로그인 실패: {login_result[:100]}")
            return None

        parts = login_result.split("Ξ")
        logger.info(f"[SmartLogen] {warehouse} 로그인 성공 (BranCD={parts[1]})")
        return (opener, parts)

    except Exception as e:
        logger.error(f"[SmartLogen] {warehouse} 로그인 오류: {e}")
        return None


def _fetch_orders(
    opener: urllib.request.OpenerDirector,
    warehouse: str,
    branch_cd: str,
    user_id: str,
    from_date: str,
    to_date: str,
) -> list[dict]:
    """
    OrderSelect API 호출 → SEED 복호화 → 파싱

    주문 페이지에서 접수된 발송 내역을 모두 가져옵니다.
    """
    try:
        # order 페이지 먼저 로드 (세션 컨텍스트 설정)
        opener.open(f"{SMART_LOGEN_BASE}/order", timeout=30)
    except Exception as e:
        logger.warning(f"[SmartLogen] order 페이지 로드 실패: {e}")

    params = urllib.parse.urlencode({
        "data0": from_date,     # 시작일
        "data1": to_date,       # 종료일
        "data2": branch_cd,     # BranCD
        "data3": user_id,       # UserID
        "data4": "",            # orderState (빈값 = 전체)
    }).encode()

    try:
        req = urllib.request.Request(
            f"{SMART_LOGEN_BASE}/OrderSelect",
            data=params,
            headers={
                "Referer": f"{SMART_LOGEN_BASE}/order",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        resp = opener.open(req, timeout=60)
        encrypted = resp.read().decode("utf-8")

        if len(encrypted) <= 24:
            logger.info(f"[SmartLogen] {warehouse} {from_date}~{to_date}: 데이터 없음")
            return []

        logger.info(f"[SmartLogen] {warehouse} 암호화 응답: {len(encrypted)} bytes")

        decrypted = _decrypt_seed(encrypted)
        if not decrypted or "FALSE" in decrypted[:20]:
            error_msg = decrypted[:100] if decrypted else "복호화 실패"
            logger.warning(f"[SmartLogen] {warehouse} 조회 오류: {error_msg}")
            return []

        # 파싱: ≡ 구분자로 레코드 분리, 각 레코드는 Ξ로 필드 분리
        records = []
        raw_records = [r for r in decrypted.split("≡") if r.strip()]

        for raw in raw_records:
            fields = raw.split("Ξ")

            # 첫 번째 레코드는 요약 (5개 필드): [상태, 총건수, 접수건수, 출력건수, ?]
            if len(fields) < 20:
                continue

            # 운송장번호 없는 건은 스킵
            slip_no = fields[55].strip() if len(fields) > 55 else ""
            if not slip_no:
                continue

            # 운송장번호 형식 통일 (하이픈 제거)
            slip_no_clean = slip_no.replace("-", "")

            records.append({
                "warehouse": warehouse,
                "slip_no": slip_no_clean,
                "slip_no_display": slip_no,
                "rcv_name": fields[24].strip() if len(fields) > 24 else "",
                "rcv_tel": fields[25].strip() if len(fields) > 25 else "",
                "rcv_cell": fields[26].strip() if len(fields) > 26 else "",
                "rcv_zip": fields[27].strip() if len(fields) > 27 else "",
                "rcv_addr1": fields[29].strip() if len(fields) > 29 else "",
                "rcv_addr2": fields[30].strip() if len(fields) > 30 else "",
                "snd_name": fields[10].strip() if len(fields) > 10 else "",
                "snd_tel": fields[11].strip() if len(fields) > 11 else "",
                "take_dt": fields[7].strip() if len(fields) > 7 else "",
                "snd_dt": fields[36].strip() if len(fields) > 36 else "",
                "dlv_dt": fields[37].strip() if len(fields) > 37 else "",
                "status": fields[8].strip() if len(fields) > 8 else "",
                "qty": fields[42].strip() if len(fields) > 42 else "1",
                "goods_nm": fields[39].strip() if len(fields) > 39 else "",
            })

        logger.info(f"[SmartLogen] {warehouse} {from_date}~{to_date}: {len(records)}건 조회 완료")
        return records

    except Exception as e:
        logger.error(f"[SmartLogen] {warehouse} OrderSelect 오류: {e}")
        return []


async def fetch_shipments(
    warehouse: str = "",
    from_date: str = "",
    to_date: str = "",
    days: int = 7,
) -> list[dict]:
    """
    SmartLogen에서 주문(발송) 내역 가져오기

    Args:
        warehouse: "용산", "김포", 또는 "" (둘 다)
        from_date: 시작일 YYYYMMDD (없으면 days일 전)
        to_date: 종료일 YYYYMMDD (없으면 오늘)
        days: from_date 미지정 시 몇 일 전부터 (기본 7일, 최대 30일)

    Returns:
        발송 내역 dict 리스트
    """
    if not to_date:
        to_date = datetime.now().strftime("%Y%m%d")
    if not from_date:
        days = min(days, 30)
        from_dt = datetime.strptime(to_date, "%Y%m%d") - timedelta(days=days)
        from_date = from_dt.strftime("%Y%m%d")

    all_records = []

    # 조회할 창고 결정
    warehouses = [warehouse] if warehouse and warehouse in SMART_ACCOUNTS else list(SMART_ACCOUNTS.keys())

    for wh in warehouses:
        login_result = _login(wh)
        if not login_result:
            continue

        opener, parts = login_result
        branch_cd = parts[1]  # BranCD
        user_id = parts[3]    # UserID

        records = _fetch_orders(opener, wh, branch_cd, user_id, from_date, to_date)
        all_records.extend(records)

    return all_records


def save_fetched_to_db(records: list[dict], conn) -> int:
    """
    SmartLogen에서 가져온 레코드를 shipments DB에 저장 (UPSERT)
    """
    if not records:
        return 0

    saved = 0
    try:
        for rec in records:
            slip_no = rec.get("slip_no", "").strip()
            if not slip_no:
                continue

            warehouse = rec.get("warehouse", "")
            rcv_name = rec.get("rcv_name", "")
            rcv_tel = rec.get("rcv_tel", "")
            rcv_cell = rec.get("rcv_cell", "")
            rcv_addr1 = rec.get("rcv_addr1", "")
            rcv_addr2 = rec.get("rcv_addr2", "")
            rcv_zip = rec.get("rcv_zip", "")
            snd_name = rec.get("snd_name", "")
            snd_tel = rec.get("snd_tel", "")
            take_dt = rec.get("take_dt", "").replace("-", "").replace(".", "")[:8]
            qty_str = rec.get("qty", "1")
            qty = int(qty_str) if qty_str.isdigit() else 1
            status = rec.get("status", "")
            goods_nm = rec.get("goods_nm", "")

            if not take_dt:
                take_dt = datetime.now().strftime("%Y%m%d")

            # UPSERT: 있으면 상태 업데이트, 없으면 삽입
            existing = conn.execute(
                "SELECT id FROM shipments WHERE slip_no = ? AND warehouse = ?",
                (slip_no, warehouse)
            ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE shipments
                       SET status = ?, rcv_tel = ?, rcv_cell = ?,
                           rcv_addr1 = ?, rcv_addr2 = ?,
                           goods_nm = CASE WHEN ? != '' THEN ? ELSE goods_nm END
                       WHERE slip_no = ? AND warehouse = ?""",
                    (status, rcv_tel, rcv_cell,
                     rcv_addr1, rcv_addr2,
                     goods_nm, goods_nm,
                     slip_no, warehouse)
                )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO shipments
                       (warehouse, slip_no, rcv_name, rcv_tel, rcv_cell,
                        rcv_addr1, rcv_addr2, rcv_zip,
                        snd_name, snd_tel,
                        goods_nm, qty, take_dt, status)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (warehouse, slip_no, rcv_name, rcv_tel, rcv_cell,
                     rcv_addr1, rcv_addr2, rcv_zip,
                     snd_name, snd_tel,
                     goods_nm, qty, take_dt, status)
                )
            saved += 1

        conn.commit()
    except Exception as e:
        logger.error(f"[SmartLogen] DB 저장 오류: {e}")

    return saved
