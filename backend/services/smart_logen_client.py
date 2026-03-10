"""
SmartLogen 웹 포털 클라이언트
- smart.ilogen.com:8080 로그인 후 발송 실적 조회
- PickSndRecordSelect (집하배송조회) API 호출
- SEED 암호화 응답 → Node.js로 복호화 → 파싱
"""
import os
import logging
import subprocess
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

# SEED 복호화 Node.js 스크립트 경로
DECRYPT_SCRIPT = str(Path(__file__).parent / "decrypt_seed.js")
SEED_JS = str(Path(__file__).parent / "seedForNode.js")


def _decrypt_seed(encrypted_text: str) -> str:
    """Node.js를 통한 SEED 복호화"""
    if not encrypted_text or len(encrypted_text) < 20:
        return ""
    try:
        result = subprocess.run(
            ["node", DECRYPT_SCRIPT, encrypted_text],
            capture_output=True, text=True, timeout=10,
            cwd=str(Path(__file__).parent),
        )
        if result.returncode != 0:
            logger.error(f"[SmartLogen] SEED 복호화 오류: {result.stderr}")
            return ""
        return result.stdout.strip()
    except Exception as e:
        logger.error(f"[SmartLogen] SEED 복호화 예외: {e}")
        return ""


def _login(warehouse: str) -> Optional[urllib.request.OpenerDirector]:
    """SmartLogen 로그인 후 쿠키가 설정된 opener 반환"""
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

        logger.info(f"[SmartLogen] {warehouse} 로그인 성공")
        return opener

    except Exception as e:
        logger.error(f"[SmartLogen] {warehouse} 로그인 오류: {e}")
        return None


def _fetch_pick_snd_records(
    opener: urllib.request.OpenerDirector,
    warehouse: str,
    from_date: str,
    to_date: str,
    gubun: str = "D",  # D=배송, S=발송
) -> list[dict]:
    """
    PickSndRecordSelect API 호출 → 복호화 → 파싱

    반환 필드 (arrData):
    [0]=구분, [1]=집배일자, [2]=운송장번호, [3]=수량, [4]=운임구분,
    [5]=운임, [6]=산간료, [7]=해운료, [8]=제주운임, [9]=고객명,
    [10]=연락처, [11]=배송여부
    """
    acct = SMART_ACCOUNTS.get(warehouse)
    if not acct:
        return []

    params = urllib.parse.urlencode({
        "gubun": gubun,
        "branchCd": acct["branchCd"],
        "tradeCd": acct["userId"],
        "zjGb": "1",  # 전체
        "fromDate": from_date,
        "toDate": to_date,
        "unimGb": "1,2,3,4,5",  # 모든 운임 구분
    }).encode()

    try:
        resp = opener.open(
            f"{SMART_LOGEN_BASE}/PickSndRecordSelect",
            params, timeout=30,
        )
        encrypted = resp.read().decode("utf-8")

        if len(encrypted) <= 24:
            # 24바이트 이하 = 빈 응답 (패딩만)
            logger.info(f"[SmartLogen] {warehouse} {from_date}~{to_date} gubun={gubun}: 데이터 없음")
            return []

        decrypted = _decrypt_seed(encrypted)
        if not decrypted or "FALSE" in decrypted:
            error_msg = decrypted.replace("FALSE-", "").replace("Ξ≡", "") if decrypted else "복호화 실패"
            logger.warning(f"[SmartLogen] {warehouse} 조회 오류: {error_msg}")
            return []

        # 파싱: ≡ 구분자로 레코드 분리, 각 레코드는 Ξ로 필드 분리
        records = []
        raw_records = [r for r in decrypted.split("≡") if r.strip()]

        for raw in raw_records:
            fields = raw.split("Ξ")
            if len(fields) < 10:
                continue

            records.append({
                "warehouse": warehouse,
                "gubun": fields[0].strip(),        # 구분
                "take_dt": fields[1].strip(),       # 집배일자
                "slip_no": fields[2].strip(),       # 운송장번호
                "qty": fields[3].strip(),           # 수량
                "fare_type": fields[4].strip(),     # 운임구분
                "fare": fields[5].strip(),          # 운임
                "mountain_fee": fields[6].strip(),  # 산간료
                "sea_fee": fields[7].strip(),       # 해운료
                "jeju_fee": fields[8].strip(),      # 제주운임
                "rcv_name": fields[9].strip(),      # 고객명 (받는사람)
                "rcv_tel": fields[10].strip() if len(fields) > 10 else "",  # 연락처
                "status": fields[11].strip() if len(fields) > 11 else "",   # 배송여부
            })

        logger.info(f"[SmartLogen] {warehouse} {from_date}~{to_date}: {len(records)}건 조회")
        return records

    except Exception as e:
        logger.error(f"[SmartLogen] {warehouse} PickSndRecordSelect 오류: {e}")
        return []


async def fetch_shipments(
    warehouse: str = "",
    from_date: str = "",
    to_date: str = "",
    days: int = 7,
) -> list[dict]:
    """
    SmartLogen에서 발송 실적 가져오기 (동기 함수이지만 async 인터페이스 제공)

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
        days = min(days, 30)  # SmartLogen 최대 30일 제한
        from_dt = datetime.strptime(to_date, "%Y%m%d") - timedelta(days=days)
        from_date = from_dt.strftime("%Y%m%d")

    all_records = []

    # 조회할 창고 결정
    warehouses = [warehouse] if warehouse and warehouse in SMART_ACCOUNTS else list(SMART_ACCOUNTS.keys())

    for wh in warehouses:
        opener = _login(wh)
        if not opener:
            continue

        # 발송(S) 조회 → 가장 중요
        records_s = _fetch_pick_snd_records(opener, wh, from_date, to_date, gubun="S")
        all_records.extend(records_s)

        # 배송(D) 조회도 추가 — 배송 상태 확인용
        records_d = _fetch_pick_snd_records(opener, wh, from_date, to_date, gubun="D")
        # 중복 운송장번호 제거 (S에 이미 있는 건 제외)
        existing_slips = {r["slip_no"] for r in records_s}
        for r in records_d:
            if r["slip_no"] not in existing_slips:
                all_records.append(r)
                existing_slips.add(r["slip_no"])

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
            take_dt = rec.get("take_dt", "").replace("-", "").replace(".", "")[:8]
            qty_str = rec.get("qty", "1")
            qty = int(qty_str) if qty_str.isdigit() else 1
            status = rec.get("status", "")
            fare = rec.get("fare", "0")

            if not take_dt:
                take_dt = datetime.now().strftime("%Y%m%d")

            # UPSERT: 있으면 상태 업데이트, 없으면 삽입
            existing = conn.execute(
                "SELECT id FROM shipments WHERE slip_no = ? AND warehouse = ?",
                (slip_no, warehouse)
            ).fetchone()

            if existing:
                conn.execute(
                    "UPDATE shipments SET status = ?, rcv_tel = ? WHERE slip_no = ? AND warehouse = ?",
                    (status, rcv_tel, slip_no, warehouse)
                )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO shipments
                       (warehouse, slip_no, rcv_name, rcv_tel,
                        qty, take_dt, status, memo)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (warehouse, slip_no, rcv_name, rcv_tel,
                     qty, take_dt, status, f"운임:{fare}")
                )
            saved += 1

        conn.commit()
    except Exception as e:
        logger.error(f"[SmartLogen] DB 저장 오류: {e}")

    return saved
