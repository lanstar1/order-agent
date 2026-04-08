"""
선적 메일 파싱 서비스
- IMAP으로 메일서버 접속
- "Final shipping", "shipping list" 등 키워드 메일 검색
- BOR로 시작하는 엑셀 첨부파일 다운로드 → 모델명 파싱
- 오더리스트의 BOR 번호와 매칭 → 해당 품목들 선적 확인
- 선적일(메일 수신일) + 입고예정일(+8일) 저장
"""

import imaplib
import email
from email.header import decode_header
import logging
import io
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


def _decode_header_value(value):
    """메일 헤더 디코딩"""
    if not value:
        return ""
    decoded = decode_header(value)
    parts = []
    for part, charset in decoded:
        if isinstance(part, bytes):
            parts.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(str(part))
    return " ".join(parts)


def _parse_email_date(date_str):
    """메일 날짜 파싱 → KST datetime"""
    if not date_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.astimezone(KST)
    except Exception:
        return None


def scan_shipping_emails(
    imap_server: str,
    imap_user: str,
    imap_password: str,
    imap_port: int = 993,
    days_back: int = 90,
    search_folder: str = "INBOX",
) -> list:
    """
    IMAP 메일서버에서 선적 관련 메일을 검색하고 BOR 첨부파일을 파싱

    Returns: [{
        "bor_number": "BOR-2601001",
        "subject": "Final shipping list...",
        "email_date": "2026-03-15",
        "shipping_date": "2026-03-15",
        "arrival_date": "2026-03-23",
        "filename": "BOR-2601001.xlsx",
        "models": ["LS-1000H", "LS-1200HB", ...],
    }, ...]
    """
    results = []

    try:
        # IMAP 접속
        logger.info(f"[선적메일] IMAP 접속: {imap_server}:{imap_port}")
        mail = imaplib.IMAP4_SSL(imap_server, imap_port)
        mail.login(imap_user, imap_password)
        mail.select(search_folder, readonly=True)

        # 날짜 범위 설정
        since_date = (datetime.now(KST) - timedelta(days=days_back)).strftime("%d-%b-%Y")

        # 선적 관련 키워드로 검색
        keywords = ["Final shipping", "final shipping", "shipping list", "Shipping List", "shipping"]
        all_uids = set()

        for kw in keywords:
            try:
                status, data = mail.search(None, f'(SINCE {since_date} SUBJECT "{kw}")')
                if status == "OK" and data[0]:
                    uids = data[0].split()
                    all_uids.update(uids)
            except Exception as e:
                logger.debug(f"[선적메일] 키워드 '{kw}' 검색 실패: {e}")

        logger.info(f"[선적메일] 선적 관련 메일 {len(all_uids)}건 발견")

        for uid in all_uids:
            try:
                status, msg_data = mail.fetch(uid, "(RFC822)")
                if status != "OK":
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                subject = _decode_header_value(msg.get("Subject", ""))
                date_str = msg.get("Date", "")
                email_dt = _parse_email_date(date_str)

                if not email_dt:
                    continue

                email_date = email_dt.strftime("%Y-%m-%d")
                arrival_date = (email_dt + timedelta(days=8)).strftime("%Y-%m-%d")

                # 첨부파일 검색 (BOR로 시작하는 엑셀)
                for part in msg.walk():
                    content_disposition = str(part.get("Content-Disposition", ""))
                    if "attachment" not in content_disposition:
                        continue

                    filename = _decode_header_value(part.get_filename() or "")
                    if not filename:
                        continue

                    # BOR로 시작하는 엑셀 파일만
                    if not filename.upper().startswith("BOR"):
                        continue
                    if not any(filename.lower().endswith(ext) for ext in [".xlsx", ".xls", ".csv"]):
                        continue

                    # BOR 번호 추출 (파일명에서)
                    bor_match = re.match(r'(BOR[-_]?\d+)', filename, re.IGNORECASE)
                    bor_number = bor_match.group(1).upper() if bor_match else filename.split(".")[0].upper()
                    # BOR-2601001 형식으로 통일
                    bor_number = bor_number.replace("_", "-")

                    # 엑셀 파싱 → 모델명 추출
                    file_data = part.get_payload(decode=True)
                    models = _parse_bor_excel(file_data, filename)

                    if models:
                        results.append({
                            "bor_number": bor_number,
                            "subject": subject[:200],
                            "email_date": email_date,
                            "shipping_date": email_date,
                            "arrival_date": arrival_date,
                            "filename": filename,
                            "models": models,
                            "model_count": len(models),
                        })
                        logger.info(f"[선적메일] {bor_number}: {len(models)}개 모델 (선적일: {email_date})")

            except Exception as e:
                logger.warning(f"[선적메일] 메일 파싱 실패 (uid={uid}): {e}")

        mail.logout()

    except imaplib.IMAP4.error as e:
        logger.error(f"[선적메일] IMAP 접속 실패: {e}")
        raise Exception(f"메일 접속 실패: {e}")
    except Exception as e:
        logger.error(f"[선적메일] 메일 스캔 오류: {e}", exc_info=True)
        raise

    # BOR 번호 기준 중복 제거 (최신 메일 우선)
    seen_bors = {}
    for r in sorted(results, key=lambda x: x["email_date"], reverse=True):
        bor = r["bor_number"]
        if bor not in seen_bors:
            seen_bors[bor] = r
    results = list(seen_bors.values())

    logger.info(f"[선적메일] 총 {len(results)}건 BOR 선적 정보 파싱 완료")
    return results


def _parse_bor_excel(file_data: bytes, filename: str) -> list:
    """BOR 엑셀 첨부파일에서 모델명 추출"""
    models = []
    try:
        if filename.lower().endswith(".xlsx"):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file_data), data_only=True)
            sh = wb.active
            for r in range(1, sh.max_row + 1):
                for c in range(1, min(sh.max_column + 1, 10)):
                    val = str(sh.cell(r, c).value or "").strip()
                    # LS- 또는 LSP- 등 랜스타 모델명 패턴
                    if val and re.match(r'^(LS[- P]|LSN-|HT-)', val, re.IGNORECASE):
                        # 모델명만 추출 (쉼표 이전)
                        model = val.split(",")[0].strip()
                        if len(model) >= 4 and model not in models:
                            models.append(model)
        elif filename.lower().endswith(".xls"):
            import xlrd
            wb = xlrd.open_workbook(file_contents=file_data)
            sh = wb.sheet_by_index(0)
            for r in range(sh.nrows):
                for c in range(min(sh.ncols, 10)):
                    val = str(sh.cell_value(r, c)).strip()
                    if val and re.match(r'^(LS[- P]|LSN-|HT-)', val, re.IGNORECASE):
                        model = val.split(",")[0].strip()
                        if len(model) >= 4 and model not in models:
                            models.append(model)
    except Exception as e:
        logger.warning(f"[선적메일] 엑셀 파싱 실패 ({filename}): {e}")

    return models


# ─── DB 저장/조회 ─────────────────────────────────────────

def save_shipping_info(conn, shipping_data: list):
    """선적 정보를 DB에 저장"""
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    saved = 0
    for item in shipping_data:
        bor = item["bor_number"]
        models_json = ",".join(item["models"])
        conn.execute("""
            INSERT INTO shipping_mail_info
                (bor_number, subject, email_date, shipping_date, arrival_date,
                 filename, models, model_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bor_number) DO UPDATE SET
                subject=excluded.subject, email_date=excluded.email_date,
                shipping_date=excluded.shipping_date, arrival_date=excluded.arrival_date,
                filename=excluded.filename, models=excluded.models,
                model_count=excluded.model_count, updated_at=excluded.updated_at
        """, (bor, item["subject"], item["email_date"], item["shipping_date"],
              item["arrival_date"], item["filename"], models_json,
              item["model_count"], now))
        saved += 1
    conn.commit()
    logger.info(f"[선적메일] {saved}건 저장 완료")
    return saved


def get_shipping_info_map(conn) -> dict:
    """
    선적 정보를 모델명 기준으로 매핑
    Returns: {"LS-1000H": {"bor": "BOR-2601001", "shipping_date": "2026-03-15", "arrival_date": "2026-03-23"}}
    """
    rows = conn.execute("""
        SELECT bor_number, shipping_date, arrival_date, models
        FROM shipping_mail_info
        ORDER BY email_date DESC
    """).fetchall()

    model_map = {}
    for r in rows:
        bor = r[0]
        ship_date = r[1]
        arr_date = r[2]
        models_str = r[3] or ""
        for m in models_str.split(","):
            m = m.strip().upper()
            if m and m not in model_map:
                model_map[m] = {
                    "bor_number": bor,
                    "shipping_date": ship_date,
                    "arrival_date": arr_date,
                }
    return model_map


def get_all_shipping_info(conn) -> list:
    """전체 선적 정보 조회"""
    rows = conn.execute("""
        SELECT bor_number, subject, email_date, shipping_date, arrival_date,
               filename, models, model_count, updated_at
        FROM shipping_mail_info
        ORDER BY email_date DESC
    """).fetchall()
    return [dict(r) for r in rows]
