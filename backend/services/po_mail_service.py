"""
PO/PI 메일 자동 스캔 서비스

화이트리스트 등록된 거래처(예: T3 INDUSTRIAL - ianhung@livewire-asiapacific.com)의
PI(Proforma Invoice) PDF 첨부 메일을 자동으로 검색하여:
- Claude AI로 PDF 분석 → PO NO, 모델명/품명/수량/Delivery date 추출
- orderlist_items DB 저장
- 구글시트 오더리스트의 거래처별 탭에 자동 기록

PK(Packing List, 선적 통지) 메일은 발주가 아니라 이미 출고된 건이므로 제외한다.
"""

import base64
import email
import imaplib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_connection

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

ORDERLIST_SHEET_ID = "1ej0cxyM3NHJKpF-KBXbZ16fH-lZcUrr3Z3eTwFVFSco"

# ─────────────────────────────────────────
#  메일 헬퍼 (shipping_mail_service와 동일 패턴)
# ─────────────────────────────────────────

def _safe_fetch_body(fetch_result):
    if not fetch_result:
        return None
    for part in fetch_result:
        if isinstance(part, tuple) and len(part) >= 2 and isinstance(part[1], bytes):
            return part[1]
    return None


def _decode_header_value(value):
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


def _parse_email_date(date_str: str):
    if not date_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str)
    except Exception:
        return None


# ─────────────────────────────────────────
#  PK vs PI 판별
# ─────────────────────────────────────────

_PK_KEYWORDS = ("PACKING", "PACKING LIST", "PK", "FINAL SHIPPING", "SHIPPING LIST", "SHIPPING ADVICE")
_PI_KEYWORDS = ("PROFORMA", "PROFORMA INVOICE", "PI", "P.I.", "PO.", "PURCHASE ORDER")


def is_pi_document(filename: str, subject: str = "") -> bool:
    """PI(발주) 문서인지 판별. PK 키워드가 있으면 무조건 제외."""
    fn = (filename or "").upper()
    sub = (subject or "").upper()

    # PK 키워드 우선 검사 (배제)
    for kw in _PK_KEYWORDS:
        # 파일명에 PK가 단어로 나타나는 경우 (PK., PK_, -PK, PK-)
        if kw == "PK":
            if re.search(r'(^|[^A-Z])PK([^A-Z]|$)', fn):
                return False
        elif kw in fn:
            return False
        if kw in sub:
            return False

    # PI 키워드 매칭
    for kw in _PI_KEYWORDS:
        if kw == "PI":
            if re.search(r'(^|[^A-Z])PI([^A-Z]|$)', fn):
                return True
        elif kw in fn:
            return True
        if kw in sub:
            return True

    return False


# ─────────────────────────────────────────
#  Claude API로 PI PDF 분석
# ─────────────────────────────────────────

_PI_EXTRACTION_PROMPT = """You are extracting purchase order data from a PI (Proforma Invoice) or PO (Purchase Order) document.

Extract the following fields and return ONLY valid JSON (no markdown, no explanation):

{
  "po_number": "purchase order number (look for 'PO NO.', 'Order No.', 'P/O No.')",
  "pi_number": "proforma invoice number (look for 'PI NO', 'Invoice No.', 'P.I. No.')",
  "order_date": "PI issue date in YYYY-MM-DD format",
  "delivery_date": "expected delivery date in YYYY-MM-DD format (look for 'Delivery date', 'ETD', 'Ship date')",
  "supplier_name": "shipper/exporter company name",
  "items": [
    {
      "part_no": "supplier part number (e.g., '6IC-FJJHNMT-(LS)-1P')",
      "model_name": "OUR model name in square brackets (e.g., 'LS-6IC-FJM' from '[LS-6IC-FJM] Description'). If no [LS-XXX] format, leave empty",
      "description": "item description without the [LS-XXX] prefix",
      "qty": 3000,
      "unit": "PC or BAG or PCS",
      "unit_price": 1.23,
      "amount": 3690.00
    }
  ]
}

CRITICAL RULES:
- model_name MUST be the text inside [square brackets] starting with LS-. If brackets contain something other than LS-prefix, leave model_name empty.
- qty must be an integer (remove commas, units).
- Dates: convert "2026/05/11" → "2026-05-11", "May 11, 2026" → "2026-05-11".
- If a field is missing, use empty string "" for strings or 0 for numbers.
- Return ONLY the JSON object, no other text."""


def extract_pi_with_claude(pdf_bytes: bytes, filename: str = "") -> dict:
    """
    Claude Sonnet으로 PI PDF 분석 → 구조화된 발주 데이터 추출
    Returns: {po_number, pi_number, order_date, delivery_date, supplier_name, items: [...]}
    """
    import anthropic

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY 환경변수 없음")

    client = anthropic.Anthropic()
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    logger.info(f"[PO메일] Claude API PDF 분석 시작: {filename} ({len(pdf_bytes)} bytes)")

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_PI_EXTRACTION_PROMPT,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": pdf_b64,
                        },
                    },
                    {"type": "text", "text": f"파일명: {filename}\n위 PI/PO 문서를 분석해 JSON으로 반환해줘."},
                ],
            }],
        )
    except Exception as e:
        logger.error(f"[PO메일] Claude API 호출 실패: {e}")
        raise

    raw_text = ""
    for blk in resp.content:
        if getattr(blk, "type", "") == "text":
            raw_text += blk.text

    # JSON 추출 (마크다운 펜스 제거)
    cleaned = raw_text.strip()
    cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
    cleaned = re.sub(r'\s*```$', '', cleaned)

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        # JSON 객체만 추출 시도
        m = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
        else:
            logger.error(f"[PO메일] JSON 파싱 실패. 응답: {raw_text[:500]}")
            raise

    # 필드 정규화
    items = data.get("items", []) or []
    for it in items:
        # 모델명 정규화: 대문자 + 공백 제거
        it["model_name"] = str(it.get("model_name", "")).strip().upper()
        it["part_no"] = str(it.get("part_no", "")).strip()
        it["description"] = str(it.get("description", "")).strip()
        it["unit"] = str(it.get("unit", "")).strip() or "PCS"
        # 수량 정규화
        try:
            qty_raw = it.get("qty", 0)
            it["qty"] = int(float(str(qty_raw).replace(",", "")))
        except (ValueError, TypeError):
            it["qty"] = 0

    logger.info(f"[PO메일] Claude 분석 완료: PO={data.get('po_number')}, 품목 {len(items)}개")
    return data


# ─────────────────────────────────────────
#  화이트리스트 관리
# ─────────────────────────────────────────

def list_po_senders(conn=None, active_only: bool = False) -> list:
    """등록된 PO 발신자 목록"""
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    try:
        where = "WHERE is_active = 1" if active_only else ""
        rows = conn.execute(f"""
            SELECT id, email, supplier_name, sheet_tab_prefix, is_active,
                   last_scanned_at, last_scanned_count, created_at, updated_at
            FROM po_mail_senders {where}
            ORDER BY supplier_name ASC, email ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        if close:
            conn.close()


def add_po_sender(email_addr: str, supplier_name: str = "", sheet_tab_prefix: str = "") -> int:
    """PO 발신자 등록"""
    conn = get_connection()
    try:
        cur = conn.execute("""
            INSERT INTO po_mail_senders (email, supplier_name, sheet_tab_prefix, is_active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(email) DO UPDATE SET
                supplier_name = excluded.supplier_name,
                sheet_tab_prefix = excluded.sheet_tab_prefix,
                is_active = 1,
                updated_at = CURRENT_TIMESTAMP
        """, (email_addr.strip().lower(), supplier_name.strip(), sheet_tab_prefix.strip()))
        conn.commit()
        return cur.lastrowid or 0
    finally:
        conn.close()


def update_po_sender(sender_id: int, **kwargs) -> None:
    allowed = {"email", "supplier_name", "sheet_tab_prefix", "is_active"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = ?" for k in updates) + ", updated_at = CURRENT_TIMESTAMP"
    conn = get_connection()
    try:
        conn.execute(
            f"UPDATE po_mail_senders SET {set_clause} WHERE id = ?",
            list(updates.values()) + [sender_id]
        )
        conn.commit()
    finally:
        conn.close()


def remove_po_sender(sender_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM po_mail_senders WHERE id = ?", (sender_id,))
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────
#  메일 스캔
# ─────────────────────────────────────────

def scan_po_emails_for_sender(
    imap_server: str, imap_user: str, imap_password: str,
    sender_email: str, supplier_name: str = "",
    imap_port: int = 993, days_back: int = 90,
) -> list:
    """
    특정 발신자의 PO/PI PDF 메일 스캔
    Returns: [{
        "sender_email": str, "supplier_name": str,
        "message_uid": str, "subject": str, "email_date": "YYYY-MM-DD",
        "filename": str, "pdf_bytes": bytes
    }, ...]
    """
    results = []

    try:
        logger.info(f"[PO메일] IMAP 접속: {imap_server}:{imap_port} ({imap_user}) — 발신자 {sender_email}")
        mail = imaplib.IMAP4_SSL(imap_server, imap_port)
        mail.login(imap_user, imap_password)
        mail.select("INBOX", readonly=True)

        since_date = (datetime.now(KST) - timedelta(days=days_back)).strftime("%d-%b-%Y")
        cutoff = datetime.now(KST) - timedelta(days=days_back)

        # UID 기반 SEARCH (Ecount/일반 IMAP 호환)
        try:
            status, data = mail.uid("search", None, f'(SINCE {since_date} FROM "{sender_email}")')
        except Exception:
            status, data = mail.uid("search", None, f"(SINCE {since_date})")

        all_uids = data[0].split() if status == "OK" and data[0] else []
        logger.info(f"[PO메일] {sender_email}: UID {len(all_uids)}건 (SINCE {since_date})")

        for uid in reversed(all_uids):  # 최신 우선
            try:
                # 1단계: 헤더만 빠르게 확인
                status, hdr_data = mail.uid("fetch", uid, "(RFC822.HEADER)")
                if status != "OK" or not hdr_data or not hdr_data[0]:
                    continue
                hdr_raw = _safe_fetch_body(hdr_data)
                if not hdr_raw:
                    continue
                hdr_msg = email.message_from_bytes(hdr_raw)

                # 날짜 필터
                email_dt = _parse_email_date(hdr_msg.get("Date", ""))
                if not email_dt:
                    continue
                if email_dt.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
                    logger.info(f"[PO메일] {sender_email} {email_dt.strftime('%Y-%m-%d')} → 기간 초과, 중단")
                    break

                # 발신자 필터 (FROM SEARCH가 안 통하는 서버 대비 재확인)
                msg_from = _decode_header_value(hdr_msg.get("From", "")).lower()
                if sender_email.lower() not in msg_from:
                    continue

                subject = _decode_header_value(hdr_msg.get("Subject", ""))

                # 2단계: 매칭된 메일 전체 fetch
                status, msg_data = mail.uid("fetch", uid, "(RFC822)")
                if status != "OK" or not msg_data or not msg_data[0]:
                    continue
                raw = _safe_fetch_body(msg_data)
                if not raw:
                    continue
                msg = email.message_from_bytes(raw)

                # PDF 첨부 추출
                for part in msg.walk():
                    content_disposition = str(part.get("Content-Disposition", ""))
                    if "attachment" not in content_disposition:
                        continue

                    filename = _decode_header_value(part.get_filename() or "")
                    if not filename or not filename.lower().endswith(".pdf"):
                        continue

                    # PI vs PK 판별 (PK는 제외)
                    if not is_pi_document(filename, subject):
                        logger.debug(f"[PO메일] PK/비PI 제외: {filename}")
                        continue

                    pdf_bytes = part.get_payload(decode=True)
                    if not pdf_bytes:
                        continue

                    uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
                    results.append({
                        "sender_email": sender_email,
                        "supplier_name": supplier_name,
                        "message_uid": uid_str,
                        "subject": subject[:200],
                        "email_date": email_dt.strftime("%Y-%m-%d"),
                        "filename": filename,
                        "pdf_bytes": pdf_bytes,
                    })
                    logger.info(f"[PO메일] PI 발견: {filename} ({email_dt.strftime('%Y-%m-%d')})")

            except Exception as e:
                logger.warning(f"[PO메일] 메일 파싱 실패 uid={uid}: {e}")

        mail.logout()

    except Exception as e:
        logger.error(f"[PO메일] {sender_email} 스캔 오류: {e}", exc_info=True)
        raise

    return results


# ─────────────────────────────────────────
#  중복 체크 + DB 저장
# ─────────────────────────────────────────

def is_already_imported(conn, sender_email: str, message_uid: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM po_mail_imports WHERE sender_email = ? AND message_uid = ? LIMIT 1",
        (sender_email, message_uid)
    ).fetchone()
    return row is not None


def save_po_to_orderlist(conn, mail_meta: dict, extracted: dict, sheet_tab: str) -> int:
    """
    추출된 PI 데이터를 orderlist_items 테이블에 저장
    Returns: 저장된 품목 수
    """
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    po_number = (extracted.get("po_number") or extracted.get("pi_number") or "").strip()
    pi_number = (extracted.get("pi_number") or "").strip()
    order_date = extracted.get("order_date") or mail_meta.get("email_date", "")
    delivery_date = extracted.get("delivery_date", "")
    supplier_name = mail_meta.get("supplier_name", "") or extracted.get("supplier_name", "")
    items = extracted.get("items", []) or []

    # 동일 PO 기존 데이터 삭제 후 재등록 (중복 방지)
    if po_number:
        conn.execute(
            "DELETE FROM orderlist_items WHERE sheet_tab = ? AND order_no = ?",
            (sheet_tab, po_number)
        )

    saved_count = 0
    for idx, it in enumerate(items):
        model = it.get("model_name", "")
        if not model:
            # 모델명 없으면 part_no를 fallback으로 사용
            model = it.get("part_no", "")
        if not model:
            continue

        description = it.get("description", "")
        qty = int(it.get("qty", 0) or 0)
        unit = it.get("unit", "PCS")

        # delivery_date → shipping_date 컬럼에 저장 (입고예정용)
        conn.execute("""
            INSERT INTO orderlist_items
                (sheet_tab, order_no, seller, order_date, category,
                 model_name, description, qty, unit, unit_price,
                 total_value, row_index, raw_row, synced_at, shipping_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sheet_tab, po_number, supplier_name,
            order_date, "",
            model, description, qty, unit,
            str(it.get("unit_price", "")), str(it.get("amount", "")),
            idx + 1, "", now,
            delivery_date,
        ))
        saved_count += 1

    return saved_count


def log_po_import(conn, mail_meta: dict, extracted: dict, sheet_tab: str,
                  item_count: int, status: str = "success", error: str = "",
                  source: str = "mail") -> None:
    """PO 메일 자동 등록 이력 저장"""
    conn.execute("""
        INSERT INTO po_mail_imports
            (sender_email, message_uid, subject, email_date, filename,
             po_number, pi_number, delivery_date, sheet_tab,
             item_count, status, error_message, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        mail_meta.get("sender_email", ""),
        mail_meta.get("message_uid", ""),
        mail_meta.get("subject", ""),
        mail_meta.get("email_date", ""),
        mail_meta.get("filename", ""),
        extracted.get("po_number", ""),
        extracted.get("pi_number", ""),
        extracted.get("delivery_date", ""),
        sheet_tab,
        item_count,
        status,
        error[:500],
        source,
    ))


# ─────────────────────────────────────────
#  구글시트 자동 등록 (NAM 패턴 재사용)
# ─────────────────────────────────────────

def _get_sheets_credentials():
    import json as _json
    from google.oauth2.service_account import Credentials

    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        return None
    try:
        sa_info = _json.loads(sa_json)
        return Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    except Exception as e:
        logger.error(f"[구글시트] 인증 실패: {e}")
        return None


def _sheets_api_request(method: str, url: str, body=None):
    import httpx
    from google.auth.transport.requests import Request

    creds = _get_sheets_credentials()
    if not creds:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 환경변수 없음")
    creds.refresh(Request())

    headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}
    if method == "GET":
        resp = httpx.get(url, headers=headers, timeout=30)
    elif method == "POST":
        resp = httpx.post(url, headers=headers, json=body or {}, timeout=30)
    elif method == "PUT":
        resp = httpx.put(url, headers=headers, json=body or {}, timeout=30)
    else:
        raise ValueError(f"Unsupported method: {method}")
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def write_po_to_sheet(scan_results: list, sheet_tab_prefix: str = "PO") -> dict:
    """
    PO 메일 결과를 구글시트의 거래처별 탭에 추가/덮어쓰기

    scan_results: [{"mail_meta": {...}, "extracted": {...}}, ...]
    sheet_tab_prefix: 탭 이름 prefix (예: "T3" → "T3-2026")

    BOR/NAM 패턴과 동일한 헤더 구조:
      Seller: | <PO_NO>
      <공급사명> | Date: | <발주일>
      Item | Description | Quantity | Unit | Order Date | Shipping Date | Arrival Date
      ... 품목들 ...
    """
    if not scan_results:
        return {"status": "no_data"}

    year = datetime.now(KST).strftime("%Y")
    tab_name = f"{sheet_tab_prefix}-{year}" if sheet_tab_prefix else f"PO-{year}"
    base_url = f"https://sheets.googleapis.com/v4/spreadsheets/{ORDERLIST_SHEET_ID}"

    try:
        # 1. 탭 존재 여부 확인 + 생성
        sheet_meta = _sheets_api_request("GET", f"{base_url}?fields=sheets.properties")
        existing_tabs = [s["properties"]["title"] for s in sheet_meta.get("sheets", [])]
        if tab_name not in existing_tabs:
            _sheets_api_request("POST", f"{base_url}:batchUpdate", {
                "requests": [{"addSheet": {"properties": {"title": tab_name}}}]
            })
            logger.info(f"[구글시트] '{tab_name}' 탭 신규 생성")

        # 2. 기존 데이터 클리어
        _sheets_api_request("POST", f"{base_url}/values/'{tab_name}'!A1:Z2000:clear", {})

        # 3. 데이터 행 구성
        rows = []
        for r in scan_results:
            mail_meta = r["mail_meta"]
            ex = r["extracted"]
            po_number = ex.get("po_number", "") or ex.get("pi_number", "")
            supplier = mail_meta.get("supplier_name", "") or ex.get("supplier_name", "")
            order_date = ex.get("order_date") or mail_meta.get("email_date", "")
            delivery_date = ex.get("delivery_date", "")

            rows.append(["Seller:", "", "No.:", po_number])
            rows.append([supplier, "", "Date:", order_date])
            if delivery_date:
                rows.append(["Delivery:", delivery_date, "PI:", ex.get("pi_number", "")])
            rows.append(["Item", "Description", "Quantity", "Unit",
                         "Order Date", "Shipping Date", "Arrival Date"])

            for it in ex.get("items", []):
                model = it.get("model_name") or it.get("part_no", "")
                rows.append([
                    model,
                    it.get("description", ""),
                    it.get("qty", 0),
                    it.get("unit", "PCS"),
                    order_date,
                    delivery_date,  # 입고예정 = delivery_date
                    "",
                ])
            rows.append([])  # 빈 행 구분

        # 4. 시트에 쓰기
        _sheets_api_request("PUT",
            f"{base_url}/values/'{tab_name}'!A1?valueInputOption=USER_ENTERED",
            {"values": rows}
        )

        logger.info(f"[구글시트] '{tab_name}'에 {len(scan_results)}건 PO 기록 완료 ({len(rows)}행)")
        return {"status": "ok", "tab": tab_name, "rows": len(rows), "po_count": len(scan_results)}

    except Exception as e:
        logger.error(f"[구글시트] PO 기록 실패: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


# ─────────────────────────────────────────
#  엔드 투 엔드: 전체 발신자 스캔 + 처리
# ─────────────────────────────────────────

def run_po_mail_scan_all(
    imap_server: str, imap_user: str, imap_password: str,
    imap_port: int = 993, days_back: int = 90,
    skip_duplicates: bool = True,
) -> dict:
    """
    화이트리스트의 모든 활성 발신자에 대해 PO 메일 스캔 → DB + 구글시트 저장
    """
    senders = list_po_senders(active_only=True)
    if not senders:
        return {"status": "no_senders", "total_senders": 0}

    total_imported = 0
    total_skipped = 0
    total_failed = 0
    per_sender_results = []

    conn = get_connection()
    try:
        # 발신자별로 메일 스캔
        for sender in senders:
            email_addr = sender["email"]
            supplier_name = sender["supplier_name"]
            sheet_prefix = sender["sheet_tab_prefix"] or "PO"
            sheet_tab = f"{sheet_prefix}-{datetime.now(KST).strftime('%Y')}"

            try:
                mails = scan_po_emails_for_sender(
                    imap_server, imap_user, imap_password,
                    email_addr, supplier_name, imap_port, days_back
                )
            except Exception as e:
                logger.error(f"[PO메일] {email_addr} 스캔 실패: {e}")
                per_sender_results.append({
                    "email": email_addr, "supplier": supplier_name,
                    "scanned": 0, "imported": 0, "skipped": 0, "failed": 1,
                    "error": str(e),
                })
                total_failed += 1
                continue

            sender_imported = 0
            sender_skipped = 0
            sender_failed = 0
            sheet_payload = []

            for mail_meta in mails:
                # 중복 체크
                if skip_duplicates and is_already_imported(
                    conn, mail_meta["sender_email"], mail_meta["message_uid"]
                ):
                    sender_skipped += 1
                    continue

                # Claude로 PDF 분석
                try:
                    extracted = extract_pi_with_claude(
                        mail_meta["pdf_bytes"], mail_meta["filename"]
                    )
                except Exception as e:
                    logger.error(f"[PO메일] PDF 분석 실패 ({mail_meta['filename']}): {e}")
                    log_po_import(conn, mail_meta, {}, sheet_tab, 0,
                                  status="failed", error=str(e))
                    sender_failed += 1
                    continue

                # DB 저장
                try:
                    item_count = save_po_to_orderlist(conn, mail_meta, extracted, sheet_tab)
                    log_po_import(conn, mail_meta, extracted, sheet_tab,
                                  item_count, status="success")
                    sender_imported += 1

                    # 구글시트용 페이로드 누적
                    sheet_payload.append({"mail_meta": mail_meta, "extracted": extracted})
                except Exception as e:
                    logger.error(f"[PO메일] DB 저장 실패: {e}", exc_info=True)
                    log_po_import(conn, mail_meta, extracted, sheet_tab, 0,
                                  status="failed", error=str(e))
                    sender_failed += 1

            conn.commit()

            # 구글시트 동기화 (발신자 단위로 탭에 누적)
            sheet_result = {}
            if sheet_payload:
                try:
                    # 기존 동일 탭 데이터에 PI들 누적 (모든 활성 PI 다시 쓰기)
                    # 현재 발신자의 모든 임포트된 PO를 다시 모아서 시트에 기록
                    all_imports = conn.execute("""
                        SELECT pi.po_number, pi.pi_number, pi.delivery_date,
                               pi.email_date, pi.subject, pi.filename
                        FROM po_mail_imports pi
                        WHERE pi.sender_email = ? AND pi.status = 'success'
                        ORDER BY pi.email_date DESC
                        LIMIT 20
                    """, (email_addr,)).fetchall()

                    # 발신자가 등록된 모든 품목을 orderlist_items에서 다시 조회
                    rebuild_payload = []
                    for imp in all_imports:
                        po_no = imp[0]
                        items_rows = conn.execute("""
                            SELECT model_name, description, qty, unit, shipping_date, order_date
                            FROM orderlist_items
                            WHERE sheet_tab = ? AND order_no = ?
                            ORDER BY row_index ASC
                        """, (sheet_tab, po_no)).fetchall()

                        if not items_rows:
                            continue

                        rebuild_payload.append({
                            "mail_meta": {
                                "supplier_name": supplier_name,
                                "email_date": imp[3], "subject": imp[4],
                                "filename": imp[5],
                            },
                            "extracted": {
                                "po_number": po_no,
                                "pi_number": imp[1],
                                "delivery_date": imp[2],
                                "order_date": imp[5] if not imp[3] else imp[3],
                                "supplier_name": supplier_name,
                                "items": [
                                    {
                                        "model_name": r[0],
                                        "description": r[1],
                                        "qty": r[2],
                                        "unit": r[3] or "PCS",
                                    } for r in items_rows
                                ],
                            },
                        })

                    if rebuild_payload:
                        sheet_result = write_po_to_sheet(rebuild_payload, sheet_prefix)
                except Exception as e:
                    logger.error(f"[PO메일] 구글시트 동기화 실패 ({email_addr}): {e}", exc_info=True)
                    sheet_result = {"status": "error", "error": str(e)}

            # 발신자 last_scanned 업데이트
            conn.execute("""
                UPDATE po_mail_senders
                SET last_scanned_at = ?, last_scanned_count = ?, updated_at = CURRENT_TIMESTAMP
                WHERE email = ?
            """, (
                datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                sender_imported + sender_skipped,
                email_addr,
            ))
            conn.commit()

            per_sender_results.append({
                "email": email_addr, "supplier": supplier_name,
                "scanned": len(mails),
                "imported": sender_imported,
                "skipped": sender_skipped,
                "failed": sender_failed,
                "sheet": sheet_result,
            })
            total_imported += sender_imported
            total_skipped += sender_skipped
            total_failed += sender_failed

    finally:
        conn.close()

    return {
        "status": "ok",
        "total_senders": len(senders),
        "total_imported": total_imported,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "per_sender": per_sender_results,
    }


# ─────────────────────────────────────────
#  WeChat 등 수동 업로드 처리
# ─────────────────────────────────────────

def import_po_from_upload(
    pdf_bytes: bytes, filename: str,
    sender_email: str = "wechat",
    supplier_name: str = "",
    sheet_tab_prefix: str = "WECHAT",
    source: str = "wechat",
) -> dict:
    """
    수동 업로드된 PI PDF를 분석 → DB + 구글시트 저장
    WeChat 등 메일 외 채널로 받은 발주서용
    """
    if not is_pi_document(filename):
        # 수동 업로드는 사용자가 의도적으로 올린 거니까 PK 키워드 있어도 일단 분석
        logger.info(f"[PO수동] 파일명에 PI 키워드 없지만 진행: {filename}")

    extracted = extract_pi_with_claude(pdf_bytes, filename)

    conn = get_connection()
    try:
        year = datetime.now(KST).strftime("%Y")
        sheet_tab = f"{sheet_tab_prefix}-{year}"

        mail_meta = {
            "sender_email": sender_email,
            "supplier_name": supplier_name,
            "message_uid": f"upload-{datetime.now(KST).strftime('%Y%m%d%H%M%S')}",
            "subject": f"수동 업로드: {filename}",
            "email_date": datetime.now(KST).strftime("%Y-%m-%d"),
            "filename": filename,
        }

        item_count = save_po_to_orderlist(conn, mail_meta, extracted, sheet_tab)
        log_po_import(conn, mail_meta, extracted, sheet_tab, item_count,
                      status="success", source=source)
        conn.commit()

        # 구글시트 등록
        sheet_result = {}
        try:
            sheet_result = write_po_to_sheet(
                [{"mail_meta": mail_meta, "extracted": extracted}],
                sheet_tab_prefix
            )
        except Exception as e:
            logger.error(f"[PO수동] 구글시트 등록 실패: {e}")
            sheet_result = {"status": "error", "error": str(e)}

        return {
            "status": "ok",
            "po_number": extracted.get("po_number", ""),
            "pi_number": extracted.get("pi_number", ""),
            "delivery_date": extracted.get("delivery_date", ""),
            "item_count": item_count,
            "items": extracted.get("items", []),
            "sheet": sheet_result,
        }
    finally:
        conn.close()
