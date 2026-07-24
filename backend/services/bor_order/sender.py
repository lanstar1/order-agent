"""
중국오더(BOR) 모듈 - 발주 메일 발송 (SMTP)

승인된 초안(bor_order_drafts)의 xlsx를 첨부하여 표준 문구로 발송한다.
SMTP: wsmtp.ecount.com:587 STARTTLS (config MAIL_SMTP_HOST/PORT, MAIL_USER/MAIL_PASSWORD)

★ 자동 발송 금지 — 이 함수는 반드시 사용자의 명시적 승인(confirm) 후에만
  라우트에서 호출되어야 한다. 스케줄러에서 직접 호출하지 말 것.
"""
import base64
import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.database import get_connection, now_kst
from config import MAIL_SMTP_HOST, MAIL_SMTP_PORT, MAIL_USER, MAIL_PASSWORD
from services.bor_order import db as bor_db

logger = logging.getLogger(__name__)

# 표준 발송 문구 (kyu 발신 관례에 맞춤)
BODY_TEMPLATE = (
    "Dear Mr. Gu,\n"
    "\n"
    "We will order the items in the attached file.\n"
    "Please check the attached new order list.\n"
    "{note}"
    "\n"
    "Thank you.\n"
    "\n"
    "Best regards,\n"
    "LINEUP SYSTEM CO., LTD.\n"
)


def send_order_mail(draft_id: int, recipient: str, cc: str, body_note: str = "") -> dict:
    """발주 초안 메일 발송 → 성공 시 draft status='sent' 갱신

    Returns: {success, message_id?, sent_to?, error?}
    """
    bor_db.ensure_tables()

    if not MAIL_USER or not MAIL_PASSWORD:
        return {"success": False, "error": "메일 계정(MAIL_USER/MAIL_PASSWORD) 미설정"}
    if not recipient:
        return {"success": False, "error": "수신자(recipient) 미지정"}

    # ── 초안 조회 ──
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, status, xlsx_filename, xlsx_b64 FROM bor_order_drafts WHERE id = ?",
            (draft_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return {"success": False, "error": f"초안 #{draft_id} 없음"}
    status = str(row[1] or "")
    if status == "sent":
        return {"success": False, "error": f"초안 #{draft_id}은 이미 발송됨"}
    if status == "discarded":
        return {"success": False, "error": f"초안 #{draft_id}은 폐기 상태"}
    xlsx_filename = row[2] or f"new order-{draft_id}.xlsx"
    xlsx_b64 = row[3] or ""
    if not xlsx_b64:
        return {"success": False, "error": "첨부할 xlsx가 없음 — 초안을 다시 생성하세요"}

    try:
        xlsx_bytes = base64.b64decode(xlsx_b64)
    except Exception as e:
        return {"success": False, "error": f"xlsx 디코딩 실패: {e}"}

    # ── 메시지 구성 ──
    note = f"\n{body_note.strip()}\n" if body_note.strip() else ""
    body = BODY_TEMPLATE.format(note=note)

    msg = MIMEMultipart()
    msg["From"] = MAIL_USER
    msg["To"] = recipient
    if cc:
        msg["Cc"] = cc
    msg["Subject"] = "new order"
    msg["Date"] = formatdate(localtime=True)
    message_id = make_msgid()
    msg["Message-ID"] = message_id
    msg.attach(MIMEText(body, "plain", "utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(xlsx_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{xlsx_filename}"')
    msg.attach(part)

    recipients = [addr.strip() for addr in ([recipient] + (cc.split(",") if cc else [])) if addr.strip()]

    # ── SMTP 발송 (STARTTLS) ──
    try:
        with smtplib.SMTP(MAIL_SMTP_HOST, MAIL_SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(MAIL_USER, MAIL_PASSWORD)
            server.sendmail(MAIL_USER, recipients, msg.as_string())
    except Exception as e:
        logger.error(f"[BOR/발송] SMTP 실패 (draft #{draft_id}): {e}")
        return {"success": False, "error": f"SMTP 발송 실패: {e}"}

    # ── 발송 상태 갱신 (잠김 재시도 — 갱신 실패 시 중복 발송 위험) ──
    sent_at = now_kst()
    bor_db.write_with_retry(lambda conn: conn.execute(
        "UPDATE bor_order_drafts "
        "SET status = 'sent', sent_at = ?, sent_to = ?, mail_message_id = ? "
        "WHERE id = ?",
        (sent_at, ", ".join(recipients), message_id, draft_id),
    ))

    logger.info(f"[BOR/발송] 초안 #{draft_id} 발송 완료 → {recipients} ({xlsx_filename})")
    return {
        "success": True,
        "message_id": message_id,
        "sent_to": ", ".join(recipients),
        "sent_at": sent_at,
        "filename": xlsx_filename,
    }
