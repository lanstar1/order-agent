"""메일 디버그 — 실제 메일 내용 fetch"""
import imaplib
import email as email_mod
from email.header import decode_header
from fastapi import APIRouter
from datetime import datetime, timedelta

router = APIRouter(prefix="/api/debug-mail", tags=["debug"])


def _dh(v):
    if not v:
        return ""
    decoded = decode_header(v)
    parts = []
    for p, c in decoded:
        if isinstance(p, bytes):
            parts.append(p.decode(c or "utf-8", errors="replace"))
        else:
            parts.append(str(p))
    return " ".join(parts)


@router.get("/fetch")
async def debug_fetch_emails():
    """실제 메일 내용 확인 (발신자, 제목, 첨부파일명)"""
    results = {}

    # Ecount — 전체 메일 fetch
    try:
        from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD
        mail = imaplib.IMAP4_SSL(MAIL_IMAP_SERVER, MAIL_IMAP_PORT)
        mail.login(MAIL_USER, MAIL_PASSWORD)
        mail.select("INBOX", readonly=True)
        status, data = mail.search(None, "ALL")
        uids = data[0].split() if data[0] else []
        emails = []
        for uid in uids[:15]:
            status, msg_data = mail.fetch(uid, "(RFC822)")
            msg = email_mod.message_from_bytes(msg_data[0][1])
            attachments = []
            for part in msg.walk():
                fn = _dh(part.get_filename() or "")
                if fn:
                    attachments.append(fn)
            emails.append({
                "from": _dh(msg.get("From", ""))[:80],
                "subject": _dh(msg.get("Subject", ""))[:80],
                "date": msg.get("Date", "")[:30],
                "attachments": attachments,
            })
        mail.logout()
        results["ecount_emails"] = emails
    except Exception as e:
        results["ecount_error"] = str(e)

    # Naver — 각 폴더에서 163.com 발신자 검색
    try:
        from config import MAIL2_IMAP_SERVER, MAIL2_IMAP_PORT, MAIL2_USER, MAIL2_PASSWORD
        mail = imaplib.IMAP4_SSL(MAIL2_IMAP_SERVER, MAIL2_IMAP_PORT)
        mail.login(MAIL2_USER, MAIL2_PASSWORD)

        status, folder_list = mail.list()
        folder_results = {}

        for f in (folder_list or [])[:25]:
            try:
                decoded = f.decode("utf-8", errors="replace")
                # 폴더명 추출
                parts = decoded.rsplit('"', 2)
                folder_name = parts[-2] if len(parts) >= 3 else "INBOX"

                st, _ = mail.select('"' + folder_name + '"', readonly=True)
                if st != "OK":
                    continue
                status, data = mail.search(None, "ALL")
                uids = data[0].split() if data[0] else []
                total = len(uids)

                if not uids:
                    continue

                # 최근 5개 FROM 확인
                found_163 = 0
                samples = []
                for uid in uids[-5:]:
                    try:
                        st2, md = mail.fetch(uid, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])")
                        header = md[0][1].decode("utf-8", errors="replace")
                        if "163.com" in header:
                            found_163 += 1
                            samples.append(header.strip()[:150])
                    except Exception:
                        pass

                folder_results[folder_name] = {
                    "total": total,
                    "found_163": found_163,
                    "samples": samples,
                }
            except Exception:
                pass

        mail.logout()
        results["naver_folders"] = folder_results
    except Exception as e:
        results["naver_error"] = str(e)

    return results
