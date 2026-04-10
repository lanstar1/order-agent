"""
메일 자동화 API 라우터
- GET  /api/mail-auto/dashboard    대시보드 데이터
- GET  /api/mail-auto/logs         처리 로그
- POST /api/mail-auto/trigger      수동 실행
- POST /api/mail-auto/process-file 업로드 Excel HS코드 처리 (테스트용)
- GET  /api/mail-auto/exchange-rate 환율 조회
- POST /api/mail-auto/exchange-rate 환율 수동 설정
- GET  /api/mail-auto/oem-products  OEM 미매핑 제품
- POST /api/mail-auto/oem-products  OEM 품목코드 매핑
- POST /api/mail-auto/auth          비밀번호 인증
"""

import logging
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query, Request
from fastapi.responses import StreamingResponse
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_connection
from services.mail_auto_service import (
    fetch_bor_emails, process_excel_hs_code, create_purchase_slip,
    fetch_exchange_rate, run_mail_automation_pipeline,
    get_auto_state, start_mail_auto_scheduler, stop_mail_auto_scheduler,
    MAIL_AUTO_PASSWORD, ERP_SUPPLIER_CODE,
)

router = APIRouter(prefix="/api/mail-auto", tags=["mail-auto"])
logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

# 메모리 캐시 (세션 비밀번호 검증 대용 - 간단 구현)
_exchange_rate_cache = {"rate": None, "updated": None}


def _ensure_tables():
    """테이블 생성 (없으면)"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS mail_processing_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            subject TEXT,
            sender TEXT,
            received_at TEXT,
            attachment_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            hs_code_count INTEGER DEFAULT 0,
            erp_slip_id TEXT DEFAULT '',
            reply_sent INTEGER DEFAULT 0,
            error_message TEXT DEFAULT '',
            result_json TEXT DEFAULT '{}',
            processed_at TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS oem_product_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            item_code TEXT DEFAULT '',
            category TEXT DEFAULT '',
            mapped_by TEXT DEFAULT 'manual',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS mail_auto_exchange_rate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate REAL NOT NULL,
            source TEXT DEFAULT 'auto',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    conn.commit()


_ensure_tables()


# ─── 인증 ────────────────────────────────────────────────

@router.post("/auth")
async def mail_auto_auth(request: Request):
    """비밀번호 인증"""
    body = await request.json()
    pw = body.get("password", "")
    if pw == MAIL_AUTO_PASSWORD:
        return {"success": True}
    raise HTTPException(status_code=401, detail="비밀번호가 올바르지 않습니다")


# ─── 대시보드 ─────────────────────────────────────────────

@router.get("/dashboard")
async def get_dashboard():
    """대시보드 요약 데이터"""
    conn = get_connection()
    
    # 최근 처리 통계
    today = datetime.now(KST).strftime("%Y-%m-%d")
    week_ago = (datetime.now(KST) - timedelta(days=7)).strftime("%Y-%m-%d")
    
    stats = {
        "today_count": 0, "week_count": 0, "total_count": 0,
        "today_hs": 0, "total_hs": 0,
        "pending_oem": 0,
    }
    
    try:
        r = conn.execute("SELECT COUNT(*) FROM mail_processing_log WHERE processed_at LIKE ?", (f"{today}%",))
        stats["today_count"] = r.fetchone()[0]
        
        r = conn.execute("SELECT COUNT(*) FROM mail_processing_log WHERE processed_at >= ?", (week_ago,))
        stats["week_count"] = r.fetchone()[0]
        
        r = conn.execute("SELECT COUNT(*), COALESCE(SUM(hs_code_count),0) FROM mail_processing_log")
        row = r.fetchone()
        stats["total_count"] = row[0]
        stats["total_hs"] = row[1]
        
        r = conn.execute("SELECT COALESCE(SUM(hs_code_count),0) FROM mail_processing_log WHERE processed_at LIKE ?", (f"{today}%",))
        stats["today_hs"] = r.fetchone()[0]
        
        r = conn.execute("SELECT COUNT(*) FROM oem_product_mapping WHERE item_code='' OR item_code IS NULL")
        stats["pending_oem"] = r.fetchone()[0]
    except Exception as e:
        logger.warning(f"[대시보드] 통계 조회 오류: {e}")
    
    # 환율
    rate = _exchange_rate_cache.get("rate")
    rate_updated = _exchange_rate_cache.get("updated")
    
    return {
        "stats": stats,
        "exchange_rate": rate,
        "exchange_rate_updated": rate_updated,
        "auto": get_auto_state(),
    }


# ─── 자동 실행 스케줄러 제어 ───────────────────────────────

@router.post("/scheduler/start")
async def scheduler_start(request: Request):
    """자동 실행 시작"""
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    interval = body.get("interval_min", 3)
    start_mail_auto_scheduler(interval_min=interval)
    return {"success": True, "state": get_auto_state()}


@router.post("/scheduler/stop")
async def scheduler_stop():
    """자동 실행 중지"""
    stop_mail_auto_scheduler()
    return {"success": True, "state": get_auto_state()}


@router.get("/scheduler/status")
async def scheduler_status():
    """자동 실행 상태"""
    return get_auto_state()


# ─── 처리 로그 ────────────────────────────────────────────

@router.get("/logs")
async def get_logs(limit: int = 20, offset: int = 0):
    """처리 로그 목록"""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, message_id, subject, sender, received_at, 
                  attachment_count, status, hs_code_count, reply_sent,
                  error_message, processed_at, result_json
           FROM mail_processing_log 
           ORDER BY id DESC LIMIT ? OFFSET ?""",
        (limit, offset)
    ).fetchall()
    
    total = conn.execute("SELECT COUNT(*) FROM mail_processing_log").fetchone()[0]
    
    return {
        "total": total,
        "logs": [
            {
                "id": r[0], "message_id": r[1], "subject": r[2],
                "sender": r[3], "received_at": r[4],
                "attachment_count": r[5], "status": r[6],
                "hs_code_count": r[7], "reply_sent": bool(r[8]),
                "error_message": r[9], "processed_at": r[10],
                "result": json.loads(r[11]) if r[11] else {},
            }
            for r in rows
        ],
    }


# ─── 수동 실행 ────────────────────────────────────────────

@router.post("/trigger")
async def trigger_pipeline(request: Request):
    """메일 자동화 파이프라인 수동 실행"""
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    days_back = body.get("days_back", 30)
    auto_reply = body.get("auto_reply", False)
    auto_erp = body.get("auto_erp", True)
    custom_rate = body.get("exchange_rate", None)
    reply_template = body.get("reply_template", "")
    
    exchange_rate = custom_rate or _exchange_rate_cache.get("rate")
    conn = get_connection()
    
    result = await run_mail_automation_pipeline(
        days_back=days_back,
        exchange_rate=exchange_rate,
        auto_reply=auto_reply,
        auto_erp=auto_erp,
        db_conn=conn,
        reply_template=reply_template,
    )
    
    # 환율 캐시 갱신
    if result.get("exchange_rate"):
        _exchange_rate_cache["rate"] = result["exchange_rate"]
        _exchange_rate_cache["updated"] = datetime.now(KST).isoformat()
    
    return result


# ─── Excel 테스트 처리 ────────────────────────────────────

@router.post("/process-file")
async def process_uploaded_file(file: UploadFile = File(...)):
    """업로드된 Excel 파일에 HS코드 적용 (테스트용)"""
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(400, "xlsx 파일만 지원합니다")
    
    data = await file.read()
    result = process_excel_hs_code(data, file.filename)
    
    if not result["success"]:
        raise HTTPException(400, result.get("error", "처리 실패"))
    
    # 처리된 Excel 반환
    return {
        "filename": result["filename"],
        "stats": result["stats"],
        "items": result["items"][:50],  # 최대 50개
        "erp_lines": result["erp_lines"][:50],
        "oem_items": result["oem_items"],
    }


@router.post("/process-file/download")
async def process_and_download(file: UploadFile = File(...)):
    """업로드 Excel → HS코드 입력 → 다운로드"""
    data = await file.read()
    result = process_excel_hs_code(data, file.filename)
    
    if not result["success"]:
        raise HTTPException(400, result.get("error"))
    
    return StreamingResponse(
        io.BytesIO(result["output_data"]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=HS_{file.filename}"},
    )


# ─── ERP 구매전표 테스트 ──────────────────────────────────

@router.post("/test-erp")
async def test_erp_purchase(file: UploadFile = File(...), exchange_rate: float = Form(0)):
    """업로드 Excel → ERP 구매전표 미리보기 (실제 전송 안함)"""
    data = await file.read()
    result = process_excel_hs_code(data, file.filename)
    
    if not result["success"]:
        raise HTTPException(400, result.get("error"))
    
    # 환율 조회
    rate = exchange_rate
    if rate <= 0:
        rate_info = await fetch_exchange_rate()
        rate = rate_info["rate"]
    
    # ERP 라인 미리보기 생성
    erp_preview = []
    for item in result["erp_lines"]:
        price_usd = item.get("price_usd", 0)
        tax_rate = item.get("tax_rate", 1.2)
        price_krw = round(price_usd * tax_rate * rate)
        erp_preview.append({
            "prod_cd": item["prod_cd"],
            "qty": item["qty"],
            "price_usd": price_usd,
            "tax_rate": tax_rate,
            "price_krw": price_krw,
            "supply_amt": round(price_krw * item["qty"]),
            "description": item.get("description", ""),
        })
    
    # 전표일자 (오늘 +8일)
    io_date = (datetime.now(KST) + timedelta(days=8)).strftime("%Y-%m-%d")
    
    return {
        "erp_lines": erp_preview,
        "oem_items": result["oem_items"],
        "total_lines": len(erp_preview),
        "total_amount": sum(l["supply_amt"] for l in erp_preview),
        "exchange_rate": rate,
        "io_date": io_date,
        "cust_code": ERP_SUPPLIER_CODE,
    }


@router.post("/submit-erp")
async def submit_erp_purchase(request: Request):
    """ERP 구매전표 실제 전송 (미리보기 데이터 기반)"""
    from services.erp_client import erp_client
    
    body = await request.json()
    erp_lines = body.get("erp_lines", [])
    io_date_str = body.get("io_date", "")
    
    if not erp_lines:
        raise HTTPException(400, "전표 항목이 없습니다")
    
    io_date = io_date_str.replace("-", "") if io_date_str else (
        datetime.now(KST) + timedelta(days=8)
    ).strftime("%Y%m%d")
    
    # 미리보기에서 이미 KRW 단가가 계산됨
    lines = [
        {
            "prod_cd": item["prod_cd"],
            "qty": item["qty"],
            "unit": "EA",
            "price": item["price_krw"],
        }
        for item in erp_lines
    ]
    
    try:
        result = await erp_client.save_purchase(
            cust_code=ERP_SUPPLIER_CODE,
            lines=lines,
            io_date=io_date,
        )
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


# ─── 환율 ─────────────────────────────────────────────────

@router.get("/exchange-rate")
async def get_exchange_rate():
    """환율 조회 (전신환매도율 = 기준율 + 스프레드)"""
    cached = _exchange_rate_cache.get("rate")
    if cached and _exchange_rate_cache.get("base_rate"):
        return {
            "rate": cached,
            "base_rate": _exchange_rate_cache.get("base_rate"),
            "spread": _exchange_rate_cache.get("spread", 1.75),
            "source": _exchange_rate_cache.get("source", "cache"),
            "updated": _exchange_rate_cache.get("updated"),
        }
    
    rate_info = await fetch_exchange_rate()
    _exchange_rate_cache["rate"] = rate_info["rate"]
    _exchange_rate_cache["base_rate"] = rate_info.get("base_rate")
    _exchange_rate_cache["spread"] = rate_info.get("spread", 1.75)
    _exchange_rate_cache["source"] = rate_info.get("source", "")
    _exchange_rate_cache["updated"] = datetime.now(KST).isoformat()
    
    return {
        "rate": rate_info["rate"],
        "base_rate": rate_info.get("base_rate"),
        "spread": rate_info.get("spread", 1.75),
        "source": rate_info.get("source", ""),
        "updated": _exchange_rate_cache["updated"],
    }


@router.post("/exchange-rate")
async def set_exchange_rate(request: Request):
    """환율 수동 설정"""
    body = await request.json()
    rate = body.get("rate")
    if not rate or float(rate) <= 0:
        raise HTTPException(400, "유효한 환율을 입력하세요")
    
    _exchange_rate_cache["rate"] = float(rate)
    _exchange_rate_cache["updated"] = datetime.now(KST).isoformat()
    
    # DB 저장
    conn = get_connection()
    conn.execute(
        "INSERT INTO mail_auto_exchange_rate (rate, source) VALUES (?, 'manual')",
        (float(rate),)
    )
    conn.commit()
    
    return {"rate": float(rate), "source": "manual", "updated": _exchange_rate_cache["updated"]}


# ─── OEM 제품 매핑 ─────────────────────────────────────────

@router.get("/oem-products")
async def get_oem_products():
    """OEM 미매핑 제품 목록"""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, description, item_code, category, mapped_by, created_at 
           FROM oem_product_mapping ORDER BY id DESC"""
    ).fetchall()
    
    return {
        "products": [
            {
                "id": r[0], "description": r[1], "item_code": r[2],
                "category": r[3], "mapped_by": r[4], "created_at": r[5],
            }
            for r in rows
        ]
    }


@router.post("/oem-products")
async def map_oem_product(request: Request):
    """OEM 제품 품목코드 매핑"""
    body = await request.json()
    product_id = body.get("id")
    item_code = body.get("item_code", "").strip()
    
    if not item_code:
        raise HTTPException(400, "품목코드를 입력하세요")
    
    conn = get_connection()
    if product_id:
        conn.execute(
            "UPDATE oem_product_mapping SET item_code=?, updated_at=datetime('now','localtime') WHERE id=?",
            (item_code, product_id)
        )
    else:
        desc = body.get("description", "")
        category = body.get("category", "")
        conn.execute(
            "INSERT INTO oem_product_mapping (description, item_code, category) VALUES (?,?,?)",
            (desc, item_code, category)
        )
    conn.commit()
    
    return {"success": True}


# ─── 메일 미리보기 (IMAP 검색만) ──────────────────────────

@router.get("/preview-emails")
async def preview_emails(days_back: int = 30):
    """처리 대상 메일 미리보기 (실제 처리 없이 검색만)"""
    emails = fetch_bor_emails(days_back=days_back)
    
    conn = get_connection()
    processed_ids = set()
    try:
        cursor = conn.execute("SELECT message_id FROM mail_processing_log")
        processed_ids = {row[0] for row in cursor.fetchall()}
    except Exception:
        pass
    
    return {
        "total": len(emails),
        "emails": [
            {
                "subject": m["subject"],
                "date": str(m.get("date_kst", "")),
                "message_id": m["message_id"],
                "attachments": [
                    {"filename": a["filename"], "bor_number": a.get("bor_number", "")}
                    for a in m["attachments"]
                ],
                "already_processed": m["message_id"] in processed_ids,
            }
            for m in emails
        ],
    }
