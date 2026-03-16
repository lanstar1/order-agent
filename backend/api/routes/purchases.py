"""
구매입력 API 라우터
- POST /api/purchases/process        — 텍스트 → AI 분석 → 구매 라인
- POST /api/purchases/process-image  — 이미지/PDF → OCR → AI 분석
- POST /api/purchases/confirm        — 사용자 확인
- POST /api/purchases/submit-erp/{id} — ERP 구매입력(SavePurchases) 전송
- GET  /api/purchases/list           — 구매입력 목록
- GET  /api/purchases/{id}           — 구매입력 상세
"""
import uuid
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query, Depends
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from models.schemas import (
    OrderCreateRequest, OrderProcessResponse, OrderConfirmRequest,
    ERPSubmitResponse, OrderStatus, OrderLineExtracted, ProductCandidate
)
from agents.extraction import extract_order_lines
from agents.resolution import resolve_product
from services.erp_client import erp_client
from db.database import get_connection
from config import UPLOAD_DIR, CONFIDENCE_THRESHOLD

router = APIRouter(prefix="/api/purchases", tags=["purchases"])
logger = logging.getLogger(__name__)


def _ensure_purchase_tables():
    """purchase_orders 및 관련 테이블 생성 (없으면)"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS purchase_orders (
            order_id   TEXT PRIMARY KEY,
            cust_code  TEXT NOT NULL,
            cust_name  TEXT DEFAULT '',
            raw_text   TEXT DEFAULT '',
            doc_no     TEXT DEFAULT '',
            io_date    TEXT DEFAULT '',
            status     TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS purchase_order_lines (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    TEXT NOT NULL,
            line_no     INTEGER NOT NULL,
            raw_text    TEXT DEFAULT '',
            qty         REAL,
            unit        TEXT DEFAULT '',
            price       REAL DEFAULT 0,
            selected_cd TEXT,
            is_confirmed INTEGER DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES purchase_orders(order_id)
        );
        CREATE TABLE IF NOT EXISTS purchase_order_candidates (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            line_id      INTEGER NOT NULL,
            prod_cd      TEXT,
            prod_name    TEXT,
            score        REAL DEFAULT 0,
            match_reason TEXT DEFAULT '',
            was_selected INTEGER DEFAULT 0,
            FOREIGN KEY (line_id) REFERENCES purchase_order_lines(id)
        );
        CREATE TABLE IF NOT EXISTS purchase_order_submissions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id     TEXT NOT NULL,
            success      INTEGER DEFAULT 0,
            erp_slip_no  TEXT DEFAULT '',
            response     TEXT DEFAULT '',
            submitted_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (order_id) REFERENCES purchase_orders(order_id)
        );
    """)
    conn.close()


_ensure_purchase_tables()


# ─────────────────────────────────────────
#  헬퍼: 엑셀/CSV → 텍스트 변환
# ─────────────────────────────────────────
def _spreadsheet_to_text(file_path: str, suffix: str) -> str:
    """엑셀(.xlsx/.xls) 또는 CSV 파일을 AI가 읽을 수 있는 텍스트로 변환"""
    if suffix == ".csv":
        import csv
        rows = []
        for enc in ("utf-8-sig", "cp949", "euc-kr"):
            try:
                with open(file_path, encoding=enc, errors="replace") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        cells = [str(c).strip() for c in row if str(c).strip()]
                        if cells:
                            rows.append(" | ".join(cells))
                break
            except Exception:
                continue
        return "\n".join(rows)
    else:
        import openpyxl
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        rows = []
        for ws in wb.worksheets:
            for row in ws.iter_rows(values_only=True):
                cells = [str(c).strip() for c in row if c is not None and str(c).strip()]
                if cells:
                    rows.append(" | ".join(cells))
        wb.close()
        return "\n".join(rows)


# ─────────────────────────────────────────
#  헬퍼: 추출된 라인 → DB 저장 + ERP 단가 조회
# ─────────────────────────────────────────
async def _build_and_save_lines(
    conn, order_id: str, cust_code: str, extracted: list
) -> list:
    """AI 추출 결과를 상품 매칭 → ERP 단가 조회 → DB 저장 순으로 처리"""
    result_lines = []
    for item in extracted:
        candidates_raw = await resolve_product(
            item.get("product_hint", item.get("raw_text", "")),
            item.get("implicit_notes", ""),
            cust_code=cust_code,
            normalized_hints=item.get("normalized_hints", []),
            detected_specs=item.get("detected_specs"),
        )
        candidates = [ProductCandidate(**c) for c in candidates_raw]
        auto_select = None
        if candidates and candidates[0].score >= CONFIDENCE_THRESHOLD:
            auto_select = candidates[0].prod_cd

        selected_model = ""
        if auto_select and candidates:
            sel_cand = next((c for c in candidates if c.prod_cd == auto_select), None)
            if sel_cand:
                selected_model = sel_cand.model_name or ""

        # AI가 텍스트에서 단가를 직접 추출한 경우 사용
        ai_price = item.get("price")
        price_val = float(ai_price) if ai_price else None

        line = OrderLineExtracted(
            line_no=item["line_no"],
            raw_text=item.get("raw_text", ""),
            qty=item.get("qty"),
            unit=item.get("unit"),
            price=price_val,
            candidates=candidates,
            selected_cd=auto_select,
            is_confirmed=bool(auto_select),
            model_name=selected_model or None,
        )
        result_lines.append(line)

    # ERP에서 OUT_PRICE 일괄 조회 (단가가 없는 자동선택 품목)
    no_price_cds = list({l.selected_cd for l in result_lines if l.selected_cd and not l.price})
    if no_price_cds:
        try:
            erp_prices = await erp_client.get_product_prices(no_price_cds)
            logger.info(f"[purchase process] ERP 단가 조회: {len(erp_prices)}건")
            for line in result_lines:
                if line.selected_cd and not line.price:
                    line.price = erp_prices.get(line.selected_cd)
        except Exception as pe:
            logger.warning(f"[purchase process] ERP 단가 조회 실패 (무시): {pe}")

    # DB 저장
    for line in result_lines:
        cur = conn.execute(
            "INSERT INTO purchase_order_lines(order_id,line_no,raw_text,qty,unit,price,selected_cd,is_confirmed) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (order_id, line.line_no, line.raw_text, line.qty, line.unit,
             line.price or 0, line.selected_cd, int(line.is_confirmed))
        )
        line_id = cur.lastrowid
        for c in line.candidates:
            conn.execute(
                "INSERT INTO purchase_order_candidates(line_id,prod_cd,prod_name,score,match_reason,was_selected) "
                "VALUES(?,?,?,?,?,?)",
                (line_id, c.prod_cd, c.prod_name, c.score, c.match_reason,
                 1 if c.prod_cd == line.selected_cd else 0)
            )
        conn.commit()

    return result_lines


# ─────────────────────────────────────────
#  구매입력 생성 및 처리 (텍스트)
# ─────────────────────────────────────────
@router.post("/process", response_model=OrderProcessResponse)
async def process_purchase(req: OrderCreateRequest, user: dict = Depends(get_current_user)):
    """텍스트를 받아 AI 처리 후 구매 라인 반환"""
    order_id = "PO-" + str(uuid.uuid4())[:8].upper()

    if not req.raw_text or not req.raw_text.strip():
        raise HTTPException(400, "발주서 텍스트를 입력해주세요.")

    conn = get_connection()
    conn.execute(
        "INSERT INTO purchase_orders(order_id,cust_code,cust_name,raw_text,status) VALUES(?,?,?,?,?)",
        (order_id, req.cust_code, req.cust_name, req.raw_text, OrderStatus.PROCESSING)
    )
    conn.commit()

    extracted = await extract_order_lines(req.raw_text, req.cust_name, cust_code=req.cust_code)
    result_lines = await _build_and_save_lines(conn, order_id, req.cust_code, extracted)

    needs_review = any(not l.is_confirmed for l in result_lines)
    new_status = OrderStatus.REVIEWING if needs_review else OrderStatus.CONFIRMED
    conn.execute("UPDATE purchase_orders SET status=?,updated_at=datetime('now','localtime') WHERE order_id=?",
                 (new_status, order_id))
    conn.commit()
    conn.close()

    return OrderProcessResponse(
        order_id=order_id,
        cust_code=req.cust_code,
        cust_name=req.cust_name,
        status=new_status,
        lines=result_lines,
        created_at=datetime.now(),
        message="검토 필요 항목이 있습니다." if needs_review else "모든 항목 자동 매칭 완료",
    )


# ─────────────────────────────────────────
#  이미지/PDF/엑셀/CSV 업로드 처리
# ─────────────────────────────────────────
@router.post("/process-image", response_model=OrderProcessResponse)
async def process_purchase_image(
    cust_code: str = Form(...),
    cust_name: str = Form(...),
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    """이미지/PDF/엑셀/CSV 파일 업로드 후 AI 분석 → 구매입력 처리
    - 이미지/PDF: Claude Vision OCR
    - 엑셀(.xlsx/.xls) / CSV: 텍스트 변환 후 AI 추출
    """
    EXCEL_EXTS = {".xlsx", ".xls", ".csv"}
    IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
    ALL_EXTS = EXCEL_EXTS | IMAGE_EXTS | {".pdf"}

    suffix = Path(file.filename).suffix.lower() if file.filename else ".jpg"
    if suffix not in ALL_EXTS:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix} (지원: 이미지, PDF, XLSX, XLS, CSV)")

    order_id = "PO-" + str(uuid.uuid4())[:8].upper()
    save_path = UPLOAD_DIR / f"{order_id}{suffix}"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 10MB를 초과합니다.")
    with open(save_path, "wb") as f:
        f.write(content)

    conn = get_connection()
    conn.execute(
        "INSERT INTO purchase_orders(order_id,cust_code,cust_name,raw_text,status) VALUES(?,?,?,?,?)",
        (order_id, cust_code, cust_name, f"[파일: {file.filename}]", OrderStatus.PROCESSING)
    )
    conn.commit()

    # 파일 종류별 분기 처리
    if suffix in EXCEL_EXTS:
        # ── 엑셀/CSV: 텍스트로 변환 후 AI 추출 ──
        try:
            raw_text = _spreadsheet_to_text(str(save_path), suffix)
        except Exception as e:
            conn.execute("UPDATE purchase_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.commit()
            conn.close()
            raise HTTPException(500, f"파일 읽기 실패: {e}")

        if not raw_text.strip():
            conn.execute("UPDATE purchase_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.commit()
            conn.close()
            raise HTTPException(422, "파일에서 내용을 읽을 수 없습니다.")

        conn.execute("UPDATE purchase_orders SET raw_text=? WHERE order_id=?", (raw_text, order_id))
        conn.commit()

        try:
            extracted = await extract_order_lines(raw_text, cust_name, cust_code=cust_code)
        except Exception as e:
            conn.execute("UPDATE purchase_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.commit()
            conn.close()
            raise HTTPException(500, f"AI 분석 실패: {e}")

    else:
        # ── 이미지/PDF: Claude Vision OCR ──
        from agents.ocr import ocr_and_extract
        try:
            extracted, raw_text = await ocr_and_extract(str(save_path), cust_name)
        except Exception as e:
            conn.execute("UPDATE purchase_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.commit()
            conn.close()
            raise HTTPException(500, f"OCR 처리 실패: {e}")

        conn.execute("UPDATE purchase_orders SET raw_text=? WHERE order_id=?", (raw_text, order_id))
        conn.commit()

    if not extracted:
        conn.execute("UPDATE purchase_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
        conn.commit()
        conn.close()
        raise HTTPException(422, "파일에서 발주 항목을 인식할 수 없습니다.")

    result_lines = await _build_and_save_lines(conn, order_id, cust_code, extracted)

    needs_review = any(not l.is_confirmed for l in result_lines)
    new_status = OrderStatus.REVIEWING if needs_review else OrderStatus.CONFIRMED
    conn.execute("UPDATE purchase_orders SET status=?,updated_at=datetime('now','localtime') WHERE order_id=?",
                 (new_status, order_id))
    conn.commit()
    conn.close()

    return OrderProcessResponse(
        order_id=order_id,
        cust_code=cust_code,
        cust_name=cust_name,
        status=new_status,
        lines=result_lines,
        created_at=datetime.now(),
        message="검토 필요 항목이 있습니다." if needs_review else "모든 항목 자동 매칭 완료",
    )


# ─────────────────────────────────────────
#  사용자 확인
# ─────────────────────────────────────────
@router.post("/confirm", response_model=dict)
async def confirm_purchase(req: OrderConfirmRequest, user: dict = Depends(get_current_user)):
    """사용자가 검토한 라인별 최종 상품 코드를 저장"""
    conn = get_connection()
    row = conn.execute("SELECT order_id FROM purchase_orders WHERE order_id=?", (req.order_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"구매입력 {req.order_id}을 찾을 수 없습니다.")

    for line in req.lines:
        conn.execute(
            "UPDATE purchase_order_lines SET selected_cd=?,qty=?,unit=?,price=?,is_confirmed=1 WHERE order_id=? AND line_no=?",
            (line.prod_cd, line.qty, line.unit, line.price or 0, req.order_id, line.line_no)
        )

    conn.execute(
        "UPDATE purchase_orders SET status='confirmed',updated_at=datetime('now','localtime') WHERE order_id=?",
        (req.order_id,)
    )
    conn.commit()
    conn.close()
    return {"success": True, "order_id": req.order_id, "message": "확인 완료. ERP 구매입력 전송 준비됩니다."}


# ─────────────────────────────────────────
#  ERP 구매입력 전송
# ─────────────────────────────────────────
@router.post("/submit-erp/{order_id}", response_model=ERPSubmitResponse)
async def submit_purchase_to_erp(order_id: str, emp_cd: str = "", user: dict = Depends(get_current_user)):
    """확인된 구매입력을 ECOUNT ERP SavePurchases로 전송"""
    conn = get_connection()
    try:
        order = conn.execute("SELECT * FROM purchase_orders WHERE order_id=?", (order_id,)).fetchone()
        if not order:
            raise HTTPException(404, "구매입력 데이터를 찾을 수 없습니다.")
        if order["status"] not in ("confirmed", "reviewing"):
            raise HTTPException(400, f"ERP 전송 불가 상태: {order['status']}")

        lines = conn.execute(
            "SELECT * FROM purchase_order_lines WHERE order_id=? AND is_confirmed=1 AND selected_cd IS NOT NULL",
            (order_id,)
        ).fetchall()

        if not lines:
            raise HTTPException(400, "확인된 라인이 없습니다.")

        # 품목별 단가 조회 (구매단가는 ERP GetBasicProductsList에서 IN_PRICE로 오지만
        # 현재 API는 OUT_PRICE만 반환 → 0 처리, 사용자가 직접 단가 입력한 경우 해당 값 사용)
        all_prod_cds = list({l["selected_cd"] for l in lines})
        erp_prices = {}
        try:
            erp_prices = await erp_client.get_product_prices(all_prod_cds)
            logger.info(f"[submit_purchase] 단가 조회 완료: {len(erp_prices)}건")
        except Exception as pe:
            logger.warning(f"[submit_purchase] 단가 조회 실패: {pe}")

        erp_lines = [
            {
                "prod_cd": l["selected_cd"],
                "qty":     l["qty"],
                "unit":    l["unit"] or "",
                "price":   float(l["price"]) if l["price"] else erp_prices.get(l["selected_cd"], 0),
            }
            for l in lines
        ]

        upload_ser = str(int(time.time()))[-8:]

        result = await erp_client.save_purchase(
            cust_code=order["cust_code"],
            lines=erp_lines,
            upload_ser=upload_ser,
            emp_cd=emp_cd,
            doc_no=order["doc_no"] or "",
            io_date=order["io_date"] or "",
        )

        erp_data = result.get("data", {})
        inner = erp_data.get("Data", {})
        success_cnt = inner.get("SuccessCnt", -1)
        fail_cnt = inner.get("FailCnt", 0)
        slip_nos = inner.get("SlipNos", [])
        result_details = inner.get("ResultDetails", [])

        actually_saved = result.get("success") and (success_cnt > 0)

        if actually_saved:
            slip_no = slip_nos[0] if slip_nos else ""
            conn.execute("UPDATE purchase_orders SET status='submitted',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.execute(
                "INSERT INTO purchase_order_submissions(order_id,success,erp_slip_no,response) VALUES(?,1,?,?)",
                (order_id, slip_no, json.dumps(result, ensure_ascii=False))
            )
            conn.commit()
            conn.close()
            return ERPSubmitResponse(order_id=order_id, success=True,
                                     erp_slip_no=slip_no,
                                     message=f"ERP 구매입력 완료 (전표번호: {slip_no})")
        else:
            if result_details:
                errors = "; ".join([rd.get("TotalError", "") for rd in result_details if not rd.get("IsSuccess")])
                err_msg = errors or "ERP 구매입력 저장 실패"
            elif result.get("error"):
                err_msg = result.get("error", "알 수 없는 오류")
                if isinstance(err_msg, dict):
                    err_msg = err_msg.get("Message") or err_msg.get("message") or json.dumps(err_msg, ensure_ascii=False)
            else:
                err_msg = f"SuccessCnt={success_cnt}, FailCnt={fail_cnt}"

            conn.execute(
                "INSERT INTO purchase_order_submissions(order_id,success,response) VALUES(?,0,?)",
                (order_id, json.dumps(result, ensure_ascii=False))
            )
            conn.commit()
            conn.close()
            return ERPSubmitResponse(order_id=order_id, success=False,
                                     message=f"ERP 오류: {err_msg}")

    except HTTPException:
        conn.close()
        raise
    except Exception as e:
        logger.error(f"[submit-purchase] 예외: {e}", exc_info=True)
        try:
            conn.execute(
                "INSERT INTO purchase_order_submissions(order_id,success,response) VALUES(?,0,?)",
                (order_id, json.dumps({"error": str(e)}, ensure_ascii=False))
            )
            conn.commit()
        except Exception:
            pass
        conn.close()
        return ERPSubmitResponse(order_id=order_id, success=False,
                                 message=f"서버 오류: {str(e)}")


# ─────────────────────────────────────────
#  구매입력 목록 조회
# ─────────────────────────────────────────
@router.get("/list")
async def list_purchases(limit: int = 20, user: dict = Depends(get_current_user)):
    conn = get_connection()
    rows = conn.execute(
        "SELECT order_id,cust_name,status,created_at,updated_at FROM purchase_orders ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"orders": [dict(r) for r in rows]}


@router.get("/{order_id}")
async def get_purchase(order_id: str, user: dict = Depends(get_current_user)):
    conn = get_connection()
    order = conn.execute("SELECT * FROM purchase_orders WHERE order_id=?", (order_id,)).fetchone()
    if not order:
        conn.close()
        raise HTTPException(404)
    lines = conn.execute("SELECT * FROM purchase_order_lines WHERE order_id=?", (order_id,)).fetchall()

    submissions = conn.execute(
        "SELECT success, erp_slip_no, submitted_at FROM purchase_order_submissions WHERE order_id=? ORDER BY submitted_at DESC",
        (order_id,)
    ).fetchall()

    lines_with_candidates = []
    for l in lines:
        l_dict = dict(l)
        cands = conn.execute(
            "SELECT prod_cd, prod_name, score, match_reason, was_selected FROM purchase_order_candidates WHERE line_id=? ORDER BY score DESC LIMIT 3",
            (l["id"],)
        ).fetchall()
        l_dict["candidates"] = [dict(c) for c in cands]
        lines_with_candidates.append(l_dict)

    conn.close()
    return {
        "order": dict(order),
        "lines": lines_with_candidates,
        "submissions": [dict(s) for s in submissions],
    }
