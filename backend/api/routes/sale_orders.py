"""
주문서입력 API 라우터 (수주/Sales Order)
- POST /api/sale-orders/process        — 텍스트 발주서 → AI 분석 → 주문서 라인
- POST /api/sale-orders/process-image   — 이미지 발주서 → OCR → AI 분석
- POST /api/sale-orders/confirm         — 사용자 확인
- POST /api/sale-orders/submit-erp/{id} — ERP 주문서입력 전송
- GET  /api/sale-orders/list            — 주문서 목록
- GET  /api/sale-orders/{id}            — 주문서 상세
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

router = APIRouter(prefix="/api/sale-orders", tags=["sale-orders"])
logger = logging.getLogger(__name__)


def _ensure_sale_orders_table():
    """sale_orders 및 sale_order_lines 테이블 생성 (없으면)"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sale_orders (
            order_id   TEXT PRIMARY KEY,
            cust_code  TEXT NOT NULL,
            cust_name  TEXT DEFAULT '',
            raw_text   TEXT DEFAULT '',
            doc_no     TEXT DEFAULT '',
            time_date  TEXT DEFAULT '',
            status     TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS sale_order_lines (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    TEXT NOT NULL,
            line_no     INTEGER NOT NULL,
            raw_text    TEXT DEFAULT '',
            qty         REAL,
            unit        TEXT DEFAULT '',
            price       REAL DEFAULT 0,
            selected_cd TEXT,
            is_confirmed INTEGER DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES sale_orders(order_id)
        );
        CREATE TABLE IF NOT EXISTS sale_order_candidates (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            line_id      INTEGER NOT NULL,
            prod_cd      TEXT,
            prod_name    TEXT,
            score        REAL DEFAULT 0,
            match_reason TEXT DEFAULT '',
            was_selected INTEGER DEFAULT 0,
            FOREIGN KEY (line_id) REFERENCES sale_order_lines(id)
        );
        CREATE TABLE IF NOT EXISTS sale_order_submissions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id     TEXT NOT NULL,
            success      INTEGER DEFAULT 0,
            erp_slip_no  TEXT DEFAULT '',
            response     TEXT DEFAULT '',
            submitted_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (order_id) REFERENCES sale_orders(order_id)
        );
    """)
    conn.close()


# 앱 시작 시 테이블 확보
_ensure_sale_orders_table()


# ─────────────────────────────────────────
#  주문서 생성 및 처리 (텍스트)
# ─────────────────────────────────────────
@router.post("/process", response_model=OrderProcessResponse)
async def process_sale_order(req: OrderCreateRequest, user: dict = Depends(get_current_user)):
    """발주서 텍스트를 받아 AI 처리 후 주문서 라인 반환"""
    order_id = "SO-" + str(uuid.uuid4())[:8].upper()

    if not req.raw_text or not req.raw_text.strip():
        raise HTTPException(400, "발주서 텍스트를 입력해주세요.")

    conn = get_connection()
    conn.execute(
        "INSERT INTO sale_orders(order_id,cust_code,cust_name,raw_text,status) VALUES(?,?,?,?,?)",
        (order_id, req.cust_code, req.cust_name, req.raw_text, OrderStatus.PROCESSING)
    )
    conn.commit()

    # 1. 주문 라인 추출 (학습 데이터 활용)
    extracted = await extract_order_lines(req.raw_text, req.cust_name, cust_code=req.cust_code)

    # 2. 각 라인 상품 매칭 (학습 데이터 우선)
    result_lines = []
    for item in extracted:
        candidates_raw = await resolve_product(
            item.get("product_hint", item.get("raw_text", "")),
            item.get("implicit_notes", ""),
            cust_code=req.cust_code,
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

        line = OrderLineExtracted(
            line_no=item["line_no"],
            raw_text=item.get("raw_text", ""),
            qty=item.get("qty"),
            unit=item.get("unit"),
            candidates=candidates,
            selected_cd=auto_select,
            is_confirmed=bool(auto_select),
            model_name=selected_model or None,
        )
        result_lines.append(line)

        cur = conn.execute(
            "INSERT INTO sale_order_lines(order_id,line_no,raw_text,qty,unit,selected_cd,is_confirmed) VALUES(?,?,?,?,?,?,?)",
            (order_id, line.line_no, line.raw_text, line.qty, line.unit, line.selected_cd, int(line.is_confirmed))
        )
        line_id = cur.lastrowid
        for c in candidates:
            conn.execute(
                "INSERT INTO sale_order_candidates(line_id,prod_cd,prod_name,score,match_reason,was_selected) VALUES(?,?,?,?,?,?)",
                (line_id, c.prod_cd, c.prod_name, c.score, c.match_reason, 1 if c.prod_cd == auto_select else 0)
            )
        conn.commit()

    needs_review = any(not l.is_confirmed for l in result_lines)
    new_status = OrderStatus.REVIEWING if needs_review else OrderStatus.CONFIRMED
    conn.execute("UPDATE sale_orders SET status=?,updated_at=datetime('now','localtime') WHERE order_id=?",
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
#  이미지 업로드 + OCR 처리
# ─────────────────────────────────────────
@router.post("/process-image", response_model=OrderProcessResponse)
async def process_sale_order_image(
    cust_code: str = Form(...),
    cust_name: str = Form(...),
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    """이미지/PDF 발주서를 업로드하고 Claude Vision OCR 후 주문서 처리"""
    from agents.ocr import ocr_and_extract

    allowed_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf"}
    suffix = Path(file.filename).suffix.lower() if file.filename else ".jpg"
    if suffix not in allowed_exts:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix}")

    order_id = "SO-" + str(uuid.uuid4())[:8].upper()
    save_path = UPLOAD_DIR / f"{order_id}{suffix}"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 10MB를 초과합니다.")
    with open(save_path, "wb") as f:
        f.write(content)

    conn = get_connection()
    conn.execute(
        "INSERT INTO sale_orders(order_id,cust_code,cust_name,raw_text,status) VALUES(?,?,?,?,?)",
        (order_id, cust_code, cust_name, f"[이미지: {file.filename}]", OrderStatus.PROCESSING)
    )
    conn.commit()

    try:
        extracted, raw_text = await ocr_and_extract(str(save_path), cust_name)
    except Exception as e:
        conn.execute("UPDATE sale_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
        conn.commit()
        conn.close()
        raise HTTPException(500, f"OCR 처리 실패: {e}")

    if not extracted:
        conn.execute("UPDATE sale_orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
        conn.commit()
        conn.close()
        raise HTTPException(422, "이미지에서 발주 항목을 인식할 수 없습니다.")

    conn.execute("UPDATE sale_orders SET raw_text=? WHERE order_id=?", (raw_text, order_id))
    conn.commit()

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

        line = OrderLineExtracted(
            line_no=item["line_no"],
            raw_text=item.get("raw_text", ""),
            qty=item.get("qty"),
            unit=item.get("unit"),
            candidates=candidates,
            selected_cd=auto_select,
            is_confirmed=bool(auto_select),
            model_name=selected_model or None,
        )
        result_lines.append(line)

        cur = conn.execute(
            "INSERT INTO sale_order_lines(order_id,line_no,raw_text,qty,unit,selected_cd,is_confirmed) VALUES(?,?,?,?,?,?,?)",
            (order_id, line.line_no, line.raw_text, line.qty, line.unit, line.selected_cd, int(line.is_confirmed))
        )
        line_id = cur.lastrowid
        for c in candidates:
            conn.execute(
                "INSERT INTO sale_order_candidates(line_id,prod_cd,prod_name,score,match_reason,was_selected) VALUES(?,?,?,?,?,?)",
                (line_id, c.prod_cd, c.prod_name, c.score, c.match_reason, 1 if c.prod_cd == auto_select else 0)
            )
        conn.commit()

    needs_review = any(not l.is_confirmed for l in result_lines)
    new_status = OrderStatus.REVIEWING if needs_review else OrderStatus.CONFIRMED
    conn.execute("UPDATE sale_orders SET status=?,updated_at=datetime('now','localtime') WHERE order_id=?",
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
async def confirm_sale_order(req: OrderConfirmRequest, user: dict = Depends(get_current_user)):
    """사용자가 검토한 라인별 최종 상품 코드를 저장"""
    conn = get_connection()
    row = conn.execute("SELECT order_id FROM sale_orders WHERE order_id=?", (req.order_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"주문서 {req.order_id}을 찾을 수 없습니다.")

    for line in req.lines:
        conn.execute(
            "UPDATE sale_order_lines SET selected_cd=?,qty=?,unit=?,price=?,is_confirmed=1 WHERE order_id=? AND line_no=?",
            (line.prod_cd, line.qty, line.unit, line.price or 0, req.order_id, line.line_no)
        )

    conn.execute(
        "UPDATE sale_orders SET status='confirmed',updated_at=datetime('now','localtime') WHERE order_id=?",
        (req.order_id,)
    )
    conn.commit()
    conn.close()
    return {"success": True, "order_id": req.order_id, "message": "확인 완료. ERP 주문서 전송 준비됩니다."}


# ─────────────────────────────────────────
#  ERP 견적서입력 전송
# ─────────────────────────────────────────
@router.post("/submit-erp/{order_id}", response_model=ERPSubmitResponse)
async def submit_sale_order_to_erp(order_id: str, emp_cd: str = "", user: dict = Depends(get_current_user)):
    """확인된 견적서를 ECOUNT ERP 견적서입력(SaveQuotation)으로 전송"""
    conn = get_connection()
    try:
        order = conn.execute("SELECT * FROM sale_orders WHERE order_id=?", (order_id,)).fetchone()
        if not order:
            raise HTTPException(404, "주문서를 찾을 수 없습니다.")
        if order["status"] not in ("confirmed", "reviewing"):
            raise HTTPException(400, f"ERP 전송 불가 상태: {order['status']}")

        lines = conn.execute(
            "SELECT * FROM sale_order_lines WHERE order_id=? AND is_confirmed=1 AND selected_cd IS NOT NULL",
            (order_id,)
        ).fetchall()

        if not lines:
            raise HTTPException(400, "확인된 라인이 없습니다.")

        # 품목별 출고단가 조회
        all_prod_cds = list({l["selected_cd"] for l in lines})
        erp_prices = {}
        try:
            erp_prices = await erp_client.get_product_prices(all_prod_cds)
            logger.info(f"[submit_sale_order] 단가 조회 완료: {len(erp_prices)}건")
        except Exception as pe:
            logger.warning(f"[submit_sale_order] 단가 조회 실패: {pe}")

        erp_lines = [
            {
                "prod_cd": l["selected_cd"],
                "qty":     l["qty"],
                "unit":    l["unit"] or "",
                "price":   erp_prices.get(l["selected_cd"], 0),
            }
            for l in lines
        ]

        upload_ser = str(int(time.time()))[-8:]

        result = await erp_client.save_quotation(
            cust_code=order["cust_code"],
            lines=erp_lines,
            upload_ser=upload_ser,
            emp_cd=emp_cd,
            doc_no=order["doc_no"] or "",
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
            conn.execute("UPDATE sale_orders SET status='submitted',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.execute(
                "INSERT INTO sale_order_submissions(order_id,success,erp_slip_no,response) VALUES(?,1,?,?)",
                (order_id, slip_no, json.dumps(result, ensure_ascii=False))
            )
            conn.commit()
            conn.close()
            return ERPSubmitResponse(order_id=order_id, success=True,
                                      erp_slip_no=slip_no,
                                      message=f"ERP 견적서 전송 완료 (전표번호: {slip_no})")
        else:
            if result_details:
                errors = "; ".join([rd.get("TotalError", "") for rd in result_details if not rd.get("IsSuccess")])
                err_msg = errors or "ERP 견적서 저장 실패"
            elif result.get("error"):
                err_msg = result.get("error", "알 수 없는 오류")
                if isinstance(err_msg, dict):
                    err_msg = err_msg.get("Message") or err_msg.get("message") or json.dumps(err_msg, ensure_ascii=False)
            else:
                err_msg = f"SuccessCnt={success_cnt}, FailCnt={fail_cnt}"

            conn.execute(
                "INSERT INTO sale_order_submissions(order_id,success,response) VALUES(?,0,?)",
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
        logger.error(f"[submit-sale-order] 예외: {e}", exc_info=True)
        try:
            conn.execute(
                "INSERT INTO sale_order_submissions(order_id,success,response) VALUES(?,0,?)",
                (order_id, json.dumps({"error": str(e)}, ensure_ascii=False))
            )
            conn.commit()
        except Exception:
            pass
        conn.close()
        return ERPSubmitResponse(order_id=order_id, success=False,
                                  message=f"서버 오류: {str(e)}")


# ─────────────────────────────────────────
#  주문서 목록 조회
# ─────────────────────────────────────────
@router.get("/list")
async def list_sale_orders(limit: int = 20, user: dict = Depends(get_current_user)):
    conn = get_connection()
    rows = conn.execute(
        "SELECT order_id,cust_name,status,created_at,updated_at FROM sale_orders ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"orders": [dict(r) for r in rows]}


@router.get("/{order_id}")
async def get_sale_order(order_id: str, user: dict = Depends(get_current_user)):
    conn = get_connection()
    order = conn.execute("SELECT * FROM sale_orders WHERE order_id=?", (order_id,)).fetchone()
    if not order:
        conn.close()
        raise HTTPException(404)
    lines = conn.execute("SELECT * FROM sale_order_lines WHERE order_id=?", (order_id,)).fetchall()

    submissions = conn.execute(
        "SELECT success, erp_slip_no, submitted_at FROM sale_order_submissions WHERE order_id=? ORDER BY submitted_at DESC",
        (order_id,)
    ).fetchall()

    lines_with_candidates = []
    for l in lines:
        l_dict = dict(l)
        cands = conn.execute(
            "SELECT prod_cd, prod_name, score, match_reason, was_selected FROM sale_order_candidates WHERE line_id=? ORDER BY score DESC LIMIT 3",
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
