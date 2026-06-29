"""
발주서 처리 API 라우터
"""
import uuid
import json
import asyncio
import logging
from datetime import datetime
from typing import AsyncGenerator
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends, Query
from fastapi.responses import StreamingResponse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from models.schemas import (
    OrderCreateRequest, OrderProcessResponse, OrderConfirmRequest,
    ERPSubmitResponse, OrderStatus, OrderLineExtracted, ProductCandidate
)
from agents.extraction import extract_order_lines
from agents.resolution import resolve_product
from services.erp_client import erp_client
from db.database import get_connection
from config import UPLOAD_DIR, CONFIDENCE_THRESHOLD
from security import get_current_user, sanitize_for_prompt, validate_file_upload

router = APIRouter(prefix="/api/orders", tags=["orders"])
logger = logging.getLogger(__name__)

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────
#  발주서 생성 및 처리 (텍스트)
# ─────────────────────────────────────────
@router.post("/process", response_model=OrderProcessResponse)
async def process_order(req: OrderCreateRequest, user: dict = Depends(get_current_user)):
    """발주서 텍스트를 받아 AI 처리 후 결과 반환"""
    order_id = str(uuid.uuid4())[:8].upper()

    if not req.raw_text or not req.raw_text.strip():
        raise HTTPException(400, "발주서 텍스트를 입력해주세요.")

    # 입력 검증 및 프롬프트 보호
    if len(req.raw_text) > 50000:
        raise HTTPException(400, "발주서 텍스트가 너무 깁니다. (최대 50,000자)")
    req.raw_text = sanitize_for_prompt(req.raw_text)

    # DB 저장
    conn = get_connection()
    conn.execute(
        "INSERT INTO orders(order_id,cust_code,cust_name,raw_text,status) VALUES(?,?,?,?,?)",
        (order_id, req.cust_code, req.cust_name, req.raw_text, OrderStatus.PROCESSING)
    )
    conn.commit()

    # 1. 주문 라인 추출 (학습 데이터 활용)
    extracted = await extract_order_lines(req.raw_text, req.cust_name, cust_code=req.cust_code)

    # 2. 각 라인 상품 매칭 (학습 데이터 우선 매칭)
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

        # 최고 신뢰도 후보가 CONFIDENCE_THRESHOLD 이상이면 자동 선택
        if candidates and candidates[0].score >= CONFIDENCE_THRESHOLD:
            auto_select = candidates[0].prod_cd

        # 선택된 후보의 모델명
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

        # DB 라인 저장
        cur = conn.execute(
            "INSERT INTO order_lines(order_id,line_no,raw_text,qty,unit,selected_cd,is_confirmed) VALUES(?,?,?,?,?,?,?)",
            (order_id, line.line_no, line.raw_text, line.qty, line.unit, line.selected_cd, int(line.is_confirmed))
        )
        line_id = cur.lastrowid
        # 후보 저장
        for c in candidates:
            conn.execute(
                "INSERT INTO match_candidates(line_id,prod_cd,prod_name,score,match_reason,was_selected) VALUES(?,?,?,?,?,?)",
                (line_id, c.prod_cd, c.prod_name, c.score, c.match_reason, 1 if c.prod_cd == auto_select else 0)
            )
        conn.commit()

    # 상태 업데이트
    needs_review = any(not l.is_confirmed for l in result_lines)
    new_status = OrderStatus.REVIEWING if needs_review else OrderStatus.CONFIRMED
    conn.execute("UPDATE orders SET status=?,updated_at=datetime('now','localtime') WHERE order_id=?",
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
async def process_image(
    cust_code: str = Form(...),
    cust_name: str = Form(...),
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    """이미지/PDF 발주서를 업로드하고 Claude Vision OCR 후 처리"""
    from agents.ocr import ocr_and_extract

    # ── 파일 확장자 검증
    allowed_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf"}
    suffix = Path(file.filename).suffix.lower() if file.filename else ".jpg"
    if suffix not in allowed_exts:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix}")

    # ── 파일 저장
    order_id = str(uuid.uuid4())[:8].upper()
    save_path = UPLOAD_DIR / f"{order_id}{suffix}"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    content = await file.read()
    # 파일 보안 검증 (MIME 타입 확인)
    validate_file_upload(content, file.filename or "upload.jpg", "image")
    with open(save_path, "wb") as f:
        f.write(content)

    # ── DB 초기 저장
    conn = get_connection()
    conn.execute(
        "INSERT INTO orders(order_id,cust_code,cust_name,raw_text,status) VALUES(?,?,?,?,?)",
        (order_id, cust_code, cust_name, f"[이미지: {file.filename}]", OrderStatus.PROCESSING)
    )
    conn.commit()

    # ── OCR + 추출 (Claude Vision)
    try:
        extracted, raw_text = await ocr_and_extract(str(save_path), cust_name)
    except Exception as e:
        conn.execute("UPDATE orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
        conn.commit()
        conn.close()
        raise HTTPException(500, f"OCR 처리 실패: {e}")

    if not extracted:
        conn.execute("UPDATE orders SET status='failed',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
        conn.commit()
        conn.close()
        raise HTTPException(422, "이미지에서 발주 항목을 인식할 수 없습니다. 이미지가 선명한지 확인해주세요.")

    # raw_text 업데이트
    conn.execute("UPDATE orders SET raw_text=? WHERE order_id=?", (raw_text, order_id))
    conn.commit()

    # ── 상품 매칭 (텍스트 처리와 동일 로직, 학습 데이터 활용)
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
            "INSERT INTO order_lines(order_id,line_no,raw_text,qty,unit,selected_cd,is_confirmed) VALUES(?,?,?,?,?,?,?)",
            (order_id, line.line_no, line.raw_text, line.qty, line.unit, line.selected_cd, int(line.is_confirmed))
        )
        line_id = cur.lastrowid
        for c in candidates:
            conn.execute(
                "INSERT INTO match_candidates(line_id,prod_cd,prod_name,score,match_reason,was_selected) VALUES(?,?,?,?,?,?)",
                (line_id, c.prod_cd, c.prod_name, c.score, c.match_reason, 1 if c.prod_cd == auto_select else 0)
            )
        conn.commit()

    needs_review = any(not l.is_confirmed for l in result_lines)
    new_status = OrderStatus.REVIEWING if needs_review else OrderStatus.CONFIRMED
    conn.execute("UPDATE orders SET status=?,updated_at=datetime('now','localtime') WHERE order_id=?",
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
#  사용자 확인 (라인별 상품 선택)
# ─────────────────────────────────────────
@router.post("/confirm", response_model=dict)
async def confirm_order(req: OrderConfirmRequest):
    """사용자가 검토한 라인별 최종 상품 코드를 저장"""
    conn = get_connection()

    # 주문 존재 여부 확인
    row = conn.execute("SELECT order_id FROM orders WHERE order_id=?", (req.order_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"주문 {req.order_id}을 찾을 수 없습니다.")

    for line in req.lines:
        conn.execute(
            "UPDATE order_lines SET selected_cd=?,qty=?,unit=?,price=?,is_confirmed=1 WHERE order_id=? AND line_no=?",
            (line.prod_cd, line.qty, line.unit, line.price or 0, req.order_id, line.line_no)
        )
        # 피드백 로그 저장 (학습 데이터)
        raw = conn.execute(
            "SELECT raw_text FROM order_lines WHERE order_id=? AND line_no=?",
            (req.order_id, line.line_no)
        ).fetchone()
        if raw:
            cust = conn.execute("SELECT cust_code,cust_name FROM orders WHERE order_id=?",
                                (req.order_id,)).fetchone()
            conn.execute(
                "INSERT INTO feedback_log(cust_code,raw_text,prod_cd,qty,unit) VALUES(?,?,?,?,?)",
                (cust["cust_code"] if cust else "", raw["raw_text"], line.prod_cd, line.qty, line.unit)
            )

    conn.execute(
        "UPDATE orders SET status='confirmed',updated_at=datetime('now','localtime') WHERE order_id=?",
        (req.order_id,)
    )
    conn.commit()
    conn.close()
    return {"success": True, "order_id": req.order_id, "message": "확인 완료. ERP 전송 준비됩니다."}


# ─────────────────────────────────────────
#  ERP 전송
# ─────────────────────────────────────────
@router.post("/submit-erp/{order_id}", response_model=ERPSubmitResponse)
async def submit_to_erp(order_id: str, emp_cd: str = ""):
    """확인된 발주서를 ECOUNT ERP에 판매 전표로 저장 (emp_cd: 로그인한 담당자 코드)"""
    import time
    conn = get_connection()
    try:
        order = conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
        if not order:
            raise HTTPException(404, "주문을 찾을 수 없습니다.")
        if order["status"] not in ("confirmed", "reviewing"):
            raise HTTPException(400, f"ERP 전송 불가 상태: {order['status']}")

        lines = conn.execute(
            "SELECT * FROM order_lines WHERE order_id=? AND is_confirmed=1 AND selected_cd IS NOT NULL",
            (order_id,)
        ).fetchall()

        if not lines:
            raise HTTPException(400, "확인된 라인이 없습니다.")

        # ── ERP에서 품목별 출고단가 자동 조회 ──
        all_prod_cds = list({l["selected_cd"] for l in lines})
        erp_prices: dict = {}
        try:
            erp_prices = await erp_client.get_product_prices(all_prod_cds)
            logger.info(f"[submit_to_erp] 단가 조회 완료: {len(erp_prices)}건 (품목수={len(all_prod_cds)})")
        except Exception as pe:
            logger.warning(f"[submit_to_erp] 단가 조회 실패 (단가 없이 전송): {pe}")

        erp_lines = [
            {
                "prod_cd": l["selected_cd"],
                "qty":     l["qty"],
                "unit":    l["unit"] or "",
                "price":   erp_prices.get(l["selected_cd"], 0),
            }
            for l in lines
        ]

        # UPLOAD_SER_NO: ECOUNT는 숫자만 허용 → timestamp 기반 숫자 사용
        upload_ser = str(int(time.time()))[-8:]

        result = await erp_client.save_sale(
            cust_code=order["cust_code"],
            lines=erp_lines,
            upload_ser=upload_ser,
            emp_cd=emp_cd,
        )

        # ERP 응답 상세 분석 (Status=200 이어도 SuccessCnt=0이면 실패)
        erp_data = result.get("data", {})
        inner   = erp_data.get("Data", {})
        success_cnt = inner.get("SuccessCnt", -1)
        fail_cnt    = inner.get("FailCnt", 0)
        slip_nos    = inner.get("SlipNos", [])
        result_details = inner.get("ResultDetails", [])

        # 실제 성공 여부: success_cnt > 0 이어야 진짜 저장됨
        actually_saved = result.get("success") and (success_cnt > 0)

        if actually_saved:
            slip_no = slip_nos[0] if slip_nos else ""
            conn.execute("UPDATE orders SET status='submitted',updated_at=datetime('now','localtime') WHERE order_id=?", (order_id,))
            conn.execute(
                "INSERT INTO erp_submissions(order_id,success,erp_slip_no,response) VALUES(?,1,?,?)",
                (order_id, slip_no, json.dumps(result, ensure_ascii=False))
            )
            # ── 단가 이력 저장 (ERP에서 조회한 단가 기준으로 upsert)
            cust_code_val = order["cust_code"]
            for l in lines:
                price_val = erp_prices.get(l["selected_cd"], 0)
                if l["selected_cd"] and float(price_val) > 0:
                    conn.execute(
                        """INSERT INTO product_prices(cust_code, prod_cd, price, updated_at)
                           VALUES(?,?,?,datetime('now','localtime'))
                           ON CONFLICT(cust_code, prod_cd)
                           DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at""",
                        (cust_code_val, l["selected_cd"], float(price_val))
                    )

            # ── 자동 학습 파이프라인: ERP 전송 성공 = 확정된 매칭 데이터 ──
            try:
                auto_trained = 0
                for l in lines:
                    if l["selected_cd"] and l["raw_text"]:
                        conn.execute(
                            """INSERT INTO feedback_log(cust_code, raw_text, prod_cd, qty, unit)
                               VALUES(?,?,?,?,?)""",
                            (cust_code_val, l["raw_text"], l["selected_cd"],
                             l["qty"], l["unit"] or "EA")
                        )
                        auto_trained += 1
                if auto_trained > 0:
                    from services.ai_metrics import record_auto_training, record_match_result
                    record_auto_training(cust_code_val, order_id, auto_trained)
                    # 매칭 결과 메트릭도 기록
                    auto_cnt = sum(1 for l in lines if l["is_confirmed"])
                    record_match_result(
                        cust_code=cust_code_val,
                        order_id=order_id,
                        total_lines=len(lines),
                        auto_matched=auto_cnt,
                        manual_fixed=len(lines) - auto_cnt,
                        avg_confidence=0.0,
                    )
                    logger.info(f"[AutoTrain] ERP 전송 성공 → {auto_trained}건 학습 데이터 자동 축적")
            except Exception as train_err:
                logger.warning(f"[AutoTrain] 자동 학습 실패: {train_err}")

            conn.commit()
            conn.close()
            return ERPSubmitResponse(order_id=order_id, success=True,
                                      erp_slip_no=slip_no,
                                      message=f"ERP 전송 완료 (전표번호: {slip_no})")
        else:
            # 오류 메시지 조합
            if result_details:
                errors = "; ".join([rd.get("TotalError", "") for rd in result_details if not rd.get("IsSuccess")])
                err_msg = errors or "ERP 저장 실패"
            elif result.get("error"):
                err_msg = result.get("error", "알 수 없는 오류")
                if isinstance(err_msg, dict):
                    err_msg = err_msg.get("Message") or err_msg.get("message") or json.dumps(err_msg, ensure_ascii=False)
            else:
                err_msg = f"SuccessCnt={success_cnt}, FailCnt={fail_cnt}"

            conn.execute(
                "INSERT INTO erp_submissions(order_id,success,response) VALUES(?,0,?)",
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
        logger.error(f"[submit-erp] 예외 발생: {e}", exc_info=True)
        try:
            conn.execute(
                "INSERT INTO erp_submissions(order_id,success,response) VALUES(?,0,?)",
                (order_id, json.dumps({"error": str(e)}, ensure_ascii=False))
            )
            conn.commit()
        except Exception:
            pass
        conn.close()
        # 500 대신 200으로 반환해서 프론트에서 오류 내용을 볼 수 있게
        return ERPSubmitResponse(order_id=order_id, success=False,
                                  message=f"서버 오류: {str(e)}")


# ─────────────────────────────────────────
#  주문 목록 조회
# ─────────────────────────────────────────
@router.get("/list")
async def list_orders(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: str = Query("", description="상태 필터"),
    cust_code: str = Query("", description="거래처코드 필터"),
):
    conn = get_connection()
    query = "SELECT order_id,cust_name,cust_code,status,created_at,updated_at FROM orders WHERE 1=1"
    params = []

    if status:
        query += " AND status=?"
        params.append(status)
    if cust_code:
        query += " AND cust_code=?"
        params.append(cust_code)

    # 전체 건수
    count_row = conn.execute(
        query.replace("SELECT order_id,cust_name,cust_code,status,created_at,updated_at", "SELECT COUNT(*) as cnt"),
        params
    ).fetchone()
    total = count_row["cnt"] if count_row else 0

    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return {"orders": [dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


@router.get("/{order_id}")
async def get_order(order_id: str):
    conn = get_connection()
    order = conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
    if not order:
        conn.close()
        raise HTTPException(404)
    lines = conn.execute("SELECT * FROM order_lines WHERE order_id=?", (order_id,)).fetchall()

    # ERP 전송 이력
    submissions = conn.execute(
        "SELECT success, erp_slip_no, submitted_at FROM erp_submissions WHERE order_id=? ORDER BY submitted_at DESC",
        (order_id,)
    ).fetchall()

    # 각 라인별 매칭 후보 (상위 3개)
    lines_with_candidates = []
    for l in lines:
        l_dict = dict(l)
        cands = conn.execute(
            "SELECT prod_cd, prod_name, score, match_reason, was_selected FROM match_candidates WHERE line_id=? ORDER BY score DESC LIMIT 3",
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
