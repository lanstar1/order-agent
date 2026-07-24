"""
중국오더(BOR) 자동화 API 라우터
- rest list / 최근 오더 메일 수집 → 수요 계산 → 오더 초안(draft) 생성 → 검토/수정 → xlsx 다운로드 → 승인 후 발송
- 발송(send)은 반드시 body.confirm === true + 로그인 사용자만 가능 (스케줄러는 send 경로를 호출하지 않음)
"""
import base64
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from db.database import get_connection
from security import get_current_user

from services.bor_order import db as bor_db
from services.bor_order.mail_collector import collect_restlist, collect_recent_orders
from services.bor_order.engine import generate_draft
from services.bor_order.xlsx_writer import build_order_xlsx
from services.bor_order.sender import send_order_mail

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bor-order", tags=["bor-order"])
KST = timezone(timedelta(hours=9))

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# bor_config에서 수정 허용하는 키 (그 외 키는 무시)
_ALLOWED_CONFIG_KEYS = {
    "target_months", "bulk_factors", "recipient", "cc",
    "exclude_models", "demand_overrides",
}


# ─── Pydantic 모델 ──────────────────────────────────────

class CollectRequest(BaseModel):
    days_back: Optional[int] = None


class GenerateRequest(BaseModel):
    target_months: Optional[float] = None


class LineUpdate(BaseModel):
    qty_final: Optional[float] = None
    included: Optional[int] = None  # 0/1 (bool도 허용 — int로 강제 변환됨)


# ─── 내부 유틸 ───────────────────────────────────────────

def _loads_or_none(text):
    """JSON 문자열 파싱 (실패 시 None)"""
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _get_draft_row(conn, draft_id: int):
    row = conn.execute(
        "SELECT * FROM bor_order_drafts WHERE id = ?", (draft_id,)
    ).fetchone()
    if not row:
        raise HTTPException(404, "오더 초안을 찾을 수 없습니다")
    return row


def _regenerate_draft_xlsx(conn, draft_id: int) -> str:
    """포함(included=1) 라인 기준으로 xlsx 재생성 → drafts에 저장. 파일명 반환"""
    rows = conn.execute(
        "SELECT * FROM bor_order_draft_lines "
        "WHERE draft_id = ? AND included = 1 AND qty_final > 0 "
        "ORDER BY line_no",
        (draft_id,),
    ).fetchall()
    if not rows:
        return ""
    lines = [dict(r) for r in rows]
    order_date = datetime.now(KST).strftime("%Y-%m-%d")
    filename, xlsx_bytes = build_order_xlsx(lines, order_date)
    conn.execute(
        "UPDATE bor_order_drafts SET xlsx_filename = ?, xlsx_b64 = ? WHERE id = ?",
        (filename, base64.b64encode(xlsx_bytes).decode("ascii"), draft_id),
    )
    conn.commit()
    return filename


def _get_next_weekly_run() -> str:
    """주간 스케줄러(bor_order_weekly) 다음 실행 시각 (KST)"""
    try:
        from services.scheduler_service import _scheduler_state
        scheduler = _scheduler_state.get("scheduler")
        if scheduler:
            job = scheduler.get_job("bor_order_weekly")
            if job and job.next_run_time:
                return job.next_run_time.astimezone(KST).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return ""


# ─── 상태 요약 ───────────────────────────────────────────

@router.get("/status")
def api_status():
    """최신 rest 스냅샷 요약 + 최근 오더 수집 현황 + 최신 draft 요약 + 스케줄러 다음 실행"""
    conn = get_connection()
    try:
        # 1. rest list 최신 스냅샷 (계약상 최신 스냅샷 1개만 유지됨)
        restlist = {}
        try:
            row = conn.execute(
                "SELECT snapshot_date, source_file, COUNT(*) AS line_count, "
                "SUM(rest_qty * unit_price) AS total_amount_usd, "
                "SUM(rest_qty) AS total_rest_qty, MAX(fetched_at) AS fetched_at "
                "FROM bor_restlist_lines GROUP BY snapshot_date, source_file "
                "ORDER BY snapshot_date DESC LIMIT 1"
            ).fetchone()
            if row:
                restlist = {
                    "snapshot_date": row["snapshot_date"],
                    "source_file": row["source_file"],
                    "line_count": row["line_count"],
                    "total_amount_usd": round(row["total_amount_usd"] or 0, 2),
                    "total_rest_qty": row["total_rest_qty"] or 0,
                    "fetched_at": row["fetched_at"],
                }
        except Exception as e:
            logger.warning(f"[BOR오더] rest 스냅샷 조회 실패: {e}")

        # 2. 최근 오더 수집 현황 (채널별)
        recent_orders = {"total_lines": 0, "by_source": {}, "last_fetched_at": ""}
        try:
            rows = conn.execute(
                "SELECT source, COUNT(*) AS cnt, MAX(order_date) AS last_order_date, "
                "MAX(fetched_at) AS last_fetched "
                "FROM bor_recent_orders GROUP BY source"
            ).fetchall()
            for r in rows:
                recent_orders["by_source"][r["source"]] = {
                    "lines": r["cnt"],
                    "last_order_date": r["last_order_date"],
                }
                recent_orders["total_lines"] += r["cnt"]
                if (r["last_fetched"] or "") > recent_orders["last_fetched_at"]:
                    recent_orders["last_fetched_at"] = r["last_fetched"]
        except Exception as e:
            logger.warning(f"[BOR오더] 최근 오더 현황 조회 실패: {e}")

        # 3. 최신 draft 요약
        latest_draft = {}
        try:
            row = conn.execute(
                "SELECT id, created_at, status, xlsx_filename, summary_json "
                "FROM bor_order_drafts ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                cnt = conn.execute(
                    "SELECT COUNT(*) AS total, "
                    "SUM(CASE WHEN included = 1 THEN 1 ELSE 0 END) AS included_cnt "
                    "FROM bor_order_draft_lines WHERE draft_id = ?",
                    (row["id"],),
                ).fetchone()
                latest_draft = {
                    "id": row["id"],
                    "created_at": row["created_at"],
                    "status": row["status"],
                    "xlsx_filename": row["xlsx_filename"],
                    "summary": _loads_or_none(row["summary_json"]),
                    "line_count": cnt["total"] if cnt else 0,
                    "included_count": (cnt["included_cnt"] or 0) if cnt else 0,
                }
        except Exception as e:
            logger.warning(f"[BOR오더] 최신 draft 조회 실패: {e}")

        return {
            "restlist": restlist,
            "recent_orders": recent_orders,
            "latest_draft": latest_draft,
            "next_scheduled_run": _get_next_weekly_run(),
        }
    finally:
        conn.close()


# ─── 메일 수집 ───────────────────────────────────────────

@router.post("/collect")
def api_collect(body: CollectRequest = None, user: dict = Depends(get_current_user)):
    """rest list + 최근 오더(양 채널) 메일 수집"""
    days_back = body.days_back if body else None
    try:
        if days_back:
            rest_result = collect_restlist(days_back=days_back)
            orders_result = collect_recent_orders(days_back=days_back)
        else:
            rest_result = collect_restlist()          # 기본 60일
            orders_result = collect_recent_orders()   # 기본 90일
        return {"status": "ok", "restlist": rest_result, "recent_orders": orders_result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[BOR오더] 메일 수집 실패: {e}", exc_info=True)
        raise HTTPException(500, f"메일 수집 실패: {str(e)}")


# ─── 초안 생성 ───────────────────────────────────────────

@router.post("/generate")
async def api_generate(body: GenerateRequest = None, user: dict = Depends(get_current_user)):
    """오더 초안 생성 (수요 계산 → 라인 산출 → xlsx 생성)"""
    target_months = body.target_months if body else None
    try:
        draft_id = await generate_draft(target_months=target_months)
        return {"status": "ok", "draft_id": draft_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[BOR오더] 초안 생성 실패: {e}", exc_info=True)
        raise HTTPException(500, f"초안 생성 실패: {str(e)}")


# ─── 초안 조회/수정/삭제 ─────────────────────────────────

@router.get("/drafts")
def api_list_drafts(limit: int = 10):
    """초안 목록 (최신순, xlsx 본문 제외)"""
    limit = max(1, min(limit, 50))
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, created_at, status, params_json, summary_json, "
            "xlsx_filename, sent_at, sent_to "
            "FROM bor_order_drafts ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "created_at": r["created_at"],
                "status": r["status"],
                "params": _loads_or_none(r["params_json"]),
                "summary": _loads_or_none(r["summary_json"]),
                "xlsx_filename": r["xlsx_filename"],
                "sent_at": r["sent_at"],
                "sent_to": r["sent_to"],
            })
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


@router.get("/drafts/{draft_id}")
def api_get_draft(draft_id: int):
    """초안 상세 (라인 포함)"""
    conn = get_connection()
    try:
        row = _get_draft_row(conn, draft_id)
        lines = conn.execute(
            "SELECT * FROM bor_order_draft_lines WHERE draft_id = ? ORDER BY line_no",
            (draft_id,),
        ).fetchall()
        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "status": row["status"],
            "params": _loads_or_none(row["params_json"]),
            "summary": _loads_or_none(row["summary_json"]),
            "xlsx_filename": row["xlsx_filename"],
            "sent_at": row["sent_at"],
            "sent_to": row["sent_to"],
            "lines": [dict(l) for l in lines],
        }
    finally:
        conn.close()


@router.delete("/drafts/{draft_id}")
def api_discard_draft(draft_id: int, user: dict = Depends(get_current_user)):
    """초안 폐기 (status='discarded' — 물리 삭제 아님)"""
    conn = bor_db.get_write_connection()
    try:
        row = _get_draft_row(conn, draft_id)
        if row["status"] == "sent":
            raise HTTPException(400, "이미 발송된 초안은 폐기할 수 없습니다")
        conn.execute(
            "UPDATE bor_order_drafts SET status = 'discarded' WHERE id = ?",
            (draft_id,),
        )
        conn.commit()
        return {"status": "ok", "draft_id": draft_id, "draft_status": "discarded"}
    finally:
        conn.close()


@router.patch("/drafts/{draft_id}/lines/{line_id}")
def api_update_line(draft_id: int, line_id: int, body: LineUpdate,
                    user: dict = Depends(get_current_user)):
    """초안 라인 수정 (qty_final / included) 후 xlsx 재생성"""
    if body.qty_final is None and body.included is None:
        raise HTTPException(400, "수정할 항목이 없습니다 (qty_final 또는 included)")
    if body.qty_final is not None and body.qty_final < 0:
        raise HTTPException(400, "qty_final은 0 이상이어야 합니다")

    conn = bor_db.get_write_connection()
    try:
        draft = _get_draft_row(conn, draft_id)
        if draft["status"] in ("sent", "discarded"):
            raise HTTPException(400, f"'{draft['status']}' 상태의 초안은 수정할 수 없습니다")

        line = conn.execute(
            "SELECT id FROM bor_order_draft_lines WHERE id = ? AND draft_id = ?",
            (line_id, draft_id),
        ).fetchone()
        if not line:
            raise HTTPException(404, "초안 라인을 찾을 수 없습니다")

        sets, params = [], []
        if body.qty_final is not None:
            sets.append("qty_final = ?")
            params.append(body.qty_final)
        if body.included is not None:
            sets.append("included = ?")
            params.append(1 if body.included else 0)
        params.extend([line_id, draft_id])
        conn.execute(
            f"UPDATE bor_order_draft_lines SET {', '.join(sets)} "
            "WHERE id = ? AND draft_id = ?",
            tuple(params),
        )
        conn.commit()

        # 수정 반영된 xlsx 재생성 (실패해도 라인 수정 자체는 유지)
        xlsx_filename = ""
        xlsx_warning = ""
        try:
            xlsx_filename = _regenerate_draft_xlsx(conn, draft_id)
        except Exception as e:
            logger.warning(f"[BOR오더] xlsx 재생성 실패 (라인 수정은 반영됨): {e}")
            xlsx_warning = f"xlsx 재생성 실패: {str(e)}"

        result = {"status": "ok", "draft_id": draft_id, "line_id": line_id,
                  "xlsx_filename": xlsx_filename}
        if xlsx_warning:
            result["warning"] = xlsx_warning
        return result
    finally:
        conn.close()


# ─── xlsx 다운로드 ───────────────────────────────────────

@router.get("/drafts/{draft_id}/xlsx")
def api_download_xlsx(draft_id: int):
    """초안 xlsx 파일 다운로드"""
    conn = get_connection()
    try:
        row = _get_draft_row(conn, draft_id)
        if not row["xlsx_b64"]:
            raise HTTPException(404, "생성된 xlsx 파일이 없습니다")
        data = base64.b64decode(row["xlsx_b64"])
        filename = row["xlsx_filename"] or f"new order-draft{draft_id}.xlsx"
    finally:
        conn.close()

    return StreamingResponse(
        io.BytesIO(data),
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─── 발송 (수동 승인 전용) ───────────────────────────────

@router.post("/drafts/{draft_id}/send")
async def api_send_draft(draft_id: int, request: Request,
                         user: dict = Depends(get_current_user)):
    """초안 메일 발송 — body.confirm === true 필수 (아니면 400).

    이 엔드포인트는 로그인 사용자의 명시적 확인(confirm)으로만 실행되며,
    주간 스케줄러(bor_order_weekly)는 collect/generate만 호출하고
    send 경로(send_order_mail)는 어디서도 자동 호출하지 않는다.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    # confirm이 정확히 true(boolean)가 아니면 400
    if body.get("confirm") is not True:
        raise HTTPException(400, "발송하려면 body에 confirm: true 가 필요합니다")

    conn = get_connection()
    try:
        draft = _get_draft_row(conn, draft_id)
        if draft["status"] == "sent":
            raise HTTPException(400, "이미 발송된 초안입니다")
        if draft["status"] == "discarded":
            raise HTTPException(400, "폐기된 초안은 발송할 수 없습니다")
        if not draft["xlsx_b64"]:
            raise HTTPException(400, "생성된 xlsx 파일이 없어 발송할 수 없습니다")
    finally:
        conn.close()

    config = bor_db.get_config()
    recipient = (body.get("recipient") or config.get("recipient") or "").strip()
    cc = body.get("cc")
    if cc is None:
        cc = config.get("cc") or ""
    body_note = str(body.get("body_note") or "")

    if not recipient:
        raise HTTPException(400, "수신자(recipient)가 없습니다 — body 또는 config에 설정하세요")

    try:
        result = send_order_mail(draft_id, recipient=recipient, cc=cc, body_note=body_note)
        logger.info(f"[BOR오더] draft {draft_id} 발송 완료 → {recipient} (by {user.get('emp_cd', '')})")
        return {"status": "ok", "draft_id": draft_id, "recipient": recipient, "result": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[BOR오더] draft {draft_id} 발송 실패: {e}", exc_info=True)
        raise HTTPException(500, f"메일 발송 실패: {str(e)}")


# ─── 설정 ────────────────────────────────────────────────

@router.get("/config")
def api_get_config():
    """BOR 설정 조회 (기본값 병합)"""
    return {"config": bor_db.get_config()}


@router.put("/config")
async def api_put_config(request: Request, user: dict = Depends(get_current_user)):
    """BOR 설정 저장 — 허용 키만 반영, 나머지는 무시"""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON body가 필요합니다")
    if not isinstance(body, dict):
        raise HTTPException(400, "JSON object body가 필요합니다")

    updated = []
    for key, value in body.items():
        if key not in _ALLOWED_CONFIG_KEYS:
            continue
        if value is None:
            value = ""
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        bor_db.set_config(key, value)
        updated.append(key)

    return {"status": "ok", "updated": updated, "config": bor_db.get_config()}


# ─── 판매 이력 시드 (수요 계산용 공백 구간 채움) ──────────────
#
# 이카운트 판매현황 CSV(연/월/일="YYYYMMDD-순번", 품목코드, 거래처코드, 모델명,
# 판매수량/수량, 안전재고수량 6컬럼)를 sales_records에 주입한다.
# - 지정한 날짜 창(date_from~date_to) 안의 행만 삽입
# - 창 안에 기존 데이터가 있으면 409 (이중 집계 방지, force="true"로만 우회)
# - 기존 행은 절대 삭제/수정하지 않음. slip_no는 SEED- 접두로 실전표와 구분.

@router.post("/admin/seed-sales")
def api_seed_sales(
    file: UploadFile = File(...),
    date_from: str = Form(...),
    date_to: str = Form(...),
    confirm: str = Form(""),
    force: str = Form(""),
    replace: str = Form(""),
    user: dict = Depends(get_current_user),
):
    if confirm != "true":
        raise HTTPException(status_code=400, detail="confirm='true'가 필요합니다")

    d_from = date_from.replace("-", "").strip()
    d_to = date_to.replace("-", "").strip()
    if not (d_from.isdigit() and d_to.isdigit() and len(d_from) == 8 and len(d_to) == 8 and d_from <= d_to):
        raise HTTPException(status_code=400, detail="date_from/date_to는 YYYYMMDD 형식이어야 합니다")

    import csv as csv_mod
    import re

    raw = file.file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp949", errors="replace")

    reader = csv_mod.reader(io.StringIO(text))
    header = None
    qty_idx = code_idx = model_idx = cust_idx = None
    rows_to_insert = []
    skipped = 0
    slip_re = re.compile(r"^\d{8}-\S+")

    for cols in reader:
        if not cols:
            continue
        if header is None:
            if "품목코드" in [c.strip() for c in cols]:
                header = [c.strip() for c in cols]
                code_idx = header.index("품목코드")
                cust_idx = header.index("거래처코드") if "거래처코드" in header else None
                model_idx = header.index("모델명") if "모델명" in header else None
                qty_idx = header.index("판매수량") if "판매수량" in header else (
                    header.index("수량") if "수량" in header else None)
                if qty_idx is None:
                    raise HTTPException(status_code=400, detail="판매수량/수량 컬럼을 찾지 못했습니다")
            continue
        date_raw = str(cols[0]).strip()
        if not slip_re.match(date_raw):
            continue  # 소계/푸터 행
        slip_date = date_raw.split("-")[0]
        if not (d_from <= slip_date <= d_to):
            skipped += 1
            continue
        try:
            qty = float(str(cols[qty_idx]).strip().replace(",", "") or 0)
        except ValueError:
            continue
        rows_to_insert.append((
            slip_date,
            f"SEED-{date_raw}-r{len(rows_to_insert)}",
            str(cols[code_idx]).strip() if code_idx is not None and code_idx < len(cols) else "",
            str(cols[cust_idx]).strip() if cust_idx is not None and cust_idx < len(cols) else "",
            str(cols[model_idx]).strip() if model_idx is not None and model_idx < len(cols) else "",
            qty,
            "BOR demand seed",
        ))

    if header is None:
        raise HTTPException(status_code=400, detail="품목코드 헤더 행을 찾지 못했습니다 (이카운트 판매현황 CSV인지 확인)")
    if not rows_to_insert:
        raise HTTPException(status_code=400, detail=f"창({d_from}~{d_to}) 안에 삽입할 행이 없습니다 (창 밖 {skipped}행)")

    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT COUNT(*) FROM sales_records WHERE slip_date >= ? AND slip_date <= ?",
            (d_from, d_to),
        ).fetchone()[0]
        replaced = 0
        if existing and replace == "true":
            # 기존 행을 백업 테이블에 보존한 뒤 창 안에서 삭제 (부분 수집 데이터 교체용)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sales_records_seed_backup (
                    id INTEGER, slip_date TEXT, slip_no TEXT, item_code TEXT,
                    customer_name TEXT, item_name TEXT, model_name TEXT,
                    quantity REAL, unit_price REAL, supply_amount REAL, vat REAL,
                    total_amount REAL, cost_price REAL, warehouse TEXT,
                    account_date TEXT, item_group TEXT, note TEXT, staff_name TEXT,
                    customer_group TEXT, safety_stock REAL, display_code TEXT,
                    gross_profit REAL, backed_up_at TEXT
                )""")
            conn.execute(
                "INSERT INTO sales_records_seed_backup "
                "SELECT id, slip_date, slip_no, item_code, customer_name, item_name, "
                "model_name, quantity, unit_price, supply_amount, vat, total_amount, "
                "cost_price, warehouse, account_date, item_group, note, staff_name, "
                "customer_group, safety_stock, display_code, gross_profit, ? "
                "FROM sales_records WHERE slip_date >= ? AND slip_date <= ?",
                (datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"), d_from, d_to),
            )
            conn.execute(
                "DELETE FROM sales_records WHERE slip_date >= ? AND slip_date <= ?",
                (d_from, d_to),
            )
            replaced = existing
        elif existing and force != "true":
            raise HTTPException(
                status_code=409,
                detail=f"창({d_from}~{d_to})에 기존 판매 데이터 {existing}행이 있습니다. "
                       f"이중 집계 위험 — 창을 조정하거나 replace='true'(백업 후 교체) 또는 "
                       f"force='true'(그대로 추가)를 사용하세요.")

        CHUNK = 500
        inserted = 0
        for i in range(0, len(rows_to_insert), CHUNK):
            chunk = rows_to_insert[i:i + CHUNK]
            placeholders = ",".join(["(?,?,?,?,?,?,?)"] * len(chunk))
            params = [v for row in chunk for v in row]
            conn.execute(
                "INSERT INTO sales_records (slip_date, slip_no, item_code, customer_name, "
                "model_name, quantity, note) VALUES " + placeholders,
                params,
            )
            inserted += len(chunk)
        conn.commit()
    finally:
        conn.close()

    months = sorted({r[0][:6] for r in rows_to_insert})
    logger.info(f"[BOR오더] 판매 시드 완료: {inserted}행 ({d_from}~{d_to}, by {user.get('name')})")
    return {"status": "ok", "inserted": inserted, "window": [d_from, d_to],
            "months": months, "skipped_out_of_window": skipped,
            "existing_in_window_before": existing}
