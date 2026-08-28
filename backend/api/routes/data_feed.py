"""
데이터 피드 + 상품 최신 거래 조회 API

- NAS 수집기가 매일 새벽 ECOUNT 판매/구매현황 CSV를 업로드 (X-Feed-Key 인증, raw body)
- 쇼핑몰 주문 처리 시 상품의 최신 거래 정보(최근 판매단가·구매처·입고단가)를 실시간 조회

적재는 파일에 포함된 전표일자(+dates 파라미터) 단위로 통째 교체(replace-by-date)라서
같은 구간을 몇 번 올려도 중복이 생기지 않는다. 판매 CSV는 구양식(19컬럼)과
현행 웹양식(거래처코드 포함 20컬럼)을 헤더명 기준으로 모두 지원한다.
"""
import csv
import io
import logging

from fastapi import APIRouter, Header, HTTPException, Query, Request

from config import DATA_FEED_KEY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datafeed", tags=["Data Feed"])


# ──────────────────────────── 인증 ────────────────────────────

def _check_feed_key(x_feed_key):
    if not DATA_FEED_KEY:
        raise HTTPException(status_code=503, detail="DATA_FEED_KEY 미설정 — 피드 비활성 상태")
    if x_feed_key != DATA_FEED_KEY:
        raise HTTPException(status_code=401, detail="X-Feed-Key 인증 실패")


def _feed_or_user(request: Request, x_feed_key):
    """X-Feed-Key(머신) 또는 JWT(웹 UI/에이전트) 중 하나로 인증."""
    if DATA_FEED_KEY and x_feed_key == DATA_FEED_KEY:
        return {"emp_cd": "datafeed", "name": "feed"}
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        from security import decode_token
        payload = decode_token(auth[7:])
        return {"emp_cd": payload["sub"], "name": payload.get("name", "")}
    raise HTTPException(status_code=401, detail="X-Feed-Key 또는 Bearer 토큰이 필요합니다")


# ──────────────────────────── 파싱 ────────────────────────────

def _decode_csv(content: bytes) -> str:
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            return content.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    raise HTTPException(status_code=400, detail="CSV 인코딩을 판별할 수 없습니다")


def _num(v) -> float:
    s = str(v).strip().replace(",", "").replace(" ", "")
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _norm_slip(raw: str):
    """'2026/08/28-3' | '20260828-3' → ('20260828', '20260828-3'). 연도 없으면 None."""
    raw = str(raw).strip()
    if "-" not in raw:
        return None
    date_part, _, no = raw.partition("-")
    digits = date_part.replace("/", "").replace(".", "")
    if len(digits) != 8 or not digits.isdigit():
        return None            # 연도 없는 '월/일' 형식 등은 스킵 (수집기가 정규화해서 보냄)
    return digits, f"{digits}-{no.strip()}"


class _HeaderMap:
    def __init__(self, header_row):
        self.names = [str(c).strip() for c in header_row]

    def find(self, exact=(), contains=()):
        for i, n in enumerate(self.names):
            if n in exact:
                return i
        for i, n in enumerate(self.names):
            if n and any(c in n for c in contains):
                return i
        return None

    def get(self, row, idx, default=""):
        if idx is None or idx >= len(row):
            return default
        return str(row[idx]).strip()


def _iter_csv(text: str):
    header = None
    for line in csv.reader(io.StringIO(text)):
        if not line:
            continue
        if header is None:
            if any(str(c).strip() == "품목코드" for c in line):
                header = _HeaderMap(line)
            continue
        yield header, line


def parse_sales_csv(text: str) -> list:
    rows = []
    for h, line in _iter_csv(text):
        slip = _norm_slip(line[0] if line else "")
        if not slip:
            continue
        i_code = h.find(exact=("품목코드",))
        i_cust = h.find(contains=("매처명", "거래처명"))
        i_name = h.find(contains=("품명",))
        i_model = h.find(exact=("모델명",))
        i_qty = h.find(exact=("수량",))
        i_price = h.find(exact=("단가",))
        i_supply = h.find(exact=("공급가액",))
        i_vat = h.find(exact=("부가세",))
        i_total = h.find(exact=("합 계", "합계"))
        i_cost = h.find(exact=("입고단가",))
        i_wh = h.find(exact=("창고",))
        i_acc = h.find(exact=("회계반영일자",))
        i_igrp = h.find(contains=("품목그룹",))
        i_note = h.find(contains=("비고",))
        i_staff = h.find(exact=("전표출력",), contains=("전표출력",))
        i_cgrp = h.find(contains=("거래처그룹",))
        i_safe = h.find(contains=("안전재고",))
        i_disp = h.find(exact=("진열코드",))
        qty = _num(h.get(line, i_qty))
        supply = _num(h.get(line, i_supply))
        cost = _num(h.get(line, i_cost))
        rows.append({
            "slip_date": slip[0], "slip_no": slip[1],
            "item_code": h.get(line, i_code),
            "customer_name": h.get(line, i_cust),
            "item_name": h.get(line, i_name),
            "model_name": h.get(line, i_model),
            "quantity": qty,
            "unit_price": _num(h.get(line, i_price)),
            "supply_amount": supply,
            "vat": _num(h.get(line, i_vat)),
            "total_amount": _num(h.get(line, i_total)),
            "cost_price": cost,
            "warehouse": h.get(line, i_wh),
            "account_date": h.get(line, i_acc),
            "item_group": h.get(line, i_igrp),
            "note": h.get(line, i_note),
            "staff_name": h.get(line, i_staff),
            "customer_group": h.get(line, i_cgrp),
            "safety_stock": _num(h.get(line, i_safe)),
            "display_code": h.get(line, i_disp),
            "gross_profit": supply - (cost * qty),
        })
    return rows


def parse_purchase_csv(text: str) -> list:
    rows = []
    for h, line in _iter_csv(text):
        slip = _norm_slip(line[0] if line else "")
        if not slip:
            continue
        rows.append({
            "slip_date": slip[0], "slip_no": slip[1],
            "item_code": h.get(line, h.find(exact=("품목코드",))),
            "item_name": h.get(line, h.find(contains=("품명",))),
            "spec": h.get(line, h.find(exact=("규격",), contains=("규격",))),
            "quantity": _num(h.get(line, h.find(exact=("수량",)))),
            "unit_price": _num(h.get(line, h.find(exact=("단가",)))),
            "partner_price": _num(h.get(line, h.find(exact=("파트너가",)))),
            "cost_price": _num(h.get(line, h.find(exact=("입고단가",)))),
            "supply_amount": _num(h.get(line, h.find(exact=("공급가액",)))),
            "vat": _num(h.get(line, h.find(exact=("부가세",)))),
            "total_amount": _num(h.get(line, h.find(exact=("합 계", "합계")))),
            "supplier_name": h.get(line, h.find(exact=("구매처",), contains=("구매처",))),
        })
    return rows


# ──────────────────────────── 적재 (날짜 교체) ────────────────────────────

def _parse_dates_param(dates: str) -> set:
    out = set()
    for d in (dates or "").replace(" ", "").split(","):
        digits = d.replace("/", "").replace(".", "")
        if len(digits) == 8 and digits.isdigit():
            out.add(digits)
    return out


def _replace_by_date(table: str, rows: list, extra_dates: set, insert_sql: str, to_params):
    from db.database import get_connection
    conn = get_connection()
    try:
        dates = sorted({r["slip_date"] for r in rows} | extra_dates)
        if dates:
            ph = ",".join(["?"] * len(dates))
            conn.execute(f"DELETE FROM {table} WHERE slip_date IN ({ph})", tuple(dates))
        for r in rows:
            conn.execute(insert_sql, to_params(r))
        conn.commit()
        return dates
    finally:
        conn.close()


_SALES_INSERT = """
    INSERT INTO sales_records (
        slip_date, slip_no, item_code, customer_name, item_name,
        model_name, quantity, unit_price, supply_amount, vat,
        total_amount, cost_price, warehouse, account_date, item_group,
        note, staff_name, customer_group, safety_stock, display_code, gross_profit
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

_PURCHASE_INSERT = """
    INSERT INTO purchase_records (
        slip_date, slip_no, item_code, item_name, spec,
        quantity, unit_price, partner_price, cost_price,
        supply_amount, vat, total_amount, supplier_name
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


@router.post("/sales-csv")
async def feed_sales_csv(request: Request, dates: str = Query(""),
                         x_feed_key: str = Header(None)):
    """판매현황 CSV(raw body) 적재. dates=YYYYMMDD,...는 0건 날짜 삭제용."""
    _check_feed_key(x_feed_key)
    rows = parse_sales_csv(_decode_csv(await request.body()))
    replaced = _replace_by_date(
        "sales_records", rows, _parse_dates_param(dates), _SALES_INSERT,
        lambda r: (r["slip_date"], r["slip_no"], r["item_code"], r["customer_name"],
                   r["item_name"], r["model_name"], r["quantity"], r["unit_price"],
                   r["supply_amount"], r["vat"], r["total_amount"], r["cost_price"],
                   r["warehouse"], r["account_date"], r["item_group"], r["note"],
                   r["staff_name"], r["customer_group"], r["safety_stock"],
                   r["display_code"], r["gross_profit"]))
    logger.info(f"[DataFeed] 판매 적재 {len(rows)}행, 교체일자 {replaced}")
    return {"success": True, "rows": len(rows), "replaced_dates": replaced}


@router.post("/purchase-csv")
async def feed_purchase_csv(request: Request, dates: str = Query(""),
                            x_feed_key: str = Header(None)):
    """구매현황 CSV(raw body) 적재."""
    _check_feed_key(x_feed_key)
    rows = parse_purchase_csv(_decode_csv(await request.body()))
    replaced = _replace_by_date(
        "purchase_records", rows, _parse_dates_param(dates), _PURCHASE_INSERT,
        lambda r: (r["slip_date"], r["slip_no"], r["item_code"], r["item_name"],
                   r["spec"], r["quantity"], r["unit_price"], r["partner_price"],
                   r["cost_price"], r["supply_amount"], r["vat"], r["total_amount"],
                   r["supplier_name"]))
    logger.info(f"[DataFeed] 구매 적재 {len(rows)}행, 교체일자 {replaced}")
    return {"success": True, "rows": len(rows), "replaced_dates": replaced}


# ──────────────────────────── 상품 최신 거래 조회 ────────────────────────────

def _rowdicts(cur, cols):
    out = []
    for row in cur.fetchall():
        out.append({c: row[c] for c in cols})
    return out


@router.get("/product-latest")
async def product_latest(request: Request, q: str = Query(..., min_length=1),
                         limit: int = Query(5, ge=1, le=50),
                         x_feed_key: str = Header(None)):
    """
    품목코드/모델명/품명으로 상품을 찾아 최신 거래 정보를 반환.
    쇼핑몰 주문 인입 시 이 엔드포인트 하나로 최근 판매단가·최근 구매처·입고단가를 얻는다.
    """
    _feed_or_user(request, x_feed_key)
    from db.database import get_connection
    conn = get_connection()
    try:
        qs = q.strip()
        like = f"%{qs}%"

        # 1) 품목 확정: 판매/구매에서 정확 일치 우선, 없으면 부분 일치
        exact_s = conn.execute(
            "SELECT item_code FROM sales_records "
            "WHERE (item_code=? OR model_name=?) AND slip_no NOT LIKE 'SEED-%' "
            "ORDER BY slip_date DESC LIMIT 1", (qs, qs)).fetchone()
        exact_p = None
        if not exact_s:
            exact_p = conn.execute(
                "SELECT item_code FROM purchase_records WHERE item_code=? OR spec=? "
                "ORDER BY slip_date DESC LIMIT 1", (qs, qs)).fetchone()

        candidates = []
        if exact_s or exact_p:
            item_code = (exact_s or exact_p)["item_code"]
        else:
            cur = conn.execute(
                "SELECT item_code, MAX(item_name) AS item_name, MAX(model_name) AS model_name, "
                "MAX(slip_date) AS last_date "
                "FROM sales_records "
                "WHERE (item_code LIKE ? OR model_name LIKE ? OR item_name LIKE ?) "
                "AND slip_no NOT LIKE 'SEED-%' "
                "GROUP BY item_code ORDER BY last_date DESC LIMIT 6",
                (like, like, like))
            candidates = _rowdicts(cur, ["item_code", "item_name", "model_name", "last_date"])
            if not candidates:
                cur = conn.execute(
                    "SELECT item_code, MAX(item_name) AS item_name, MAX(spec) AS model_name, "
                    "MAX(slip_date) AS last_date "
                    "FROM purchase_records "
                    "WHERE item_code LIKE ? OR spec LIKE ? OR item_name LIKE ? "
                    "GROUP BY item_code ORDER BY last_date DESC LIMIT 6",
                    (like, like, like))
                candidates = _rowdicts(cur, ["item_code", "item_name", "model_name", "last_date"])
            if not candidates:
                return {"success": False, "query": qs,
                        "error": "일치하는 품목이 없습니다", "candidates": []}
            item_code = candidates[0]["item_code"]

        # 2) 최신 거래 이력 (최신순)
        cur = conn.execute(
            "SELECT slip_date, slip_no, customer_name, quantity, unit_price, "
            "supply_amount, warehouse, item_name, model_name, cost_price "
            "FROM sales_records WHERE item_code=? AND slip_no NOT LIKE 'SEED-%' "
            "ORDER BY slip_date DESC, id DESC LIMIT ?", (item_code, limit))
        recent_sales = _rowdicts(cur, ["slip_date", "slip_no", "customer_name", "quantity",
                                       "unit_price", "supply_amount", "warehouse",
                                       "item_name", "model_name", "cost_price"])
        cur = conn.execute(
            "SELECT slip_date, slip_no, supplier_name, quantity, unit_price, "
            "partner_price, cost_price, supply_amount, item_name, spec "
            "FROM purchase_records WHERE item_code=? "
            "ORDER BY slip_date DESC, id DESC LIMIT ?", (item_code, limit))
        recent_purchases = _rowdicts(cur, ["slip_date", "slip_no", "supplier_name", "quantity",
                                           "unit_price", "partner_price", "cost_price",
                                           "supply_amount", "item_name", "spec"])

        s0 = recent_sales[0] if recent_sales else {}
        p0 = recent_purchases[0] if recent_purchases else {}
        summary = {
            "item_code": item_code,
            "item_name": s0.get("item_name") or p0.get("item_name") or "",
            "model_name": s0.get("model_name") or p0.get("spec") or "",
            "latest_sale_date": s0.get("slip_date"),
            "latest_sale_price": s0.get("unit_price"),
            "latest_sale_customer": s0.get("customer_name"),
            "latest_purchase_date": p0.get("slip_date"),
            "latest_purchase_price": p0.get("unit_price"),
            "latest_cost_price": p0.get("cost_price") or s0.get("cost_price"),
            "latest_supplier": p0.get("supplier_name"),
        }
        return {"success": True, "query": qs, "summary": summary,
                "recent_sales": recent_sales, "recent_purchases": recent_purchases,
                "candidates": candidates[1:] if candidates else []}
    finally:
        conn.close()
