"""
B2B 간편발주 서비스 — 고정 거래처 전용.

고도몰(B2C)과 별개 채널. 거래처가 PIN 로그인 → 자주 사는 품목 + 수량 → 원터치 발주.
발주 확정 순간 서버가 ECOUNT 품목조회로 거래처 단가유형(1~10)에 맞는 단가를 재조회해
그 값을 판매전표(SaveSale)로 전송한다. 클라이언트가 보낸 단가는 신뢰하지 않는다.

재고/단가/전표는 모두 ECOUNT 하나를 정본으로 공유한다.
"""
import logging
import time

from db.database import get_connection, safe_add_column
from services.erp_client import erp_client
from security import hash_password, verify_password
from config import ERP_WH_CD, ERP_EMP_CD

logger = logging.getLogger(__name__)

_SCHEMA_READY = False


# ─────────────────────────────────────────
#  스키마 (customers 확장 + b2b 전용 테이블)
# ─────────────────────────────────────────
def ensure_b2b_schema():
    """customers 테이블에 B2B 컬럼 추가 + b2b_favorites/b2b_orders/b2b_order_lines 생성."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    # SQLite 문법으로 작성 — DB 래퍼의 _sql_to_pg 가 PostgreSQL로 자동 변환한다
    # (sale_orders.py 등 기존 테이블과 동일 패턴).
    ddl = """
        CREATE TABLE IF NOT EXISTS b2b_favorites (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            cust_code  TEXT NOT NULL,
            prod_cd    TEXT NOT NULL,
            prod_name  TEXT DEFAULT '',
            unit       TEXT DEFAULT '',
            sort_order INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS b2b_orders (
            order_id    TEXT PRIMARY KEY,
            cust_code   TEXT NOT NULL,
            cust_name   TEXT DEFAULT '',
            status      TEXT DEFAULT 'pending',
            erp_slip_no TEXT DEFAULT '',
            total_amt   REAL DEFAULT 0,
            memo        TEXT DEFAULT '',
            error       TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS b2b_order_lines (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    TEXT NOT NULL,
            prod_cd     TEXT NOT NULL,
            prod_name   TEXT DEFAULT '',
            qty         REAL DEFAULT 0,
            unit        TEXT DEFAULT '',
            unit_price  REAL DEFAULT 0,
            price_src   TEXT DEFAULT '',
            amount      REAL DEFAULT 0
        );
    """

    last_err = None
    for attempt in range(4):
        conn = get_connection()
        try:
            # SQLite: 다른 커넥션(스케줄러 등)의 쓰기 락을 최대 20초 대기 (PG는 무시됨)
            try:
                conn.execute("PRAGMA busy_timeout=20000")
            except Exception:
                pass
            # customers 확장 (단가유형/PIN/사용여부/출하창고)
            safe_add_column(conn, "customers", "price_tier", "INTEGER DEFAULT 0")
            safe_add_column(conn, "customers", "pin_hash", "TEXT DEFAULT ''")
            safe_add_column(conn, "customers", "b2b_enabled", "INTEGER DEFAULT 0")
            safe_add_column(conn, "customers", "wh_cd", "TEXT DEFAULT ''")
            conn.executescript(ddl)
            conn.commit()
            _SCHEMA_READY = True
            logger.info("[B2B] 스키마 준비 완료")
            return
        except Exception as e:  # 일시적 'database is locked' 등 → 백오프 재시도
            last_err = e
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(0.4 * (attempt + 1))
        finally:
            conn.close()
    logger.error(f"[B2B] 스키마 준비 실패(재시도 소진): {last_err}")
    raise last_err


# ─────────────────────────────────────────
#  거래처(B2B 계정) 관리
# ─────────────────────────────────────────
def get_customer(cust_code: str) -> dict | None:
    ensure_b2b_schema()
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT cust_code, cust_name, alias, price_tier, b2b_enabled, wh_cd, pin_hash "
            "FROM customers WHERE cust_code=?",
            (cust_code,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def setup_b2b_customer(cust_code: str, pin: str = "", price_tier: int | None = None,
                       wh_cd: str | None = None, enabled: bool | None = None) -> dict:
    """관리자용: 거래처의 B2B 활성화/단가유형/PIN/출하창고 설정."""
    ensure_b2b_schema()
    conn = get_connection()
    try:
        row = conn.execute("SELECT cust_code FROM customers WHERE cust_code=?", (cust_code,)).fetchone()
        if not row:
            raise ValueError(f"거래처 {cust_code}가 customers에 없습니다. 먼저 동기화/등록하세요.")
        if pin:
            conn.execute("UPDATE customers SET pin_hash=? WHERE cust_code=?",
                         (hash_password(pin), cust_code))
        if price_tier is not None:
            conn.execute("UPDATE customers SET price_tier=? WHERE cust_code=?",
                         (int(price_tier), cust_code))
        if wh_cd is not None:
            conn.execute("UPDATE customers SET wh_cd=? WHERE cust_code=?", (wh_cd, cust_code))
        if enabled is not None:
            conn.execute("UPDATE customers SET b2b_enabled=? WHERE cust_code=?",
                         (1 if enabled else 0, cust_code))
        conn.commit()
    finally:
        conn.close()
    return get_customer(cust_code)


def verify_login(cust_code: str, pin: str) -> dict | None:
    """PIN 로그인 검증. 성공 시 거래처 dict, 실패 시 None."""
    cust = get_customer(cust_code)
    if not cust or not cust.get("b2b_enabled"):
        return None
    if not cust.get("pin_hash") or not verify_password(pin, cust["pin_hash"]):
        return None
    return cust


# ─────────────────────────────────────────
#  자주 사는 품목 (favorites)
# ─────────────────────────────────────────
def get_favorites(cust_code: str) -> list:
    ensure_b2b_schema()
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT prod_cd, prod_name, unit, sort_order FROM b2b_favorites "
            "WHERE cust_code=? ORDER BY sort_order, id",
            (cust_code,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def set_favorites(cust_code: str, items: list) -> int:
    """items: [{prod_cd, prod_name?, unit?}] — 거래처 즐겨찾기 전체 교체."""
    ensure_b2b_schema()
    conn = get_connection()
    try:
        conn.execute("DELETE FROM b2b_favorites WHERE cust_code=?", (cust_code,))
        for i, it in enumerate(items):
            pc = (it.get("prod_cd") or "").strip()
            if not pc:
                continue
            conn.execute(
                "INSERT INTO b2b_favorites(cust_code, prod_cd, prod_name, unit, sort_order) "
                "VALUES(?,?,?,?,?)",
                (cust_code, pc, it.get("prod_name", ""), it.get("unit", ""), i),
            )
        conn.commit()
    finally:
        conn.close()
    return len(items)


# ─────────────────────────────────────────
#  카탈로그 + 거래처별 단가
# ─────────────────────────────────────────
async def build_catalog(cust_code: str) -> dict:
    """
    거래처의 즐겨찾기 품목 + 각 품목의 거래처 단가(단가유형 기준)를 반환한다.
    """
    cust = get_customer(cust_code)
    if not cust:
        raise ValueError("거래처를 찾을 수 없습니다.")
    favs = get_favorites(cust_code)
    prod_cds = [f["prod_cd"] for f in favs]
    tier = int(cust.get("price_tier") or 0)

    prices = await erp_client.get_customer_prices(prod_cds, price_tier=tier) if prod_cds else {}

    items = []
    for f in favs:
        p = prices.get(f["prod_cd"], {})
        items.append({
            "prod_cd":   f["prod_cd"],
            "prod_name": f["prod_name"] or p.get("name", ""),
            "unit":      f["unit"],
            "price":     p.get("price", 0),
            "price_src": p.get("source", "auto"),  # tier{n} | base | auto(ERP 자동단가)
        })
    return {
        "cust_code": cust_code,
        "cust_name": cust.get("cust_name", ""),
        "price_tier": tier,
        "items": items,
    }


# ─────────────────────────────────────────
#  발주 → ECOUNT 판매전표(SaveSale)
# ─────────────────────────────────────────
async def submit_order(cust_code: str, lines: list, memo: str = "") -> dict:
    """
    lines: [{prod_cd, qty}] — 클라이언트는 품목/수량만. 단가는 서버가 재조회(불신 원칙).
    - 거래처 단가유형에 맞는 단가를 서버가 ECOUNT에서 재조회해 PRICE로 전송.
    - 단가 0/미등록이면 PRICE 생략 → ECOUNT가 거래처 단가 자동 적용.
    - 결제조건(월말외상)은 지정하지 않음 (별도 후처리).
    Returns: {success, order_id, erp_slip_no?, total_amt, message}
    """
    ensure_b2b_schema()
    cust = get_customer(cust_code)
    if not cust:
        raise ValueError("거래처를 찾을 수 없습니다.")

    clean = [(str(l.get("prod_cd", "")).strip(), float(l.get("qty") or 0))
             for l in lines if str(l.get("prod_cd", "")).strip() and float(l.get("qty") or 0) > 0]
    if not clean:
        raise ValueError("발주 품목이 없습니다.")

    tier = int(cust.get("price_tier") or 0)
    prod_cds = list({pc for pc, _ in clean})
    prices = await erp_client.get_customer_prices(prod_cds, price_tier=tier)

    order_id = f"B2B{int(time.time())}"
    erp_lines, rec_lines, total = [], [], 0.0
    for pc, qty in clean:
        pinfo = prices.get(pc, {})
        unit_price = float(pinfo.get("price") or 0)
        amount = round(unit_price * qty, 2)
        total += amount
        erp_lines.append({
            "prod_cd": pc,
            "qty": qty,
            "unit": "",
            "price": unit_price,  # 0이면 save_sale가 PRICE 생략 → ERP 자동단가
        })
        rec_lines.append({
            "prod_cd": pc, "prod_name": pinfo.get("name", ""), "qty": qty,
            "unit_price": unit_price, "price_src": pinfo.get("source", "auto"), "amount": amount,
        })

    # 발주 이력 저장 (전송 전 pending)
    _save_order(order_id, cust, total, memo, rec_lines, status="pending")

    wh = cust.get("wh_cd") or ERP_WH_CD
    upload_ser = str(int(time.time()))[-8:]

    try:
        result = await erp_client.save_sale(
            cust_code=cust_code,
            lines=erp_lines,
            upload_ser=upload_ser,
            wh_cd=wh,
            emp_cd=ERP_EMP_CD,
        )
    except Exception as e:
        _mark_order(order_id, "error", error=str(e))
        await _notify_exception(cust, order_id, f"SaveSale 예외: {e}")
        raise

    inner = (result.get("data", {}) or {}).get("Data", {}) or {}
    success_cnt = inner.get("SuccessCnt", -1)
    slip_nos = inner.get("SlipNos", []) or []
    ok = result.get("success") and success_cnt and success_cnt > 0

    if ok:
        slip_no = slip_nos[0] if slip_nos else ""
        _mark_order(order_id, "submitted", erp_slip_no=slip_no)
        return {"success": True, "order_id": order_id, "erp_slip_no": slip_no,
                "total_amt": total, "message": f"발주 완료 (전표 {slip_no})"}
    else:
        details = inner.get("ResultDetails", []) or []
        err = "; ".join(d.get("TotalError", "") for d in details if not d.get("IsSuccess")) \
              or str(result.get("error") or f"SuccessCnt={success_cnt}")
        _mark_order(order_id, "error", error=err)
        await _notify_exception(cust, order_id, f"ERP 저장 실패: {err}")
        return {"success": False, "order_id": order_id, "total_amt": total,
                "message": f"ERP 오류: {err}"}


def get_order_history(cust_code: str, limit: int = 20) -> list:
    ensure_b2b_schema()
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT order_id, status, erp_slip_no, total_amt, created_at "
            "FROM b2b_orders WHERE cust_code=? ORDER BY created_at DESC LIMIT ?",
            (cust_code, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────
#  내부 헬퍼
# ─────────────────────────────────────────
def _save_order(order_id, cust, total, memo, rec_lines, status):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO b2b_orders(order_id, cust_code, cust_name, status, total_amt, memo) "
            "VALUES(?,?,?,?,?,?)",
            (order_id, cust["cust_code"], cust.get("cust_name", ""), status, total, memo),
        )
        for l in rec_lines:
            conn.execute(
                "INSERT INTO b2b_order_lines(order_id, prod_cd, prod_name, qty, unit_price, price_src, amount) "
                "VALUES(?,?,?,?,?,?,?)",
                (order_id, l["prod_cd"], l["prod_name"], l["qty"], l["unit_price"], l["price_src"], l["amount"]),
            )
        conn.commit()
    finally:
        conn.close()


def _mark_order(order_id, status, erp_slip_no=None, error=None):
    conn = get_connection()
    try:
        if erp_slip_no is not None:
            conn.execute("UPDATE b2b_orders SET status=?, erp_slip_no=? WHERE order_id=?",
                         (status, erp_slip_no, order_id))
        elif error is not None:
            conn.execute("UPDATE b2b_orders SET status=?, error=? WHERE order_id=?",
                         (status, error[:500], order_id))
        else:
            conn.execute("UPDATE b2b_orders SET status=? WHERE order_id=?", (status, order_id))
        conn.commit()
    finally:
        conn.close()


async def _notify_exception(cust, order_id, detail: str):
    """예외건만 텔레그램 알림 (best-effort)."""
    try:
        from services.telegram_service import TelegramService
        conn = get_connection()
        try:
            rows = conn.execute("SELECT key, value FROM inventory_alert_settings").fetchall()
            s = {r[0]: r[1] for r in rows}
        finally:
            conn.close()
        token, chat = s.get("telegram_bot_token", ""), s.get("telegram_chat_id", "")
        if not (token and chat):
            return
        msg = (f"⚠️ <b>B2B 발주 예외</b>\n"
               f"거래처: {cust.get('cust_name','')} ({cust.get('cust_code','')})\n"
               f"주문: {order_id}\n{detail}")
        await TelegramService(token, chat).send_message(msg)
    except Exception as e:
        logger.warning(f"[B2B] 텔레그램 알림 실패: {e}")
