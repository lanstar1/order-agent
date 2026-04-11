"""
오더리스트 서비스 - Google Sheets 해외 발주(오더) 현황 동기화 및 조회

구글시트 구조: 발주서 형태 (카테고리 행 + 상품 행 반복)
- Row pattern:
  - 카테고리 행: col[1]에 카테고리명 (예: "U/UTP CAT.5E LAN CABLE"), col[0] 비어있음
  - 상품 행: col[0]에 모델명+설명 (예: "LS-5UTPD-10MG, U/UTP Cat.5e ..."), col[2]에 수량
  - 헤더 행: "Seller:", "Date:", "Item" 등 포함
  - 주문번호/공급자: 첫 몇 행에 위치

시트 탭: 연도별(2026, 2025), Stock, 인천항 등
"""
import re
import json
import logging
import httpx
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.database import get_connection
from config import GOOGLE_API_KEY

logger = logging.getLogger(__name__)

ORDERLIST_SHEET_ID = "1ej0cxyM3NHJKpF-KBXbZ16fH-lZcUrr3Z3eTwFVFSco"


# ─────────────────────────────────────────
#  DB 테이블
# ─────────────────────────────────────────
def _ensure_orderlist_tables():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orderlist_items (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_tab       TEXT NOT NULL,
            order_no        TEXT DEFAULT '',
            seller          TEXT DEFAULT '',
            order_date      TEXT DEFAULT '',
            category        TEXT DEFAULT '',
            model_name      TEXT DEFAULT '',
            description     TEXT DEFAULT '',
            qty             INTEGER DEFAULT 0,
            unit            TEXT DEFAULT 'PCS',
            unit_price      TEXT DEFAULT '',
            total_value     TEXT DEFAULT '',
            row_index       INTEGER DEFAULT 0,
            raw_row         TEXT DEFAULT '',
            synced_at       TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_orderlist_tab ON orderlist_items(sheet_tab);
        CREATE INDEX IF NOT EXISTS idx_orderlist_model ON orderlist_items(model_name);
        CREATE INDEX IF NOT EXISTS idx_orderlist_search ON orderlist_items(description);

        CREATE TABLE IF NOT EXISTS orderlist_sync_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_tab   TEXT NOT NULL,
            item_count  INTEGER DEFAULT 0,
            synced_at   TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    conn.close()

_ensure_orderlist_tables()


# ─────────────────────────────────────────
#  구글시트에서 탭 목록 조회
# ─────────────────────────────────────────
def get_sheet_tabs() -> list:
    """오더리스트 구글시트의 탭 목록 반환"""
    if not GOOGLE_API_KEY:
        return []
    try:
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{ORDERLIST_SHEET_ID}?key={GOOGLE_API_KEY}&fields=sheets.properties"
        r = httpx.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        tabs = []
        for s in data.get("sheets", []):
            props = s.get("properties", {})
            tabs.append({
                "title": props.get("title", ""),
                "index": props.get("index", 0),
                "gid": props.get("sheetId", 0),
            })
        return tabs
    except Exception as e:
        logger.error(f"[OrderList] 탭 목록 조회 실패: {e}")
        return []


# ─────────────────────────────────────────
#  구글시트 → DB 동기화
# ─────────────────────────────────────────
def _parse_order_rows(rows: list, tab_title: str) -> list:
    """
    발주서 형태의 rows를 파싱하여 상품 항목 리스트로 변환

    패턴:
    - Seller 행 → seller 추출
    - No.: 행 → order_no 추출
    - Date: 행 → order_date 추출
    - 카테고리 행: col[0] 비어있고 col[1]에 텍스트 (대문자 카테고리)
    - 상품 행: col[0]에 "LS-xxxx, 설명..." 형태
    """
    items = []
    order_no = ""
    order_date = ""
    current_category = ""

    for row_idx, row in enumerate(rows):
        if not row:
            continue

        # 패딩: 최소 5개 컬럼 확보
        while len(row) < 5:
            row.append("")

        col0 = str(row[0]).strip()
        col1 = str(row[1]).strip()
        col2 = str(row[2]).strip()
        col3 = str(row[3]).strip()

        # ── 헤더/메타 행 파싱 ──
        # 구글시트 구조:
        #   Row A: col[0]="Seller:" col[2]="No.:" col[3]=주문번호
        #   Row B: col[0]=업체명    col[2]="Date:" col[3]=주문일
        #   Row C: col[0]="Item"    col[2]="Description" col[3]="Quantity"

        # "Seller:" 행 → 같은 행에서 No. 추출
        if col0.startswith("Seller"):
            if "No.:" in col2 or "No.:" in col3:
                no_text = col3.replace("No.:", "").strip() if "No.:" in col3 else col3.strip()
                # col3에 "No.:"만 있고 값이 없으면 col[4] 확인
                if not no_text and len(row) > 4:
                    no_text = str(row[4]).strip()
                if no_text:
                    order_no = no_text
            continue

        # 업체명 행 (Seller 바로 다음) → 같은 행에서 Date 추출
        if row_idx > 0:
            prev = rows[row_idx - 1] if rows[row_idx - 1] else []
            while len(prev) < 5:
                prev.append("")
            if str(prev[0]).strip().startswith("Seller"):
                # 이 행에서 Date 추출
                if "Date:" in col2 or "Date" in col2:
                    date_text = col3.replace("Date:", "").strip() if col3 else ""
                    if date_text:
                        order_date = date_text
                continue

        # "No.:" 가 독립 행에 있는 경우 (대체 구조 지원)
        if "No.:" in col0 or "No.:" in col2:
            no_text = col3 if "No.:" in col2 else col0.split("No.:")[1].strip() if "No.:" in col0 else ""
            if no_text:
                order_no = no_text
            continue

        if "Date:" in col0 or "Date:" in col2:
            date_text = col3 if "Date:" in col2 else col0.split("Date:")[1].strip() if "Date:" in col0 else ""
            if date_text:
                order_date = date_text
            continue

        # 헤더 행 스킵
        if col0 == "Item" or col0 == "No." or col0 == "#":
            continue

        # 카테고리 행: col[0]이 비어있고 col[1]에 대문자 텍스트
        if not col0 and col1 and not col2:
            # 대문자 비율 체크 (카테고리는 보통 대문자)
            upper_ratio = sum(1 for c in col1 if c.isupper()) / max(len(col1.replace(" ", "")), 1)
            if upper_ratio > 0.5 or col1.isupper():
                current_category = col1
                continue

        # 상품 행: col[0]에 모델명+설명이 있는 경우
        if col0 and ("LS-" in col0.upper() or "," in col0):
            # 모델명 추출 (첫 번째 콤마 앞)
            parts = col0.split(",", 1)
            model = parts[0].strip()
            desc = parts[1].strip() if len(parts) > 1 else ""

            # 수량 파싱
            qty_str = col2.replace(",", "").strip()
            try:
                qty = int(float(qty_str)) if qty_str else 0
            except (ValueError, TypeError):
                qty = 0

            unit = col3 if col3 else "PCS"

            # 단가/총액 (Stock 시트에 있을 수 있음)
            unit_price = ""
            total_value = ""
            if len(row) > 5:
                unit_price = str(row[5]).strip() if row[5] else ""
            if len(row) > 7:
                total_value = str(row[7]).strip() if row[7] else ""

            items.append({
                "sheet_tab": tab_title,
                "order_no": order_no,
                "order_date": order_date,
                "category": current_category,
                "model_name": model,
                "description": desc,
                "qty": qty,
                "unit": unit,
                "unit_price": unit_price,
                "total_value": total_value,
                "row_index": row_idx + 1,
                "raw_row": json.dumps(row[:10], ensure_ascii=False),
            })

    return items


def sync_orderlist(tab_title: str = "") -> dict:
    """
    구글시트에서 오더리스트 동기화
    전체 탭 동기화 (오래된 오더는 get_all_pending_orders_map의 90일 필터로 제어)
    """
    if not GOOGLE_API_KEY:
        return {"success": False, "error": "GOOGLE_API_KEY 미설정"}

    tabs = get_sheet_tabs()
    if not tabs:
        return {"success": False, "error": "시트 탭 조회 실패"}

    if tab_title:
        tabs = [t for t in tabs if t["title"] == tab_title]

    conn = get_connection()
    total_items = 0
    synced_tabs = []

    for tab in tabs:
        title = tab["title"]
        try:
            # 구글시트 API로 데이터 조회
            encoded_title = title.replace("'", "''")
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{ORDERLIST_SHEET_ID}/values/'{encoded_title}'!A1:Z500?key={GOOGLE_API_KEY}"
            r = httpx.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            rows = data.get("values", [])

            if not rows:
                logger.info(f"[OrderList] {title}: 데이터 없음 (스킵)")
                continue

            # 파싱
            items = _parse_order_rows(rows, title)
            if not items:
                logger.info(f"[OrderList] {title}: 파싱된 항목 없음")
                continue

            # 기존 데이터 삭제 후 새로 삽입
            conn.execute("DELETE FROM orderlist_items WHERE sheet_tab = ?", (title,))
            for item in items:
                conn.execute("""
                    INSERT INTO orderlist_items(
                        sheet_tab, order_no, order_date, category,
                        model_name, description, qty, unit, unit_price,
                        total_value, row_index, raw_row
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    item["sheet_tab"], item["order_no"],
                    item["order_date"], item["category"], item["model_name"],
                    item["description"], item["qty"], item["unit"],
                    item["unit_price"], item["total_value"],
                    item["row_index"], item["raw_row"],
                ))

            # 동기화 로그
            conn.execute(
                "INSERT INTO orderlist_sync_log(sheet_tab, item_count) VALUES(?,?)",
                (title, len(items))
            )

            total_items += len(items)
            synced_tabs.append({"tab": title, "items": len(items)})
            logger.info(f"[OrderList] {title}: {len(items)}건 동기화")

        except Exception as e:
            logger.error(f"[OrderList] {title} 동기화 실패: {e}")
            synced_tabs.append({"tab": title, "items": 0, "error": str(e)})

    conn.commit()
    conn.close()

    return {
        "success": True,
        "total_items": total_items,
        "tabs": synced_tabs,
    }


# ─────────────────────────────────────────
#  조회 / 검색
# ─────────────────────────────────────────
def get_orderlist_data(
    query: str = "",
    tab: str = "",
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """
    오더리스트 조회/검색
    query: 모델명/설명/카테고리 검색
    tab: 특정 탭 필터 (빈 문자열이면 전체)
    """
    conn = get_connection()

    where_parts = ["1=1"]
    params = []

    if tab:
        where_parts.append("o.sheet_tab = ?")
        params.append(tab)

    if query:
        where_parts.append("""(
            o.model_name LIKE ? OR o.description LIKE ?
            OR o.category LIKE ? OR o.order_no LIKE ?
        )""")
        q = f"%{query}%"
        params.extend([q, q, q, q])

    where = " AND ".join(where_parts)

    # 전체 건수
    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM orderlist_items o WHERE {where}", params
    ).fetchone()["cnt"]

    # 페이지네이션
    offset = (page - 1) * page_size
    rows = conn.execute(
        f"""SELECT o.* FROM orderlist_items o
            WHERE {where}
            ORDER BY o.sheet_tab DESC, o.row_index ASC
            LIMIT ? OFFSET ?""",
        params + [page_size, offset]
    ).fetchall()

    conn.close()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "items": [dict(r) for r in rows],
    }


def get_orderlist_tabs() -> list:
    """DB에 저장된 오더리스트 탭 목록 (건수 포함)"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT sheet_tab, COUNT(*) as item_count,
               MAX(synced_at) as last_sync
        FROM orderlist_items
        GROUP BY sheet_tab
        ORDER BY sheet_tab DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def autocomplete_orderlist(query: str, limit: int = 15) -> list:
    """오더리스트 DB에서 모델명/카테고리/주문번호 자동완성 검색"""
    if not query or not query.strip():
        return []

    conn = get_connection()
    q = f"%{query.strip()}%"
    rows = conn.execute("""
        SELECT DISTINCT model_name, category, description, sheet_tab, order_no, qty, unit
        FROM orderlist_items
        WHERE model_name LIKE ? OR description LIKE ? OR category LIKE ? OR order_no LIKE ?
        ORDER BY sheet_tab DESC, model_name ASC
        LIMIT ?
    """, (q, q, q, q, limit)).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def get_orderlist_summary() -> dict:
    """오더리스트 요약 통계"""
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) as cnt FROM orderlist_items").fetchone()["cnt"]
    tabs = conn.execute("""
        SELECT sheet_tab, COUNT(*) as cnt, SUM(qty) as total_qty
        FROM orderlist_items GROUP BY sheet_tab ORDER BY sheet_tab DESC
    """).fetchall()
    last_sync = conn.execute(
        "SELECT MAX(synced_at) as ts FROM orderlist_sync_log"
    ).fetchone()["ts"]
    conn.close()

    return {
        "total_items": total,
        "last_sync": last_sync,
        "tabs": [dict(t) for t in tabs],
    }
