"""
적정재고 관리 서비스 (온라인관리품목)
- inventory_snapshots 일별 차이 → 판매량 자동 계산
- 이동평균(7일/30일) 기반 판매속도 분석
- 오더리스트(구글시트) 연동 → 이미 발주된 품목 확인
- 리드타임 기반 발주 시점/수량 자동 추천
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


# ─── 관리품목 CRUD ───────────────────────────────────────

def get_planning_targets(conn, active_only=True) -> list:
    where = "WHERE is_active = 1" if active_only else ""
    rows = conn.execute(f"""
        SELECT id, prod_cd, model_name, prod_name, lead_time_days,
               safety_stock_days, moq, supplier_group, is_active, created_at
        FROM inventory_planning_targets {where}
        ORDER BY model_name ASC
    """).fetchall()
    return [dict(r) for r in rows]


def add_planning_target(conn, prod_cd: str, model_name: str, prod_name: str,
                        lead_time_days: int = 40, safety_stock_days: int = 10,
                        moq: int = 0, supplier_group: str = ""):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO inventory_planning_targets
            (prod_cd, model_name, prod_name, lead_time_days, safety_stock_days,
             moq, supplier_group, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(prod_cd) DO UPDATE SET
            model_name=excluded.model_name, prod_name=excluded.prod_name,
            lead_time_days=excluded.lead_time_days, safety_stock_days=excluded.safety_stock_days,
            moq=excluded.moq, supplier_group=excluded.supplier_group,
            is_active=1, updated_at=excluded.updated_at
    """, (prod_cd, model_name, prod_name, lead_time_days, safety_stock_days,
          moq, supplier_group, now, now))
    conn.commit()


def update_planning_target(conn, target_id: int, **kwargs):
    allowed = {"lead_time_days", "safety_stock_days", "moq", "supplier_group", "is_active"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return
    updates["updated_at"] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    conn.execute(
        f"UPDATE inventory_planning_targets SET {set_clause} WHERE id = ?",
        list(updates.values()) + [target_id]
    )
    conn.commit()


def remove_planning_target(conn, target_id: int):
    conn.execute("DELETE FROM inventory_planning_targets WHERE id = ?", (target_id,))
    conn.commit()


def bulk_add_planning_targets(conn, items: list):
    """[{prod_cd, model_name, prod_name, lead_time_days, safety_stock_days}, ...]"""
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    added = 0
    for item in items:
        try:
            conn.execute("""
                INSERT INTO inventory_planning_targets
                    (prod_cd, model_name, prod_name, lead_time_days, safety_stock_days, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(prod_cd) DO UPDATE SET
                    model_name=excluded.model_name, prod_name=excluded.prod_name,
                    is_active=1, updated_at=excluded.updated_at
            """, (
                item["prod_cd"], item.get("model_name", ""), item.get("prod_name", ""),
                item.get("lead_time_days", 40), item.get("safety_stock_days", 10), now, now
            ))
            added += 1
        except Exception as e:
            logger.warning(f"품목 추가 실패 {item.get('prod_cd')}: {e}")
    conn.commit()
    return added


# ─── 일별 판매량 계산 (스냅샷 차이) ─────────────────────

def get_daily_sales(conn, prod_cd: str, days: int = 180) -> list:
    """
    최근 N일간 일별 판매량 조회
    판매량 = 전일 재고 - 금일 재고 (양수만, 음수=입고)
    Returns: [{"date": "20260407", "sales": 15, "stock": 480}, ...]
    """
    now = datetime.now(KST)
    start = (now - timedelta(days=days + 1)).strftime("%Y%m%d")

    rows = conn.execute("""
        SELECT snapshot_date, bal_qty
        FROM inventory_snapshots
        WHERE prod_cd = ? AND snapshot_date >= ?
        ORDER BY snapshot_date ASC
    """, (prod_cd, start)).fetchall()

    if len(rows) < 2:
        return []

    daily = []
    for i in range(1, len(rows)):
        prev_date, prev_qty = rows[i - 1][0], rows[i - 1][1]
        curr_date, curr_qty = rows[i][0], rows[i][1]
        diff = prev_qty - curr_qty  # 양수 = 판매, 음수 = 입고
        daily.append({
            "date": curr_date,
            "sales": max(diff, 0),  # 판매량 (입고일은 0)
            "stock_change": diff,    # 실제 변동 (입고 포함)
            "stock": curr_qty,       # 해당일 재고
        })

    return daily


def calc_sales_velocity(daily_sales: list) -> dict:
    """
    판매속도 통계 계산
    - 7일/30일/90일(3개월)/180일(6개월) 이동평균
    - 권장수량 계산은 장기 평균(90일) 기반으로 단기 급등에 의한 과잉발주 방지
    """
    if not daily_sales:
        return {"avg_7d": 0, "avg_30d": 0, "avg_90d": 0, "avg_180d": 0,
                "max_daily": 0, "total_days": 0, "selling_days": 0, "total_sold_30d": 0}

    sales_only = [d["sales"] for d in daily_sales]
    recent_7 = sales_only[-7:] if len(sales_only) >= 7 else sales_only
    recent_30 = sales_only[-30:] if len(sales_only) >= 30 else sales_only
    recent_90 = sales_only[-90:] if len(sales_only) >= 90 else sales_only
    recent_180 = sales_only  # 전체 (최대 180일)

    return {
        "avg_7d": round(sum(recent_7) / len(recent_7), 1) if recent_7 else 0,
        "avg_30d": round(sum(recent_30) / len(recent_30), 1) if recent_30 else 0,
        "avg_90d": round(sum(recent_90) / len(recent_90), 1) if recent_90 else 0,
        "avg_180d": round(sum(recent_180) / len(recent_180), 1) if recent_180 else 0,
        "max_daily": max(sales_only) if sales_only else 0,
        "total_sold_30d": sum(recent_30),
        "total_sold_90d": sum(recent_90),
        "total_days": len(daily_sales),
        "selling_days": sum(1 for s in sales_only if s > 0),
    }


# ─── 오더리스트 연동 (이미 발주된 품목 확인) ────────────

def get_pending_orders(conn, model_name: str) -> list:
    """
    오더리스트에서 해당 모델의 발주 이력 조회
    최근 탭(연도) 기준으로 검색
    """
    if not model_name:
        return []

    rows = conn.execute("""
        SELECT sheet_tab, order_no, order_date, model_name, description, qty, unit
        FROM orderlist_items
        WHERE model_name LIKE ?
        ORDER BY sheet_tab DESC, order_date DESC
        LIMIT 5
    """, (f"%{model_name}%",)).fetchall()

    return [dict(r) for r in rows]


def get_all_pending_orders_map(conn) -> dict:
    """
    전체 오더리스트를 모델명 기준으로 그룹핑
    Returns: {"LS-1000H": [{"order_date": "...", "qty": 100, ...}, ...]}
    """
    rows = conn.execute("""
        SELECT model_name, sheet_tab, order_no, order_date, qty, unit
        FROM orderlist_items
        WHERE model_name != ''
        ORDER BY sheet_tab DESC
    """).fetchall()

    order_map = {}
    for r in rows:
        model = r[0].strip().upper()
        if model not in order_map:
            order_map[model] = []
        order_map[model].append({
            "sheet_tab": r[1], "order_no": r[2],
            "order_date": r[3], "qty": r[4], "unit": r[5],
        })
    return order_map


# ─── 핵심: 적정재고 분석 ───────────────────────────────

def analyze_single_product(conn, target: dict, order_map: dict = None) -> dict:
    """
    단일 품목의 적정재고 분석
    - 일별 판매 데이터 → 판매속도 계산
    - 현재고 → 소진일 예측
    - 오더리스트 → 발주 여부 확인
    - 발주 필요 여부 + 권장 수량 산출
    """
    prod_cd = target["prod_cd"]
    model_name = target.get("model_name", "")
    lead_time = target.get("lead_time_days", 40)
    safety_days = target.get("safety_stock_days", 10)

    # 1. 일별 판매량 (6개월)
    daily = get_daily_sales(conn, prod_cd, days=180)
    velocity = calc_sales_velocity(daily)

    # 2. 현재고 (최신 스냅샷)
    latest = conn.execute("""
        SELECT bal_qty, snapshot_date FROM inventory_snapshots
        WHERE prod_cd = ? ORDER BY snapshot_date DESC LIMIT 1
    """, (prod_cd,)).fetchone()
    current_stock = latest[0] if latest else 0
    stock_date = latest[1] if latest else ""

    # 3. 소진일 예측 — 30일 평균 기준 (현재 추세 반영)
    avg_daily_for_stockout = velocity["avg_30d"] if velocity["avg_30d"] > 0 else velocity["avg_7d"]
    if avg_daily_for_stockout > 0:
        days_until_stockout = round(current_stock / avg_daily_for_stockout, 1)
    else:
        days_until_stockout = 9999  # 판매 없음

    # 4. 오더리스트 확인
    pending_orders = []
    if order_map and model_name:
        key = model_name.strip().upper()
        pending_orders = order_map.get(key, [])
        # 부분 매칭도 시도
        if not pending_orders:
            for k, v in order_map.items():
                if key in k or k in key:
                    pending_orders = v
                    break

    has_pending_order = len(pending_orders) > 0

    # 5. 발주 판단
    need_order = days_until_stockout <= (lead_time + safety_days) and not has_pending_order

    # 6. 상태 분류
    if has_pending_order:
        status = "ordered"       # 이미 발주됨
        status_label = "발주완료"
    elif days_until_stockout <= lead_time:
        status = "urgent"        # 긴급 발주 필요
        status_label = "긴급발주"
    elif days_until_stockout <= (lead_time + safety_days):
        status = "warning"       # 곧 발주 필요
        status_label = "발주검토"
    elif avg_daily_for_stockout == 0:
        status = "no_sales"      # 최근 판매 없음
        status_label = "판매없음"
    else:
        status = "safe"          # 여유
        status_label = "여유"

    # 7. 권장 발주 수량
    #    - 90일(3개월) 평균 기반으로 계산 → 단기 급등에 의한 과잉발주 방지
    #    - 90일 데이터 부족 시 30일 → 7일 순으로 fallback
    #    - MOQ 이상으로 올림
    recommended_qty = 0
    moq = target.get("moq", 0) or 0
    avg_for_order = velocity["avg_90d"] or velocity["avg_30d"] or velocity["avg_7d"]
    if need_order and avg_for_order > 0:
        required = avg_for_order * (lead_time + safety_days)
        raw_qty = max(0, round(required - current_stock))
        # MOQ 적용: MOQ보다 작으면 MOQ로 올림
        if moq > 0 and raw_qty > 0:
            recommended_qty = max(raw_qty, moq)
            # MOQ 단위로 올림 (예: MOQ=100, raw=230 → 300)
            if recommended_qty > moq:
                recommended_qty = ((recommended_qty + moq - 1) // moq) * moq
        else:
            recommended_qty = raw_qty

    # 8. 권장 발주일 (소진일 - 리드타임)
    order_deadline = ""
    if avg_daily_for_stockout > 0 and days_until_stockout < 9999:
        deadline_days = max(0, days_until_stockout - lead_time)
        deadline_date = datetime.now(KST) + timedelta(days=deadline_days)
        order_deadline = deadline_date.strftime("%Y-%m-%d")

    return {
        "id": target["id"],
        "prod_cd": prod_cd,
        "model_name": model_name,
        "prod_name": target.get("prod_name", ""),
        "current_stock": current_stock,
        "stock_date": stock_date,
        "avg_daily_7d": velocity["avg_7d"],
        "avg_daily_30d": velocity["avg_30d"],
        "avg_daily_90d": velocity["avg_90d"],
        "avg_daily_180d": velocity["avg_180d"],
        "total_sold_30d": velocity.get("total_sold_30d", 0),
        "total_sold_90d": velocity.get("total_sold_90d", 0),
        "selling_days": velocity.get("selling_days", 0),
        "max_daily": velocity.get("max_daily", 0),
        "days_until_stockout": days_until_stockout,
        "lead_time_days": lead_time,
        "safety_stock_days": safety_days,
        "moq": moq,
        "supplier_group": target.get("supplier_group", ""),
        "status": status,
        "status_label": status_label,
        "need_order": need_order,
        "recommended_qty": recommended_qty,
        "order_deadline": order_deadline,
        "has_pending_order": has_pending_order,
        "pending_orders": pending_orders[:3],
        "daily_sales": daily[-30:],  # 최근 30일만 (차트용)
    }


def analyze_all_targets(conn) -> dict:
    """전체 관리품목 일괄 분석 (오더리스트 + 선적메일 연동)"""
    targets = get_planning_targets(conn, active_only=True)
    if not targets:
        return {"items": [], "summary": {"total": 0, "urgent": 0, "warning": 0, "ordered": 0, "safe": 0}}

    # 오더리스트 + 선적정보 한 번에 로딩
    order_map = get_all_pending_orders_map(conn)

    # 선적 메일 정보 로딩
    try:
        from services.shipping_mail_service import get_shipping_info_map
        shipping_map = get_shipping_info_map(conn)
    except Exception:
        shipping_map = {}

    results = []
    summary = {"total": 0, "urgent": 0, "warning": 0, "ordered": 0, "safe": 0, "no_sales": 0}

    for target in targets:
        analysis = analyze_single_product(conn, target, order_map)

        # 선적 정보 매칭
        model_key = (analysis.get("model_name") or "").strip().upper()
        ship_info = shipping_map.get(model_key, {})
        analysis["shipping_date"] = ship_info.get("shipping_date", "")
        analysis["arrival_date"] = ship_info.get("arrival_date", "")
        analysis["shipping_bor"] = ship_info.get("bor_number", "")

        results.append(analysis)
        summary["total"] += 1
        if analysis["status"] in summary:
            summary[analysis["status"]] += 1

    # 긴급 → 경고 → 발주완료 → 여유 순 정렬
    status_order = {"urgent": 0, "warning": 1, "ordered": 2, "no_sales": 3, "safe": 4}
    results.sort(key=lambda x: (status_order.get(x["status"], 9), x["days_until_stockout"]))

    return {"items": results, "summary": summary}


# ─── 품목 마스터 검색 (등록 시 사용) ─────────────────────

def search_products_master(conn, query: str, limit: int = 20) -> list:
    """
    products_master.csv 또는 inventory_snapshots에서 품목 검색
    관리품목 등록 시 품목코드/모델명/품명을 찾기 위해 사용
    """
    from services.inventory_monitor import load_products_master
    master = load_products_master()

    q = query.upper()
    results = []
    for prod_cd, info in master.items():
        if (q in prod_cd.upper() or q in info["name"].upper() or q in info["model"].upper()):
            results.append({
                "prod_cd": prod_cd,
                "prod_name": info["name"],
                "model_name": info["model"],
                "unit_price": info["price"],
            })
            if len(results) >= limit:
                break

    return results
