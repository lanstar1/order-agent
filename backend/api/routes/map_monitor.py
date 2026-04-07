"""
MAP Monitor API Routes - 지도가 감시 시스템
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
import json, csv, io, logging

from db.database import get_connection

logger = logging.getLogger("map_monitor")
router = APIRouter(prefix="/api/map", tags=["MAP Monitor"])


# ── Pydantic Models ──────────────────────────────────

class SettingsUpdate(BaseModel):
    min_price: Optional[int] = None
    tolerance_pct: Optional[float] = None
    schedules: Optional[List[str]] = None
    watch_interval_hours: Optional[int] = None
    platforms: Optional[List[str]] = None
    alert_email: Optional[bool] = None
    alert_kakao: Optional[bool] = None
    alert_nateon: Optional[bool] = None
    alert_email_address: Optional[str] = None

class ProductCreate(BaseModel):
    model_name: str
    product_name: str
    brand: str = "LANstar"
    features: str = ""
    map_price: int
    tolerance_pct: Optional[float] = None
    search_keywords: str = ""

class ProductUpdate(BaseModel):
    product_name: Optional[str] = None
    brand: Optional[str] = None
    features: Optional[str] = None
    map_price: Optional[int] = None
    tolerance_pct: Optional[float] = None
    search_keywords: Optional[str] = None
    is_active: Optional[bool] = None
    is_watched: Optional[bool] = None
    watch_interval_hours: Optional[int] = None

class ViolationResolve(BaseModel):
    resolution_note: str = ""


def _days_ago(days: int) -> str:
    """N일 전 날짜 문자열 반환 (YYYY-MM-DD HH:MM:SS)"""
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


# ═══════════════════════════════════════════════════════
# 1. 설정 API
# ═══════════════════════════════════════════════════════

@router.get("/settings")
async def get_settings():
    conn = get_connection()
    row = conn.execute("SELECT * FROM map_settings WHERE id = 1").fetchone()
    conn.close()
    if not row:
        return {}
    result = dict(row)
    for f in ['schedules', 'platforms']:
        if result.get(f) and isinstance(result[f], str):
            try: result[f] = json.loads(result[f])
            except: pass
    return result

@router.put("/settings")
async def update_settings(data: SettingsUpdate):
    conn = get_connection()
    sets, vals = [], []
    for k, v in data.dict(exclude_unset=True).items():
        if k in ['schedules', 'platforms'] and isinstance(v, list):
            sets.append(f"{k} = ?"); vals.append(json.dumps(v, ensure_ascii=False))
        elif isinstance(v, bool):
            sets.append(f"{k} = ?"); vals.append(1 if v else 0)
        else:
            sets.append(f"{k} = ?"); vals.append(v)
    if not sets:
        conn.close(); raise HTTPException(400, "변경할 항목 없음")
    sets.append("updated_at = datetime('now','localtime')")
    conn.execute(f"UPDATE map_settings SET {', '.join(sets)} WHERE id = 1", vals)
    conn.commit()
    # 스케줄러 재로드 (스케줄 또는 간격 변경 시)
    if any(k in data.dict(exclude_unset=True) for k in ['schedules', 'watch_interval_hours']):
        try:
            from services.map_scheduler import reload_map_schedule
            reload_map_schedule()
        except Exception as e:
            logger.warning(f"스케줄러 재로드 실패: {e}")
    row = conn.execute("SELECT * FROM map_settings WHERE id = 1").fetchone()
    conn.close()
    result = dict(row)
    for f in ['schedules', 'platforms']:
        if result.get(f) and isinstance(result[f], str):
            try: result[f] = json.loads(result[f])
            except: pass
    return result


# ═══════════════════════════════════════════════════════
# 2. 제품 API
# ═══════════════════════════════════════════════════════

@router.get("/products")
async def list_products(search: str = "", active_only: bool = True, watched_only: bool = False):
    conn = get_connection()
    sql = "SELECT * FROM map_products WHERE 1=1"
    params = []
    if active_only: sql += " AND is_active = 1"
    if watched_only: sql += " AND is_watched = 1"
    if search:
        sql += " AND (model_name LIKE ? OR product_name LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    sql += " ORDER BY model_name"
    rows = conn.execute(sql, params).fetchall()
    products = [dict(r) for r in rows]

    # 각 제품별 현재 최저가 및 위반 건수 추가
    cutoff = _days_ago(1)
    for p in products:
        row = conn.execute(
            "SELECT MIN(effective_price) as min_p FROM map_price_records WHERE product_id=? AND collected_at>?",
            (p["id"], cutoff)
        ).fetchone()
        p["current_min_price"] = row["min_p"] if row and row["min_p"] else 0

        row2 = conn.execute(
            "SELECT COUNT(*) as cnt FROM map_violations WHERE product_id=? AND is_resolved=0",
            (p["id"],)
        ).fetchone()
        p["active_violations"] = row2["cnt"] if row2 else 0

    conn.close()
    return products

@router.post("/products")
async def create_product(data: ProductCreate):
    conn = get_connection()
    if not data.search_keywords:
        data.search_keywords = f"{data.brand} {data.model_name} {data.product_name}"
    try:
        cur = conn.execute(
            """INSERT INTO map_products (model_name, product_name, brand, features, map_price, tolerance_pct, search_keywords)
               VALUES (?,?,?,?,?,?,?)""",
            (data.model_name, data.product_name, data.brand, data.features,
             data.map_price, data.tolerance_pct, data.search_keywords))
        conn.commit()
        pid = cur.lastrowid
        conn.close()
        return {"id": pid, "message": "제품 등록 완료"}
    except Exception as e:
        conn.close()
        if "unique" in str(e).lower() or "UNIQUE" in str(e):
            raise HTTPException(409, f"모델명 '{data.model_name}'이 이미 등록됨")
        raise HTTPException(500, str(e))

@router.put("/products/{product_id}")
async def update_product(product_id: int, data: ProductUpdate):
    conn = get_connection()
    ud = data.dict(exclude_unset=True)
    if not ud: conn.close(); raise HTTPException(400, "변경 항목 없음")
    # bool → int 변환 (SQLite)
    for k in ['is_active', 'is_watched']:
        if k in ud and isinstance(ud[k], bool):
            ud[k] = 1 if ud[k] else 0
    sets = [f"{k} = ?" for k in ud]; vals = list(ud.values())
    sets.append("updated_at = datetime('now','localtime')")
    conn.execute(f"UPDATE map_products SET {', '.join(sets)} WHERE id = ?", vals + [product_id])
    conn.commit(); conn.close()
    return {"message": "수정 완료"}

@router.delete("/products/{product_id}")
async def delete_product(product_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM map_violations WHERE product_id = ?", (product_id,))
    conn.execute("DELETE FROM map_price_records WHERE product_id = ?", (product_id,))
    conn.execute("DELETE FROM map_products WHERE id = ?", (product_id,))
    conn.commit(); conn.close()
    return {"message": "삭제 완료"}

class BulkUpdate(BaseModel):
    product_ids: List[int]
    is_active: Optional[bool] = None
    is_watched: Optional[bool] = None

@router.put("/products/bulk")
async def bulk_update_products(data: BulkUpdate):
    """일괄 감시/상시감시 ON/OFF"""
    if not data.product_ids:
        raise HTTPException(400, "제품 ID가 없습니다")
    conn = get_connection()
    sets = []
    vals = []
    if data.is_active is not None:
        sets.append("is_active = ?"); vals.append(1 if data.is_active else 0)
    if data.is_watched is not None:
        sets.append("is_watched = ?"); vals.append(1 if data.is_watched else 0)
        if not data.is_watched:
            sets.append("watch_interval_hours = NULL")
    if not sets:
        conn.close(); raise HTTPException(400, "변경 항목 없음")
    sets.append("updated_at = datetime('now','localtime')")
    placeholders = ",".join(["?"] * len(data.product_ids))
    conn.execute(f"UPDATE map_products SET {', '.join(sets)} WHERE id IN ({placeholders})", vals + data.product_ids)
    conn.commit(); conn.close()
    return {"message": f"{len(data.product_ids)}개 제품 업데이트 완료"}

@router.post("/products/upload")
async def upload_products_excel(file: UploadFile = File(...)):
    """엑셀/CSV로 제품 일괄 등록. 컬럼: 모델명|품명|브랜드|제품특징|지도가"""
    conn = get_connection()
    content = await file.read()
    added = updated = 0; errors = []
    try:
        if file.filename.endswith('.csv'):
            rows = list(csv.DictReader(io.StringIO(content.decode('utf-8-sig'))))
        else:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
            ws = wb.active
            headers = [c.value for c in ws[1] if c.value]
            rows = []
            for r in ws.iter_rows(min_row=2, values_only=True):
                if r[0]:
                    rows.append(dict(zip(headers, r)))
        col_map = {'모델명':'mn','model':'mn','model_name':'mn',
                   '품명':'pn','품목명':'pn','제품명':'pn','product_name':'pn','name':'pn',
                   '브랜드':'br','brand':'br',
                   '제품특징':'ft','특징':'ft','features':'ft',
                   '지도가':'mp','가격':'mp','price':'mp','map_price':'mp'}
        for i, row in enumerate(rows, 2):
            try:
                n = {}
                for k, v in row.items():
                    if k and k.strip() in col_map: n[col_map[k.strip()]] = v
                mn = str(n.get('mn','')).strip()
                pn = str(n.get('pn','')).strip()
                br = str(n.get('br','LANstar')).strip()
                ft = str(n.get('ft','')).strip()
                mp = int(float(str(n.get('mp',0)).replace(',','')))
                if not mn or not pn: errors.append(f"행{i}: 모델명/품명 누락"); continue
                sk = f"{br} {mn} {pn} {ft}"
                existing = conn.execute("SELECT id FROM map_products WHERE model_name=?", (mn,)).fetchone()
                if existing:
                    conn.execute("""UPDATE map_products SET product_name=?, brand=?, features=?, map_price=?,
                        search_keywords=?, updated_at=datetime('now','localtime') WHERE model_name=?""",
                        (pn, br, ft, mp, sk, mn))
                    updated += 1
                else:
                    conn.execute("""INSERT INTO map_products (model_name, product_name, brand, features, map_price, search_keywords)
                        VALUES (?,?,?,?,?,?)""", (mn, pn, br, ft, mp, sk))
                    added += 1
            except Exception as e: errors.append(f"행{i}: {e}")
        conn.commit()
    except Exception as e:
        conn.rollback(); conn.close(); raise HTTPException(500, f"파일 처리 오류: {e}")
    conn.close()
    return {"message": f"신규 {added}개, 업데이트 {updated}개", "added": added, "updated": updated, "errors": errors[:20]}

@router.put("/products/{product_id}/watch")
async def toggle_watch(product_id: int, watched: bool = True, interval_hours: int = 2):
    conn = get_connection()
    conn.execute("""UPDATE map_products SET is_watched=?, watch_interval_hours=?,
        updated_at=datetime('now','localtime') WHERE id=?""",
        (1 if watched else 0, interval_hours if watched else None, product_id))
    conn.commit(); conn.close()
    return {"message": f"상시감시 {'ON' if watched else 'OFF'}"}

@router.put("/products/{product_id}/map-price")
async def update_map_price(product_id: int, map_price: int = Query(...)):
    conn = get_connection()
    conn.execute("UPDATE map_products SET map_price=?, updated_at=datetime('now','localtime') WHERE id=?",
                 (map_price, product_id))
    conn.commit(); conn.close()
    return {"message": f"지도가 → {map_price:,}원"}

@router.put("/products/batch")
async def batch_update_products(action: str = Query(...), ids: str = Query(...)):
    """일괄 업데이트. action: monitor_on/monitor_off/watch_on/watch_off, ids: 쉼표 구분 ID"""
    conn = get_connection()
    id_list = [int(i) for i in ids.split(",") if i.strip()]
    if not id_list:
        conn.close(); raise HTTPException(400, "대상 ID가 없습니다")
    placeholders = ",".join(["?"] * len(id_list))
    if action == "monitor_on":
        conn.execute(f"UPDATE map_products SET is_active=1, updated_at=datetime('now','localtime') WHERE id IN ({placeholders})", id_list)
    elif action == "monitor_off":
        conn.execute(f"UPDATE map_products SET is_active=0, updated_at=datetime('now','localtime') WHERE id IN ({placeholders})", id_list)
    elif action == "watch_on":
        conn.execute(f"UPDATE map_products SET is_watched=1, watch_interval_hours=2, updated_at=datetime('now','localtime') WHERE id IN ({placeholders})", id_list)
    elif action == "watch_off":
        conn.execute(f"UPDATE map_products SET is_watched=0, watch_interval_hours=NULL, updated_at=datetime('now','localtime') WHERE id IN ({placeholders})", id_list)
    else:
        conn.close(); raise HTTPException(400, f"알 수 없는 action: {action}")
    conn.commit(); conn.close()
    labels = {"monitor_on":"감시 ON","monitor_off":"감시 OFF","watch_on":"상시감시 ON","watch_off":"상시감시 OFF"}
    return {"message": f"{len(id_list)}개 제품 {labels.get(action,action)} 완료"}


# ═══════════════════════════════════════════════════════
# 3. 위반 API
# ═══════════════════════════════════════════════════════

@router.get("/violations")
async def list_violations(
    severity: str = "", platform: str = "", resolved: bool = False,
    days: int = 7, limit: int = 50, offset: int = 0
):
    conn = get_connection()
    cutoff = _days_ago(days)
    sql = """SELECT v.*, p.model_name, p.product_name, p.brand
        FROM map_violations v JOIN map_products p ON v.product_id = p.id
        WHERE v.detected_at > ?"""
    params = [cutoff]
    if not resolved: sql += " AND v.is_resolved = 0"
    if severity: sql += " AND v.severity = ?"; params.append(severity)
    if platform: sql += " AND v.platform = ?"; params.append(platform)

    # 총 건수
    count_sql = sql.replace("SELECT v.*, p.model_name, p.product_name, p.brand", "SELECT COUNT(*) as cnt")
    total = conn.execute(count_sql, params).fetchone()["cnt"]

    sql += " ORDER BY v.detected_at DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return {"violations": [dict(r) for r in rows], "total": total}

@router.put("/violations/{violation_id}/resolve")
async def resolve_violation(violation_id: int, data: ViolationResolve):
    conn = get_connection()
    conn.execute("""UPDATE map_violations SET is_resolved=1,
        resolved_at=datetime('now','localtime'), resolution_note=? WHERE id=?""",
        (data.resolution_note, violation_id))
    conn.commit(); conn.close()
    return {"message": "해결 처리 완료"}


# ═══════════════════════════════════════════════════════
# 4. 대시보드 API
# ═══════════════════════════════════════════════════════

@router.get("/dashboard")
async def get_dashboard():
    conn = get_connection()
    # 설정
    s = conn.execute("SELECT * FROM map_settings WHERE id=1").fetchone()
    s = dict(s) if s else {}
    mp = s.get('min_price', 5000)

    # 제품 통계
    monitored = conn.execute("SELECT COUNT(*) as c FROM map_products WHERE is_active=1 AND map_price>=?", (mp,)).fetchone()["c"]
    watched = conn.execute("SELECT COUNT(*) as c FROM map_products WHERE is_watched=1").fetchone()["c"]
    total_p = conn.execute("SELECT COUNT(*) as c FROM map_products WHERE is_active=1").fetchone()["c"]

    # 위반 통계 (7일)
    cutoff7 = _days_ago(7)
    vs = conn.execute("""SELECT COUNT(*) as total,
        SUM(CASE WHEN severity='CRITICAL' THEN 1 ELSE 0 END) as critical,
        SUM(CASE WHEN severity='HIGH' THEN 1 ELSE 0 END) as high,
        SUM(CASE WHEN severity='MEDIUM' THEN 1 ELSE 0 END) as medium
        FROM map_violations WHERE detected_at>? AND is_resolved=0""", (cutoff7,)).fetchone()
    vs = dict(vs) if vs else {"total":0,"critical":0,"high":0,"medium":0}

    # Top 셀러 (30일)
    cutoff30 = _days_ago(30)
    top_rows = conn.execute("""SELECT seller_name, platform, COUNT(*) as cnt,
        MAX(detected_at) as last_at FROM map_violations WHERE detected_at>?
        GROUP BY seller_name, platform ORDER BY cnt DESC LIMIT 10""", (cutoff30,)).fetchall()
    top_sellers = [dict(r) for r in top_rows]

    # 최근 수집
    lc = conn.execute("SELECT * FROM map_collection_logs ORDER BY started_at DESC LIMIT 1").fetchone()

    # 최근 위반 5건
    rv = conn.execute("""SELECT v.*, p.model_name, p.product_name FROM map_violations v
        JOIN map_products p ON v.product_id=p.id WHERE v.is_resolved=0
        ORDER BY v.detected_at DESC LIMIT 5""").fetchall()

    conn.close()
    schedules = json.loads(s['schedules']) if isinstance(s.get('schedules'), str) else s.get('schedules', [])
    platforms = json.loads(s['platforms']) if isinstance(s.get('platforms'), str) else s.get('platforms', [])
    return {
        "total_products": total_p, "monitored_count": monitored, "watch_count": watched,
        "violation_stats": vs, "top_sellers": top_sellers,
        "recent_violations": [dict(r) for r in rv],
        "last_collection": dict(lc) if lc else None,
        "settings_summary": {"min_price": mp, "schedules": schedules,
                             "platforms": platforms, "watch_interval_hours": s.get('watch_interval_hours', 2)}
    }


# ═══════════════════════════════════════════════════════
# 5. 셀러 API
# ═══════════════════════════════════════════════════════

@router.get("/sellers")
async def list_sellers(risk: str = "", platform: str = ""):
    conn = get_connection()
    cutoff30 = _days_ago(30)
    sql = """SELECT s.*, (SELECT COUNT(*) FROM map_violations v
        WHERE v.seller_name=s.seller_name AND v.platform=s.platform AND v.detected_at>?) as recent_violations
        FROM map_sellers s WHERE 1=1"""
    params = [cutoff30]
    if risk: sql += " AND s.risk_level=?"; params.append(risk)
    if platform: sql += " AND s.platform=?"; params.append(platform)
    sql += " ORDER BY s.total_violations DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════
# 6. 가격 이력 API
# ═══════════════════════════════════════════════════════

@router.get("/price-history/{product_id}")
async def get_price_history(product_id: int, days: int = 7, platform: str = ""):
    conn = get_connection()
    cutoff = _days_ago(days)
    sql = """SELECT seller_name, platform, effective_price, display_price,
        coupon_price, is_violation, collected_at FROM map_price_records
        WHERE product_id=? AND collected_at>?"""
    params = [product_id, cutoff]
    if platform: sql += " AND platform=?"; params.append(platform)
    sql += " ORDER BY collected_at ASC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════
# 7. 수집 실행 API (백그라운드 + 진행률)
# ═══════════════════════════════════════════════════════

@router.post("/collect/run")
async def run_collection_now():
    """즉시 가격 수집 - 백그라운드 실행"""
    from services.map_collector_service import start_collection_background, collection_progress
    if collection_progress.get("running"):
        return {"message": "이미 수집 중입니다", "status": "running", "progress": collection_progress}
    import asyncio
    asyncio.create_task(start_collection_background())
    return {"message": "수집 시작됨", "status": "started"}

@router.get("/collect/progress")
async def get_collection_progress():
    """수집 진행률 조회"""
    from services.map_collector_service import collection_progress
    return collection_progress

@router.get("/collect/status")
async def get_collection_status():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM map_collection_logs ORDER BY started_at DESC LIMIT 10").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════
# 8. 통계 API
# ═══════════════════════════════════════════════════════

@router.get("/stats/violations-by-day")
async def violations_by_day(days: int = 30):
    conn = get_connection()
    cutoff = _days_ago(days)
    rows = conn.execute("""SELECT DATE(detected_at) as date, COUNT(*) as total,
        SUM(CASE WHEN severity='CRITICAL' THEN 1 ELSE 0 END) as critical,
        SUM(CASE WHEN severity='HIGH' THEN 1 ELSE 0 END) as high
        FROM map_violations WHERE detected_at>? GROUP BY DATE(detected_at) ORDER BY date""", (cutoff,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@router.get("/stats/violations-by-platform")
async def violations_by_platform(days: int = 30):
    conn = get_connection()
    cutoff = _days_ago(days)
    rows = conn.execute("""SELECT platform, COUNT(*) as count,
        SUM(CASE WHEN severity IN ('CRITICAL','HIGH') THEN 1 ELSE 0 END) as severe
        FROM map_violations WHERE detected_at>? GROUP BY platform ORDER BY count DESC""", (cutoff,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ═══════════════════════════════════════════════════════
# 9. 스케줄러 상태 API
# ═══════════════════════════════════════════════════════

@router.get("/scheduler/status")
async def scheduler_status():
    try:
        from services.map_scheduler import get_scheduler_status
        return get_scheduler_status()
    except Exception as e:
        return {"running": False, "error": str(e), "jobs": []}
