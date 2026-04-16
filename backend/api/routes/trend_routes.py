"""
FastAPI 라우터 - 트렌드 프로필 CRUD, 수집, 분석 API
order-agent 통합 시 app.include_router(router) 로 등록
"""
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import httpx
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

from db.database import get_connection
from services.trend_constants import (
    TREND_MONTHLY_START_PERIOD,
    TREND_DEFAULT_RESULT_COUNT,
    NAVER_ROOT_CATEGORIES,
    get_latest_collectible_period,
    list_monthly_periods,
    normalize_spreadsheet_id,
    build_sheet_url,
    now_kst_str,
)

logger = logging.getLogger("trend_routes")
router = APIRouter(prefix="/api/trends", tags=["Naver Trend"])


# ── 헬퍼 ─────────────────────────────────────────────

def _id() -> str:
    return uuid.uuid4().hex[:12]


def _now() -> str:
    return now_kst_str()


def _json(v) -> str:
    return json.dumps(v, ensure_ascii=False) if v else "[]"


def _parse(v, fallback=None):
    if not v:
        return fallback if fallback is not None else []
    try:
        return json.loads(v)
    except Exception:
        return fallback if fallback is not None else []


def _row_to_profile(r) -> dict:
    return {
        "id": r["id"], "slug": r["slug"], "name": r["name"], "status": r["status"],
        "categoryCid": r["category_cid"], "categoryPath": r["category_path"],
        "categoryDepth": r["category_depth"], "timeUnit": r["time_unit"],
        "devices": _parse(r["devices"]), "genders": _parse(r["genders"]),
        "ages": _parse(r["ages"]), "resultCount": r["result_count"],
        "excludeBrandProducts": bool(r["exclude_brand_products"]),
        "customExcludedTerms": _parse(r["custom_excluded_terms"]),
        "spreadsheetId": r["spreadsheet_id"],
        "startPeriod": r["start_period"], "endPeriod": r["end_period"],
        "lastCollectedPeriod": r["last_collected_period"],
        "lastSyncedAt": r["last_synced_at"], "syncStatus": r["sync_status"],
        "createdAt": r["created_at"], "updatedAt": r["updated_at"],
    }


def _row_to_run(r) -> dict:
    return {
        "id": r["id"], "profileId": r["profile_id"], "status": r["status"],
        "runType": r["run_type"],
        "startPeriod": r["start_period"], "endPeriod": r["end_period"],
        "totalTasks": r["total_tasks"], "completedTasks": r["completed_tasks"],
        "failedTasks": r["failed_tasks"], "totalSnapshots": r["total_snapshots"],
        "sheetUrl": r["sheet_url"],
        "createdAt": r["created_at"], "startedAt": r["started_at"],
        "completedAt": r["completed_at"], "updatedAt": r["updated_at"],
        "cancelledAt": r["cancelled_at"],
    }


def _row_to_task(r) -> dict:
    return {
        "id": r["id"], "runId": r["run_id"], "profileId": r["profile_id"],
        "period": r["period"], "status": r["status"],
        "completedPages": r["completed_pages"], "totalPages": r["total_pages"],
        "retryCount": r["retry_count"],
        "failureReason": r["failure_reason"], "failureSnippet": r["failure_snippet"],
        "createdAt": r["created_at"], "updatedAt": r["updated_at"],
    }


def _row_to_snapshot(r) -> dict:
    return {
        "id": r["id"], "profileId": r["profile_id"], "runId": r["run_id"],
        "taskId": r["task_id"], "period": r["period"],
        "keyword": r["keyword"], "rank": r["rank"],
        "categoryCid": r["category_cid"], "device": r["device"],
        "gender": r["gender"], "age": r["age"],
        "brandExcluded": bool(r["brand_excluded"]),
        "collectedAt": r["collected_at"],
    }


def _total_pages(result_count: int) -> int:
    return max(1, (result_count + 19) // 20)


def _make_slug(name: str, conn) -> str:
    base = name.strip().lower().replace(" ", "-").replace("/", "-").replace(">", "")
    base = "".join(c for c in base if c.isalnum() or c == "-" or ord(c) >= 0xAC00)
    base = base.strip("-")[:40] or f"trend-{int(datetime.now().timestamp())}"
    slug = base
    n = 2
    while conn.execute("SELECT 1 FROM trend_profiles WHERE slug=?", (slug,)).fetchone():
        slug = f"{base}-{n}"
        n += 1
    return slug


# ── Pydantic 모델 ────────────────────────────────────

class CollectRequest(BaseModel):
    name: str = "트렌드 분석"
    categoryCid: str = ""
    categoryPath: str = ""
    categoryDepth: int = 0
    devices: List[str] = ["pc", "mo"]
    genders: List[str] = ["f", "m"]
    ages: List[str] = ["10", "20", "30", "40", "50", "60"]
    resultCount: int = 20
    excludeBrandProducts: bool = True
    customExcludedTerms: List[str] = []
    spreadsheetId: str = ""
    startPeriod: str = ""
    endPeriod: str = ""


class BackfillRequest(BaseModel):
    startPeriod: str = ""
    endPeriod: str = ""


# ═══════════════════════════════════════════════════════
# Health & Dashboard
# ═══════════════════════════════════════════════════════

@router.get("/health")
async def health():
    return {"ok": True, "service": "datalab-trend-api"}


@router.get("/admin/board")
async def admin_board():
    conn = get_connection()
    try:
        profiles = [_row_to_profile(r) for r in
                    conn.execute("SELECT * FROM trend_profiles ORDER BY updated_at DESC").fetchall()]

        active_count = sum(1 for p in profiles if p["status"] == "active")

        qr = conn.execute("SELECT COUNT(*) as c FROM trend_runs WHERE status IN ('queued','running')").fetchone()
        queued_running = qr["c"] if qr else 0

        sr = conn.execute("SELECT COUNT(*) as c FROM trend_snapshots").fetchone()
        total_snap = sr["c"] if sr else 0

        fr = conn.execute("SELECT COUNT(*) as c FROM trend_tasks WHERE status='failed'").fetchone()
        failed = fr["c"] if fr else 0

        runs = []
        for row in conn.execute("""
            SELECT * FROM trend_runs
            ORDER BY CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1
                     WHEN 'completed' THEN 2 ELSE 3 END, updated_at DESC
            LIMIT 10
        """).fetchall():
            run = _row_to_run(row)
            tasks = [_row_to_task(t) for t in
                     conn.execute("SELECT * FROM trend_tasks WHERE run_id=? ORDER BY period", (run["id"],)).fetchall()]
            # profile for this run
            pr = conn.execute("SELECT * FROM trend_profiles WHERE id=?", (run["profileId"],)).fetchone()
            run["profile"] = _row_to_profile(pr) if pr else None
            run["tasks"] = tasks
            runs.append(run)

        return {
            "ok": True,
            "board": {
                "metrics": [
                    {"id": "profiles", "label": "활성 프로필", "value": f"{active_count}개"},
                    {"id": "runs", "label": "대기/실행", "value": f"{queued_running}건",
                     "tone": "progress" if queued_running > 0 else "stable"},
                    {"id": "snapshots", "label": "누적 수집", "value": f"{total_snap:,}건"},
                    {"id": "failures", "label": "실패 태스크", "value": f"{failed}건",
                     "tone": "attention" if failed > 0 else "stable"},
                ],
                "profiles": profiles,
                "runs": runs,
            }
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════
# Profiles
# ═══════════════════════════════════════════════════════

@router.get("/profiles")
async def list_profiles():
    conn = get_connection()
    try:
        rows = conn.execute("SELECT * FROM trend_profiles ORDER BY updated_at DESC").fetchall()
        return {"ok": True, "profiles": [_row_to_profile(r) for r in rows]}
    finally:
        conn.close()


@router.post("/profiles")
async def create_profile(body: CollectRequest):
    conn = get_connection()
    try:
        now = _now()
        pid = _id()
        slug = _make_slug(body.categoryPath or body.name, conn)
        latest = get_latest_collectible_period()
        sp = body.startPeriod or TREND_MONTHLY_START_PERIOD
        ep = body.endPeriod or latest
        rc = 20 if body.resultCount <= 20 else 40

        conn.execute("""
            INSERT INTO trend_profiles
            (id, slug, name, status, category_cid, category_path, category_depth,
             time_unit, devices, genders, ages, result_count,
             exclude_brand_products, custom_excluded_terms, spreadsheet_id,
             start_period, end_period, last_collected_period, sync_status,
             created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            pid, slug, body.name.strip(), "active",
            body.categoryCid, body.categoryPath.strip(), body.categoryDepth,
            "month", _json(sorted(body.devices)), _json(sorted(body.genders)),
            _json(sorted(body.ages)), rc,
            1 if body.excludeBrandProducts else 0,
            _json(body.customExcludedTerms), normalize_spreadsheet_id(body.spreadsheetId),
            sp, ep, "", "idle", now, now,
        ))
        conn.commit()
        row = conn.execute("SELECT * FROM trend_profiles WHERE id=?", (pid,)).fetchone()
        return {"ok": True, "profile": _row_to_profile(row)}
    finally:
        conn.close()


@router.post("/collect")
async def start_collection(body: CollectRequest):
    """수집 시작 — 프로필 재사용 또는 생성 후 backfill run 생성"""
    conn = get_connection()
    try:
        # 같은 필터의 기존 프로필 찾기
        rows = conn.execute("""
            SELECT * FROM trend_profiles
            WHERE category_cid=? AND devices=? AND genders=? AND ages=?
              AND result_count=? AND exclude_brand_products=?
            ORDER BY updated_at DESC LIMIT 1
        """, (
            body.categoryCid, _json(sorted(body.devices)),
            _json(sorted(body.genders)), _json(sorted(body.ages)),
            20 if body.resultCount <= 20 else 40,
            1 if body.excludeBrandProducts else 0,
        )).fetchall()

        if rows:
            profile = _row_to_profile(rows[0])
            pid = profile["id"]
        else:
            res = await create_profile(body)
            profile = res["profile"]
            pid = profile["id"]

        # 이미 진행 중인 run 확인
        active = conn.execute(
            "SELECT * FROM trend_runs WHERE profile_id=? AND status IN ('queued','running') LIMIT 1",
            (pid,)
        ).fetchone()
        if active:
            run = _row_to_run(active)
            tasks = [_row_to_task(t) for t in
                     conn.execute("SELECT * FROM trend_tasks WHERE run_id=? ORDER BY period", (run["id"],)).fetchall()]
            run["profile"] = profile
            run["tasks"] = tasks
            return {"ok": True, "run": run, "reused": True}

        # 새 backfill 생성
        return await _create_backfill(conn, pid, profile)
    finally:
        conn.close()


@router.delete("/profiles/{profile_id}")
async def delete_profile(profile_id: str):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM trend_snapshots WHERE profile_id=?", (profile_id,))
        conn.execute("DELETE FROM trend_tasks WHERE profile_id=?", (profile_id,))
        conn.execute("DELETE FROM trend_runs WHERE profile_id=?", (profile_id,))
        conn.execute("DELETE FROM trend_profiles WHERE id=?", (profile_id,))
        conn.commit()
        return {"ok": True, "deletedProfileId": profile_id}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════
# Categories (네이버 API 프록시)
# ═══════════════════════════════════════════════════════

@router.get("/categories/{cid}")
async def get_categories(cid: str):
    if cid == "0" or cid == "root":
        return {"ok": True, "nodes": NAVER_ROOT_CATEGORIES}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"https://datalab.naver.com/shoppingInsight/getCategory.naver?cid={cid}",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://datalab.naver.com/"}
            )
            r.raise_for_status()
            data = r.json()
            nodes = [{"cid": str(n.get("cid", "")), "name": n.get("name", "")}
                     for n in data.get("childList", [])]
            return {"ok": True, "nodes": nodes}
    except Exception as e:
        logger.error(f"카테고리 조회 실패: {e}")
        raise HTTPException(500, f"네이버 카테고리 조회 실패: {e}")


# ═══════════════════════════════════════════════════════
# Runs
# ═══════════════════════════════════════════════════════

@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM trend_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run not found")
        run = _row_to_run(row)
        tasks = [_row_to_task(t) for t in
                 conn.execute("SELECT * FROM trend_tasks WHERE run_id=? ORDER BY period", (run_id,)).fetchall()]
        pr = conn.execute("SELECT * FROM trend_profiles WHERE id=?", (run["profileId"],)).fetchone()
        run["profile"] = _row_to_profile(pr) if pr else None
        run["tasks"] = tasks
        return {"ok": True, "run": run}
    finally:
        conn.close()


@router.get("/runs/{run_id}/snapshots")
async def get_snapshots(
    run_id: str,
    period: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM trend_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Run not found")
        profile_id = row["profile_id"]

        if not period:
            lr = conn.execute(
                "SELECT period FROM trend_snapshots WHERE profile_id=? ORDER BY period DESC LIMIT 1",
                (profile_id,)
            ).fetchone()
            period = lr["period"] if lr else ""

        if not period:
            return {"ok": True, "period": "", "items": [], "totalItems": 0, "page": 1, "totalPages": 0}

        total = conn.execute(
            "SELECT COUNT(*) as c FROM trend_snapshots WHERE profile_id=? AND period=?",
            (profile_id, period)
        ).fetchone()["c"]

        offset = (page - 1) * limit
        items = [_row_to_snapshot(s) for s in conn.execute(
            "SELECT * FROM trend_snapshots WHERE profile_id=? AND period=? ORDER BY rank LIMIT ? OFFSET ?",
            (profile_id, period, limit, offset)
        ).fetchall()]

        return {
            "ok": True, "period": period, "items": items,
            "totalItems": total, "page": page,
            "totalPages": max(1, (total + limit - 1) // limit),
        }
    finally:
        conn.close()


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str):
    conn = get_connection()
    try:
        now = _now()
        conn.execute("UPDATE trend_tasks SET status='cancelled', updated_at=? WHERE run_id=? AND status IN ('pending','running')", (now, run_id))
        conn.execute("UPDATE trend_runs SET status='cancelled', cancelled_at=?, updated_at=? WHERE id=?", (now, now, run_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.delete("/runs/{run_id}")
async def delete_run(run_id: str):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM trend_snapshots WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM trend_tasks WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM trend_runs WHERE id=?", (run_id,))
        conn.commit()
        return {"ok": True, "deletedRunId": run_id}
    finally:
        conn.close()


@router.post("/runs/{run_id}/retry-failures")
async def retry_failures(run_id: str):
    conn = get_connection()
    try:
        now = _now()
        conn.execute(
            "UPDATE trend_tasks SET status='pending', retry_count=retry_count+1, failure_reason=NULL, failure_snippet=NULL, updated_at=? WHERE run_id=? AND status='failed'",
            (now, run_id))
        conn.execute("UPDATE trend_runs SET status='queued', failed_tasks=0, updated_at=? WHERE id=?", (now, run_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════
# Collection Control
# ═══════════════════════════════════════════════════════

@router.post("/profiles/{profile_id}/backfill")
async def start_backfill(profile_id: str, body: BackfillRequest = BackfillRequest()):
    conn = get_connection()
    try:
        pr = conn.execute("SELECT * FROM trend_profiles WHERE id=?", (profile_id,)).fetchone()
        if not pr:
            raise HTTPException(404, "Profile not found")
        profile = _row_to_profile(pr)
        return await _create_backfill(conn, profile_id, profile)
    finally:
        conn.close()


@router.post("/profiles/{profile_id}/sync-sheet")
async def sync_sheet(profile_id: str, background_tasks: BackgroundTasks):
    """구글 시트 동기화 (백그라운드)"""
    conn = get_connection()
    try:
        pr = conn.execute("SELECT * FROM trend_profiles WHERE id=?", (profile_id,)).fetchone()
        if not pr:
            raise HTTPException(404, "Profile not found")
        now = _now()
        conn.execute("UPDATE trend_profiles SET sync_status='syncing', updated_at=? WHERE id=?", (now, profile_id))
        conn.commit()
    finally:
        conn.close()

    from services.google_sheets import sync_profile_to_sheets
    background_tasks.add_task(sync_profile_to_sheets, profile_id)
    return {"ok": True, "message": "시트 동기화 시작"}


# ═══════════════════════════════════════════════════════
# Worker
# ═══════════════════════════════════════════════════════

@router.post("/worker/process-next")
async def worker_process_next():
    from services.naver_collector import process_next_task
    result = process_next_task()
    return result or {"ok": True, "processed": False, "message": "처리할 태스크 없음"}


# ═══════════════════════════════════════════════════════
# Analysis
# ═══════════════════════════════════════════════════════

@router.get("/profiles/{profile_id}/analysis")
async def get_analysis(profile_id: str):
    conn = get_connection()
    try:
        pr = conn.execute("SELECT * FROM trend_profiles WHERE id=?", (profile_id,)).fetchone()
        if not pr:
            raise HTTPException(404, "Profile not found")
        profile = _row_to_profile(pr)

        snapshots = [_row_to_snapshot(s) for s in conn.execute(
            "SELECT * FROM trend_snapshots WHERE profile_id=? ORDER BY period, rank",
            (profile_id,)
        ).fetchall()]

        if not snapshots:
            return {"ok": False, "message": "수집된 스냅샷이 없습니다. 먼저 수집을 실행하세요."}

        observed = sorted(set(s["period"] for s in snapshots))

        from services.trend_analysis import build_trend_analysis
        analysis = build_trend_analysis(snapshots, profile, observed)
        return {"ok": True, "analysis": analysis}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════
# 내부 헬퍼
# ═══════════════════════════════════════════════════════

async def _create_backfill(conn, profile_id: str, profile: dict) -> dict:
    """backfill run 생성 — 미수집 월에 대해 태스크 생성"""
    latest = get_latest_collectible_period()
    sp = profile.get("startPeriod") or TREND_MONTHLY_START_PERIOD
    ep = latest
    all_periods = list_monthly_periods(sp, ep)
    rc = profile.get("resultCount", 20)

    # 이미 수집된 월 확인
    existing = set()
    for r in conn.execute(
        "SELECT DISTINCT period FROM trend_snapshots WHERE profile_id=? AND rank<=?",
        (profile_id, rc)
    ).fetchall():
        cnt = conn.execute(
            "SELECT COUNT(*) as c FROM trend_snapshots WHERE profile_id=? AND period=? AND rank<=?",
            (profile_id, r["period"], rc)
        ).fetchone()["c"]
        if cnt >= rc:
            existing.add(r["period"])

    # pending 태스크가 있는 월 제외
    pending_periods = set()
    for r in conn.execute(
        "SELECT DISTINCT period FROM trend_tasks WHERE profile_id=? AND status IN ('pending','running')",
        (profile_id,)
    ).fetchall():
        pending_periods.add(r["period"])

    target = [p for p in all_periods if p not in existing and p not in pending_periods]

    now = _now()
    run_id = _id()
    status = "queued" if target else "completed"

    conn.execute("""
        INSERT INTO trend_runs (id, profile_id, status, run_type, start_period, end_period,
            total_tasks, completed_tasks, failed_tasks, total_snapshots,
            created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (run_id, profile_id, status, "backfill", sp, ep,
          len(target), 0, 0, 0, now, now))

    tp = _total_pages(rc)
    for period in target:
        conn.execute("""
            INSERT INTO trend_tasks (id, run_id, profile_id, period, status,
                completed_pages, total_pages, retry_count, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (_id(), run_id, profile_id, period, "pending", 0, tp, 0, now, now))

    conn.commit()

    run_row = conn.execute("SELECT * FROM trend_runs WHERE id=?", (run_id,)).fetchone()
    run = _row_to_run(run_row)
    tasks = [_row_to_task(t) for t in
             conn.execute("SELECT * FROM trend_tasks WHERE run_id=? ORDER BY period", (run_id,)).fetchall()]
    run["profile"] = profile
    run["tasks"] = tasks

    return {"ok": True, "run": run, "targetPeriods": len(target)}
