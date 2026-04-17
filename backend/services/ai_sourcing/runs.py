from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Sequence

from .analysis import apply_brand_exclusion, build_trend_analysis
from .constants import (
    DEFAULT_OPERATOR_ID,
    TREND_MONTHLY_START_PERIOD,
    TREND_PAGE_SIZE,
    get_latest_collectible_trend_period,
    get_trend_total_pages,
    list_monthly_periods,
    normalize_excluded_terms,
    normalize_trend_result_count,
    normalize_trend_spreadsheet_id,
    now_iso,
)
from .crawler import collect_monthly_ranks, summarize_failure_snippet
from .db import all_rows, json_dump, json_parse, one, run, run_many, scalar
from .mappers import map_profile, map_run, map_snapshot, map_task
from .models import (
    TrendAdminBoard,
    TrendAdminMetric,
    TrendCollectionRun,
    TrendKeywordSnapshot,
    TrendProfile,
    TrendProfileInput,
    TrendRunDetail,
)

_process_lock = asyncio.Lock()


# ---------- helpers ----------


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _slugify(value: str) -> str:
    compact = value.strip().lower()
    compact = re.sub(r"\s+", "-", compact)
    compact = re.sub(r"[^0-9a-z\uac00-\ud7a3\-]", "", compact)
    compact = re.sub(r"-+", "-", compact).strip("-")
    return compact or f"trend-{int(datetime.now(tz=timezone.utc).timestamp() * 1000)}"


def _normalize_profile_input(input_: TrendProfileInput) -> TrendProfileInput:
    trimmed_name = (input_.name or "").strip() or input_.categoryPath.strip() or "한이룸 트렌드 분석"
    return TrendProfileInput(
        name=trimmed_name,
        categoryCid=int(input_.categoryCid),
        categoryPath=input_.categoryPath.strip(),
        categoryDepth=int(input_.categoryDepth),
        timeUnit=input_.timeUnit,
        devices=sorted(input_.devices or []),
        genders=sorted(input_.genders or []),
        ages=sorted(input_.ages or []),
        spreadsheetId=normalize_trend_spreadsheet_id(input_.spreadsheetId or ""),
        resultCount=normalize_trend_result_count(input_.resultCount),
        excludeBrandProducts=bool(input_.excludeBrandProducts),
        customExcludedTerms=normalize_excluded_terms(input_.customExcludedTerms),
    )


async def list_profiles() -> list[TrendProfile]:
    rows = all_rows("SELECT * FROM trend_profiles ORDER BY updated_at DESC")
    return [map_profile(row) for row in rows]


def _load_snapshots(profile_id: str, result_count: int) -> list[TrendKeywordSnapshot]:
    rows = all_rows(
        "SELECT * FROM trend_snapshots WHERE profile_id = ? AND rank <= ? ORDER BY period ASC, rank ASC",
        [profile_id, result_count],
    )
    return [map_snapshot(row) for row in rows]


# ---------- detail builder ----------


async def build_run_detail(run_record: TrendCollectionRun) -> TrendRunDetail:
    profile_row = one("SELECT * FROM trend_profiles WHERE id = ?", [run_record.profileId])
    profile = map_profile(profile_row)
    tasks = [map_task(row) for row in all_rows(
        "SELECT * FROM trend_tasks WHERE run_id = ? ORDER BY period ASC", [run_record.id]
    )]
    snapshots = _load_snapshots(profile.id, profile.resultCount)
    visible = [s for s in snapshots if not (profile.excludeBrandProducts and s.brandExcluded)]
    completed_periods = sorted(
        {task.period for task in tasks if task.status == "completed"},
        reverse=True,
    )
    snapshot_periods = sorted({s.period for s in snapshots}, reverse=True)
    latest_completed_period = (
        completed_periods[0] if completed_periods else (snapshot_periods[0] if snapshot_periods else None)
    )
    preview_source = [s for s in visible if s.period == latest_completed_period] if latest_completed_period else []
    snapshots_preview = preview_source[:TREND_PAGE_SIZE]

    running_task = next((t for t in tasks if t.status == "running"), None) or next(
        (t for t in tasks if t.status == "pending"), None
    )
    durations = []
    for task in tasks:
        if task.status == "completed" and task.startedAt and task.completedAt:
            try:
                start = datetime.fromisoformat(task.startedAt.replace("Z", "+00:00"))
                end = datetime.fromisoformat(task.completedAt.replace("Z", "+00:00"))
                durations.append(max(1.0, (end - start).total_seconds()))
            except ValueError:
                continue
    average_task_seconds = round(sum(durations) / len(durations)) if durations else 8
    remaining_tasks = max(0, run_record.totalTasks - run_record.completedTasks)
    eta_minutes = (
        0
        if run_record.status == "completed" or remaining_tasks == 0
        else max(1, -(-(remaining_tasks * average_task_seconds) // 60))
    )
    if eta_minutes > 0:
        estimated_completion_at = (
            datetime.now(tz=timezone.utc).timestamp() + eta_minutes * 60
        )
        estimated_iso = datetime.fromtimestamp(estimated_completion_at, tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    else:
        estimated_iso = run_record.completedAt

    current_page = None
    if running_task:
        delta = 1 if (running_task.status == "running" and running_task.completedPages < running_task.totalPages) else 0
        current_page = min(running_task.totalPages, max(1, running_task.completedPages + delta))

    expected_periods = list_monthly_periods(profile.startPeriod, profile.endPeriod)
    completed_period_count = len({s.period for s in snapshots})
    analysis_ready = run_record.status == "completed" and completed_period_count >= len(expected_periods)
    analysis = build_trend_analysis(profile, snapshots) if analysis_ready else None

    return TrendRunDetail(
        **run_record.model_dump(),
        profile=profile,
        tasks=tasks,
        snapshotsPreview=snapshots_preview,
        currentPeriod=running_task.period if running_task else None,
        currentPage=current_page,
        latestCompletedPeriod=latest_completed_period,
        remainingTasks=remaining_tasks,
        averageTaskSeconds=average_task_seconds,
        etaMinutes=eta_minutes,
        estimatedCompletionAt=estimated_iso,
        canCancel=run_record.status in ("queued", "running"),
        canDelete=True,
        analysisReady=analysis_ready,
        confidenceScore=analysis["confidenceScore"] if analysis else None,
        analysisSummary=analysis["summary"] if analysis else None,
        analysisCards=analysis["cards"] if analysis else [],
    ).model_copy(update={"totalSnapshots": len(snapshots)})


# ---------- board detail (lighter) ----------


async def build_run_board_detail(run_record: TrendCollectionRun) -> TrendRunDetail:
    profile_row = one("SELECT * FROM trend_profiles WHERE id = ?", [run_record.profileId])
    profile = map_profile(profile_row)
    tasks = [map_task(row) for row in all_rows(
        "SELECT * FROM trend_tasks WHERE run_id = ? ORDER BY period ASC", [run_record.id]
    )]
    completed_periods = sorted(
        {task.period for task in tasks if task.status == "completed"},
        reverse=True,
    )
    latest_completed_period = completed_periods[0] if completed_periods else None
    if latest_completed_period is None:
        period_value = scalar(
            "SELECT period FROM trend_snapshots WHERE profile_id = ? AND rank <= ? ORDER BY period DESC LIMIT 1",
            [profile.id, profile.resultCount],
        )
        latest_completed_period = period_value

    preview_rows: list[TrendKeywordSnapshot] = []
    if latest_completed_period:
        rows = all_rows(
            "SELECT * FROM trend_snapshots WHERE profile_id = ? AND period = ? AND rank <= ? ORDER BY rank ASC LIMIT ?",
            [profile.id, latest_completed_period, profile.resultCount, TREND_PAGE_SIZE],
        )
        preview_rows = [map_snapshot(row) for row in rows]
    snapshots_preview = [
        snapshot for snapshot in preview_rows if not (profile.excludeBrandProducts and snapshot.brandExcluded)
    ]

    running_task = next((t for t in tasks if t.status == "running"), None) or next(
        (t for t in tasks if t.status == "pending"), None
    )
    durations = []
    for task in tasks:
        if task.status == "completed" and task.startedAt and task.completedAt:
            try:
                start = datetime.fromisoformat(task.startedAt.replace("Z", "+00:00"))
                end = datetime.fromisoformat(task.completedAt.replace("Z", "+00:00"))
                durations.append(max(1.0, (end - start).total_seconds()))
            except ValueError:
                continue
    average_task_seconds = round(sum(durations) / len(durations)) if durations else 8
    remaining_tasks = max(0, run_record.totalTasks - run_record.completedTasks)
    eta_minutes = (
        0
        if run_record.status == "completed" or remaining_tasks == 0
        else max(1, -(-(remaining_tasks * average_task_seconds) // 60))
    )
    if eta_minutes > 0:
        estimated = datetime.now(tz=timezone.utc).timestamp() + eta_minutes * 60
        estimated_iso = datetime.fromtimestamp(estimated, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    else:
        estimated_iso = run_record.completedAt
    current_page = None
    if running_task:
        delta = 1 if (running_task.status == "running" and running_task.completedPages < running_task.totalPages) else 0
        current_page = min(running_task.totalPages, max(1, running_task.completedPages + delta))

    expected_periods = list_monthly_periods(profile.startPeriod, profile.endPeriod)
    completed_period_count = len({task.period for task in tasks if task.status == "completed"})

    return TrendRunDetail(
        **run_record.model_dump(),
        profile=profile,
        tasks=tasks,
        snapshotsPreview=snapshots_preview,
        currentPeriod=running_task.period if running_task else None,
        currentPage=current_page,
        latestCompletedPeriod=latest_completed_period,
        remainingTasks=remaining_tasks,
        averageTaskSeconds=average_task_seconds,
        etaMinutes=eta_minutes,
        estimatedCompletionAt=estimated_iso,
        canCancel=run_record.status in ("queued", "running"),
        canDelete=True,
        analysisReady=run_record.status == "completed" and completed_period_count >= len(expected_periods),
        analysisCards=[],
    )


# ---------- admin board ----------


async def get_admin_board() -> TrendAdminBoard:
    profiles = await list_profiles()
    run_rows = all_rows(
        """SELECT * FROM trend_runs
           ORDER BY CASE status
             WHEN 'running' THEN 0
             WHEN 'queued' THEN 1
             WHEN 'completed' THEN 2
             WHEN 'cancelled' THEN 3
             WHEN 'failed' THEN 4
             ELSE 5 END,
             updated_at DESC
           LIMIT 8"""
    )
    run_details = [await build_run_board_detail(map_run(row)) for row in run_rows]
    total_snapshots = scalar("SELECT COUNT(*) FROM trend_snapshots WHERE rank <= ?", [40]) or 0
    failed_tasks = scalar("SELECT COUNT(*) FROM trend_tasks WHERE status = 'failed'", []) or 0
    queued_runs = scalar(
        "SELECT COUNT(*) FROM trend_runs WHERE status IN ('queued', 'running')", []
    ) or 0

    latest_sync = None
    sync_values = [profile.lastSyncedAt for profile in profiles if profile.lastSyncedAt]
    if sync_values:
        latest_sync = sorted(sync_values, reverse=True)[0]

    metrics = [
        TrendAdminMetric(
            id="profiles",
            label="활성 프로필",
            value=f"{len([p for p in profiles if p.status == 'active'])}개",
            hint="수집 가능한 필터 프로필 개수",
            tone="stable",
        ),
        TrendAdminMetric(
            id="runs",
            label="대기/실행 런",
            value=f"{queued_runs}건",
            hint="백그라운드 워커가 처리할 백필 런 상태",
            tone="progress" if queued_runs > 0 else "stable",
        ),
        TrendAdminMetric(
            id="snapshots",
            label="누적 수집",
            value=f"{int(total_snapshots):,}건",
            hint="2021-01부터 누적된 월별 인기검색어 캐시",
            tone="stable",
        ),
        TrendAdminMetric(
            id="failures",
            label="실패 태스크",
            value=f"{failed_tasks}건",
            hint=f"마지막 동기화 {latest_sync}" if latest_sync else "시트 동기화는 아직 비활성화입니다.",
            tone="attention" if int(failed_tasks) > 0 else "stable",
        ),
    ]

    return TrendAdminBoard(
        generatedAt=now_iso(),
        metrics=metrics,
        profiles=profiles,
        runs=run_details,
    )


# ---------- profile create / reuse ----------


async def create_profile(input_: TrendProfileInput) -> dict[str, Any]:
    normalized = _normalize_profile_input(input_)
    if normalized.timeUnit != "month":
        return {"ok": False, "code": "TIME_UNIT_NOT_SUPPORTED", "message": "v1에서는 월간만 지원합니다."}
    latest_period = get_latest_collectible_trend_period()
    now = now_iso()
    slug_base = _slugify(normalized.name)
    slug = slug_base
    suffix = 2
    while one("SELECT id FROM trend_profiles WHERE slug = ?", [slug]):
        slug = f"{slug_base}-{suffix}"
        suffix += 1

    profile = TrendProfile(
        id=_new_uuid(),
        slug=slug,
        status="active",
        startPeriod=TREND_MONTHLY_START_PERIOD,
        endPeriod=latest_period,
        lastCollectedPeriod=None,
        lastSyncedAt=None,
        syncStatus="idle",
        latestRunId=None,
        resultCount=normalized.resultCount or 20,
        excludeBrandProducts=normalized.excludeBrandProducts,
        customExcludedTerms=list(normalized.customExcludedTerms or []),
        createdAt=now,
        updatedAt=now,
        name=normalized.name,
        categoryCid=normalized.categoryCid,
        categoryPath=normalized.categoryPath,
        categoryDepth=normalized.categoryDepth,
        timeUnit="month",
        devices=list(normalized.devices or []),
        genders=list(normalized.genders or []),
        ages=list(normalized.ages or []),
        spreadsheetId=normalize_trend_spreadsheet_id(normalized.spreadsheetId),
    )

    run(
        """INSERT INTO trend_profiles (
             id, slug, name, status, start_period, end_period, last_collected_period, last_synced_at, sync_status, latest_run_id,
             created_at, updated_at, category_cid, category_path, category_depth, time_unit,
             devices_json, genders_json, ages_json, spreadsheet_id, result_count, exclude_brand_products, custom_excluded_terms_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            profile.id,
            profile.slug,
            profile.name,
            profile.status,
            profile.startPeriod,
            profile.endPeriod,
            profile.lastCollectedPeriod,
            profile.lastSyncedAt,
            profile.syncStatus,
            profile.latestRunId,
            profile.createdAt,
            profile.updatedAt,
            profile.categoryCid,
            profile.categoryPath,
            profile.categoryDepth,
            profile.timeUnit,
            json_dump(profile.devices),
            json_dump(profile.genders),
            json_dump(profile.ages),
            profile.spreadsheetId,
            profile.resultCount,
            1 if profile.excludeBrandProducts else 0,
            json_dump(profile.customExcludedTerms),
        ],
    )
    return {"ok": True, "profile": profile.model_dump()}


async def start_collection(input_: TrendProfileInput) -> dict[str, Any]:
    normalized = _normalize_profile_input(input_)
    if normalized.timeUnit != "month":
        return {"ok": False, "code": "TIME_UNIT_NOT_SUPPORTED", "message": "v1에서는 월간만 지원합니다."}
    existing = one(
        """SELECT * FROM trend_profiles
            WHERE category_cid = ?
              AND time_unit = 'month'
              AND devices_json = ?
              AND genders_json = ?
              AND ages_json = ?
              AND result_count = ?
              AND exclude_brand_products = ?
              AND custom_excluded_terms_json = ?
            ORDER BY updated_at DESC
            LIMIT 1""",
        [
            normalized.categoryCid,
            json_dump(list(normalized.devices)),
            json_dump(list(normalized.genders)),
            json_dump(list(normalized.ages)),
            normalized.resultCount,
            1 if normalized.excludeBrandProducts else 0,
            json_dump(list(normalized.customExcludedTerms or [])),
        ],
    )
    profile = map_profile(existing) if existing else None
    profile_id = profile.id if profile else None
    if not profile_id:
        created = await create_profile(normalized)
        if not created.get("ok"):
            return created
        profile = TrendProfile(**created["profile"])
        profile_id = profile.id

    latest_period = get_latest_collectible_trend_period()
    now = now_iso()
    if profile and profile.endPeriod != latest_period:
        run(
            "UPDATE trend_profiles SET end_period = ?, updated_at = ? WHERE id = ?",
            [latest_period, now, profile.id],
        )
        profile = profile.model_copy(update={"endPeriod": latest_period, "updatedAt": now})

    active_row = one(
        "SELECT * FROM trend_runs WHERE profile_id = ? AND status IN ('queued','running') ORDER BY updated_at DESC LIMIT 1",
        [profile_id],
    )
    if active_row:
        detail = await build_run_detail(map_run(active_row))
        return {"ok": True, "reusedCachedResult": False, "run": detail.model_dump()}

    reusable = await _find_reusable_completed_run(profile) if profile else None
    if reusable:
        detail = await build_run_detail(reusable)
        return {"ok": True, "reusedCachedResult": True, "run": detail.model_dump()}

    started = await start_backfill(profile_id)
    if started.get("ok"):
        started["reusedCachedResult"] = False
    return started


async def _find_reusable_completed_run(profile: TrendProfile) -> TrendCollectionRun | None:
    periods = list_monthly_periods(profile.startPeriod, get_latest_collectible_trend_period())
    completed_rows = all_rows(
        "SELECT period, COUNT(*) AS count FROM trend_snapshots WHERE profile_id = ? AND rank <= ? GROUP BY period",
        [profile.id, profile.resultCount],
    )
    completed_map = {row["period"]: int(row["count"]) >= profile.resultCount for row in completed_rows}
    if not periods or any(not completed_map.get(period) for period in periods):
        return None
    run_row = one(
        "SELECT * FROM trend_runs WHERE profile_id = ? AND status = 'completed' ORDER BY updated_at DESC LIMIT 1",
        [profile.id],
    )
    if not run_row:
        return None

    existing_task_rows = all_rows("SELECT period FROM trend_tasks WHERE run_id = ?", [run_row["id"]])
    existing_periods = {row["period"] for row in existing_task_rows}
    missing = [period for period in periods if period not in existing_periods]
    now = now_iso()

    if missing:
        rows = []
        for period in missing:
            rows.append([
                _new_uuid(),
                run_row["id"],
                profile.id,
                period,
                "completed",
                get_trend_total_pages(profile.resultCount),
                get_trend_total_pages(profile.resultCount),
                0,
                now,
                now,
                None,
                None,
                now,
            ])
        run_many(
            """INSERT INTO trend_tasks (
                 id, run_id, profile_id, period, status, completed_pages, total_pages, retry_count,
                 started_at, completed_at, failure_reason, failure_snippet, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )

    run(
        """UPDATE trend_runs
              SET status = 'completed',
                  start_period = ?,
                  end_period = ?,
                  total_tasks = ?,
                  completed_tasks = ?,
                  failed_tasks = 0,
                  total_snapshots = ?,
                  cancelled_at = NULL,
                  failure_reason = NULL,
                  updated_at = ?
            WHERE id = ?""",
        [
            profile.startPeriod,
            get_latest_collectible_trend_period(),
            len(periods),
            len(periods),
            len(periods) * profile.resultCount,
            now,
            run_row["id"],
        ],
    )
    refreshed = one("SELECT * FROM trend_runs WHERE id = ?", [run_row["id"]])
    return map_run(refreshed) if refreshed else None


# ---------- start / cancel / delete / retry ----------


async def start_backfill(profile_id: str) -> dict[str, Any]:
    profile_row = one("SELECT * FROM trend_profiles WHERE id = ?", [profile_id])
    if not profile_row:
        return {
            "ok": False,
            "code": "TREND_PROFILE_NOT_FOUND",
            "message": "profileId에 해당하는 트렌드 프로필이 없습니다.",
        }
    profile = map_profile(profile_row)
    latest_period = get_latest_collectible_trend_period()
    now = now_iso()
    if profile.endPeriod != latest_period:
        run(
            "UPDATE trend_profiles SET end_period = ?, updated_at = ? WHERE id = ?",
            [latest_period, now, profile_id],
        )
        profile = profile.model_copy(update={"endPeriod": latest_period, "updatedAt": now})

    periods = list_monthly_periods(profile.startPeriod, latest_period)
    completed_rows = all_rows(
        "SELECT period, COUNT(*) AS count FROM trend_snapshots WHERE profile_id = ? AND rank <= ? GROUP BY period",
        [profile_id, profile.resultCount],
    )
    completed_map = {row["period"]: int(row["count"]) >= profile.resultCount for row in completed_rows}
    pending_rows = all_rows(
        "SELECT DISTINCT period FROM trend_tasks WHERE profile_id = ? AND status IN ('pending','running')",
        [profile_id],
    )
    pending = {row["period"] for row in pending_rows}
    target_periods = [period for period in periods if not completed_map.get(period) and period not in pending]

    run_id = _new_uuid()
    status = "queued" if target_periods else "completed"
    completed_at = None if target_periods else now

    run(
        """INSERT INTO trend_runs (
             id, profile_id, status, requested_by, run_type, start_period, end_period, total_tasks,
             completed_tasks, failed_tasks, total_snapshots, sheet_url, started_at, completed_at, cancelled_at,
             failure_reason, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            run_id,
            profile_id,
            status,
            DEFAULT_OPERATOR_ID,
            "backfill",
            profile.startPeriod,
            latest_period,
            len(target_periods),
            0,
            0,
            0,
            None,
            None,
            completed_at,
            None,
            None,
            now,
            now,
        ],
    )

    if target_periods:
        rows = []
        total_pages = get_trend_total_pages(profile.resultCount)
        for period in target_periods:
            rows.append(
                [_new_uuid(), run_id, profile_id, period, "pending", 0, total_pages, 0, None, None, None, None, now]
            )
        run_many(
            """INSERT INTO trend_tasks (
                 id, run_id, profile_id, period, status, completed_pages, total_pages, retry_count,
                 started_at, completed_at, failure_reason, failure_snippet, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )

    run(
        "UPDATE trend_profiles SET latest_run_id = ?, updated_at = ? WHERE id = ?",
        [run_id, now, profile_id],
    )
    row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    record = map_run(row)
    detail = await build_run_detail(record)
    return {"ok": True, "run": detail.model_dump()}


async def get_run(run_id: str) -> dict[str, Any]:
    row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    if not row:
        return {
            "ok": False,
            "code": "TREND_RUN_NOT_FOUND",
            "message": "runId에 해당하는 트렌드 수집 런이 없습니다.",
        }
    detail = await build_run_detail(map_run(row))
    return {"ok": True, "run": detail.model_dump()}


async def cancel_run(run_id: str) -> dict[str, Any]:
    row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    if not row:
        return {
            "ok": False,
            "code": "TREND_RUN_NOT_FOUND",
            "message": "runId에 해당하는 트렌드 수집 런이 없습니다.",
        }
    if row["status"] not in ("queued", "running"):
        detail = await build_run_detail(map_run(row))
        return {"ok": True, "run": detail.model_dump()}

    now = now_iso()
    partial_ids = [task["id"] for task in all_rows(
        "SELECT id FROM trend_tasks WHERE run_id = ? AND status IN ('pending','running')", [run_id]
    )]
    if partial_ids:
        run_many(
            "DELETE FROM trend_snapshots WHERE run_id = ? AND task_id = ?",
            [[run_id, task_id] for task_id in partial_ids],
        )
    run(
        """UPDATE trend_tasks
              SET status = 'cancelled', completed_pages = 0, completed_at = NULL,
                  failure_reason = COALESCE(failure_reason, '사용자가 취합을 중지했습니다.'),
                  failure_snippet = COALESCE(failure_snippet, 'cancelled by operator'),
                  updated_at = ?
            WHERE run_id = ? AND status IN ('pending','running')""",
        [now, run_id],
    )
    totals = one(
        """SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
              (SELECT COUNT(*) FROM trend_snapshots WHERE run_id = ?) AS snapshots
           FROM trend_tasks WHERE run_id = ?""",
        [run_id, run_id],
    )
    run(
        """UPDATE trend_runs
              SET status = 'cancelled', total_tasks = ?, completed_tasks = ?, failed_tasks = ?, total_snapshots = ?,
                  cancelled_at = ?, completed_at = NULL, failure_reason = NULL, updated_at = ?
            WHERE id = ?""",
        [
            int(totals["total"] or 0),
            int(totals["completed"] or 0),
            int(totals["failed"] or 0),
            int(totals["snapshots"] or 0),
            now,
            now,
            run_id,
        ],
    )
    refreshed = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    detail = await build_run_detail(map_run(refreshed))
    return {"ok": True, "run": detail.model_dump()}


async def delete_run(run_id: str) -> dict[str, Any]:
    row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    if not row:
        return {
            "ok": False,
            "code": "TREND_RUN_NOT_FOUND",
            "message": "runId에 해당하는 트렌드 수집 런이 없습니다.",
        }
    partial_ids = [task["id"] for task in all_rows(
        "SELECT id FROM trend_tasks WHERE run_id = ? AND status != 'completed'", [run_id]
    )]
    if partial_ids:
        run_many(
            "DELETE FROM trend_snapshots WHERE run_id = ? AND task_id = ?",
            [[run_id, task_id] for task_id in partial_ids],
        )
    run("DELETE FROM trend_tasks WHERE run_id = ?", [run_id])
    run("DELETE FROM trend_runs WHERE id = ?", [run_id])
    run(
        "UPDATE trend_profiles SET latest_run_id = NULL, updated_at = ? WHERE latest_run_id = ?",
        [now_iso(), run_id],
    )
    return {"ok": True, "deletedRunId": run_id}


async def retry_failed(run_id: str) -> dict[str, Any]:
    row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    if not row:
        return {
            "ok": False,
            "code": "TREND_RUN_NOT_FOUND",
            "message": "runId에 해당하는 트렌드 수집 런이 없습니다.",
        }
    now = now_iso()
    run(
        """UPDATE trend_tasks
              SET status = 'pending', retry_count = retry_count + 1, completed_pages = 0,
                  started_at = NULL, completed_at = NULL, failure_reason = NULL, failure_snippet = NULL,
                  updated_at = ?
            WHERE run_id = ? AND status = 'failed'""",
        [now, run_id],
    )
    run(
        "UPDATE trend_runs SET status = 'queued', failed_tasks = 0, failure_reason = NULL, completed_at = NULL, updated_at = ? WHERE id = ?",
        [now, run_id],
    )
    refreshed = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    detail = await build_run_detail(map_run(refreshed))
    return {"ok": True, "run": detail.model_dump()}


# ---------- snapshot pagination ----------


async def get_run_snapshots_page(run_id: str, requested_period: str, requested_page: int) -> dict[str, Any]:
    run_row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    if not run_row:
        return {"ok": False, "code": "TREND_RUN_NOT_FOUND", "message": "runId에 해당하는 트렌드 수집 런이 없습니다."}
    profile_row = one("SELECT * FROM trend_profiles WHERE id = ?", [run_row["profile_id"]])
    if not profile_row:
        return {"ok": False, "code": "TREND_PROFILE_NOT_FOUND", "message": "runId에 연결된 분석 조건을 찾지 못했습니다."}
    profile = map_profile(profile_row)
    brand_clause = "AND brand_excluded = 0" if profile.excludeBrandProducts else ""

    latest_stored = scalar(
        f"SELECT period FROM trend_snapshots WHERE profile_id = ? AND rank <= ? {brand_clause} ORDER BY period DESC LIMIT 1",
        [profile.id, profile.resultCount],
    ) or ""
    period = requested_period or latest_stored
    if not period:
        return {
            "ok": False,
            "code": "TREND_SNAPSHOTS_NOT_READY",
            "message": "아직 조회 가능한 월별 인기검색어 스냅샷이 없습니다.",
        }
    total_items = scalar(
        f"SELECT COUNT(*) FROM trend_snapshots WHERE profile_id = ? AND period = ? AND rank <= ? {brand_clause}",
        [profile.id, period, profile.resultCount],
    ) or 0
    if not total_items:
        return {
            "ok": False,
            "code": "TREND_PERIOD_NOT_FOUND",
            "message": "선택한 월의 인기검색어 스냅샷을 찾지 못했습니다.",
        }
    total_pages = max(1, -(-int(total_items) // TREND_PAGE_SIZE))
    page = min(max(1, requested_page), total_pages)
    offset = (page - 1) * TREND_PAGE_SIZE
    rows = all_rows(
        f"SELECT * FROM trend_snapshots WHERE profile_id = ? AND period = ? AND rank <= ? {brand_clause} ORDER BY rank ASC LIMIT ? OFFSET ?",
        [profile.id, period, profile.resultCount, TREND_PAGE_SIZE, offset],
    )
    items = [map_snapshot(row).model_dump() for row in rows]
    return {
        "ok": True,
        "period": period,
        "page": page,
        "totalPages": total_pages,
        "totalItems": int(total_items),
        "items": items,
    }


# ---------- worker ----------


async def process_next_queued_run() -> dict[str, Any]:
    async with _process_lock:
        run_row = one(
            """SELECT * FROM trend_runs
                WHERE status IN ('queued','running')
                  AND id IN (SELECT run_id FROM trend_tasks WHERE status = 'pending')
                ORDER BY updated_at DESC LIMIT 1"""
        )
        if not run_row:
            return {"ok": True, "processed": False}

        task_row = one(
            "SELECT * FROM trend_tasks WHERE run_id = ? AND status = 'pending' ORDER BY period ASC LIMIT 1",
            [run_row["id"]],
        )
        if not task_row:
            return {"ok": True, "processed": False}

        profile_row = one("SELECT * FROM trend_profiles WHERE id = ?", [run_row["profile_id"]])
        if not profile_row:
            now = now_iso()
            run(
                "UPDATE trend_tasks SET status = 'failed', failure_reason = ?, failure_snippet = ?, updated_at = ? WHERE id = ?",
                ["Trend profile is missing.", "Missing profile", now, task_row["id"]],
            )
            await refresh_run_state(run_row["id"])
            return {
                "ok": False,
                "processed": True,
                "code": "TREND_PROFILE_NOT_FOUND",
                "message": "Trend profile is missing.",
                "runId": run_row["id"],
                "taskId": task_row["id"],
                "period": task_row["period"],
            }

        now = now_iso()
        run(
            "UPDATE trend_runs SET status = 'running', started_at = COALESCE(started_at, ?), updated_at = ? WHERE id = ?",
            [now, now, run_row["id"]],
        )
        run(
            "UPDATE trend_tasks SET status = 'running', started_at = ?, updated_at = ? WHERE id = ?",
            [now, now, task_row["id"]],
        )

        profile = map_profile(profile_row)

    try:
        cached = _read_cached_monthly_ranks(profile, task_row["period"])
        if cached is not None:
            ranks = cached
        else:
            async def on_page(page: int) -> None:
                run(
                    "UPDATE trend_tasks SET completed_pages = ?, updated_at = ? WHERE id = ?",
                    [page, now_iso(), task_row["id"]],
                )

            raw_ranks = await collect_monthly_ranks(
                category_cid=profile.categoryCid,
                period=task_row["period"],
                devices=list(profile.devices),
                genders=list(profile.genders),
                ages=list(profile.ages),
                result_count=profile.resultCount,
                on_page_collected=on_page,
            )
            ranks = [{"rank": r.rank, "keyword": r.keyword, "linkId": r.linkId} for r in raw_ranks]
    except Exception as error:  # noqa: BLE001
        message = str(error) or "Naver collection failed."
        snippet = summarize_failure_snippet(message)
        failed_at = now_iso()
        run(
            "UPDATE trend_tasks SET status = 'failed', failure_reason = ?, failure_snippet = ?, updated_at = ? WHERE id = ?",
            [message, snippet, failed_at, task_row["id"]],
        )
        await refresh_run_state(run_row["id"])
        run("UPDATE trend_profiles SET updated_at = ? WHERE id = ?", [failed_at, profile.id])
        return {
            "ok": False,
            "processed": True,
            "code": "TREND_COLLECTION_FAILED",
            "message": message,
            "runId": run_row["id"],
            "taskId": task_row["id"],
            "period": task_row["period"],
        }

    latest_run = one("SELECT * FROM trend_runs WHERE id = ?", [run_row["id"]])
    latest_task = one("SELECT * FROM trend_tasks WHERE id = ?", [task_row["id"]])
    if not latest_run or not latest_task:
        return {"ok": True, "processed": False}
    if latest_run["status"] == "cancelled" or latest_task["status"] == "cancelled":
        run("DELETE FROM trend_snapshots WHERE run_id = ? AND task_id = ?", [run_row["id"], task_row["id"]])
        return {
            "ok": True,
            "processed": False,
            "runId": run_row["id"],
            "taskId": task_row["id"],
            "period": task_row["period"],
        }

    collected_at = now_iso()
    run("DELETE FROM trend_snapshots WHERE profile_id = ? AND period = ?", [profile.id, task_row["period"]])

    snapshot_rows: list[list[Any]] = []
    brand_terms = list(profile.customExcludedTerms) if profile.excludeBrandProducts else []
    for rank in ranks:
        snapshot_rows.append([
            _new_uuid(),
            profile.id,
            run_row["id"],
            task_row["id"],
            task_row["period"],
            rank["rank"],
            rank["keyword"],
            rank["linkId"],
            profile.categoryCid,
            profile.categoryPath,
            json_dump(list(profile.devices)),
            json_dump(list(profile.genders)),
            json_dump(list(profile.ages)),
            collected_at,
            1 if apply_brand_exclusion(rank["keyword"], brand_terms) else 0,
        ])
    run_many(
        """INSERT INTO trend_snapshots (
             id, profile_id, run_id, task_id, period, rank, keyword, link_id, category_cid, category_path,
             devices_json, genders_json, ages_json, collected_at, brand_excluded
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        snapshot_rows,
    )

    run(
        "UPDATE trend_tasks SET status = 'completed', completed_pages = ?, completed_at = ?, updated_at = ? WHERE id = ?",
        [get_trend_total_pages(profile.resultCount), collected_at, collected_at, task_row["id"]],
    )
    run(
        "UPDATE trend_profiles SET last_collected_period = ?, updated_at = ? WHERE id = ?",
        [task_row["period"], collected_at, profile.id],
    )
    await refresh_run_state(run_row["id"])
    return {
        "ok": True,
        "processed": True,
        "runId": run_row["id"],
        "taskId": task_row["id"],
        "period": task_row["period"],
    }


def _read_cached_monthly_ranks(profile: TrendProfile, period: str) -> list[dict] | None:
    row = one(
        """SELECT tp.id AS profile_id
             FROM trend_profiles tp
             JOIN trend_snapshots ts ON ts.profile_id = tp.id
            WHERE ts.period = ?
              AND tp.category_cid = ?
              AND tp.devices_json = ?
              AND tp.genders_json = ?
              AND tp.ages_json = ?
              AND tp.result_count = ?
              AND tp.exclude_brand_products = ?
              AND tp.custom_excluded_terms_json = ?
              AND ts.rank <= ?
              AND tp.id != ?
            GROUP BY tp.id
           HAVING COUNT(*) >= ?
            ORDER BY MAX(ts.collected_at) DESC
            LIMIT 1""",
        [
            period,
            profile.categoryCid,
            json_dump(list(profile.devices)),
            json_dump(list(profile.genders)),
            json_dump(list(profile.ages)),
            profile.resultCount,
            1 if profile.excludeBrandProducts else 0,
            json_dump(list(profile.customExcludedTerms)),
            profile.resultCount,
            profile.id,
            profile.resultCount,
        ],
    )
    if not row:
        return None
    rows = all_rows(
        "SELECT * FROM trend_snapshots WHERE profile_id = ? AND period = ? AND rank <= ? ORDER BY rank ASC",
        [row["profile_id"], period, profile.resultCount],
    )
    if len(rows) != profile.resultCount:
        return None
    return [{"rank": int(r["rank"]), "keyword": r["keyword"], "linkId": r["link_id"]} for r in rows]


async def refresh_run_state(run_id: str) -> None:
    row = one("SELECT * FROM trend_runs WHERE id = ?", [run_id])
    if not row:
        return
    if row["status"] == "cancelled":
        return
    totals = one(
        """SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
              (SELECT COUNT(*) FROM trend_snapshots WHERE run_id = ?) AS snapshots
           FROM trend_tasks WHERE run_id = ?""",
        [run_id, run_id],
    )
    total = int(totals["total"] or 0)
    completed = int(totals["completed"] or 0)
    failed = int(totals["failed"] or 0)
    snapshots = int(totals["snapshots"] or 0)
    now = now_iso()
    status = "running"
    completed_at = None
    failure_reason = None
    if total == 0 or completed == total:
        status = "completed"
        completed_at = now
    elif completed + failed == total and failed > 0:
        status = "failed"
        completed_at = now
        failure_reason = f"{failed}개 월 수집이 실패했습니다."
    run(
        """UPDATE trend_runs
              SET status = ?, total_tasks = ?, completed_tasks = ?, failed_tasks = ?, total_snapshots = ?,
                  completed_at = ?, cancelled_at = NULL, failure_reason = ?, updated_at = ?
            WHERE id = ?""",
        [status, total, completed, failed, snapshots, completed_at, failure_reason, now, run_id],
    )
