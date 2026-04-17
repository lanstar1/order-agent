from __future__ import annotations

import sqlite3
from typing import Any

from .constants import TREND_DEFAULT_RESULT_COUNT, normalize_trend_result_count
from .db import json_parse
from .models import (
    TrendCollectionRun,
    TrendCollectionTask,
    TrendKeywordSnapshot,
    TrendProfile,
)


def _dict(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return dict(row) if isinstance(row, sqlite3.Row) else row


def map_profile(row: sqlite3.Row | dict[str, Any]) -> TrendProfile:
    data = _dict(row)
    return TrendProfile(
        id=data["id"],
        slug=data["slug"],
        name=data["name"],
        status=data["status"],
        startPeriod=data["start_period"],
        endPeriod=data["end_period"],
        lastCollectedPeriod=data.get("last_collected_period"),
        lastSyncedAt=data.get("last_synced_at"),
        syncStatus=data["sync_status"],
        latestRunId=data.get("latest_run_id"),
        resultCount=normalize_trend_result_count(int(data.get("result_count") or TREND_DEFAULT_RESULT_COUNT)),
        excludeBrandProducts=bool(int(data.get("exclude_brand_products") or 0)),
        customExcludedTerms=json_parse(data.get("custom_excluded_terms_json"), []),
        createdAt=data["created_at"],
        updatedAt=data["updated_at"],
        categoryCid=int(data["category_cid"]),
        categoryPath=data["category_path"],
        categoryDepth=int(data["category_depth"]),
        timeUnit=data["time_unit"],
        devices=json_parse(data["devices_json"], []),
        genders=json_parse(data["genders_json"], []),
        ages=json_parse(data["ages_json"], []),
        spreadsheetId=data["spreadsheet_id"],
    )


def map_run(row: sqlite3.Row | dict[str, Any]) -> TrendCollectionRun:
    data = _dict(row)
    return TrendCollectionRun(
        id=data["id"],
        profileId=data["profile_id"],
        status=data["status"],
        requestedBy=data["requested_by"],
        runType=data["run_type"],
        startPeriod=data["start_period"],
        endPeriod=data["end_period"],
        totalTasks=int(data["total_tasks"]),
        completedTasks=int(data["completed_tasks"]),
        failedTasks=int(data["failed_tasks"]),
        totalSnapshots=int(data["total_snapshots"]),
        sheetUrl=data.get("sheet_url"),
        startedAt=data.get("started_at"),
        completedAt=data.get("completed_at"),
        cancelledAt=data.get("cancelled_at"),
        failureReason=data.get("failure_reason"),
        createdAt=data["created_at"],
        updatedAt=data["updated_at"],
    )


def map_task(row: sqlite3.Row | dict[str, Any]) -> TrendCollectionTask:
    data = _dict(row)
    return TrendCollectionTask(
        id=data["id"],
        runId=data["run_id"],
        profileId=data["profile_id"],
        period=data["period"],
        status=data["status"],
        completedPages=int(data["completed_pages"]),
        totalPages=int(data["total_pages"]),
        retryCount=int(data["retry_count"]),
        startedAt=data.get("started_at"),
        completedAt=data.get("completed_at"),
        failureReason=data.get("failure_reason"),
        failureSnippet=data.get("failure_snippet"),
        updatedAt=data["updated_at"],
    )


def map_snapshot(row: sqlite3.Row | dict[str, Any]) -> TrendKeywordSnapshot:
    data = _dict(row)
    return TrendKeywordSnapshot(
        id=data["id"],
        profileId=data["profile_id"],
        runId=data["run_id"],
        taskId=data["task_id"],
        period=data["period"],
        rank=int(data["rank"]),
        keyword=data["keyword"],
        linkId=data["link_id"],
        categoryCid=int(data["category_cid"]),
        categoryPath=data["category_path"],
        devices=json_parse(data["devices_json"], []),
        genders=json_parse(data["genders_json"], []),
        ages=json_parse(data["ages_json"], []),
        brandExcluded=bool(int(data.get("brand_excluded") or 0)),
        collectedAt=data["collected_at"],
    )
