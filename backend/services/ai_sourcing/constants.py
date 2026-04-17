from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo

TrendTimeUnit = Literal["date", "week", "month"]
TrendDeviceCode = Literal["pc", "mo"]
TrendGenderCode = Literal["f", "m"]
TrendAgeCode = Literal["10", "20", "30", "40", "50", "60"]

TREND_MONTHLY_START_PERIOD = "2021-01"
TREND_TIMEZONE = ZoneInfo("Asia/Seoul")
TREND_DEFAULT_RESULT_COUNT = 20
TREND_RESULT_COUNT_OPTIONS: tuple[int, ...] = (20, 40)
TREND_MAX_RANK = 40
TREND_PAGE_SIZE = 20
TREND_DEVICE_OPTIONS: tuple[str, ...] = ("pc", "mo")
TREND_GENDER_OPTIONS: tuple[str, ...] = ("f", "m")
TREND_AGE_OPTIONS: tuple[str, ...] = ("10", "20", "30", "40", "50", "60")
DEFAULT_OPERATOR_ID = "haniroom-trend-operator"


def get_latest_collectible_trend_period(now: datetime | None = None) -> str:
    now = now or datetime.now(tz=TREND_TIMEZONE)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    localized = now.astimezone(TREND_TIMEZONE)
    year = localized.year
    month = localized.month - 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year}-{month:02d}"


def normalize_trend_result_count(value: int | None) -> int:
    return 40 if value == 40 else 20


def get_trend_total_pages(result_count: int = TREND_DEFAULT_RESULT_COUNT) -> int:
    return max(1, -(-result_count // TREND_PAGE_SIZE))


def list_monthly_periods(start_period: str | None = None, end_period: str | None = None) -> list[str]:
    start_period = start_period or TREND_MONTHLY_START_PERIOD
    end_period = end_period or get_latest_collectible_trend_period()
    start_year, start_month = (int(x) for x in start_period.split("-"))
    end_year, end_month = (int(x) for x in end_period.split("-"))
    periods: list[str] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def normalize_excluded_terms(values: list[str] | None) -> list[str]:
    if not values:
        return []
    cleaned = {value.strip().lower() for value in values if value and value.strip()}
    return sorted(cleaned)


def serialize_trend_filter(values: list[str]) -> str:
    return ",".join(values)


def normalize_trend_spreadsheet_id(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    import re

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", value)
    if match:
        return match.group(1)
    match = re.search(r"[?&]id=([a-zA-Z0-9\-_]+)", value)
    if match:
        return match.group(1)
    return value


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{datetime.now(tz=timezone.utc).microsecond // 1000:03d}Z"
