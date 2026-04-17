from __future__ import annotations

import asyncio
import json as jsonlib
import random
from calendar import monthrange
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

import httpx

from .constants import (
    TREND_PAGE_SIZE,
    get_trend_total_pages,
    serialize_trend_filter,
)

NAVER_BASE_URL = "https://datalab.naver.com"
NAVER_CATEGORY_PAGE_URL = f"{NAVER_BASE_URL}/shoppingInsight/sCategory.naver"
NAVER_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

FALLBACK_PATH = Path(__file__).resolve().parent / "data" / "category_fallback.json"
_fallback_cache: dict | None = None


@dataclass
class KeywordRank:
    rank: int
    keyword: str
    linkId: str


def _load_fallback() -> dict:
    global _fallback_cache
    if _fallback_cache is None:
        _fallback_cache = jsonlib.loads(FALLBACK_PATH.read_text(encoding="utf-8"))
    return _fallback_cache


def get_static_roots() -> list[dict]:
    return _load_fallback()["roots"]


def get_static_children(cid: int) -> list[dict]:
    return _load_fallback()["children"].get(str(cid), [])


def summarize_failure_snippet(value: str) -> str:
    return " ".join((value or "").split())[:220]


def month_period_to_date_range(period: str) -> tuple[str, str]:
    year, month = (int(part) for part in period.split("-"))
    last_day = monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


async def _bootstrap_client() -> httpx.AsyncClient:
    client = httpx.AsyncClient(
        base_url=NAVER_BASE_URL,
        timeout=httpx.Timeout(30.0, connect=15.0),
        follow_redirects=True,
        headers={
            "User-Agent": NAVER_BROWSER_USER_AGENT,
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    try:
        resp = await client.get(
            NAVER_CATEGORY_PAGE_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        resp.raise_for_status()
    except Exception:
        await client.aclose()
        raise
    return client


async def _request_json(client: httpx.AsyncClient, path: str, *, method: str = "GET", data: dict | None = None):
    headers = {
        "User-Agent": NAVER_BROWSER_USER_AGENT,
        "Referer": NAVER_CATEGORY_PAGE_URL,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }
    if method.upper() == "POST":
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        response = await client.post(path, data=data or {}, headers=headers)
    else:
        response = await client.get(path, headers=headers)

    text = response.text
    if response.status_code >= 400:
        snippet = summarize_failure_snippet(text)
        raise RuntimeError(f"Naver request failed with status {response.status_code}. {snippet}")

    stripped = text.strip()
    if stripped.startswith("<!DOCTYPE html") or stripped.startswith("<html"):
        raise RuntimeError(f"Naver returned an HTML error page. {summarize_failure_snippet(text)}")

    return jsonlib.loads(text)


async def fetch_category_children(cid: int) -> list[dict]:
    try:
        client = await _bootstrap_client()
    except Exception:
        return get_static_children(cid)
    try:
        payload = await _request_json(client, f"/shoppingInsight/getCategory.naver?cid={cid}")
        nodes = payload.get("childList") or []
        return [
            {
                "cid": node.get("cid"),
                "name": node.get("name"),
                "fullPath": node.get("fullPath"),
                "level": node.get("level"),
                "leaf": node.get("leaf"),
            }
            for node in nodes
        ]
    except Exception:
        return get_static_children(cid)
    finally:
        await client.aclose()


PageCallback = Callable[[int], Awaitable[None]]


async def collect_monthly_ranks(
    *,
    category_cid: int,
    period: str,
    devices: list[str],
    genders: list[str],
    ages: list[str],
    result_count: int,
    on_page_collected: PageCallback | None = None,
) -> list[KeywordRank]:
    client = await _bootstrap_client()
    start_date, end_date = month_period_to_date_range(period)
    total_pages = get_trend_total_pages(result_count)
    pages: list[list[dict]] = []

    try:
        for page in range(1, total_pages + 1):
            body = {
                "cid": str(category_cid),
                "timeUnit": "month",
                "startDate": start_date,
                "endDate": end_date,
                "page": str(page),
                "count": str(TREND_PAGE_SIZE),
                "device": serialize_trend_filter(devices),
                "gender": serialize_trend_filter(genders),
                "age": serialize_trend_filter(ages),
            }
            payload = await _request_json(
                client,
                "/shoppingInsight/getCategoryKeywordRank.naver",
                method="POST",
                data=body,
            )
            ranks = payload.get("ranks") or []
            if not ranks:
                raise RuntimeError(f"No ranks were returned for {period} page {page}.")
            pages.append(ranks)
            if on_page_collected is not None:
                await on_page_collected(page)
            if page < total_pages:
                await asyncio.sleep(0.14 + random.random() * 0.12)
    finally:
        await client.aclose()

    merged = sorted(
        [item for page in pages for item in page],
        key=lambda item: int(item.get("rank", 0)),
    )
    if len(merged) != result_count:
        raise RuntimeError(f"Expected {result_count} keywords but received {len(merged)}.")
    ranks_set = {int(item.get("rank")) for item in merged}
    if len(ranks_set) != result_count:
        raise RuntimeError("Duplicate ranks detected while merging keyword pages.")
    if int(merged[0].get("rank")) != 1 or int(merged[-1].get("rank")) != result_count:
        raise RuntimeError("Rank range is incomplete.")

    return [
        KeywordRank(rank=int(item["rank"]), keyword=item["keyword"], linkId=str(item.get("linkId", "")))
        for item in merged
    ]
