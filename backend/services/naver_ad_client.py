"""Naver Search-Ads API — /keywordstool endpoint.

Docs: https://naver.github.io/searchad-apidoc/
Auth: HMAC-SHA256 signature of "timestamp.METHOD.path"
Headers: X-Timestamp, X-API-KEY, X-Customer, X-Signature.

Quota: relatively small (shared across advertiser). Caller must cache.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional


BASE_URL = "https://api.searchad.naver.com"
KEYWORDSTOOL_PATH = "/keywordstool"


@dataclass
class KeywordStat:
    keyword: str
    monthly_pc: int = 0
    monthly_mobile: int = 0
    competition: str = "unknown"
    avg_click: float = 0.0
    avg_position: float = 0.0

    @property
    def monthly_total(self) -> int:
        return self.monthly_pc + self.monthly_mobile


def _sign(method: str, path: str, timestamp: str, secret: str) -> str:
    msg = f"{timestamp}.{method}.{path}"
    sig = hmac.new(
        secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256
    ).digest()
    return base64.b64encode(sig).decode("utf-8")


def build_headers(
    method: str, path: str, *, api_key: str, customer_id: str, secret: str,
    now_ms: Optional[int] = None,
) -> dict:
    """Return the signed headers for a Naver Search-Ads request."""
    ts = str(now_ms if now_ms is not None else int(time.time() * 1000))
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": api_key,
        "X-Customer": str(customer_id),
        "X-Signature": _sign(method, path, ts, secret),
    }


class NaverAdClient:
    def __init__(
        self,
        api_key: str,
        secret: str,
        customer_id: str,
        *,
        http_fetcher: Optional[callable] = None,
        sleep_between_calls: float = 0.3,
    ):
        self.api_key = api_key
        self.secret = secret
        self.customer_id = customer_id
        self._fetch = http_fetcher or self._default_fetcher
        self._sleep = sleep_between_calls

    def _default_fetcher(self, url: str, headers: dict) -> dict:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def keywordstool(self, keywords: list[str]) -> list[KeywordStat]:
        """Look up monthly search volume and competition for up to 5 keywords
        per call (API limit)."""
        stats: list[KeywordStat] = []
        for batch in _chunks(keywords, 5):
            hint = ",".join(batch)
            qs = urllib.parse.urlencode({"hintKeywords": hint, "showDetail": 1})
            path = f"{KEYWORDSTOOL_PATH}?{qs}"
            url = f"{BASE_URL}{path}"
            # Signature uses the bare path (no query string) per Naver docs.
            headers = build_headers(
                "GET", KEYWORDSTOOL_PATH,
                api_key=self.api_key, customer_id=self.customer_id,
                secret=self.secret,
            )
            data = self._fetch(url, headers)
            for row in data.get("keywordList") or []:
                stats.append(_parse_keyword_row(row))
            time.sleep(self._sleep)
        return stats


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _parse_int(v) -> int:
    if v in (None, "", "< 10"):
        return 10 if v == "< 10" else 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _parse_keyword_row(row: dict) -> KeywordStat:
    return KeywordStat(
        keyword=row.get("relKeyword", ""),
        monthly_pc=_parse_int(row.get("monthlyPcQcCnt")),
        monthly_mobile=_parse_int(row.get("monthlyMobileQcCnt")),
        competition=(row.get("compIdx") or "unknown").lower(),
        avg_click=float(row.get("monthlyAvePcClkCnt") or 0),
        avg_position=float(row.get("plAvgDepth") or 0),
    )
