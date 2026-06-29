"""Naver Search Open API — /v1/search/shop.json

Docs: https://developers.naver.com/docs/serviceapi/search/shop/shop.md
Auth: headers X-Naver-Client-Id / X-Naver-Client-Secret
Limits: 25,000 calls/day, max 100 items/request.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional


SHOP_ENDPOINT = "https://openapi.naver.com/v1/search/shop.json"


@dataclass
class ShopStats:
    keyword: str
    total: int
    prices: list[int] = field(default_factory=list)
    products: list[dict] = field(default_factory=list)

    @property
    def price_summary(self) -> dict:
        if not self.prices:
            return {}
        xs = sorted(self.prices)
        n = len(xs)
        def q(p):
            return xs[min(n - 1, int(n * p))]
        return {
            "min": xs[0],
            "p25": q(0.25),
            "median": q(0.5),
            "p75": q(0.75),
            "max": xs[-1],
            "sample_size": n,
        }


class NaverSearchClient:
    """Thin client for /v1/search/shop.json."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        http_fetcher: Optional[callable] = None,
        sleep_between_calls: float = 0.1,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self._fetch = http_fetcher or self._default_fetcher
        self._sleep = sleep_between_calls

    def _default_fetcher(self, url: str, headers: dict) -> dict:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def shop(self, query: str, *, display: int = 40) -> ShopStats:
        if display > 100:
            display = 100
        url = (
            f"{SHOP_ENDPOINT}?query={urllib.parse.quote(query)}"
            f"&display={display}&sort=sim"
        )
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "User-Agent": "sourcing-agent/1.0",
        }
        data = self._fetch(url, headers)
        items = data.get("items") or []
        stats = ShopStats(
            keyword=query,
            total=int(data.get("total", len(items))),
        )
        for item in items:
            try:
                price = int(item.get("lprice") or 0)
            except (TypeError, ValueError):
                price = 0
            if price > 0:
                stats.prices.append(price)
            stats.products.append({
                "title": _strip_html(item.get("title", "")),
                "link": item.get("link"),
                "lprice": price,
                "mall_name": item.get("mallName"),
                "category1": item.get("category1"),
                "category2": item.get("category2"),
            })
        time.sleep(self._sleep)
        return stats


def _strip_html(s: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", s or "").strip()
