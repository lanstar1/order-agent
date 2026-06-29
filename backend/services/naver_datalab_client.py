"""Naver DataLab Shopping-Insight API.

Docs: https://developers.naver.com/docs/serviceapi/datalab/shopping/shopping.md
Auth: same Client-Id/Secret as the Search API.
Endpoints used:
- POST /v1/datalab/shopping/categories       (카테고리 트렌드)
- POST /v1/datalab/shopping/category/gender  (성별 비중)
- POST /v1/datalab/shopping/category/age     (연령 비중)
"""
from __future__ import annotations

import json
import time
import urllib.request
from typing import Optional


CATEGORY_TREND_ENDPOINT = "https://openapi.naver.com/v1/datalab/shopping/categories"
CATEGORY_GENDER_ENDPOINT = "https://openapi.naver.com/v1/datalab/shopping/category/gender"
CATEGORY_AGE_ENDPOINT = "https://openapi.naver.com/v1/datalab/shopping/category/age"


class NaverDataLabClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        http_fetcher: Optional[callable] = None,
        sleep_between_calls: float = 0.2,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self._fetch = http_fetcher or self._default_fetcher
        self._sleep = sleep_between_calls

    def _default_fetcher(self, url: str, headers: dict, body: str) -> dict:
        req = urllib.request.Request(
            url, data=body.encode("utf-8"), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _headers(self) -> dict:
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Content-Type": "application/json",
            "User-Agent": "sourcing-agent/1.0",
        }

    def category_trend(
        self,
        *,
        start_date: str,
        end_date: str,
        time_unit: str,  # "date" | "week" | "month"
        category: list[dict],  # [{"name":"차량용품","param":["50000006"]}]
        device: Optional[str] = None,
        gender: Optional[str] = None,
        ages: Optional[list[str]] = None,
    ) -> dict:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "category": category,
        }
        if device:
            body["device"] = device
        if gender:
            body["gender"] = gender
        if ages:
            body["ages"] = ages
        data = self._fetch(CATEGORY_TREND_ENDPOINT, self._headers(), json.dumps(body))
        time.sleep(self._sleep)
        return data

    def category_gender(self, **kw) -> dict:
        return self._post(CATEGORY_GENDER_ENDPOINT, kw)

    def category_age(self, **kw) -> dict:
        return self._post(CATEGORY_AGE_ENDPOINT, kw)

    def _post(self, url: str, body: dict) -> dict:
        data = self._fetch(url, self._headers(), json.dumps(body))
        time.sleep(self._sleep)
        return data
