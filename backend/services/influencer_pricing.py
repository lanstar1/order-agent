"""Influencer fee estimator.

estimated_quote_krw =
  avg_views * cpm_rate / 1000
  * content_format_multiplier
  * engagement_bonus
  * category_premium

All variables are configurable via `PricingConfig`. Keep defaults in one place
so they match the UI settings page.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PricingConfig:
    # CPM per platform / format (KRW per 1000 views)
    cpm_rate: dict = field(default_factory=lambda: {
        "youtube_integrated": 15_000,
        "youtube_ppl":         8_000,
        "instagram_reels":    10_000,
        "instagram_feed":      8_000,
        "blog":                6_000,
    })
    # Content-format multiplier
    format_multiplier: dict = field(default_factory=lambda: {
        "integrated_review": 1.5,
        "ppl":               1.0,
        "live":              2.0,
    })
    # Engagement bonus (ER in percent)
    er_tiers: list = field(default_factory=lambda: [
        (10.0, 1.6),
        (5.0,  1.3),
        (0.0,  1.0),
    ])
    # Category premium
    category_premium: dict = field(default_factory=lambda: {
        "tech":   1.2,
        "beauty": 1.2,
        "camping": 1.1,
        "auto":   1.1,
        "daily":  1.0,
        "kids":   1.1,
    })
    # Uncertainty band (±)
    uncertainty_band: float = 0.5    # ±50 %


@dataclass
class QuoteBreakdown:
    cpm_rate: int
    format_multiplier: float
    engagement_bonus: float
    category_premium: float
    base_views: int
    raw_quote_krw: int
    low_krw: int
    high_krw: int
    notes: list[str] = field(default_factory=list)

    @property
    def mid_krw(self) -> int:
        return self.raw_quote_krw


def _er_bonus(er_pct: float, tiers: list) -> float:
    for threshold, mult in sorted(tiers, reverse=True):
        if er_pct >= threshold:
            return mult
    return 1.0


def estimate_quote(
    *,
    platform: str,
    avg_views: int,
    engagement_rate_pct: float,
    content_format: str = "integrated_review",
    category: str = "daily",
    config: Optional[PricingConfig] = None,
) -> QuoteBreakdown:
    cfg = config or PricingConfig()
    # Map platform + format to a CPM key
    if platform == "youtube":
        cpm_key = "youtube_integrated" if content_format == "integrated_review" else "youtube_ppl"
    elif platform == "instagram":
        cpm_key = "instagram_reels" if content_format != "feed" else "instagram_feed"
    elif platform == "blog":
        cpm_key = "blog"
    else:
        cpm_key = "youtube_ppl"
    cpm = cfg.cpm_rate.get(cpm_key, 8_000)
    fmt = cfg.format_multiplier.get(content_format, 1.0)
    bonus = _er_bonus(engagement_rate_pct, cfg.er_tiers)
    premium = cfg.category_premium.get(category, 1.0)

    base = max(0, avg_views) * cpm / 1000.0
    raw = int(base * fmt * bonus * premium)
    low = int(raw * (1 - cfg.uncertainty_band))
    high = int(raw * (1 + cfg.uncertainty_band))
    notes = [
        f"CPM {cpm:,}원/1000뷰 ({cpm_key})",
        f"포맷 ×{fmt}",
        f"ER ×{bonus}",
        f"카테고리 ×{premium}",
        "±50% 범위는 실제 협상 시세와 다를 수 있습니다.",
    ]
    return QuoteBreakdown(
        cpm_rate=int(cpm),
        format_multiplier=fmt,
        engagement_bonus=bonus,
        category_premium=premium,
        base_views=int(avg_views),
        raw_quote_krw=raw,
        low_krw=low,
        high_krw=high,
        notes=notes,
    )
