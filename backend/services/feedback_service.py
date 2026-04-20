"""Phase 5 — feedback loop.

Ties sourcing outcomes back to actual purchasing and sales data.
Works off `sourced_products` columns:
  purchased_at, listed_at, erp_item_code, sales_count_30d, revenue_krw_30d,
  return_rate_30d

External hooks (optional):
- `fetch_erp_sales_fn(erp_item_code) -> {"sales_count_30d": int, "revenue_krw_30d": int, "return_rate_30d": float}`
  In the order-agent repo this is a thin wrapper around `services/erp_client.py`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional


FetchErpSalesFn = Callable[[str], dict]
"""erp_item_code -> metrics dict."""


@dataclass
class FeedbackUpdate:
    product_id: int
    purchased_at: Optional[str] = None
    listed_at: Optional[str] = None
    erp_item_code: Optional[str] = None
    sales_count_30d: Optional[int] = None
    revenue_krw_30d: Optional[int] = None
    return_rate_30d: Optional[float] = None


def mark_purchased(conn, product_id: int, erp_item_code: str) -> None:
    """Record that a sourced_product was actually purchased for stock."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE sourced_products
           SET sourcing_status='purchased', purchased_at=?, erp_item_code=?
           WHERE id=?""",
        (now, erp_item_code, product_id),
    )
    conn.commit()


def mark_listed(conn, product_id: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE sourced_products SET listed_at=? WHERE id=?", (now, product_id),
    )
    conn.commit()


def refresh_sales(
    conn, product_id: int, *, fetch_fn: FetchErpSalesFn,
) -> FeedbackUpdate:
    """Pull latest 30d metrics from ERP and persist them."""
    row = conn.execute(
        "SELECT erp_item_code FROM sourced_products WHERE id=?", (product_id,),
    ).fetchone()
    if not row or not row[0]:
        return FeedbackUpdate(product_id=product_id)
    metrics = fetch_fn(row[0]) or {}
    sales = int(metrics.get("sales_count_30d") or 0)
    revenue = int(metrics.get("revenue_krw_30d") or 0)
    returns = float(metrics.get("return_rate_30d") or 0.0)
    conn.execute(
        """UPDATE sourced_products
           SET sales_count_30d=?, revenue_krw_30d=?, return_rate_30d=?
           WHERE id=?""",
        (sales, revenue, returns, product_id),
    )
    conn.commit()
    return FeedbackUpdate(
        product_id=product_id,
        erp_item_code=row[0],
        sales_count_30d=sales,
        revenue_krw_30d=revenue,
        return_rate_30d=returns,
    )


# ----------------------------------------------------------------- #
# Aggregations for the "hit-rate dashboard"
# ----------------------------------------------------------------- #


def sourcing_hit_rate(conn, *, threshold_revenue_krw: int = 1_000_000) -> dict:
    """Return the fraction of purchased products that crossed a 30-day revenue
    threshold."""
    row = conn.execute(
        """SELECT
             COUNT(*) AS total_purchased,
             SUM(CASE WHEN revenue_krw_30d >= ? THEN 1 ELSE 0 END) AS hits
           FROM sourced_products WHERE sourcing_status='purchased'""",
        (threshold_revenue_krw,),
    ).fetchone()
    total = int(row[0] or 0)
    hits = int(row[1] or 0)
    rate = (hits / total) if total else 0.0
    return {
        "total_purchased": total, "hits": hits,
        "hit_rate": round(rate, 3),
        "threshold_revenue_krw": threshold_revenue_krw,
    }


def score_vs_outcome(conn) -> list[dict]:
    """Per-score breakdown to see if market_size_score predicts sales."""
    rows = conn.execute(
        """SELECT mr.market_size_score, COUNT(*) AS n,
                  AVG(sp.revenue_krw_30d) AS avg_rev,
                  AVG(sp.return_rate_30d) AS avg_return
           FROM sourced_products sp
           JOIN market_research mr ON mr.product_id=sp.id AND mr.is_latest=TRUE
           WHERE sp.sourcing_status='purchased'
           GROUP BY mr.market_size_score
           ORDER BY mr.market_size_score DESC""",
    ).fetchall()
    return [
        {
            "market_size_score": r[0], "samples": r[1],
            "avg_revenue_krw_30d": int(r[2] or 0),
            "avg_return_rate_30d": round(float(r[3] or 0), 3),
        } for r in rows
    ]
