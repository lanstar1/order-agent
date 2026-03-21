"""
ERP 데이터 커넥터 — Super Agent용 ECOUNT ERP 데이터 조회
기존 erp_client.py를 활용하여 매출/재고/거래처 데이터를 AI 분석용으로 정제
"""
import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


async def fetch_erp_sales_data(
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    customer_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    ERP에서 매출 데이터 조회
    Returns: {success, data: [{date, customer, product, qty, amount, ...}], summary}
    """
    try:
        from db.database import get_connection
        conn = get_connection()

        # 기간 기본값: 최근 3개월
        if not period_start:
            period_start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        if not period_end:
            period_end = datetime.now().strftime("%Y-%m-%d")

        # sales_records 테이블 조회 (sales_analytics에서 수집된 데이터)
        query = """
            SELECT sale_date, cust_code, cust_name, prod_code, prod_name,
                   qty, unit_price, amount, wh_name
            FROM sales_records
            WHERE sale_date BETWEEN ? AND ?
        """
        params = [period_start, period_end]

        if customer_code:
            query += " AND cust_code = ?"
            params.append(customer_code)

        query += " ORDER BY sale_date DESC"

        rows = conn.execute(query, params).fetchall()
        conn.close()

        data = [dict(row) for row in rows]

        # 요약 통계
        total_amount = sum(float(r.get("amount", 0) or 0) for r in data)
        unique_customers = len(set(r.get("cust_code", "") for r in data))
        unique_products = len(set(r.get("prod_code", "") for r in data))

        return {
            "success": True,
            "data": data[:1000],  # 최대 1000건
            "total_rows": len(data),
            "summary": {
                "period": f"{period_start} ~ {period_end}",
                "total_amount": total_amount,
                "unique_customers": unique_customers,
                "unique_products": unique_products,
                "record_count": len(data),
            },
        }
    except Exception as e:
        logger.error(f"[ERP] 매출 데이터 조회 실패: {e}")
        return {"success": False, "error": str(e), "data": []}


async def fetch_erp_inventory_data() -> Dict[str, Any]:
    """
    ERP에서 재고 데이터 조회
    Returns: {success, data: [{product, qty, warehouse, ...}], summary}
    """
    try:
        # 기존 ERP 클라이언트를 통한 조회 시도
        from services.erp_client import ERPClient
        erp = ERPClient()
        result = await erp.get_inventory_list()

        if result.get("success"):
            items = result.get("data", [])
            return {
                "success": True,
                "data": items[:500],
                "total_items": len(items),
                "summary": {
                    "total_items": len(items),
                    "total_qty": sum(int(i.get("qty", 0) or 0) for i in items),
                },
            }
        return {"success": False, "error": "ERP 조회 실패", "data": []}
    except Exception as e:
        logger.warning(f"[ERP] 재고 데이터 조회 실패: {e}")
        return {"success": False, "error": str(e), "data": []}


async def fetch_erp_customers() -> Dict[str, Any]:
    """
    DB에서 거래처 목록 조회
    """
    try:
        from db.database import get_connection
        conn = get_connection()
        rows = conn.execute(
            "SELECT cust_code, cust_name, alias FROM customers ORDER BY cust_name"
        ).fetchall()
        conn.close()

        data = [dict(row) for row in rows]
        return {
            "success": True,
            "data": data,
            "total": len(data),
        }
    except Exception as e:
        logger.error(f"[ERP] 거래처 조회 실패: {e}")
        return {"success": False, "error": str(e), "data": []}


async def fetch_shipping_data(
    days: int = 30,
    warehouse: Optional[str] = None,
) -> Dict[str, Any]:
    """
    택배 발송 데이터 조회 (SmartLogen 동기화 데이터)
    """
    try:
        from db.database import get_connection
        conn = get_connection()

        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        query = "SELECT * FROM shipments WHERE take_dt >= ?"
        params = [cutoff]

        if warehouse:
            query += " AND warehouse = ?"
            params.append(warehouse)

        query += " ORDER BY take_dt DESC"
        rows = conn.execute(query, params).fetchall()
        conn.close()

        data = [dict(row) for row in rows]

        # 상태별 통계
        status_counts = {}
        for r in data:
            s = r.get("status", "알 수 없음")
            status_counts[s] = status_counts.get(s, 0) + 1

        return {
            "success": True,
            "data": data[:500],
            "total": len(data),
            "summary": {
                "total_shipments": len(data),
                "status_counts": status_counts,
                "period_days": days,
            },
        }
    except Exception as e:
        logger.error(f"[ERP] 택배 데이터 조회 실패: {e}")
        return {"success": False, "error": str(e), "data": []}


def format_erp_data_for_llm(data: Dict[str, Any], data_type: str = "sales") -> str:
    """ERP 데이터를 LLM 분석용 텍스트로 변환"""
    if not data.get("success") or not data.get("data"):
        return f"[{data_type}] 데이터를 조회할 수 없습니다: {data.get('error', '알 수 없는 오류')}"

    lines = []
    summary = data.get("summary", {})

    if data_type == "sales":
        lines.append(f"## 매출 데이터 ({summary.get('period', '')})")
        lines.append(f"- 총 매출: {summary.get('total_amount', 0):,.0f}원")
        lines.append(f"- 거래처 수: {summary.get('unique_customers', 0)}개")
        lines.append(f"- 제품 수: {summary.get('unique_products', 0)}개")
        lines.append(f"- 건수: {summary.get('record_count', 0)}건")
        lines.append("")

        # 상위 20건 미리보기
        for i, row in enumerate(data["data"][:20]):
            lines.append(
                f"  {row.get('sale_date','')}\t{row.get('cust_name','')}\t"
                f"{row.get('prod_name','')}\t수량:{row.get('qty','')}\t"
                f"금액:{row.get('amount','')}"
            )

    elif data_type == "inventory":
        lines.append(f"## 재고 현황")
        lines.append(f"- 품목 수: {summary.get('total_items', 0)}")
        lines.append(f"- 총 수량: {summary.get('total_qty', 0):,}")
        lines.append("")
        for row in data["data"][:30]:
            lines.append(
                f"  {row.get('prod_name','')}\t수량:{row.get('qty','')}\t"
                f"창고:{row.get('wh_name','')}"
            )

    elif data_type == "shipping":
        lines.append(f"## 택배 발송 현황 (최근 {summary.get('period_days', 30)}일)")
        lines.append(f"- 총 발송: {summary.get('total_shipments', 0)}건")
        for status, cnt in summary.get("status_counts", {}).items():
            lines.append(f"  - {status}: {cnt}건")

    return "\n".join(lines)


# ─── Tool Registry 연동 ───

async def _erp_query_tool(query_type: str, period_start: str = "", period_end: str = "", customer_code: str = "") -> "ToolResult":
    """ERP 데이터 조회 (Tool 인터페이스)"""
    from super_agent.tools.tool_registry import ToolResult

    try:
        if query_type == "sales":
            data = await fetch_erp_sales_data(period_start or None, period_end or None, customer_code or None)
        elif query_type == "inventory":
            data = await fetch_erp_inventory_data()
        elif query_type == "customers":
            data = await fetch_erp_customers()
        elif query_type == "shipping":
            days = 30
            data = await fetch_shipping_data(days=days)
        else:
            return ToolResult(success=False, error=f"지원하지 않는 조회 유형: {query_type}")

        if data.get("success"):
            text = format_for_llm(data, query_type)
            return ToolResult(success=True, data=text, metadata={"type": query_type, "count": len(data.get("data", []))})
        else:
            return ToolResult(success=False, error=data.get("error", "ERP 조회 실패"))
    except Exception as e:
        return ToolResult(success=False, error=f"ERP 조회 오류: {e}")


def register_erp_tools(registry):
    from super_agent.tools.tool_registry import ToolDefinition
    registry.register(ToolDefinition(
        name="erp_query",
        description="ECOUNT ERP 데이터 조회. 매출, 재고, 거래처, 배송 데이터를 조회",
        parameters={
            "type": "object",
            "properties": {
                "query_type": {"type": "string", "description": "조회 유형: sales, inventory, customers, shipping"},
                "period_start": {"type": "string", "description": "시작일 (YYYY-MM-DD, 매출 조회 시)"},
                "period_end": {"type": "string", "description": "종료일 (YYYY-MM-DD, 매출 조회 시)"},
                "customer_code": {"type": "string", "description": "거래처 코드 (매출 필터링 시)"},
            },
            "required": ["query_type"],
        },
        execute_fn=_erp_query_tool,
        category="data",
    ))
