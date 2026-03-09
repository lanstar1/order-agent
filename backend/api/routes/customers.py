"""
거래처 관리 API
"""
import logging
import asyncio
from fastapi import APIRouter, HTTPException, Depends
from db.database import get_connection
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

router = APIRouter(prefix="/api/customers", tags=["customers"])
logger = logging.getLogger(__name__)


@router.get("/")
async def list_customers(q: str = "", user: dict = Depends(get_current_user)):
    """거래처 목록 (q 파라미터로 검색, 없으면 전체)"""
    conn = get_connection()
    if q:
        pattern = f"%{q}%"
        rows = conn.execute(
            "SELECT cust_code, cust_name, alias FROM customers WHERE cust_name LIKE ? OR cust_code LIKE ? ORDER BY cust_name LIMIT 100",
            (pattern, pattern)
        ).fetchall()
    else:
        rows = conn.execute("SELECT cust_code, cust_name, alias FROM customers ORDER BY cust_name").fetchall()
    conn.close()
    return {"customers": [dict(r) for r in rows]}


@router.post("/")
async def create_customer(cust_code: str, cust_name: str, alias: str = "", user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO customers(cust_code,cust_name,alias) VALUES(?,?,?)",
            (cust_code, cust_name, alias)
        )
        conn.commit()
    except Exception as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()
    return {"success": True}


@router.post("/sync-erp")
async def sync_customers_from_erp(user: dict = Depends(get_current_user)):
    """ECOUNT ERP에서 거래처 목록을 가져와 DB에 동기화"""
    try:
        from services.erp_client import erp_client
        all_customers = []
        page = 1
        max_pages = 20  # 안전 제한

        while page <= max_pages:
            result = await erp_client.get_customer_list(page=page, per_page=500)
            if not result.get("success"):
                if page == 1:
                    raise HTTPException(500, f"ERP 거래처 조회 실패: {result.get('error', '알 수 없는 오류')}")
                break
            customers = result.get("customers", [])
            if not customers:
                break
            all_customers.extend(customers)
            total = result.get("total", 0)
            if len(all_customers) >= total:
                break
            page += 1

        if not all_customers:
            return {"success": True, "message": "ERP에서 가져온 거래처가 없습니다.", "synced": 0, "total": 0}

        # DB에 upsert
        conn = get_connection()
        inserted = 0
        updated = 0
        for c in all_customers:
            existing = conn.execute(
                "SELECT cust_code FROM customers WHERE cust_code=?", (c["cust_code"],)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE customers SET cust_name=? WHERE cust_code=?",
                    (c["cust_name"], c["cust_code"])
                )
                updated += 1
            else:
                conn.execute(
                    "INSERT INTO customers(cust_code, cust_name, alias) VALUES(?,?,?)",
                    (c["cust_code"], c["cust_name"], "")
                )
                inserted += 1
        conn.commit()
        conn.close()

        logger.info(f"[CustomerSync] ERP 동기화 완료: 신규 {inserted}, 업데이트 {updated}, 총 {len(all_customers)}")
        return {
            "success": True,
            "synced": len(all_customers),
            "inserted": inserted,
            "updated": updated,
            "total": len(all_customers),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[CustomerSync] 오류: {e}", exc_info=True)
        raise HTTPException(500, f"거래처 동기화 실패: {str(e)}")


@router.get("/count")
async def customer_count(user: dict = Depends(get_current_user)):
    """거래처 수 조회"""
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) as cnt FROM customers").fetchone()
    conn.close()
    return {"count": row["cnt"] if row else 0}


@router.put("/{cust_code}")
async def update_customer(cust_code: str, cust_name: str = None, alias: str = None, new_code: str = None, user: dict = Depends(get_current_user)):
    """거래처 정보 업데이트 (new_code 전달 시 cust_code 자체도 변경)"""
    conn = get_connection()
    try:
        if new_code and new_code != cust_code:
            # cust_code 변경: 관련 테이블도 같이 업데이트
            conn.execute("UPDATE customers SET cust_code=? WHERE cust_code=?", (new_code, cust_code))
            conn.execute("UPDATE orders SET cust_code=? WHERE cust_code=?", (new_code, cust_code))
        if cust_name:
            target = new_code if new_code else cust_code
            conn.execute("UPDATE customers SET cust_name=? WHERE cust_code=?", (cust_name, target))
        if alias is not None:
            target = new_code if new_code else cust_code
            conn.execute("UPDATE customers SET alias=? WHERE cust_code=?", (alias, target))
        conn.commit()
    except Exception as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()
    return {"success": True}
