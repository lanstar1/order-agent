"""
거래처 관리 API
"""
from fastapi import APIRouter, HTTPException
from db.database import get_connection
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

router = APIRouter(prefix="/api/customers", tags=["customers"])


@router.get("/")
async def list_customers(q: str = ""):
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
async def create_customer(cust_code: str, cust_name: str, alias: str = ""):
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


@router.put("/{cust_code}")
async def update_customer(cust_code: str, cust_name: str = None, alias: str = None, new_code: str = None):
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
