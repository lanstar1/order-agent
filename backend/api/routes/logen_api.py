"""
로젠택배 OpenAPI 라우트
- 계약정보 확인
- 주문등록 + 송장번호 조회
- 스마트스토어/쿠팡 공통 사용
"""
import logging
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/logen", tags=["Logen"])


# ── 요청 모델 ──

class LogenOrder(BaseModel):
    rcv_name: str
    rcv_addr: str
    rcv_tel: str
    qty: int = 1
    goods_nm: str = ""
    goods_amt: int = 0
    in_qty: int = 0       # 0이면 qty와 동일하게 처리
    snd_msg: str = ""

class LogenRegisterRequest(BaseModel):
    orders: list[LogenOrder]


# ── 엔드포인트 ──

@router.get("/contract")
async def get_contract():
    """계약정보 조회 (연결 테스트용)"""
    from services.ilogen_openapi import check_contract
    try:
        result = await check_contract()
        return result
    except Exception as e:
        logger.error(f"[로젠API] 계약정보 조회 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/register")
async def register_orders(req: LogenRegisterRequest = Body(...)):
    """
    주문 일괄 등록 + 송장번호 자동 조회.

    요청 예시:
    {
        "orders": [
            {
                "rcv_name": "홍길동",
                "rcv_addr": "서울시 강남구 테헤란로 123",
                "rcv_tel": "01012345678",
                "qty": 1,
                "goods_nm": "랜케이블 CAT.6",
                "goods_amt": 8500,
                "snd_msg": "부재시 경비실"
            }
        ]
    }
    """
    from services.ilogen_openapi import register_and_get_slips

    if not req.orders:
        return {"success": True, "message": "등록할 주문이 없습니다.", "results": []}

    orders = []
    for o in req.orders:
        orders.append({
            "rcv_name": o.rcv_name,
            "rcv_addr": o.rcv_addr,
            "rcv_tel": o.rcv_tel,
            "qty": o.qty,
            "goods_nm": o.goods_nm,
            "goods_amt": o.goods_amt,
            "in_qty": o.in_qty if o.in_qty > 0 else o.qty,
            "snd_msg": o.snd_msg,
        })

    try:
        result = await register_and_get_slips(orders)
        return result
    except Exception as e:
        logger.error(f"[로젠API] 주문등록 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query-slips")
async def query_slips(fix_take_nos: list[str] = Body(...)):
    """송장번호 개별 조회 (주문번호 목록으로)"""
    from services.ilogen_openapi import query_slip_numbers

    if not fix_take_nos:
        return {"success": True, "slips": [], "message": "조회 대상 없음"}

    try:
        result = await query_slip_numbers(fix_take_nos)
        return result
    except Exception as e:
        logger.error(f"[로젠API] 송장조회 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
