"""
B2B 간편발주 API — 고정 거래처 전용 채널 (고도몰 B2C와 분리).

거래처 흐름: PIN 로그인 → 즐겨찾기 품목+수량 → 원터치 발주 → ECOUNT 판매전표 자동 생성.
관리자 흐름: 거래처 B2B 활성화/단가유형/PIN/즐겨찾기 설정 (직원 JWT 필요).
"""
import logging
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, Field
from typing import Optional, List

from security import (
    create_customer_token, get_current_customer, get_current_user,
    check_rate_limit, validate_code,
)
from services import b2b_service

router = APIRouter(prefix="/api/b2b", tags=["b2b"])
logger = logging.getLogger(__name__)


# ─────────────── 스키마 ───────────────
class LoginReq(BaseModel):
    cust_code: str = Field(..., min_length=1, max_length=50)
    pin: str = Field(..., min_length=4, max_length=32)


class OrderLine(BaseModel):
    prod_cd: str = Field(..., min_length=1, max_length=50)
    qty: float = Field(..., gt=0)


class OrderReq(BaseModel):
    lines: List[OrderLine]
    memo: str = Field("", max_length=500)


class SetupReq(BaseModel):
    cust_code: str = Field(..., min_length=1, max_length=50)
    pin: Optional[str] = Field(None, min_length=4, max_length=32)
    price_tier: Optional[int] = Field(None, ge=0, le=10)
    wh_cd: Optional[str] = Field(None, max_length=20)
    enabled: Optional[bool] = None


class FavItem(BaseModel):
    prod_cd: str = Field(..., min_length=1, max_length=50)
    prod_name: str = Field("", max_length=200)
    unit: str = Field("", max_length=20)


# ─────────────── 거래처(고객) 엔드포인트 ───────────────
@router.post("/login")
async def b2b_login(req: LoginReq, request: Request):
    """거래처 PIN 로그인 → JWT."""
    check_rate_limit(request.client.host if request.client else "unknown", limit=20)
    cust = b2b_service.verify_login(req.cust_code.strip(), req.pin)
    if not cust:
        raise HTTPException(401, "거래처코드 또는 PIN이 올바르지 않습니다.")
    token = create_customer_token(cust["cust_code"], cust.get("cust_name", ""))
    logger.info(f"[B2B] 로그인: {cust['cust_code']}")
    return {
        "success": True,
        "token": token,
        "cust_code": cust["cust_code"],
        "cust_name": cust.get("cust_name", ""),
    }


@router.get("/me")
async def b2b_me(cust: dict = Depends(get_current_customer)):
    info = b2b_service.get_customer(cust["cust_code"]) or {}
    return {
        "cust_code": cust["cust_code"],
        "cust_name": info.get("cust_name", cust.get("cust_name", "")),
        "price_tier": info.get("price_tier", 0),
    }


@router.get("/catalog")
async def b2b_catalog(cust: dict = Depends(get_current_customer)):
    """즐겨찾기 품목 + 거래처별 단가 (발주 화면용)."""
    try:
        return await b2b_service.build_catalog(cust["cust_code"])
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/order")
async def b2b_order(req: OrderReq, cust: dict = Depends(get_current_customer)):
    """원터치 발주 → ECOUNT 판매전표. 단가는 서버가 재조회(클라이언트 값 미신뢰)."""
    try:
        result = await b2b_service.submit_order(
            cust["cust_code"],
            [l.model_dump() for l in req.lines],
            memo=req.memo,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not result.get("success"):
        # 200으로 내려주되 success=false (프론트가 메시지 표시)
        return result
    return result


@router.get("/orders")
async def b2b_orders(cust: dict = Depends(get_current_customer)):
    return {"orders": b2b_service.get_order_history(cust["cust_code"])}


# ─────────────── 관리자 엔드포인트 (직원 JWT) ───────────────
@router.post("/admin/setup")
async def admin_setup(req: SetupReq, user: dict = Depends(get_current_user)):
    """거래처 B2B 활성화/단가유형/PIN/출하창고 설정."""
    validate_code(req.cust_code, "거래처코드")
    try:
        cust = b2b_service.setup_b2b_customer(
            req.cust_code.strip(),
            pin=req.pin or "",
            price_tier=req.price_tier,
            wh_cd=req.wh_cd,
            enabled=req.enabled,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    # pin_hash는 응답에서 제외
    cust.pop("pin_hash", None)
    return {"success": True, "customer": cust}


@router.get("/admin/favorites/{cust_code}")
async def admin_get_favorites(cust_code: str, user: dict = Depends(get_current_user)):
    return {"favorites": b2b_service.get_favorites(cust_code)}


@router.put("/admin/favorites/{cust_code}")
async def admin_set_favorites(cust_code: str, items: List[FavItem],
                              user: dict = Depends(get_current_user)):
    n = b2b_service.set_favorites(cust_code, [i.model_dump() for i in items])
    return {"success": True, "count": n}


@router.get("/admin/price-preview/{cust_code}")
async def admin_price_preview(cust_code: str, user: dict = Depends(get_current_user)):
    """관리자가 거래처 단가유형 설정을 검증할 수 있게 카탈로그 단가 미리보기."""
    try:
        return await b2b_service.build_catalog(cust_code)
    except ValueError as e:
        raise HTTPException(400, str(e))
