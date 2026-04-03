"""
매입정산 API 라우터
- POST /api/reconcile/upload-vendor-ledger — 거래처 원장 엑셀 업로드·파싱
- POST /api/reconcile/compare             — 거래처 원장 vs ERP 데이터 비교 (AI 매칭)
- POST /api/reconcile/save-purchase        — 누락 매입전표 ERP 입력
- POST /api/reconcile/validate-purchase    — 매입전표 입력 전 유효성 검사
- GET  /api/reconcile/session/{session_id} — 이전 비교 결과 조회
"""
import os
import uuid
import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from pydantic import BaseModel
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from config import UPLOAD_DIR, ERP_WH_CD, ERP_EMP_CD
from services.vendor_parser import parse_vendor_ledger
from services.ai_matcher import match_products_ai, check_sales_history
from services.erp_client import erp_client

router = APIRouter(prefix="/api/reconcile", tags=["purchase-reconciliation"])
logger = logging.getLogger(__name__)

# 세션별 데이터 저장 (메모리)
reconcile_sessions: dict = {}


# ──── 모델 ────
class CompareRequest(BaseModel):
    vendor_items: list[dict]
    erp_purchase_data: list[dict]
    erp_sales_data: list[dict] = []
    use_ai: bool = True


class PurchaseItem(BaseModel):
    io_date: str               # "20260301"
    cust_code: str = ""
    cust_name: str = ""
    wh_cd: str = ""
    prod_cd: str = ""
    prod_name: str = ""
    size_des: str = ""
    qty: int = 1
    price: float = 0
    supply_amt: float = 0
    vat_amt: float = 0
    remarks: str = "매입정산 자동입력"


class PurchaseSaveRequest(BaseModel):
    items: list[PurchaseItem]
    upload_ser_no: str = "1"


# ──── 엔드포인트 ────

@router.post("/upload-vendor-ledger")
async def upload_vendor_ledger(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    """거래처 원장 엑셀 업로드 및 파싱"""
    file_ext = os.path.splitext(file.filename)[1]
    if file_ext.lower() not in (".xlsx", ".xls"):
        raise HTTPException(400, "엑셀 파일(.xlsx)만 업로드 가능합니다")

    os.makedirs(str(UPLOAD_DIR), exist_ok=True)
    saved_name = f"reconcile_{uuid.uuid4().hex[:8]}{file_ext}"
    saved_path = os.path.join(str(UPLOAD_DIR), saved_name)

    with open(saved_path, "wb") as f:
        content = await file.read()
        f.write(content)

    try:
        result = parse_vendor_ledger(saved_path)
        result["file_id"] = saved_name
        result["original_filename"] = file.filename
        return result
    except Exception as e:
        logger.error(f"거래처 원장 파싱 실패: {e}")
        raise HTTPException(500, f"파싱 실패: {str(e)}")


@router.post("/compare")
async def compare_ledgers(
    req: CompareRequest,
    user: dict = Depends(get_current_user),
):
    """거래처 원장 vs ERP 데이터 비교 (AI 매칭)"""
    # 거래처 원장에서 "매출" 항목 = 우리의 매입
    our_purchase_from_vendor = [
        item for item in req.vendor_items
        if item.get("tx_type") == "매출"
    ]

    # AI or 규칙 기반 매칭
    match_results = await match_products_ai(
        our_purchase_from_vendor, req.erp_purchase_data
    )

    matched = [r for r in match_results if r["match_type"] != "unmatched"]
    unmatched = [r for r in match_results if r["match_type"] == "unmatched"]

    # 누락 건 → 판매이력 확인
    sales_check = []
    if unmatched and req.erp_sales_data:
        unmatched_vendor_items = [r["vendor_item"] for r in unmatched]
        sales_check = await check_sales_history(
            unmatched_vendor_items, req.erp_sales_data
        )

    session_id = uuid.uuid4().hex[:12]
    reconcile_sessions[session_id] = {
        "matched": matched,
        "unmatched": unmatched,
        "sales_check": sales_check,
    }

    return {
        "session_id": session_id,
        "summary": {
            "total_vendor_items": len(our_purchase_from_vendor),
            "matched_count": len(matched),
            "unmatched_count": len(unmatched),
            "with_sales_history": sum(
                1 for s in sales_check if s.get("has_sales_history")
            ),
        },
        "matched": matched,
        "unmatched": unmatched,
        "sales_check": sales_check,
    }


@router.get("/session/{session_id}")
async def get_session(
    session_id: str,
    user: dict = Depends(get_current_user),
):
    """이전 비교 결과 조회"""
    if session_id not in reconcile_sessions:
        raise HTTPException(404, "세션을 찾을 수 없습니다")
    return reconcile_sessions[session_id]


@router.post("/save-purchase")
async def save_purchase(
    req: PurchaseSaveRequest,
    user: dict = Depends(get_current_user),
):
    """누락 매입전표를 ERP에 입력 (기존 erp_client.save_purchase 활용)"""
    if not req.items:
        raise HTTPException(400, "입력할 항목이 없습니다")

    results = []
    success_count = 0
    fail_count = 0

    # 거래처코드별로 그룹핑
    from collections import defaultdict
    grouped: dict[str, list[PurchaseItem]] = defaultdict(list)
    for item in req.items:
        grouped[item.cust_code].append(item)

    for cust_code, items in grouped.items():
        lines = []
        io_date = items[0].io_date if items else ""

        for item in items:
            lines.append({
                "prod_cd": item.prod_cd,
                "qty": item.qty,
                "unit": "",
                "price": item.price,
            })

        try:
            result = await erp_client.save_purchase(
                cust_code=cust_code,
                lines=lines,
                upload_ser=req.upload_ser_no,
                wh_cd=items[0].wh_cd or ERP_WH_CD,
                emp_cd=ERP_EMP_CD,
                io_date=io_date,
            )

            status = result.get("Status", "")
            data = result.get("Data", {})

            if status == "200":
                s_cnt = int(data.get("SuccessCnt", 0))
                f_cnt = int(data.get("FailCnt", 0))
                success_count += s_cnt
                fail_count += f_cnt
                results.append({
                    "cust_code": cust_code,
                    "status": "success",
                    "success": s_cnt,
                    "failed": f_cnt,
                    "slip_nos": data.get("SlipNos", ""),
                    "details": data.get("ResultDetails", ""),
                })
            else:
                error = result.get("Error", {})
                fail_count += len(items)
                results.append({
                    "cust_code": cust_code,
                    "status": "error",
                    "success": 0,
                    "failed": len(items),
                    "error_message": error.get("Message", "알 수 없는 오류"),
                })

        except Exception as e:
            fail_count += len(items)
            results.append({
                "cust_code": cust_code,
                "status": "error",
                "success": 0,
                "failed": len(items),
                "error_message": str(e),
            })

    return {
        "status": "success" if fail_count == 0 else "partial" if success_count > 0 else "error",
        "total": len(req.items),
        "success": success_count,
        "failed": fail_count,
        "results": results,
    }


@router.post("/validate-purchase")
async def validate_purchase(
    req: PurchaseSaveRequest,
    user: dict = Depends(get_current_user),
):
    """매입전표 입력 전 유효성 검사"""
    errors = []
    for i, item in enumerate(req.items):
        item_errors = []
        if not item.io_date or len(item.io_date) != 8:
            item_errors.append("전표일자(YYYYMMDD) 형식 오류")
        if not item.cust_code and not item.cust_name:
            item_errors.append("거래처코드 또는 거래처명 필요")
        if not item.prod_cd and not item.prod_name:
            item_errors.append("품목코드 또는 품목명 필요")
        if item.qty <= 0:
            item_errors.append("수량은 1 이상이어야 함")
        if item_errors:
            errors.append({"index": i, "errors": item_errors})

    return {
        "valid": len(errors) == 0,
        "error_count": len(errors),
        "errors": errors,
    }
