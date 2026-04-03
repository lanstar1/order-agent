"""
매입정산 API 라우터
- POST /api/reconcile/upload-vendor-ledger — 거래처 원장 엑셀 업로드·파싱
- POST /api/reconcile/compare             — 거래처 원장 vs ERP 데이터 비교 (AI 매칭)
- POST /api/reconcile/save-purchase        — 누락 매입전표 ERP 입력
- POST /api/reconcile/validate-purchase    — 매입전표 입력 전 유효성 검사
- GET  /api/reconcile/session/{session_id} — 이전 비교 결과 조회
- GET  /api/reconcile/download-result/{session_id} — 비교 결과를 엑셀로 다운로드
"""
import os
import uuid
import logging
import json
import io
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from config import UPLOAD_DIR, ERP_WH_CD, ERP_EMP_CD
from services.vendor_parser import parse_vendor_ledger
from services.ai_matcher import match_products_ai, check_sales_history, _is_shipping_item, _get_field
from services.erp_client import erp_client
from services.erp_web_scraper import erp_web_scraper

router = APIRouter(prefix="/api/reconcile", tags=["purchase-reconciliation"])
logger = logging.getLogger(__name__)

# 세션별 데이터 저장 (메모리)
reconcile_sessions: dict = {}

# 거래처 데이터 캐시 (메모리)
_vendor_cache: Optional[list[dict]] = None


# ──── 모델 ────
class ERPFetchRequest(BaseModel):
    cust_code: str = ""
    from_date: str = ""         # "20260301"
    to_date: str = ""           # "20260331"


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


# ──── 모델 (병렬 조회용) ────

class ERPFetchBothRequest(BaseModel):
    from_date: str = ""              # "20260401"
    to_date: str = ""                # "20260403"
    purchase_cust_code: str = ""     # 구매현황 거래처 필터
    sales_cust_code: str = ""        # 판매현황 거래처 필터 (보통 빈 문자열)


# ──── 엔드포인트 ────

@router.post("/fetch-erp-data")
async def fetch_erp_data(
    req: ERPFetchBothRequest,
    user: dict = Depends(get_current_user),
):
    """
    구매현황 + 판매현황을 **브라우저 탭 2개로 병렬 조회**.
    기존 순차 호출 대비 약 40~50 % 시간 절약.
    """
    try:
        result = await erp_web_scraper.get_both(
            from_date=req.from_date,
            to_date=req.to_date,
            purchase_cust_code=req.purchase_cust_code,
            sales_cust_code=req.sales_cust_code,
        )

        purchase = result.get("purchase", {})
        sales = result.get("sales", {})

        if not purchase.get("success") and not sales.get("success"):
            raise HTTPException(
                502,
                f"ERP 조회 실패 - 구매: {purchase.get('error', '?')}, 판매: {sales.get('error', '?')}"
            )

        return {
            "success": True,
            "purchase": {
                "success": purchase.get("success", False),
                "items": purchase.get("items", []),
                "total": purchase.get("total", 0),
                "error": purchase.get("error", ""),
            },
            "sales": {
                "success": sales.get("success", False),
                "items": sales.get("items", []),
                "total": sales.get("total", 0),
                "error": sales.get("error", ""),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ERP 병렬 조회 오류: {e}", exc_info=True)
        raise HTTPException(500, f"ERP 조회 오류: {str(e)}")


@router.post("/fetch-erp-purchases")
async def fetch_erp_purchases(
    req: ERPFetchRequest,
    user: dict = Depends(get_current_user),
):
    """ERP 웹에서 구매현황 자동 조회 (단독)"""
    try:
        result = await erp_web_scraper.get_purchase_list(
            from_date=req.from_date,
            to_date=req.to_date,
            cust_code=req.cust_code,
        )
        if not result.get("success"):
            raise HTTPException(
                502,
                f"ERP 구매현황 조회 실패: {result.get('error', '알 수 없는 오류')}"
            )
        return {
            "success": True,
            "items": result["items"],
            "total": result["total"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ERP 구매현황 조회 오류: {e}", exc_info=True)
        raise HTTPException(500, f"ERP 조회 오류: {str(e)}")


@router.post("/fetch-erp-sales")
async def fetch_erp_sales(
    req: ERPFetchRequest,
    user: dict = Depends(get_current_user),
):
    """ERP 웹에서 판매현황 자동 조회 (단독)"""
    try:
        result = await erp_web_scraper.get_sales_list(
            from_date=req.from_date,
            to_date=req.to_date,
            cust_code=req.cust_code,
        )
        if not result.get("success"):
            raise HTTPException(
                502,
                f"ERP 판매현황 조회 실패: {result.get('error', '알 수 없는 오류')}"
            )
        return {
            "success": True,
            "items": result["items"],
            "total": result["total"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ERP 판매현황 조회 오류: {e}", exc_info=True)
        raise HTTPException(500, f"ERP 조회 오류: {str(e)}")


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
    all_vendor_items = [
        item for item in req.vendor_items
        if item.get("tx_type") == "매출" or not item.get("tx_type")
    ]

    # 배송료/운송비 항목 분리
    shipping_items = [item for item in all_vendor_items if _is_shipping_item(item)]
    regular_items = [item for item in all_vendor_items if not _is_shipping_item(item)]

    # AI or 규칙 기반 매칭 (일반 품목만)
    match_results = await match_products_ai(
        regular_items, req.erp_purchase_data
    )

    matched = [r for r in match_results if r["match_type"] != "unmatched"]
    unmatched = [r for r in match_results if r["match_type"] == "unmatched"]

    # 금액 차이 감지 (매칭된 항목 중 금액이 다른 건)
    amount_mismatches = []
    for r in matched:
        v = r.get("vendor_item", {})
        e = r.get("erp_match", {})
        v_amt = float(v.get("amount", 0) or 0)
        e_amt = float(_get_field(e, "total", "합계", default=0) or 0)
        try:
            e_amt = float(str(e_amt).replace(",", ""))
        except:
            e_amt = 0
        v_qty = int(v.get("qty", 0) or 0)
        e_qty = int(str(_get_field(e, "qty", "수량", default=0) or 0).replace(",", ""))

        if v_amt and e_amt and abs(v_amt - e_amt) > 1:
            r["amount_diff"] = v_amt - e_amt
            r["amount_diff_pct"] = round((v_amt - e_amt) / max(v_amt, 1) * 100, 1)
            amount_mismatches.append(r)
        if v_qty and e_qty and v_qty != e_qty:
            r["qty_diff"] = v_qty - e_qty

    # 누락 건 → 판매이력 확인
    sales_check = []
    if unmatched and req.erp_sales_data:
        unmatched_vendor_items = [r["vendor_item"] for r in unmatched]
        sales_check = await check_sales_history(
            unmatched_vendor_items, req.erp_sales_data
        )

    # 배송료 항목도 ERP 구매현황에서 매칭 시도
    shipping_match_results = []
    if shipping_items and req.erp_purchase_data:
        shipping_match_results = await match_products_ai(
            shipping_items, req.erp_purchase_data
        )

    session_id = uuid.uuid4().hex[:12]
    reconcile_sessions[session_id] = {
        "matched": matched,
        "unmatched": unmatched,
        "sales_check": sales_check,
        "shipping_items": [
            {
                "vendor_item": item,
                "erp_match": next((r.get("erp_match") for r in shipping_match_results
                                   if r.get("vendor_item") == item and r.get("match_type") != "unmatched"), None),
                "match_type": next((r.get("match_type") for r in shipping_match_results
                                    if r.get("vendor_item") == item and r.get("match_type") != "unmatched"), "unmatched"),
            }
            for item in shipping_items
        ],
        "amount_mismatches": amount_mismatches,
    }

    return {
        "session_id": session_id,
        "summary": {
            "total_vendor_items": len(all_vendor_items),
            "matched_count": len(matched),
            "unmatched_count": len(unmatched),
            "with_sales_history": sum(
                1 for s in sales_check if s.get("has_sales_history")
            ),
            "shipping_count": len(shipping_items),
            "amount_mismatch_count": len(amount_mismatches),
        },
        "matched": matched,
        "unmatched": unmatched,
        "sales_check": sales_check,
        "shipping_items": reconcile_sessions[session_id]["shipping_items"],
        "amount_mismatches": amount_mismatches,
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


@router.get("/download-result/{session_id}")
async def download_result_excel(
    session_id: str,
    user: dict = Depends(get_current_user),
):
    """비교 결과를 엑셀로 다운로드"""
    if session_id not in reconcile_sessions:
        raise HTTPException(404, "세션을 찾을 수 없습니다")

    data = reconcile_sessions[session_id]

    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    except ImportError:
        raise HTTPException(500, "openpyxl이 설치되지 않았습니다")

    wb = openpyxl.Workbook()

    # Style definitions
    header_font = Font(bold=True, size=11)
    header_fill = PatternFill("solid", fgColor="4472C4")
    header_font_white = Font(bold=True, size=11, color="FFFFFF")
    green_fill = PatternFill("solid", fgColor="E2EFDA")
    red_fill = PatternFill("solid", fgColor="FCE4EC")
    yellow_fill = PatternFill("solid", fgColor="FFF9C4")
    blue_fill = PatternFill("solid", fgColor="DBEAFE")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )

    def style_header(ws, row, max_col):
        for col in range(1, max_col + 1):
            cell = ws.cell(row=row, column=col)
            cell.font = header_font_white
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center")
            cell.border = thin_border

    def style_data_cell(cell, fill=None):
        cell.border = thin_border
        cell.alignment = Alignment(horizontal="center", vertical="center")
        if fill:
            cell.fill = fill

    # ── Sheet 1: 비교 요약 ──
    ws1 = wb.active
    ws1.title = "비교요약"
    matched = data.get("matched", [])
    unmatched = data.get("unmatched", [])
    sales_check = data.get("sales_check", [])
    shipping = data.get("shipping_items", [])
    amt_mismatch = data.get("amount_mismatches", [])

    ws1["A1"] = "매입정산 비교 결과"
    ws1["A1"].font = Font(bold=True, size=14)
    ws1.merge_cells("A1:D1")

    summary_data = [
        ["구분", "건수", "비고"],
        ["✅ 매칭됨", len(matched), "거래처 원장 = ERP 구매현황"],
        ["🔴 매입전표 누락", len(unmatched), "거래처 원장에 있으나 ERP에 없음"],
        ["📦 판매이력 확인", sum(1 for s in sales_check if s.get("has_sales_history")), "판매이력은 있으나 매입전표 누락"],
        ["🚚 배송료/운송비", len(shipping), "배송 관련 항목"],
        ["⚠️ 금액 불일치", len(amt_mismatch), "매칭되었으나 금액이 다른 항목"],
        ["전체", len(matched) + len(unmatched) + len(shipping), ""],
    ]
    for r_idx, row_data in enumerate(summary_data, start=3):
        for c_idx, val in enumerate(row_data, start=1):
            cell = ws1.cell(row=r_idx, column=c_idx, value=val)
            style_data_cell(cell)
    style_header(ws1, 3, 3)
    ws1.column_dimensions["A"].width = 20
    ws1.column_dimensions["B"].width = 12
    ws1.column_dimensions["C"].width = 40

    # ── Sheet 2: 매칭 상세 ──
    ws2 = wb.create_sheet("매칭됨")
    headers2 = ["#", "비교결과", "거래처 날짜", "거래처 품목명", "수량", "금액",
                 "ERP 품목코드", "ERP 품목명", "ERP 수량", "ERP 금액", "금액차이", "신뢰도", "비고"]
    for c, h in enumerate(headers2, 1):
        ws2.cell(row=1, column=c, value=h)
    style_header(ws2, 1, len(headers2))

    for i, r in enumerate(matched, 1):
        v = r.get("vendor_item", {})
        e = r.get("erp_match", {}) or {}
        row = i + 1
        v_amt = float(v.get("amount", 0) or 0)
        e_amt = float(str(e.get("total", e.get("합계", 0)) or 0).replace(",", ""))
        diff = v_amt - e_amt if v_amt and e_amt else 0
        conf = round((r.get("confidence", 0)) * 100)

        vals = [
            i,
            "✅ 일치" if abs(diff) <= 1 else "⚠️ 금액차이",
            v.get("date", ""),
            v.get("product_name", ""),
            v.get("qty", 0),
            v_amt,
            e.get("prod_cd", e.get("품목코드", "")),
            e.get("prod_name", e.get("품명 및 모델", "")),
            e.get("qty", e.get("수량", "")),
            e_amt,
            diff if abs(diff) > 1 else 0,
            f"{conf}%",
            r.get("reason", ""),
        ]
        fill = yellow_fill if abs(diff) > 1 else green_fill
        for c, val in enumerate(vals, 1):
            cell = ws2.cell(row=row, column=c, value=val)
            style_data_cell(cell, fill)

    for col_letter in ["A","B","C","D","E","F","G","H","I","J","K","L","M"]:
        ws2.column_dimensions[col_letter].width = 15
    ws2.column_dimensions["D"].width = 30
    ws2.column_dimensions["H"].width = 30
    ws2.column_dimensions["M"].width = 25

    # ── Sheet 3: 매입전표 누락 ──
    ws3 = wb.create_sheet("매입전표누락")
    headers3 = ["#", "거래처 날짜", "품목명", "모델명", "수량", "단가", "금액", "판매이력", "검색범위", "추천 품목코드", "추천 품목명", "신뢰도", "추천사항"]
    for c, h in enumerate(headers3, 1):
        ws3.cell(row=1, column=c, value=h)
    style_header(ws3, 1, len(headers3))

    row_idx = 2
    # First add unmatched items that have sales check
    sales_check_map = {id(s.get("vendor_item", {})): s for s in sales_check}

    for i, r in enumerate(unmatched):
        v = r.get("vendor_item", {})
        # Find matching sales_check entry
        sc = None
        for s in sales_check:
            sv = s.get("vendor_item", {})
            if sv.get("product_name") == v.get("product_name") and sv.get("date") == v.get("date"):
                sc = s
                break

        best = sc.get("best_candidate", {}) if sc else {}
        fill = blue_fill if (sc and sc.get("has_sales_history")) else red_fill

        vals = [
            i + 1,
            v.get("date", ""),
            v.get("product_name", ""),
            v.get("model_name", ""),
            v.get("qty", 0),
            v.get("unit_price", 0),
            v.get("amount", 0),
            "있음" if (sc and sc.get("has_sales_history")) else "없음",
            sc.get("search_range", "") if sc else "",
            best.get("product_code", "") if best else "",
            best.get("product_name", "") if best else "",
            f"{round((best.get('confidence', 0)) * 100)}%" if best else "",
            sc.get("recommendation", "") if sc else "확인 필요",
        ]
        for c, val in enumerate(vals, 1):
            cell = ws3.cell(row=row_idx, column=c, value=val)
            style_data_cell(cell, fill)
        row_idx += 1

    for col_letter in ["A","B","C","D","E","F","G","H","I","J","K","L","M"]:
        ws3.column_dimensions[col_letter].width = 15
    ws3.column_dimensions["C"].width = 30
    ws3.column_dimensions["K"].width = 30
    ws3.column_dimensions["M"].width = 35

    # ── Sheet 4: 배송료 ──
    if shipping:
        ws4 = wb.create_sheet("배송료")
        headers4 = ["#", "날짜", "품목명", "수량", "금액", "ERP매칭", "비고"]
        for c, h in enumerate(headers4, 1):
            ws4.cell(row=1, column=c, value=h)
        style_header(ws4, 1, len(headers4))
        for i, s in enumerate(shipping, 1):
            v = s.get("vendor_item", {})
            e_match = s.get("erp_match")
            vals = [
                i, v.get("date", ""), v.get("product_name", ""),
                v.get("qty", 0), v.get("amount", 0),
                "매칭됨" if e_match else "미매칭",
                s.get("match_type", ""),
            ]
            fill = green_fill if e_match else yellow_fill
            for c, val in enumerate(vals, 1):
                cell = ws4.cell(row=i+1, column=c, value=val)
                style_data_cell(cell, fill)
        for col_letter in ["A","B","C","D","E","F","G"]:
            ws4.column_dimensions[col_letter].width = 18
        ws4.column_dimensions["C"].width = 30

    # Save to buffer
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    filename = f"매입정산_비교결과_{session_id}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"}
    )


@router.get("/vendor-list")
async def get_vendor_list(
    q: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    """거래처 목록 조회 및 검색

    - 첫 로드 시 /app/data/vendors.json에서 데이터 로드
    - 메모리에 캐시하여 성능 최적화
    - q 파라미터로 거래처명 또는 코드 검색 (대소문자 무시)
    - 검색 결과는 최대 50개, 검색 없을 시 전체 반환
    """
    global _vendor_cache

    try:
        # 캐시에 없으면 파일에서 로드
        if _vendor_cache is None:
            vendor_path = Path(__file__).parent.parent.parent.parent / "data" / "vendors.json"
            if not vendor_path.exists():
                raise HTTPException(
                    404,
                    f"거래처 데이터 파일을 찾을 수 없습니다: {vendor_path}"
                )

            with open(vendor_path, "r", encoding="utf-8") as f:
                _vendor_cache = json.load(f)

        # 검색 쿼리가 있으면 필터링
        vendors = _vendor_cache
        if q:
            q_lower = q.lower()
            vendors = [
                v for v in _vendor_cache
                if q_lower in v.get("name", "").lower() or
                   q_lower in v.get("code", "").lower()
            ]
            # 검색 결과는 최대 50개
            vendors = vendors[:50]

        return {
            "vendors": vendors,
            "total": len(vendors),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"거래처 목록 조회 오류: {e}", exc_info=True)
        raise HTTPException(500, f"거래처 목록 조회 실패: {str(e)}")
