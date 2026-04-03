"""
바코드 ERP Bridge API 라우트
- 쿠팡 PO 파일 처리: 바코드→품목코드 변환, 납품부족사유 관리, 이카운트 전표 등록
"""
import json
import logging
from urllib.parse import quote

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from services.barcode_service import (
    fill_shortage_reasons,
    load_master,
    parse_po_items,
    send_to_ecount,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/barcode", tags=["바코드 ERP Bridge"])


@router.get("/master-info")
async def master_info():
    """마스터 데이터 현황 반환"""
    barcode_to_code, code_to_barcode, discontinued, price_up = load_master()
    return {
        "po_count": len(barcode_to_code),
        "order_count": len(code_to_barcode),
        "order_with_barcode": sum(1 for v in code_to_barcode.values() if v),
        "discontinued_count": len(discontinued),
        "price_up_count": len(price_up),
    }


@router.post("/parse-po")
async def parse_po(file: UploadFile = File(...)):
    """PO 파일을 읽어 항목 목록 반환 (납품부족사유 선택용)"""
    contents = await file.read()
    try:
        items = parse_po_items(contents)
        return JSONResponse(content={"items": items, "total": len(items)})
    except Exception as e:
        logger.error(f"[바코드] PO 파싱 실패: {e}")
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.post("/download-po")
async def download_po(
    file: UploadFile = File(...),
    shortage_reasons: str = Form("{}"),
):
    """납품부족사유를 채워서 PO 파일 다운로드"""
    contents = await file.read()
    try:
        reasons = json.loads(shortage_reasons)
        output = fill_shortage_reasons(contents, reasons)
        safe_name = quote(file.filename.replace(".xlsx", "_납품부족사유.xlsx"))
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{safe_name}"},
        )
    except Exception as e:
        logger.error(f"[바코드] PO 다운로드 실패: {e}")
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.post("/send-to-ecount")
async def api_send_to_ecount(
    file: UploadFile = File(...),
    staff_code: str = Form(""),
    io_date: str = Form(""),
    shortage_reasons: str = Form("{}"),
):
    """PO 파일 → 이카운트 판매 전표 등록"""
    contents = await file.read()
    try:
        result = await send_to_ecount(
            contents=contents,
            staff_code=staff_code,
            io_date=io_date,
            shortage_reasons_json=shortage_reasons,
        )
        return JSONResponse(content=result)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})
    except Exception as e:
        logger.error(f"[바코드] 이카운트 전송 실패: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": f"이카운트 전송 실패: {str(e)}"})
