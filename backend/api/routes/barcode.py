"""
바코드 ERP Bridge API 라우트
- 쿠팡 PO 파일 처리: 바코드→품목코드 변환, 납품부족사유 관리, 이카운트 전표 등록
- 마스터 데이터 업로드/갱신
"""
import json
import logging
import shutil
from datetime import datetime
from urllib.parse import quote

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from services.barcode_service import (
    MASTER_PATH,
    fill_shortage_reasons,
    load_master,
    parse_po_items,
    send_to_ecount,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/barcode", tags=["바코드 ERP Bridge"])


@router.post("/upload-master")
async def upload_master(file: UploadFile = File(...)):
    """마스터 데이터(master_data.xlsx) 업로드/갱신"""
    if not file.filename.endswith((".xlsx", ".xls")):
        return JSONResponse(status_code=400, content={"detail": "xlsx 파일만 업로드 가능합니다."})

    try:
        from pathlib import Path
        master = Path(MASTER_PATH)
        master.parent.mkdir(parents=True, exist_ok=True)

        # 기존 파일 백업
        if master.exists():
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = master.parent / f"master_data_backup_{ts}.xlsx"
            shutil.copy2(str(master), str(backup))
            logger.info(f"[바코드] 기존 마스터 백업: {backup}")

        # 새 파일 저장
        contents = await file.read()
        with open(str(master), "wb") as f:
            f.write(contents)

        # 검증: 로드해서 매핑 수 확인
        barcode_to_code, code_to_barcode, discontinued, price_up, needs_label = load_master()
        logger.info(f"[바코드] 마스터 업로드 완료: PO {len(barcode_to_code)}개, 주문서 {len(code_to_barcode)}개")

        return {
            "success": True,
            "filename": file.filename,
            "po_count": len(barcode_to_code),
            "order_count": len(code_to_barcode),
            "discontinued_count": len(discontinued),
            "price_up_count": len(price_up),
            "needs_label_count": len(needs_label),
        }
    except Exception as e:
        logger.error(f"[바코드] 마스터 업로드 실패: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": f"업로드 실패: {str(e)}"})


@router.get("/master-info")
async def master_info():
    """마스터 데이터 현황 반환"""
    barcode_to_code, code_to_barcode, discontinued, price_up, needs_label = load_master()
    return {
        "po_count": len(barcode_to_code),
        "order_count": len(code_to_barcode),
        "order_with_barcode": sum(1 for v in code_to_barcode.values() if v),
        "discontinued_count": len(discontinued),
        "price_up_count": len(price_up),
        "needs_label_count": len(needs_label),
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
