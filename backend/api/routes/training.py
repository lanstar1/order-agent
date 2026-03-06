"""
발주서 학습 데이터 API 라우터
- 판매전표 엑셀 업로드 + 원본 발주서 텍스트/이미지 매칭
- 학습 데이터 조회/삭제
- 통계 조회
"""
import logging
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query, Depends
from fastapi.responses import Response
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user
from services.training_service import (
    parse_sales_slip_excel,
    save_training_pair,
    get_training_pairs,
    get_training_pair_detail,
    get_training_pair_image,
    delete_training_pair,
    get_training_stats,
)
from services.bulk_training_service import (
    create_session as bulk_create_session,
    get_session as bulk_get_session,
    extract_po_image as bulk_extract_po,
    suggest_matches as bulk_suggest_matches,
    confirm_and_save as bulk_confirm_save,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/training", tags=["training"])


# ─────────────────────────────────────────
#  엑셀 미리보기 (파싱만, 저장 안 함)
# ─────────────────────────────────────────
@router.post("/preview-excel")
async def preview_excel(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    """판매전표 엑셀 업로드 → 파싱 결과 미리보기 (저장 안 함)"""
    allowed_exts = {".xlsx", ".xls", ".xlsm"}
    suffix = Path(file.filename).suffix.lower() if file.filename else ""
    if suffix not in allowed_exts:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix}. xlsx/xls/xlsm만 가능합니다.")

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 10MB를 초과합니다.")

    try:
        result = parse_sales_slip_excel(content, file.filename)
        return {
            "success": True,
            "filename": file.filename,
            "vendor": result["vendor"],
            "total_items": result["total_items"],
            "items": result["items"],
        }
    except Exception as e:
        logger.error(f"[Training] 엑셀 파싱 실패: {e}", exc_info=True)
        raise HTTPException(422, f"엑셀 파싱 실패: {str(e)}")


# ─────────────────────────────────────────
#  학습 데이터 저장 (엑셀 업로드 + 원문)
# ─────────────────────────────────────────
@router.post("/upload")
async def upload_training_data(
    file: UploadFile = File(...),
    cust_code: str = Form(...),
    cust_name: str = Form(...),
    raw_po_text: str = Form(""),
    order_id: str = Form(""),
    memo: str = Form(""),
    user: dict = Depends(get_current_user),
):
    """판매전표 엑셀 + 원본 발주서 텍스트를 매칭하여 학습 데이터로 저장"""
    allowed_exts = {".xlsx", ".xls", ".xlsm"}
    suffix = Path(file.filename).suffix.lower() if file.filename else ""
    if suffix not in allowed_exts:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix}")

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 10MB를 초과합니다.")

    try:
        parsed = parse_sales_slip_excel(content, file.filename)
    except Exception as e:
        raise HTTPException(422, f"엑셀 파싱 실패: {str(e)}")

    if not parsed["items"]:
        raise HTTPException(422, "엑셀에서 품목 데이터를 찾을 수 없습니다.")

    result = save_training_pair(
        cust_code=cust_code,
        cust_name=cust_name,
        raw_po_text=raw_po_text,
        items=parsed["items"],
        order_id=order_id,
        memo=memo,
    )

    if not result.get("success"):
        raise HTTPException(500, f"저장 실패: {result.get('error', '알 수 없는 오류')}")

    return {
        "success": True,
        "pair_id": result["pair_id"],
        "item_count": result["item_count"],
        "message": f"학습 데이터 저장 완료: {result['item_count']}개 품목",
    }


# ─────────────────────────────────────────
#  JSON 직접 저장 (엑셀 없이, 이미지 선택 가능)
# ─────────────────────────────────────────
class TrainingItemInput(BaseModel):
    item_code: str
    product_name: str = ""
    model_name: str = ""
    spec: str = ""
    qty: float = 0
    unit: str = "EA"
    unit_price: float = 0
    supply_price: float = 0
    tax: float = 0
    raw_line_text: str = ""


class TrainingPairInput(BaseModel):
    cust_code: str
    cust_name: str
    raw_po_text: str = ""
    raw_po_image_base64: str = ""       # base64 인코딩된 이미지
    raw_po_image_type: str = ""         # MIME type (image/png 등)
    items: List[TrainingItemInput]
    order_id: str = ""
    memo: str = ""


@router.post("/save-json")
async def save_training_json(body: TrainingPairInput, user: dict = Depends(get_current_user)):
    """JSON으로 직접 학습 데이터 저장 (이미지 포함 가능)"""
    if not body.items:
        raise HTTPException(400, "최소 1개 이상의 품목이 필요합니다.")

    # base64 이미지 디코딩
    raw_po_image = None
    raw_po_image_type = ""
    if body.raw_po_image_base64:
        import base64
        try:
            raw_po_image = base64.b64decode(body.raw_po_image_base64)
            raw_po_image_type = body.raw_po_image_type or "image/png"
        except Exception as e:
            logger.warning(f"[Training] 이미지 디코딩 실패: {e}")

    items_dict = [item.dict() for item in body.items]
    result = save_training_pair(
        cust_code=body.cust_code,
        cust_name=body.cust_name,
        raw_po_text=body.raw_po_text,
        items=items_dict,
        order_id=body.order_id,
        memo=body.memo,
        raw_po_image=raw_po_image,
        raw_po_image_type=raw_po_image_type,
    )

    if not result.get("success"):
        raise HTTPException(500, f"저장 실패: {result.get('error')}")

    return {
        "success": True,
        "pair_id": result["pair_id"],
        "item_count": result["item_count"],
        "message": f"학습 데이터 저장 완료: {result['item_count']}개 품목",
    }


# ─────────────────────────────────────────
#  발주서 이미지 서빙
# ─────────────────────────────────────────
@router.get("/pairs/{pair_id}/image")
async def get_pair_image(pair_id: int):
    """학습 데이터의 발주서 이미지 반환"""
    img_bytes, mime_type = get_training_pair_image(pair_id)
    if not img_bytes:
        raise HTTPException(404, "이미지가 없습니다.")
    return Response(content=img_bytes, media_type=mime_type)


# ─────────────────────────────────────────
#  조회
# ─────────────────────────────────────────
@router.get("/pairs")
async def list_training_pairs(
    cust_code: str = Query("", description="거래처코드 필터"),
    limit: int = Query(50, ge=1, le=200),
):
    """학습 데이터 목록 조회"""
    pairs = get_training_pairs(cust_code=cust_code, limit=limit)
    return {"pairs": pairs, "total": len(pairs)}


@router.get("/pairs/{pair_id}")
async def training_pair_detail(pair_id: int):
    """학습 데이터 상세 조회"""
    detail = get_training_pair_detail(pair_id)
    if not detail:
        raise HTTPException(404, "학습 데이터를 찾을 수 없습니다.")
    return detail


@router.delete("/pairs/{pair_id}")
async def remove_training_pair(pair_id: int, user: dict = Depends(get_current_user)):
    """학습 데이터 삭제"""
    result = delete_training_pair(pair_id)
    if not result.get("success"):
        raise HTTPException(500, f"삭제 실패: {result.get('error')}")
    return {"success": True, "message": "삭제 완료"}


@router.get("/stats")
async def training_stats():
    """학습 데이터 통계"""
    return get_training_stats()


# ─────────────────────────────────────────
#  대량 학습 (Bulk Training) API
# ─────────────────────────────────────────

@router.post("/bulk/create-session")
async def bulk_create(
    file: UploadFile = File(...),
    cust_code: str = Form(...),
    cust_name: str = Form(...),
    user: dict = Depends(get_current_user),
):
    """대량 학습 세션 생성 + 판매전표 엑셀 파싱"""
    allowed_exts = {".xlsx", ".xls", ".xlsm"}
    suffix = Path(file.filename).suffix.lower() if file.filename else ""
    if suffix not in allowed_exts:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix}")

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 10MB를 초과합니다.")

    try:
        result = bulk_create_session(cust_code, cust_name, content, file.filename)
        return result
    except Exception as e:
        logger.error(f"[BulkTrain] 세션 생성 실패: {e}", exc_info=True)
        raise HTTPException(500, f"세션 생성 실패: {str(e)}")


@router.post("/bulk/extract-po")
async def bulk_extract(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    user: dict = Depends(get_current_user),
):
    """발주서 이미지 1건 AI 추출"""
    allowed_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf"}
    suffix = Path(file.filename).suffix.lower() if file.filename else ""
    if suffix not in allowed_exts:
        raise HTTPException(400, f"지원하지 않는 파일 형식: {suffix}")

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "파일 크기가 10MB를 초과합니다.")

    media_types = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".gif": "image/gif",
        ".webp": "image/webp", ".pdf": "application/pdf",
    }
    media_type = media_types.get(suffix, "image/jpeg")

    try:
        result = await bulk_extract_po(session_id, file.filename, content, media_type)
        return result
    except Exception as e:
        logger.error(f"[BulkTrain] 추출 실패: {e}", exc_info=True)
        raise HTTPException(500, f"추출 실패: {str(e)}")


@router.post("/bulk/suggest-matches")
async def bulk_suggest(session_id: str = Form(...), user: dict = Depends(get_current_user)):
    """매칭 제안 생성"""
    result = bulk_suggest_matches(session_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


class BulkMatchItem(BaseModel):
    po_item: dict
    excel_item: dict


class BulkConfirmation(BaseModel):
    extraction_id: int
    matches: List[BulkMatchItem]


class BulkConfirmRequest(BaseModel):
    session_id: str
    confirmations: List[BulkConfirmation]


@router.post("/bulk/confirm")
async def bulk_confirm(body: BulkConfirmRequest, user: dict = Depends(get_current_user)):
    """확인된 매칭 저장"""
    confirmations = [c.dict() for c in body.confirmations]
    result = bulk_confirm_save(body.session_id, confirmations)
    if not result.get("success"):
        raise HTTPException(500, result.get("error", "저장 실패"))
    return result


@router.get("/bulk/session/{session_id}")
async def bulk_session_detail(session_id: str):
    """세션 상태 조회"""
    session = bulk_get_session(session_id)
    if not session:
        raise HTTPException(404, "세션을 찾을 수 없습니다.")
    return session
