"""
AICC REST 라우터
"""
from fastapi import APIRouter, Depends, HTTPException
import os

from services.aicc_data_loader import data_loader
from services.aicc_session_manager import session_manager
from security import get_current_user

router = APIRouter(prefix="/api/aicc", tags=["aicc"])

# ── REST 엔드포인트 ────────────────────────────────────────

@router.get("/models")
async def get_models(q: str = ""):
    """드롭다운 자동완성 — q 없으면 전체, 있으면 필터"""
    if q and len(q) >= 2:
        return data_loader.search_models(q, limit=15)
    return data_loader.dropdown_models  # 전체 목록

@router.get("/sessions")
async def get_sessions(current_user=Depends(get_current_user)):
    """관리자: 전체 세션 목록"""
    return {"sessions": session_manager.get_all_sessions()}

@router.get("/sessions/{session_id}")
async def get_session_detail(session_id: str, current_user=Depends(get_current_user)):
    s = session_manager.get_session(session_id)
    if not s:
        raise HTTPException(404, "세션 없음")
    return session_manager._serialize(s)

@router.post("/sessions/{session_id}/intervene")
async def intervene(session_id: str, current_user=Depends(get_current_user)):
    session_manager.intervene(session_id)
    await session_manager.send_to_customer(session_id, {
        "type": "admin_joined",
        "content": "담당자가 연결되었습니다. 직접 안내해 드리겠습니다."
    })
    return {"ok": True}

@router.post("/sessions/{session_id}/close")
async def close_session(session_id: str, current_user=Depends(get_current_user)):
    session_manager.close_session(session_id)
    await session_manager.send_to_customer(session_id, {
        "type": "session_closed",
        "content": "상담이 종료되었습니다. 감사합니다."
    })
    return {"ok": True}

@router.post("/sessions/{session_id}/admin-message")
async def admin_message_rest(session_id: str, body: dict, current_user=Depends(get_current_user)):
    """WebSocket 미연결 시 REST 폴백"""
    content = body.get("content", "")
    session_manager.add_message(session_id, "admin", content)
    await session_manager.send_to_customer(session_id, {
        "type": "admin_message", "content": content
    })
    return {"ok": True}

@router.get("/inventory/{model_name}")
async def get_inventory(model_name: str):
    """ERP 재고조회"""
    erp_code = data_loader.get_erp_code(model_name)
    if not erp_code:
        return {"available": False, "message": "ERP 코드 없음"}
    try:
        from services.erp_client import ERPClient
        erp = ERPClient()
        result = await erp.get_inventory_by_location(erp_code)
        return {
            "model_name": model_name,
            "erp_code": erp_code,
            "yongsan": result.get("yongsan", 0),
            "gimpo": result.get("gimpo", 0),
            "total": result.get("total", 0),
        }
    except Exception as e:
        return {"available": False, "message": f"조회 실패: {str(e)}"}
