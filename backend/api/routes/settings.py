"""
시스템 설정 API 라우터
- Claude 모델 선택
- 기타 시스템 설정
"""
import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user

from db.database import get_connection
import config

router = APIRouter(prefix="/api/settings", tags=["settings"])
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  사용 가능한 Claude 모델 목록
# ─────────────────────────────────────────
AVAILABLE_MODELS = [
    {
        "id": "claude-haiku-4-5-20251001",
        "name": "Claude Haiku 4.5",
        "tier": "basic",
        "description": "가장 빠르고 경제적 · 간단한 분석에 적합",
    },
    {
        "id": "claude-sonnet-4-5-20250929",
        "name": "Claude Sonnet 4.5",
        "tier": "standard",
        "description": "속도와 성능의 균형 · 기본 권장 모델",
    },
    {
        "id": "claude-sonnet-4-20250514",
        "name": "Claude Sonnet 4",
        "tier": "standard",
        "description": "안정적인 성능의 표준 모델",
    },
    {
        "id": "claude-opus-4-20250514",
        "name": "Claude Opus 4",
        "tier": "premium",
        "description": "복잡한 분석에 강력한 성능",
    },
    {
        "id": "claude-opus-4-5-20251101",
        "name": "Claude Opus 4.5",
        "tier": "premium",
        "description": "최상위 성능 · 고난도 분석에 최적",
    },
    {
        "id": "claude-opus-4-6-20260320",
        "name": "Claude Opus 4.6",
        "tier": "premium",
        "description": "최신 최상위 모델 · 최고 정확도",
    },
]


class ModelSettingRequest(BaseModel):
    model_id: str


# ─────────────────────────────────────────
#  설정 테이블 초기화 (app_settings)
# ─────────────────────────────────────────
def ensure_settings_table():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.commit()
    conn.close()


# ─────────────────────────────────────────
#  모델 목록 조회
# ─────────────────────────────────────────
@router.get("/models")
async def get_models():
    """사용 가능한 Claude 모델 목록 + 현재 선택된 모델"""
    ensure_settings_table()
    conn = get_connection()
    row = conn.execute("SELECT value FROM app_settings WHERE key='claude_model'").fetchone()
    conn.close()

    current = row["value"] if row else config.CLAUDE_MODEL

    return {
        "models": AVAILABLE_MODELS,
        "current_model": current,
    }


# ─────────────────────────────────────────
#  모델 변경
# ─────────────────────────────────────────
@router.post("/models")
async def set_model(req: ModelSettingRequest, user: dict = Depends(get_current_user)):
    """Claude 모델 변경"""
    valid_ids = {m["id"] for m in AVAILABLE_MODELS}
    if req.model_id not in valid_ids:
        raise HTTPException(400, f"유효하지 않은 모델: {req.model_id}")

    ensure_settings_table()
    conn = get_connection()
    conn.execute("""
        INSERT INTO app_settings(key, value, updated_at)
        VALUES('claude_model', ?, datetime('now','localtime'))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (req.model_id,))
    conn.commit()
    conn.close()

    # 런타임 config도 업데이트
    config.CLAUDE_MODEL = req.model_id

    model_info = next((m for m in AVAILABLE_MODELS if m["id"] == req.model_id), None)
    logger.info(f"[설정] Claude 모델 변경: {req.model_id}")
    return {
        "success": True,
        "model_id": req.model_id,
        "model_name": model_info["name"] if model_info else req.model_id,
        "message": f"모델이 {model_info['name']}(으)로 변경되었습니다." if model_info else "모델 변경 완료",
    }


# ─────────────────────────────────────────
#  전체 설정 조회
# ─────────────────────────────────────────
@router.get("/")
async def get_all_settings():
    """전체 설정 조회"""
    ensure_settings_table()
    conn = get_connection()
    rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    conn.close()

    settings = {r["key"]: r["value"] for r in rows}

    return {
        "claude_model": settings.get("claude_model", config.CLAUDE_MODEL),
        "confidence_threshold": config.CONFIDENCE_THRESHOLD,
        "erp_com_code": config.ERP_COM_CODE,
    }
