"""
시스템 설정 API 라우터
- LLM 모델 통합 관리 (기능별)
- 관리자 비밀번호 접근 제어
"""
import logging
from typing import Optional, Dict
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from security import get_current_user, hash_password, verify_password

from db.database import get_connection
import config

router = APIRouter(prefix="/api/settings", tags=["settings"])
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  사용 가능한 모델 목록
# ─────────────────────────────────────────
CLAUDE_MODELS = [
    {"id": "claude-haiku-4-5-20251001", "name": "Claude Haiku 4.5", "tier": "basic",
     "description": "가장 빠르고 경제적"},
    {"id": "claude-sonnet-4-5-20250929", "name": "Claude Sonnet 4.5", "tier": "standard",
     "description": "속도와 성능의 균형 (권장)"},
    {"id": "claude-sonnet-4-20250514", "name": "Claude Sonnet 4", "tier": "standard",
     "description": "안정적인 표준 모델"},
    {"id": "claude-opus-4-20250514", "name": "Claude Opus 4", "tier": "premium",
     "description": "복잡한 분석에 강력"},
    {"id": "claude-opus-4-5-20251101", "name": "Claude Opus 4.5", "tier": "premium",
     "description": "최상위 성능"},
    {"id": "claude-opus-4-6-20260320", "name": "Claude Opus 4.6", "tier": "premium",
     "description": "최신 최상위 모델"},
]

SA_MODELS = [
    {"id": "claude-sonnet", "name": "Claude Sonnet 4.5", "tier": "standard"},
    {"id": "claude-haiku", "name": "Claude Haiku 4.5", "tier": "basic"},
    {"id": "gpt-4o", "name": "GPT-4o", "tier": "premium"},
    {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "tier": "basic"},
    {"id": "gemini-flash", "name": "Gemini 2.0 Flash", "tier": "basic"},
]

# 기능별 LLM 설정 정의
LLM_FEATURES = [
    {
        "key": "llm_order_main",
        "label": "발주서 분석 (메인)",
        "description": "발주서 이미지/텍스트에서 품목을 추출하는 주요 모델",
        "models": CLAUDE_MODELS,
        "default": "claude-sonnet-4-5-20250929",
        "config_attr": "CLAUDE_MODEL",
    },
    {
        "key": "llm_order_light",
        "label": "발주서 분석 (경량)",
        "description": "간단한 발주서를 빠르게 처리하는 경량 모델",
        "models": CLAUDE_MODELS,
        "default": "claude-haiku-4-5-20251001",
        "config_attr": "CLAUDE_MODEL_LIGHT",
    },
    {
        "key": "llm_aicc",
        "label": "AI 상담 (AICC)",
        "description": "고객 상담 응답 생성에 사용하는 모델",
        "models": CLAUDE_MODELS,
        "default": "claude-sonnet-4-20250514",
        "config_attr": None,
    },
    {
        "key": "llm_sa_default",
        "label": "Super Agent (기본)",
        "description": "Super Agent 추론/문서작성/코드 등 기본 작업 모델",
        "models": SA_MODELS,
        "default": "claude-sonnet",
        "config_attr": None,
    },
    {
        "key": "llm_sa_fast",
        "label": "Super Agent (빠른)",
        "description": "Super Agent 분류/간단 작업용 빠른 모델",
        "models": SA_MODELS,
        "default": "claude-haiku",
        "config_attr": None,
    },
]

# 레거시 호환
AVAILABLE_MODELS = CLAUDE_MODELS


# ─────────────────────────────────────────
#  설정 테이블 초기화
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


def _upsert_setting(conn, key: str, value: str):
    conn.execute("""
        INSERT INTO app_settings(key, value, updated_at)
        VALUES(?, ?, datetime('now','localtime'))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, value))


def get_llm_setting(key: str, default: str = "") -> str:
    """DB에서 LLM 설정값 조회 (다른 모듈에서도 사용)"""
    try:
        ensure_settings_table()
        conn = get_connection()
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return default


# ─────────────────────────────────────────
#  관리자 비밀번호
# ─────────────────────────────────────────
_DEFAULT_ADMIN_PW = "admin"


def _get_admin_hash() -> str:
    """DB에서 관리자 비밀번호 해시 조회 (없으면 기본값 저장 후 반환)"""
    ensure_settings_table()
    conn = get_connection()
    row = conn.execute("SELECT value FROM app_settings WHERE key='admin_password_hash'").fetchone()
    if row:
        conn.close()
        return row["value"]
    # 초기 비밀번호 설정
    hashed = hash_password(_DEFAULT_ADMIN_PW)
    _upsert_setting(conn, "admin_password_hash", hashed)
    conn.commit()
    conn.close()
    return hashed


class AdminVerifyRequest(BaseModel):
    password: str


class AdminPasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str


@router.post("/admin-verify")
async def admin_verify(req: AdminVerifyRequest, user: dict = Depends(get_current_user)):
    """관리자 비밀번호 검증 (설정 페이지 접근용)"""
    stored_hash = _get_admin_hash()
    if not verify_password(req.password, stored_hash):
        raise HTTPException(403, "관리자 비밀번호가 올바르지 않습니다")
    return {"success": True, "message": "관리자 인증 성공"}


@router.post("/admin-password")
async def change_admin_password(req: AdminPasswordChangeRequest, user: dict = Depends(get_current_user)):
    """관리자 비밀번호 변경"""
    stored_hash = _get_admin_hash()
    if not verify_password(req.current_password, stored_hash):
        raise HTTPException(403, "현재 관리자 비밀번호가 올바르지 않습니다")

    new_hash = hash_password(req.new_password)
    conn = get_connection()
    _upsert_setting(conn, "admin_password_hash", new_hash)
    conn.commit()
    conn.close()

    logger.info(f"[설정] 관리자 비밀번호 변경 by {user['emp_cd']}")
    return {"success": True, "message": "관리자 비밀번호가 변경되었습니다"}


# ─────────────────────────────────────────
#  LLM 통합 설정 API
# ─────────────────────────────────────────
class LLMConfigRequest(BaseModel):
    settings: Dict[str, str]  # {"llm_order_main": "claude-sonnet-4-5-20250929", ...}


@router.get("/llm-config")
async def get_llm_config(user: dict = Depends(get_current_user)):
    """전체 기능별 LLM 설정 조회"""
    ensure_settings_table()
    conn = get_connection()
    rows = conn.execute("SELECT key, value FROM app_settings WHERE key LIKE 'llm_%'").fetchall()
    conn.close()

    db_settings = {r["key"]: r["value"] for r in rows}

    features = []
    for feat in LLM_FEATURES:
        current = db_settings.get(feat["key"])
        if not current:
            # DB에 없으면 config 또는 기본값
            if feat["config_attr"] and hasattr(config, feat["config_attr"]):
                current = getattr(config, feat["config_attr"])
            else:
                current = feat["default"]
        features.append({
            "key": feat["key"],
            "label": feat["label"],
            "description": feat["description"],
            "models": feat["models"],
            "current": current,
            "default": feat["default"],
        })

    return {"features": features}


@router.post("/llm-config")
async def set_llm_config(req: LLMConfigRequest, user: dict = Depends(get_current_user)):
    """기능별 LLM 모델 일괄 변경"""
    ensure_settings_table()
    conn = get_connection()

    changed = []
    for feat in LLM_FEATURES:
        key = feat["key"]
        if key in req.settings:
            model_id = req.settings[key]
            valid_ids = {m["id"] for m in feat["models"]}
            if model_id not in valid_ids:
                conn.close()
                raise HTTPException(400, f"'{feat['label']}'에 유효하지 않은 모델: {model_id}")
            _upsert_setting(conn, key, model_id)
            changed.append(key)

            # 런타임 config 업데이트
            if feat["config_attr"] and hasattr(config, feat["config_attr"]):
                setattr(config, feat["config_attr"], model_id)

    conn.commit()
    conn.close()

    logger.info(f"[설정] LLM 설정 변경: {changed} by {user['emp_cd']}")
    return {"success": True, "changed": changed, "message": f"{len(changed)}개 모델 설정이 변경되었습니다"}


# ─────────────────────────────────────────
#  레거시 호환 API (기존 모델 조회/변경)
# ─────────────────────────────────────────
class ModelSettingRequest(BaseModel):
    model_id: str


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


@router.post("/models")
async def set_model(req: ModelSettingRequest, user: dict = Depends(get_current_user)):
    """Claude 모델 변경 (레거시)"""
    valid_ids = {m["id"] for m in AVAILABLE_MODELS}
    if req.model_id not in valid_ids:
        raise HTTPException(400, f"유효하지 않은 모델: {req.model_id}")

    ensure_settings_table()
    conn = get_connection()
    _upsert_setting(conn, "claude_model", req.model_id)
    _upsert_setting(conn, "llm_order_main", req.model_id)
    conn.commit()
    conn.close()

    config.CLAUDE_MODEL = req.model_id

    model_info = next((m for m in AVAILABLE_MODELS if m["id"] == req.model_id), None)
    logger.info(f"[설정] Claude 모델 변경: {req.model_id}")
    return {
        "success": True,
        "model_id": req.model_id,
        "model_name": model_info["name"] if model_info else req.model_id,
        "message": f"모델이 {model_info['name']}(으)로 변경되었습니다." if model_info else "모델 변경 완료",
    }


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
