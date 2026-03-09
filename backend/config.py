"""
설정 관리 - 환경변수 또는 config.json 으로 관리
"""
import os
import hashlib
import secrets
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent

# .env 파일 로딩 (프로젝트 루트)
_env_path = BASE_DIR / ".env"
if _env_path.exists():
    load_dotenv(_env_path)


# ─────────────────────────────────────────
#  Claude API
# ─────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL       = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")
CLAUDE_MODEL_LIGHT = os.getenv("CLAUDE_MODEL_LIGHT", "claude-haiku-4-5-20251001")  # 간단한 작업용

# ─────────────────────────────────────────
#  Google API (Drive 파일 목록 조회용)
# ─────────────────────────────────────────
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

# ─────────────────────────────────────────
#  ECOUNT ERP
# ─────────────────────────────────────────
ERP_COM_CODE  = os.getenv("ERP_COM_CODE",  "")
ERP_USER_ID   = os.getenv("ERP_USER_ID",   "")
ERP_ZONE      = os.getenv("ERP_ZONE",      "CD")
ERP_API_KEY   = os.getenv("ERP_API_KEY",   "")
ERP_WH_CD     = os.getenv("ERP_WH_CD",     "10")   # 기본 창고코드
ERP_EMP_CD    = os.getenv("ERP_EMP_CD",   "")    # 담당자 코드 (ERP에서 필수 설정된 경우)

# ─────────────────────────────────────────
#  데이터베이스
# ─────────────────────────────────────────
DB_PATH        = BASE_DIR / "data" / "order_agent.db"
CHROMA_PATH    = BASE_DIR / "data" / "chroma_db"
PRODUCTS_CSV   = BASE_DIR / "data" / "products" / "products.csv"

# ─────────────────────────────────────────
#  서버
# ─────────────────────────────────────────
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# ─────────────────────────────────────────
#  파일 업로드
# ─────────────────────────────────────────
UPLOAD_DIR      = BASE_DIR / "data" / "uploads"
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10MB

# ─────────────────────────────────────────
#  AI 파라미터
# ─────────────────────────────────────────
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.90"))
TOP_K_RESULTS        = 5      # RAG 검색 상위 후보 수

# ─────────────────────────────────────────
#  JWT 인증
# ─────────────────────────────────────────
# JWT_SECRET_KEY: 프로덕션에서는 반드시 환경변수로 설정할 것!
# 미설정 시 ERP_COM_CODE 기반 결정론적 키 생성 (서버 재시작 시 토큰 유지)
_jwt_fallback = hashlib.sha256(
    f"order-agent-jwt-{os.getenv('ERP_COM_CODE', 'dev')}".encode()
).hexdigest()
JWT_SECRET_KEY  = os.getenv("JWT_SECRET_KEY", _jwt_fallback)
JWT_ALGORITHM   = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "24"))

# ─────────────────────────────────────────
#  CORS 허용 도메인
# ─────────────────────────────────────────
ALLOWED_ORIGINS = [
    o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()
] or ["*"]  # 환경변수 미설정 시 개발 모드 허용

# ─────────────────────────────────────────
#  Rate Limiting
# ─────────────────────────────────────────
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))
