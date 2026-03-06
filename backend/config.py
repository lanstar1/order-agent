"""
설정 관리 - 환경변수 또는 config.json 으로 관리
"""
import os
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

# ─────────────────────────────────────────
#  Google API (Drive 파일 목록 조회용)
# ─────────────────────────────────────────
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

# ─────────────────────────────────────────
#  ECOUNT ERP
# ─────────────────────────────────────────
ERP_COM_CODE  = os.getenv("ERP_COM_CODE",  "89356")
ERP_USER_ID   = os.getenv("ERP_USER_ID",   "TIGER")
ERP_ZONE      = os.getenv("ERP_ZONE",      "CD")
ERP_API_KEY   = os.getenv("ERP_API_KEY",   "1d667e0cff97845728acdeed64e34ce789")
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
DEBUG = os.getenv("DEBUG", "true").lower() == "true"

# ─────────────────────────────────────────
#  파일 업로드
# ─────────────────────────────────────────
UPLOAD_DIR      = BASE_DIR / "data" / "uploads"
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10MB

# ─────────────────────────────────────────
#  AI 파라미터
# ─────────────────────────────────────────
CONFIDENCE_THRESHOLD = 0.90   # 이 값 이상이면 자동처리(STP)
TOP_K_RESULTS        = 5      # RAG 검색 상위 후보 수
