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
#  Google Service Account (Drive 파일 업로드용)
# ─────────────────────────────────────────
# JSON 키 파일 내용을 환경변수로 전달 (Render에서는 Secret File 또는 환경변수)
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
# CS 파일 업로드용 Drive 폴더 ID
GOOGLE_CS_FOLDER_ID = os.getenv("GOOGLE_CS_FOLDER_ID", "")

# ─────────────────────────────────────────
#  ECOUNT ERP
# ─────────────────────────────────────────
ERP_COM_CODE  = os.getenv("ERP_COM_CODE",  "")
ERP_USER_ID   = os.getenv("ERP_USER_ID",   "")
ERP_ZONE      = os.getenv("ERP_ZONE",      "CD")
ERP_API_KEY   = os.getenv("ERP_API_KEY",   "")
ERP_WH_CD     = os.getenv("ERP_WH_CD",     "10")   # 기본 창고코드
ERP_EMP_CD    = os.getenv("ERP_EMP_CD",   "")    # 담당자 코드 (ERP에서 필수 설정된 경우)

# ECOUNT 웹 로그인 (구매/판매현황 조회용 - OAPI에 없는 기능)
ERP_WEB_USER_ID = os.getenv("ERP_WEB_USER_ID", "")   # 웹 로그인 ID
ERP_WEB_USER_PW = os.getenv("ERP_WEB_USER_PW", "")   # 웹 로그인 PW

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
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB (영상 파일 업로드 지원)

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

# ─────────────────────────────────────────
#  네이버 검색/데이터랩 API (MAP 감시 + 트렌드 분석)
# ─────────────────────────────────────────
NAVER_SEARCH_ID     = os.getenv("NAVER_SEARCH_ID", "")
NAVER_SEARCH_SECRET = os.getenv("NAVER_SEARCH_SECRET", "")

# ─────────────────────────────────────────
#  네이버 커머스 API (스마트스토어)
# ─────────────────────────────────────────
NAVER_CLIENT_ID     = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")
NAVER_COMMERCE_URL  = os.getenv("NAVER_COMMERCE_URL", "https://api.commerce.naver.com")

# 스마트스토어 ERP 연동 고정값
SMARTSTORE_CUST_CODE = os.getenv("SMARTSTORE_CUST_CODE", "")   # 스마트스토어 거래처코드
SMARTSTORE_EMP_CODE  = os.getenv("SMARTSTORE_EMP_CODE", "")    # 담당자 코드
SMARTSTORE_WH_CODE   = os.getenv("SMARTSTORE_WH_CODE", "30")   # 출하창고 코드

# iLogen 택배 계정 (창고별)
LOGEN_GIMPO_ID       = os.getenv("LOGEN_GIMPO_ID", "")
LOGEN_GIMPO_PW       = os.getenv("LOGEN_GIMPO_PW", "")
LOGEN_GIMPO_PW_PREV  = os.getenv("LOGEN_GIMPO_PW_PREV", "")
LOGEN_YONGSAN_ID     = os.getenv("LOGEN_YONGSAN_ID", "")
LOGEN_YONGSAN_PW     = os.getenv("LOGEN_YONGSAN_PW", "")
LOGEN_YONGSAN_PW_PREV = os.getenv("LOGEN_YONGSAN_PW_PREV", "")

# iLogen 보내는분 정보 (창고별)
SENDER_GIMPO_NAME    = os.getenv("SENDER_GIMPO_NAME", "랜스타")
SENDER_GIMPO_TEL     = os.getenv("SENDER_GIMPO_TEL", "")
SENDER_GIMPO_ADDR    = os.getenv("SENDER_GIMPO_ADDR", "")
SENDER_YONGSAN_NAME  = os.getenv("SENDER_YONGSAN_NAME", "랜스타")
SENDER_YONGSAN_TEL   = os.getenv("SENDER_YONGSAN_TEL", "")
SENDER_YONGSAN_ADDR  = os.getenv("SENDER_YONGSAN_ADDR", "")

# 스마트스토어 상품매핑 경로
SMARTSTORE_PRODUCT_MAP_PATH = BASE_DIR / "data" / "smartstore" / "smartstore_product_map.json"
SMARTSTORE_MODEL_MAP_PATH   = BASE_DIR / "data" / "smartstore" / "smartstore_model_map.json"

# ─────────────────────────────────────────
#  바코드 ERP Bridge (쿠팡 PO → 이카운트)
# ─────────────────────────────────────────
# 기본 ERP 인증은 위 ERP_* 변수를 공유하며, 아래는 바코드 전용 고정값
BARCODE_CUST_CODE = os.getenv("BARCODE_CUST_CODE", "202308091")  # 바코드서버 거래처코드
BARCODE_WH_CD     = os.getenv("BARCODE_WH_CD", "30")             # 바코드서버 출하창고
BARCODE_MASTER_PATH = os.getenv("BARCODE_MASTER_PATH", "")       # master_data.xlsx 경로 (빈값이면 data/barcode/master_data.xlsx)

# ─── 선적 메일 (IMAP) ──────────────────────────────────
MAIL_IMAP_SERVER = os.getenv("MAIL_IMAP_SERVER", "wmbox3.ecount.com")
MAIL_IMAP_PORT = int(os.getenv("MAIL_IMAP_PORT", "993"))
MAIL_USER = os.getenv("MAIL_USER", "")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")
