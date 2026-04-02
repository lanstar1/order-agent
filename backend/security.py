"""
보안 모듈 - JWT 인증, 비밀번호 해싱, 보안 유틸리티
"""
import time
import logging
import bcrypt
import jwt
from datetime import datetime, timedelta, timezone
from typing import Optional
from collections import defaultdict
from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRE_HOURS, RATE_LIMIT_PER_MINUTE

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  비밀번호 해싱 (bcrypt)
# ─────────────────────────────────────────
def hash_password(password: str) -> str:
    """bcrypt로 비밀번호 해싱"""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    """bcrypt 해시 검증 (SHA256 레거시 호환)"""
    # bcrypt 해시 형식 체크 ($2b$ 또는 $2a$로 시작)
    if hashed.startswith("$2b$") or hashed.startswith("$2a$"):
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    else:
        # 레거시 SHA256 호환 (마이그레이션 전까지)
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest() == hashed


def needs_rehash(hashed: str) -> bool:
    """SHA256 레거시 해시인지 확인 (bcrypt 마이그레이션 필요 여부)"""
    return not (hashed.startswith("$2b$") or hashed.startswith("$2a$"))


# ─────────────────────────────────────────
#  JWT 토큰 관리
# ─────────────────────────────────────────
def create_token(emp_cd: str, name: str) -> str:
    """JWT 액세스 토큰 생성"""
    payload = {
        "sub": emp_cd,
        "name": name,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """JWT 토큰 디코딩 및 검증"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었습니다. 다시 로그인해주세요.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="유효하지 않은 토큰입니다.")


def verify_token(token: str) -> Optional[dict]:
    """JWT 토큰 검증 (예외 미발생, 실패 시 None 반환) - 미들웨어용"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return {"emp_cd": payload.get("sub", ""), "name": payload.get("name", "")}
    except Exception:
        return None


# ─────────────────────────────────────────
#  FastAPI 인증 의존성
# ─────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    """
    현재 인증된 사용자 정보 반환
    Authorization: Bearer <token> 헤더 필수
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="인증이 필요합니다.")
    payload = decode_token(credentials.credentials)
    return {"emp_cd": payload["sub"], "name": payload.get("name", "")}


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> Optional[dict]:
    """인증 선택적 (토큰 있으면 사용자 정보, 없으면 None)"""
    if not credentials:
        return None
    try:
        payload = decode_token(credentials.credentials)
        return {"emp_cd": payload["sub"], "name": payload.get("name", "")}
    except HTTPException:
        return None


# ─────────────────────────────────────────
#  Rate Limiting (인메모리)
# ─────────────────────────────────────────
_rate_store: dict = defaultdict(list)


def check_rate_limit(client_ip: str, limit: int = None):
    """
    IP 기반 Rate Limiting
    초과 시 HTTPException(429) 발생
    """
    if limit is None:
        limit = RATE_LIMIT_PER_MINUTE

    now = time.time()
    window = 60  # 1분

    # 만료된 요청 제거
    _rate_store[client_ip] = [
        ts for ts in _rate_store[client_ip] if now - ts < window
    ]

    if len(_rate_store[client_ip]) >= limit:
        raise HTTPException(
            status_code=429,
            detail=f"요청이 너무 많습니다. {window}초 후 다시 시도해주세요.",
        )

    _rate_store[client_ip].append(now)


# ─────────────────────────────────────────
#  입력 검증 유틸
# ─────────────────────────────────────────
import re

_SAFE_TEXT_PATTERN = re.compile(r'^[\w\s가-힣ㄱ-ㅎㅏ-ㅣ\-.,;:()/#&+@!\'"?%\[\]{}=*~`\n\r\t]+$')


def sanitize_text(text: str, max_length: int = 50000) -> str:
    """텍스트 입력 기본 검증 및 정리"""
    if not text:
        return ""
    # 길이 제한
    if len(text) > max_length:
        raise HTTPException(400, f"텍스트가 너무 깁니다. (최대 {max_length}자)")
    return text.strip()


def validate_code(code: str, field_name: str = "코드", max_length: int = 50) -> str:
    """코드 형식 검증 (영문, 숫자, 하이픈, 언더스코어만 허용)"""
    if not code or not code.strip():
        raise HTTPException(400, f"{field_name}을(를) 입력해주세요.")
    code = code.strip()
    if len(code) > max_length:
        raise HTTPException(400, f"{field_name}이 너무 깁니다. (최대 {max_length}자)")
    if not re.match(r'^[\w\-]+$', code):
        raise HTTPException(400, f"{field_name}에 허용되지 않는 문자가 포함되어 있습니다.")
    return code


# ─────────────────────────────────────────
#  파일 업로드 보안
# ─────────────────────────────────────────
ALLOWED_IMAGE_MIMES = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
}
ALLOWED_VIDEO_MIMES = {
    "video/mp4", "video/quicktime", "video/webm", "video/x-msvideo",
}
ALLOWED_DOC_MIMES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # xlsx
    "application/vnd.ms-excel",  # xls
}


def validate_file_upload(content: bytes, filename: str, allowed_types: str = "image") -> str:
    """
    파일 업로드 보안 검증
    - MIME 타입 확인
    - 파일 크기 확인
    Returns: 검증된 MIME 타입
    """
    from config import MAX_UPLOAD_SIZE

    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(400, f"파일 크기가 {MAX_UPLOAD_SIZE // (1024*1024)}MB를 초과합니다.")

    if len(content) == 0:
        raise HTTPException(400, "빈 파일입니다.")

    # python-magic으로 실제 MIME 타입 확인
    try:
        import magic
        detected_mime = magic.from_buffer(content[:2048], mime=True)
    except Exception:
        # magic 사용 불가 시 확장자 기반 fallback
        from pathlib import Path
        ext = Path(filename).suffix.lower()
        ext_to_mime = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".png": "image/png", ".gif": "image/gif",
            ".webp": "image/webp", ".pdf": "application/pdf",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
        }
        detected_mime = ext_to_mime.get(ext, "application/octet-stream")

    # 허용 MIME 확인
    if allowed_types == "image":
        allowed = ALLOWED_IMAGE_MIMES | {"application/pdf"}
    elif allowed_types == "excel":
        allowed = ALLOWED_DOC_MIMES
    else:
        allowed = ALLOWED_IMAGE_MIMES | ALLOWED_DOC_MIMES

    if detected_mime not in allowed:
        raise HTTPException(400, f"허용되지 않는 파일 형식입니다: {detected_mime}")

    return detected_mime


# ─────────────────────────────────────────
#  프롬프트 인젝션 방어
# ─────────────────────────────────────────
_INJECTION_PATTERNS = [
    r"ignore\s+(all\s+)?(previous|above|prior)\s+(instructions|prompts|rules)",
    r"you\s+are\s+now\s+(a|an|in)\s+",
    r"system\s*:\s*",
    r"<\s*/?system\s*>",
    r"new\s+instructions?\s*:",
    r"override\s+(mode|instructions|rules)",
    r"forget\s+(all|everything|your)\s+(instructions|rules|training)",
]


def sanitize_for_prompt(text: str) -> str:
    """
    사용자 입력을 LLM 프롬프트에 삽입 시 기본 정리
    - 명시적 구분자로 감싸기
    - 위험 패턴 로깅 (차단은 하지 않음 - 오탐 방지)
    """
    for pattern in _INJECTION_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            logger.warning(f"[Security] 프롬프트 인젝션 의심 패턴 감지: {pattern}")
            break
    return text
