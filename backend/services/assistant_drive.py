"""상담봇 ↔ 구글 드라이브 연결 (서비스 계정 토큰).

`cert_lookup.drive` 는 표준 라이브러리만 쓰도록 만들어져 있어 서비스 계정 JWT 를 직접
서명하지 않는다. 대신 토큰 공급자를 주입받는 훅을 열어 두었고, 이 모듈이 그 훅을 채운다.

토큰 발급 로직은 `services.google_drive_service` 와 같은 방식(JWT → OAuth2 교환)이지만
그쪽은 async httpx 라 조회 엔진(동기)에서 바로 못 쓴다. 여기서는 urllib 로 동기 구현한다.

자격증명이 없으면 아무것도 하지 않는다 — `cert_lookup.drive` 가 로컬 동기화 폴더나
폴더 링크로 폴백하므로 상담봇은 그대로 동작한다.
"""
import base64
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

#: 인증자료를 읽기만 한다. 업로드용 서비스와 달리 쓰기 권한을 요구하지 않는다.
SCOPE = "https://www.googleapis.com/auth/drive.readonly"
_TOKEN_URL = "https://oauth2.googleapis.com/token"

_lock = threading.Lock()
_cache = {"token": "", "expires_at": 0.0}


def _sa_info():
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        return None
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("[상담봇] GOOGLE_SERVICE_ACCOUNT_JSON 파싱 실패 — 드라이브 링크 없이 동작합니다")
        return None
    if not info.get("client_email") or not info.get("private_key"):
        logger.warning("[상담봇] 서비스 계정 JSON 에 client_email/private_key 가 없습니다")
        return None
    return info


def configured() -> bool:
    return _sa_info() is not None


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def get_token() -> str:
    """액세스 토큰(캐시). 실패는 전부 빈 문자열 — 호출측이 폴백한다."""
    now = time.time()
    with _lock:
        if _cache["token"] and _cache["expires_at"] > now + 60:
            return _cache["token"]
    info = _sa_info()
    if info is None:
        return ""
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        issued = int(now)
        header = _b64(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
        claim = _b64(json.dumps({
            "iss": info["client_email"],
            "scope": SCOPE,
            "aud": _TOKEN_URL,
            "iat": issued,
            "exp": issued + 3600,
        }).encode())
        key = serialization.load_pem_private_key(info["private_key"].encode(), password=None)
        sig = key.sign(f"{header}.{claim}".encode(), padding.PKCS1v15(), hashes.SHA256())
        assertion = f"{header}.{claim}.{_b64(sig)}"

        body = urllib.parse.urlencode({
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }).encode()
        req = urllib.request.Request(_TOKEN_URL, data=body,
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            payload = json.loads(r.read().decode("utf-8"))
        token = payload.get("access_token") or ""
        if not token:
            return ""
        with _lock:
            _cache["token"] = token
            _cache["expires_at"] = now + float(payload.get("expires_in") or 3600)
        return token
    except Exception as exc:                      # noqa: BLE001 - 폴백이 정상 경로
        logger.warning("[상담봇] 드라이브 토큰 발급 실패 (%s: %s) — 폴더 링크로 폴백합니다",
                       type(exc).__name__, exc)
        return ""


_wired = False


def setup() -> bool:
    """cert_lookup.drive 에 토큰 공급자를 등록한다. 프로세스당 한 번이면 된다."""
    global _wired
    if _wired:
        return configured()
    from cert_lookup import drive as cert_drive

    cert_drive.set_token_provider(get_token)
    _wired = True
    if configured():
        logger.info("[상담봇] 드라이브 서비스 계정 연결됨 (%s)", _sa_info()["client_email"])
    else:
        logger.info("[상담봇] 드라이브 서비스 계정 미설정 — 폴더 링크로 안내합니다")
    return configured()


def status() -> dict:
    """진단용. 키 값 자체는 절대 내보내지 않는다."""
    info = _sa_info()
    from cert_lookup import drive as cert_drive

    out = {
        "service_account_configured": info is not None,
        "client_email": info.get("client_email") if info else None,
        "scope": SCOPE,
        "wired": _wired,
    }
    out.update(cert_drive.get_resolver().auth_status())
    return out
