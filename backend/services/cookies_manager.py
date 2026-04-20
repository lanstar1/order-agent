"""YouTube cookies 관리 — IP 차단 우회용.

Render 공용 IP가 YouTube에 차단됐을 때 사용자의 브라우저 쿠키(cookies.txt
Netscape 형식)를 세션에 첨부하면 "이미 로그인한 실제 유저" 로 간주되어
차단을 우회할 수 있다.

두 가지 구성 경로:
1. ``YOUTUBE_COOKIES_FILE`` (파일 경로)
2. ``YOUTUBE_COOKIES_TEXT`` (파일 내용 평문 — Render env var에 붙여넣기)

어느 쪽이든 전역 경로를 반환한다. 한 번 생성된 tmp 파일은 프로세스 생명주기
동안 재사용 (캐시).

보안 주의:
- 쿠키는 세션 탈취 위험이 있음. 전용 YouTube 계정 권장.
- 파일 권한은 owner-only(0600) 으로 저장.
"""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CACHED_PATH: Optional[str] = None


def get_cookies_file_path() -> Optional[str]:
    """사용 가능한 cookies.txt 경로를 반환한다. 없으면 None.

    우선순위:
    1. YOUTUBE_COOKIES_FILE (기존 파일 경로)
    2. YOUTUBE_COOKIES_TEXT (Netscape 형식 평문 — /tmp 에 즉시 기록)
    """
    global _CACHED_PATH
    if _CACHED_PATH and Path(_CACHED_PATH).exists():
        return _CACHED_PATH

    # ① 기존 파일 경로
    file_path = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()
    if file_path and Path(file_path).exists():
        _CACHED_PATH = file_path
        return file_path

    # ② 평문 내용 → 임시 파일
    cookies_text = os.environ.get("YOUTUBE_COOKIES_TEXT", "").strip()
    if cookies_text:
        # cookies.txt Netscape 형식 검증: 첫 줄에 "# Netscape HTTP Cookie File"
        # 또는 공백·탭 구분 필드가 있어야 함. 너무 엄격히 하지 않고 크기만 체크.
        if len(cookies_text) < 50:
            logger.warning("[cookies] YOUTUBE_COOKIES_TEXT 가 너무 짧습니다 — 무시")
            return None
        # 첫 줄이 주석 헤더가 아니면 Netscape 표준 헤더를 자동 추가
        if not cookies_text.lstrip().startswith("#"):
            cookies_text = "# Netscape HTTP Cookie File\n" + cookies_text
        try:
            tmpdir = Path(tempfile.gettempdir()) / "sourcing_cookies"
            tmpdir.mkdir(parents=True, exist_ok=True)
            cookie_file = tmpdir / "youtube_cookies.txt"
            cookie_file.write_text(cookies_text, encoding="utf-8")
            try:
                cookie_file.chmod(0o600)
            except Exception:
                pass
            _CACHED_PATH = str(cookie_file)
            logger.info(f"[cookies] YOUTUBE_COOKIES_TEXT → {cookie_file} 기록 완료")
            return _CACHED_PATH
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[cookies] tmp 파일 생성 실패: {exc}")
            return None

    return None


def build_session_with_cookies(cookies_path: str):
    """requests.Session 에 cookies.txt 를 로드해 반환.

    youtube-transcript-api v1.x 의 ``http_client=`` 인자에 전달하기 위함.
    """
    import requests
    from http.cookiejar import MozillaCookieJar

    session = requests.Session()
    try:
        jar = MozillaCookieJar(cookies_path)
        jar.load(ignore_discard=True, ignore_expires=True)
        session.cookies = jar  # type: ignore[assignment]
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[cookies] MozillaCookieJar 로드 실패: {exc}")
    # YouTube이 일반 브라우저로 간주하도록 User-Agent 설정
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return session


def reset_cache():
    """테스트용 — 캐시 초기화."""
    global _CACHED_PATH
    _CACHED_PATH = None
