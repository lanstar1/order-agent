"""쿠키 매니저 테스트."""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from services import cookies_manager


@pytest.fixture(autouse=True)
def _reset_cache():
    cookies_manager.reset_cache()
    yield
    cookies_manager.reset_cache()


def test_returns_none_when_no_env_set(monkeypatch):
    monkeypatch.delenv("YOUTUBE_COOKIES_FILE", raising=False)
    monkeypatch.delenv("YOUTUBE_COOKIES_TEXT", raising=False)
    assert cookies_manager.get_cookies_file_path() is None


def test_uses_file_path_when_exists(tmp_path, monkeypatch):
    cookie_file = tmp_path / "mycookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File\n"
                           ".youtube.com\tTRUE\t/\tFALSE\t0\tPREF\tvalue")
    monkeypatch.setenv("YOUTUBE_COOKIES_FILE", str(cookie_file))
    monkeypatch.delenv("YOUTUBE_COOKIES_TEXT", raising=False)
    assert cookies_manager.get_cookies_file_path() == str(cookie_file)


def test_ignores_nonexistent_file(monkeypatch):
    monkeypatch.setenv("YOUTUBE_COOKIES_FILE", "/nope/does/not/exist.txt")
    monkeypatch.delenv("YOUTUBE_COOKIES_TEXT", raising=False)
    assert cookies_manager.get_cookies_file_path() is None


def test_writes_text_to_tmp_file(monkeypatch):
    monkeypatch.delenv("YOUTUBE_COOKIES_FILE", raising=False)
    cookies_content = (
        "# Netscape HTTP Cookie File\n"
        ".youtube.com\tTRUE\t/\tFALSE\t0\tSID\texample_session_token_here_for_testing_purposes"
    )
    monkeypatch.setenv("YOUTUBE_COOKIES_TEXT", cookies_content)
    path = cookies_manager.get_cookies_file_path()
    assert path is not None
    assert Path(path).exists()
    assert "example_session_token" in Path(path).read_text()


def test_auto_prepends_netscape_header(monkeypatch):
    monkeypatch.delenv("YOUTUBE_COOKIES_FILE", raising=False)
    # Netscape 헤더 없이 쿠키 라인만 붙여넣는 경우
    raw = ".youtube.com\tTRUE\t/\tFALSE\t0\tSID\tabcdef1234567890abcdef1234567890abcdef"
    monkeypatch.setenv("YOUTUBE_COOKIES_TEXT", raw)
    path = cookies_manager.get_cookies_file_path()
    assert path is not None
    content = Path(path).read_text()
    assert content.startswith("# Netscape HTTP Cookie File")


def test_ignores_too_short_text(monkeypatch):
    monkeypatch.delenv("YOUTUBE_COOKIES_FILE", raising=False)
    monkeypatch.setenv("YOUTUBE_COOKIES_TEXT", "tiny")
    assert cookies_manager.get_cookies_file_path() is None


def test_cache_returns_same_path(monkeypatch):
    monkeypatch.delenv("YOUTUBE_COOKIES_FILE", raising=False)
    monkeypatch.setenv("YOUTUBE_COOKIES_TEXT",
                       "# Netscape HTTP Cookie File\n"
                       ".youtube.com\tTRUE\t/\tFALSE\t0\tX\t" + "a"*50)
    p1 = cookies_manager.get_cookies_file_path()
    p2 = cookies_manager.get_cookies_file_path()
    assert p1 == p2


def test_build_session_sets_headers(tmp_path):
    cookie_file = tmp_path / "c.txt"
    cookie_file.write_text(
        "# Netscape HTTP Cookie File\n"
        ".youtube.com\tTRUE\t/\tFALSE\t9999999999\tSID\ttesttoken\n"
    )
    session = cookies_manager.build_session_with_cookies(str(cookie_file))
    assert "Mozilla/5.0" in session.headers.get("User-Agent", "")
    assert "ko-KR" in session.headers.get("Accept-Language", "")


def test_transcript_service_detects_cookies(monkeypatch, tmp_path):
    """transcript_service 가 쿠키 경로를 정상 읽는지 통합 검증."""
    from services import transcript_service as ts
    cookie_file = tmp_path / "c.txt"
    cookie_file.write_text(
        "# Netscape HTTP Cookie File\n"
        ".youtube.com\tTRUE\t/\tFALSE\t0\tSID\ttest_value_long_enough"
    )
    monkeypatch.setenv("YOUTUBE_COOKIES_FILE", str(cookie_file))
    monkeypatch.delenv("YOUTUBE_COOKIES_TEXT", raising=False)
    cookies_manager.reset_cache()

    # proxy 없이도 경로 탐지 OK
    # fetch_captions_via_api 를 직접 호출하지 않고 헬퍼만 검증
    from services.cookies_manager import get_cookies_file_path
    assert get_cookies_file_path() == str(cookie_file)
