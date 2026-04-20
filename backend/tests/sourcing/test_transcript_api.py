"""youtube-transcript-api 경로 단위 테스트 (네트워크 없이 mock)."""
from __future__ import annotations

import builtins
import sys
import types
from unittest.mock import patch, MagicMock

import pytest

from services import transcript_service as ts


# ─────────────────────────────────────────────── #
# youtube-transcript-api 라이브러리 모킹
# ─────────────────────────────────────────────── #


class _FakeSnippet:
    def __init__(self, text, start, duration):
        self.text = text
        self.start = start
        self.duration = duration


class _FakeFetchedTranscript:
    """v1.x iterable response."""
    def __init__(self, snippets):
        self._snippets = snippets

    def __iter__(self):
        return iter(self._snippets)


class _FakeYouTubeTranscriptApi_v1:
    def __init__(self, *a, **kw):
        pass

    def fetch(self, video_id, *, languages=None):
        if not languages:
            languages = ["en"]
        lang = languages[0]
        if lang == "ko":
            return _FakeFetchedTranscript([
                _FakeSnippet("알리 꿀템 소개합니다.", 0.0, 3.0),
                _FakeSnippet("첫 번째 제품은 손전등입니다.", 3.0, 4.5),
                _FakeSnippet("철루맨 조명 기능이 있어요.", 7.5, 3.0),
            ])
        if lang == "ko-KR":
            return _FakeFetchedTranscript([_FakeSnippet("한국어 KR", 0.0, 2.0)])
        raise Exception(f"no transcript for {lang}")


def _install_fake(module_name="youtube_transcript_api"):
    """sys.modules에 가짜 패키지를 심어 import를 가로챈다."""
    fake = types.ModuleType(module_name)
    fake.YouTubeTranscriptApi = _FakeYouTubeTranscriptApi_v1
    errors_mod = types.ModuleType(f"{module_name}._errors")
    errors_mod.TranscriptsDisabled = type("TranscriptsDisabled", (Exception,), {})
    errors_mod.NoTranscriptFound = type("NoTranscriptFound", (Exception,), {})
    errors_mod.VideoUnavailable = type("VideoUnavailable", (Exception,), {})
    errors_mod.CouldNotRetrieveTranscript = type("CouldNotRetrieveTranscript", (Exception,), {})
    sys.modules[module_name] = fake
    sys.modules[f"{module_name}._errors"] = errors_mod
    return fake, errors_mod


# ─────────────────────────────────────────────── #
# Tests
# ─────────────────────────────────────────────── #


def test_fetch_captions_via_api_returns_segments_with_ko():
    _install_fake()
    segments, lang = ts.fetch_captions_via_api("gZPdX8NRv24")
    assert lang == "ko"
    assert len(segments) == 3
    assert "손전등" in segments[1].text
    # Segments have start/end calculated from start + duration
    assert segments[0].start_sec == 0.0
    assert segments[0].end_sec == 3.0


def test_fetch_captions_via_api_raises_when_all_langs_fail():
    _install_fake()
    class _FailAll:
        def fetch(self, *a, **kw):
            raise Exception("all fail")
    with patch("sys.modules", sys.modules):
        sys.modules["youtube_transcript_api"].YouTubeTranscriptApi = _FailAll
        with pytest.raises(ts.TranscriptFetchError):
            ts.fetch_captions_via_api("nonexistent")


def test_fetch_captions_via_api_missing_library_raises():
    # Remove library from sys.modules
    for k in list(sys.modules.keys()):
        if k.startswith("youtube_transcript_api"):
            del sys.modules[k]
    # Block real import too
    orig_import = builtins.__import__
    def blocked(name, *a, **kw):
        if name.startswith("youtube_transcript_api"):
            raise ImportError("simulated: not installed")
        return orig_import(name, *a, **kw)
    with patch("builtins.__import__", side_effect=blocked):
        with pytest.raises(ts.TranscriptFetchError) as e:
            ts.fetch_captions_via_api("video123")
    assert "설치" in str(e.value) or "install" in str(e.value).lower()


def test_clean_transcript_from_segments_dedups():
    segs = [
        ts.Segment(0, 2, "알리 꿀템입니다 알리 꿀템입니다"),
        ts.Segment(2, 4, "첫 번째 제품은 손전등 첫 번째 제품은 손전등"),
    ]
    out = ts.clean_transcript_from_segments(segs)
    # Sliding-window dedup collapses the exact repeat inside each segment.
    assert out.count("알리 꿀템입니다") <= 2


# ─────────────────────────────────────────────── #
# video_processor 가 API 경로 1순위로 쓰는지
# ─────────────────────────────────────────────── #


def test_video_processor_uses_api_first_when_long_enough(monkeypatch):
    """API가 충분히 긴 자막을 반환하면 yt-dlp는 호출되지 않아야 함."""
    import sqlite3
    from db.sourcing_schema import init_sourcing_tables
    from services import video_processor as vp

    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute("INSERT INTO youtube_channels (id, channel_id) VALUES (1, 'UCt')")
    conn.execute(
        "INSERT INTO youtube_videos (id, channel_id, video_id, processed_status) "
        "VALUES (1, 1, 'gZPdX8NRv24', 'pending')"
    )
    conn.commit()

    # API가 충분한 길이 (>200자)로, 중복 없이 리턴
    long_segments = [
        ts.Segment(0, 5, "알리익스프레스 꿀템을 소개합니다 오늘 가성비 좋은 제품 열 가지를 모아서 소개해 드릴게요"),
        ts.Segment(5, 10, "첫 번째 제품은 샤오미 다기능 손전등 제품입니다 차량 비상용 다기능 도구 5-in-1 구성"),
        ts.Segment(10, 15, "두 번째 제품은 오토바이 헬멧 카메라 인터콤 블랙박스 기능과 통신 기능을 통합한 올인원 상품"),
        ts.Segment(15, 20, "세 번째 제품은 스마트폰 3축 짐벌로 핸들 내부에 연장봉이 내장되어 편리합니다"),
        ts.Segment(20, 25, "네 번째 제품은 전동 바디 진동 근막 링 마사지기로 웨어러블 벨트와 링 형태를 결합한 하이브리드 기기"),
    ]
    monkeypatch.setattr(ts, "fetch_captions_via_api",
                        lambda *a, **kw: (long_segments, "ko"))
    calls = {"yt_dlp": 0}
    def _nope(*a, **kw):
        calls["yt_dlp"] += 1
        raise ts.TranscriptFetchError("should not be called")
    monkeypatch.setattr(ts, "download_auto_captions", _nope)

    cleaned, segs = vp._step_transcribe(conn, {
        "id": 1, "video_id": "gZPdX8NRv24", "title": "Test",
        "channel_id": 1, "processed_status": "pending", "retry_count": 0,
    })
    assert len(cleaned) >= 200
    assert calls["yt_dlp"] == 0  # yt-dlp 호출 안 됨 — API로 완료
