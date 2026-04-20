"""Gemini 전사 경로 단위 테스트 (네트워크 없음, mock 기반)."""
from __future__ import annotations

import os
import sys
import types
from unittest.mock import patch, MagicMock

import pytest

from services import gemini_transcript as gt


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    """테스트마다 Google API key 환경을 초기화."""
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_TRANSCRIPT_MODEL", raising=False)
    yield


def _install_fake_genai(text="전사된 샘플 텍스트입니다. 첫 번째 제품은 샤오미 손전등입니다.",
                        usage_in=1000, usage_out=500):
    """google.generativeai 라이브러리를 가짜 모듈로 대체."""
    fake = types.ModuleType("google.generativeai")

    class _FakeUsage:
        prompt_token_count = usage_in
        candidates_token_count = usage_out

    class _FakeResponse:
        def __init__(self, text):
            self.text = text
            self.usage_metadata = _FakeUsage()
            self.candidates = []

    class _FakeModel:
        def __init__(self, name):
            self.name = name
        def generate_content(self, content, **kw):
            return _FakeResponse(text)

    fake.configure = lambda **kw: None
    fake.GenerativeModel = _FakeModel
    # types submodule
    types_mod = types.ModuleType("google.generativeai.types")
    class _FakePart:
        @classmethod
        def from_uri(cls, *, file_uri, mime_type):
            return {"_uri": file_uri, "_mime": mime_type}
    types_mod.Part = _FakePart
    fake.types = types_mod

    # Install into sys.modules so `import google.generativeai` works
    sys.modules["google.generativeai"] = fake
    sys.modules["google.generativeai.types"] = types_mod
    return fake


def test_raises_when_no_api_key():
    with pytest.raises(gt.GeminiTranscriptError) as e:
        gt.fetch_transcript_via_gemini("https://www.youtube.com/watch?v=abc")
    assert "GOOGLE_API_KEY" in str(e.value)


def test_success_returns_text_and_meta(monkeypatch):
    _install_fake_genai(text="실제 전사 결과")
    monkeypatch.setenv("GOOGLE_API_KEY", "AIza_fake_key")
    text, meta = gt.fetch_transcript_via_gemini("https://www.youtube.com/watch?v=gZPdX8NRv24")
    assert "실제 전사 결과" in text
    assert meta["provider"] == "google"
    assert meta["input_tokens"] == 1000
    assert meta["output_tokens"] == 500
    assert meta["latency_ms"] >= 0


def test_uses_custom_model_from_env(monkeypatch):
    _install_fake_genai()
    monkeypatch.setenv("GOOGLE_API_KEY", "k")
    monkeypatch.setenv("GEMINI_TRANSCRIPT_MODEL", "gemini-2.5-pro")
    _, meta = gt.fetch_transcript_via_gemini("https://yt/x")
    assert meta["model"] == "gemini-2.5-pro"


def test_raises_on_empty_response(monkeypatch):
    _install_fake_genai(text="")
    monkeypatch.setenv("GOOGLE_API_KEY", "k")
    with pytest.raises(gt.GeminiTranscriptError) as e:
        gt.fetch_transcript_via_gemini("https://yt/x")
    assert "빈 응답" in str(e.value)


def test_raises_when_sdk_missing(monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "k")
    # Block import of google.generativeai
    for k in list(sys.modules.keys()):
        if k.startswith("google.generativeai"):
            del sys.modules[k]
    import builtins
    orig = builtins.__import__
    def blocked(name, *a, **kw):
        if name.startswith("google.generativeai"):
            raise ImportError("simulated")
        return orig(name, *a, **kw)
    with patch("builtins.__import__", side_effect=blocked):
        with pytest.raises(gt.GeminiTranscriptError) as e:
            gt.fetch_transcript_via_gemini("https://yt/x")
    assert "google-generativeai" in str(e.value) or "설치" in str(e.value)


def test_api_call_error_is_wrapped(monkeypatch):
    _install_fake_genai()
    # Make GenerativeModel raise on generate_content
    class _BrokenModel:
        def __init__(self, *a, **kw): pass
        def generate_content(self, *a, **kw):
            raise RuntimeError("quota exceeded")
    sys.modules["google.generativeai"].GenerativeModel = _BrokenModel
    monkeypatch.setenv("GOOGLE_API_KEY", "k")
    with pytest.raises(gt.GeminiTranscriptError) as e:
        gt.fetch_transcript_via_gemini("https://yt/x")
    assert "quota" in str(e.value).lower()


# ─── video_processor 통합 — Gemini 성공 시 하위 경로 스킵 ──── #


def test_video_processor_uses_gemini_first(monkeypatch):
    """Gemini 가 긴 자막을 돌려주면 transcript-api/yt-dlp 는 호출되지 않아야."""
    import sqlite3
    from db.sourcing_schema import init_sourcing_tables
    from services import video_processor as vp
    from services import transcript_service as ts

    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute("INSERT INTO youtube_channels (id, channel_id) VALUES (1, 'UCt')")
    conn.execute(
        "INSERT INTO youtube_videos (id, channel_id, video_id, duration_seconds, processed_status) "
        "VALUES (1, 1, 'gZPdX8NRv24', 300, 'pending')"
    )
    conn.commit()

    long_text = (
        "알리익스프레스에서 실패없는 꿀템들만 모아서 소개해 드립니다. "
        "오늘도 알리에서 판매량도 높고 한국에서 인기가 많은 가성비 좋은 아이디어 제품들 열 가지를 모아서 소개해 드리니 "
        "좋은 제품들 놓치지 말고 끝까지 시청 부탁드립니다. "
        "첫 번째 제품은 샤오미 다기능 손전등 제품입니다. "
        "단순한 조명 기구의 기능을 넘어 차량용 생존 도구까지 모두 결합된 완벽한 5-in-1 구성을 가지고 있습니다. "
        "두 번째 제품은 오토바이 헬멧 카메라 인터콤입니다. "
        "기존 오토바이 헤드셋의 기능에 고화질 블랙박스 기능까지 완벽하게 결합한 올인원 상품입니다."
    )
    monkeypatch.setattr(gt, "fetch_transcript_via_gemini",
                        lambda url, **kw: (long_text, {
                            "provider": "google", "model": "gemini-2.5-flash",
                            "input_tokens": 1000, "output_tokens": 500,
                            "latency_ms": 2000,
                        }))

    calls = {"api": 0, "yt_dlp": 0}
    def _nope_api(*a, **kw):
        calls["api"] += 1
        raise ts.TranscriptFetchError("should not be called")
    def _nope_yt(*a, **kw):
        calls["yt_dlp"] += 1
        raise ts.TranscriptFetchError("should not be called")
    monkeypatch.setattr(ts, "fetch_captions_via_api", _nope_api)
    monkeypatch.setattr(ts, "download_auto_captions", _nope_yt)

    cleaned, segments = vp._step_transcribe(conn, {
        "id": 1, "video_id": "gZPdX8NRv24", "title": "t",
        "channel_id": 1, "processed_status": "pending", "retry_count": 0,
        "duration_seconds": 300,
    })
    assert len(cleaned) >= 200
    assert calls["api"] == 0
    assert calls["yt_dlp"] == 0
    # LLM 로그에 transcribe_gemini 기록됐는지
    row = conn.execute(
        "SELECT service FROM llm_call_logs WHERE related_entity='video:1'"
    ).fetchone()
    assert row is not None and row[0] == "transcribe_gemini"


def test_video_processor_falls_back_when_gemini_fails(monkeypatch):
    """Gemini 실패 시 기존 transcript-api/yt-dlp 경로로 fallback."""
    import sqlite3
    from db.sourcing_schema import init_sourcing_tables
    from services import video_processor as vp
    from services import transcript_service as ts

    conn = sqlite3.connect(":memory:")
    init_sourcing_tables(conn, dialect="sqlite")
    conn.execute("INSERT INTO youtube_channels (id, channel_id) VALUES (1, 'UCt')")
    conn.execute(
        "INSERT INTO youtube_videos (id, channel_id, video_id, processed_status) "
        "VALUES (1, 1, 'gZPdX8NRv24', 'pending')"
    )
    conn.commit()

    monkeypatch.setattr(gt, "fetch_transcript_via_gemini",
                        lambda *a, **kw: (_ for _ in ()).throw(gt.GeminiTranscriptError("quota")))

    long_segments = [
        ts.Segment(0, 5, "알리익스프레스에서 실패없는 꿀템들만 모아서 소개해 드립니다 오늘도 판매량 높은 가성비 좋은 제품 열 가지를 준비했습니다"),
        ts.Segment(5, 10, "첫 번째 제품은 샤오미 다기능 손전등 제품입니다 차량 비상용 5-in-1 구성으로 편리한 기능이 모두 결합되어 있습니다"),
        ts.Segment(10, 15, "두 번째 제품은 오토바이 헬멧 카메라 인터콤 제품입니다 고화질 블랙박스와 통신 기능을 하나로 합친 올인원 상품"),
        ts.Segment(15, 20, "세 번째 제품은 스마트폰 3축 짐벌 제품입니다 핸들 내부에 연장봉이 내장되어 브이로그 촬영에 매우 편리한 아이템"),
    ]
    monkeypatch.setattr(ts, "fetch_captions_via_api",
                        lambda *a, **kw: (long_segments, "ko"))
    cleaned, _ = vp._step_transcribe(conn, {
        "id": 1, "video_id": "gZPdX8NRv24", "title": "t",
        "channel_id": 1, "processed_status": "pending", "retry_count": 0,
    })
    assert len(cleaned) >= 200
