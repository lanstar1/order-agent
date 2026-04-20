"""Transcript acquisition and cleaning.

Two responsibilities:
1. Fetch auto-generated SRT via yt-dlp (thin wrapper, pure-python alternative via
   youtube-transcript-api in future).
2. Clean the "sliding-window" 3x repetition typical of YouTube Korean
   auto-captions and parse timestamp segments.

This module is dependency-light so it can run inside tests without the network.
"""
from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


SEGMENT_TEXT_RE = re.compile(r"\[음악\]|>>|&nbsp;", re.I)
TIMECODE_RE = re.compile(
    r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*(\d{2}):(\d{2}):(\d{2})[,.](\d{3})"
)


@dataclass
class Segment:
    start_sec: float
    end_sec: float
    text: str

    def to_dict(self) -> dict:
        return asdict(self)


# --------------------------------------------------------------------------- #
# SRT parsing
# --------------------------------------------------------------------------- #


def _time_to_sec(h: str, m: str, s: str, ms: str) -> float:
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def parse_srt(srt_text: str) -> list[Segment]:
    """Return timestamped segments from SRT text. Empty list on failure."""
    segments: list[Segment] = []
    if not srt_text:
        return segments

    blocks = re.split(r"\n\s*\n", srt_text.strip())
    for b in blocks:
        lines = b.strip().split("\n")
        if len(lines) < 2:
            continue
        # Find the timecode line (sometimes the first line is the block index,
        # sometimes the timecode is first).
        m = None
        content_start = 1
        for idx, line in enumerate(lines[:2]):
            m = TIMECODE_RE.search(line)
            if m:
                content_start = idx + 1
                break
        if not m:
            continue
        start = _time_to_sec(*m.group(1, 2, 3, 4))
        end = _time_to_sec(*m.group(5, 6, 7, 8))
        text = " ".join(lines[content_start:]).strip()
        text = SEGMENT_TEXT_RE.sub(" ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            segments.append(Segment(start, end, text))
    return segments


# --------------------------------------------------------------------------- #
# Sliding-window dedup (word-level LCS walk)
# --------------------------------------------------------------------------- #


def dedupe_sliding_window(text: str, max_overlap_words: int = 15) -> str:
    """Collapse repeated prefixes common in Korean auto-captions.

    Algorithm: walk words; if the next k (3 ≤ k ≤ max_overlap_words) words
    equal the tail of the accumulator, skip them. Otherwise append one word.
    """
    if not text:
        return ""
    words = [w for w in text.split(" ") if w]
    out: list[str] = []
    i = 0
    n = len(words)
    while i < n:
        appended = False
        for k in range(min(max_overlap_words, len(out)), 2, -1):
            if i + k <= n and out[-k:] == words[i : i + k]:
                i += k
                appended = True
                break
        if not appended:
            out.append(words[i])
            i += 1
    return " ".join(out)


def clean_transcript_from_srt(srt_text: str) -> tuple[str, list[Segment]]:
    """Return (deduplicated_text, segments)."""
    segments = parse_srt(srt_text)
    joined = " ".join(s.text for s in segments)
    cleaned = dedupe_sliding_window(joined)
    return cleaned, segments


# --------------------------------------------------------------------------- #
# yt-dlp wrapper (runtime; not used in unit tests)
# --------------------------------------------------------------------------- #


class TranscriptFetchError(RuntimeError):
    pass


def download_auto_captions(
    video_url: str,
    *,
    lang_priority: tuple[str, ...] = ("ko", "ko-KR", "en"),
    work_dir: Optional[Path] = None,
    yt_dlp_path: str = "yt-dlp",
    timeout_sec: int = 120,
) -> tuple[str, str]:
    """Download auto-captions and return (srt_text, language_used).

    Raises TranscriptFetchError when no caption is available in any priority
    language. Caller is responsible for scheduling retries.
    """
    work_dir = Path(work_dir or Path.cwd())
    work_dir.mkdir(parents=True, exist_ok=True)

    last_err: Optional[str] = None
    for lang in lang_priority:
        out_path = work_dir / f"caption_{lang}.srt"
        if out_path.exists():
            out_path.unlink()
        cmd = [
            yt_dlp_path,
            "--skip-download",
            "--write-auto-sub",
            "--sub-lang",
            lang,
            "--convert-subs",
            "srt",
            # Render Dockerfile에는 Node.js가 있음 — yt-dlp가 YouTube JS
            # challenge를 해결하도록 runtime 명시. 없어도 동작하는 영상은
            # 그대로 작동하므로 안전한 addition.
            "--extractor-args",
            "youtube:player_client=android,web;js_runtime=node",
            "-o",
            str(work_dir / "caption_%(subtitle_lang)s.%(ext)s"),
            video_url,
        ]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            last_err = f"yt-dlp timeout ({lang}): {exc}"
            continue
        if proc.returncode != 0:
            last_err = f"yt-dlp failed ({lang}): {proc.stderr.strip()[:200]}"
            continue
        # yt-dlp names output caption_<lang>.srt; but the `%(subtitle_lang)s`
        # template may produce slightly different names — walk the work_dir.
        srt_files = sorted(work_dir.glob(f"caption_{lang}*.srt"))
        if not srt_files:
            last_err = f"no srt file produced for {lang}"
            continue
        text = srt_files[0].read_text(encoding="utf-8")
        if text.strip():
            return text, lang
        last_err = f"empty caption for {lang}"

    raise TranscriptFetchError(last_err or "no captions available")


# --------------------------------------------------------------------------- #
# youtube-transcript-api 경로 — JS runtime 불필요 (1순위 시도)
# --------------------------------------------------------------------------- #


def _build_proxy_config():
    """환경변수에서 Webshare residential proxy 설정을 읽는다. 없으면 None."""
    import os
    user = os.environ.get("YT_TRANSCRIPT_PROXY_USERNAME", "").strip()
    pw = os.environ.get("YT_TRANSCRIPT_PROXY_PASSWORD", "").strip()
    if not user or not pw:
        return None
    try:
        from youtube_transcript_api.proxies import WebshareProxyConfig  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001
        return None
    return WebshareProxyConfig(proxy_username=user, proxy_password=pw)


def fetch_captions_via_api(
    video_id: str,
    *,
    lang_priority: tuple[str, ...] = ("ko", "ko-KR", "en"),
) -> tuple[list[Segment], str]:
    """youtube-transcript-api로 자막을 받아 (segments, language) 반환.

    클라우드 공용 IP가 YouTube에 차단될 때는 ``YT_TRANSCRIPT_PROXY_USERNAME``
    / ``YT_TRANSCRIPT_PROXY_PASSWORD`` 환경변수가 있으면 Webshare 프록시를 사용.
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi  # type: ignore[import-not-found]
        try:
            from youtube_transcript_api._errors import (  # type: ignore[import-not-found]
                TranscriptsDisabled, NoTranscriptFound,
                VideoUnavailable, CouldNotRetrieveTranscript,
            )
        except Exception:  # noqa: BLE001
            TranscriptsDisabled = NoTranscriptFound = VideoUnavailable = \
                CouldNotRetrieveTranscript = Exception  # fallback catch-all
    except ImportError as exc:
        raise TranscriptFetchError(
            "youtube-transcript-api 패키지가 설치되지 않았습니다."
        ) from exc

    proxy_config = _build_proxy_config()
    last_err: Optional[str] = None
    for lang in lang_priority:
        try:
            # v1.x instance API 우선
            try:
                ytt = (YouTubeTranscriptApi(proxy_config=proxy_config)
                       if proxy_config else YouTubeTranscriptApi())
                fetched = ytt.fetch(video_id, languages=[lang])
                # FetchedTranscript iterable → snippets with .text .start .duration
                raw = [
                    {"text": s.text, "start": s.start, "duration": s.duration}
                    for s in fetched
                ]
            except (AttributeError, TypeError):
                # v0.6.x 클래스 메서드 레거시
                raw = YouTubeTranscriptApi.get_transcript(video_id, languages=[lang])
        except (TranscriptsDisabled, NoTranscriptFound) as exc:
            last_err = f"{lang}: {type(exc).__name__}"
            continue
        except VideoUnavailable as exc:
            raise TranscriptFetchError(f"영상을 재생할 수 없습니다: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 - runtime fallback
            last_err = f"{lang}: {exc}"
            continue

        segments: list[Segment] = []
        for s in raw:
            text = (s.get("text") or "").strip()
            text = SEGMENT_TEXT_RE.sub(" ", text)
            text = re.sub(r"\s+", " ", text).strip()
            if not text:
                continue
            start = float(s.get("start") or 0.0)
            end = start + float(s.get("duration") or 0.0)
            segments.append(Segment(start, end, text))
        if segments:
            return segments, lang

    raise TranscriptFetchError(
        f"자막을 찾을 수 없습니다 (시도: {', '.join(lang_priority)}). "
        f"원인: {last_err or 'unknown'}"
    )


def clean_transcript_from_segments(segments: list[Segment]) -> str:
    """segments → sliding-window dedup된 단일 문자열."""
    joined = " ".join(s.text for s in segments)
    return dedupe_sliding_window(joined)
