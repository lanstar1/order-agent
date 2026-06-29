"""YouTube URL / handle / channel-id parser.

Accepted inputs:
- https://www.youtube.com/watch?v=VIDEO_ID[&...]
- https://youtu.be/VIDEO_ID
- https://www.youtube.com/@channelhandle
- https://www.youtube.com/@channelhandle/videos
- https://www.youtube.com/@한글핸들              (Unicode handles supported)
- https://www.youtube.com/@%EC%95%8C%EB%9C%B0   (percent-encoded)
- https://www.youtube.com/channel/UCxxxxxxx
- https://www.youtube.com/c/CustomName      (legacy)
- @channelhandle            (bare handle, ASCII or Unicode)
- UCxxxxxxxxxxxxxxxxxxxxxx  (bare channel id; 24 chars, starts with UC)
- 11-char video id
"""
from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse, parse_qs

Kind = Literal["video", "channel_id", "handle", "custom", "unknown"]


@dataclass
class ParseResult:
    kind: Kind
    value: str                  # video_id / UCxxx / @handle / custom
    raw: str                    # original input

    def __bool__(self) -> bool:
        return self.kind != "unknown"


VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
CHANNEL_ID_RE = re.compile(r"^UC[A-Za-z0-9_-]{22}$")

# YouTube handle: 3-30 chars of Unicode word chars + `.` `-`.
# `\w` with re.UNICODE includes Hangul, Kana, CJK, Latin, digits, underscore.
# This matches YouTube's actual policy (e.g. `@알뜨직구`, `@のり弁`, `@naver`).
HANDLE_RE = re.compile(r"^@[\w.\-]{3,30}$", re.UNICODE)


def _safe_unquote(s: str) -> str:
    """Percent-decode defensively — malformed sequences pass through unchanged."""
    try:
        return urllib.parse.unquote(s)
    except Exception:
        return s


def parse_youtube_input(raw: str) -> ParseResult:
    """Parse a YouTube identifier of unknown format.

    Handles browser-style percent-encoded Unicode handles
    (e.g. `/@%EC%95%8C%EB%9C%B0%EC%A7%81%EA%B5%AC` → `@알뜨직구`).
    """
    if not raw:
        return ParseResult("unknown", "", raw)

    s = raw.strip()

    # Bare identifiers first (handle both raw and percent-encoded forms)
    decoded = _safe_unquote(s)
    if CHANNEL_ID_RE.match(decoded):
        return ParseResult("channel_id", decoded, raw)
    if HANDLE_RE.match(decoded):
        return ParseResult("handle", decoded, raw)
    # 11-char bare video id — conservative: only if clearly not a URL
    if VIDEO_ID_RE.match(decoded) and "/" not in decoded and "." not in decoded:
        return ParseResult("video", decoded, raw)

    # URL parsing
    if not s.startswith(("http://", "https://")):
        s = "https://" + s

    u = urlparse(s)
    host = (u.hostname or "").lower().removeprefix("www.")
    # Decode percent-encoded path so Unicode handles become readable.
    path = _safe_unquote(u.path or "/")

    if host == "youtu.be":
        vid = path.lstrip("/").split("/")[0]
        if VIDEO_ID_RE.match(vid):
            return ParseResult("video", vid, raw)
        return ParseResult("unknown", "", raw)

    if host in ("youtube.com", "m.youtube.com", "music.youtube.com"):
        # /watch?v=VIDEO_ID
        if path == "/watch":
            qs = parse_qs(u.query)
            vids = qs.get("v", [])
            if vids and VIDEO_ID_RE.match(vids[0]):
                return ParseResult("video", vids[0], raw)
        # /shorts/VIDEO_ID
        if path.startswith("/shorts/"):
            vid = path.split("/", 2)[2].split("/")[0]
            if VIDEO_ID_RE.match(vid):
                return ParseResult("video", vid, raw)
        # /@handle[/...]  (ASCII + Unicode)
        if path.startswith("/@"):
            handle = "@" + path[2:].split("/")[0]
            if HANDLE_RE.match(handle):
                return ParseResult("handle", handle, raw)
        # /channel/UCxxx
        if path.startswith("/channel/"):
            cid = path.split("/", 2)[2].split("/")[0]
            if CHANNEL_ID_RE.match(cid):
                return ParseResult("channel_id", cid, raw)
        # /c/CustomName  (legacy — needs API resolution)
        if path.startswith("/c/"):
            cust = path.split("/", 2)[2].split("/")[0]
            if cust:
                return ParseResult("custom", cust, raw)

    return ParseResult("unknown", "", raw)
