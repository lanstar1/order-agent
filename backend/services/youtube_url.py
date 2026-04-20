"""YouTube URL / handle / channel-id parser.

Accepted inputs:
- https://www.youtube.com/watch?v=VIDEO_ID[&...]
- https://youtu.be/VIDEO_ID
- https://www.youtube.com/@channelhandle
- https://www.youtube.com/@channelhandle/videos
- https://www.youtube.com/channel/UCxxxxxxx
- https://www.youtube.com/c/CustomName      (legacy)
- @channelhandle            (bare handle)
- UCxxxxxxxxxxxxxxxxxxxxxx  (bare channel id; 24 chars, starts with UC)
- 11-char video id
"""
from __future__ import annotations

import re
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
HANDLE_RE = re.compile(r"^@[A-Za-z0-9._-]{3,30}$")


def parse_youtube_input(raw: str) -> ParseResult:
    """Parse a YouTube identifier of unknown format."""
    if not raw:
        return ParseResult("unknown", "", raw)

    s = raw.strip()

    # Bare identifiers first
    if CHANNEL_ID_RE.match(s):
        return ParseResult("channel_id", s, raw)
    if HANDLE_RE.match(s):
        return ParseResult("handle", s, raw)
    # 11-char bare video id — conservative: only if clearly not a URL
    if VIDEO_ID_RE.match(s) and "/" not in s and "." not in s:
        return ParseResult("video", s, raw)

    # URL parsing
    # Ensure scheme so urlparse treats path correctly
    if not s.startswith(("http://", "https://")):
        s = "https://" + s

    u = urlparse(s)
    host = (u.hostname or "").lower().removeprefix("www.")
    path = u.path or "/"

    if host == "youtu.be":
        # youtu.be/VIDEO_ID
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
        # /@handle[/...]
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
