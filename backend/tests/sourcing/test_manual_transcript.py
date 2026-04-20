"""수동 자막 업로드 경로 테스트 (YouTube IP 차단 우회용)."""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from db.sourcing_schema import init_sourcing_tables
from services import video_processor as vp
from services import transcript_corrector as tc
from services import product_extractor as px


LONG_PLAIN = (
    "알리익스프레스에서 실패없는 꿀템들만 모아서 소개해 드립니다. "
    "오늘도 알리에서 판매량도 높고 한국에서 인기가 많은 가성비 좋은 제품 열 가지를 모아서 "
    "소개해 드리니 좋은 제품들 놓치지 말고 끝까지 시청 부탁드립니다. "
    "첫 번째 제품은 샤오미 다기능 손전등 제품입니다. "
    "철루맨 조명 3,100m마 배터리, 체련된 디자인의 차량용 비상 도구입니다. "
    "두 번째 제품은 오토바이 헬멧 카메라 인터콤입니다. "
    "실소파 라이더용 가성비 제품으로 블랙박스 기능까지 포함됩니다. "
    "세 번째 제품은 스마트폰 3축 짐벌로 핸들 내부에 연장봉이 내장되어 있습니다. "
    "시엔성 좋은 OLED 화면이 탑재되어 있어 편리합니다."
)

LONG_SRT = (
    "1\n00:00:00,000 --> 00:00:05,000\n알리익스프레스에서 실패없는 꿀템들만 모아서 소개해 드립니다.\n"
    "\n"
    "2\n00:00:05,000 --> 00:00:30,000\n"
    "첫 번째 제품은 샤오미 다기능 손전등 제품입니다. "
    "철루맨 조명 3,100m마 배터리, 체련된 디자인의 차량용 비상 도구입니다.\n"
    "\n"
    "3\n00:00:30,000 --> 00:01:00,000\n"
    "두 번째 제품은 오토바이 헬멧 카메라 인터콤입니다. "
    "실소파 라이더용 가성비 제품으로 블랙박스 기능까지 포함됩니다.\n"
    "\n"
    "4\n00:01:00,000 --> 00:01:30,000\n"
    "세 번째 제품은 스마트폰 3축 짐벌로 핸들 내부에 연장봉이 내장되어 있습니다. "
    "시엔성 좋은 OLED 화면이 탑재되어 있어 편리합니다."
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_sourcing_tables(c, dialect="sqlite")
    c.execute("INSERT INTO youtube_channels (id, channel_id) VALUES (1, 'UCtest')")
    c.execute(
        "INSERT INTO youtube_videos (id, channel_id, video_id, processed_status) "
        "VALUES (1, 1, 'abc123xxxxx', 'failed')"
    )
    c.commit()
    return c


def test_manual_srt_processes_end_to_end(conn):
    """SRT 형식 자막 직접 주입 → 보정·추출까지 완료."""
    result = vp.process_video_from_manual_transcript(
        conn, 1, LONG_SRT,
        correct_llm_fn=tc.fake_rule_based_llm,
        extract_llm_fn=px.fake_keyword_extractor,
    )
    assert result.products_created >= 2
    row = conn.execute(
        "SELECT processed_status, transcript_corrected FROM youtube_videos WHERE id=1"
    ).fetchone()
    assert row[0] == "done"
    # 보정 확인 — 철루맨 → 1,000루멘
    assert "1,000루멘" in row[1]


def test_manual_plain_text_also_works(conn):
    """SRT 아닌 평문 자막도 처리 가능."""
    result = vp.process_video_from_manual_transcript(
        conn, 1, LONG_PLAIN,
        correct_llm_fn=tc.fake_rule_based_llm,
        extract_llm_fn=px.fake_keyword_extractor,
    )
    assert result.products_created >= 1


def test_manual_too_short_rejected(conn):
    with pytest.raises(vp.ProcessingError) as e:
        vp.process_video_from_manual_transcript(
            conn, 1, "짧음",
            correct_llm_fn=tc.fake_rule_based_llm,
            extract_llm_fn=px.fake_keyword_extractor,
        )
    assert "너무 짧" in str(e.value)


def test_manual_empty_rejected(conn):
    with pytest.raises(vp.ProcessingError) as e:
        vp.process_video_from_manual_transcript(
            conn, 1, "   ",
            correct_llm_fn=tc.fake_rule_based_llm,
            extract_llm_fn=px.fake_keyword_extractor,
        )
    assert "비어" in str(e.value)


def test_ip_block_detection_in_error_message(conn):
    """YouTube IP 차단 감지 시 친절한 안내 메시지."""
    def block_api(*a, **kw):
        raise vp.ts.TranscriptFetchError(
            "ko: YouTube is blocking requests from your IP. "
            "This usually is due to one of the following reasons"
        )
    def block_yt(*a, **kw):
        raise vp.ts.TranscriptFetchError("yt-dlp blocked")
    with patch("services.transcript_service.fetch_captions_via_api", side_effect=block_api), \
         patch("services.transcript_service.download_auto_captions", side_effect=block_yt):
        with pytest.raises(vp.ProcessingError) as e:
            vp.process_video_full(
                conn, 1,
                correct_llm_fn=tc.fake_rule_based_llm,
                extract_llm_fn=px.fake_keyword_extractor,
            )
    msg = str(e.value)
    assert "차단" in msg
    assert "자막 붙여넣기" in msg or "자막" in msg
