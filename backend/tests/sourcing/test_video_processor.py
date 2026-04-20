"""비디오 처리 파이프라인 테스트 (네트워크·LLM 없이 fake만 사용)."""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pytest

from db.sourcing_schema import init_sourcing_tables
from services import video_processor as vp
from services import transcript_corrector as tc
from services import product_extractor as px


# ─────────────────────────────────────────────── #
# Fixtures
# ─────────────────────────────────────────────── #


SAMPLE_RAW_TRANSCRIPT = (
    "알리익스프레스에서 실패없는 꿀템들만 모아서 소개해 드립니다. "
    "첫 번째 제품은 샤오미 다기능 손전등 제품입니다. "
    "철루맨 조명 3,100m마 배터리, 체련된 디자인. "
    "두 번째 제품은 오토바이 헬멧 카메라 인터콤입니다. "
    "실소파 라이더용 가성비 제품. "
    "세 번째 제품은 스마트폰 3축 짐벌입니다. "
    "시엔성 좋은 OLED 화면."
)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    init_sourcing_tables(c, dialect="sqlite")
    # seed a channel + video in pending state
    c.execute(
        "INSERT INTO youtube_channels (id, channel_id, channel_handle, channel_title) "
        "VALUES (1, 'UCtest123test123test1234', '@test', 'Test Channel')"
    )
    c.execute(
        "INSERT INTO youtube_videos (id, channel_id, video_id, title, processed_status) "
        "VALUES (1, 1, 'gZPdX8NRv24', '알리 BEST 10', 'pending')"
    )
    c.commit()
    return c


# ─────────────────────────────────────────────── #
# Tests
# ─────────────────────────────────────────────── #


def _fake_download(url, work_dir=None, **kw):
    """transcript_service.download_auto_captions 모킹: 간단한 SRT 리턴."""
    srt = (
        "1\n"
        "00:00:00,000 --> 00:00:05,000\n"
        "알리익스프레스에서 실패없는 꿀템들만 모아서 소개해 드립니다.\n"
        "\n"
        "2\n"
        "00:00:05,000 --> 00:00:30,000\n"
        "첫 번째 제품은 샤오미 다기능 손전등 제품입니다. "
        "철루맨 조명 3,100m마 배터리, 체련된 디자인.\n"
        "\n"
        "3\n"
        "00:00:30,000 --> 00:01:00,000\n"
        "두 번째 제품은 오토바이 헬멧 카메라 인터콤입니다. "
        "실소파 라이더용 가성비 제품.\n"
        "\n"
        "4\n"
        "00:01:00,000 --> 00:01:30,000\n"
        "세 번째 제품은 스마트폰 3축 짐벌입니다. "
        "시엔성 좋은 OLED 화면. 어쩌고 저쩌고 채우는 내용입니다. "
        "부족한 길이를 채우기 위해 더 긴 설명을 추가합니다."
    )
    return srt, "ko"


def test_process_video_full_end_to_end(conn):
    """가짜 자막 다운로드 + fake LLM들로 전체 파이프라인 실행."""
    with patch("services.transcript_service.download_auto_captions",
               side_effect=_fake_download):
        result = vp.process_video_full(
            conn, 1,
            correct_llm_fn=tc.fake_rule_based_llm,
            extract_llm_fn=px.fake_keyword_extractor,
        )

    assert result.products_created >= 2
    assert result.transcript_chars > 0
    assert result.correction_ratio >= 0.0

    # DB 상태 확인
    row = conn.execute(
        "SELECT processed_status, internal_step, transcript_corrected, "
        "correction_ratio, needs_human_review FROM youtube_videos WHERE id=1"
    ).fetchone()
    assert row[0] == "done"
    assert row[1] == "done"
    assert row[2]  # corrected transcript 저장됨
    # 보정 확인 — "철루맨" → "1,000루멘"
    assert "1,000루멘" in row[2]
    assert "세련된" in row[2]

    # 제품 DB 저장
    products = conn.execute(
        "SELECT position, product_name, target_persona FROM sourced_products WHERE video_id=1 ORDER BY position"
    ).fetchall()
    assert len(products) >= 2
    for p in products:
        assert p[1]  # product_name
        persona = json.loads(p[2])
        assert persona.get("label")

    # LLM 호출 로그
    n_logs = conn.execute(
        "SELECT COUNT(*) FROM llm_call_logs WHERE related_entity='video:1'"
    ).fetchone()[0]
    assert n_logs >= 2  # 보정 + 추출 각 1건


def test_process_video_marks_failed_on_fetch_error(conn):
    """자막 다운로드 실패 시 processed_status='failed', retry_count+1."""
    from services import transcript_service as ts

    def fail(url, **kw):
        raise ts.TranscriptFetchError("yt-dlp failed: rate limited")

    with patch("services.transcript_service.download_auto_captions", side_effect=fail):
        with pytest.raises(vp.ProcessingError) as e:
            vp.process_video_full(
                conn, 1,
                correct_llm_fn=tc.fake_rule_based_llm,
                extract_llm_fn=px.fake_keyword_extractor,
            )
    assert "자막" in str(e.value)

    row = conn.execute(
        "SELECT processed_status, error_reason, retry_count FROM youtube_videos WHERE id=1"
    ).fetchone()
    assert row[0] == "failed"
    assert "자막" in (row[1] or "")
    assert row[2] >= 1


def test_process_video_guards_too_short_transcript(conn):
    def tiny(url, **kw):
        return "1\n00:00:00,000 --> 00:00:02,000\n짧음\n", "ko"
    with patch("services.transcript_service.download_auto_captions", side_effect=tiny):
        with pytest.raises(vp.ProcessingError) as e:
            vp.process_video_full(
                conn, 1,
                correct_llm_fn=tc.fake_rule_based_llm,
                extract_llm_fn=px.fake_keyword_extractor,
            )
    assert "짧습니다" in str(e.value)


def test_process_video_missing_row_raises(conn):
    with pytest.raises(vp.ProcessingError) as e:
        vp.process_video_full(conn, 9999)
    assert "찾을 수 없습니다" in str(e.value)


def test_insert_product_idempotent(conn):
    """같은 (video_id, position) 재INSERT 시 스킵."""
    rec = {
        "position": 1, "product_name": "샤오미 손전등",
        "category": "차량용품", "search_keywords_kr": ["손전등"],
        "target_persona": {"label": "30~50대 차박 캠핑러"},
    }
    pid1 = vp._insert_product(conn, video_row_id=1, rec=rec)
    pid2 = vp._insert_product(conn, video_row_id=1, rec=rec)
    assert pid1 == pid2
    cnt = conn.execute(
        "SELECT COUNT(*) FROM sourced_products WHERE video_id=1"
    ).fetchone()[0]
    assert cnt == 1
