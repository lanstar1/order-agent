"""비디오 처리 파이프라인 오케스트레이션.

단일 진입점: ``process_video_full(conn, video_row_id)``.

플로우:
  pending → transcribing → correcting → extracting → done
             │              │             │
             └ yt-dlp SRT   └ OpenAI     └ Claude Haiku
                             mini        (per paragraph)

실패 시: processed_status='failed', error_reason 기록, retry_count+1.
멱등성: 같은 video_row_id 재처리 시 이전 transcript·products 덮어씀 가능
(사용자가 명시적으로 재처리 버튼 누른 경우).
"""
from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from services import transcript_service as ts
from services import transcript_corrector as tc
from services import product_extractor as px
from services import llm_adapters
from services.llm_logger import LLMCallRecord, log_llm_call


logger = logging.getLogger(__name__)


class ProcessingError(RuntimeError):
    pass


@dataclass
class ProcessResult:
    video_row_id: int
    transcript_chars: int
    correction_ratio: float
    needs_human_review: bool
    products_created: int
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "video_row_id": self.video_row_id,
            "transcript_chars": self.transcript_chars,
            "correction_ratio": round(self.correction_ratio, 3),
            "needs_human_review": self.needs_human_review,
            "products_created": self.products_created,
            "error": self.error,
        }


def _load_video(conn, video_row_id: int) -> dict:
    row = conn.execute(
        """SELECT id, video_id, title, channel_id, processed_status, retry_count
           FROM youtube_videos WHERE id=?""",
        (video_row_id,),
    ).fetchone()
    if not row:
        raise ProcessingError(f"영상을 찾을 수 없습니다 (id={video_row_id})")
    return {
        "id": row[0], "video_id": row[1], "title": row[2],
        "channel_id": row[3], "processed_status": row[4],
        "retry_count": row[5] or 0,
    }


def _set_step(conn, video_row_id: int, step: str):
    conn.execute(
        "UPDATE youtube_videos SET internal_step=?, processed_status='in_progress' WHERE id=?",
        (step, video_row_id),
    )
    conn.commit()


def _mark_failed(conn, video_row_id: int, error: str):
    conn.execute(
        """UPDATE youtube_videos
           SET processed_status='failed', error_reason=?,
               retry_count=COALESCE(retry_count, 0) + 1
           WHERE id=?""",
        (error[:500], video_row_id),
    )
    conn.commit()


def _mark_done(conn, video_row_id: int):
    conn.execute(
        """UPDATE youtube_videos
           SET processed_status='done', internal_step='done',
               processed_at=CURRENT_TIMESTAMP, error_reason=NULL
           WHERE id=?""",
        (video_row_id,),
    )
    conn.commit()


# ─── Pipeline steps ──────────────────────────────────────────── #


def _step_transcribe(conn, video: dict) -> tuple[str, list]:
    """자막 수집 + sliding-window dedup.

    1순위: youtube-transcript-api (JS 런타임 불필요, 가볍고 빠름)
    2순위: yt-dlp (Node.js runtime hint 포함)
    """
    _set_step(conn, video["id"], "transcribing")
    vid = video["video_id"]

    cleaned = ""
    segments: list = []
    api_err: Optional[str] = None
    yt_dlp_err: Optional[str] = None

    # ─ 1순위 — youtube-transcript-api ─
    try:
        segments, _lang = ts.fetch_captions_via_api(vid)
        cleaned = ts.clean_transcript_from_segments(segments)
        logger.info(f"[video_processor] transcript-api 성공 video={video['id']} chars={len(cleaned)}")
    except ts.TranscriptFetchError as exc:
        api_err = str(exc)
        logger.info(f"[video_processor] transcript-api 실패, yt-dlp 폴백: {api_err}")

    # ─ 2순위 폴백 — yt-dlp ─
    if not cleaned or len(cleaned) < 200:
        url = f"https://www.youtube.com/watch?v={vid}"
        try:
            with tempfile.TemporaryDirectory() as tmp:
                srt_text, _lang = ts.download_auto_captions(url, work_dir=Path(tmp))
            cleaned, segments = ts.clean_transcript_from_srt(srt_text)
            logger.info(f"[video_processor] yt-dlp 성공 video={video['id']} chars={len(cleaned)}")
        except ts.TranscriptFetchError as exc:
            yt_dlp_err = str(exc)

    if not cleaned or len(cleaned) < 200:
        reasons = []
        if api_err:
            reasons.append(f"youtube-transcript-api: {api_err}")
        if yt_dlp_err:
            reasons.append(f"yt-dlp: {yt_dlp_err[:200]}")
        raise ProcessingError(
            "자막을 가져올 수 없습니다. 자동자막 미생성 / 영상 비공개 / 자막 자체 없음 "
            "중 하나일 수 있습니다. " + " | ".join(reasons)
        )

    conn.execute(
        """UPDATE youtube_videos
           SET transcript_raw=?, transcript_segments=?
           WHERE id=?""",
        (
            cleaned,
            json.dumps([s.to_dict() for s in segments], ensure_ascii=False),
            video["id"],
        ),
    )
    conn.commit()
    return cleaned, segments


def _step_correct(conn, video_row_id: int, raw_cleaned: str,
                   *, llm_fn: Optional[Callable] = None) -> tc.CorrectionResult:
    """LLM 보정. 변경비율 30% 초과 시 원본으로 폴백."""
    _set_step(conn, video_row_id, "correcting")
    llm_fn = llm_fn or llm_adapters.openai_correct_fn
    result = tc.correct_transcript(raw_cleaned, llm_fn=llm_fn)

    conn.execute(
        """UPDATE youtube_videos
           SET transcript_corrected=?, correction_model=?,
               correction_tokens=?, correction_ratio=?,
               needs_human_review=?
           WHERE id=?""",
        (
            result.corrected, result.model,
            result.input_tokens + result.output_tokens,
            result.ratio,
            1 if result.needs_human_review else 0,
            video_row_id,
        ),
    )
    conn.commit()

    # LLM 호출 로그
    try:
        log_llm_call(conn, LLMCallRecord(
            service="correct_transcript",
            provider=result.provider or "unknown",
            model=result.model or "unknown",
            prompt_version=result.prompt_version,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            latency_ms=result.latency_ms,
            success=not result.error,
            error_message=result.error,
            related_entity=f"video:{video_row_id}",
        ))
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[video_processor] llm log 실패: {exc}")

    return result


def _insert_product(conn, *, video_row_id: int, rec: dict) -> int:
    """제품 한 건을 sourced_products에 INSERT — 기존 동일 position 있으면 스킵."""
    # Idempotency via (video_id, position) uniqueness at app level
    existing = conn.execute(
        "SELECT id FROM sourced_products WHERE video_id=? AND position=?",
        (video_row_id, rec.get("position", 0)),
    ).fetchone()
    if existing:
        return existing[0]

    cur = conn.execute(
        """INSERT INTO sourced_products (
            video_id, position, product_name, brand, brand_confidence,
            category, subcategory,
            key_features, specs, price_range_usd, target_use_case,
            search_keywords_kr, target_persona,
            start_sec, end_sec
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            video_row_id,
            rec.get("position", 0),
            rec.get("product_name", ""),
            rec.get("brand"),
            rec.get("brand_confidence"),
            rec.get("category", ""),
            rec.get("subcategory"),
            json.dumps(rec.get("key_features") or [], ensure_ascii=False),
            json.dumps(rec.get("specs") or {}, ensure_ascii=False),
            json.dumps(rec.get("price_range_usd") or {}, ensure_ascii=False),
            json.dumps(rec.get("target_use_case") or [], ensure_ascii=False),
            json.dumps(rec.get("search_keywords_kr") or [], ensure_ascii=False),
            json.dumps(rec.get("target_persona") or {}, ensure_ascii=False),
            rec.get("start_sec"),
            rec.get("end_sec"),
        ),
    )
    return cur.lastrowid


def _step_extract(conn, video_row_id: int, corrected: str, segments: list,
                   *, llm_fn: Optional[Callable] = None) -> int:
    """Claude 단락별 제품 추출 + DB 저장."""
    _set_step(conn, video_row_id, "extracting")
    llm_fn = llm_fn or llm_adapters.claude_extract_fn

    outcome = px.extract_products(
        corrected, segments=segments, llm_fn=llm_fn,
    )

    created = 0
    for rec in outcome.products:
        try:
            pid = _insert_product(conn, video_row_id=video_row_id, rec=rec)
            if pid:
                created += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[video_processor] 제품 INSERT 실패 (position={rec.get('position')}): {exc}")

    conn.commit()

    # 종합 로그 1건
    try:
        log_llm_call(conn, LLMCallRecord(
            service="extract_products",
            provider=outcome.provider or "unknown",
            model=outcome.model or "unknown",
            prompt_version=outcome.prompt_version,
            input_tokens=outcome.input_tokens,
            output_tokens=outcome.output_tokens,
            latency_ms=outcome.latency_ms,
            success=len(outcome.products) > 0,
            error_message=("no products extracted" if not outcome.products else None),
            related_entity=f"video:{video_row_id}",
        ))
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[video_processor] extract llm log 실패: {exc}")

    return created


# ─── Public API ─────────────────────────────────────────────── #


def process_video_full(
    conn, video_row_id: int,
    *,
    correct_llm_fn: Optional[Callable] = None,
    extract_llm_fn: Optional[Callable] = None,
) -> ProcessResult:
    """End-to-end. Raises ProcessingError on known failure modes."""
    video = _load_video(conn, video_row_id)

    try:
        # 1. Transcribe
        cleaned, segments = _step_transcribe(conn, video)
        logger.info(f"[video_processor] transcribed video={video_row_id} chars={len(cleaned)}")

        # 2. Correct
        corr = _step_correct(conn, video_row_id, cleaned, llm_fn=correct_llm_fn)
        logger.info(f"[video_processor] corrected video={video_row_id} ratio={corr.ratio:.3f}")

        # 3. Extract
        created = _step_extract(
            conn, video_row_id, corr.corrected, segments,
            llm_fn=extract_llm_fn,
        )
        logger.info(f"[video_processor] extracted video={video_row_id} products={created}")

        _mark_done(conn, video_row_id)
        return ProcessResult(
            video_row_id=video_row_id,
            transcript_chars=len(cleaned),
            correction_ratio=corr.ratio,
            needs_human_review=corr.needs_human_review,
            products_created=created,
        )

    except ProcessingError as exc:
        _mark_failed(conn, video_row_id, str(exc))
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception(f"[video_processor] 예기치 못한 오류 video={video_row_id}")
        _mark_failed(conn, video_row_id, f"내부 오류: {exc}")
        raise ProcessingError(f"처리 중 오류: {exc}") from exc
