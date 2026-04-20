"""Google Gemini로 YouTube 영상 전사 — IP 차단 회피의 결정적 해결책.

기존 youtube-transcript-api / yt-dlp 경로는 Render 공용 IP가 YouTube에
차단되어 실패함. Gemini API 는 Google 자체 서버에서 YouTube 영상을
처리하므로 사용자 서버 IP와 무관하게 동작.

Gemini는 파일 URI 로 YouTube 링크를 받아 멀티모달 처리:
  - 영상 다운로드·디코딩 구글이 수행
  - 자동 자막 대비 훨씬 정확 (음성 자체를 ASR + 비주얼)
  - 오탈자·문법 수정까지 프롬프트로 같이 요청 가능

비용 (gemini-2.5-flash 기준):
  10분 영상 ≈ 150K input tokens ≈ $0.01 per video
  10분 영상 전사만: ~3~5K output tokens ≈ $0.003
  총 ~$0.015 / video (Claude Haiku 기반 추출 + Sonnet 자료 생성 제외)

환경변수:
  GOOGLE_API_KEY          필수 — 기존 order-agent에서 이미 사용 중
  GEMINI_TRANSCRIPT_MODEL 선택 — 기본 gemini-2.5-flash
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)


class GeminiTranscriptError(RuntimeError):
    pass


DEFAULT_PROMPT = (
    "아래 YouTube 영상의 한국어 내레이션을 정확하게 전사해주세요.\n\n"
    "규칙:\n"
    "1. 자동 자막에서 흔히 발생하는 오탈자, 띄어쓰기, 비문을 자연스럽게 수정.\n"
    "   (예: '철루맨' → '1,000루멘', '체련된' → '세련된')\n"
    "2. 브랜드명·제품명·수치는 정확히. 확실하지 않으면 [?] 표기 후 원음을 괄호로 유지.\n"
    "3. 여러 제품이 순서대로 소개되는 영상이면 각 제품을 단락으로 구분해주세요.\n"
    "   (첫 번째 제품은…, 두 번째 제품은…)\n"
    "4. 음악/잡음 마커([음악], >> 등)는 제거.\n"
    "5. 전사 텍스트만 출력. 타임스탬프·추가 설명·JSON은 불필요.\n"
)


def _default_model() -> str:
    return os.environ.get("GEMINI_TRANSCRIPT_MODEL", "gemini-2.5-flash").strip() \
           or "gemini-2.5-flash"


def fetch_transcript_via_gemini(
    video_url: str,
    *,
    prompt: Optional[str] = None,
    model_name: Optional[str] = None,
    timeout_sec: int = 300,
) -> tuple[str, dict]:
    """Gemini 에 YouTube URL 을 전달해 전사 받기.

    Returns (transcript_text, meta). meta는 provider, model, input_tokens,
    output_tokens, latency_ms 포함.
    Raises GeminiTranscriptError on any failure.
    """
    key = os.environ.get("GOOGLE_API_KEY", "").strip()
    if not key:
        raise GeminiTranscriptError("GOOGLE_API_KEY 환경변수가 설정되지 않았습니다.")

    try:
        import google.generativeai as genai  # type: ignore[import-not-found]
    except ImportError as exc:
        raise GeminiTranscriptError(
            "google-generativeai 패키지가 설치되지 않았습니다."
        ) from exc

    model_name = model_name or _default_model()
    system_prompt = prompt or DEFAULT_PROMPT

    genai.configure(api_key=key)
    start = time.monotonic()

    # Build content — YouTube URL 은 Part.file_data / dict 모두로 전달 가능
    content: list = [system_prompt]
    try:
        from google.generativeai import types as gtypes  # type: ignore[import-not-found]
        if hasattr(gtypes, "Part") and hasattr(gtypes.Part, "from_uri"):
            content.append(gtypes.Part.from_uri(file_uri=video_url, mime_type="video/*"))
        else:
            raise ImportError("types.Part.from_uri 미지원")
    except Exception:  # noqa: BLE001 - fallback to dict form
        content.append({"file_data": {"mime_type": "video/mp4", "file_uri": video_url}})

    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(
            content,
            request_options={"timeout": timeout_sec},
        )
    except Exception as exc:  # noqa: BLE001
        raise GeminiTranscriptError(f"Gemini API 호출 실패: {exc}") from exc

    # response.text 가 None 이거나 빈 경우 대비
    try:
        text = (response.text or "").strip()
    except (ValueError, AttributeError) as exc:
        # response.parts 에서 직접 꺼내기 (응답 블록 때 finish_reason 확인)
        parts = []
        try:
            for cand in getattr(response, "candidates", []) or []:
                for p in (getattr(cand, "content", None) and getattr(cand.content, "parts", [])) or []:
                    if getattr(p, "text", None):
                        parts.append(p.text)
        except Exception:  # noqa: BLE001
            pass
        text = " ".join(parts).strip()
        if not text:
            raise GeminiTranscriptError(
                f"Gemini 응답에서 텍스트를 추출할 수 없음 (safety/finish 이슈 가능): {exc}"
            ) from exc

    if not text:
        raise GeminiTranscriptError("Gemini가 빈 응답을 반환했습니다.")

    latency_ms = int((time.monotonic() - start) * 1000)
    usage = getattr(response, "usage_metadata", None)
    meta = {
        "provider": "google",
        "model": model_name,
        "input_tokens": getattr(usage, "prompt_token_count", 0) if usage else 0,
        "output_tokens": getattr(usage, "candidates_token_count", 0) if usage else 0,
        "latency_ms": latency_ms,
    }
    logger.info(
        f"[gemini_transcript] model={model_name} chars={len(text)} "
        f"tokens=in:{meta['input_tokens']} out:{meta['output_tokens']} "
        f"latency={latency_ms}ms"
    )
    return text, meta
