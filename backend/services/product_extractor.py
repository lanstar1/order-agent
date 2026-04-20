"""Extract structured product records from a cleaned transcript.

Strategy (per plan v3, H4):
1. Split the transcript into paragraphs per product using "첫 번째", "두 번째", …
   markers. Fallback: a single chunk if no markers.
2. For each paragraph, run Claude with tool_use JSON schema.
3. Validate via pydantic; on parse failure retry once with "return only JSON".
4. Normalise persona labels against the whitelist; queue unknown labels as
   pending.

This file is network-free by default — pass any `llm_fn` for real calls.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from . import transcript_service as ts


PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "extract_products.txt"
PERSONA_DICT_PATH = Path(__file__).resolve().parents[1] / "data" / "persona_dictionary.json"


# --------------------------------------------------------------------------- #
# Product paragraph segmentation
# --------------------------------------------------------------------------- #


NUMBER_MARKERS = [
    ("첫 번째", 1),
    ("두 번째", 2),
    ("세 번째", 3),
    ("네 번째", 4),
    ("다섯 번째", 5),
    ("여섯 번째", 6),
    ("일곱 번째", 7),
    ("여덟 번째", 8),
    ("아홉 번째", 9),
    ("열 번째", 10),
    ("열한 번째", 11),
    ("열두 번째", 12),
    ("열세 번째", 13),
    ("열네 번째", 14),
    ("열다섯 번째", 15),
]


@dataclass
class ProductParagraph:
    position: int
    text: str
    start_sec: Optional[float] = None
    end_sec: Optional[float] = None


def split_products(
    cleaned_text: str,
    segments: Optional[list[ts.Segment]] = None,
) -> list[ProductParagraph]:
    """Split the transcript into per-product paragraphs with best-effort timing.

    Timing strategy: if segments are provided, match each product marker to the
    earliest segment whose text contains the marker; use that segment's
    start_sec as the product start.
    """
    # Find marker positions in the text
    positions: list[tuple[int, int, str]] = []  # (char_index, product_no, marker)
    for marker, n in NUMBER_MARKERS:
        needle = f"{marker} 제품"
        idx = cleaned_text.find(needle)
        if idx == -1:
            # try bare marker (e.g. "첫 번째 " followed by non-제품 word)
            idx = cleaned_text.find(marker + " ")
        if idx >= 0:
            positions.append((idx, n, marker))
    positions.sort()
    if not positions:
        return [ProductParagraph(position=1, text=cleaned_text.strip())]

    # Build paragraphs
    paragraphs: list[ProductParagraph] = []
    for i, (char_idx, n, marker) in enumerate(positions):
        next_idx = positions[i + 1][0] if i + 1 < len(positions) else len(cleaned_text)
        paragraph_text = cleaned_text[char_idx:next_idx].strip()

        start_sec = end_sec = None
        if segments:
            # Find segment containing the marker
            for seg in segments:
                if marker in seg.text:
                    start_sec = seg.start_sec
                    break
            # end_sec = start_sec of the next product's first segment if any
            if i + 1 < len(positions):
                next_marker = positions[i + 1][2]
                for seg in segments:
                    if next_marker in seg.text:
                        end_sec = seg.start_sec
                        break
            else:
                end_sec = segments[-1].end_sec if segments else None

        paragraphs.append(
            ProductParagraph(
                position=n, text=paragraph_text, start_sec=start_sec, end_sec=end_sec
            )
        )
    return paragraphs


# --------------------------------------------------------------------------- #
# Persona dictionary + normalisation
# --------------------------------------------------------------------------- #


LABEL_PATTERN = re.compile(r"^\d{2}~\d{2}대\s+\S+.*$")


@dataclass
class PersonaDict:
    labels: dict[str, dict] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path = PERSONA_DICT_PATH) -> "PersonaDict":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return cls(labels={})
        return cls(labels={entry["label"]: entry for entry in data.get("labels", [])})

    def is_approved(self, label: str) -> bool:
        return label in self.labels


def validate_persona_label(label: str) -> bool:
    """Check that the label follows the '{연령대} {라이프스타일}' pattern."""
    if not label:
        return False
    return bool(LABEL_PATTERN.match(label))


# --------------------------------------------------------------------------- #
# Extraction runner
# --------------------------------------------------------------------------- #


ExtractLLMFn = Callable[[str, str], tuple[dict, dict]]
"""Signature: (system_prompt, paragraph_text) -> (product_dict, meta).

`product_dict` must follow the schema in `prompts/extract_products.txt`.
`meta` keys expected: provider, model, input_tokens, output_tokens.
"""


@dataclass
class ExtractionOutcome:
    products: list[dict]
    latency_ms: int
    input_tokens: int
    output_tokens: int
    provider: str
    model: str
    prompt_version: str = "extract_products@v1"
    failures: list[dict] = field(default_factory=list)


def _load_prompt() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "Extract a product JSON from the transcript."


REQUIRED_FIELDS = ("product_name", "category")


def _validate_product_record(rec: dict) -> Optional[str]:
    for f in REQUIRED_FIELDS:
        if not rec.get(f):
            return f"missing field: {f}"
    persona = rec.get("target_persona") or {}
    label = persona.get("label", "")
    if label and not validate_persona_label(label):
        return f"bad persona label: {label!r}"
    return None


def extract_products(
    cleaned_text: str,
    *,
    segments: Optional[list[ts.Segment]] = None,
    llm_fn: ExtractLLMFn,
) -> ExtractionOutcome:
    paragraphs = split_products(cleaned_text, segments=segments)
    system_prompt = _load_prompt()
    start = time.monotonic()

    products: list[dict] = []
    failures: list[dict] = []
    total_in = total_out = 0
    provider = model = ""

    for para in paragraphs:
        try:
            rec, meta = llm_fn(system_prompt, para.text)
        except Exception as exc:  # noqa: BLE001
            failures.append({"position": para.position, "error": str(exc)})
            continue
        total_in += int(meta.get("input_tokens", 0))
        total_out += int(meta.get("output_tokens", 0))
        provider = meta.get("provider", provider)
        model = meta.get("model", model)

        err = _validate_product_record(rec)
        if err:
            failures.append({"position": para.position, "error": err, "record": rec})
            continue

        rec["position"] = para.position
        if para.start_sec is not None:
            rec["start_sec"] = int(para.start_sec)
        if para.end_sec is not None:
            rec["end_sec"] = int(para.end_sec)
        products.append(rec)

    return ExtractionOutcome(
        products=products,
        failures=failures,
        latency_ms=int((time.monotonic() - start) * 1000),
        input_tokens=total_in,
        output_tokens=total_out,
        provider=provider,
        model=model,
    )


# --------------------------------------------------------------------------- #
# Test-friendly fake LLM that parses a few marker products
# --------------------------------------------------------------------------- #


def fake_keyword_extractor(_system_prompt: str, paragraph_text: str) -> tuple[dict, dict]:
    """Deterministic extractor used in tests. Detects a couple of known products
    from the pilot transcript by keywords."""
    t = paragraph_text
    # Pick by keyword
    if "손전등" in t:
        product_name = "샤오미 다기능 손전등"
        category, sub = "차량용품", "비상 조명"
        keywords = ["차량용 후레쉬", "비상탈출 해머", "다기능 손전등"]
        persona = "30~50대 차박 캠핑러"
    elif "헬멧 카메라" in t or "인터" in t:
        product_name = "오토바이 헬멧 카메라 인터콤"
        category, sub = "오토바이용품", "헬멧 액세서리"
        keywords = ["오토바이 헤드셋", "오토바이 블랙박스"]
        persona = "30~40대 남성 라이더"
    elif "짐벌" in t:
        product_name = "스마트폰 3축 짐벌"
        category, sub = "촬영장비", "스마트폰 짐벌"
        keywords = ["스마트폰 짐벌", "브이로그 장비"]
        persona = "20~30대 1인 크리에이터"
    elif "마사지" in t:
        product_name = "전동 바디 진동 근막 링 마사지기"
        category, sub = "헬스케어", "마사지기"
        keywords = ["마사지건", "근막 마사지기"]
        persona = "30~40대 홈트족"
    elif "시트 쿠션" in t or "다리 받침" in t:
        product_name = "확장형 다리 받침 자동차 시트 쿠션"
        category, sub = "차량용품", "시트 액세서리"
        keywords = ["자동차 시트 쿠션", "다리 받침 쿠션"]
        persona = "30~50대 장거리 운전자"
    elif "전동 공구" in t or "드릴" in t:
        product_name = "DEKO 126종 전동 공구 콤보 키트"
        category, sub = "공구·DIY", "전동 드릴 세트"
        keywords = ["전동 드릴 세트", "공구세트"]
        persona = "30~50대 홈오너 DIY러"
    elif "스피커" in t:
        product_name = "아이야마 S400 액티브 북셀프 스피커"
        category, sub = "오디오", "북셀프 스피커"
        keywords = ["북셀프 스피커", "블루투스 스피커"]
        persona = "20~40대 오디오 입문자"
    elif "점프 스타터" in t or "공기 압축기" in t:
        product_name = "다기능 점프 스타터 파워뱅크"
        category, sub = "차량용품", "비상 점프 스타터"
        keywords = ["점프 스타터", "타이어 공기압 충전기"]
        persona = "30~50대 차박 캠핑러"
    elif "블렌더" in t or "믹서기" in t:
        product_name = "대용량 휴대용 블렌더 600ml"
        category, sub = "주방가전", "휴대용 믹서"
        keywords = ["휴대용 믹서기", "무선 블렌더"]
        persona = "20~30대 헬창·운동러"
    elif "수납박스" in t or "알루미늄" in t:
        product_name = "알루미늄 합금 캠핑 수납박스"
        category, sub = "캠핑용품", "수납·정리"
        keywords = ["캠핑 수납박스", "알루미늄 박스"]
        persona = "30~50대 차박 캠핑러"
    else:
        # Cannot match — return a degenerate stub so the test sees a failure
        return {"product_name": "", "category": ""}, {
            "provider": "fake",
            "model": "kw",
            "input_tokens": len(t),
            "output_tokens": 0,
        }

    record = {
        "product_name": product_name,
        "brand": "[?]",
        "brand_confidence": "low",
        "category": category,
        "subcategory": sub,
        "key_features": [],
        "specs": {},
        "price_range_usd": {"low": 20, "high": 150, "source": "estimate"},
        "target_use_case": [],
        "search_keywords_kr": keywords,
        "target_persona": {
            "label": persona,
            "gender": "male_dominant",
            "age_min": 30,
            "age_max": 50,
            "lifestyle_tags": [],
            "purchase_motivation": [],
            "rationale": "test",
        },
    }
    return record, {
        "provider": "fake",
        "model": "kw",
        "input_tokens": len(t),
        "output_tokens": 200,
    }
