"""Marketing asset generator — B2C, B2B, Influencer.

Calls Claude (or a fake `synth_fn`) three times, each with its own prompt
template, and persists results into `marketing_assets`.

Guards:
- **forbidden_claim_check**: scans generated text for certification/warranty
  claims that are NOT in the product DB. Replaces them with "[확인 필요]".
- **bundle price range**: B2C bundle prices must fall inside the recommended
  price range; otherwise the asset is flagged `needs_human_review`.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

PROMPTS_DIR = Path(__file__).resolve().parents[1] / "prompts"

PROMPT_FILES = {
    "b2c":        PROMPTS_DIR / "marketing_b2c.txt",
    "b2b":        PROMPTS_DIR / "marketing_b2b.txt",
    "influencer": PROMPTS_DIR / "marketing_influencer.txt",
}

PROMPT_VERSIONS = {
    "b2c":        "marketing_b2c@v1",
    "b2b":        "marketing_b2b@v1",
    "influencer": "marketing_influencer@v1",
}


# Claims that MUST come from the product DB, not the LLM.
# If these appear in the output text, they will be scrubbed.
FORBIDDEN_CLAIM_PATTERNS = [
    re.compile(r"KC\s*인증\s*(완료|받은|취득|획득)"),
    re.compile(r"IP\s*\d{2}\s*(등급|방수|인증)"),
    re.compile(r"(\d+)\s*(개월|년)\s*(무상\s*)?A/?S"),
    re.compile(r"무상\s*A/?S\s*(\d+)\s*(개월|년)"),
    re.compile(r"CE\s*(인증|마크)"),
    re.compile(r"FCC\s*(인증|마크)"),
    re.compile(r"식약처\s*(인증|허가)"),
    re.compile(r"무선국\s*인증"),
]


@dataclass
class MarketingAsset:
    kind: str                          # b2c / b2b / influencer
    title: str
    body_markdown: str                 # human-readable form
    metadata: dict                     # raw JSON payload from the LLM
    prompt_version: str
    needs_human_review: bool = False
    review_reasons: list[str] = field(default_factory=list)
    llm_meta: dict = field(default_factory=dict)


MarketingFn = Callable[[str, dict], tuple[dict, dict]]
"""(system_prompt, context) -> (json_payload, llm_meta)"""


def _load_prompt(kind: str) -> str:
    return PROMPT_FILES[kind].read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Claim guard
# --------------------------------------------------------------------------- #


def scan_forbidden_claims(text: str, allowed_specs: dict) -> list[str]:
    """Return the list of forbidden claim substrings the LLM emitted that are
    NOT backed by a matching key in allowed_specs."""
    hits: list[str] = []
    allowed_joined = " ".join(
        str(v) for v in allowed_specs.values() if v is not None
    ).lower()
    for pat in FORBIDDEN_CLAIM_PATTERNS:
        for m in pat.finditer(text):
            hit = m.group(0)
            if hit.lower() not in allowed_joined:
                hits.append(hit)
    return hits


def scrub_forbidden_claims(text: str, allowed_specs: dict) -> tuple[str, list[str]]:
    """Replace forbidden claims with '[확인 필요]' and return (scrubbed, hits)."""
    hits: list[str] = []
    allowed_joined = " ".join(
        str(v) for v in allowed_specs.values() if v is not None
    ).lower()
    def _sub(m: re.Match) -> str:
        hit = m.group(0)
        if hit.lower() in allowed_joined:
            return hit
        hits.append(hit)
        return "[확인 필요]"
    out = text
    for pat in FORBIDDEN_CLAIM_PATTERNS:
        out = pat.sub(_sub, out)
    return out, hits


def _walk_strings(obj, fn: Callable[[str], str]) -> None:
    """Mutate all string values under obj in-place using fn."""
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if isinstance(v, str):
                obj[k] = fn(v)
            else:
                _walk_strings(v, fn)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str):
                obj[i] = fn(v)
            else:
                _walk_strings(v, fn)


def _price_guard_b2c(payload: dict, recommended: dict) -> list[str]:
    """Return list of issues if bundle prices violate the recommended range."""
    reasons: list[str] = []
    if not recommended:
        return reasons
    low = recommended.get("low") or 0
    high = recommended.get("high") or 0
    for bundle in payload.get("bundle_proposals") or []:
        tier = bundle.get("tier")
        price = int(bundle.get("price_krw") or 0)
        if tier == "bait" and low and price < low * 0.7:
            reasons.append(f"bundle:bait price {price} too low vs recommended low {low}")
        if tier == "crosssell" and high and price < high:
            reasons.append(f"bundle:crosssell price {price} below high {high}")
    return reasons


# --------------------------------------------------------------------------- #
# Rendering: JSON payload → human-readable markdown
# --------------------------------------------------------------------------- #


def render_markdown(kind: str, payload: dict) -> str:
    if kind == "b2c":
        return _render_b2c_md(payload)
    if kind == "b2b":
        return _render_b2b_md(payload)
    return _render_influencer_md(payload)


def _render_b2c_md(p: dict) -> str:
    out = [f"# {p.get('title','(제목 없음)')}\n",
           f"**{p.get('hero_headline','')}**\n",
           "## 페인포인트 시나리오"]
    for s in p.get("painpoint_scenarios", []):
        out.append(f"- {s}")
    out.append("\n## 핵심 셀링 5가지")
    for i, sp in enumerate(p.get("selling_points", []), 1):
        out.append(f"**{i}. {sp.get('solution','')}**")
        out.append(f"- 문제: {sp.get('problem','')}")
        out.append(f"- 기대효과: {sp.get('benefit','')}")
        if sp.get("visual_hint"):
            out.append(f"- 시각: {sp['visual_hint']}")
    out.append("\n## FAQ — 방어형")
    for q in (p.get("faq") or {}).get("defensive", []):
        out.append(f"- Q. {q['q']}\n  A. {q['a']}")
    out.append("\n## FAQ — 전환 유도형")
    for q in (p.get("faq") or {}).get("conversion", []):
        out.append(f"- Q. {q['q']}\n  A. {q['a']}")
    kws = p.get("seo_keywords") or {}
    out.append("\n## SEO 키워드")
    out.append(f"- 메인: {' · '.join(kws.get('main', []))}")
    out.append(f"- 서브: {' · '.join(kws.get('sub', []))}")
    out.append(f"- 해시태그: {' '.join(kws.get('hashtags', []))}")
    out.append("\n## 묶음 제안")
    for b in p.get("bundle_proposals") or []:
        out.append(f"- **{b.get('tier')}** · ₩{b.get('price_krw'):,} — "
                   f"{b.get('spec','')} ({b.get('angle','')})")
    return "\n".join(out)


def _render_b2b_md(p: dict) -> str:
    one = p.get("one_pager") or {}
    out = ["# B2B 벤더 전략\n",
           "## 제품 개요", one.get("product_overview", ""), "",
           "## 핵심 스펙"]
    for row in one.get("specs_table") or []:
        out.append(f"- {row.get('label')}: {row.get('value')}")
    st = one.get("supply_terms") or {}
    out.append("\n## 공급 조건")
    out.append(f"- MSRP: ₩{(st.get('msrp_krw') or 0):,}")
    out.append(f"- 공급가: ₩{(st.get('wholesale_krw') or 0):,}")
    out.append(f"- 마진율: {st.get('margin_pct_min')}~{st.get('margin_pct_max')}%")
    out.append(f"- MOQ: {st.get('moq')}")
    out.append(f"- 오픈 기간: {st.get('open_window')}")
    out.append(f"- 정산: {st.get('settlement')}")
    out.append("\n## 초도 프로모션")
    for b in one.get("promotion_opening") or []:
        out.append(f"- {b}")
    out.append("\n## 판매 근거")
    for r in one.get("sales_rationale") or []:
        out.append(f"- {r.get('metric')}: {r.get('value')}")
    cm = p.get("cold_mail") or {}
    out.append("\n## 콜드 메일 초안")
    out.append(f"**제목**: {cm.get('subject','')}")
    out.append("")
    out.append(cm.get("body", ""))
    return "\n".join(out)


def _render_influencer_md(p: dict) -> str:
    out = ["# 인플루언서 마케팅 전략\n"]
    mix = p.get("channel_mix") or {}
    out.append(f"**채널 믹스**: 메가 {mix.get('mega_macro_ratio',1)} : "
               f"마이크로 {mix.get('micro_ratio',3)} : 나노 {mix.get('nano_ratio',6)}")
    if mix.get("notes"):
        out.append(f"- {mix['notes']}")
    fg = p.get("format_guide") or {}
    out.append("\n## 포맷 가이드")
    out.append(f"- 유튜브: {fg.get('youtube','')}")
    out.append(f"- 숏폼: {fg.get('shortform','')}")
    out.append(f"- 블로그: {fg.get('blog','')}")
    va = p.get("viral_assets") or {}
    sc = va.get("shortform_script_30s") or {}
    out.append("\n## 30초 숏폼 스크립트")
    out.append(f"- 훅(0~3초): {sc.get('hook_0_3s','')}")
    out.append(f"- 문제(3~10초): {sc.get('problem_3_10s','')}")
    out.append(f"- 해결(10~23초): {sc.get('solution_10_23s','')}")
    out.append(f"- CTA(23~30초): {sc.get('cta_23_30s','')}")
    out.append("\n## 카드뉴스 6컷")
    for c in va.get("cardnews_6") or []:
        out.append(f"{c.get('idx')}/6 — {c.get('copy','')} ({c.get('visual','')})")
    ch = va.get("community_hooks") or {}
    out.append("\n## 커뮤니티 시딩 훅")
    out.append(f"- 맘카페: {ch.get('mom_cafe','')}")
    out.append(f"- 보배드림: {ch.get('boba_dream','')}")
    out.append(f"- 디시 갤러리: {ch.get('dc_gallery','')}")
    if p.get("ad_disclosure_notice"):
        out.append(f"\n> {p['ad_disclosure_notice']}")
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #


def generate_asset(
    kind: str,
    *,
    product: dict,
    market: dict,
    company_sender: Optional[dict] = None,
    synth_fn: MarketingFn,
) -> MarketingAsset:
    if kind not in PROMPT_FILES:
        raise ValueError(f"unknown marketing kind: {kind}")

    system_prompt = _load_prompt(kind)
    context = {
        "product": product,
        "market": market,
    }
    if kind == "b2b":
        context["company_sender"] = company_sender or {
            "name": "(주)랜스타 신제품 소싱팀",
            "contact_email": "kyu@lanstar.co.kr",
        }

    payload, meta = synth_fn(system_prompt, context)

    # Claim-guard: walk every string in the JSON
    specs = (product.get("specs") or {})
    review_reasons: list[str] = []
    all_hits: list[str] = []

    def _scrub(s: str) -> str:
        scrubbed, hits = scrub_forbidden_claims(s, specs)
        if hits:
            all_hits.extend(hits)
        return scrubbed

    _walk_strings(payload, _scrub)
    if all_hits:
        review_reasons.append(
            f"scrubbed unverifiable claims: {sorted(set(all_hits))[:5]}"
        )

    # B2C price guard
    if kind == "b2c":
        issues = _price_guard_b2c(payload, market.get("recommended_price_range_krw") or {})
        if issues:
            review_reasons.extend(issues)

    body_md = render_markdown(kind, payload)
    title = _asset_title(kind, payload, product)

    return MarketingAsset(
        kind=kind,
        title=title,
        body_markdown=body_md,
        metadata=payload,
        prompt_version=PROMPT_VERSIONS[kind],
        needs_human_review=bool(review_reasons),
        review_reasons=review_reasons,
        llm_meta=meta or {},
    )


def _asset_title(kind: str, payload: dict, product: dict) -> str:
    name = product.get("product_name") or "(unknown)"
    if kind == "b2c":
        return payload.get("title") or f"B2C · {name}"
    if kind == "b2b":
        subj = (payload.get("cold_mail") or {}).get("subject", "")
        return subj or f"B2B · {name}"
    return f"인플루언서 · {name}"


def generate_all_kinds(
    *,
    product: dict,
    market: dict,
    synth_fn: MarketingFn,
    company_sender: Optional[dict] = None,
) -> list[MarketingAsset]:
    """Generate b2c, b2b, influencer in sequence. Each failure becomes a stub."""
    assets: list[MarketingAsset] = []
    for kind in ("b2c", "b2b", "influencer"):
        try:
            asset = generate_asset(
                kind, product=product, market=market,
                company_sender=company_sender, synth_fn=synth_fn,
            )
        except Exception as exc:  # noqa: BLE001
            asset = MarketingAsset(
                kind=kind, title=f"(failed) {kind}", body_markdown="",
                metadata={}, prompt_version=PROMPT_VERSIONS[kind],
                needs_human_review=True, review_reasons=[str(exc)],
            )
        assets.append(asset)
    return assets


def persist_asset(conn, product_id: int, asset: MarketingAsset) -> int:
    # conn.execute() (not cursor) for order-agent's _sql_to_pg translation.
    cur = conn.execute(
        """INSERT INTO marketing_assets
           (product_id, kind, title, body_markdown, metadata, prompt_version)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            product_id, asset.kind, asset.title, asset.body_markdown,
            json.dumps(asset.metadata, ensure_ascii=False),
            asset.prompt_version,
        ),
    )
    conn.commit()
    return cur.lastrowid
