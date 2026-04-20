"""신제품 소싱 파이프라인 엔드포인트 — 실제 LLM 호출 연결.

기존 routes/sourcing.py (CRUD·조회)는 건드리지 않고 새 라우트만 추가.
main.py에서 이 모듈을 include_router로 등록.

엔드포인트:
  POST /videos/{vid}/process           전사→보정→제품추출 일괄 실행
  POST /products/{pid}/analyze         시장성 분석 (네이버 3종 API + Claude)
  POST /products/{pid}/marketing       마케팅 자료 생성 ({kind: b2c/b2b/influencer})
  POST /products/{pid}/find-influencers YouTube Data API로 인플루언서 매칭
  POST /matches/{mid}/outreach-draft   컨택 초안 생성 + DB 저장
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from db.database import get_connection
from security import get_current_user

from services import video_processor
from services import market_analyzer as ma
from services import marketing_generator as mg
from services import influencer_finder as inf_find
from services import influencer_pricing as pricing
from services import outreach_service as outreach
from services import llm_adapters
from services.youtube_client import YouTubeClient
from services.naver_ad_client import NaverAdClient
from services.naver_datalab_client import NaverDataLabClient
from services.naver_search_client import NaverSearchClient


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sourcing", tags=["sourcing-pipeline"])


# ─── DB dependency (기존 sourcing.py와 동일 패턴) ──────────── #


def get_db():
    conn = get_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── Pydantic bodies ───────────────────────────────────────── #


class MarketingRequest(BaseModel):
    kind: str = Field(..., pattern="^(b2c|b2b|influencer)$")


class OutreachDraftBody(BaseModel):
    channel_kind: str = Field(..., pattern="^(email|instagram_dm)$")
    offer_kind: str = Field(..., pattern="^(gift|paid)$")


class ManualTranscriptBody(BaseModel):
    raw_transcript: str


# ─── 진단 — 쿠키·프록시 상태 확인 ──────────────────────────── #


@router.get("/diagnostics/cookies")
def diag_cookies(user=Depends(get_current_user)):
    """현재 설정된 YouTube 쿠키 상태를 반환. UI '🔍 진단' 버튼이 호출."""
    import time
    import os
    from pathlib import Path
    from services import cookies_manager

    env_file_val = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()
    env_text = os.environ.get("YOUTUBE_COOKIES_TEXT", "").strip()
    proxy_user = os.environ.get("YT_TRANSCRIPT_PROXY_USERNAME", "").strip()

    cookies_manager.reset_cache()  # 최신 env 반영
    resolved = cookies_manager.get_cookies_file_path()

    info: dict = {
        "env": {
            "YOUTUBE_COOKIES_FILE_set": bool(env_file_val),
            "YOUTUBE_COOKIES_FILE_value": env_file_val if env_file_val else None,
            "YOUTUBE_COOKIES_TEXT_length": len(env_text),
            "YT_TRANSCRIPT_PROXY_configured": bool(proxy_user),
        },
        "resolved_path": resolved,
        "path_exists": bool(resolved and Path(resolved).exists()),
        "status": "unknown",
    }

    if not resolved:
        info["status"] = "no_cookies_configured"
        info["hint"] = (
            "쿠키가 설정되지 않았습니다. Render Environment 에 "
            "YOUTUBE_COOKIES_TEXT 를 추가한 후 재배포해주세요."
        )
        return info

    # 파일 파싱
    try:
        from http.cookiejar import MozillaCookieJar
        jar = MozillaCookieJar(resolved)
        jar.load(ignore_discard=True, ignore_expires=True)
        cookies_list = list(jar)
    except Exception as exc:  # noqa: BLE001
        info["status"] = "parse_error"
        info["parse_error"] = str(exc)
        info["hint"] = (
            "cookies.txt 형식이 올바르지 않습니다. "
            "'Get cookies.txt LOCALLY' 확장에서 Netscape 형식으로 export 후 "
            "전체 내용 그대로 복사해주세요."
        )
        return info

    # YouTube / Google 도메인 쿠키 추출
    yt_cookies = [c for c in cookies_list
                  if c.domain.endswith("youtube.com")
                  or c.domain.endswith(".youtube.com")
                  or c.domain.endswith("google.com")
                  or c.domain.endswith(".google.com")]
    auth_names = {"SID", "HSID", "SSID", "APISID", "SAPISID",
                  "__Secure-1PSID", "__Secure-3PSID",
                  "__Secure-1PSIDTS", "__Secure-3PSIDTS", "LOGIN_INFO"}
    found_auth = sorted({c.name for c in yt_cookies if c.name in auth_names})
    now = time.time()
    expired_auth = [c.name for c in yt_cookies
                    if c.name in auth_names and c.expires and c.expires < now]

    info["total_cookies"] = len(cookies_list)
    info["youtube_domain_cookies"] = len(yt_cookies)
    info["auth_cookies_found"] = found_auth
    info["auth_cookies_expired"] = expired_auth

    if not found_auth:
        info["status"] = "no_auth_cookies"
        info["hint"] = (
            "로그인 세션 쿠키가 없습니다 (SID/HSID/APISID/SAPISID 등 발견 실패). "
            "youtube.com 에 로그인한 상태에서 cookies.txt 를 다시 내보내주세요."
        )
    elif expired_auth:
        info["status"] = "expired"
        info["hint"] = (
            f"만료된 쿠키: {expired_auth}. 브라우저에서 쿠키를 새로 추출해주세요."
        )
    else:
        info["status"] = "ok"
        info["hint"] = "쿠키가 정상 적용되어 있습니다. '처리 시작' 시도해보세요."

    return info


@router.post("/diagnostics/test-youtube")
def diag_test_youtube_access(user=Depends(get_current_user)):
    """쿠키로 실제 youtube.com 접속 테스트 — 로그인 상태 확인."""
    import requests
    from services import cookies_manager

    cookies_manager.reset_cache()
    path = cookies_manager.get_cookies_file_path()
    if path:
        session = cookies_manager.build_session_with_cookies(path)
    else:
        session = requests.Session()

    try:
        r = session.get("https://www.youtube.com/", timeout=15, allow_redirects=True)
        body_head = r.text[:8000]
        is_consent = "consent.youtube.com" in r.url
        looks_logged_in = (
            '"isLoggedIn":true' in body_head
            or 'accounts_switcher' in body_head
            or any(c.name == "LOGIN_INFO" for c in session.cookies)
        )
        has_bot_guard = (
            "unusual traffic" in body_head.lower()
            or ("bot" in body_head.lower() and "detected" in body_head.lower())
        )
        return {
            "ok": True,
            "http_status": r.status_code,
            "final_url": r.url,
            "redirected_to_consent": is_consent,
            "looks_logged_in": looks_logged_in,
            "has_bot_guard": has_bot_guard,
            "response_bytes": len(r.content),
            "cookies_sent": len(session.cookies),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


# ─── Video processing ─────────────────────────────────────── #


@router.post("/videos/{vid}/process")
def process_video(vid: int,
                  conn=Depends(get_db),
                  user=Depends(get_current_user)):
    """전사 → 보정 → 제품 추출 파이프라인을 동기 실행.

    영상 하나당 보통 15~60초. UI는 버튼 disable + 로딩 상태 표시 권장.
    """
    try:
        result = video_processor.process_video_full(conn, vid)
        return {
            "ok": True,
            **result.to_dict(),
            "message": (
                f"완료! 제품 {result.products_created}개 추출됨"
                if result.products_created
                else "전사·보정은 됐지만 제품을 추출하지 못했습니다 (자막 구조 확인 필요)"
            ),
        }
    except video_processor.ProcessingError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("[pipeline] video process failed")
        raise HTTPException(500, f"영상 처리 실패: {exc}")


@router.post("/videos/{vid}/upload-transcript")
def upload_manual_transcript(vid: int, body: ManualTranscriptBody,
                             conn=Depends(get_db),
                             user=Depends(get_current_user)):
    """사용자가 직접 붙여넣은 자막으로 파이프라인 실행.

    Render IP가 YouTube에 차단됐을 때 우회 경로. DownSub 같은 외부 서비스에서
    받은 SRT 또는 평문을 body.raw_transcript 에 넣어 POST.
    """
    try:
        result = video_processor.process_video_from_manual_transcript(
            conn, vid, body.raw_transcript,
        )
        return {
            "ok": True,
            **result.to_dict(),
            "message": (
                f"완료! 제품 {result.products_created}개 추출됨"
                if result.products_created
                else "전사·보정은 됐지만 제품을 추출하지 못했습니다"
            ),
        }
    except video_processor.ProcessingError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("[pipeline] manual transcript failed")
        raise HTTPException(500, f"자막 처리 실패: {exc}")


@router.post("/videos/{vid}/process-async")
def process_video_async(vid: int,
                        background: BackgroundTasks,
                        user=Depends(get_current_user)):
    """파이프라인을 백그라운드로 돌리고 즉시 응답. UI는 status polling 으로 확인."""
    # 상태만 '처리중'으로 즉시 표시
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE youtube_videos SET processed_status='in_progress', internal_step='queued' WHERE id=?",
            (vid,),
        )
        conn.commit()
    finally:
        conn.close()

    def _bg():
        bg_conn = get_connection()
        try:
            video_processor.process_video_full(bg_conn, vid)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[pipeline-async] video {vid} failed: {exc}")
        finally:
            try:
                bg_conn.close()
            except Exception:
                pass

    background.add_task(_bg)
    return {"ok": True, "message": "백그라운드에서 처리 중입니다. 잠시 후 새로고침해주세요."}


# ─── Market research ──────────────────────────────────────── #


def _naver_clients() -> tuple[Optional[NaverAdClient],
                               Optional[NaverDataLabClient],
                               Optional[NaverSearchClient]]:
    """환경변수가 있는 클라이언트만 반환. 없으면 None (조용히 skip)."""
    ad = dl = sch = None
    try:
        if (os.environ.get("NAVER_AD_API_KEY")
                and os.environ.get("NAVER_AD_CUSTOMER_ID")
                and os.environ.get("NAVER_AD_SECRET_KEY")):
            ad = NaverAdClient(
                api_key=os.environ["NAVER_AD_API_KEY"],
                secret=os.environ["NAVER_AD_SECRET_KEY"],
                customer_id=os.environ["NAVER_AD_CUSTOMER_ID"],
            )
    except Exception as exc:
        logger.warning(f"[pipeline] NaverAdClient init 실패: {exc}")

    try:
        cid = os.environ.get("NAVER_SEARCH_CLIENT_ID")
        csec = os.environ.get("NAVER_SEARCH_CLIENT_SECRET")
        if cid and csec:
            dl = NaverDataLabClient(client_id=cid, client_secret=csec)
            sch = NaverSearchClient(client_id=cid, client_secret=csec)
    except Exception as exc:
        logger.warning(f"[pipeline] Naver Client init 실패: {exc}")

    return ad, dl, sch


@router.post("/products/{pid}/analyze")
def analyze_product(pid: int,
                    conn=Depends(get_db),
                    user=Depends(get_current_user)):
    """시장성 분석 — 네이버 3종 API로 수치 수집 후 Claude 종합."""
    # Load product
    row = conn.execute(
        """SELECT id, product_name, brand, category, subcategory,
                  key_features, search_keywords_kr, target_persona
           FROM sourced_products WHERE id=?""",
        (pid,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "제품을 찾을 수 없습니다")

    product = {
        "id": row[0], "product_name": row[1], "brand": row[2],
        "category": row[3], "subcategory": row[4],
        "key_features": _loads(row[5]),
        "search_keywords_kr": _loads(row[6]),
        "target_persona": _loads(row[7]),
    }

    ad, dl, sch = _naver_clients()
    try:
        result = ma.run_analysis(
            product=product,
            ad_client=ad,
            search_client=sch,
            datalab_client=dl,
            synth_fn=llm_adapters.claude_synth_fn,
        )
        ma.persist_research(conn, result)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[pipeline] analyze failed")
        raise HTTPException(500, f"시장성 분석 실패: {exc}")

    return {
        "ok": True,
        "version": result.version,
        "market_size_score": result.market_size_score,
        "competition_score": result.competition_score,
        "opportunity_summary": result.opportunity_summary,
        "recommended_price_range_krw": result.recommended_price_range_krw,
        "risk_factors": result.risk_factors,
    }


# ─── Marketing assets ─────────────────────────────────────── #


@router.post("/products/{pid}/marketing")
def generate_marketing(pid: int, body: MarketingRequest,
                       conn=Depends(get_db),
                       user=Depends(get_current_user)):
    row = conn.execute(
        """SELECT id, product_name, brand, category, subcategory,
                  key_features, specs, target_persona
           FROM sourced_products WHERE id=?""",
        (pid,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "제품을 찾을 수 없습니다")

    product = {
        "id": row[0], "product_name": row[1], "brand": row[2],
        "category": row[3], "subcategory": row[4],
        "key_features": _loads(row[5]),
        "specs": _loads(row[6]),
        "target_persona": _loads(row[7]),
    }

    latest = ma.load_latest_research(conn, product_id=pid) or {}
    market = {
        "recommended_price_range_krw": latest.get("recommended_price_range_krw") or {},
        "opportunity_summary": latest.get("opportunity_summary", ""),
        "positioning_statement": "",
    }
    if not market["recommended_price_range_krw"]:
        raise HTTPException(
            400,
            "먼저 '시장성 분석'을 실행해주세요. 가격 추천 범위가 마케팅 자료 생성에 필요합니다.",
        )

    try:
        asset = mg.generate_asset(
            body.kind, product=product, market=market,
            synth_fn=llm_adapters.claude_marketing_fn,
        )
        aid = mg.persist_asset(conn, product_id=pid, asset=asset)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[pipeline] marketing failed")
        raise HTTPException(500, f"마케팅 자료 생성 실패: {exc}")

    return {
        "ok": True,
        "asset_id": aid,
        "kind": body.kind,
        "title": asset.title,
        "needs_human_review": asset.needs_human_review,
        "review_reasons": asset.review_reasons,
    }


# ─── Influencer finding ──────────────────────────────────── #


@router.post("/products/{pid}/find-influencers")
def find_influencers(pid: int,
                     conn=Depends(get_db),
                     user=Depends(get_current_user)):
    key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if not key:
        raise HTTPException(400, "YOUTUBE_API_KEY 환경변수가 필요합니다.")

    row = conn.execute(
        "SELECT id, product_name, search_keywords_kr, target_persona, category "
        "FROM sourced_products WHERE id=?", (pid,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "제품을 찾을 수 없습니다")

    keywords = _loads(row[2]) or []
    persona = _loads(row[3]) or {}
    category = row[4] or ""
    # 키워드 3개만 (쿼터 절감)
    keywords = keywords[:3]
    if not keywords:
        # 페르소나 + 카테고리로 폴백
        label = persona.get("label") or category
        if label:
            keywords = [label]
        else:
            raise HTTPException(400, "검색 키워드가 없습니다. 먼저 제품 추출을 해주세요.")

    yt = YouTubeClient(key)
    try:
        candidates = inf_find.find_candidates(youtube=yt, keywords=keywords)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[pipeline] find influencers failed")
        raise HTTPException(500, f"인플루언서 검색 실패: {exc}")

    # 단가 산정 (상위 후보만)
    for c in candidates:
        if c.excluded:
            continue
        snap = c.channel_snapshot
        quote = pricing.estimate_quote(
            platform="youtube",
            avg_views=snap.avg_views,
            engagement_rate_pct=snap.engagement_rate,
            content_format="integrated_review",
            category="camping" if "캠핑" in (category + (persona.get("label") or "")) else "daily",
        )
        # match 에 단가 저장 — persist_matches 직전에 channel_snapshot에 담을 수 없어
        # 우선 전량 persist 후 quote UPDATE
    match_ids = inf_find.persist_matches(conn, product_id=pid, candidates=candidates)

    # 단가·견적 UPDATE
    updated = 0
    for c, mid in zip(candidates, match_ids):
        if c.excluded:
            continue
        snap = c.channel_snapshot
        quote = pricing.estimate_quote(
            platform="youtube",
            avg_views=snap.avg_views,
            engagement_rate_pct=snap.engagement_rate,
            content_format="integrated_review",
        )
        conn.execute(
            "UPDATE product_influencer_matches SET estimated_quote_krw=?, quote_breakdown=? WHERE id=?",
            (
                quote.raw_quote_krw,
                json.dumps({
                    "low": quote.low_krw, "high": quote.high_krw,
                    "cpm": quote.cpm_rate, "fmt": quote.format_multiplier,
                    "er_bonus": quote.engagement_bonus,
                    "notes": quote.notes,
                }, ensure_ascii=False),
                mid,
            ),
        )
        updated += 1
    conn.commit()

    accepted = sum(1 for c in candidates if not c.excluded)
    return {
        "ok": True,
        "total_found": len(candidates),
        "accepted": accepted,
        "excluded": len(candidates) - accepted,
        "message": f"후보 {accepted}명 추출 (제외 {len(candidates)-accepted}명)",
    }


# ─── Outreach draft generation ──────────────────────────── #


@router.post("/matches/{mid}/outreach-draft")
def create_outreach_draft(mid: int, body: OutreachDraftBody,
                          conn=Depends(get_db),
                          user=Depends(get_current_user)):
    row = conn.execute(
        """SELECT m.id, m.product_id, m.influencer_id, m.estimated_quote_krw, m.quote_breakdown,
                  i.platform, i.handle, i.display_name, i.follower_count,
                  i.avg_views, i.engagement_rate, i.contact_email,
                  sp.product_name, sp.key_features, sp.target_persona
           FROM product_influencer_matches m
           JOIN influencers i ON i.id=m.influencer_id
           JOIN sourced_products sp ON sp.id=m.product_id
           WHERE m.id=?""",
        (mid,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "매칭을 찾을 수 없습니다")

    quote_bd = _loads(row[4]) or {}
    influencer = {
        "platform": row[5], "handle": row[6], "display_name": row[7],
        "follower_count": row[8], "avg_views": row[9],
        "engagement_rate": row[10], "contact_email": row[11],
    }
    product = {
        "name": row[12],
        "key_features": _loads(row[13]),
        "target_persona": _loads(row[14]),
    }
    sender = {
        "name": "(주)랜스타 신제품 소싱팀",
        "contact_email": "kyu@lanstar.co.kr",
    }
    estimated_quote = {
        "raw_quote_krw": row[3] or 0,
        "low_krw": quote_bd.get("low", 0),
        "high_krw": quote_bd.get("high", 0),
    }

    try:
        draft = outreach.generate_draft(
            match_id=mid,
            influencer=influencer,
            product=product,
            offer_kind=body.offer_kind,
            channel_kind=body.channel_kind,
            sender=sender,
            estimated_quote=estimated_quote,
            synth_fn=llm_adapters.claude_outreach_fn,
        )
        did = outreach.persist_draft(conn, draft)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[pipeline] outreach draft failed")
        raise HTTPException(500, f"컨택 초안 생성 실패: {exc}")

    return {
        "ok": True,
        "draft_id": did,
        "subject": draft.subject,
        "body": draft.body,
        "product_proposal": draft.product_proposal,
    }


# ─── helper ──────────────────────────────────────────────── #


def _loads(s):
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except Exception:
        return None
