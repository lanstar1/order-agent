"""
AI 상품소싱 — 네이버 쇼핑 인기 키워드 크롤링 + 시즌/스테디/경계 키워드 분석 시스템.

원본 naver-trend-maker(Cloudflare Worker + Next.js)를 FastAPI + 바닐라 JS SPA로
재구현한 모듈. 백필 런/태스크/스냅샷 오케스트레이션과 트렌드 분석 로직이 모두 포함된다.
"""
from __future__ import annotations

import logging
from fastapi import APIRouter, Header, HTTPException

from services.ai_sourcing.crawler import fetch_category_children, get_static_children, get_static_roots
from services.ai_sourcing.models import TrendProfileInput
from services.ai_sourcing.runs import (
    cancel_run,
    create_profile,
    delete_run,
    get_admin_board,
    get_run,
    get_run_snapshots_page,
    list_profiles,
    process_next_queued_run,
    retry_failed,
    start_backfill,
    start_collection,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ai-sourcing", tags=["AI Sourcing"])


def _require_auth(authorization: str = Header("")) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "인증이 필요합니다")
    from security import verify_token
    payload = verify_token(authorization.replace("Bearer ", ""))
    if not payload:
        raise HTTPException(401, "토큰이 유효하지 않습니다")
    return payload


@router.get("/health")
async def health():
    return {"ok": True, "service": "ai-sourcing"}


@router.get("/board")
async def admin_board(authorization: str = Header("")):
    _require_auth(authorization)
    board = await get_admin_board()
    return {"ok": True, "board": board.model_dump()}


@router.get("/profiles")
async def profiles(authorization: str = Header("")):
    _require_auth(authorization)
    items = await list_profiles()
    return {"ok": True, "profiles": [profile.model_dump() for profile in items]}


@router.post("/profiles")
async def profiles_create(body: TrendProfileInput, authorization: str = Header("")):
    _require_auth(authorization)
    return await create_profile(body)


@router.post("/collect")
async def collect(body: TrendProfileInput, authorization: str = Header("")):
    _require_auth(authorization)
    return await start_collection(body)


@router.get("/categories/{cid}")
async def categories(cid: int, authorization: str = Header("")):
    _require_auth(authorization)
    nodes = await fetch_category_children(cid)
    if not nodes:
        nodes = get_static_roots() if cid == 0 else get_static_children(cid)
    return {"ok": True, "nodes": nodes}


@router.get("/runs/{run_id}")
async def run_detail(run_id: str, authorization: str = Header("")):
    _require_auth(authorization)
    return await get_run(run_id)


@router.post("/runs/{run_id}/cancel")
async def run_cancel(run_id: str, authorization: str = Header("")):
    _require_auth(authorization)
    return await cancel_run(run_id)


@router.delete("/runs/{run_id}")
async def run_delete(run_id: str, authorization: str = Header("")):
    _require_auth(authorization)
    return await delete_run(run_id)


@router.get("/runs/{run_id}/snapshots")
async def run_snapshots(
    run_id: str,
    period: str = "",
    page: int = 1,
    authorization: str = Header(""),
):
    _require_auth(authorization)
    return await get_run_snapshots_page(run_id, period.strip(), max(1, page))


@router.post("/runs/{run_id}/retry-failures")
async def run_retry(run_id: str, authorization: str = Header("")):
    _require_auth(authorization)
    return await retry_failed(run_id)


@router.post("/profiles/{profile_id}/backfill")
async def profile_backfill(profile_id: str, authorization: str = Header("")):
    _require_auth(authorization)
    return await start_backfill(profile_id)


@router.post("/worker/process-next")
async def worker_process_next(authorization: str = Header("")):
    _require_auth(authorization)
    return await process_next_queued_run()
