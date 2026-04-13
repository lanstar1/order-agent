"""
Content Factory — 콘텐츠 자동화 API
"""
import json
import logging
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from security import get_current_user
from db.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/content", tags=["content"])


# ── Pydantic Models ──

class ContentGenerate(BaseModel):
    source_id: Optional[int] = None
    platform: str = "threads"
    content_type: str = "inertia_break"
    manual_text: Optional[str] = None

class ContentUpdate(BaseModel):
    body: Optional[str] = None
    title: Optional[str] = None
    hashtags: Optional[str] = None
    scheduled_at: Optional[str] = None

class ContentSchedule(BaseModel):
    platform: str = "threads"
    scheduled_at: str

class ContentPublish(BaseModel):
    platform: str = "threads"

class SourceCreate(BaseModel):
    source_type: str = "manual"
    title: str
    summary: Optional[str] = None
    source_url: Optional[str] = None


# ── 소재함 ──

@router.get("/sources")
async def list_sources(
    status: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None),
    min_score: Optional[float] = Query(None),
    limit: int = Query(20),
    user: dict = Depends(get_current_user),
):
    conn = get_connection()
    try:
        sql = "SELECT * FROM content_sources WHERE 1=1"
        params = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if source_type:
            sql += " AND source_type = ?"
            params.append(source_type)
        if min_score is not None:
            sql += " AND relevance_score >= ?"
            params.append(min_score)
        sql += " ORDER BY collected_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return {"sources": [dict(r) for r in rows], "total": len(rows)}
    finally:
        conn.close()


@router.post("/sources")
async def create_source(data: SourceCreate, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO content_sources (source_type, title, summary, source_url, status, collected_at) VALUES (?, ?, ?, ?, 'pending', NOW())",
            (data.source_type, data.title, data.summary, data.source_url)
        )
        conn.commit()
        return {"id": cur.lastrowid, "message": "소재 추가됨"}
    finally:
        conn.close()


@router.post("/sources/collect")
async def trigger_collection(user: dict = Depends(get_current_user)):
    from services.content_service import collect_all_sources
    result = await collect_all_sources()
    return {"message": "수집 완료", "collected": result}


@router.post("/sources/{source_id}/evaluate")
async def evaluate_source(source_id: int, user: dict = Depends(get_current_user)):
    from services.content_service import evaluate_source_relevance
    return await evaluate_source_relevance(source_id)


# ── 콘텐츠 ──

@router.get("/items")
async def list_items(
    status: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    content_type: Optional[str] = Query(None),
    limit: int = Query(20),
    user: dict = Depends(get_current_user),
):
    conn = get_connection()
    try:
        sql = "SELECT * FROM content_items WHERE 1=1"
        params = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if platform:
            sql += " AND platform = ?"
            params.append(platform)
        if content_type:
            sql += " AND content_type = ?"
            params.append(content_type)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return {"items": [dict(r) for r in rows], "total": len(rows)}
    finally:
        conn.close()


@router.post("/items/generate")
async def generate_content(data: ContentGenerate, user: dict = Depends(get_current_user)):
    from services.content_service import generate_content_from_source
    return await generate_content_from_source(
        source_id=data.source_id,
        platform=data.platform,
        content_type=data.content_type,
        manual_text=data.manual_text,
    )


@router.put("/items/{item_id}")
async def update_item(item_id: int, data: ContentUpdate, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        sets, params = [], []
        if data.body is not None:
            sets.append("body = ?"); params.append(data.body)
        if data.title is not None:
            sets.append("title = ?"); params.append(data.title)
        if data.hashtags is not None:
            sets.append("hashtags = ?"); params.append(data.hashtags)
        if data.scheduled_at is not None:
            sets.append("scheduled_at = ?"); params.append(data.scheduled_at)
        if not sets:
            raise HTTPException(400, "수정할 내용 없음")
        sets.append("updated_at = NOW()")
        params.append(item_id)
        conn.execute(f"UPDATE content_items SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        return {"message": "수정 완료"}
    finally:
        conn.close()


@router.put("/items/{item_id}/approve")
async def approve_item(item_id: int, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute("UPDATE content_items SET status = 'approved', updated_at = NOW() WHERE id = ?", (item_id,))
        conn.commit()
        return {"message": "승인됨"}
    finally:
        conn.close()


@router.put("/items/{item_id}/reject")
async def reject_item(item_id: int, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute("UPDATE content_items SET status = 'rejected', updated_at = NOW() WHERE id = ?", (item_id,))
        conn.commit()
        return {"message": "폐기됨"}
    finally:
        conn.close()


@router.post("/items/{item_id}/regenerate")
async def regenerate_item(item_id: int, user: dict = Depends(get_current_user)):
    from services.content_service import regenerate_content
    return await regenerate_content(item_id)


# ── 발행 ──

@router.post("/items/{item_id}/publish")
async def publish_item(item_id: int, data: ContentPublish, user: dict = Depends(get_current_user)):
    from services.content_service import publish_content
    return await publish_content(item_id, data.platform)


@router.get("/publish/status")
async def get_publish_status(user: dict = Depends(get_current_user)):
    from services.content_service import check_sns_connection
    return await check_sns_connection()


# ── 스케줄 ──

@router.post("/items/{item_id}/schedule")
async def schedule_item(item_id: int, data: ContentSchedule, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE content_items SET status = 'scheduled', platform = ?, scheduled_at = ?, updated_at = NOW() WHERE id = ?",
            (data.platform, data.scheduled_at, item_id)
        )
        conn.commit()
        return {"message": f"ID={item_id} 예약 완료"}
    finally:
        conn.close()


@router.delete("/items/{item_id}/schedule")
async def cancel_schedule(item_id: int, user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        conn.execute("UPDATE content_items SET status = 'approved', scheduled_at = NULL, updated_at = NOW() WHERE id = ? AND status = 'scheduled'", (item_id,))
        conn.commit()
        return {"message": f"ID={item_id} 예약 취소"}
    finally:
        conn.close()


@router.get("/schedule/queue")
async def get_queue(user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, platform, content_type, title,
                   substring(body from 1 for 100) as body_preview,
                   scheduled_at, status
            FROM content_items
            WHERE status IN ('scheduled', 'approved') AND scheduled_at IS NOT NULL
            ORDER BY scheduled_at ASC LIMIT 30
        """).fetchall()
        return {"queue": [dict(r) for r in rows], "total": len(rows)}
    finally:
        conn.close()


# ── 분석 ──

@router.get("/analytics")
async def get_analytics(user: dict = Depends(get_current_user)):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT ci.content_type, ci.platform, COUNT(*) as total
            FROM content_items ci
            WHERE ci.status = 'published'
            GROUP BY ci.content_type, ci.platform
        """).fetchall()
        return {"analytics": [dict(r) for r in rows]}
    finally:
        conn.close()
