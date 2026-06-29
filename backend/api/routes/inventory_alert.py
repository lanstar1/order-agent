"""
재고 변동 모니터링 API 라우터
- 수동 실행, 설정 관리, 이력 조회, 키워드 관리
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from db.database import get_connection
from services.inventory_monitor import (
    run_inventory_monitor,
    get_exclude_keywords,
    add_exclude_keyword,
    remove_exclude_keyword,
    get_alert_settings,
    update_alert_settings,
    get_snapshot,
    load_products_master,
    compare_inventory,
)
from services.telegram_service import TelegramService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/inventory-monitor", tags=["inventory-monitor"])

KST = timezone(timedelta(hours=9))


# ─── Pydantic 모델 ──────────────────────────────────────────────

class AlertSettingsUpdate(BaseModel):
    threshold_amount: Optional[int] = None
    threshold_qty: Optional[int] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    enabled: Optional[bool] = None

class KeywordAction(BaseModel):
    keyword: str


# ─── 수동 실행 ──────────────────────────────────────────────────

@router.post("/run")
async def manual_run():
    """재고 모니터링 수동 실행"""
    conn = get_connection()
    try:
        settings = get_alert_settings(conn)
    finally:
        conn.close()

    telegram = None
    if settings["telegram_bot_token"] and settings["telegram_chat_id"]:
        telegram = TelegramService(settings["telegram_bot_token"], settings["telegram_chat_id"])

    result = await run_inventory_monitor(telegram_service=telegram)
    return result


# ─── 알림 이력 조회 ─────────────────────────────────────────────

@router.get("/history")
async def get_alert_history(days: int = 30, page: int = 1, per_page: int = 50):
    """알림 이력 조회"""
    now = datetime.now(KST)
    start_date = (now - timedelta(days=days)).strftime("%Y%m%d")

    conn = get_connection()
    try:
        total_row = conn.execute(
            "SELECT COUNT(*) FROM inventory_alert_history WHERE check_date >= ?",
            (start_date,)
        ).fetchone()
        total = total_row[0] if total_row else 0

        offset = (page - 1) * per_page
        rows = conn.execute(
            """SELECT check_date, prod_cd, prod_name, model_name, unit_price,
                      prev_qty, curr_qty, diff_qty, diff_amount, trigger_type, created_at
               FROM inventory_alert_history
               WHERE check_date >= ?
               ORDER BY check_date DESC, diff_amount DESC
               LIMIT ? OFFSET ?""",
            (start_date, per_page, offset)
        ).fetchall()

        items = []
        for row in rows:
            items.append({
                "check_date": row[0], "prod_cd": row[1], "prod_name": row[2],
                "model_name": row[3], "unit_price": row[4], "prev_qty": row[5],
                "curr_qty": row[6], "diff_qty": row[7], "diff_amount": row[8],
                "trigger_type": row[9], "created_at": row[10],
            })
    finally:
        conn.close()

    return {"total": total, "page": page, "per_page": per_page, "items": items}


# ─── 날짜별 비교 ───────────────────────────────────────────────

@router.get("/compare")
async def compare_dates(date1: str, date2: str):
    """두 날짜의 스냅샷을 비교"""
    conn = get_connection()
    try:
        snap1 = get_snapshot(conn, date1)
        snap2 = get_snapshot(conn, date2)
        settings = get_alert_settings(conn)
        exclude_keywords = get_exclude_keywords(conn)
    finally:
        conn.close()

    if not snap1:
        raise HTTPException(status_code=404, detail=f"{date1} 스냅샷이 없습니다")
    if not snap2:
        raise HTTPException(status_code=404, detail=f"{date2} 스냅샷이 없습니다")

    products_master = load_products_master()
    alerts = compare_inventory(
        prev_snapshot=snap1, curr_snapshot=snap2,
        products_master=products_master, exclude_keywords=exclude_keywords,
        threshold_amount=settings["threshold_amount"],
        threshold_qty=settings["threshold_qty"],
    )

    return {"date1": date1, "date2": date2, "total_alerts": len(alerts), "alerts": alerts}


# ─── 스냅샷 목록 ───────────────────────────────────────────────

@router.get("/snapshots")
async def list_snapshots(days: int = 30):
    """저장된 스냅샷 날짜 목록"""
    now = datetime.now(KST)
    start_date = (now - timedelta(days=days)).strftime("%Y%m%d")

    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT snapshot_date, COUNT(*) as item_count, MIN(created_at) as created_at
               FROM inventory_snapshots
               WHERE snapshot_date >= ?
               GROUP BY snapshot_date
               ORDER BY snapshot_date DESC""",
            (start_date,)
        ).fetchall()
    finally:
        conn.close()

    return [{"date": row[0], "item_count": row[1], "created_at": row[2]} for row in rows]


# ─── 설정 관리 ──────────────────────────────────────────────────

@router.get("/settings")
async def get_settings():
    conn = get_connection()
    try:
        settings = get_alert_settings(conn)
    finally:
        conn.close()

    if settings["telegram_bot_token"]:
        token = settings["telegram_bot_token"]
        settings["telegram_bot_token_masked"] = token[:10] + "..." + token[-5:]
    return settings


@router.put("/settings")
async def update_settings_api(body: AlertSettingsUpdate):
    conn = get_connection()
    try:
        update_data = {k: v for k, v in body.dict().items() if v is not None}
        if "enabled" in update_data:
            update_data["enabled"] = "true" if update_data["enabled"] else "false"
        update_alert_settings(conn, update_data)
    finally:
        conn.close()
    return {"status": "ok", "updated": list(update_data.keys())}


# ─── 키워드 관리 ────────────────────────────────────────────────

@router.get("/keywords")
async def list_keywords():
    conn = get_connection()
    try:
        keywords = get_exclude_keywords(conn)
    finally:
        conn.close()
    return {"keywords": keywords}


@router.post("/keywords")
async def add_keyword_api(body: KeywordAction):
    conn = get_connection()
    try:
        add_exclude_keyword(conn, body.keyword)
    finally:
        conn.close()
    return {"status": "ok", "keyword": body.keyword}


@router.delete("/keywords/{keyword}")
async def delete_keyword_api(keyword: str):
    conn = get_connection()
    try:
        remove_exclude_keyword(conn, keyword)
    finally:
        conn.close()
    return {"status": "ok", "keyword": keyword}


# ─── 텔레그램 테스트 ───────────────────────────────────────────

@router.post("/telegram/test")
async def test_telegram():
    conn = get_connection()
    try:
        settings = get_alert_settings(conn)
    finally:
        conn.close()

    if not settings["telegram_bot_token"] or not settings["telegram_chat_id"]:
        raise HTTPException(status_code=400, detail="텔레그램 봇 토큰과 chat_id를 먼저 설정해주세요")

    telegram = TelegramService(settings["telegram_bot_token"], settings["telegram_chat_id"])
    bot_info = await telegram.test_connection()
    if not bot_info.get("ok"):
        raise HTTPException(status_code=400, detail=f"봇 연결 실패: {bot_info.get('error', '')}")

    now_str = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    test_msg = (
        "✅ <b>재고 모니터링 텔레그램 연결 테스트</b>\n\n"
        f"봇: @{bot_info.get('bot_username', '')}\n"
        f"시간: {now_str}\n\n"
        "이 메시지가 보이면 텔레그램 알림이 정상적으로 작동합니다!"
    )
    send_result = await telegram.send_message(test_msg)

    return {"bot_info": bot_info, "message_sent": send_result.get("ok", False)}
