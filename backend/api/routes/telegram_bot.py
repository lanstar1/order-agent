"""
텔레그램 봇 양방향 통신
- Webhook으로 사용자 메시지/버튼 클릭 수신
- 명령어 처리: 승인, 스캔, 상태, 도움
"""

import logging
import json
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request
from db.database import get_connection
from services.telegram_service import TelegramService

router = APIRouter(prefix="/api/telegram", tags=["telegram-bot"])
logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


def _get_telegram():
    """DB에서 텔레그램 설정 로드"""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT key, value FROM inventory_alert_settings").fetchall()
        settings = {row[0]: row[1] for row in rows}
        bot_token = settings.get("telegram_bot_token", "")
        chat_id = settings.get("telegram_chat_id", "")
        if bot_token and chat_id:
            return TelegramService(bot_token, chat_id), chat_id
    except Exception:
        pass
    return None, None


@router.post("/webhook")
async def telegram_webhook(request: Request):
    """텔레그램 Webhook — 메시지/콜백 수신"""
    body = await request.json()
    
    # 콜백 쿼리 (인라인 버튼 클릭)
    callback = body.get("callback_query")
    if callback:
        data = callback.get("data", "")
        chat_id = str(callback.get("message", {}).get("chat", {}).get("id", ""))
        callback_id = callback.get("id", "")
        
        telegram, _ = _get_telegram()
        if telegram:
            # 버튼 클릭 응답
            await _answer_callback(telegram, callback_id)
            await _handle_command(telegram, data, chat_id)
        return {"ok": True}
    
    # 일반 메시지
    message = body.get("message", {})
    text = message.get("text", "").strip()
    chat_id = str(message.get("chat", {}).get("id", ""))
    
    if not text:
        return {"ok": True}
    
    telegram, _ = _get_telegram()
    if telegram:
        await _handle_command(telegram, text, chat_id)
    
    return {"ok": True}


async def _answer_callback(telegram, callback_id):
    """콜백 쿼리 응답 (버튼 로딩 해제)"""
    import httpx
    url = f"{telegram.api_base}/answerCallbackQuery"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json={"callback_query_id": callback_id})
    except Exception:
        pass


async def _handle_command(telegram, text: str, chat_id: str):
    """명령어 처리"""
    cmd = text.lower().strip().replace("/", "")
    
    if cmd in ("승인", "approve", "처리", "실행"):
        await _cmd_approve(telegram)
    elif cmd in ("스캔", "scan", "검색"):
        await _cmd_scan(telegram)
    elif cmd in ("상태", "status"):
        await _cmd_status(telegram)
    elif cmd in ("도움", "help", "start", "명령", "메뉴"):
        await _cmd_help(telegram)
    else:
        # 알 수 없는 명령 → 도움말 안내
        await telegram.send_message(
            f"❓ 알 수 없는 명령: <code>{text[:30]}</code>\n\n"
            "아래 명령어를 사용하세요:\n"
            "• <b>승인</b> — 대기 중인 메일 처리\n"
            "• <b>스캔</b> — 즉시 메일 스캔\n"
            "• <b>상태</b> — 시스템 현황\n"
            "• <b>도움</b> — 명령어 목록"
        )


async def _cmd_help(telegram):
    await telegram.send_message(
        "🤖 <b>LANstar Agent 텔레그램 명령어</b>\n\n"
        "📬 <b>메일 자동화</b>\n"
        "• <b>스캔</b> — 즉시 메일 스캔\n"
        "• <b>승인</b> — 대기 중인 메일 전체 승인 → 자동 처리\n"
        "• <b>상태</b> — 스케줄러/대기/매핑 현황\n\n"
        "📦 <b>알림 (자동)</b>\n"
        "• 재고 변동 알림\n"
        "• BOR 선적 메일 감지 알림\n"
        "• 파이프라인 처리 결과 알림"
    )


async def _cmd_status(telegram):
    conn = get_connection()
    
    # 대기 건수
    pending = conn.execute(
        "SELECT COUNT(*) FROM mail_processing_log WHERE status = 'pending'"
    ).fetchone()[0]
    
    # 오늘 처리 건수
    today = datetime.now(KST).strftime("%Y-%m-%d")
    completed = conn.execute(
        "SELECT COUNT(*) FROM mail_processing_log WHERE status = 'completed' AND processed_at LIKE ?",
        (today + "%",)
    ).fetchone()[0]
    
    # 매핑 건수
    mapping = conn.execute("SELECT COUNT(*) FROM product_code_mapping").fetchone()[0]
    
    # 스케줄러
    from services.mail_auto_service import get_auto_state
    state = get_auto_state()
    sched = "🟢 ON" if state["enabled"] else "🔴 OFF"
    last = state.get("last_check", "없음")
    if last and len(last) > 16:
        last = last[11:16]
    
    await telegram.send_message(
        f"📊 <b>LANstar Agent 상태</b>\n\n"
        f"🔄 스케줄러: {sched} ({state['interval_min']}분)\n"
        f"🕐 마지막 스캔: {last}\n"
        f"📬 승인 대기: <b>{pending}건</b>\n"
        f"✅ 오늘 처리: {completed}건\n"
        f"🔗 품목 매핑: {mapping:,}건"
    )


async def _cmd_scan(telegram):
    await telegram.send_message("🔍 메일 스캔 시작...")
    
    from services.mail_auto_service import _auto_check_and_process
    await _auto_check_and_process()
    
    conn = get_connection()
    pending = conn.execute(
        "SELECT COUNT(*) FROM mail_processing_log WHERE status = 'pending'"
    ).fetchone()[0]
    
    if pending > 0:
        await telegram.send_message(
            f"✅ 스캔 완료 — <b>{pending}건</b> 승인 대기 중\n\n"
            "👉 <b>승인</b> 입력 시 자동 처리됩니다"
        )
    else:
        await telegram.send_message("✅ 스캔 완료 — 신규 메일 없음")


async def _cmd_approve(telegram):
    conn = get_connection()
    rows = conn.execute(
        "SELECT message_id, subject FROM mail_processing_log WHERE status = 'pending'"
    ).fetchall()
    
    if not rows:
        await telegram.send_message("📭 승인 대기 중인 메일이 없습니다.")
        return
    
    message_ids = [r[0] for r in rows]
    subjects = [r[1][:40] for r in rows]
    
    await telegram.send_message(
        f"⏳ <b>{len(rows)}건 처리 시작</b>\n"
        + "\n".join(f"  📋 {s}" for s in subjects)
    )
    
    # processing 상태로 변경
    for mid in message_ids:
        conn.execute(
            "UPDATE mail_processing_log SET status = 'processing' WHERE message_id = ?",
            (mid,)
        )
    conn.commit()
    
    # 파이프라인 실행
    from services.mail_auto_service import run_mail_automation_pipeline, _send_telegram_notification
    result = await run_mail_automation_pipeline(
        days_back=30,
        auto_reply=False,
        auto_erp=True,
        db_conn=conn,
    )
    
    # 결과 알림
    if result.get("new_processed", 0) > 0:
        await _send_telegram_notification(result)
    else:
        await telegram.send_message("⚠️ 처리할 신규 메일이 없습니다. (이미 처리됨)")


@router.post("/setup-webhook")
async def setup_webhook(request: Request):
    """Webhook URL 등록"""
    import httpx
    body = await request.json()
    webhook_url = body.get("url", "")
    
    if not webhook_url:
        return {"error": "webhook URL을 입력하세요"}
    
    telegram, _ = _get_telegram()
    if not telegram:
        return {"error": "텔레그램 봇 설정이 없습니다"}
    
    url = f"{telegram.api_base}/setWebhook"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json={"url": webhook_url})
        result = resp.json()
    
    return result
