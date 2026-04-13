"""
Content Factory — 완전 자동 스케줄러
매일 정해진 시간에 소재 수집 → 평가 → 콘텐츠 생성 → 알림

main.py에서 호출:
    from services.content_scheduler import setup_content_scheduler
    setup_content_scheduler()

사용자는 대시보드에서 "오늘의 콘텐츠"를 확인하고 발행만 하면 됨.
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

# ── 주간 콘텐츠 스케줄 ──
# 요일별로 어떤 필라를 생성할지 (0=월 ~ 6=일)
WEEKLY_SCHEDULE = {
    0: [  # 월요일
        {"platform": "instagram", "content_type": "reels", "pillar": "inertia_break", "desc": "관성 깨기 릴스"},
        {"platform": "threads", "content_type": "inertia_break", "desc": "관성 깨기 쓰레드"},
    ],
    1: [  # 화요일
        {"platform": "threads", "content_type": "news_20people", "desc": "AI 뉴스 해석"},
    ],
    2: [  # 수요일
        {"platform": "instagram", "content_type": "reels", "pillar": "vp_coding", "desc": "코딩일지 릴스"},
        {"platform": "threads", "content_type": "vp_coding", "desc": "코딩일지 쓰레드"},
    ],
    3: [  # 목요일
        {"platform": "instagram", "content_type": "reels", "pillar": "trend_apply", "desc": "트렌드 적용기 릴스"},
        {"platform": "threads", "content_type": "trend_apply", "desc": "트렌드 적용기 쓰레드"},
    ],
    4: [  # 금요일
        {"platform": "threads", "content_type": "employee_reaction", "desc": "직원 반응"},
    ],
    5: [  # 토요일
        {"platform": "threads", "content_type": "weekly_ax", "desc": "주간 AX 리포트"},
    ],
    # 일요일: 휴식
}


# ── 메인 자동 생성 함수 ──

async def auto_collect_and_evaluate():
    """매일 06:00 — 소재 수집 + 상위 5개 자동 평가"""
    logger.info("[콘텐츠 스케줄러] 소재 자동 수집 시작")
    try:
        from services.content_service import collect_all_sources, evaluate_source_relevance
        from db.database import get_connection

        # 1. 수집
        result = await collect_all_sources()
        logger.info(f"[콘텐츠 스케줄러] 수집 완료: RSS {result.get('rss', 0)}건, GitHub {result.get('github', 0)}건")

        # 2. 미평가 소재 중 상위 5개 자동 평가
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT id FROM content_sources WHERE status = 'pending' ORDER BY collected_at DESC LIMIT 5"
            ).fetchall()
        finally:
            conn.close()

        evaluated = 0
        for row in rows:
            try:
                await evaluate_source_relevance(dict(row)["id"])
                evaluated += 1
            except Exception as e:
                logger.warning(f"[콘텐츠 스케줄러] 평가 실패 (ID={dict(row)['id']}): {e}")

        logger.info(f"[콘텐츠 스케줄러] {evaluated}건 평가 완료")
    except Exception as e:
        logger.error(f"[콘텐츠 스케줄러] 수집/평가 실패: {e}", exc_info=True)


async def auto_generate_daily_content():
    """매일 07:00 — 오늘 요일에 맞는 콘텐츠 자동 생성"""
    now = datetime.now(KST)
    weekday = now.weekday()  # 0=월 ~ 6=일
    schedule = WEEKLY_SCHEDULE.get(weekday, [])

    if not schedule:
        logger.info(f"[콘텐츠 스케줄러] 오늘({now.strftime('%A')})은 콘텐츠 생성 없음")
        return

    logger.info(f"[콘텐츠 스케줄러] 오늘 생성 예정: {len(schedule)}건")

    from services.content_service import generate_content_from_source, get_prompt, call_claude
    from db.database import get_connection

    generated = []

    for item in schedule:
        try:
            # 최고 점수 미사용 소재 선택
            source_data = await _pick_best_source(item.get("pillar", item["content_type"]))

            if item["content_type"] == "reels":
                # 릴스 스크립트 생성
                result = await _generate_reels_script(source_data, item.get("pillar", "inertia_break"))
                if result:
                    generated.append({"type": "릴스", "desc": item["desc"], "id": result.get("item_id")})
                    # 쓰레드 텍스트도 자동 생성
                    threads_text = result.get("threads_text", "")
                    if threads_text:
                        conn = get_connection()
                        try:
                            conn.execute(
                                "INSERT INTO content_items (platform, content_type, title, body, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'draft', datetime('now','localtime'), datetime('now','localtime'))",
                                ("threads", item.get("pillar", "inertia_break"), f"[자동] {item['desc']} 쓰레드", threads_text)
                            )
                            conn.commit()
                        finally:
                            conn.close()
            else:
                # 쓰레드 텍스트 생성
                result = await generate_content_from_source(
                    source_id=source_data.get("source_id"),
                    platform=item["platform"],
                    content_type=item["content_type"],
                    manual_text=source_data.get("manual_text"),
                )
                if result and not result.get("error"):
                    generated.append({"type": "쓰레드", "desc": item["desc"], "id": result.get("item_id")})

        except Exception as e:
            logger.error(f"[콘텐츠 스케줄러] {item['desc']} 생성 실패: {e}")

    # 텔레그램 알림
    if generated:
        await _notify_content_ready(generated)

    logger.info(f"[콘텐츠 스케줄러] 오늘 {len(generated)}건 생성 완료")


async def check_and_publish_scheduled():
    """매분 — 예약 시간 도래한 콘텐츠 자동 발행"""
    from db.database import get_connection
    conn = get_connection()
    try:
        now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute("""
            SELECT id, platform FROM content_items
            WHERE status = 'scheduled' AND scheduled_at IS NOT NULL AND scheduled_at <= ?
            ORDER BY scheduled_at ASC LIMIT 5
        """, (now_kst,)).fetchall()

        if not rows:
            return

        from services.content_service import publish_content
        for row in rows:
            row = dict(row)
            try:
                await publish_content(row["id"], row["platform"])
                logger.info(f"[예약발행] ID={row['id']} → {row['platform']} 완료")
            except Exception as e:
                logger.error(f"[예약발행] ID={row['id']} 실패: {e}")
                conn.execute("UPDATE content_items SET status='publish_error', updated_at=datetime('now','localtime') WHERE id=?", (row["id"],))
                conn.commit()
    finally:
        conn.close()


# ── 내부 헬퍼 ──

async def _pick_best_source(pillar: str) -> dict:
    """평가 완료된 소재 중 최고 점수 + 미사용 소재 선택. 없으면 Git 커밋 기반 텍스트 반환."""
    from db.database import get_connection
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT id, title, summary FROM content_sources
            WHERE status = 'evaluated' AND relevance_score >= 5
            ORDER BY relevance_score DESC, collected_at DESC LIMIT 1
        """).fetchone()

        if row:
            row = dict(row)
            return {"source_id": row["id"], "manual_text": None}

        # 소재가 없으면 Git 최근 커밋에서 자동 생성
        row2 = conn.execute("""
            SELECT title, summary FROM content_sources
            WHERE source_type = 'github' AND status = 'pending'
            ORDER BY collected_at DESC LIMIT 1
        """).fetchone()

        if row2:
            row2 = dict(row2)
            return {"source_id": None, "manual_text": f"Git 커밋: {row2['title']} - {row2.get('summary', '')}"}

        # 최후 폴백: 기본 소재
        return {"source_id": None, "manual_text": "이번 주에 AI 자동화 시스템을 개선한 경험을 기반으로 콘텐츠를 작성하세요."}
    finally:
        conn.close()


async def _generate_reels_script(source_data: dict, pillar: str) -> dict:
    """릴스 스크립트 JSON 자동 생성"""
    from services.content_service import call_claude, get_prompt
    from db.database import get_connection

    reels_prompt = get_prompt("story", "reels_script")
    source_text = source_data.get("manual_text", "")

    if source_data.get("source_id"):
        conn = get_connection()
        try:
            row = conn.execute("SELECT title, summary FROM content_sources WHERE id = ?", (source_data["source_id"],)).fetchone()
            if row:
                row = dict(row)
                source_text = f"제목: {row['title']}\n내용: {row.get('summary', '')}"
        finally:
            conn.close()

    # 에피소드 번호 자동 증가
    conn = get_connection()
    try:
        cnt = conn.execute("SELECT COUNT(*) as cnt FROM content_items WHERE content_type = 'reels'").fetchone()
        ep_num = (dict(cnt)["cnt"] if isinstance(cnt, dict) else cnt[0]) + 1
    finally:
        conn.close()

    prompt = reels_prompt.format(source_data=source_text, episode_num=f"EP.{ep_num:02d}")
    result_text = await call_claude("릴스 스크립트 전문가. JSON만 출력.", prompt, max_tokens=4096)

    try:
        import re
        cleaned = re.sub(r'^\s*```(?:json)?\s*\n?', '', result_text.strip())
        cleaned = re.sub(r'\n?\s*```\s*$', '', cleaned.strip())
        script = json.loads(cleaned)
        conn = get_connection()
        try:
            cur = conn.execute(
                "INSERT INTO content_items (platform, content_type, title, body, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'draft', datetime('now','localtime'), datetime('now','localtime'))",
                ("instagram", "reels", f"[자동] EP.{ep_num:02d} 릴스 스크립트", json.dumps(script, ensure_ascii=False))
            )
            item_id = cur.lastrowid
            if source_data.get("source_id"):
                conn.execute("UPDATE content_sources SET status='used', used_at=datetime('now','localtime') WHERE id=?", (source_data["source_id"],))
            conn.commit()
        finally:
            conn.close()
        return {"item_id": item_id, "threads_text": script.get("threads_text", "")}
    except json.JSONDecodeError:
        logger.warning(f"[콘텐츠 스케줄러] 릴스 JSON 파싱 실패")
        return None


async def _notify_content_ready(generated: list):
    """텔레그램으로 콘텐츠 생성 완료 알림"""
    try:
        from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return

        import httpx
        items_text = "\n".join([f"  • {g['type']}: {g['desc']} (ID: {g.get('id', '?')})" for g in generated])
        message = f"📝 콘텐츠 팩토리 — 오늘의 콘텐츠 준비 완료\n\n{items_text}\n\n대시보드에서 확인 후 발행해주세요."

        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            )
    except Exception as e:
        logger.warning(f"[콘텐츠 스케줄러] 텔레그램 알림 실패: {e}")


# ── 스케줄러 등록 ──

def setup_content_scheduler():
    """APScheduler에 콘텐츠 자동 생성 작업 등록"""
    try:
        from services.scheduler_service import _scheduler_state
        scheduler = _scheduler_state.get("scheduler")
        if not scheduler:
            logger.warning("[콘텐츠 스케줄러] APScheduler 인스턴스 없음 — 등록 스킵")
            return

        from apscheduler.triggers.cron import CronTrigger

        def _run_async(coro_func):
            def wrapper():
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(coro_func())
                    else:
                        asyncio.run(coro_func())
                except Exception as e:
                    logger.error(f"[콘텐츠 스케줄러] 실행 실패: {e}")
            return wrapper

        # 매일 06:00 KST (UTC 21:00 전날) — 소재 수집 + 평가
        scheduler.add_job(
            _run_async(auto_collect_and_evaluate),
            CronTrigger(hour=21, minute=0, timezone="UTC"),  # KST 06:00
            id="content_collect_evaluate",
            name="콘텐츠 소재 수집+평가 (매일 06:00 KST)",
            replace_existing=True,
        )

        # 매일 07:00 KST (UTC 22:00 전날) — 콘텐츠 자동 생성
        scheduler.add_job(
            _run_async(auto_generate_daily_content),
            CronTrigger(hour=22, minute=0, timezone="UTC"),  # KST 07:00
            id="content_auto_generate",
            name="콘텐츠 자동 생성 (매일 07:00 KST)",
            replace_existing=True,
        )

        # 매분 — 예약 발행 체크
        from apscheduler.triggers.interval import IntervalTrigger
        scheduler.add_job(
            _run_async(check_and_publish_scheduled),
            IntervalTrigger(minutes=1),
            id="content_publish_check",
            name="예약 발행 체크 (매분)",
            replace_existing=True,
        )

        logger.info("[콘텐츠 스케줄러] 3개 작업 등록 완료 (수집 06:00, 생성 07:00, 발행 매분)")

    except Exception as e:
        logger.warning(f"[콘텐츠 스케줄러] 등록 실패: {e}")
