"""
Content Factory — 서비스 레이어
order-agent 패턴: get_connection() + conn.execute(sql, params) + ? placeholder
"""
import os
import json
import httpx
import logging
from datetime import datetime, timedelta
from typing import Optional
from db.database import get_connection
from config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
THREADS_USER_ID = os.getenv("THREADS_USER_ID", "")
THREADS_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN", "")
IG_USER_ID = os.getenv("IG_USER_ID", "")
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "")

# ── 프롬프트 로더 (DB 우선, 하드코딩 폴백) ──

_FB_SYS = "당신은 20인 유통회사 부사장의 SNS 콘텐츠를 작성합니다. 반말, 담백한 톤. 회사명 노출 금지. 200~400자."
_FB_PIL = {"inertia_break": "관성 깨기 글. 소재: {source_data}", "trend_apply": "트렌드 적용. 소재: {source_data}",
           "vp_coding": "코딩 경험. 소재: {source_data}", "news_20people": "뉴스 해석. 뉴스: {source_data}",
           "employee_reaction": "직원 반응. 소재: {source_data}", "weekly_ax": "주간 회고. 데이터: {source_data}"}


def get_prompt(category: str, key: str) -> str:
    """DB에서 프롬프트 로드, 없으면 폴백"""
    try:
        conn = get_connection()
        row = conn.execute("SELECT content FROM prompt_templates WHERE category = ? AND key = ?", (category, key)).fetchone()
        conn.close()
        if row:
            return dict(row)["content"]
    except Exception:
        pass
    if category == "story" and key == "system":
        return _FB_SYS
    if category == "story":
        return _FB_PIL.get(key, _FB_PIL["inertia_break"])
    return ""


def get_system_prompt():
    return get_prompt("story", "system")


def get_pillar_prompt(ct):
    return get_prompt("story", ct)

RSS_FEEDS = [
    {"name": "TechCrunch AI", "url": "https://techcrunch.com/category/artificial-intelligence/feed/"},
    {"name": "The Verge AI", "url": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"},
    {"name": "Anthropic Blog", "url": "https://www.anthropic.com/feed.xml"},
    {"name": "GeekNews", "url": "https://news.hada.io/rss/news"},
    {"name": "AI타임스", "url": "https://www.aitimes.com/rss/allArticle.xml"},
]


async def call_claude(system: str, user_message: str, max_tokens: int = 1024) -> str:
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": max_tokens, "system": system, "messages": [{"role": "user", "content": user_message}]},
        )
        data = resp.json()
        return data["content"][0]["text"]


async def collect_rss_feeds() -> list:
    import feedparser
    collected = []
    conn = get_connection()
    try:
        for fi in RSS_FEEDS:
            try:
                feed = feedparser.parse(fi["url"])
                for entry in feed.entries[:5]:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")[:500]
                    link = entry.get("link", "")
                    if conn.execute("SELECT id FROM content_sources WHERE source_url = ?", (link,)).fetchone():
                        continue
                    conn.execute("INSERT INTO content_sources (source_type,title,summary,raw_data,source_url,status,collected_at) VALUES (?,?,?,?,?,'pending',datetime('now','localtime'))", ("news", title, summary, json.dumps({"feed": fi["name"]}), link))
                    collected.append({"title": title, "source": fi["name"]})
                conn.commit()
            except Exception as e:
                logger.warning(f"RSS fail ({fi['name']}): {e}")
    finally:
        conn.close()
    return collected


async def collect_github_commits() -> list:
    if not GITHUB_TOKEN:
        return []
    collected = []
    async with httpx.AsyncClient() as client:
        for repo in ["lanstar1/order-agent", "lanstar1/godomall"]:
            try:
                since = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
                resp = await client.get(f"https://api.github.com/repos/{repo}/commits", headers={"Authorization": f"token {GITHUB_TOKEN}"}, params={"since": since})
                commits = resp.json()
                conn = get_connection()
                try:
                    for c in commits[:10]:
                        msg = c.get("commit", {}).get("message", "").split("\n")[0]
                        sha = c.get("sha", "")[:7]
                        if msg.startswith(("feat:", "fix:")) and len(msg) > 20:
                            conn.execute("INSERT INTO content_sources (source_type,title,summary,raw_data,status,collected_at) VALUES (?,?,?,?,'pending',datetime('now','localtime'))", ("github", f"[{sha}] {msg[:100]}", msg, json.dumps({"repo": repo, "sha": sha})))
                            collected.append({"title": msg[:80]})
                    conn.commit()
                finally:
                    conn.close()
            except Exception as e:
                logger.warning(f"GitHub fail ({repo}): {e}")
    return collected


async def collect_all_sources() -> dict:
    rss = await collect_rss_feeds()
    gh = await collect_github_commits()
    return {"rss": len(rss), "github": len(gh)}


async def evaluate_source_relevance(source_id: int) -> dict:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM content_sources WHERE id = ?", (source_id,)).fetchone()
        if not row:
            return {"error": "소재 없음"}
        row = dict(row)
        prompt = f'소재 평가. JSON만 출력.\n기준: 관성연결도(0~3),실전연결성(0~3),공감도(0~2),화제성(0~2)\n소재: {row["title"]} - {row.get("summary","")}\n응답: {{"score":0,"reason":"","recommended_pillar":"inertia_break","suggested_hook":""}}'
        txt = await call_claude("소재 평가. JSON만.", prompt)
        try:
            result = json.loads(txt.strip().strip("```json").strip("```"))
            conn.execute("UPDATE content_sources SET relevance_score=?, status='evaluated' WHERE id=?", (result.get("score", 0), source_id))
            conn.commit()
            return {"source_id": source_id, **result}
        except json.JSONDecodeError:
            return {"source_id": source_id, "error": "파싱 실패"}
    finally:
        conn.close()


async def generate_content_from_source(source_id: Optional[int], platform: str, content_type: str, manual_text: Optional[str] = None) -> dict:
    conn = get_connection()
    try:
        if source_id:
            row = conn.execute("SELECT * FROM content_sources WHERE id=?", (source_id,)).fetchone()
            if not row:
                return {"error": "소재 없음"}
            row = dict(row)
            source_data = f"제목: {row['title']}\n내용: {row.get('summary','')}"
        elif manual_text:
            source_data = manual_text
        else:
            return {"error": "소재 ID 또는 수동 텍스트 필요"}

        if platform == "threads":
            tmpl = get_pillar_prompt(content_type)
            body = await call_claude(get_system_prompt(), tmpl.format(source_data=source_data))
        elif platform == "instagram":
            body = await call_claude("카드뉴스 JSON. JSON만 출력.", f'인스타 카드뉴스 JSON. 회사명 금지. 5~6장. 관성→전환.\n소재: {source_data}\n응답: {{"slides":[{{"type":"cover","text":"","subtext":""}}],"caption":"","hashtags":[]}}')
        else:
            return {"error": f"미지원: {platform}"}

        cur = conn.execute("INSERT INTO content_items (source_id,platform,content_type,body,status,created_at,updated_at) VALUES (?,?,?,?,'draft',datetime('now','localtime'),datetime('now','localtime'))", (source_id, platform, content_type, body))
        item_id = cur.lastrowid
        if source_id:
            conn.execute("UPDATE content_sources SET status='used',used_at=datetime('now','localtime') WHERE id=?", (source_id,))
        conn.commit()
        return {"item_id": item_id, "platform": platform, "content_type": content_type, "body": body}
    finally:
        conn.close()


async def regenerate_content(item_id: int) -> dict:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM content_items WHERE id=?", (item_id,)).fetchone()
        if not row:
            return {"error": "없음"}
        row = dict(row)
    finally:
        conn.close()
    return await generate_content_from_source(row.get("source_id"), row["platform"], row["content_type"])


async def publish_content(item_id: int, platform: str) -> dict:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM content_items WHERE id=?", (item_id,)).fetchone()
        if not row:
            return {"error": "없음"}
        item = dict(row)
        if platform == "threads":
            result = await publish_to_threads(item["body"])
        else:
            result = {"error": "Instagram 캐러셀 미구현", "post_id": None}
        conn.execute("INSERT INTO content_publish_log (content_id,platform,platform_post_id,published_at) VALUES (?,?,?,datetime('now','localtime'))", (item_id, platform, result.get("post_id")))
        conn.execute("UPDATE content_items SET status='published',published_at=datetime('now','localtime') WHERE id=?", (item_id,))
        conn.commit()
        return {"message": "발행 완료", **result}
    finally:
        conn.close()


async def publish_to_threads(text: str) -> dict:
    if not THREADS_USER_ID or not THREADS_ACCESS_TOKEN:
        return {"error": "Threads API 미설정", "post_id": None}
    async with httpx.AsyncClient() as client:
        cr = await client.post(f"https://graph.threads.net/v1.0/{THREADS_USER_ID}/threads", params={"media_type": "TEXT", "text": text, "access_token": THREADS_ACCESS_TOKEN})
        container = cr.json()
        if "id" not in container:
            return {"error": f"실패: {container}", "post_id": None}
        pr = await client.post(f"https://graph.threads.net/v1.0/{THREADS_USER_ID}/threads_publish", params={"creation_id": container["id"], "access_token": THREADS_ACCESS_TOKEN})
        return {"post_id": pr.json().get("id"), "platform": "threads"}


async def check_sns_connection() -> dict:
    status = {"threads": {"connected": False}, "instagram": {"connected": False}}
    if THREADS_ACCESS_TOKEN and THREADS_USER_ID:
        try:
            async with httpx.AsyncClient() as c:
                r = await c.get(f"https://graph.threads.net/v1.0/{THREADS_USER_ID}", params={"fields": "id,username", "access_token": THREADS_ACCESS_TOKEN})
                d = r.json()
                if "id" in d:
                    status["threads"] = {"connected": True, "username": d.get("username", ""), "user_id": d["id"]}
        except Exception:
            pass
    if IG_ACCESS_TOKEN and IG_USER_ID:
        try:
            async with httpx.AsyncClient() as c:
                r = await c.get(f"https://graph.facebook.com/v19.0/{IG_USER_ID}", params={"fields": "id,username", "access_token": IG_ACCESS_TOKEN})
                d = r.json()
                if "id" in d:
                    status["instagram"] = {"connected": True, "username": d.get("username", ""), "user_id": d["id"]}
        except Exception:
            pass
    return status
