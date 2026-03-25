"""
AICC DB 서비스 — 세션/메시지 영구저장 + 제품 지식 DB
"""
import json
from typing import Dict, List, Optional
from db.database import get_connection, now_kst


# ── 세션 영구 저장 ─────────────────────────────────────

def save_session(sid: str, customer_name: str, model: str, erp_code: str,
                 menu: str, status: str = "active"):
    """세션을 DB에 저장/업데이트"""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO aicc_sessions (id, customer_name, selected_model, erp_code,
                   selected_menu, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   status=excluded.status, updated_at=excluded.updated_at""",
            (sid, customer_name, model, erp_code, menu, status,
             now_kst(), now_kst()),
        )
        conn.commit()
    finally:
        conn.close()


def update_session_status(sid: str, status: str):
    """세션 상태 업데이트"""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE aicc_sessions SET status=?, updated_at=? WHERE id=?",
            (status, now_kst(), sid),
        )
        conn.commit()
    finally:
        conn.close()


def save_message(sid: str, role: str, content: str, image_id: str = ""):
    """메시지를 DB에 저장"""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO aicc_messages (session_id, role, content, image_id, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (sid, role, content, image_id or "", now_kst()),
        )
        conn.commit()
    finally:
        conn.close()


def get_all_sessions(limit: int = 100, include_empty: bool = True) -> List[dict]:
    """전체 세션 목록 (최신순), user_msg_count 포함"""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT s.id, s.customer_name, s.selected_model, s.erp_code,
                      s.selected_menu, s.status, s.created_at, s.updated_at,
                      COALESCE(mc.total_count, 0) as msg_count,
                      COALESCE(mc.user_count, 0) as user_msg_count
               FROM aicc_sessions s
               LEFT JOIN (
                   SELECT session_id,
                          COUNT(*) as total_count,
                          SUM(CASE WHEN role = 'user' THEN 1 ELSE 0 END) as user_count
                   FROM aicc_messages
                   GROUP BY session_id
               ) mc ON s.id = mc.session_id
               ORDER BY s.created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            if not include_empty and d.get("user_msg_count", 0) == 0:
                continue
            results.append(d)
        return results
    except Exception as e:
        print(f"[AICC DB] get_all_sessions 오류: {e}")
        return []
    finally:
        conn.close()


def get_session_messages(sid: str) -> List[dict]:
    """세션의 전체 메시지"""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT role, content, image_id, created_at
               FROM aicc_messages WHERE session_id=? ORDER BY created_at""",
            (sid,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── 제품 지식 DB ───────────────────────────────────────

def upsert_product_knowledge(model_name: str, category: str, data: dict):
    """제품 지식 저장/업데이트"""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO aicc_product_knowledge (model_name, category, data_json, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(model_name) DO UPDATE SET
                   category=excluded.category,
                   data_json=excluded.data_json,
                   updated_at=excluded.updated_at""",
            (model_name, category, json.dumps(data, ensure_ascii=False), now_kst()),
        )
        conn.commit()
    finally:
        conn.close()


def bulk_upsert_product_knowledge(products: Dict[str, dict]):
    """제품 지식 일괄 저장 (JSON 파일 전체 임포트)"""
    conn = get_connection()
    try:
        count = 0
        for model_name, data in products.items():
            category = data.get("카테고리", data.get("category", ""))
            conn.execute(
                """INSERT INTO aicc_product_knowledge (model_name, category, data_json, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(model_name) DO UPDATE SET
                       category=excluded.category,
                       data_json=excluded.data_json,
                       updated_at=excluded.updated_at""",
                (model_name, category, json.dumps(data, ensure_ascii=False), now_kst()),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def get_product_knowledge(model_name: str) -> Optional[dict]:
    """특정 제품 지식 조회"""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT model_name, category, data_json, updated_at FROM aicc_product_knowledge WHERE model_name=?",
            (model_name,),
        ).fetchone()
        if row:
            return {
                "model_name": row["model_name"],
                "category": row["category"],
                "data": json.loads(row["data_json"]),
                "updated_at": row["updated_at"],
            }
        return None
    finally:
        conn.close()


def search_product_knowledge(query: str, limit: int = 5) -> List[dict]:
    """제품 지식 검색 (모델명 또는 JSON 내용 검색)"""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT model_name, category, data_json, updated_at
               FROM aicc_product_knowledge
               WHERE model_name LIKE ? OR data_json LIKE ?
               LIMIT ?""",
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()
        return [{
            "model_name": r["model_name"],
            "category": r["category"],
            "data": json.loads(r["data_json"]),
            "updated_at": r["updated_at"],
        } for r in rows]
    finally:
        conn.close()


def get_all_product_knowledge() -> List[dict]:
    """전체 제품 지식 목록 (요약)"""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT model_name, category, updated_at FROM aicc_product_knowledge ORDER BY model_name",
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_product_knowledge(model_name: str) -> bool:
    """제품 지식 삭제"""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM aicc_product_knowledge WHERE model_name=?", (model_name,))
        conn.commit()
        return True
    finally:
        conn.close()


# ── AI 미답변 기록 ─────────────────────────────────────

def save_unanswered(session_id: str, model_name: str, user_question: str, ai_response: str):
    """AI가 답변하지 못한 질문을 기록"""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO aicc_unanswered (session_id, model_name, user_question, ai_response, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (session_id, model_name, user_question, ai_response, now_kst()),
        )
        conn.commit()
    except Exception as e:
        print(f"[AICC DB] 미답변 저장 오류: {e}")
    finally:
        conn.close()


def get_unanswered(resolved: bool = False, limit: int = 100) -> List[dict]:
    """미답변 목록 조회"""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id, session_id, model_name, user_question, ai_response,
                      resolved, admin_note, created_at
               FROM aicc_unanswered
               WHERE resolved = ?
               ORDER BY created_at DESC LIMIT ?""",
            (1 if resolved else 0, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def resolve_unanswered(unanswered_id: int, admin_note: str = ""):
    """미답변을 해결 처리"""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE aicc_unanswered SET resolved=1, admin_note=?, resolved_at=? WHERE id=?",
            (admin_note, now_kst(), unanswered_id),
        )
        conn.commit()
    finally:
        conn.close()


def count_unanswered() -> int:
    """미해결 미답변 수"""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM aicc_unanswered WHERE resolved=0"
        ).fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()
