"""
사용자 활동 로그 서비스
- API 요청 기록 (로그인, 발주처리, ERP전송 등)
- 관리자 전용 조회
"""
import logging
from datetime import datetime
from db.database import get_connection

logger = logging.getLogger(__name__)


def ensure_activity_table():
    """활동 로그 테이블 생성"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_cd      TEXT NOT NULL,
            emp_name    TEXT DEFAULT '',
            action      TEXT NOT NULL,
            detail      TEXT DEFAULT '',
            ip_address  TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_activity_emp ON activity_log(emp_cd);
        CREATE INDEX IF NOT EXISTS idx_activity_created ON activity_log(created_at);
        CREATE INDEX IF NOT EXISTS idx_activity_action ON activity_log(action);
    """)
    conn.close()


def log_activity(emp_cd: str, emp_name: str, action: str, detail: str = "", ip_address: str = ""):
    """활동 기록 저장"""
    try:
        conn = get_connection()
        conn.execute(
            """INSERT INTO activity_log(emp_cd, emp_name, action, detail, ip_address)
               VALUES(?,?,?,?,?)""",
            (emp_cd, emp_name, action, detail, ip_address)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[ActivityLog] 기록 실패: {e}")


def get_activity_logs(
    page: int = 1,
    page_size: int = 50,
    emp_cd: str = "",
    action: str = "",
    date_from: str = "",
    date_to: str = "",
) -> dict:
    """활동 로그 조회 (관리자 전용)"""
    conn = get_connection()

    where_parts = ["1=1"]
    params = []

    if emp_cd:
        where_parts.append("a.emp_cd = ?")
        params.append(emp_cd)
    if action:
        where_parts.append("a.action LIKE ?")
        params.append(f"%{action}%")
    if date_from:
        where_parts.append("a.created_at >= ?")
        params.append(date_from)
    if date_to:
        where_parts.append("a.created_at <= ?")
        params.append(date_to + " 23:59:59")

    where = " AND ".join(where_parts)

    total = conn.execute(
        f"SELECT COUNT(*) as cnt FROM activity_log a WHERE {where}", params
    ).fetchone()["cnt"]

    offset = (page - 1) * page_size
    rows = conn.execute(
        f"""SELECT a.* FROM activity_log a
            WHERE {where}
            ORDER BY a.created_at DESC
            LIMIT ? OFFSET ?""",
        params + [page_size, offset]
    ).fetchall()

    conn.close()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        "items": [dict(r) for r in rows],
    }


def get_activity_summary() -> dict:
    """활동 로그 요약 통계"""
    conn = get_connection()

    total = conn.execute("SELECT COUNT(*) as cnt FROM activity_log").fetchone()["cnt"]

    # 최근 7일 사용자별 활동 수
    # PostgreSQL: NOW() - INTERVAL '7 days', SQLite: date('now','-7 days')
    import os
    use_pg = bool(os.getenv("DATABASE_URL", ""))
    date_7days = "NOW() - INTERVAL '7 days'" if use_pg else "date('now', '-7 days')"

    by_user = conn.execute(f"""
        SELECT emp_cd, emp_name, COUNT(*) as cnt,
               MAX(created_at) as last_activity
        FROM activity_log
        WHERE created_at >= {date_7days}
        GROUP BY emp_cd, emp_name
        ORDER BY cnt DESC
    """).fetchall()

    # 최근 7일 액션별 통계
    by_action = conn.execute(f"""
        SELECT action, COUNT(*) as cnt
        FROM activity_log
        WHERE created_at >= {date_7days}
        GROUP BY action
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()

    conn.close()

    return {
        "total": total,
        "by_user": [dict(r) for r in by_user],
        "by_action": [dict(r) for r in by_action],
    }
