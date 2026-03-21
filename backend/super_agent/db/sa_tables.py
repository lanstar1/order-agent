"""Super Agent DB 테이블 초기화 + CRUD 함수"""
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


def init_super_agent_tables(conn):
    """Super Agent 전용 테이블 생성 (SQLite/PostgreSQL 호환)"""
    cur = conn

    # ── super_agent_jobs: 사용자 요청 루트 ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS super_agent_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE NOT NULL,
        job_type TEXT DEFAULT 'freeform',
        deliverable_type TEXT DEFAULT 'report',
        status TEXT DEFAULT 'received',
        title TEXT DEFAULT '',
        user_prompt TEXT NOT NULL,
        file_ids TEXT DEFAULT '[]',
        constraints_json TEXT DEFAULT '{}',
        plan_json TEXT DEFAULT '{}',
        result_summary TEXT DEFAULT '',
        progress_json TEXT DEFAULT '{}',
        total_cost REAL DEFAULT 0,
        total_tokens INTEGER DEFAULT 0,
        elapsed_ms INTEGER DEFAULT 0,
        error_message TEXT DEFAULT '',
        created_by TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── super_agent_tasks: 실행 단위 ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS super_agent_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id TEXT UNIQUE NOT NULL,
        job_id TEXT NOT NULL,
        task_key TEXT NOT NULL,
        task_kind TEXT DEFAULT 'analysis',
        title TEXT NOT NULL,
        objective TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        sequence_no INTEGER DEFAULT 0,
        depends_on TEXT DEFAULT '[]',
        preferred_llm TEXT DEFAULT 'claude-sonnet',
        actual_llm TEXT DEFAULT '',
        input_json TEXT DEFAULT '{}',
        output_json TEXT DEFAULT '{}',
        output_text TEXT DEFAULT '',
        tokens_input INTEGER DEFAULT 0,
        tokens_output INTEGER DEFAULT 0,
        cost_amount REAL DEFAULT 0,
        latency_ms INTEGER DEFAULT 0,
        error_message TEXT DEFAULT '',
        started_at TEXT,
        completed_at TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── super_agent_artifacts: 산출물 ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS super_agent_artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artifact_id TEXT UNIQUE NOT NULL,
        job_id TEXT NOT NULL,
        task_id TEXT,
        artifact_type TEXT DEFAULT 'report',
        title TEXT DEFAULT '',
        content_format TEXT DEFAULT 'markdown',
        content_text TEXT DEFAULT '',
        file_path TEXT DEFAULT '',
        file_name TEXT DEFAULT '',
        is_final INTEGER DEFAULT 0,
        version INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── super_agent_uploads: 업로드 파일 ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS super_agent_uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id TEXT UNIQUE NOT NULL,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_size INTEGER DEFAULT 0,
        mime_type TEXT DEFAULT '',
        parsed_data TEXT DEFAULT '',
        row_count INTEGER DEFAULT 0,
        column_names TEXT DEFAULT '[]',
        created_by TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── super_agent_events: 이벤트 로그 ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS super_agent_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL,
        task_id TEXT DEFAULT '',
        event_type TEXT NOT NULL,
        message TEXT DEFAULT '',
        data_json TEXT DEFAULT '{}',
        created_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 인덱스 ──
    index_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_sa2_jobs_status ON super_agent_jobs(status)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_jobs_created ON super_agent_jobs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_jobs_type ON super_agent_jobs(job_type)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_tasks_job ON super_agent_tasks(job_id)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_tasks_status ON super_agent_tasks(status)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_artifacts_job ON super_agent_artifacts(job_id)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_artifacts_final ON super_agent_artifacts(is_final)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_uploads_created ON super_agent_uploads(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sa2_events_job ON super_agent_events(job_id)",
    ]
    for sql in index_sqls:
        try:
            cur.execute(sql)
        except Exception as e:
            logger.debug(f"인덱스 생성 스킵: {e}")

    # ── 마이그레이션: result_json 컬럼 추가 ──
    try:
        cur.execute("ALTER TABLE super_agent_jobs ADD COLUMN result_json TEXT DEFAULT '{}'")
        logger.info("[SuperAgent] result_json 컬럼 추가")
    except Exception:
        pass  # 이미 존재

    # ── 마이그레이션: artifact_path, artifact_name 컬럼 추가 ──
    for col in ["artifact_path", "artifact_name"]:
        try:
            cur.execute(f"ALTER TABLE super_agent_jobs ADD COLUMN {col} TEXT DEFAULT ''")
            logger.info(f"[SuperAgent] {col} 컬럼 추가")
        except Exception:
            pass

    # ── created_by 인덱스 추가 ──
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sa2_jobs_created_by ON super_agent_jobs(created_by)")
    except Exception:
        pass

    conn.commit()
    logger.info("[SuperAgent] DB 테이블 초기화 완료")


# ──────────────────────────────────────────────────
#  CRUD 함수 (Job 관리)
# ──────────────────────────────────────────────────

def save_job(conn, job_data: Dict[str, Any]):
    """새 Job을 DB에 저장"""
    conn.execute(
        """INSERT INTO super_agent_jobs
           (job_id, status, user_prompt, deliverable_type, created_by, created_at, result_json, artifact_path, artifact_name, title)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            job_data["job_id"],
            job_data.get("status", "queued"),
            job_data.get("prompt", ""),
            job_data.get("deliverable_type", "report"),
            job_data.get("created_by", ""),
            job_data.get("created_at", datetime.now().isoformat()),
            json.dumps(job_data.get("result") or {}, ensure_ascii=False),
            job_data.get("artifact_path", ""),
            job_data.get("artifact_name", ""),
            job_data.get("title", ""),
        ),
    )
    conn.commit()


def update_job(conn, job_id: str, updates: Dict[str, Any]):
    """Job 상태/결과 업데이트"""
    set_parts = []
    values = []

    field_map = {
        "status": "status",
        "result": "result_json",
        "title": "title",
        "job_type": "job_type",
        "artifact_path": "artifact_path",
        "artifact_name": "artifact_name",
        "total_cost": "total_cost",
        "total_tokens": "total_tokens",
        "elapsed_ms": "elapsed_ms",
        "error_message": "error_message",
        "result_summary": "result_summary",
    }

    for key, col in field_map.items():
        if key in updates:
            val = updates[key]
            if key == "result":
                val = json.dumps(val or {}, ensure_ascii=False)
            set_parts.append(f"{col} = ?")
            values.append(val)

    if not set_parts:
        return

    set_parts.append("updated_at = ?")
    values.append(datetime.now().isoformat())
    values.append(job_id)

    sql = f"UPDATE super_agent_jobs SET {', '.join(set_parts)} WHERE job_id = ?"
    conn.execute(sql, tuple(values))
    conn.commit()


def get_job(conn, job_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Job 조회 (user_id 지정 시 소유권 검증)"""
    if user_id:
        row = conn.execute(
            "SELECT * FROM super_agent_jobs WHERE job_id = ? AND created_by = ?",
            (job_id, user_id),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM super_agent_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()

    if not row:
        return None
    return _row_to_dict(row)


def list_jobs_by_user(conn, user_id: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
    """사용자별 Job 목록 조회"""
    total_row = conn.execute(
        "SELECT COUNT(*) FROM super_agent_jobs WHERE created_by = ?",
        (user_id,),
    ).fetchone()
    total = total_row[0] if total_row else 0

    rows = conn.execute(
        "SELECT * FROM super_agent_jobs WHERE created_by = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (user_id, limit, offset),
    ).fetchall()

    items = [_row_to_dict(r) for r in rows]
    return {"items": items, "total": total}


def delete_job_db(conn, job_id: str, user_id: str) -> bool:
    """Job 삭제 (소유권 검증)"""
    row = conn.execute(
        "SELECT status FROM super_agent_jobs WHERE job_id = ? AND created_by = ?",
        (job_id, user_id),
    ).fetchone()
    if not row:
        return False

    conn.execute("DELETE FROM super_agent_jobs WHERE job_id = ? AND created_by = ?", (job_id, user_id))
    conn.commit()
    return True


def _row_to_dict(row) -> Dict[str, Any]:
    """DB row → dict 변환"""
    if row is None:
        return {}
    # sqlite3.Row 또는 psycopg2 결과 처리
    if hasattr(row, "keys"):
        d = dict(row)
    else:
        # psycopg2 DictRow fallback
        d = dict(row) if hasattr(row, "__iter__") else {}

    # result_json 파싱
    rj = d.get("result_json", "{}")
    if isinstance(rj, str):
        try:
            d["result"] = json.loads(rj)
        except (json.JSONDecodeError, TypeError):
            d["result"] = {}
    else:
        d["result"] = rj or {}

    return d
