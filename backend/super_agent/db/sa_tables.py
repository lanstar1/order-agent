"""Super Agent DB 테이블 초기화"""
import logging

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

    conn.commit()
    logger.info("[SuperAgent] DB 테이블 초기화 완료")
