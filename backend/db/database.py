"""
데이터베이스 초기화 및 세션 관리
SQLite (로컬/NAS) 또는 PostgreSQL (Render) 자동 선택
- DATABASE_URL 환경변수가 있으면 PostgreSQL
- 없으면 기존 SQLite 사용
"""
import os
import re
import hashlib
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras


# ─── SQL 변환 유틸 (SQLite → PostgreSQL) ───────────────
def _sql_to_pg(sql):
    """SQLite SQL 구문을 PostgreSQL 호환으로 변환"""
    if not sql or not sql.strip():
        return None

    stripped = sql.strip()

    # PRAGMA 처리
    if stripped.upper().startswith("PRAGMA"):
        pragma_match = re.match(
            r"PRAGMA\s+table_info\((\w+)\)", stripped, re.IGNORECASE
        )
        if pragma_match:
            table = pragma_match.group(1)
            # SQL 인젝션 방지: 테이블명 화이트리스트 검증
            _ALLOWED_TABLES = {
                'customers', 'orders', 'order_lines', 'match_candidates',
                'erp_submissions', 'feedback_log', 'employees',
                'chat_sessions', 'chat_messages', 'product_prices',
                'material_sources', 'price_data', 'drive_documents',
                'app_settings', 'product_aliases', 'po_training_pairs',
                'po_training_items', 'bulk_training_sessions',
                'bulk_training_extractions', 'ai_metrics',
                'activity_log', 'orderlist_items', 'orderlist_sync_log',
                'shipments',
                'cs_tickets', 'cs_test_results', 'cs_files', 'cs_action_logs',
                'aicc_sessions', 'aicc_messages',
                'aicc_product_knowledge', 'aicc_unanswered',
                'sales_records', 'sales_fetch_log', 'sales_price_standards', 'sales_alerts',
                'super_agent_jobs', 'super_agent_tasks', 'super_agent_artifacts',
                'super_agent_uploads', 'super_agent_events',
                'erp_cache',
            }
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
                logger.warning(f"[DB] PRAGMA table_info 거부: 잘못된 테이블명 '{table}'")
                return None
            # 재고 모니터링 테이블 추가
            _ALLOWED_TABLES.update({
                'inventory_snapshots', 'inventory_alert_history',
                'inventory_exclude_keywords', 'inventory_alert_settings',
            })
            # MAP 감시 테이블 추가
            _ALLOWED_TABLES.update({
                'map_settings', 'map_products', 'map_sellers',
                'map_price_records', 'map_violations', 'map_collection_logs',
                'map_price_history', 'map_warning_emails',
            })
            if table.lower() not in _ALLOWED_TABLES:
                logger.warning(f"[DB] PRAGMA table_info 거부: 미허용 테이블 '{table}'")
                return None
            return (
                f"SELECT ordinal_position - 1 as cid, column_name as name, "
                f"data_type as type, 0 as notnull, NULL as dflt_value, 0 as pk "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{table}' ORDER BY ordinal_position"
            )
        return None  # 다른 PRAGMA는 무시

    # 파라미터 플레이스홀더: ? → %s
    sql = sql.replace("?", "%s")

    # datetime 함수 (CREATE TABLE DEFAULT 및 INSERT/UPDATE 모두 처리)
    sql = sql.replace("datetime('now','localtime')", "NOW()")
    sql = sql.replace("datetime('now', 'localtime')", "NOW()")
    sql = sql.replace("datetime('now')", "NOW()")

    # json_extract → PG JSON 연산자 (Python 측 파싱으로 대체 권장, 호환용)
    sql = re.sub(
        r"json_extract\((\w+),\s*'\$\.(\w+)'\)",
        r"(\1::json->'\2')::text",
        sql,
        flags=re.IGNORECASE,
    )

    # AUTOINCREMENT → SERIAL
    sql = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "SERIAL PRIMARY KEY",
        sql,
        flags=re.IGNORECASE,
    )

    # BLOB → BYTEA
    sql = re.sub(r"\bBLOB\b", "BYTEA", sql, flags=re.IGNORECASE)

    # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    if re.search(r"INSERT\s+OR\s+IGNORE", sql, re.IGNORECASE):
        sql = re.sub(
            r"INSERT\s+OR\s+IGNORE\s+INTO",
            "INSERT INTO",
            sql,
            flags=re.IGNORECASE,
        )
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # LIKE → ILIKE (PostgreSQL 대소문자 무시 검색)
    sql = re.sub(r"\bLIKE\b", "ILIKE", sql)

    return sql


# ─── PostgreSQL 래퍼 클래스 ───────────────────────────
class _PgCursorWrapper:
    """SQLite cursor 호환 인터페이스를 제공하는 PostgreSQL cursor 래퍼"""

    def __init__(self, cursor=None, lastrowid=None):
        self._cursor = cursor
        self._lastrowid = lastrowid

    @property
    def lastrowid(self):
        return self._lastrowid

    def fetchone(self):
        if self._cursor is None:
            return None
        try:
            return self._cursor.fetchone()
        except psycopg2.ProgrammingError:
            return None

    def fetchall(self):
        if self._cursor is None:
            return []
        try:
            return self._cursor.fetchall()
        except psycopg2.ProgrammingError:
            return []


class _PgConnectionWrapper:
    """SQLite connection 호환 인터페이스를 제공하는 PostgreSQL connection 래퍼"""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        pg_sql = _sql_to_pg(sql)
        if pg_sql is None:
            return _PgCursorWrapper()

        is_insert = pg_sql.strip().upper().startswith("INSERT")
        has_on_conflict = "ON CONFLICT" in pg_sql.upper()
        has_returning = "RETURNING" in pg_sql.upper()

        cursor = self._conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )

        # INSERT에 RETURNING id 추가 (lastrowid 지원용)
        # ON CONFLICT DO NOTHING이나 기존 RETURNING은 제외
        if is_insert and not has_returning and not has_on_conflict:
            try_sql = pg_sql.rstrip().rstrip(";") + " RETURNING id"
            try:
                cursor.execute("SAVEPOINT _ret_sp")
                cursor.execute(try_sql, params or ())
                row = cursor.fetchone()
                cursor.execute("RELEASE SAVEPOINT _ret_sp")
                lastrowid = row[0] if row else None
                return _PgCursorWrapper(cursor, lastrowid)
            except Exception:
                # id 컬럼 없는 테이블 → RETURNING 없이 재시도
                try:
                    cursor.execute("ROLLBACK TO SAVEPOINT _ret_sp")
                    cursor.execute("RELEASE SAVEPOINT _ret_sp")
                except Exception:
                    pass
                cursor = self._conn.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
                cursor.execute(pg_sql, params or ())
                return _PgCursorWrapper(cursor)
        else:
            cursor.execute(pg_sql, params or ())
            return _PgCursorWrapper(cursor)

    def executescript(self, sql):
        """SQL 스크립트 실행 (세미콜론으로 분리, 개별 실행)"""
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            pg_stmt = _sql_to_pg(stmt)
            if pg_stmt:
                cursor = self._conn.cursor()
                try:
                    cursor.execute("SAVEPOINT _script_sp")
                    cursor.execute(pg_stmt)
                    cursor.execute("RELEASE SAVEPOINT _script_sp")
                except Exception as e:
                    logger.warning(f"[DB/PG] executescript 구문 스킵: {e}")
                    try:
                        sp_cur = self._conn.cursor()
                        sp_cur.execute("ROLLBACK TO SAVEPOINT _script_sp")
                        sp_cur.execute("RELEASE SAVEPOINT _script_sp")
                    except Exception:
                        pass
        self._conn.commit()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def cursor(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


# ─── 유틸리티 함수 ──────────────────────────────────
def now_kst() -> str:
    """현재 한국 시간(KST, UTC+9) 문자열 반환"""
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def column_exists(conn, table_name, column_name):
    """테이블에 특정 컬럼이 존재하는지 확인 (SQLite/PG 모두 지원)"""
    if USE_PG:
        row = conn.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name=? AND column_name=?",
            (table_name, column_name),
        ).fetchone()
        return row is not None
    else:
        cols = [
            r[1]
            for r in conn.execute(
                f"PRAGMA table_info({table_name})"
            ).fetchall()
        ]
        return column_name in cols


def safe_add_column(conn, table_name, column_name, column_def):
    """ALTER TABLE ADD COLUMN 안전 실행 (SQLite/PostgreSQL 호환)

    PostgreSQL에서 ALTER TABLE 실패 시 트랜잭션이 abort 상태가 되므로
    반드시 rollback 후 다음 작업을 수행해야 한다.
    """
    try:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        conn.commit()
        logger.info(f"[DB] {table_name}.{column_name} 컬럼 추가 완료")
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


# ─── 연결 함수 ──────────────────────────────────
def get_connection():
    """데이터베이스 연결 반환 (PG 또는 SQLite 자동 선택)"""
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        return _PgConnectionWrapper(conn)
    else:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA cache_size=-8000")  # 8MB 캐시
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn


def _sa_init_tables(conn, cur_or_conn):
    """판매에이전트 테이블 초기화 (스키마 마이그레이션 포함)"""

    def _table_exists(table_name):
        try:
            if USE_PG:
                row = cur_or_conn.execute(
                    f"SELECT 1 FROM information_schema.tables WHERE table_name='{table_name}'"
                ).fetchone()
            else:
                row = cur_or_conn.execute(
                    f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                ).fetchone()
            return row is not None
        except Exception:
            return False

    conn.commit()


# ─── 테이블 초기화 ──────────────────────────────────
def init_db():
    """테이블 초기화 (최초 실행 시)"""
    conn = get_connection()
    cur_or_conn = conn

    # ── 거래처 테이블
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        cust_code  TEXT PRIMARY KEY,
        cust_name  TEXT NOT NULL,
        alias      TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 발주서 헤더
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id    TEXT PRIMARY KEY,
        cust_code   TEXT NOT NULL,
        cust_name   TEXT NOT NULL,
        raw_text    TEXT,
        image_path  TEXT,
        status      TEXT DEFAULT 'pending',
        memo        TEXT,
        created_at  TEXT DEFAULT (datetime('now','localtime')),
        updated_at  TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 발주서 라인
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS order_lines (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id     TEXT NOT NULL,
        line_no      INTEGER NOT NULL,
        raw_text     TEXT,
        qty          REAL,
        unit         TEXT,
        price        REAL DEFAULT 0,
        selected_cd  TEXT,
        is_confirmed INTEGER DEFAULT 0,
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )""")

    # ── 매칭 후보
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS match_candidates (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id      INTEGER NOT NULL,
        prod_cd      TEXT NOT NULL,
        prod_name    TEXT,
        score        REAL,
        match_reason TEXT,
        was_selected INTEGER DEFAULT 0,
        FOREIGN KEY (line_id) REFERENCES order_lines(id)
    )""")

    # ── ERP 전송 로그
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS erp_submissions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id    TEXT NOT NULL,
        success     INTEGER DEFAULT 0,
        erp_slip_no TEXT,
        response    TEXT,
        submitted_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 피드백 / 학습 데이터
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS feedback_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        cust_code    TEXT,
        raw_text     TEXT NOT NULL,
        prod_cd      TEXT NOT NULL,
        prod_name    TEXT,
        qty          REAL,
        unit         TEXT,
        created_at   TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 직원 (로그인)
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        emp_cd        TEXT PRIMARY KEY,
        name          TEXT NOT NULL,
        password_hash TEXT NOT NULL
    )""")

    # 초기 직원 데이터 (비밀번호 = 담당자코드 숫자)
    _init_employees = [
        ("42", "김대기"), ("04", "김재호"), ("51", "박인수"),
        ("49", "박진주"), ("55", "백광현"), ("60", "신시은"),
        ("15", "윤웅렬"), ("50", "이준호"), ("82", "이지원"),
        ("59", "전성진"), ("28", "정광규"), ("38", "정성우"),
        ("01", "정정섭"), ("53", "황지성"),
    ]
    for emp_cd, name in _init_employees:
        pw_hash = hashlib.sha256(emp_cd.encode()).hexdigest()
        cur_or_conn.execute(
            "INSERT OR IGNORE INTO employees(emp_cd, name, password_hash) VALUES(?,?,?)",
            (emp_cd, name, pw_hash),
        )

    # ── 채팅 세션
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS chat_sessions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id  TEXT UNIQUE NOT NULL,
        emp_cd      TEXT,
        title       TEXT DEFAULT '새 대화',
        created_at  TEXT DEFAULT (datetime('now','localtime')),
        updated_at  TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 채팅 메시지
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS chat_messages (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id  TEXT NOT NULL,
        role        TEXT NOT NULL,
        content     TEXT NOT NULL,
        file_path   TEXT,
        file_name   TEXT,
        created_at  TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (session_id) REFERENCES chat_sessions(session_id)
    )""")

    # ── 단가 이력 (ERP 단가 캐시)
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS product_prices (
        cust_code  TEXT NOT NULL,
        prod_cd    TEXT NOT NULL,
        price      REAL DEFAULT 0,
        updated_at TEXT,
        PRIMARY KEY (cust_code, prod_cd)
    )""")

    # ── 택배 발송 기록
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS shipments (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        warehouse       TEXT NOT NULL DEFAULT '',
        slip_no         TEXT NOT NULL,
        cust_cd         TEXT DEFAULT '',
        snd_name        TEXT DEFAULT '',
        snd_tel         TEXT DEFAULT '',
        snd_addr        TEXT DEFAULT '',
        rcv_name        TEXT NOT NULL,
        rcv_tel         TEXT DEFAULT '',
        rcv_cell        TEXT DEFAULT '',
        rcv_addr1       TEXT DEFAULT '',
        rcv_addr2       TEXT DEFAULT '',
        rcv_zip         TEXT DEFAULT '',
        goods_nm        TEXT DEFAULT '',
        qty             INTEGER DEFAULT 1,
        fare_type       TEXT DEFAULT '020',
        dlv_fare        INTEGER DEFAULT 0,
        take_dt         TEXT NOT NULL,
        status          TEXT DEFAULT '접수',
        memo            TEXT DEFAULT '',
        created_at      TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(slip_no, warehouse)
    )""")

    # ── CS/RMA 티켓 ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS cs_tickets (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id       TEXT UNIQUE NOT NULL,
        customer_name   TEXT NOT NULL,
        contact_info    TEXT NOT NULL,
        product_name    TEXT NOT NULL,
        serial_number   TEXT DEFAULT '',
        defect_symptom  TEXT NOT NULL,
        courier         TEXT DEFAULT '',
        tracking_no     TEXT DEFAULT '',
        current_status  TEXT DEFAULT '접수완료',
        final_action    TEXT DEFAULT '',
        created_by      TEXT DEFAULT '',
        received_by     TEXT DEFAULT '',
        handover_by     TEXT DEFAULT '',
        tested_by       TEXT DEFAULT '',
        resolved_by     TEXT DEFAULT '',
        received_at     TEXT DEFAULT '',
        handover_at     TEXT DEFAULT '',
        tested_at       TEXT DEFAULT '',
        resolved_at     TEXT DEFAULT '',
        created_at      TEXT DEFAULT (datetime('now','localtime')),
        updated_at      TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── CS 테스트 결과 ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS cs_test_results (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id       TEXT NOT NULL,
        test_status     TEXT NOT NULL,
        test_comment    TEXT DEFAULT '',
        tested_by       TEXT DEFAULT '',
        created_at      TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (ticket_id) REFERENCES cs_tickets(ticket_id)
    )""")

    # ── CS 첨부파일 ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS cs_files (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id       TEXT NOT NULL,
        file_name       TEXT NOT NULL,
        file_url        TEXT NOT NULL,
        file_type       TEXT DEFAULT 'image',
        file_size       INTEGER DEFAULT 0,
        uploaded_by     TEXT DEFAULT '',
        created_at      TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (ticket_id) REFERENCES cs_tickets(ticket_id)
    )""")

    # ── CS 이력 로그 ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS cs_action_logs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id       TEXT NOT NULL,
        action_type     TEXT NOT NULL,
        actor_cd        TEXT DEFAULT '',
        actor_name      TEXT DEFAULT '',
        detail          TEXT DEFAULT '',
        created_at      TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (ticket_id) REFERENCES cs_tickets(ticket_id)
    )""")


    # ── CS 마이그레이션: drive_file_id 컬럼 추가 ──
    safe_add_column(conn, 'cs_files', 'drive_file_id', "TEXT DEFAULT ''")

    # ── CS 마이그레이션: file_data (바이너리) + mime_type 컬럼 추가 ──
    safe_add_column(conn, 'cs_files', 'file_data', "BLOB")
    safe_add_column(conn, 'cs_files', 'mime_type', "TEXT DEFAULT ''")

    # ── CS 마이그레이션: disk_filename 컬럼 추가 (대용량 파일 디스크 저장용) ──
    safe_add_column(conn, 'cs_files', 'disk_filename', "TEXT DEFAULT ''")

    # ── AICC 마이그레이션: image_id 컬럼 추가 ──
    safe_add_column(conn, 'aicc_messages', 'image_id', "TEXT DEFAULT ''")

    # ── AICC 마이그레이션: channel, source 컬럼 추가 ──
    safe_add_column(conn, 'aicc_sessions', 'channel', "TEXT DEFAULT 'shop'")
    safe_add_column(conn, 'aicc_sessions', 'source', "TEXT DEFAULT ''")

    # ── 판매에이전트: 업로드 파일 + 분석 작업 ──
    _sa_init_tables(conn, cur_or_conn)

    # ── 판매현황 분석 테이블 ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS sales_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slip_date TEXT NOT NULL,
        slip_no TEXT,
        item_code TEXT,
        customer_name TEXT,
        item_name TEXT,
        model_name TEXT,
        quantity REAL DEFAULT 0,
        unit_price REAL DEFAULT 0,
        supply_amount REAL DEFAULT 0,
        vat REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        cost_price REAL DEFAULT 0,
        warehouse TEXT,
        account_date TEXT,
        item_group TEXT,
        note TEXT,
        staff_name TEXT,
        customer_group TEXT,
        safety_stock REAL DEFAULT 0,
        display_code TEXT,
        gross_profit REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS sales_fetch_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetch_type TEXT,
        status TEXT,
        message TEXT,
        rows_imported INTEGER DEFAULT 0,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS sales_price_standards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_code TEXT NOT NULL,
        customer_name TEXT,
        standard_price REAL NOT NULL,
        tolerance_pct REAL DEFAULT 10.0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(item_code, customer_name)
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS sales_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_type TEXT NOT NULL,
        target_name TEXT,
        message TEXT,
        severity TEXT DEFAULT 'warning',
        is_read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── AICC 세션 테이블
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS aicc_sessions (
        id TEXT PRIMARY KEY,
        customer_name TEXT DEFAULT '',
        selected_model TEXT DEFAULT '',
        erp_code TEXT DEFAULT '',
        selected_menu TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        is_admin_intervened INTEGER DEFAULT 0,
        channel TEXT DEFAULT 'shop',
        source TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── AICC 메시지 테이블
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS aicc_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        image_id TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (session_id) REFERENCES aicc_sessions(id)
    )""")

    # ── AICC 제품 지식 DB (제품 상세 JSON — AI 답변의 1차 소스)
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS aicc_product_knowledge (
        model_name TEXT PRIMARY KEY,
        category TEXT DEFAULT '',
        data_json TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── AICC 미답변 기록 테이블
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS aicc_unanswered (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        model_name TEXT DEFAULT '',
        user_question TEXT NOT NULL,
        ai_response TEXT DEFAULT '',
        resolved INTEGER DEFAULT 0,
        admin_note TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        resolved_at TIMESTAMP
    )""")

    # ── 재고 모니터링 테이블 ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        prod_cd TEXT NOT NULL,
        bal_qty REAL NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        UNIQUE(snapshot_date, prod_cd)
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_alert_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        check_date TEXT NOT NULL,
        prod_cd TEXT NOT NULL,
        prod_name TEXT,
        model_name TEXT,
        unit_price REAL DEFAULT 0,
        prev_qty REAL DEFAULT 0,
        curr_qty REAL DEFAULT 0,
        diff_qty REAL DEFAULT 0,
        diff_amount REAL DEFAULT 0,
        trigger_type TEXT,
        created_at TEXT NOT NULL
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_exclude_keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL
    )""")

    # 기본 제외 키워드
    _default_inv_keywords = ["BOOT", "부트", "콘넥터후드", "Hood케이스", "모듈러", "콘넥터", "먼지", "커플러", "키스톤"]
    for _kw in _default_inv_keywords:
        cur_or_conn.execute(
            "INSERT OR IGNORE INTO inventory_exclude_keywords (keyword, created_at) VALUES (?, ?)",
            (_kw, "2026-04-02T00:00:00")
        )

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_alert_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""")

    # 기본 알림 설정
    for _k, _v in [("threshold_amount", "500000"), ("threshold_qty", "100"),
                    ("telegram_bot_token", "8506776023:AAE1KlZ9ZraSLdwTKDtiJolwqqGZthkUcZs"),
                    ("telegram_chat_id", "8521021134"), ("enabled", "true")]:
        cur_or_conn.execute(
            "INSERT OR IGNORE INTO inventory_alert_settings (key, value) VALUES (?, ?)", (_k, _v)
        )

    # ── ERP 캐시 (매입정산용 구매/판매현황 영속 캐시) ──
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS erp_cache (
        cache_key   TEXT PRIMARY KEY,
        filename    TEXT NOT NULL DEFAULT '',
        total       INTEGER NOT NULL DEFAULT 0,
        data_json   TEXT NOT NULL DEFAULT '[]',
        updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── 인덱스 추가 (성능 최적화) ──
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_orders_cust_code ON orders(cust_code);
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
        CREATE INDEX IF NOT EXISTS idx_order_lines_order_id ON order_lines(order_id);
        CREATE INDEX IF NOT EXISTS idx_match_candidates_line_id ON match_candidates(line_id);
        CREATE INDEX IF NOT EXISTS idx_feedback_cust_code ON feedback_log(cust_code);
        CREATE INDEX IF NOT EXISTS idx_erp_submissions_order_id ON erp_submissions(order_id);
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_emp_cd ON chat_sessions(emp_cd);
        CREATE INDEX IF NOT EXISTS idx_chat_messages_session_id ON chat_messages(session_id);
        CREATE INDEX IF NOT EXISTS idx_shipments_rcv_name ON shipments(rcv_name);
        CREATE INDEX IF NOT EXISTS idx_shipments_take_dt ON shipments(take_dt);
        CREATE INDEX IF NOT EXISTS idx_shipments_slip_no ON shipments(slip_no);
        CREATE INDEX IF NOT EXISTS idx_shipments_warehouse ON shipments(warehouse);
        CREATE INDEX IF NOT EXISTS idx_cs_tickets_status ON cs_tickets(current_status);
        CREATE INDEX IF NOT EXISTS idx_cs_tickets_created ON cs_tickets(created_at);
        CREATE INDEX IF NOT EXISTS idx_cs_tickets_customer ON cs_tickets(customer_name);
        CREATE INDEX IF NOT EXISTS idx_cs_action_logs_ticket ON cs_action_logs(ticket_id);

        CREATE INDEX IF NOT EXISTS idx_sr_date ON sales_records(slip_date);
        CREATE INDEX IF NOT EXISTS idx_sr_customer ON sales_records(customer_name);
        CREATE INDEX IF NOT EXISTS idx_sr_item ON sales_records(item_code);
        CREATE INDEX IF NOT EXISTS idx_sr_cgrp ON sales_records(customer_group);
        CREATE INDEX IF NOT EXISTS idx_sr_igrp ON sales_records(item_group);
        CREATE INDEX IF NOT EXISTS idx_sr_staff ON sales_records(staff_name);
        CREATE INDEX IF NOT EXISTS idx_sr_qty ON sales_records(quantity);
        CREATE INDEX IF NOT EXISTS idx_alert_read ON sales_alerts(is_read);
        CREATE INDEX IF NOT EXISTS idx_aicc_msg ON aicc_messages(session_id);
        CREATE INDEX IF NOT EXISTS idx_aicc_status ON aicc_sessions(status);
        CREATE INDEX IF NOT EXISTS idx_aicc_channel ON aicc_sessions(channel);
        CREATE INDEX IF NOT EXISTS idx_aicc_pk_cat ON aicc_product_knowledge(category);
        CREATE INDEX IF NOT EXISTS idx_aicc_unans_resolved ON aicc_unanswered(resolved);
        CREATE INDEX IF NOT EXISTS idx_inv_snapshots_date ON inventory_snapshots(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_inv_alert_date ON inventory_alert_history(check_date);
    """)

    # ── MAP 감시 테이블 ────────────────────────────────
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_settings (
        id INTEGER PRIMARY KEY,
        min_price INTEGER DEFAULT 5000,
        tolerance_pct REAL DEFAULT 5.0,
        schedules TEXT DEFAULT '["00:00","12:00"]',
        watch_interval_hours INTEGER DEFAULT 2,
        platforms TEXT DEFAULT '["네이버 쇼핑","쿠팡","G마켓","옥션","11번가"]',
        alert_email INTEGER DEFAULT 1,
        alert_kakao INTEGER DEFAULT 0,
        alert_nateon INTEGER DEFAULT 1,
        alert_email_address TEXT DEFAULT '',
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    )""")
    cur_or_conn.execute(
        "INSERT OR IGNORE INTO map_settings(id) VALUES(1)"
    )

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        model_name TEXT NOT NULL UNIQUE,
        product_name TEXT NOT NULL,
        brand TEXT DEFAULT 'LANstar',
        features TEXT DEFAULT '',
        map_price INTEGER NOT NULL DEFAULT 0,
        tolerance_pct REAL DEFAULT NULL,
        search_keywords TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1,
        is_watched INTEGER DEFAULT 0,
        watch_interval_hours INTEGER DEFAULT NULL,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_sellers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seller_name TEXT NOT NULL,
        platform TEXT NOT NULL,
        seller_url TEXT DEFAULT '',
        risk_level TEXT DEFAULT 'low',
        total_violations INTEGER DEFAULT 0,
        last_violation_at TEXT DEFAULT NULL,
        is_blacklisted INTEGER DEFAULT 0,
        notes TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(seller_name, platform)
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_price_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        seller_id INTEGER,
        platform TEXT NOT NULL,
        seller_name TEXT NOT NULL,
        product_url TEXT DEFAULT '',
        display_price INTEGER NOT NULL,
        sale_price INTEGER DEFAULT NULL,
        coupon_name TEXT DEFAULT '',
        coupon_discount INTEGER DEFAULT 0,
        coupon_price INTEGER DEFAULT NULL,
        point_reward INTEGER DEFAULT 0,
        effective_price INTEGER NOT NULL,
        free_shipping INTEGER DEFAULT 0,
        screenshot_path TEXT DEFAULT '',
        page_html_path TEXT DEFAULT '',
        is_violation INTEGER DEFAULT 0,
        collected_at TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (product_id) REFERENCES map_products(id)
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_violations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        price_record_id INTEGER,
        product_id INTEGER NOT NULL,
        seller_id INTEGER,
        platform TEXT NOT NULL,
        seller_name TEXT NOT NULL,
        violation_type TEXT NOT NULL,
        severity TEXT NOT NULL DEFAULT 'MEDIUM',
        map_price INTEGER NOT NULL,
        violated_price INTEGER NOT NULL,
        deviation_pct REAL NOT NULL,
        evidence_screenshot TEXT DEFAULT '',
        evidence_url TEXT DEFAULT '',
        is_notified INTEGER DEFAULT 0,
        notified_at TEXT DEFAULT NULL,
        is_resolved INTEGER DEFAULT 0,
        resolved_at TEXT DEFAULT NULL,
        resolution_note TEXT DEFAULT '',
        detected_at TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (price_record_id) REFERENCES map_price_records(id),
        FOREIGN KEY (product_id) REFERENCES map_products(id)
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_collection_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_type TEXT NOT NULL DEFAULT 'scheduled',
        platforms_searched TEXT DEFAULT '',
        products_checked INTEGER DEFAULT 0,
        prices_collected INTEGER DEFAULT 0,
        violations_found INTEGER DEFAULT 0,
        errors TEXT DEFAULT '',
        started_at TEXT DEFAULT (datetime('now','localtime')),
        finished_at TEXT DEFAULT NULL,
        status TEXT DEFAULT 'running'
    )""")

    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_map_pr_product ON map_price_records(product_id);
        CREATE INDEX IF NOT EXISTS idx_map_pr_collected ON map_price_records(collected_at);
        CREATE INDEX IF NOT EXISTS idx_map_pr_violation ON map_price_records(is_violation);
        CREATE INDEX IF NOT EXISTS idx_map_vio_product ON map_violations(product_id);
        CREATE INDEX IF NOT EXISTS idx_map_vio_severity ON map_violations(severity);
        CREATE INDEX IF NOT EXISTS idx_map_vio_detected ON map_violations(detected_at);
        CREATE INDEX IF NOT EXISTS idx_map_vio_resolved ON map_violations(is_resolved);
    """)

    # ── MAP 지도가 변경 이력
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        old_price INTEGER NOT NULL,
        new_price INTEGER NOT NULL,
        changed_by TEXT DEFAULT '',
        reason TEXT DEFAULT '',
        changed_at TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (product_id) REFERENCES map_products(id)
    )""")

    # ── MAP 경고 메일 이력
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS map_warning_emails (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        violation_id INTEGER,
        seller_name TEXT NOT NULL,
        platform TEXT NOT NULL,
        product_name TEXT DEFAULT '',
        model_name TEXT DEFAULT '',
        email_to TEXT DEFAULT '',
        email_subject TEXT DEFAULT '',
        email_body TEXT DEFAULT '',
        status TEXT DEFAULT 'draft',
        sent_at TEXT DEFAULT NULL,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (violation_id) REFERENCES map_violations(id)
    )""")

    # ── 네이버 데이터랩 테이블 ──────────────────────────
    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS datalab_keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        keyword TEXT NOT NULL,
        category_hint TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (product_id) REFERENCES map_products(id)
    )""")

    cur_or_conn.execute("""
    CREATE TABLE IF NOT EXISTS datalab_trend_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        trend_type TEXT NOT NULL,
        result_json TEXT DEFAULT '{}',
        analyzed_at TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (product_id) REFERENCES map_products(id)
    )""")

    cur_or_conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_datalab_kw_product ON datalab_keywords(product_id);
    """)
    cur_or_conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_datalab_tr_product ON datalab_trend_results(product_id);
    """)
    cur_or_conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_datalab_tr_type ON datalab_trend_results(trend_type);
    """)

    conn.commit()
    conn.close()
    db_type = "PostgreSQL" if USE_PG else f"SQLite ({DB_PATH})"
    print(f"[DB] 초기화 완료: {db_type}")


if __name__ == "__main__":
    init_db()
