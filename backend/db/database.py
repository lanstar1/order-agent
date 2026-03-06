"""
SQLite 데이터베이스 초기화 및 세션 관리
"""
import sqlite3
from pathlib import Path
import sys, os
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH


def get_connection():
    """SQLite 연결 반환 (WAL 모드)"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """테이블 초기화 (최초 실행 시)"""
    conn = get_connection()
    cur = conn.cursor()

    # ── 거래처 테이블
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        cust_code  TEXT PRIMARY KEY,
        cust_name  TEXT NOT NULL,
        alias      TEXT,          -- 거래처 별칭 (쉼표 구분)
        created_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 발주서 헤더
    cur.execute("""
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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_lines (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id     TEXT NOT NULL,
        line_no      INTEGER NOT NULL,
        raw_text     TEXT,
        qty          REAL,
        unit         TEXT,
        selected_cd  TEXT,         -- 최종 선택 PROD_CD
        is_confirmed INTEGER DEFAULT 0,
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )""")

    # ── 매칭 후보 (학습 데이터용)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS match_candidates (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id      INTEGER NOT NULL,
        prod_cd      TEXT NOT NULL,
        prod_name    TEXT,
        score        REAL,
        match_reason TEXT,
        was_selected INTEGER DEFAULT 0,   -- 사용자가 이 후보를 선택했는지
        FOREIGN KEY (line_id) REFERENCES order_lines(id)
    )""")

    # ── ERP 전송 로그
    cur.execute("""
    CREATE TABLE IF NOT EXISTS erp_submissions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id    TEXT NOT NULL,
        success     INTEGER DEFAULT 0,
        erp_slip_no TEXT,
        response    TEXT,
        submitted_at TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 피드백 / 학습 데이터 (raw_text → 최종 선택 PROD_CD 매핑)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS feedback_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        cust_code    TEXT,
        raw_text     TEXT NOT NULL,    -- 발주서 원문 상품 표현
        prod_cd      TEXT NOT NULL,    -- 최종 확정된 PROD_CD
        prod_name    TEXT,
        qty          REAL,
        unit         TEXT,
        created_at   TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 직원 (로그인)
    cur.execute("""
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
    import hashlib
    for emp_cd, name in _init_employees:
        pw_hash = hashlib.sha256(emp_cd.encode()).hexdigest()
        cur.execute(
            "INSERT OR IGNORE INTO employees(emp_cd, name, password_hash) VALUES(?,?,?)",
            (emp_cd, name, pw_hash)
        )

    # ── 채팅 세션
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_sessions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id  TEXT UNIQUE NOT NULL,
        emp_cd      TEXT,
        title       TEXT DEFAULT '새 대화',
        created_at  TEXT DEFAULT (datetime('now','localtime')),
        updated_at  TEXT DEFAULT (datetime('now','localtime'))
    )""")

    # ── 채팅 메시지
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_messages (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id  TEXT NOT NULL,
        role        TEXT NOT NULL,        -- 'user' | 'assistant'
        content     TEXT NOT NULL,
        file_path   TEXT,                 -- 첨부파일 경로
        file_name   TEXT,                 -- 원본 파일명
        created_at  TEXT DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (session_id) REFERENCES chat_sessions(session_id)
    )""")

    conn.commit()
    conn.close()
    print(f"[DB] 초기화 완료: {DB_PATH}")


if __name__ == "__main__":
    init_db()
