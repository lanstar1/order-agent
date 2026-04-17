from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Iterator, Sequence

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema.sql"
_DEFAULT_DB = BASE_DIR.parent.parent.parent / "data" / "ai_sourcing.sqlite3"
DB_PATH = Path(os.environ.get("AI_SOURCING_DB_PATH", _DEFAULT_DB)).resolve()
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_write_lock = Lock()
_schema_ready = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return
    with _write_lock:
        if _schema_ready:
            return
        conn = _connect()
        try:
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            _apply_patches(conn)
        finally:
            conn.close()
        _schema_ready = True


def _apply_patches(conn: sqlite3.Connection) -> None:
    profile_cols = {row[1] for row in conn.execute("PRAGMA table_info(trend_profiles)")}
    if "result_count" not in profile_cols:
        conn.execute("ALTER TABLE trend_profiles ADD COLUMN result_count INTEGER NOT NULL DEFAULT 20")
    if "exclude_brand_products" not in profile_cols:
        conn.execute("ALTER TABLE trend_profiles ADD COLUMN exclude_brand_products INTEGER NOT NULL DEFAULT 0")
    if "custom_excluded_terms_json" not in profile_cols:
        conn.execute(
            "ALTER TABLE trend_profiles ADD COLUMN custom_excluded_terms_json TEXT NOT NULL DEFAULT '[]'"
        )
    snap_cols = {row[1] for row in conn.execute("PRAGMA table_info(trend_snapshots)")}
    if "brand_excluded" not in snap_cols:
        conn.execute("ALTER TABLE trend_snapshots ADD COLUMN brand_excluded INTEGER NOT NULL DEFAULT 0")
    run_cols = {row[1] for row in conn.execute("PRAGMA table_info(trend_runs)")}
    if "cancelled_at" not in run_cols:
        conn.execute("ALTER TABLE trend_runs ADD COLUMN cancelled_at TEXT")


@contextmanager
def connection() -> Iterator[sqlite3.Connection]:
    ensure_schema()
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def run(sql: str, params: Sequence[Any] = ()) -> None:
    with _write_lock, connection() as conn:
        conn.execute(sql, tuple(params))


def run_many(sql: str, seq_of_params: Iterable[Sequence[Any]]) -> None:
    with _write_lock, connection() as conn:
        conn.executemany(sql, [tuple(p) for p in seq_of_params])


def one(sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
    with connection() as conn:
        row = conn.execute(sql, tuple(params)).fetchone()
        return row


def all_rows(sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
    with connection() as conn:
        return conn.execute(sql, tuple(params)).fetchall()


def scalar(sql: str, params: Sequence[Any] = ()) -> Any:
    row = one(sql, params)
    if row is None:
        return None
    return row[0]


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def json_parse(value: str | None, fallback: Any):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback
