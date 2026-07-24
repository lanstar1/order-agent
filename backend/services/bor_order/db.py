"""
중국오더(BOR) 모듈 - DB 테이블 및 설정 관리

테이블 (SQLite/PostgreSQL 겸용 — db.database.get_connection/_sql_to_pg 패턴 재사용):
- bor_config           : key-value 설정 (target_months, bulk_factors, recipient 등)
- bor_restlist_lines   : marked Rest List 최신 스냅샷 (미선적 잔량)
- bor_recent_orders    : 최근 오더 수집분 (정정섭 xlsx + kyu 텍스트)
- bor_order_drafts     : 발주 초안 헤더 (xlsx base64 포함)
- bor_order_draft_lines: 발주 초안 라인
"""
import json
import logging
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.database import get_connection, now_kst

logger = logging.getLogger(__name__)


def get_write_connection():
    """쓰기용 커넥션 — SQLite는 busy_timeout 상향 (60초)

    기동 직후 materials 시트 동기화 등 장시간 쓰기 작업과 겹치면
    기본 busy_timeout(5초)로는 'database is locked'가 발생 → 상향.
    (PG는 get_connection 그대로)
    """
    conn = get_connection()
    try:
        from db.database import USE_PG
        if not USE_PG:
            conn.execute("PRAGMA busy_timeout=60000")
    except Exception:
        pass
    return conn


def write_with_retry(work, attempts: int = 3, wait: float = 3.0):
    """쓰기 트랜잭션 재시도 헬퍼 — 'database is locked' 시 백오프 후 재시도

    work(conn)를 실행하고 commit. 락 이외의 예외는 즉시 전파.
    """
    last_err = None
    for i in range(attempts):
        conn = get_write_connection()
        try:
            result = work(conn)
            conn.commit()
            return result
        except Exception as e:
            last_err = e
            if "locked" in str(e).lower() and i < attempts - 1:
                logger.warning(f"[BOR/DB] DB 잠김 — {wait * (i + 1):.0f}초 후 재시도 ({i + 1}/{attempts})")
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(wait * (i + 1))
                continue
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass
    raise last_err

# ─── 기본 설정값 ─────────────────────────────────────────
# bulk_factors: ECOUNT는 통(100개입) 단위, 중국오더는 낱개 단위인 품목
#               → 오더수량 = ECOUNT 수량 × factor
#               키가 '*'로 끝나면 prefix 매칭 (예: LSN-BOOT-COVER-*)
DEFAULT_BULK_FACTORS = {
    "LSN-CON44": 100,
    "LSN-CON64": 100,
    "LSN-CON66": 100,
    "LSN-CAT5E-STP": 100,
    "LSN-CAT6-STP": 100,
    "LSN-CAT6-UTP": 100,
    "LSN-BOOT-COVER-*": 100,
    "LS-DC-USBPE-*": 100,
}

DEFAULT_CONFIG = {
    "target_months": "4.5",                              # 목표재고 개월수
    "trigger_ratio": "0.8",                              # 발주 트리거 — 파이프라인 < 목표×비율일 때만 라인 포함 (히스테리시스)
    "bulk_factors": json.dumps(DEFAULT_BULK_FACTORS),    # 벌크(통단위) 환산 배율
    "bulk_factors_confirmed": "0",                       # 벌크 배율 확정 여부 (0=미확정 → 라인에 플래그)
    "recipient": "guzhiyi@bor-cable.com",                # 발송 수신자
    "cc": "",                                            # 발송 참조
    "exclude_models": "[]",                              # 발주 제외 모델 (JSON 배열, model_norm 기준)
    "demand_overrides": "{}",                            # 월수요 수동 오버라이드 (JSON {model_norm: qty})
    "model_master_csv": "",                              # 선택: BOR 조달 모델 마스터 CSV 경로 ('model' 컬럼)
}

_tables_ready = False


def ensure_tables() -> None:
    """bor_* 테이블 생성 (최초 1회, 이후 no-op)"""
    global _tables_ready
    if _tables_ready:
        return

    conn = get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bor_config (
                key         TEXT PRIMARY KEY,
                value       TEXT,
                updated_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS bor_restlist_lines (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT,
                source_uid    TEXT,
                source_file   TEXT,
                po_number     TEXT,
                po_date       TEXT,
                model         TEXT,
                model_norm    TEXT,
                description   TEXT,
                rest_qty      REAL,
                unit_price    REAL,
                urgency       TEXT,
                fetched_at    TEXT
            );

            CREATE TABLE IF NOT EXISTS bor_recent_orders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT,
                mail_uid    TEXT,
                mail_date   TEXT,
                order_date  TEXT,
                subject     TEXT,
                model       TEXT,
                model_norm  TEXT,
                qty         REAL,
                note        TEXT,
                fetched_at  TEXT,
                UNIQUE(source, mail_uid, model_norm, qty)
            );

            CREATE TABLE IF NOT EXISTS bor_order_drafts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at      TEXT,
                status          TEXT DEFAULT 'draft',
                params_json     TEXT,
                summary_json    TEXT,
                xlsx_filename   TEXT,
                xlsx_b64        TEXT,
                sent_at         TEXT,
                sent_to         TEXT,
                mail_message_id TEXT
            );

            CREATE TABLE IF NOT EXISTS bor_order_draft_lines (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                draft_id         INTEGER,
                line_no          INTEGER,
                model            TEXT,
                model_norm       TEXT,
                prod_cds         TEXT,
                spec_text        TEXT,
                qty_suggested    REAL,
                qty_final        REAL,
                unit_factor      REAL DEFAULT 1,
                demand_monthly   REAL,
                demand_note      TEXT,
                stock_yongsan    REAL,
                rest_qty         REAL,
                recent_order_qty REAL,
                pipeline_qty     REAL,
                target_qty       REAL,
                coverage_months  REAL,
                safe_qty_erp     REAL,
                included         INTEGER DEFAULT 1,
                flag             TEXT,
                basis_note       TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_bor_rest_model ON bor_restlist_lines(model_norm);
            CREATE INDEX IF NOT EXISTS idx_bor_recent_model ON bor_recent_orders(model_norm);
            CREATE INDEX IF NOT EXISTS idx_bor_recent_date ON bor_recent_orders(order_date);
            CREATE INDEX IF NOT EXISTS idx_bor_draft_lines ON bor_order_draft_lines(draft_id);
        """)
        conn.commit()
        _tables_ready = True
        logger.info("[BOR/DB] bor_* 테이블 확인 완료")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_config() -> dict:
    """bor_config 조회 — DB 저장값 위에 기본값 병합하여 반환 (값은 문자열)"""
    ensure_tables()
    merged = dict(DEFAULT_CONFIG)
    conn = get_connection()
    try:
        rows = conn.execute("SELECT key, value FROM bor_config").fetchall()
        for row in rows:
            key = row[0]
            val = row[1]
            if val is not None:
                merged[key] = val
    except Exception as e:
        logger.warning(f"[BOR/DB] 설정 조회 실패(기본값 사용): {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return merged


def set_config(key: str, value: str) -> None:
    """bor_config 저장 (SQLite/PG 겸용 — DELETE 후 INSERT)"""
    ensure_tables()
    conn = get_write_connection()
    try:
        conn.execute("DELETE FROM bor_config WHERE key = ?", (key,))
        conn.execute(
            "INSERT INTO bor_config (key, value, updated_at) VALUES (?, ?, ?)",
            (key, str(value), now_kst()),
        )
        conn.commit()
        logger.info(f"[BOR/DB] 설정 저장: {key}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_config_json(cfg: dict, key: str, default):
    """설정값 JSON 파싱 헬퍼 (파싱 실패 시 default)"""
    try:
        return json.loads(cfg.get(key) or "")
    except Exception:
        return default
