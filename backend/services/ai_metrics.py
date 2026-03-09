"""
AI 메트릭 수집 및 대시보드 서비스
- LLM 호출 토큰 사용량 추적
- 매칭 정확도 통계
- STP(자동처리)율 추적
- 거래처별 신뢰도 동적 조정
"""
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from db.database import get_connection, now_kst

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
#  테이블 초기화
# ─────────────────────────────────────────
def ensure_metrics_tables():
    """AI 메트릭 테이블 생성"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ai_metrics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_type TEXT NOT NULL,
            cust_code   TEXT DEFAULT '',
            order_id    TEXT DEFAULT '',
            data        TEXT DEFAULT '{}',
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_ai_metrics_type ON ai_metrics(metric_type);
        CREATE INDEX IF NOT EXISTS idx_ai_metrics_cust ON ai_metrics(cust_code);
        CREATE INDEX IF NOT EXISTS idx_ai_metrics_created ON ai_metrics(created_at);
    """)
    conn.close()


# ─────────────────────────────────────────
#  메트릭 기록
# ─────────────────────────────────────────
def record_llm_call(
    call_type: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    duration_ms: float,
    cust_code: str = "",
    order_id: str = "",
):
    """LLM API 호출 메트릭 기록"""
    try:
        conn = get_connection()
        data = json.dumps({
            "call_type": call_type,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "duration_ms": round(duration_ms, 1),
        })
        conn.execute(
            "INSERT INTO ai_metrics(metric_type, cust_code, order_id, data, created_at) VALUES(?,?,?,?,?)",
            ("llm_call", cust_code, order_id, data, now_kst())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[Metrics] LLM 메트릭 기록 실패: {e}")


def record_match_result(
    cust_code: str,
    order_id: str,
    total_lines: int,
    auto_matched: int,
    manual_fixed: int,
    avg_confidence: float,
):
    """매칭 결과 메트릭 기록"""
    try:
        conn = get_connection()
        data = json.dumps({
            "total_lines": total_lines,
            "auto_matched": auto_matched,
            "manual_fixed": manual_fixed,
            "stp_rate": round(auto_matched / max(total_lines, 1) * 100, 1),
            "avg_confidence": round(avg_confidence, 3),
        })
        conn.execute(
            "INSERT INTO ai_metrics(metric_type, cust_code, order_id, data, created_at) VALUES(?,?,?,?,?)",
            ("match_result", cust_code, order_id, data, now_kst())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[Metrics] 매칭 메트릭 기록 실패: {e}")


def record_auto_training(
    cust_code: str,
    order_id: str,
    items_saved: int,
):
    """자동 학습 데이터 축적 메트릭"""
    try:
        conn = get_connection()
        data = json.dumps({"items_saved": items_saved})
        conn.execute(
            "INSERT INTO ai_metrics(metric_type, cust_code, order_id, data, created_at) VALUES(?,?,?,?,?)",
            ("auto_training", cust_code, order_id, data, now_kst())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[Metrics] 자동학습 메트릭 기록 실패: {e}")


# ─────────────────────────────────────────
#  대시보드 통계
# ─────────────────────────────────────────
def get_dashboard_stats(days: int = 30) -> dict:
    """AI 품질 대시보드 통계"""
    conn = get_connection()
    try:
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        # 1. 전체 LLM 토큰 사용량
        rows = conn.execute(
            "SELECT data FROM ai_metrics WHERE metric_type='llm_call' AND created_at >= ?",
            (since,)
        ).fetchall()

        total_tokens = 0
        total_cost_estimate = 0
        model_usage = {}
        for r in rows:
            d = json.loads(r["data"])
            tokens = d.get("total_tokens", 0)
            total_tokens += tokens
            model = d.get("model", "unknown")
            model_usage[model] = model_usage.get(model, 0) + tokens
            # 대략적 비용 추정 (Sonnet 기준)
            if "haiku" in model.lower():
                total_cost_estimate += tokens * 0.000001  # $1/M tokens 추정
            elif "sonnet" in model.lower():
                total_cost_estimate += tokens * 0.000006  # $6/M tokens 추정
            else:
                total_cost_estimate += tokens * 0.00003   # Opus $30/M tokens 추정

        # 2. STP율 (자동처리율)
        match_rows = conn.execute(
            "SELECT data FROM ai_metrics WHERE metric_type='match_result' AND created_at >= ?",
            (since,)
        ).fetchall()

        total_lines_sum = 0
        auto_matched_sum = 0
        for r in match_rows:
            d = json.loads(r["data"])
            total_lines_sum += d.get("total_lines", 0)
            auto_matched_sum += d.get("auto_matched", 0)

        stp_rate = round(auto_matched_sum / max(total_lines_sum, 1) * 100, 1)

        # 3. 거래처별 통계
        cust_stats = {}
        for r in match_rows:
            d = json.loads(r["data"])
            # cust_code를 가져오려면 ai_metrics 테이블에서 cust_code 컬럼 사용
        cust_rows = conn.execute(
            "SELECT cust_code, data FROM ai_metrics WHERE metric_type='match_result' AND created_at >= ? AND cust_code != ''",
            (since,)
        ).fetchall()
        for r in cust_rows:
            cc = r["cust_code"]
            d = json.loads(r["data"])
            if cc not in cust_stats:
                cust_stats[cc] = {"total_lines": 0, "auto_matched": 0, "orders": 0}
            cust_stats[cc]["total_lines"] += d.get("total_lines", 0)
            cust_stats[cc]["auto_matched"] += d.get("auto_matched", 0)
            cust_stats[cc]["orders"] += 1

        cust_stats_list = []
        for cc, s in sorted(cust_stats.items(), key=lambda x: -x[1]["orders"]):
            s["cust_code"] = cc
            s["stp_rate"] = round(s["auto_matched"] / max(s["total_lines"], 1) * 100, 1)
            cust_stats_list.append(s)

        # 4. 자동 학습 통계
        auto_train_rows = conn.execute(
            "SELECT COUNT(*) as cnt FROM ai_metrics WHERE metric_type='auto_training' AND created_at >= ?",
            (since,)
        ).fetchone()
        auto_train_count = auto_train_rows["cnt"] if auto_train_rows else 0

        # 5. 일별 STP 추이 (최근 30일) — Python에서 JSON 파싱 (PG 호환)
        daily_match_rows = conn.execute(
            "SELECT created_at, data FROM ai_metrics WHERE metric_type='match_result' AND created_at >= ?",
            (since,)
        ).fetchall()

        daily_stp_map = {}
        for r in daily_match_rows:
            day = str(r["created_at"])[:10]
            d = json.loads(r["data"])
            if day not in daily_stp_map:
                daily_stp_map[day] = {"total_lines": 0, "auto_matched": 0}
            daily_stp_map[day]["total_lines"] += d.get("total_lines", 0)
            daily_stp_map[day]["auto_matched"] += d.get("auto_matched", 0)

        # 5-2. 일별 LLM 호출 통계 (토큰, 비용)
        daily_llm_rows = conn.execute(
            "SELECT created_at, data FROM ai_metrics WHERE metric_type='llm_call' AND created_at >= ?",
            (since,)
        ).fetchall()

        daily_llm_map = {}
        for r in daily_llm_rows:
            day = str(r["created_at"])[:10]
            d = json.loads(r["data"])
            if day not in daily_llm_map:
                daily_llm_map[day] = {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0}
            daily_llm_map[day]["calls"] += 1
            daily_llm_map[day]["input_tokens"] += d.get("input_tokens", 0)
            daily_llm_map[day]["output_tokens"] += d.get("output_tokens", 0)
            total_t = d.get("total_tokens", 0)
            model = d.get("model", "")
            if "haiku" in model.lower():
                daily_llm_map[day]["cost"] += total_t * 0.000001
            elif "sonnet" in model.lower():
                daily_llm_map[day]["cost"] += total_t * 0.000006
            else:
                daily_llm_map[day]["cost"] += total_t * 0.00003

        # 모든 날짜 합치기
        all_days = sorted(set(list(daily_stp_map.keys()) + list(daily_llm_map.keys())))
        daily_trend = []
        for day in all_days:
            stp = daily_stp_map.get(day, {})
            llm = daily_llm_map.get(day, {})
            tl = stp.get("total_lines", 0)
            am = stp.get("auto_matched", 0)
            daily_trend.append({
                "date": day,
                "total_lines": tl,
                "auto_matched": am,
                "stp_rate": round(am / max(tl, 1) * 100, 1),
                "calls": llm.get("calls", 0),
                "input_tokens": llm.get("input_tokens", 0),
                "output_tokens": llm.get("output_tokens", 0),
                "cost": round(llm.get("cost", 0), 4),
            })

        return {
            "period_days": days,
            # 호환성 필드 (프론트엔드 직접 참조용)
            "total_calls": len(rows),
            "total_input_tokens": sum(json.loads(r["data"]).get("input_tokens", 0) for r in rows),
            "total_output_tokens": sum(json.loads(r["data"]).get("output_tokens", 0) for r in rows),
            "estimated_cost_usd": round(total_cost_estimate, 2),
            "stp_rate": {
                "total_lines": total_lines_sum,
                "auto_matched": auto_matched_sum,
                "stp_pct": stp_rate,
            },
            "auto_training_total": auto_train_count,
            # 상세 구조
            "token_usage": {
                "total_tokens": total_tokens,
                "cost_estimate_usd": round(total_cost_estimate, 2),
                "by_model": model_usage,
            },
            "matching": {
                "total_lines": total_lines_sum,
                "auto_matched": auto_matched_sum,
                "stp_rate": stp_rate,
                "total_orders": len(match_rows),
            },
            "auto_training": {
                "sessions_count": auto_train_count,
            },
            "customer_stats": cust_stats_list[:20],
            "daily_trend": daily_trend,
        }
    finally:
        conn.close()


# ─────────────────────────────────────────
#  거래처별 신뢰도 동적 조정
# ─────────────────────────────────────────
def get_dynamic_threshold(cust_code: str, base_threshold: float = 0.90) -> float:
    """
    거래처별 학습 데이터 양과 STP 성공률에 따라 동적 임계값 반환
    - 학습 데이터 많고 STP율 높은 거래처: 임계값 낮춤 (0.85)
    - 신규 거래처: 임계값 높임 (0.95)
    """
    conn = get_connection()
    try:
        # 학습 데이터 수 확인
        training_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM po_training_items WHERE pair_id IN (SELECT id FROM po_training_pairs WHERE cust_code=?)",
            (cust_code,)
        ).fetchone()
        train_cnt = training_count["cnt"] if training_count else 0

        # 최근 매칭 성공률
        recent_matches = conn.execute(
            "SELECT data FROM ai_metrics WHERE metric_type='match_result' AND cust_code=? ORDER BY created_at DESC LIMIT 10",
            (cust_code,)
        ).fetchall()

        if not recent_matches:
            # 신규 거래처: 높은 임계값
            return min(base_threshold + 0.05, 0.98)

        total = 0
        auto = 0
        for r in recent_matches:
            d = json.loads(r["data"])
            total += d.get("total_lines", 0)
            auto += d.get("auto_matched", 0)

        success_rate = auto / max(total, 1)

        # 학습 데이터 충분 + 높은 성공률 → 임계값 낮춤
        if train_cnt >= 50 and success_rate >= 0.9:
            return max(base_threshold - 0.05, 0.80)
        elif train_cnt >= 20 and success_rate >= 0.8:
            return max(base_threshold - 0.03, 0.85)
        elif train_cnt < 5:
            return min(base_threshold + 0.05, 0.98)
        else:
            return base_threshold

    except Exception as e:
        logger.warning(f"[Metrics] 동적 임계값 계산 실패: {e}")
        return base_threshold
    finally:
        conn.close()


# ─────────────────────────────────────────
#  거래처별 맞춤 프롬프트 생성
# ─────────────────────────────────────────
def get_customer_prompt_hints(cust_code: str) -> str:
    """
    거래처별 축적된 학습 데이터에서 패턴을 분석하여
    프롬프트에 추가할 힌트 텍스트 생성
    """
    conn = get_connection()
    try:
        # 해당 거래처의 학습 아이템 패턴 분석
        rows = conn.execute("""
            SELECT i.raw_line_text, i.item_code, i.product_name, i.unit
            FROM po_training_items i
            JOIN po_training_pairs p ON i.pair_id = p.id
            WHERE p.cust_code = ?
            ORDER BY p.created_at DESC
            LIMIT 100
        """, (cust_code,)).fetchall()

        if not rows or len(rows) < 5:
            return ""

        # 단위 패턴
        units = {}
        for r in rows:
            u = (r["unit"] or "EA").upper()
            units[u] = units.get(u, 0) + 1

        main_unit = max(units, key=units.get) if units else "EA"

        # 품목코드 접두어 패턴
        prefixes = {}
        for r in rows:
            code = r["item_code"] or ""
            if len(code) >= 3:
                prefix = code[:3].upper()
                prefixes[prefix] = prefixes.get(prefix, 0) + 1

        top_prefixes = sorted(prefixes.items(), key=lambda x: -x[1])[:5]

        hints = f"\n\n## 이 거래처({cust_code})의 특성"
        hints += f"\n- 주로 사용하는 단위: {main_unit} ({units.get(main_unit, 0)}회)"
        if top_prefixes:
            hints += f"\n- 자주 주문하는 품목 카테고리: {', '.join(p[0] for p in top_prefixes)}"
        hints += f"\n- 누적 학습 데이터: {len(rows)}건"

        return hints

    except Exception as e:
        logger.warning(f"[Metrics] 거래처 프롬프트 힌트 생성 실패: {e}")
        return ""
    finally:
        conn.close()
