"""
중국오더(BOR) 모듈 - 발주 수요 산정 및 초안 생성 엔진

수요 규칙 (scripts/safety_stock_recalc.py adjusted_avg와 동일 — 12개 관측치
 = 최근 완전월 11개 + 당월 부분월 일수비례 환산):
  1) 판매 전무 → 0 ('판매없음')
  2) 스파이크형(최대월 비중 >=40% 또는 비영월 <=4개):
     - 최근 3개월 평균 < max(10, 총÷12×0.1) && 최대월비중 >=0.6
       → 최근 3개월 평균 ('스파이크 후 수요소멸 의심')
     - 아니면 → 총판매 ÷ 12 ('스파이크분산 ÷12')
  3) 최대월 > 중앙값×4 → 해당 월을 중앙값×4로 캡 ('이상치월 보정')
  4) 그 외 → 단순 평균

발주량 = 목표재고(월수요 × target_months) − 파이프라인재고
  파이프라인 = 용산재고 + rest잔량 + 최근 60일 오더 중 rest 미반영분
  (벌크 품목은 ECOUNT 통단위 ↔ 중국오더 낱개 → factor 환산)
라운딩: <100→10단위(최소10), <500→50, <5000→100, >=5000→500
"""
import asyncio
import calendar
import csv
import json
import logging
import re
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.database import get_connection, now_kst
from config import BASE_DIR
from services.bor_order import db as bor_db

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

# 최근 오더 중복 차감 기간 (일) — 리드타임 중앙값 67일 감안
RECENT_ORDER_DAYS = 60


# ─── 모델명 정규화 ───────────────────────────────────────
def normalize_model(m: str) -> str:
    """대문자화 + '[', '<', '(', '*' 이후 주석 제거 (safety_stock_recalc.norm_model과 동일)"""
    m = str(m or "").upper().strip()
    return re.sub(r"\s*[<(\[*].*$", "", m).strip()


# ─── 발주수량 라운딩 ─────────────────────────────────────
def friendly_round(x: float) -> int:
    """<100→10단위(최소10), <500→50, <5000→100, >=5000→500"""
    if x <= 0:
        return 0
    if x < 100:
        return max(10, int(round(x / 10.0) * 10))
    if x < 500:
        return int(round(x / 50.0) * 50)
    if x < 5000:
        return int(round(x / 100.0) * 100)
    return int(round(x / 500.0) * 500)


# ─── 수요 산정 ───────────────────────────────────────────
def _prev_months(now_ym: str, n: int) -> list:
    """now_ym('YYYY-MM') 이전 n개월 목록 (과거→최근 순)"""
    y, m = int(now_ym[:4]), int(now_ym[5:7])
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append(f"{y:04d}-{m:02d}")
    return list(reversed(out))


def compute_demand(series: dict, now_ym: str, today_day: int) -> tuple:
    """월수요 산정 → (월평균, 비고)

    series: {'YYYY-MM': qty} 월별 판매량
    now_ym: 당월 'YYYY-MM' (부분월 — 일수비례 환산)
    today_day: 당월 경과일수 (예: 24일까지 데이터면 24)
    """
    complete = _prev_months(now_ym, 11)
    vals = [float(series.get(ym, 0) or 0) for ym in complete]
    # 당월 부분월: 월전체일수/경과일수 배율로 환산
    y, m = int(now_ym[:4]), int(now_ym[5:7])
    days_in_month = calendar.monthrange(y, m)[1]
    scale = days_in_month / max(1, int(today_day))
    vals.append(float(series.get(now_ym, 0) or 0) * scale)

    nonzero = sum(1 for v in vals if v > 0)
    if nonzero == 0:
        return 0.0, "판매없음"

    total = float(sum(vals))
    mx = max(vals)
    med = float(statistics.median(vals))
    max_share = mx / total if total > 0 else 0
    recent3 = sum(vals[-3:]) / 3.0
    spread_avg = total / 12.0

    if max_share >= 0.4 or nonzero <= 4:
        if recent3 < max(10.0, spread_avg * 0.1) and max_share >= 0.6:
            return recent3, "스파이크 후 수요소멸 의심(최근3개월 기준)"
        return spread_avg, "스파이크분산 ÷12"
    if med > 0 and mx > med * 4:
        capped = list(vals)
        capped[capped.index(mx)] = med * 4
        return float(sum(capped)) / len(capped), "이상치월 보정"
    return total / len(vals), ""


# ─── 벌크(통단위) 배율 ───────────────────────────────────
def resolve_bulk_factor(model_norm: str, bulk_factors: dict) -> float:
    """bulk_factors에서 모델 배율 조회 — 정확 매칭 우선, '*' 접미 키는 prefix 매칭"""
    if model_norm in bulk_factors:
        return float(bulk_factors[model_norm])
    for key, factor in bulk_factors.items():
        if key.endswith("*") and model_norm.startswith(key[:-1].upper()):
            return float(factor)
    return 1.0


# ─── 최근 오더 중복 제거 ─────────────────────────────────
def _dedupe_order_rows(rows: list) -> list:
    """수집된 원시 오더 라인의 실무적 중복 제거

    - tiger_xlsx: 같은 오더의 정정 재발행 (예: 'new order-2026-0709.xlsx' →
      'Re:' 메일의 'new order-2026-0709-1.xlsx') → 파일명 날짜 토큰이 같으면
      가장 최신 메일의 라인만 유지
    - kyu_text: 답장 체인에서 같은 오더 라인 재인용 → 10일 이내 동일
      (모델, 수량) 라인은 최초 1건만 유지

    rows: [(source, mail_uid, mail_date, order_date, model_norm, qty, note)]
    """
    # ── tiger: 파일 날짜 토큰별 최신 메일 선정 ──
    latest_by_token = {}  # token -> (mail_date, mail_uid)
    for src, uid, mdate, _odate, _m, _q, note in rows:
        if src != "tiger_xlsx":
            continue
        m = re.search(r"(\d{4}-\d{4})", str(note or ""))
        token = m.group(1) if m else f"uid:{uid}"
        cur = latest_by_token.get(token)
        cand = (str(mdate or ""), str(uid))
        if cur is None or cand > cur:
            latest_by_token[token] = cand

    out = []
    kyu_seen = []  # (order_date, model_norm, qty)
    for row in sorted(rows, key=lambda r: (str(r[3] or ""), str(r[1]))):
        src, uid, mdate, odate, mnorm, qty, note = row
        if src == "tiger_xlsx":
            m = re.search(r"(\d{4}-\d{4})", str(note or ""))
            token = m.group(1) if m else f"uid:{uid}"
            if latest_by_token.get(token) != (str(mdate or ""), str(uid)):
                continue  # 정정 재발행에 밀린 구버전 라인
            out.append(row)
        else:  # kyu_text
            dup = False
            try:
                odt = datetime.strptime(str(odate)[:10], "%Y-%m-%d")
                for sdate, smodel, sqty in kyu_seen:
                    if smodel == mnorm and sqty == qty \
                            and abs((odt - sdate).days) <= 10:
                        dup = True
                        break
                if not dup:
                    kyu_seen.append((odt, mnorm, qty))
            except ValueError:
                pass
            if not dup:
                out.append(row)
    return out


# ─── 품목 마스터 로딩 ────────────────────────────────────
def _load_prod_mapping(conn) -> dict:
    """product_code_mapping + products_master.csv → {model_norm: [(prod_cd, prod_name)]}"""
    mapping = {}

    # 1) products_master.csv (PROD_CD, PROD_NAME, MODEL_NAME)
    prod_names = {}
    csv_path = Path(BASE_DIR) / "data" / "products_master.csv"
    try:
        if csv_path.exists():
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    cd = (row.get("PROD_CD") or "").strip()
                    name = (row.get("PROD_NAME") or "").strip()
                    model = normalize_model(row.get("MODEL_NAME") or "")
                    if cd:
                        prod_names[cd] = name
                    if cd and model:
                        mapping.setdefault(model, []).append((cd, name))
    except Exception as e:
        logger.warning(f"[BOR/엔진] products_master.csv 로딩 실패: {e}")

    # 2) product_code_mapping 테이블 (model_name → prod_cd)
    try:
        rows = conn.execute(
            "SELECT model_name, prod_cd FROM product_code_mapping"
        ).fetchall()
        for row in rows:
            model = normalize_model(row[0])
            cd = str(row[1] or "").strip()
            if not model or not cd:
                continue
            existing = [c for c, _ in mapping.get(model, [])]
            if cd not in existing:
                mapping.setdefault(model, []).append((cd, prod_names.get(cd, "")))
    except Exception as e:
        logger.debug(f"[BOR/엔진] product_code_mapping 조회 스킵: {e}")

    return mapping


def _load_model_master_csv(path_str: str) -> set:
    """선택 설정 model_master_csv — 'model' 컬럼에서 BOR 조달 모델 집합 로딩"""
    models = set()
    if not path_str:
        return models
    try:
        path = Path(path_str)
        if path.exists():
            with open(path, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    m = normalize_model(row.get("model") or "")
                    if m:
                        models.add(m)
    except Exception as e:
        logger.warning(f"[BOR/엔진] model_master_csv 로딩 실패: {e}")
    return models


# ─── 발주 초안 생성 ──────────────────────────────────────
async def generate_draft(target_months: float = None) -> int:
    """발주 초안 생성 → draft_id 반환

    흐름: 최신 rest/recent 데이터 확인(없으면 수집) → 판매/재고/안전재고 수집 →
          모델별 수요·파이프라인 계산 → BOR 조달 모델만 라인 생성(qty>0) →
          xlsx 생성 → bor_order_drafts + lines 저장
    """
    from services.bor_order import erp_ext
    from services.bor_order import mail_collector
    from services.bor_order.xlsx_writer import build_order_xlsx

    bor_db.ensure_tables()
    cfg = bor_db.get_config()
    if target_months is None:
        target_months = float(cfg.get("target_months") or 4.5)
    target_months = float(target_months)

    bulk_factors = {
        normalize_model(k) if not k.endswith("*") else k.upper(): v
        for k, v in bor_db.get_config_json(cfg, "bulk_factors", {}).items()
    }
    bulk_confirmed = str(cfg.get("bulk_factors_confirmed", "0")) == "1"
    exclude_models = {normalize_model(m) for m in bor_db.get_config_json(cfg, "exclude_models", [])}
    demand_overrides = {
        normalize_model(k): float(v)
        for k, v in bor_db.get_config_json(cfg, "demand_overrides", {}).items()
    }

    # ── 1) 최신 rest/recent 데이터 확보 (없으면 메일 수집) ──
    conn = get_connection()
    try:
        rest_cnt = conn.execute("SELECT COUNT(*) FROM bor_restlist_lines").fetchone()[0]
        recent_cnt = conn.execute("SELECT COUNT(*) FROM bor_recent_orders").fetchone()[0]
    finally:
        conn.close()

    if not rest_cnt:
        logger.info("[BOR/엔진] rest 스냅샷 없음 → 메일 수집 실행")
        await asyncio.to_thread(mail_collector.collect_restlist)
    if not recent_cnt:
        logger.info("[BOR/엔진] 최근 오더 수집분 없음 → 메일 수집 실행")
        await asyncio.to_thread(mail_collector.collect_recent_orders)

    # ── 2) 판매/재고 데이터 수집 ──
    sales_by_model = erp_ext.fetch_monthly_sales_by_model(13)     # {model_norm: {ym: qty}}
    inventory = await erp_ext.fetch_inventory_all("10")           # {prod_cd: bal_qty}

    now = datetime.now(KST)
    now_ym = now.strftime("%Y-%m")
    today_day = now.day
    recent_cutoff = (now - timedelta(days=RECENT_ORDER_DAYS)).strftime("%Y-%m-%d")

    # ── 3) DB에서 rest/recent 로딩 ──
    conn = get_connection()
    try:
        rest_rows = conn.execute(
            "SELECT model_norm, model, description, SUM(rest_qty), MAX(unit_price), "
            "MAX(po_date), MAX(urgency) FROM bor_restlist_lines GROUP BY model_norm, model, description"
        ).fetchall()
        order_rows = conn.execute(
            "SELECT source, mail_uid, mail_date, order_date, model_norm, qty, note "
            "FROM bor_recent_orders"
        ).fetchall()
        prod_mapping = _load_prod_mapping(conn)
    finally:
        conn.close()

    # rest 잔량/설명/단가/최신 PO일자 집계 (같은 모델 여러 표기 → 합산)
    rest_qty_map, rest_desc_map, rest_price_map = {}, {}, {}
    max_po_date = ""
    for row in rest_rows:
        mnorm = row[0]
        rest_qty_map[mnorm] = rest_qty_map.get(mnorm, 0.0) + float(row[3] or 0)
        if mnorm not in rest_desc_map and row[2]:
            rest_desc_map[mnorm] = row[2]
        if row[4]:
            rest_price_map[mnorm] = float(row[4])
        if row[5] and str(row[5]) > max_po_date:
            max_po_date = str(row[5])

    # 오더 이력 집계: 전체 이력 모델 + 최근 60일 수량 + rest 미반영(최신 PO일 이후) 수량
    # (정정 재발행 'Re: NEW ORDER'/답장 재인용 중복 제거 후 집계)
    raw_order_rows = [
        (str(r[0] or ""), str(r[1] or ""), str(r[2] or ""), str(r[3] or ""),
         str(r[4] or ""), float(r[5] or 0), str(r[6] or ""))
        for r in order_rows
    ]
    order_models = {r[4] for r in raw_order_rows if r[4]}
    deduped = _dedupe_order_rows(raw_order_rows)
    recent_qty_map = {}       # 최근 60일 오더수량 (낱개)
    unreflected_map = {}      # 그중 rest 미반영분 (po_date 이후 오더)
    for _src, _uid, _mdate, odate, mnorm, qty, _note in deduped:
        if odate >= recent_cutoff:
            recent_qty_map[mnorm] = recent_qty_map.get(mnorm, 0.0) + qty
            # rest 스냅샷의 최신 PO일자 이후 오더는 rest에 아직 미반영으로 간주
            if not max_po_date or odate > max_po_date:
                unreflected_map[mnorm] = unreflected_map.get(mnorm, 0.0) + qty

    # ── 4) BOR 조달 모델 유니버스 ──
    master_models = _load_model_master_csv(cfg.get("model_master_csv", ""))
    universe = order_models | set(rest_qty_map.keys()) | master_models
    universe = {m for m in universe if m and m not in exclude_models}

    # ── 5) 안전재고 조회 (관련 품목코드만) ──
    all_prod_cds = []
    for mnorm in universe:
        for cd, _name in prod_mapping.get(mnorm, []):
            if cd not in all_prod_cds:
                all_prod_cds.append(cd)
    safe_map = await erp_ext.fetch_safe_qty(all_prod_cds)
    quota_hit = bool(safe_map.pop("__quota__", None))

    # ── 6) 모델별 계산 ──
    lines = []
    for mnorm in sorted(universe):
        prods = prod_mapping.get(mnorm, [])
        prod_cds = [cd for cd, _ in prods]
        stock = sum(float(inventory.get(cd, 0) or 0) for cd in prod_cds)
        safe_qty = max((float(safe_map.get(cd, 0) or 0) for cd in prod_cds), default=0.0)

        # 수요 (ECOUNT 단위 기준)
        if mnorm in demand_overrides:
            demand, demand_note = demand_overrides[mnorm], "수요 수동설정"
        else:
            series = sales_by_model.get(mnorm, {})
            demand, demand_note = compute_demand(series, now_ym, today_day)

        factor = resolve_bulk_factor(mnorm, bulk_factors)
        rest_qty = rest_qty_map.get(mnorm, 0.0)           # 낱개
        recent_qty = recent_qty_map.get(mnorm, 0.0)       # 낱개
        unreflected = unreflected_map.get(mnorm, 0.0)     # 낱개

        # 파이프라인 = 용산재고 + rest잔량 + 최근오더 미반영분 (ECOUNT 단위 환산)
        pipeline = stock + rest_qty / factor + unreflected / factor
        target_qty = demand * target_months
        need = target_qty - pipeline                       # ECOUNT 단위 부족분
        qty_pcs = friendly_round(need * factor) if need > 0 else 0
        coverage = round(pipeline / demand, 1) if demand > 0 else None

        is_new = mnorm not in order_models and mnorm not in rest_qty_map
        included = 1
        flag = ""
        if is_new:
            included, flag = 0, "신규검토"
        elif factor != 1.0 and not bulk_confirmed:
            flag = f"벌크단위 미확정(×{factor:g})"

        if qty_pcs <= 0:
            continue

        # SPECIFICATION: 품목명 [품목코드] (가능한 경우)
        spec_text = ""
        for cd, name in prods:
            if name:
                spec_text = f"{name} [{cd}]"
                break
        if not spec_text:
            spec_text = rest_desc_map.get(mnorm, "")

        basis = (
            f"월수요 {demand:.1f}"
            + (f"({demand_note})" if demand_note else "")
            + f" × {target_months:g}개월 = 목표 {target_qty:.0f}"
            + f" | 재고 {stock:.0f} + 미선적 {rest_qty:.0f} + 최근오더미반영 {unreflected:.0f}"
            + (f" (÷{factor:g} 환산)" if factor != 1.0 else "")
            + f" → 부족 {need:.0f}"
            + (f" × {factor:g}" if factor != 1.0 else "")
            + f" ≈ {qty_pcs}"
        )

        lines.append({
            "model": mnorm,
            "model_norm": mnorm,
            "prod_cds": ",".join(prod_cds),
            "spec_text": spec_text,
            "qty_suggested": float(qty_pcs),
            "qty_final": float(qty_pcs),
            "unit_factor": factor,
            "demand_monthly": round(demand, 2),
            "demand_note": demand_note,
            "stock_yongsan": stock,
            "rest_qty": rest_qty,
            "recent_order_qty": recent_qty,
            "pipeline_qty": round(pipeline, 2),
            "target_qty": round(target_qty, 2),
            "coverage_months": coverage,
            "safe_qty_erp": safe_qty,
            "included": included,
            "flag": flag,
            "basis_note": basis,
            "unit_price": rest_price_map.get(mnorm, 0.0),
        })

    # ── 7) xlsx 생성 + draft 저장 ──
    order_date = now.strftime("%Y-%m%d")
    xlsx_lines = [ln for ln in lines if ln["included"] and ln["qty_final"] > 0]
    filename, xlsx_bytes = build_order_xlsx(xlsx_lines, order_date)

    import base64
    est_amount = sum(
        ln["qty_final"] * (ln.get("unit_price") or 0) for ln in xlsx_lines
    )
    summary = {
        "line_count": len(lines),
        "included_count": len(xlsx_lines),
        "total_order_qty": sum(ln["qty_final"] for ln in xlsx_lines),
        "est_amount_usd": round(est_amount, 2),
        "new_model_count": sum(1 for ln in lines if ln["flag"] == "신규검토"),
        "quota_hit": quota_hit,
        "target_months": target_months,
    }
    params = {
        "target_months": target_months,
        "recent_order_days": RECENT_ORDER_DAYS,
        "max_po_date": max_po_date,
        "generated_at": now_kst(),
    }

    def _save(conn):
        cur = conn.execute(
            "INSERT INTO bor_order_drafts "
            "(created_at, status, params_json, summary_json, xlsx_filename, xlsx_b64) "
            "VALUES (?, 'draft', ?, ?, ?, ?)",
            (
                now_kst(),
                json.dumps(params, ensure_ascii=False),
                json.dumps(summary, ensure_ascii=False),
                filename,
                base64.b64encode(xlsx_bytes).decode("ascii"),
            ),
        )
        new_id = cur.lastrowid
        for i, ln in enumerate(lines, 1):
            conn.execute(
                "INSERT INTO bor_order_draft_lines "
                "(draft_id, line_no, model, model_norm, prod_cds, spec_text, "
                " qty_suggested, qty_final, unit_factor, demand_monthly, demand_note, "
                " stock_yongsan, rest_qty, recent_order_qty, pipeline_qty, target_qty, "
                " coverage_months, safe_qty_erp, included, flag, basis_note) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    new_id, i, ln["model"], ln["model_norm"], ln["prod_cds"],
                    ln["spec_text"], ln["qty_suggested"], ln["qty_final"],
                    ln["unit_factor"], ln["demand_monthly"], ln["demand_note"],
                    ln["stock_yongsan"], ln["rest_qty"], ln["recent_order_qty"],
                    ln["pipeline_qty"], ln["target_qty"], ln["coverage_months"],
                    ln["safe_qty_erp"], ln["included"], ln["flag"], ln["basis_note"],
                ),
            )
        return new_id

    # 다른 백그라운드 작업의 쓰기와 겹칠 수 있어 잠김 재시도 헬퍼 사용
    draft_id = bor_db.write_with_retry(_save)

    logger.info(
        f"[BOR/엔진] 초안 #{draft_id} 생성 — 라인 {len(lines)}건 "
        f"(발주대상 {len(xlsx_lines)}건, 신규검토 {summary['new_model_count']}건)"
    )
    return draft_id


def rebuild_xlsx(draft_id: int) -> str:
    """라인 수정 후 xlsx 재생성 — 새 파일명 반환 (PATCH 라우트용 헬퍼)"""
    from services.bor_order.xlsx_writer import build_order_xlsx
    import base64

    bor_db.ensure_tables()
    conn = bor_db.get_write_connection()
    try:
        row = conn.execute(
            "SELECT id, xlsx_filename FROM bor_order_drafts WHERE id = ?", (draft_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"draft {draft_id} 없음")

        line_rows = conn.execute(
            "SELECT model, spec_text, qty_final, flag FROM bor_order_draft_lines "
            "WHERE draft_id = ? AND included = 1 AND qty_final > 0 ORDER BY line_no",
            (draft_id,),
        ).fetchall()
        lines = [
            {"model": r[0], "spec_text": r[1], "qty_final": float(r[2] or 0), "flag": r[3]}
            for r in line_rows
        ]

        # 기존 파일명의 날짜 유지 (new order-YYYY-MMDD.xlsx)
        m = re.search(r"(\d{4}-\d{4})", str(row[1] or ""))
        order_date = m.group(1) if m else datetime.now(KST).strftime("%Y-%m%d")
        filename, xlsx_bytes = build_order_xlsx(lines, order_date)

        conn.execute(
            "UPDATE bor_order_drafts SET xlsx_filename = ?, xlsx_b64 = ? WHERE id = ?",
            (filename, base64.b64encode(xlsx_bytes).decode("ascii"), draft_id),
        )
        conn.commit()
        return filename
    finally:
        conn.close()
