"""
중국오더(BOR) 모듈 - ECOUNT ERP 확장 조회

erp_client 싱글턴의 세션(ensure_session/_session_id/_zone)을 재사용한다.
(erp_client.py 자체는 수정 금지 — 여기서 읽기만)

- fetch_inventory_all      : GetListInventoryBalanceStatus 1콜 → 창고 재고 전체
- fetch_safe_qty           : GetBasicProductsList ∬ 청크(700개) → SAFE_QTY
                             시간당 쿼터(HTTP 412) 시 부분 반환 + '__quota__' 표시
- fetch_monthly_sales_by_model : sales_records 월별 판매 집계 (모델 정규화)
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.database import get_connection
from config import ERP_ZONE

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

# GetBasicProductsList 1회 호출당 품목코드 수 (∬ 구분자 결합)
SAFE_QTY_CHUNK = 700


async def _erp_url(endpoint: str) -> str:
    """세션 확보 후 엔드포인트 URL 구성 (erp_client 싱글턴 세션 재사용)"""
    from services.erp_client import erp_client

    if not erp_client._session_id:
        await erp_client.ensure_session()
    zone = (erp_client._zone or ERP_ZONE).lower()
    return (
        f"https://oapi{zone}.ecount.com/OAPI/V2/{endpoint}"
        f"?SESSION_ID={erp_client._session_id}"
    )


# ─── 재고 전체 조회 (1콜) ────────────────────────────────
async def fetch_inventory_all(wh_cd: str = "10") -> dict:
    """GetListInventoryBalanceStatus — 지정 창고 전체 재고 1콜 조회

    Returns: {prod_cd: bal_qty}
    """
    from services.erp_client import erp_client

    payload = {
        "BASE_DATE": datetime.now(KST).strftime("%Y%m%d"),
        "WH_CD": wh_cd,
        "PROD_CD": "",
        "ZERO_FLAG": "N",
        "BAL_FLAG": "N",
        "DEL_GUBUN": "N",
    }

    url = await _erp_url("InventoryBalance/GetListInventoryBalanceStatus")
    data = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(url, json=payload)
            if r.status_code in (412, 500) and attempt < 2:
                logger.warning(f"[BOR/ERP] 재고조회 HTTP {r.status_code} → 백오프 재시도")
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            # 세션 만료 → 재로그인 후 재시도
            if str(data.get("Status", "")) in ("301", "302") and attempt < 2:
                logger.warning("[BOR/ERP] 세션 만료 - 재로그인 후 재고 재조회")
                await erp_client.ensure_session()
                url = await _erp_url("InventoryBalance/GetListInventoryBalanceStatus")
                continue
            break
        except httpx.HTTPError as e:
            logger.warning(f"[BOR/ERP] 재고조회 시도 {attempt + 1} 오류: {e}")
            if attempt < 2:
                await asyncio.sleep(1.0 * (attempt + 1))

    if not data or str(data.get("Status", "")) != "200":
        logger.error(f"[BOR/ERP] 재고조회 실패: {data}")
        return {}

    inventory = {}
    for item in data.get("Data", {}).get("Result", []):
        cd = str(item.get("PROD_CD", "") or "").strip()
        try:
            qty = float(str(item.get("BAL_QTY", 0) or 0).replace(",", ""))
        except (ValueError, TypeError):
            qty = 0.0
        if cd:
            inventory[cd] = inventory.get(cd, 0.0) + qty

    logger.info(f"[BOR/ERP] 재고조회 완료 — 창고 {wh_cd}, {len(inventory)}개 품목")
    return inventory


# ─── 안전재고 조회 (청크) ────────────────────────────────
async def fetch_safe_qty(prod_cds: list) -> dict:
    """GetBasicProductsList ∬ 청크(700개) — 품목별 안전재고(SAFE_QTY)

    시간당 쿼터 초과(HTTP 412) 시 그때까지의 부분 결과에
    '__quota__': 1.0 키를 추가하여 반환한다 (호출측에서 pop 후 사용).
    Returns: {prod_cd: safe_qty[, '__quota__': 1.0]}
    """
    from services.erp_client import erp_client

    result = {}
    if not prod_cds:
        return result

    for i in range(0, len(prod_cds), SAFE_QTY_CHUNK):
        chunk = prod_cds[i:i + SAFE_QTY_CHUNK]
        payload = {"PROD_CD": "∬".join(chunk)}

        url = await _erp_url("InventoryBasic/GetBasicProductsList")
        data = None
        quota = False
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    r = await client.post(url, json=payload)
                if r.status_code == 412:
                    # 시간당 쿼터 초과 — 재시도해도 소용없음 → 부분 반환
                    logger.warning("[BOR/ERP] GetBasicProductsList 쿼터 초과(412) — 부분 반환")
                    quota = True
                    break
                if r.status_code == 500 and attempt < 2:
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                r.raise_for_status()
                data = r.json()
                if str(data.get("Status", "")) in ("301", "302") and attempt < 2:
                    logger.warning("[BOR/ERP] 세션 만료 - 재로그인 후 안전재고 재조회")
                    await erp_client.ensure_session()
                    url = await _erp_url("InventoryBasic/GetBasicProductsList")
                    continue
                break
            except httpx.HTTPError as e:
                logger.warning(f"[BOR/ERP] 안전재고 시도 {attempt + 1} 오류: {e}")
                if attempt < 2:
                    await asyncio.sleep(1.0 * (attempt + 1))

        if quota:
            result["__quota__"] = 1.0
            return result

        if not data or str(data.get("Status", "")) != "200":
            logger.warning(f"[BOR/ERP] 안전재고 청크 실패 (건너뜀): {str(data)[:200]}")
            continue

        for item in data.get("Data", {}).get("Result", []):
            cd = str(item.get("PROD_CD", "") or "").strip()
            try:
                sq = float(str(item.get("SAFE_QTY", 0) or 0).replace(",", ""))
            except (ValueError, TypeError):
                sq = 0.0
            if cd:
                result[cd] = sq

        # 청크 사이 소폭 대기 (과호출 방지)
        if i + SAFE_QTY_CHUNK < len(prod_cds):
            await asyncio.sleep(0.5)

    logger.info(f"[BOR/ERP] 안전재고 조회 — {len(result)}개 품목")
    return result


# ─── 월별 판매 집계 (sales_records) ──────────────────────
def fetch_monthly_sales_by_model(months: int = 13) -> dict:
    """sales_records에서 model_name 기준 월별 판매수량 합산

    모델 정규화(normalize_model) 적용 — 같은 모델의 표기 변형 합산.
    Returns: {model_norm: {'2025-08': qty, ...}}
    """
    from services.bor_order.engine import normalize_model

    now = datetime.now(KST)
    # months개월 전 1일부터 (slip_date는 'YYYYMMDD' 문자열)
    y, m = now.year, now.month
    for _ in range(months - 1):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    since = f"{y:04d}{m:02d}01"

    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT model_name, SUBSTR(slip_date, 1, 6) AS ym, SUM(quantity) "
            "FROM sales_records "
            "WHERE slip_date >= ? AND model_name IS NOT NULL AND model_name != '' "
            "GROUP BY model_name, SUBSTR(slip_date, 1, 6)",
            (since,),
        ).fetchall()
    finally:
        conn.close()

    result = {}
    for row in rows:
        model_norm = normalize_model(row[0])
        if not model_norm:
            continue
        ym_raw = str(row[1] or "")
        if len(ym_raw) != 6:
            continue
        ym = f"{ym_raw[:4]}-{ym_raw[4:6]}"
        qty = float(row[2] or 0)
        result.setdefault(model_norm, {})
        result[model_norm][ym] = result[model_norm].get(ym, 0.0) + qty

    logger.info(f"[BOR/ERP] 월별 판매 집계 — {len(result)}개 모델 ({months}개월)")
    return result
