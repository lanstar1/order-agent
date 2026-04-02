"""
재고 변동 모니터링 서비스
- 매일 평일 오전 9시(KST) ERP 재고현황 API 호출
- 전일 스냅샷과 비교하여 재고 감소 품목 감지
- 알림 조건: 단가×감소수량 ≥ 50만원 OR 감소수량 ≥ 100개
- 예외: 키워드 필터에 해당하는 저가 소모품 → 수량 기준만 제외, 금액 기준은 여전히 적용
"""

import csv
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


# ─── 품목 마스터 로딩 ───────────────────────────────────────────

def load_products_master(csv_path: str = None) -> dict:
    """
    products_master.csv를 로딩하여 {PROD_CD: {name, model, price}} 딕셔너리 반환
    """
    if csv_path is None:
        base_dir = Path(__file__).parent.parent.parent
        csv_path = str(base_dir / "data" / "products_master.csv")

    products = {}
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                prod_cd = row["PROD_CD"].strip()
                products[prod_cd] = {
                    "name": row["PROD_NAME"].strip(),
                    "model": row["MODEL_NAME"].strip(),
                    "price": float(row["UNIT_PRICE"] or 0),
                }
        logger.info(f"품목 마스터 로딩 완료: {len(products)}개")
    except Exception as e:
        logger.error(f"품목 마스터 로딩 실패: {e}")
    return products


# ─── ERP 재고현황 조회 ──────────────────────────────────────────

async def fetch_inventory_from_erp(base_date: str = None, wh_cd: str = "") -> list:
    """
    ECOUNT ERP 재고현황 API 호출
    기존 erp_client.py의 ERPClient 패턴을 재사용합니다.

    Returns:
        [{"PROD_CD": "xxx", "BAL_QTY": 123.0}, ...]
    """
    from services.erp_client import ERPClient
    from config import ERP_ZONE

    if base_date is None:
        base_date = datetime.now(KST).strftime("%Y%m%d")

    erp = ERPClient()
    session_id = await erp.get_session()
    zone = (erp._zone or ERP_ZONE).lower()

    url = f"https://oapi{zone}.ecount.com/OAPI/V2/InventoryBalance/GetListInventoryBalanceStatus"

    payload = {
        "BASE_DATE": base_date,
        "WH_CD": wh_cd,
        "PROD_CD": "",
        "ZERO_FLAG": "N",
        "BAL_FLAG": "N",
        "DEL_GUBUN": "N",
    }

    import httpx
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{url}?SESSION_ID={session_id}",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        data = resp.json()

    if data.get("Status") != "200":
        error_msg = data.get("Error", {}).get("Message", "Unknown error")
        logger.error(f"ERP 재고조회 실패: {error_msg}")
        raise Exception(f"ERP API 오류: {error_msg}")

    results = data.get("Data", {}).get("Result", [])
    inventory = []
    for item in results:
        inventory.append({
            "PROD_CD": item["PROD_CD"],
            "BAL_QTY": float(item["BAL_QTY"]),
        })

    logger.info(f"ERP 재고조회 완료: {len(inventory)}개 품목 (기준일: {base_date})")
    return inventory


# ─── 스냅샷 저장/조회 (동기 DB) ────────────────────────────────

def save_snapshot(conn, inventory_data: list, snapshot_date: str):
    """재고 스냅샷을 DB에 저장"""
    conn.execute("DELETE FROM inventory_snapshots WHERE snapshot_date = ?", (snapshot_date,))

    for item in inventory_data:
        conn.execute(
            """INSERT INTO inventory_snapshots (snapshot_date, prod_cd, bal_qty, created_at)
               VALUES (?, ?, ?, ?)""",
            (snapshot_date, item["PROD_CD"], item["BAL_QTY"], datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
        )

    conn.commit()
    logger.info(f"스냅샷 저장 완료: {snapshot_date}, {len(inventory_data)}개 품목")


def get_snapshot(conn, snapshot_date: str) -> dict:
    """특정 날짜의 스냅샷을 {PROD_CD: BAL_QTY} 딕셔너리로 반환"""
    rows = conn.execute(
        "SELECT prod_cd, bal_qty FROM inventory_snapshots WHERE snapshot_date = ?",
        (snapshot_date,)
    ).fetchall()
    return {row[0]: row[1] for row in rows}


# ─── 재고 변동 비교 ────────────────────────────────────────────

def compare_inventory(
    prev_snapshot: dict,
    curr_snapshot: dict,
    products_master: dict,
    exclude_keywords: list,
    threshold_amount: int = 500000,
    threshold_qty: int = 100,
) -> list:
    """
    전일 vs 금일 재고를 비교하여 알림 대상 품목 추출 (감소만)

    예외 키워드 품목: 수량 기준만 제외, 금액 기준(50만원 이상)은 알림
    """
    alerts = []

    for prod_cd, prev_qty in prev_snapshot.items():
        curr_qty = curr_snapshot.get(prod_cd, 0)
        diff = prev_qty - curr_qty  # 양수 = 감소

        if diff <= 0:
            continue

        master = products_master.get(prod_cd, {})
        prod_name = master.get("name", f"(미등록) {prod_cd}")
        model_name = master.get("model", "")
        unit_price = master.get("price", 0)

        # 알림 조건 체크
        diff_amount = unit_price * diff
        trigger_amount = diff_amount >= threshold_amount
        trigger_qty = diff >= threshold_qty

        # 키워드 예외: 수량 기준만 무시, 금액 기준은 유지
        is_excluded = _is_excluded(prod_name, exclude_keywords)

        if is_excluded:
            if not trigger_amount:
                continue
            trigger = "amount"
        else:
            if not trigger_amount and not trigger_qty:
                continue
            trigger = "both" if (trigger_amount and trigger_qty) else ("amount" if trigger_amount else "qty")

        alerts.append({
            "prod_cd": prod_cd,
            "prod_name": prod_name,
            "model_name": model_name,
            "unit_price": unit_price,
            "prev_qty": prev_qty,
            "curr_qty": curr_qty,
            "diff_qty": diff,
            "diff_amount": diff_amount,
            "trigger": trigger,
        })

    alerts.sort(key=lambda x: x["diff_amount"], reverse=True)
    return alerts


def _is_excluded(prod_name: str, keywords: list) -> bool:
    """품목명에 제외 키워드가 포함되어 있는지 체크"""
    name_upper = prod_name.upper()
    for kw in keywords:
        if kw.upper() in name_upper:
            return True
    return False


# ─── 알림 이력 저장 ────────────────────────────────────────────

def save_alert_history(conn, alerts: list, check_date: str):
    """알림 이력을 DB에 저장"""
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    for alert in alerts:
        conn.execute(
            """INSERT INTO inventory_alert_history
               (check_date, prod_cd, prod_name, model_name, unit_price,
                prev_qty, curr_qty, diff_qty, diff_amount, trigger_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                check_date,
                alert["prod_cd"], alert["prod_name"], alert["model_name"],
                alert["unit_price"], alert["prev_qty"], alert["curr_qty"],
                alert["diff_qty"], alert["diff_amount"], alert["trigger"],
                now_str,
            )
        )
    conn.commit()
    logger.info(f"알림 이력 저장: {check_date}, {len(alerts)}건")


# ─── 설정/키워드 관리 (동기 DB) ─────────────────────────────────

DEFAULT_EXCLUDE_KEYWORDS = ["BOOT", "부트", "콘넥터후드", "Hood케이스", "모듈러", "콘넥터", "먼지"]

def get_exclude_keywords(conn) -> list:
    try:
        rows = conn.execute("SELECT keyword FROM inventory_exclude_keywords ORDER BY keyword").fetchall()
        return [row[0] for row in rows]
    except Exception:
        return list(DEFAULT_EXCLUDE_KEYWORDS)

def add_exclude_keyword(conn, keyword: str):
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("INSERT OR IGNORE INTO inventory_exclude_keywords (keyword, created_at) VALUES (?, ?)", (keyword, now_str))
    conn.commit()

def remove_exclude_keyword(conn, keyword: str):
    conn.execute("DELETE FROM inventory_exclude_keywords WHERE keyword = ?", (keyword,))
    conn.commit()

def get_alert_settings(conn) -> dict:
    try:
        rows = conn.execute("SELECT key, value FROM inventory_alert_settings").fetchall()
        settings = {row[0]: row[1] for row in rows}
        return {
            "threshold_amount": int(settings.get("threshold_amount", 500000)),
            "threshold_qty": int(settings.get("threshold_qty", 100)),
            "telegram_bot_token": settings.get("telegram_bot_token", ""),
            "telegram_chat_id": settings.get("telegram_chat_id", ""),
            "enabled": settings.get("enabled", "true") == "true",
        }
    except Exception:
        return {
            "threshold_amount": 500000, "threshold_qty": 100,
            "telegram_bot_token": "", "telegram_chat_id": "",
            "enabled": True,
        }

def update_alert_settings(conn, settings: dict):
    for key, value in settings.items():
        conn.execute(
            """INSERT INTO inventory_alert_settings (key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
            (key, str(value))
        )
    conn.commit()


# ─── 메인 모니터링 실행 ────────────────────────────────────────

async def run_inventory_monitor(telegram_service=None) -> dict:
    """
    재고 모니터링 메인 실행 함수

    1. 오늘 재고 조회 → 스냅샷 저장
    2. 어제(평일 기준) 스냅샷과 비교
    3. 알림 대상 추출 → 텔레그램 발송
    """
    from db.database import get_connection

    now = datetime.now(KST)
    today = now.strftime("%Y%m%d")

    # 전 영업일 계산 (월요일이면 금요일)
    if now.weekday() == 0:  # 월요일
        prev_date = (now - timedelta(days=3)).strftime("%Y%m%d")
    else:
        prev_date = (now - timedelta(days=1)).strftime("%Y%m%d")

    try:
        # 1. ERP에서 오늘 재고 조회
        logger.info(f"[재고모니터] 금일({today}) 재고 조회 시작...")
        curr_inventory = await fetch_inventory_from_erp(today)

        # 2. 스냅샷 저장 및 전일 스냅샷 조회
        conn = get_connection()
        try:
            save_snapshot(conn, curr_inventory, today)
            prev_snapshot = get_snapshot(conn, prev_date)

            if not prev_snapshot:
                msg = f"전일({prev_date}) 스냅샷이 없습니다. 오늘 스냅샷만 저장했습니다. (최초 실행 시 정상)"
                logger.warning(msg)
                return {"status": "no_prev", "alerts_count": 0, "message": msg}

            # 3. 설정 로딩
            settings = get_alert_settings(conn)
            if not settings.get("enabled", True):
                return {"status": "disabled", "alerts_count": 0, "message": "알림이 비활성화 상태입니다."}

            exclude_keywords = get_exclude_keywords(conn)
            products_master = load_products_master()

            # 4. 비교
            curr_snapshot = {item["PROD_CD"]: item["BAL_QTY"] for item in curr_inventory}
            alerts = compare_inventory(
                prev_snapshot=prev_snapshot,
                curr_snapshot=curr_snapshot,
                products_master=products_master,
                exclude_keywords=exclude_keywords,
                threshold_amount=settings["threshold_amount"],
                threshold_qty=settings["threshold_qty"],
            )

            # 5. 알림 이력 저장
            save_alert_history(conn, alerts, today)

        finally:
            conn.close()

        # 6. 텔레그램 발송
        if alerts and telegram_service:
            message = format_telegram_message(alerts, today, prev_date)
            await telegram_service.send_message(message)
            logger.info(f"[재고모니터] 텔레그램 발송 완료: {len(alerts)}건")

        result_msg = f"재고 모니터링 완료: {today} (비교: {prev_date}), 알림 {len(alerts)}건"
        logger.info(f"[재고모니터] {result_msg}")

        return {
            "status": "ok",
            "alerts_count": len(alerts),
            "alerts": alerts,
            "message": result_msg,
        }

    except Exception as e:
        error_msg = f"재고 모니터링 실패: {str(e)}"
        logger.error(f"[재고모니터] {error_msg}", exc_info=True)

        if telegram_service:
            try:
                await telegram_service.send_message(f"⚠️ 재고 모니터링 오류\n{error_msg}")
            except Exception:
                pass

        return {"status": "error", "alerts_count": 0, "message": error_msg}


# ─── 텔레그램 메시지 포맷팅 ────────────────────────────────────

def format_telegram_message(alerts: list, today: str, prev_date: str) -> str:
    today_fmt = f"{today[:4]}-{today[4:6]}-{today[6:]}"
    prev_fmt = f"{prev_date[:4]}-{prev_date[4:6]}-{prev_date[6:]}"

    lines = [
        f"📦 <b>재고 변동 알림</b>",
        f"📅 {prev_fmt} → {today_fmt}",
        f"🔔 총 {len(alerts)}건의 재고 감소 감지",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    for i, alert in enumerate(alerts, 1):
        trigger_emoji = "💰" if alert["trigger"] == "amount" else "📉" if alert["trigger"] == "qty" else "🔥"

        lines.append(f"\n{trigger_emoji} <b>{i}. {alert['prod_name'][:40]}</b>")
        if alert["model_name"]:
            lines.append(f"   모델: {alert['model_name']}")
        lines.append(f"   품목코드: {alert['prod_cd']}")
        lines.append(f"   재고: {alert['prev_qty']:,.0f} → {alert['curr_qty']:,.0f} (<b>-{alert['diff_qty']:,.0f}개</b>)")
        lines.append(f"   단가: {alert['unit_price']:,.0f}원 | 감소금액: <b>{alert['diff_amount']:,.0f}원</b>")

        if alert["trigger"] == "amount":
            lines.append(f"   ⚡ 금액 기준 초과 (≥50만원)")
        elif alert["trigger"] == "qty":
            lines.append(f"   ⚡ 수량 기준 초과 (≥100개)")
        else:
            lines.append(f"   ⚡ 금액+수량 모두 초과")

    lines.append("\n━━━━━━━━━━━━━━━━━━━━")
    total_amount = sum(a["diff_amount"] for a in alerts)
    lines.append(f"\n📊 총 감소 금액: <b>{total_amount:,.0f}원</b>")
    lines.append(f"💡 재주문이 필요한 품목을 확인하세요!")

    return "\n".join(lines)
