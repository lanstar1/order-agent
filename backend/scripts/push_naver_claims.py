"""네이버 스마트스토어 반품/교환 클레임 릴레이 (NAS → Render)

Render 출구 IP를 네이버 API 센터에 등록할 수 없어, 네이버에 IP가 이미
등록된 사무실 NAS에서 클레임을 수집해 Render의 ingest 엔드포인트로 푸시한다.

★ 의도적으로 저장소 코드를 import하지 않는 독립 실행형이다.
  NAS 컨테이너 이미지는 구버전 코드(demo 브랜치)라 services.naver_client에
  fetch_claims()가 없을 수 있다. httpx + bcrypt만 있으면 어디서든 돈다.
  (수집/정규화 로직은 backend/services/naver_client.py fetch_claims와 동일하게 유지할 것)

NAS 사용 예 (data 볼륨에 두면 컨테이너 안에서 /app/data/로 보인다):
    docker exec -e CLAIMS_RELAY_KEY=<키> order-agent \
        python /app/data/_push_naver_claims.py

환경변수:
    CLAIMS_RELAY_KEY    : Render 설정 페이지의 '클레임 릴레이 키'와 동일 값 (필수)
    CLAIMS_RELAY_TARGET : 푸시 대상 URL (기본 https://order-agent-ffr7.onrender.com)
    CLAIMS_RELAY_DAYS   : 수집 기간 일수 (기본 7)
    CLAIMS_RELAY_MODE   : full(기본) — 무조건 수집·푸시
                          poll — 1분 크론용. Render에 즉시 동기화 요청이 있거나
                                 마지막 전체 실행 후 INTERVAL_MIN 경과 시에만 전체 실행
    CLAIMS_RELAY_INTERVAL_MIN : poll 모드 자동 전체 실행 주기 (기본 30분)
    CLAIMS_RELAY_STATE  : poll 모드 상태 파일 경로 (기본 /app/data/_claims_relay_state)
    NAVER_COMMERCE_CLIENT_ID/SECRET 또는 NAVER_CLIENT_ID/SECRET : 커머스 API 키
"""
import os
import sys
import time
import base64
import logging
from datetime import datetime, timedelta, timezone

import httpx
import bcrypt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("claims-relay")

CLIENT_ID = os.getenv("NAVER_COMMERCE_CLIENT_ID", "") or os.getenv("NAVER_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "") or os.getenv("NAVER_CLIENT_SECRET", "")
COMMERCE_URL = os.getenv("NAVER_COMMERCE_URL", "https://api.commerce.naver.com")
TARGET = os.getenv("CLAIMS_RELAY_TARGET", "https://order-agent-ffr7.onrender.com").rstrip("/")
RELAY_KEY = os.getenv("CLAIMS_RELAY_KEY", "")
DAYS = int(os.getenv("CLAIMS_RELAY_DAYS", "7"))
MODE = os.getenv("CLAIMS_RELAY_MODE", "full")
INTERVAL_MIN = int(os.getenv("CLAIMS_RELAY_INTERVAL_MIN", "30"))
STATE_FILE = os.getenv("CLAIMS_RELAY_STATE", "/app/data/_claims_relay_state")
KST = timezone(timedelta(hours=9))


def get_token() -> str:
    timestamp = str(int(time.time() * 1000))
    pwd = f"{CLIENT_ID}_{timestamp}"
    sign = base64.b64encode(bcrypt.hashpw(pwd.encode(), CLIENT_SECRET.encode())).decode()
    r = httpx.post(
        f"{COMMERCE_URL}/external/v1/oauth2/token",
        data={
            "client_id": CLIENT_ID,
            "timestamp": timestamp,
            "client_secret_sign": sign,
            "grant_type": "client_credentials",
            "type": "SELF",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    body = r.json()
    if r.status_code != 200:
        raise RuntimeError(f"토큰 발급 실패 ({r.status_code}): {body.get('message') or body}")
    return body["access_token"]


def normalize_claim(item: dict):
    """product-orders/query 응답 1건 → 클레임 dict (반품/교환 외 None)"""
    po = item.get("productOrder", {}) or {}
    order = item.get("order", {}) or {}
    claim_type = po.get("claimType", "")
    if claim_type not in ("RETURN", "EXCHANGE"):
        return None
    detail = (item.get("return") if claim_type == "RETURN" else item.get("exchange")) or {}
    addr = po.get("shippingAddress", {}) or {}
    return {
        "product_order_id": po.get("productOrderId") or "",
        "order_id": order.get("orderId") or "",
        "claim_type": "반품" if claim_type == "RETURN" else "교환",
        "claim_status": detail.get("claimStatus") or po.get("claimStatus") or "",
        "customer_name": order.get("ordererName") or addr.get("name") or "",
        "contact_info": order.get("ordererTel") or addr.get("tel1") or addr.get("tel2") or "",
        "product_name": po.get("productName") or "",
        "product_option": po.get("productOption") or "",
        "quantity": po.get("quantity") or 1,
        "reason_code": detail.get("returnReason") or detail.get("exchangeReason") or "",
        "detailed_reason": detail.get("returnDetailedReason") or detail.get("exchangeDetailedReason") or "",
        "claim_request_date": detail.get("claimRequestDate") or "",
        "collect_courier": detail.get("collectDeliveryCompany") or "",
        "collect_tracking_no": detail.get("collectTrackingNumber") or "",
        # 반품 배송비 처리(이미 한글). 청구 없으면 빈 값.
        "shipping_cost_status": detail.get("claimDeliveryFeePayMethod") or "",
    }


def _request_throttled(method: str, url: str, *, max_tries: int = 4, **kwargs) -> httpx.Response:
    """레이트리밋 대응 요청 — 호출 간 0.4초 간격 + 429 시 지수 백오프 재시도"""
    time.sleep(0.4)
    for attempt in range(1, max_tries + 1):
        r = httpx.request(method, url, **kwargs)
        if r.status_code != 429:
            return r
        wait = 1.5 * attempt
        logger.warning(f"429 레이트리밋 — {wait:.1f}초 대기 후 재시도 ({attempt}/{max_tries})")
        time.sleep(wait)
    return r


def fetch_claims(token: str, days: int) -> list:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    now = datetime.now(KST)
    chunk_start = now - timedelta(days=days)
    url = f"{COMMERCE_URL}/external/v1/pay-order/seller/product-orders/last-changed-statuses"

    # 교환(EXCHANGE)은 CLAIM_REQUESTED에 안 잡히고 COLLECT_DONE에서만 잡히는 케이스가 있어
    # 두 변경유형을 모두 조회한다. (반품 요청 + 수거완료 → 반품/교환 모두 커버)
    last_changed_types = ("CLAIM_REQUESTED", "COLLECT_DONE")
    product_order_ids = []
    for lct in last_changed_types:
        chunk_start = now - timedelta(days=days)
        while chunk_start < now:
            chunk_end = min(chunk_start + timedelta(hours=23, minutes=59, seconds=59), now)
            params = {
                "lastChangedFrom": chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"),
                "lastChangedTo": chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"),
                "lastChangedType": lct,
            }
            for _ in range(5):  # 구간당 300건 초과 시 more 페이징
                r = _request_throttled("GET", url, headers=headers, params=params, timeout=30)
                r.raise_for_status()
                data = r.json().get("data", {}) or {}
                items = data.get("lastChangeStatuses", []) or []
                product_order_ids.extend(i["productOrderId"] for i in items if i.get("productOrderId"))
                more = data.get("more") or {}
                if not more.get("moreSequence"):
                    break
                params = {**params, "lastChangedFrom": more.get("moreFrom"), "moreSequence": more["moreSequence"]}
            chunk_start = chunk_end + timedelta(seconds=1)

    product_order_ids = list(dict.fromkeys(product_order_ids))
    if not product_order_ids:
        return []

    claims = []
    detail_url = f"{COMMERCE_URL}/external/v1/pay-order/seller/product-orders/query"
    for i in range(0, len(product_order_ids), 50):
        batch = product_order_ids[i:i + 50]
        r = _request_throttled("POST", detail_url, headers=headers, json={"productOrderIds": batch}, timeout=30)
        if r.status_code != 200:
            logger.warning(f"클레임 상세 조회 실패 ({r.status_code}): {r.text[:200]}")
            continue
        for item in r.json().get("data", []) or []:
            claim = normalize_claim(item)
            if claim:
                claims.append(claim)
    return claims


def _should_full_run() -> str:
    """poll 모드 — 전체 실행 사유 반환 (빈 문자열이면 스킵)"""
    try:
        r = httpx.get(
            f"{TARGET}/api/cs/naver-claims/relay-poll",
            headers={"X-Relay-Key": RELAY_KEY},
            timeout=30,
        )
        if r.status_code == 200 and r.json().get("sync_requested"):
            return "사용자 즉시 동기화 요청"
    except Exception as e:
        logger.warning(f"relay-poll 실패 (무시하고 주기 판정): {e}")

    try:
        last = float(open(STATE_FILE).read().strip())
    except Exception:
        last = 0
    if time.time() - last >= INTERVAL_MIN * 60:
        return f"주기 도래 ({INTERVAL_MIN}분)"
    return ""


def main() -> int:
    if not RELAY_KEY:
        logger.error("CLAIMS_RELAY_KEY 미설정 — 푸시 인증 키가 필요합니다.")
        return 1
    if not CLIENT_ID or not CLIENT_SECRET:
        logger.error("네이버 커머스 API 키 미설정 (NAVER_CLIENT_ID/SECRET)")
        return 1

    if MODE == "poll":
        reason = _should_full_run()
        if not reason:
            return 0  # 조용히 종료 (1분 크론 로그 비대 방지)
        logger.info(f"전체 동기화 실행 — {reason}")

    token = get_token()
    claims = fetch_claims(token, DAYS)
    logger.info(f"네이버 클레임 {len(claims)}건 수집 (기간 {DAYS}일)")

    r = httpx.post(
        f"{TARGET}/api/cs/naver-claims/ingest",
        json={"claims": claims},
        headers={"X-Relay-Key": RELAY_KEY},
        timeout=120,
    )
    if r.status_code != 200:
        logger.error(f"푸시 실패 ({r.status_code}): {r.text[:300]}")
        return 1

    body = r.json()
    logger.info(f"푸시 완료: 수신 {body.get('received')}건, 신규 {body.get('new')}건, 갱신 {body.get('updated')}건")
    try:
        open(STATE_FILE, "w").write(str(time.time()))
    except Exception as e:
        logger.warning(f"상태 파일 기록 실패: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
