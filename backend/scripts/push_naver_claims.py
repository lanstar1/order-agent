"""네이버 스마트스토어 반품/교환 클레임 릴레이 (NAS → Render)

Render 출구 IP를 네이버 API 센터에 등록할 수 없어, 네이버에 IP가 이미
등록된 사무실 NAS에서 클레임을 수집해 Render의 ingest 엔드포인트로 푸시한다.

NAS 컨테이너 안에서 실행 (앱 재시작 불필요):
    docker exec -e CLAIMS_RELAY_KEY=<키> [-e CLAIMS_RELAY_TARGET=<URL>] \
        <컨테이너명> python backend/scripts/push_naver_claims.py

환경변수:
    CLAIMS_RELAY_KEY    : Render 설정 페이지의 '클레임 릴레이 키'와 동일 값 (필수)
    CLAIMS_RELAY_TARGET : 푸시 대상 URL (기본 https://order-agent-ffr7.onrender.com)
    CLAIMS_RELAY_DAYS   : 수집 기간 일수 (기본 7)
    NAVER_CLIENT_ID/SECRET 또는 NAVER_COMMERCE_CLIENT_ID/SECRET : 커머스 API 키
"""
import os
import sys
import asyncio
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("claims-relay")

TARGET = os.getenv("CLAIMS_RELAY_TARGET", "https://order-agent-ffr7.onrender.com").rstrip("/")
RELAY_KEY = os.getenv("CLAIMS_RELAY_KEY", "")
DAYS = int(os.getenv("CLAIMS_RELAY_DAYS", "7"))


async def main() -> int:
    if not RELAY_KEY:
        logger.error("CLAIMS_RELAY_KEY 미설정 — 푸시 인증 키가 필요합니다.")
        return 1

    import httpx
    from services.naver_client import naver_client

    claims = await naver_client.fetch_claims(days=DAYS)
    logger.info(f"네이버 클레임 {len(claims)}건 수집 (기간 {DAYS}일)")

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{TARGET}/api/cs/naver-claims/ingest",
            json={"claims": claims},
            headers={"X-Relay-Key": RELAY_KEY},
        )
    if r.status_code != 200:
        logger.error(f"푸시 실패 ({r.status_code}): {r.text[:300]}")
        return 1

    body = r.json()
    logger.info(f"푸시 완료: 수신 {body.get('received')}건, 신규 {body.get('new')}건, 갱신 {body.get('updated')}건")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
