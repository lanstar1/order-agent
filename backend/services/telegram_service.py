"""
텔레그램 봇 알림 서비스
- 재고 변동 알림을 텔레그램으로 발송
- HTML 파싱 모드 지원
"""

import logging
import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"


class TelegramService:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_base = TELEGRAM_API_BASE.format(token=bot_token)

    async def send_message(self, text: str, parse_mode: str = "HTML") -> dict:
        """텔레그램 메시지 발송 (4000자 초과 시 자동 분할)"""
        if not self.bot_token or not self.chat_id:
            logger.warning("텔레그램 봇 토큰 또는 chat_id가 설정되지 않았습니다.")
            return {"ok": False, "description": "Bot token or chat_id not configured"}

        if len(text) > 4000:
            return await self._send_long_message(text, parse_mode)

        url = f"{self.api_base}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, json=payload)
                result = resp.json()

            if result.get("ok"):
                logger.info(f"텔레그램 발송 성공 (chat_id: {self.chat_id})")
            else:
                logger.error(f"텔레그램 발송 실패: {result.get('description', '')}")

            return result
        except Exception as e:
            logger.error(f"텔레그램 발송 오류: {e}")
            return {"ok": False, "description": str(e)}

    async def _send_long_message(self, text: str, parse_mode: str) -> dict:
        """긴 메시지를 분할 발송"""
        chunks = []
        lines = text.split("\n")
        current_chunk = ""

        for line in lines:
            if len(current_chunk) + len(line) + 1 > 4000:
                chunks.append(current_chunk)
                current_chunk = line
            else:
                current_chunk += ("\n" if current_chunk else "") + line

        if current_chunk:
            chunks.append(current_chunk)

        last_result = {}
        for i, chunk in enumerate(chunks):
            if i > 0:
                chunk = f"(계속 {i+1}/{len(chunks)})\n\n{chunk}"
            last_result = await self.send_message(chunk, parse_mode)

        return last_result

    async def test_connection(self) -> dict:
        """봇 연결 테스트"""
        url = f"{self.api_base}/getMe"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url)
                result = resp.json()

            if result.get("ok"):
                bot_info = result["result"]
                return {
                    "ok": True,
                    "bot_name": bot_info.get("first_name", ""),
                    "bot_username": bot_info.get("username", ""),
                }
            else:
                return {"ok": False, "error": result.get("description", "")}
        except Exception as e:
            return {"ok": False, "error": str(e)}
