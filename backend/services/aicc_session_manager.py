"""
AICC 세션 관리자
인메모리 세션 관리 + DB 영구 저장
"""
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import WebSocket


class AICCSessionManager:
    def __init__(self):
        # 인메모리 세션 딕셔너리
        self.sessions: Dict[str, dict] = {}
        # 관리자 WebSocket 목록 (세션 목록 브로드캐스트용)
        self.admin_list_ws: List[WebSocket] = []

    def create_session(self, session_id: str, customer_name: str, model_name: str,
                       erp_code: str, menu: str) -> str:
        self.sessions[session_id] = {
            "session_id": session_id,
            "customer_name": customer_name,
            "selected_model": model_name,
            "erp_code": erp_code,
            "selected_menu": menu,
            "status": "active",
            "is_admin_intervened": False,
            "messages": [],
            "customer_ws": None,
            "admin_ws": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        return session_id

    def get_session(self, session_id: str) -> Optional[dict]:
        return self.sessions.get(session_id)

    def get_all_sessions(self) -> List[dict]:
        """관리자 대시보드용 — WebSocket 객체 제외하고 반환"""
        result = []
        for s in sorted(self.sessions.values(),
                        key=lambda x: x["created_at"], reverse=True):
            result.append(self._serialize(s))
        return result

    def _serialize(self, session: dict) -> dict:
        """WebSocket 객체 제거 후 직렬화 가능한 dict 반환"""
        return {k: (v.isoformat() if isinstance(v, datetime) else v)
                for k, v in session.items()
                if k not in ("customer_ws", "admin_ws")}

    def add_message(self, session_id: str, role: str, content: str):
        s = self.sessions.get(session_id)
        if not s:
            return
        msg = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
        }
        s["messages"].append(msg)
        s["updated_at"] = datetime.now()

    async def send_to_customer(self, session_id: str, data: dict):
        s = self.sessions.get(session_id)
        if s and s.get("customer_ws"):
            try:
                await s["customer_ws"].send_json(data)
            except Exception:
                s["customer_ws"] = None

    async def send_to_admin(self, session_id: str, data: dict):
        s = self.sessions.get(session_id)
        if s and s.get("admin_ws"):
            try:
                await s["admin_ws"].send_json(data)
            except Exception:
                s["admin_ws"] = None

    async def broadcast_session_update(self, session: dict):
        """세션 업데이트 → 관리자 목록 화면 전체에 브로드캐스트"""
        data = {"type": "session_update", "session": self._serialize(session)}
        dead = []
        for ws in self.admin_list_ws:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.admin_list_ws.remove(ws)

    def intervene(self, session_id: str):
        s = self.sessions.get(session_id)
        if s:
            s["is_admin_intervened"] = True
            s["status"] = "intervened"

    def close_session(self, session_id: str):
        s = self.sessions.get(session_id)
        if s:
            s["status"] = "closed"

    def cleanup_expired(self, timeout_hours: int = 1):
        """타임아웃 세션 정리"""
        cutoff = datetime.now() - timedelta(hours=timeout_hours)
        for sid, s in list(self.sessions.items()):
            if s["status"] != "closed" and s["updated_at"] < cutoff:
                s["status"] = "closed"


# 전역 싱글톤
session_manager = AICCSessionManager()
