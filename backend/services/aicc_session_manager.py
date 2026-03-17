"""
AICC 세션 관리자 — 인메모리 + DB 영구 저장
"""
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import WebSocket


class AICCSessionManager:
    def __init__(self):
        self.sessions: Dict[str, dict] = {}
        self.admin_list_sockets: List[WebSocket] = []

    def create(self, name: str, model: str, erp_code: str, menu: str) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = {
            "session_id": sid,
            "customer_name": name,
            "selected_model": model,
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
        return sid

    def get(self, sid: str) -> Optional[dict]:
        return self.sessions.get(sid)

    def all_serialized(self) -> List[dict]:
        skip = {"customer_ws", "admin_ws"}
        result = []
        for s in sorted(self.sessions.values(), key=lambda x: x["created_at"], reverse=True):
            d = {k: v for k, v in s.items() if k not in skip}
            d["created_at"] = s["created_at"].isoformat()
            d["updated_at"] = s["updated_at"].isoformat()
            result.append(d)
        return result

    def serialize(self, s: dict) -> dict:
        skip = {"customer_ws", "admin_ws"}
        d = {k: v for k, v in s.items() if k not in skip}
        d["created_at"] = s["created_at"].isoformat()
        d["updated_at"] = s["updated_at"].isoformat()
        return d

    def add_message(self, sid: str, role: str, content: str):
        s = self.sessions.get(sid)
        if not s:
            return
        s["messages"].append({
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
        })
        s["updated_at"] = datetime.now()

    async def send_customer(self, sid: str, data: dict):
        s = self.sessions.get(sid)
        if s and s.get("customer_ws"):
            try:
                await s["customer_ws"].send_json(data)
            except Exception:
                s["customer_ws"] = None

    async def send_admin(self, sid: str, data: dict):
        s = self.sessions.get(sid)
        if s and s.get("admin_ws"):
            try:
                await s["admin_ws"].send_json(data)
            except Exception:
                s["admin_ws"] = None

    async def broadcast_admins(self, data: dict):
        dead = []
        for ws in self.admin_list_sockets:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.admin_list_sockets.remove(ws)

    def intervene(self, sid: str):
        s = self.sessions.get(sid)
        if s:
            s["is_admin_intervened"] = True
            s["status"] = "intervened"
            s["updated_at"] = datetime.now()

    def close(self, sid: str):
        s = self.sessions.get(sid)
        if s:
            s["status"] = "closed"
            s["updated_at"] = datetime.now()

    def cleanup_expired(self, hours: int = 1):
        cutoff = datetime.now() - timedelta(hours=hours)
        for sid, s in list(self.sessions.items()):
            if s["status"] != "closed" and s["updated_at"] < cutoff:
                s["status"] = "closed"


session_manager = AICCSessionManager()
