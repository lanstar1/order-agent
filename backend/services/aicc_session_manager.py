"""
AICC 세션 관리자 — 인메모리(실시간) + DB(영구 저장)
"""
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import WebSocket


class AICCSessionManager:
    def __init__(self):
        self.sessions: Dict[str, dict] = {}
        self.admin_list_sockets: List[WebSocket] = []

    def create(self, name: str, model: str, erp_code: str, menu: str,
               channel: str = "shop", source: str = "") -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = {
            "session_id": sid,
            "customer_name": name,
            "selected_model": model,
            "erp_code": erp_code,
            "selected_menu": menu,
            "status": "active",
            "is_admin_intervened": False,
            "channel": channel,
            "source": source,
            "messages": [],
            "images": {},           # image_id → {media_type, base64_data}
            "customer_ws": None,
            "admin_ws": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        # DB 영구 저장
        self._db_save_session(sid, name, model, erp_code, menu, channel, source)
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
        skip = {"customer_ws", "admin_ws", "images"}
        d = {k: v for k, v in s.items() if k not in skip}
        d["created_at"] = s["created_at"].isoformat()
        d["updated_at"] = s["updated_at"].isoformat()
        return d

    def add_message(self, sid: str, role: str, content: str, image_id: str = None):
        s = self.sessions.get(sid)
        if not s:
            return
        msg = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
        }
        if image_id:
            msg["image_id"] = image_id
        s["messages"].append(msg)
        s["updated_at"] = datetime.now()
        # DB 영구 저장
        self._db_save_message(sid, role, content, image_id)

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
            self._db_update_status(sid, "intervened")

    def close(self, sid: str):
        s = self.sessions.get(sid)
        if s:
            s["status"] = "closed"
            s["updated_at"] = datetime.now()
            self._db_update_status(sid, "closed")

    def cleanup_expired(self, hours: int = 1):
        cutoff = datetime.now() - timedelta(hours=hours)
        for sid, s in list(self.sessions.items()):
            if s["status"] != "closed" and s["updated_at"] < cutoff:
                s["status"] = "closed"
                self._db_update_status(sid, "closed")

    # ── DB 영구 저장 (비동기가 아닌 동기 — 빠르므로 문제없음) ──────

    def _db_save_session(self, sid, name, model, erp_code, menu,
                         channel="shop", source=""):
        try:
            from .aicc_db import save_session
            save_session(sid, name, model, erp_code, menu, channel=channel, source=source)
            print(f"[AICC DB] 세션 저장 OK: {sid[:8]}… ({name}, {menu}, ch={channel}, src={source})")
        except Exception as e:
            import traceback
            print(f"[AICC DB] 세션 저장 오류: {e}\n{traceback.format_exc()}")

    def _db_save_message(self, sid, role, content, image_id=None):
        try:
            from .aicc_db import save_message
            save_message(sid, role, content, image_id or "")
            print(f"[AICC DB] 메시지 저장 OK: {sid[:8]}… [{role}] {content[:30]}")
        except Exception as e:
            import traceback
            print(f"[AICC DB] 메시지 저장 오류: {e}\n{traceback.format_exc()}")

    def _db_update_status(self, sid, status):
        try:
            from .aicc_db import update_session_status
            update_session_status(sid, status)
        except Exception as e:
            import traceback
            print(f"[AICC DB] 상태 업데이트 오류: {e}\n{traceback.format_exc()}")


session_manager = AICCSessionManager()
