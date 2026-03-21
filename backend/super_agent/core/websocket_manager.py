"""
WebSocket 매니저 — Super Agent 실시간 진행상황 전송
"""
import json
import logging
import asyncio
from typing import Dict, Set, Optional
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class WSConnectionManager:
    """Job별 WebSocket 연결 관리"""

    def __init__(self):
        # job_id → set of websocket connections
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, job_id: str):
        """WebSocket 연결 수락 및 등록"""
        await websocket.accept()
        async with self._lock:
            if job_id not in self.active_connections:
                self.active_connections[job_id] = set()
            self.active_connections[job_id].add(websocket)
        logger.info(f"[WS] 연결: job_id={job_id}, 총 {len(self.active_connections.get(job_id, set()))}개")

    async def disconnect(self, websocket: WebSocket, job_id: str):
        """WebSocket 연결 해제"""
        async with self._lock:
            if job_id in self.active_connections:
                self.active_connections[job_id].discard(websocket)
                if not self.active_connections[job_id]:
                    del self.active_connections[job_id]
        logger.info(f"[WS] 연결 해제: job_id={job_id}")

    async def send_progress(
        self,
        job_id: str,
        status: str,
        message: str,
        progress_pct: int = 0,
        task_id: Optional[str] = None,
        task_key: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        """특정 Job의 모든 WebSocket 클라이언트에 진행상황 전송"""
        payload = {
            "type": "progress",
            "job_id": job_id,
            "status": status,
            "message": message,
            "progress_pct": progress_pct,
        }
        if task_id:
            payload["task_id"] = task_id
        if task_key:
            payload["task_key"] = task_key
        if data:
            payload["data"] = data

        await self._broadcast(job_id, payload)

    async def send_task_update(
        self,
        job_id: str,
        task_id: str,
        task_key: str,
        status: str,
        progress_pct: int,
        message: str,
    ):
        """개별 태스크 상태 업데이트"""
        payload = {
            "type": "task_update",
            "job_id": job_id,
            "task_id": task_id,
            "task_key": task_key,
            "status": status,
            "progress_pct": progress_pct,
            "message": message,
        }
        await self._broadcast(job_id, payload)

    async def send_completed(self, job_id: str, result: dict):
        """작업 완료 알림"""
        payload = {
            "type": "completed",
            "job_id": job_id,
            "status": "completed",
            "message": "작업이 완료되었습니다.",
            "progress_pct": 100,
            "data": result,
        }
        await self._broadcast(job_id, payload)

    async def send_error(self, job_id: str, error_msg: str):
        """에러 알림"""
        payload = {
            "type": "error",
            "job_id": job_id,
            "status": "failed",
            "message": error_msg,
            "progress_pct": 0,
        }
        await self._broadcast(job_id, payload)

    async def _broadcast(self, job_id: str, payload: dict):
        """특정 Job의 모든 연결에 메시지 전송"""
        connections = self.active_connections.get(job_id, set()).copy()
        if not connections:
            return

        dead = set()
        text = json.dumps(payload, ensure_ascii=False)

        for ws in connections:
            try:
                await ws.send_text(text)
            except Exception:
                dead.add(ws)

        # 끊어진 연결 정리
        if dead:
            async with self._lock:
                if job_id in self.active_connections:
                    self.active_connections[job_id] -= dead
                    if not self.active_connections[job_id]:
                        del self.active_connections[job_id]

    def get_connection_count(self, job_id: str) -> int:
        return len(self.active_connections.get(job_id, set()))


# 싱글턴 인스턴스
ws_manager = WSConnectionManager()
