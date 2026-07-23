#!/usr/bin/env python3
"""대화 상태 — 직전 모델 기억, 되묻기 대기.

왜 필요한가: 상담 질문의 82.9%에 모델 식별자가 없고, 그중 611건이 '이 제품/해당 제품'
같은 지시어로만 제품을 가리킨다. 인증 질문만 따로 보면 모델 미기재는 18.9%로 낮지만,
5건 중 1건을 되묻는 것은 사내 CS 체감이 나쁘다.

메모리 딕셔너리 + TTL. 프로세스 재시작이면 비는 것이 정상이다(대화 맥락은 영속 대상이 아니다).
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from . import config

#: '이 제품', '그거', '위 모델' 처럼 직전 모델을 가리키는 표현
DEICTIC_RX = re.compile(
    r'이\s*(제품|케이블|모델|것|거)|해당\s*(제품|모델|건)|그\s*(제품|모델|거)|'
    r'위\s*(모델|제품)|같은\s*(제품|모델)|동일\s*(제품|모델)|아까\s*그|이거|그거|이넘')


@dataclass
class Session:
    session_id: str
    last_models: List[str] = field(default_factory=list)
    last_doc_type: Optional[str] = None
    pending_clarification: Optional[Dict[str, Any]] = None
    turns: List[Dict[str, Any]] = field(default_factory=list)
    touched: float = field(default_factory=time.time)

    def remember(self, models: List[str], doc_type: Optional[str] = None) -> None:
        if models:
            self.last_models = list(models)
        if doc_type:
            self.last_doc_type = doc_type
        self.touched = time.time()

    def add_turn(self, message: str, reply: str, route: str) -> None:
        self.turns.append({'message': message, 'reply': reply, 'route': route,
                           'at': time.time()})
        if len(self.turns) > config.SESSION_MAX_TURNS:
            del self.turns[:-config.SESSION_MAX_TURNS]
        self.touched = time.time()

    def as_dict(self) -> Dict[str, Any]:
        return {'session_id': self.session_id, 'last_models': self.last_models,
                'last_doc_type': self.last_doc_type,
                'pending_clarification': self.pending_clarification,
                'turns': len(self.turns)}


class SessionStore:
    def __init__(self, ttl: int = config.SESSION_TTL_SEC,
                 max_sessions: int = config.SESSION_MAX_SESSIONS):
        self.ttl = ttl
        self.max_sessions = max_sessions
        self._data: Dict[str, Session] = {}
        self._lock = threading.Lock()

    def _sweep(self, now: float) -> None:
        dead = [k for k, s in self._data.items() if now - s.touched > self.ttl]
        for k in dead:
            self._data.pop(k, None)
        if len(self._data) > self.max_sessions:
            for k, _s in sorted(self._data.items(), key=lambda kv: kv[1].touched)[
                    :len(self._data) - self.max_sessions]:
                self._data.pop(k, None)

    def get(self, session_id: str) -> Session:
        """빈 session_id 는 '상태 없음' 세션 — 저장하지 않는다."""
        now = time.time()
        if not session_id:
            return Session(session_id='')
        with self._lock:
            self._sweep(now)
            s = self._data.get(session_id)
            if s is None:
                s = Session(session_id=session_id)
                self._data[session_id] = s
            s.touched = now
            return s

    def drop(self, session_id: str) -> None:
        with self._lock:
            self._data.pop(session_id, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        return len(self._data)


STORE = SessionStore()


def get(session_id: str) -> Session:
    return STORE.get(session_id)


def has_deictic(message: str) -> bool:
    return bool(DEICTIC_RX.search(message or ''))


def resolve_choice(message: str, candidates: List[str]) -> Optional[str]:
    """되묻기 응답에서 후보 하나를 골라낸다.

    '2번', '두번째', 'LS-6UTPD', 'LS-6UTPD 요' 를 전부 받는다.
    후보를 특정하지 못하면 None — 임의로 고르지 않는다.
    """
    if not candidates:
        return None
    msg = (message or '').strip()
    up = msg.upper().replace('_', '-')
    exact = [c for c in candidates if c.upper() == up]
    if exact:
        return exact[0]
    # 'LS-HD21R' 은 'LS-HD21' 을 부분문자열로 포함한다 — 가장 긴 일치를 고른다
    contained = sorted((c for c in candidates if c.upper() in up), key=len, reverse=True)
    if contained and (len(contained) == 1 or len(contained[0]) > len(contained[1])):
        return contained[0]
    m = re.search(r'(\d+)\s*(?:번|번째)?', msg)
    if m and re.fullmatch(r'\s*\d+\s*(번|번째)?\s*(요|이요|입니다|이에요)?\s*', msg):
        i = int(m.group(1)) - 1
        if 0 <= i < len(candidates):
            return candidates[i]
    words = ('첫', '두', '세', '네', '다섯')
    for i, w in enumerate(words):
        if msg.startswith(w) and i < len(candidates):
            return candidates[i]
    return None
