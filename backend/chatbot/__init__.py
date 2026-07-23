#!/usr/bin/env python3
"""랜스타 사내 영업·CS 상담 챗봇 코어.

    from chatbot import chat
    print(chat('LS-6UTPD-3MG RoHS 인증서 주세요').text)

표준 라이브러리만으로 동작한다. LLM(anthropic)은 선택적 보조이며, 키가 없으면
예외 없이 규칙 기반 응답으로 폴백한다 — LLM 없이 동작하는 것이 기본 동작이다.

설계의 뼈대(전부 실측 근거)
  0단계 라우터  인증 의도면 1·2단계를 **건너뛴다**(병렬 아님). cert_lookup 이 유일 근거.
  1단계 상담이력 **모델 스코프 필수**. 모델 없이 검색하면 top-1 의 78.5%가 다른 모델이다.
  2단계 제품분석 화이트리스트 + 인증 문구 문장단위 삭제 + 제품유형 교차검증 게이트.
"""
from .config import GREETING
from .engine import ChatReply, chat, extract_models, reset_cache
from .history import Hit, QARow, guess_models_by_name, history_for_model, search_qa
from .router import route
from .session import STORE as SESSION_STORE
from .session import Session
from .spec import (is_cert_contaminated, product_image, spec_context,
                   verify_product_type)

__all__ = [
    'ChatReply', 'GREETING', 'Hit', 'QARow', 'SESSION_STORE', 'Session',
    'chat', 'extract_models', 'guess_models_by_name', 'history_for_model',
    'is_cert_contaminated', 'product_image', 'reset_cache', 'route', 'search_qa',
    'spec_context', 'verify_product_type',
]
