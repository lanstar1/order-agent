#!/usr/bin/env python3
"""랜스타 제품 인증·기술자료 조회 엔진.

표준 라이브러리만으로 동작한다(드라이브 API 는 있으면 쓰고 없으면 로컬 폴백).

    from cert_lookup import respond, lookup

    print(respond('LS-6UTPD-3MG', 'RoHS'))            # 사내용(기본)
    print(respond('LS-1600HQ', 'RoHS', mode='customer'))
    lookup('LS-HDAOC-30M', 'RoHS').type_result.material_missing   # ['절연']

응답 규칙(없는 자료를 만들지 않기, 자재 묶음 표기, O* 주의문구, 스캔 문서 인용 금지)은
policy 와 render 가 강제한다. 원자료 그대로가 필요하면 search 의 결과 객체를 쓰면 된다.
"""
from .drive import DriveRef, DriveResolver, find_drive_root
from .loader import (BUNDLE_TYPES, KnowledgeBase, MATRIX_COLS, display_members, load_kb,
                     parse_other_types, reset_cache)
from .policy import CUSTOMER, INTERNAL, Policy, effective_caveat, get_policy
from .render import (Answer, render_ambiguous, render_content, render_family, render_kc,
                     render_not_found, render_type, respond)
from .resolver import (Resolution, extract_models, normalize_model, resolve_all, resolve_cert,
                       resolve_kc)
from .search import (DocView, FamilyResult, KCResult, LookupResult, TypeResult,
                     classify_kc_contents, family_result, kc_result, lookup,
                     normalize_doc_type, rohs_history, type_result)

__all__ = [
    'Answer', 'BUNDLE_TYPES', 'CUSTOMER', 'DocView', 'DriveRef', 'DriveResolver',
    'FamilyResult', 'INTERNAL', 'KCResult', 'KnowledgeBase', 'LookupResult', 'MATRIX_COLS',
    'Policy', 'Resolution', 'TypeResult', 'classify_kc_contents', 'display_members',
    'effective_caveat', 'extract_models', 'family_result', 'find_drive_root', 'get_policy',
    'kc_result', 'load_kb', 'lookup', 'normalize_doc_type', 'normalize_model',
    'parse_other_types', 'render_ambiguous', 'render_content', 'render_family', 'render_kc',
    'render_not_found', 'render_type', 'reset_cache', 'resolve_all', 'resolve_cert',
    'resolve_kc', 'respond', 'rohs_history', 'type_result',
]
