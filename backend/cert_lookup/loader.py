#!/usr/bin/env python3
"""데이터 적재와 인덱스 구축.

manifest 3종(문서/제품군/KC)은 한 번 읽으면 바뀌지 않으므로 모듈 레벨에 캐시한다.
CSV 파생물은 대부분 manifest 로 재현되므로 로드하지 않는다 — 유일하게 재현 불가능한
RoHS요청이력.csv 만 함께 싣는다("자료 없음"의 사유를 가르는 근거이기 때문).

파일 경로는 전부 이 모듈의 상수이고 환경변수로 덮어쓸 수 있다.
"""
from __future__ import annotations

import csv
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_tz
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------- 경로 상수

def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


#: 저장소 루트. cert_lookup 이 저장소 안에 있으므로 기본값은 상위 디렉터리.
REPO = _env('CERT_KB_REPO', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CERT_DIR = _env('CERT_KB_CERT_DIR', os.path.join(REPO, '제품인증자료'))
KC_DIR = _env('CERT_KB_KC_DIR', os.path.join(REPO, 'KC적합등록자료'))

DOC_MANIFEST = _env('CERT_KB_DOC_MANIFEST', os.path.join(CERT_DIR, 'manifest.jsonl'))
FAMILY_MANIFEST = _env('CERT_KB_FAMILY_MANIFEST', os.path.join(CERT_DIR, 'manifest_family.jsonl'))
KC_MANIFEST = _env('CERT_KB_KC_MANIFEST', os.path.join(KC_DIR, 'manifest.jsonl'))
DRIVE_FOLDERS = _env('CERT_KB_DRIVE_FOLDERS', os.path.join(CERT_DIR, 'drive_folders.json'))
ROHS_HISTORY = _env('CERT_KB_ROHS_HISTORY', os.path.join(CERT_DIR, 'RoHS요청이력.csv'))
MODEL_MATRIX = _env('CERT_KB_MODEL_MATRIX', os.path.join(CERT_DIR, '자료보유매트릭스.csv'))
REQUEST_LEDGER = _env('CERT_KB_REQUEST_LEDGER', os.path.join(CERT_DIR, '자료요청이력.csv'))
LIB_DIR = _env('CERT_KB_LIB_DIR', os.path.join(CERT_DIR, '_lib'))

#: 드라이브 동기화 루트를 찾을 때 쓰는 폴더 이름(계정 부분은 스캔으로 찾는다).
DRIVE_ROOT_NAME = _env('CERT_KB_DRIVE_ROOT_NAME', '랜스타_인증자료')
#: 동기화 루트를 직접 지정하고 싶을 때.
DRIVE_ROOT = os.environ.get('CERT_KB_DRIVE_ROOT') or None

#: coverage 매트릭스 축. 이 5종만 precomputed 마크를 가진다.
MATRIX_COLS = ('Data sheet', 'RoHS', 'CE', 'CoC', 'Fluke test')
#: 자재 커버리지(covered/missing)가 산출되는 자료유형.
BUNDLE_TYPES = ('RoHS', 'REACH', 'PFAS', 'MSDS/SDS')

KNOWN_DOC_TYPES = ('Data sheet', 'RoHS', 'CE', 'CoC', 'Fluke test', 'Test report', 'REACH',
                   'MSDS/SDS', 'PFAS', 'UL', 'Drawing', 'Manual', 'Other-cert',
                   'HDMI-DP-cert')

#: members[] 에 섞여 있는 와일드카드 표기(LS-6UTPD-*M, -XXM, 1M~20M). 실판매 모델이 아니다.
WILDCARD_MEMBER = re.compile(r'\*|X{1,2}M\b|XXM|~|\bSERIES\b|,', re.IGNORECASE)


# ---------------------------------------------------------------- 저수준 읽기

def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    """한 줄이 깨져 있어도 전체 로딩이 실패하지 않도록 줄 단위로 무시한다."""
    out: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return out
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    """저장소 CSV 공통 규칙: UTF-8 BOM + 파일마다 개수가 다른 '#' 주석행."""
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8-sig', newline='') as fh:
        rows = list(csv.reader(fh))
    n = 0
    while n < len(rows) and rows[n] and rows[n][0].lstrip('"').startswith('#'):
        n += 1
    if n >= len(rows):
        return []
    header = rows[n]
    return [dict(zip(header, r)) for r in rows[n + 1:] if any(c.strip() for c in r)]


def to_iso_date(value: Optional[str]) -> str:
    """ISO / RFC2822 가 섞여 있는 날짜 필드를 ISO 로 통일한다(실패 시 원문 유지).

    KC·RoHS이력의 날짜는 시각이 없는 'Sun, 13 Mar 2022' 형태라 email.utils 파서가 전부
    None 을 돌려준다 — strptime 폴백이 필요하다.
    """
    v = (value or '').strip()
    if not v:
        return ''
    if len(v) == 10 and v[4] == '-' and v[7] == '-':
        return v
    parsed = parsedate_tz(v)
    if parsed:
        return f'{parsed[0]:04d}-{parsed[1]:02d}-{parsed[2]:02d}'
    for fmt in ('%a, %d %b %Y', '%d %b %Y'):
        try:
            return datetime.strptime(v, fmt).date().isoformat()
        except ValueError:
            continue
    return v


# ---------------------------------------------------------------- 자료구조

@dataclass
class RohsRequest:
    """RoHS요청이력.csv 한 행. 요청 모델 토큰에는 파싱 절단 아티팩트가 섞여 있다."""
    date_raw: str
    date: str
    subject: str
    models: List[str] = field(default_factory=list)
    status: Dict[str, str] = field(default_factory=dict)


@dataclass
class KnowledgeBase:
    """조회에 필요한 전부. load_kb() 가 캐시해 재사용한다."""
    docs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    families: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    family_keys: List[str] = field(default_factory=list)
    #: 모델(대문자) -> 그 모델을 members 로 갖는 family 키들. prefix 충돌 122쌍 보정용.
    member_index: Dict[str, Set[str]] = field(default_factory=dict)
    #: family -> 문서 레코드 목록(manifest 조인 완료).
    docs_by_family: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    kc: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    kc_models: List[str] = field(default_factory=list)
    rohs_requests: List[RohsRequest] = field(default_factory=list)
    #: (모델 대문자, 자료유형) -> 요청 이력. 발송메일 전수 판독에서 나온 대장으로,
    #: RoHS 만 다루던 기존 이력(33통)보다 넓다(전 자료유형 455행).
    request_ledger: Dict[tuple, Dict[str, str]] = field(default_factory=dict)
    #: 모델(대문자) -> {자료유형: 마크}. 제품군 coverage 와 별표가 어긋나는 7칸이 있어
    #: (예: LS-OM3-LCLC RoHS 는 모델 O*, 제품군 O) 별표 판정에는 이쪽도 함께 본다.
    model_marks: Dict[str, Dict[str, str]] = field(default_factory=dict)
    drive_folders: Dict[str, Dict[str, str]] = field(default_factory=dict)
    #: 로딩 중 발견한 문제(파일 없음 등). 죽지 않고 여기에 남긴다.
    problems: List[str] = field(default_factory=list)

    # -- 조회 헬퍼 -------------------------------------------------
    def family(self, key: str) -> Optional[Dict[str, Any]]:
        """제품군 레코드."""
        return self.families.get(key)

    def documents(self, family_key: str) -> List[Dict[str, Any]]:
        """제품군에 속한 문서(manifest 원본 레코드)."""
        return self.docs_by_family.get(family_key, [])

    def kc_record(self, model: str) -> Optional[Dict[str, Any]]:
        """KC 레코드(모델 대문자 키)."""
        return self.kc.get((model or '').upper())

    def all_model_names(self) -> List[str]:
        """family 키 + members 를 합친 인증자료 모델 사전(유사 후보 제안용)."""
        pool: Set[str] = set(self.family_keys)
        for f in self.families.values():
            pool.update(f.get('members') or [])
        return sorted(pool)


# ---------------------------------------------------------------- 파서

def _parse_request_ledger(path: str) -> Dict[tuple, Dict[str, str]]:
    """자료요청이력.csv — 무엇을 언제 누구에게 요청했는가."""
    out: Dict[tuple, Dict[str, str]] = {}
    for row in read_csv_rows(path):
        model = (row.get('모델') or '').strip().upper()
        dt = (row.get('자료유형') or '').strip()
        if not model or not dt:
            continue
        out[(model, dt)] = {
            'count': (row.get('요청횟수') or '').strip(),
            'first': (row.get('최초요청') or '').strip(),
            'last': (row.get('최근요청') or '').strip(),
            'to': (row.get('수신처') or '').strip(),
            'subject': (row.get('대표 메일제목') or '').strip(),
            'quote': (row.get('근거문구') or '').strip(),
        }
    return out


def _parse_model_matrix(path: str) -> Dict[str, Dict[str, str]]:
    """모델 단위 보유 마크. 별표(추론 매핑)가 제품군보다 세밀하게 붙어 있는 칸이 있다."""
    out: Dict[str, Dict[str, str]] = {}
    for row in read_csv_rows(path):
        model = (row.get('모델명') or '').strip().upper()
        if not model:
            continue
        out[model] = {c: (row.get(c) or '').strip() for c in MATRIX_COLS if row.get(c)}
    return out


def _parse_rohs_history(path: str) -> List[RohsRequest]:
    out: List[RohsRequest] = []
    for row in read_csv_rows(path):
        raw = (row.get('수신일자') or '').strip()
        models = [t.strip() for t in (row.get('요청 모델') or '').split(',') if t.strip()]
        status: Dict[str, str] = {}
        for tok in (row.get('현재 RoHS 보유') or '').split(','):
            if '=' in tok:
                k, _, v = tok.partition('=')
                status[k.strip().upper()] = v.strip().upper()
        out.append(RohsRequest(date_raw=raw, date=to_iso_date(raw),
                               subject=(row.get('메일 제목') or '').strip(),
                               models=models, status=status))
    return out


# ---------------------------------------------------------------- 로딩

_CACHE: Optional[KnowledgeBase] = None


def load_kb(force: bool = False) -> KnowledgeBase:
    """지식베이스를 적재하고 캐시한다. 파일이 없어도 예외 없이 빈 인덱스를 돌려준다."""
    global _CACHE
    if _CACHE is not None and not force:
        return _CACHE

    kb = KnowledgeBase()

    docs = _read_jsonl(DOC_MANIFEST)
    if not docs:
        kb.problems.append(f'문서 manifest 를 읽지 못했습니다: {DOC_MANIFEST}')
    for rec in docs:
        did = rec.get('doc_id')
        if did:
            kb.docs[did] = rec

    fams = _read_jsonl(FAMILY_MANIFEST)
    if not fams:
        kb.problems.append(f'제품군 manifest 를 읽지 못했습니다: {FAMILY_MANIFEST}')
    member_index: Dict[str, Set[str]] = defaultdict(set)
    for rec in fams:
        key = rec.get('family')
        if not key:
            continue
        kb.families[key] = rec
        member_index[key.upper()].add(key)
        for m in rec.get('members') or []:
            member_index[m.upper()].add(key)
    kb.family_keys = sorted(kb.families)
    kb.member_index = {k: v for k, v in member_index.items()}

    # family.documents[] 에는 caveat_ko/deliverable/text_extractable/sources 가 없다.
    # 응답 게이트가 통째로 빠지므로 여기서 doc_id 로 manifest 레코드를 붙여 둔다.
    by_family: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in kb.docs.values():
        for f in rec.get('families') or []:
            by_family[f].append(rec)
    kb.docs_by_family = {k: v for k, v in by_family.items()}

    for rec in _read_jsonl(KC_MANIFEST):
        model = rec.get('model')
        if model:
            kb.kc[model.upper()] = rec
    kb.kc_models = sorted(kb.kc)
    if not kb.kc:
        kb.problems.append(f'KC manifest 를 읽지 못했습니다: {KC_MANIFEST}')

    kb.rohs_requests = _parse_rohs_history(ROHS_HISTORY)
    if not kb.rohs_requests:
        kb.problems.append(f'RoHS 요청이력을 읽지 못했습니다: {ROHS_HISTORY}')

    kb.request_ledger = _parse_request_ledger(REQUEST_LEDGER)
    kb.model_marks = _parse_model_matrix(MODEL_MATRIX)
    if not kb.model_marks:
        kb.problems.append(f'자료보유매트릭스를 읽지 못했습니다: {MODEL_MATRIX}')

    if os.path.exists(DRIVE_FOLDERS):
        try:
            with open(DRIVE_FOLDERS, encoding='utf-8') as fh:
                kb.drive_folders = json.load(fh)
        except (OSError, json.JSONDecodeError):
            kb.problems.append(f'drive_folders.json 을 읽지 못했습니다: {DRIVE_FOLDERS}')

    _CACHE = kb
    return kb


def reset_cache() -> None:
    """테스트용: 다음 load_kb() 가 파일을 다시 읽게 한다."""
    global _CACHE
    _CACHE = None


# ---------------------------------------------------------------- 소소한 파서

def parse_other_types(value: Any) -> List[str]:
    """other_types 는 리스트가 아니라 ', ' 결합 문자열이고 값 없음은 '-' 다."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    v = (value or '').strip()
    if not v or v == '-':
        return []
    return [t.strip() for t in v.split(',') if t.strip()]


def display_members(members: List[str]) -> List[str]:
    """와일드카드/합성 stem 을 뺀, 사람에게 보여도 되는 멤버 목록."""
    return [m for m in (members or []) if not WILDCARD_MEMBER.search(m)]
