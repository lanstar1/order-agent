#!/usr/bin/env python3
"""1단계 상담이력 검색 — 문자 2+3-gram IDF 코사인, **모델 스코프 필수**.

왜 모델 스코프가 하드 요구사항인가(전부 실측)
  · 상담 질문 5,313건 중 82.5%(4,383건)에 모델 식별자가 전혀 없다. 게시판 안에서만
    의미를 갖는 텍스트다("재고있나요?", "길이가 어떻게되나요?").
  · 실제 질문에서 모델코드만 지우고 전체 검색하면 **top-1 이 다른 모델일 확률 78.5%**.
  · '호환되나요?' 류의 답은 모델 종속이라, 모델 무관 검색은 12~24% 확률로 정반대 답을 준다.
  → 그래서 `search_qa(query)` 시그니처를 만들지 않는다. model 없이는 호출 자체가 불가능하다.

왜 문자 n-gram IDF 코사인인가(300쿼리 벤치마크)
  · 띄어쓰기 보존: cgram(2,3) IDF R@1 94.0 / 어절 IDF 98.3 / 문자 자카드 89.3 / difflib 31.0
  · 붙여쓰기(공백 제거): cgram **94.0 불변** / 어절 IDF **0.0 완전 붕괴** / difflib 31.0
  → 한국어 CS 입력에서 붙여쓰기는 흔하다. 어절 단독 금지, difflib 금지(314ms/쿼리·어순 취약).
  → 자카드가 아니라 IDF 가중(같은 비용에 +4.7pp). 길이 정규화(L2) 필수 — 최대 1,273자 질문이 있다.

인덱스는 answer 를 넣지 않는다. 답변의 8.5% 가 다른 모델명을 언급(대체품 안내)해서
엉뚱한 검색 히트를 만든다.
"""
from __future__ import annotations

import json
import math
import re
import threading
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from . import config

# ---------------------------------------------------------------- 정규화

_KEEP_RX = re.compile(r'[^0-9a-z가-힣]+')


def normalize_text(text: str) -> str:
    """공백·기호를 전부 제거한다 — 붙여쓰기/띄어쓰기 차이를 검색에서 없앤다."""
    t = unicodedata.normalize('NFKC', text or '').lower()
    return _KEEP_RX.sub('', t)


def grams(text: str) -> List[str]:
    t = normalize_text(text)
    out: List[str] = []
    for n in config.NGRAM_SIZES:
        if len(t) < n:
            continue
        out.extend(t[i:i + n] for i in range(len(t) - n + 1))
    return out


# ---------------------------------------------------------------- 모델 키 정규화

#: 'LS-AS302N라', 'LS-MV109N입니다' 처럼 한글 조사가 그대로 붙은 키가 28개(30 rows) 있다.
#: 정규화하지 않으면 LS-AS302N 조회 시 상담 3건이 조용히 누락된다.
_KOR_SUFFIX_RX = re.compile(r'^(LS[PN]?-[A-Z0-9][A-Z0-9\-.]*?)[가-힣]')
#: 게시판 아티팩트 접미 — 같은 제품의 다른 글이다.
_ARTIFACT_RX = re.compile(r'-(NOTICE|COUPANG|SMARTSTORE)$', re.IGNORECASE)
#: 'LS-1800HBS_02' 처럼 언더스코어 + 일련번호로 갈라진 게시판 글
_US_SEQ_RX = re.compile(r'-\d{1,3}$')


def _key_variants(raw: str, known_norm: Iterable[str]) -> List[str]:
    """마스터 원본 키 하나를 정규 모델 코드 목록으로 편다.

    known_norm 은 '언더스코어를 하이픈으로 바꾼 대문자 키' 집합이다. 원본 키가 그 자체로
    마스터에 존재해도(예: 'LS-AS302N라') 정규형이 따로 있으면 그쪽으로 병합해야 한다 —
    존재 여부로 조기 종료하면 한글 조사가 붙은 28개 키(30 rows)가 조용히 남는다.
    """
    known_set = set(known_norm)
    base = unicodedata.normalize('NFKC', raw or '').strip().upper()
    had_us = '_' in base
    base = base.replace('_', '-')
    parts = [p.strip() for p in base.split(',') if p.strip()]
    if len(parts) > 1:
        # 'LS-240HM,420HM' — 뒷부분에 접두가 없으면 앞부분의 접두를 물려준다
        head = parts[0]
        pre = head[:head.index('-') + 1] if '-' in head else ''
        parts = [p if p.startswith('LS') else pre + p for p in parts]
    out: List[str] = []
    for p in parts:
        cands: List[str] = []
        m = _KOR_SUFFIX_RX.match(p)
        if m:
            cands.append(m.group(1))
        art = _ARTIFACT_RX.sub('', p)
        if art != p:
            cands.append(art)
        if had_us:
            seq = _US_SEQ_RX.sub('', p)
            if seq != p:
                cands.append(seq)
        cand = next((c for c in cands if c and c != p and c in known_set), p)
        if cand and cand not in out:
            out.append(cand)
    return out or [base]


def normalize_model_key(raw: str) -> str:
    """조회용 정규화(모델 하나짜리). resolver.normalize_model 과 같은 방향이다."""
    q = unicodedata.normalize('NFKC', raw or '').strip().upper().replace('_', '-')
    return q.strip('.-, ')


# ---------------------------------------------------------------- 행 / 히트

@dataclass
class QARow:
    model: str
    question: str                 # PII 마스킹 완료
    answer: str                   # PII 마스킹 완료
    source: str                   # 'qna' | 'talk_MMDD'
    original_product_name: str
    talk: bool                    # 병합된 대화 로그인가
    volatile: bool                # 재고·배송·가격·이벤트 등 시점 의존
    raw_key: str


@dataclass
class Hit:
    row: QARow
    score: float
    level: str                    # 'cite' | 'reference'
    sibling: bool = False
    matched_model: str = ''

    @property
    def quotable(self) -> bool:
        """시점 의존 답변은 점수와 무관하게 원문 인용 금지."""
        return self.level == 'cite' and not self.row.volatile

    def as_dict(self) -> Dict[str, object]:
        return {'model': self.row.model, 'question': self.row.question,
                'answer': self.row.answer, 'score': round(self.score, 3),
                'level': self.level, 'source': self.row.source,
                'talk': self.row.talk, 'volatile': self.row.volatile,
                'quotable': self.quotable, 'sibling': self.sibling,
                'original_product_name': self.row.original_product_name}


# ---------------------------------------------------------------- 인덱스

class HistoryIndex:
    """프로세스당 하나. 빌드 0.6초, 쿼리 2.5ms(실측)."""

    def __init__(self, master: Dict[str, dict]):
        self.rows: List[QARow] = []
        self.by_model: Dict[str, List[int]] = {}
        self.models: List[str] = []
        self.aliases: Dict[str, str] = {}      # original_product_name -> model
        self._df: Dict[str, int] = {}
        self._idf: Dict[str, float] = {}
        self._vec_cache: Dict[int, Dict[str, float]] = {}
        self._alias_vecs: List[Tuple[str, str, Dict[str, float]]] = []
        self.dropped_template = 0
        self._build(master)

    # -- 빌드 -----------------------------------------------------------
    def _build(self, master: Dict[str, dict]) -> None:
        known = {unicodedata.normalize('NFKC', k).strip().upper().replace('_', '-')
                 for k in master}
        df: Dict[str, int] = {}
        self.key_merges: Dict[str, List[str]] = {}
        for raw_key, value in master.items():
            cs = (value or {}).get('고객상담') if isinstance(value, dict) else None
            if not cs:
                continue                      # 714/2039 모델만 상담이력을 갖는다
            items = cs.get('상담내역') or []
            variants = _key_variants(raw_key, known)
            if variants != [raw_key]:
                self.key_merges[raw_key] = variants
            for model in variants:
                for it in items:
                    q = (it.get('question') or '').strip()
                    a = (it.get('answer') or '').strip()
                    if not q:
                        continue
                    if config.TEMPLATE_RX.search(a):
                        self.dropped_template += 1
                        continue              # 정보요청 템플릿 182건 — 가치 0, 유사도만 높다
                    src = it.get('source') or ''
                    opn = (it.get('original_product_name') or '').strip()
                    row = QARow(model=model,
                                question=config.mask_pii(q),
                                answer=config.mask_pii(a),
                                source=src,
                                original_product_name=opn,
                                talk=src.startswith('talk'),
                                volatile=bool(config.VOLATILE_RX.search(q + ' ' + a)),
                                raw_key=raw_key)
                    idx = len(self.rows)
                    self.rows.append(row)
                    self.by_model.setdefault(model, []).append(idx)
                    for g in set(grams(row.question)):
                        df[g] = df.get(g, 0) + 1
                    if opn and not row.talk:
                        # talk 행은 opn 이 20.6% 결측 → 별칭 사전은 qna 기준으로만
                        self.aliases.setdefault(opn, model)
        self.models = sorted(self.by_model)
        n = max(1, len(self.rows))
        self._df = df
        self._idf = {g: math.log(n / c) + 1.0 for g, c in df.items()}
        self._build_alias_vectors()

    def _build_alias_vectors(self) -> None:
        for name, model in self.aliases.items():
            v = self._vector(name)
            if v:
                self._alias_vecs.append((name, model, v))

    # -- 벡터 -----------------------------------------------------------
    def _vector(self, text: str) -> Dict[str, float]:
        tf: Dict[str, float] = {}
        for g in grams(text):
            tf[g] = tf.get(g, 0.0) + 1.0
        if not tf:
            return {}
        vec = {g: c * self._idf.get(g, math.log(max(1, len(self.rows))) + 1.0)
               for g, c in tf.items()}
        norm = math.sqrt(sum(w * w for w in vec.values())) or 1.0
        return {g: w / norm for g, w in vec.items()}

    def _row_vector(self, i: int) -> Dict[str, float]:
        v = self._vec_cache.get(i)
        if v is None:
            v = self._vector(self.rows[i].question)
            if len(self._vec_cache) > 20000:
                self._vec_cache.clear()
            self._vec_cache[i] = v
        return v

    @staticmethod
    def _cos(a: Dict[str, float], b: Dict[str, float]) -> float:
        if len(a) > len(b):
            a, b = b, a
        return sum(w * b.get(g, 0.0) for g, w in a.items())

    # -- 스코프 ---------------------------------------------------------
    def scope(self, model: str, include_siblings: bool = True
              ) -> Tuple[List[int], List[int]]:
        """(정확 일치 행, 형제 모델 행). 형제는 '-' 경계 접두 일치만 인정한다.

        경계를 요구하지 않으면 LS-300H(밀폐 캐비닛)에 LS-300HO(오픈랙)가 붙는다.
        """
        q = normalize_model_key(model)
        exact = list(self.by_model.get(q, []))
        sib: List[int] = []
        if include_siblings and not exact:
            for m, idxs in self.by_model.items():
                if m == q:
                    continue
                if (m.startswith(q + '-') or q.startswith(m + '-')):
                    sib.extend(idxs)
        return exact, sib

    def rows_for(self, model: str, include_siblings: bool = True) -> List[Tuple[QARow, bool]]:
        exact, sib = self.scope(model, include_siblings)
        return ([(self.rows[i], False) for i in exact] +
                [(self.rows[i], True) for i in sib])

    # -- 검색 -----------------------------------------------------------
    def search(self, query: str, model: str, limit: int = config.HISTORY_TOP_K,
               min_score: float = config.SCORE_REF,
               include_siblings: bool = True) -> List[Hit]:
        if not model or not str(model).strip():
            raise ValueError('상담이력 검색은 모델 스코프 없이 호출할 수 없습니다 '
                             '(모델 미확정 시 top-1 의 78.5%가 다른 모델입니다).')
        qv = self._vector(query)
        if not qv:
            return []
        exact, sib = self.scope(model, include_siblings)
        out: List[Hit] = []
        for idxs, is_sib in ((exact, False), (sib, True)):
            for i in idxs:
                row = self.rows[i]
                s = self._cos(qv, self._row_vector(i))
                if row.talk:
                    s *= config.TALK_WEIGHT
                if is_sib:
                    s *= config.SIBLING_WEIGHT
                if s < min_score:
                    continue
                level = 'cite' if s >= config.SCORE_CITE else 'reference'
                out.append(Hit(row=row, score=s, level=level, sibling=is_sib,
                               matched_model=row.model))
        out.sort(key=lambda h: -h.score)
        return out[:limit]

    # -- 별칭(상품명) ---------------------------------------------------
    def guess_models_by_name(self, text: str, limit: int = 3,
                             min_score: float = 0.35) -> List[Tuple[str, float, str]]:
        """'4K 120hz hdmi 선택기 3:1' 같은 상품명으로 모델 후보를 제안한다.

        original_product_name 은 상위 키와 불일치 사례가 0건이라 '검증'에는 쓸 수 없지만
        (키에서 파생된 값), 별칭 사전으로는 유용하다. 한 모델에 별칭이 여러 개인 것도 정상.
        **자동 선택 금지** — 되묻기 후보로만 쓴다.
        """
        qv = self._vector(text)
        if not qv:
            return []
        scored: Dict[str, Tuple[float, str]] = {}
        for name, model, v in self._alias_vecs:
            s = self._cos(qv, v)
            if s < min_score:
                continue
            prev = scored.get(model)
            if prev is None or s > prev[0]:
                scored[model] = (s, name)
        ranked = sorted(scored.items(), key=lambda kv: -kv[1][0])[:limit]
        return [(m, round(s, 3), name) for m, (s, name) in ranked]


# ---------------------------------------------------------------- 모듈 캐시

_INDEX: Optional[HistoryIndex] = None
_LOCK = threading.Lock()


def load_master(path=None) -> Dict[str, dict]:
    with open(path or config.MASTER_PATH, encoding='utf-8') as f:
        return json.load(f)


def get_index(master: Optional[Dict[str, dict]] = None,
              rebuild: bool = False) -> HistoryIndex:
    """프로세스 기동 시 한 번 빌드(0.6초). 별도 디스크 캐시가 필요 없다."""
    global _INDEX
    with _LOCK:
        if _INDEX is None or rebuild:
            _INDEX = HistoryIndex(master if master is not None else load_master())
        return _INDEX


def reset_cache() -> None:
    global _INDEX
    with _LOCK:
        _INDEX = None


# ---------------------------------------------------------------- 공개 API

def search_qa(query: str, model: str, limit: int = config.HISTORY_TOP_K,
              min_score: float = config.SCORE_REF,
              include_siblings: bool = True) -> List[Hit]:
    """모델 스코프 안에서만 유사 상담을 찾는다. model 은 필수 인자다."""
    return get_index().search(query, model, limit=limit, min_score=min_score,
                              include_siblings=include_siblings)


def history_for_model(model: str, include_siblings: bool = True) -> List[Hit]:
    """그 모델의 상담이력 전량.

    714개 보유 모델의 상담건수 중앙값이 2건이라, 대부분의 모델에서는 'top-k 유사질문'
    보다 전량 노출이 실질적으로 낫다(담당자가 직접 판단).
    """
    out = [Hit(row=r, score=0.0, level='reference', sibling=sib, matched_model=r.model)
           for r, sib in get_index().rows_for(model, include_siblings)]
    return out


def model_row_count(model: str) -> int:
    exact, _sib = get_index().scope(model, include_siblings=False)
    return len(exact)


def guess_models_by_name(text: str, limit: int = 3) -> List[Tuple[str, float, str]]:
    return get_index().guess_models_by_name(text, limit=limit)


def clean_answer(text: str, drop_urls: bool = True) -> str:
    """인용 직전 정리 — URL 은 유효성 미검증(외부몰 133건, 게시판 내부링크)이라 뗀다."""
    out = text or ''
    if drop_urls:
        out = config.URL_RX.sub('[링크 생략]', out)
    return re.sub(r'\n{3,}', '\n\n', out).strip()


def talk_excerpt(text: str, query: str, max_lines: int = 4) -> str:
    """talk_* 행은 단일 Q&A 가 아니라 병합된 대화 로그(Q 줄 수 중앙값 3, 최대 59)다.

    원문 그대로 붙이면 앞뒤가 안 맞는 답변이 되므로 질의와 겹치는 줄만 발췌한다.
    """
    lines = [ln.strip() for ln in (text or '').splitlines() if ln.strip()]
    if len(lines) <= max_lines:
        return '\n'.join(lines)
    qg = set(grams(query))
    scored = []
    for i, ln in enumerate(lines):
        lg = set(grams(ln))
        ov = len(qg & lg) / (len(lg) or 1)
        scored.append((ov, -i, i, ln))
    scored.sort(reverse=True)
    picked = sorted(scored[:max_lines], key=lambda t: t[2])
    return '\n'.join(ln for *_ignore, ln in picked)
