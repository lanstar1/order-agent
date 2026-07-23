#!/usr/bin/env python3
"""모델명 정규화와 제품군/KC 인덱스 해석.

정규화 규칙 자체는 제품인증자료/_lib/model_family.py 가 이미 갖고 있다(성별코드 MM/MF 를
길이 변형으로 오인하지 않는 로직이 그 안에 있다). 여기서는 그 모듈을 import 해서 쓰고,
호출측이 반드시 알아야 하는 세 가지만 덧붙인다.

  * resolve() 는 'stem_ambiguous' 일 때 None 이 아니라 임의의 후보 하나를 돌려준다.
    게다가 그 하나는 set 순회 순서 탓에 프로세스마다 바뀐다 — 그래서 후보를 다시 계산해
    (길이, 이름) 으로 정렬해 돌려주고, 확정 답변 대신 되묻기로 강등한다.
  * family 키끼리 접두 충돌이 122쌍이라 exact 결과 하나만 믿으면 문서를 놓친다
    (LS-HDMT-10M 은 자체 family 이면서 LS-HDMT 의 member). member 역인덱스와 합집합한다.
  * resolve() 는 하이픈/언더스코어 누락을 보정하지 않는다. 앞단에서 정규화한다.
"""
from __future__ import annotations

import difflib
import os
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set

from . import loader

# 정규화 모듈은 저장소의 _lib 를 그대로 쓴다(복사본을 만들면 규칙이 갈라진다).
# CERT_KB_REPO 가 잘못 지정된 경우를 대비해 패키지 옆 경로도 후보로 둔다.
_LIB_CANDIDATES = [loader.LIB_DIR,
                   os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                '제품인증자료', '_lib')]


def _load_model_family():
    """model_family.py 를 파일 경로로 직접 적재한다.

    sys.path 에 넣지 않는 이유: 그 경로는 코드 디렉터리가 아니라 **데이터 번들**이고,
    한 번 넣으면 프로세스 전역에 영구히 남는다. order-agent 처럼 ERP·주문·배송이 같은
    프로세스에서 도는 앱에 이식되면, 나중에 그 폴더에 config.py·json.py 같은 파일이
    하나만 떨어져도 앱 전체 import 가 조용히 가로채인다.
    """
    import importlib.util
    for d in _LIB_CANDIDATES:
        path = os.path.join(d, 'model_family.py')
        if not os.path.isfile(path):
            continue
        spec = importlib.util.spec_from_file_location('_cert_model_family', path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    return None


_mf = _load_model_family()
if _mf is None:                                 # 여기서 실패하면 조회 자체가 불가능하다
    raise ImportError(
        'model_family.py 를 찾지 못했습니다. 저장소의 제품인증자료/_lib 경로를 '
        f'CERT_KB_LIB_DIR 환경변수로 지정하십시오. (탐색 경로: {_LIB_CANDIDATES})')
GENDER = _mf.GENDER
mf_candidate = _mf.candidate
mf_resolve = _mf.resolve

#: 문장에서 모델 코드를 뽑는다. LSN- 접두와 비LS 모델(DP12MM-, OM4-, USB2.0-)까지 포함하고,
#: 후행 마침표/하이픈을 삼키지 않도록 마지막 글자를 영숫자로 고정했다.
MODEL_RX = re.compile(
    r'\b(LS[PN]?-[A-Za-z0-9][A-Za-z0-9.\-]*[A-Za-z0-9]'
    r'|LS[PN]?-[A-Za-z0-9]'
    r'|DP\d+MM-\d+M'
    r'|OM[34]-[A-Z]{2,6}(?:-[A-Za-z0-9.]+)?'
    r'|USB\d(?:\.\d)?-[A-Za-z0-9.\-]+)', re.IGNORECASE)

_PREFIX_FIX = re.compile(r'^(LSP|LSN|LS)([0-9A-Z].*)$')

#: 메일·엑셀에서 복사한 모델명에는 자동 서식이 넣은 유니코드 하이픈류가 섞인다.
#: 이것을 ASCII 하이픈으로 되돌리지 않으면 자료가 있는 모델을 '미보유'로 답하게 된다.
_DASHES = dict.fromkeys(map(ord, '‐‑‒–—―−﹘﹣－'), '-')

#: 확정 답변에 써도 되는 경로. 'prefix' 는 절대 포함하지 않는다 — q.startswith(key) 는
#: 오타든 별개 제품이든 조용히 삼킨다(LS-300HO 오픈랙이 밀폐 캐비닛 LS-300H 에 붙는다).
#: 'suffix'·'stem' 도 무조건 확정은 아니다. 아래 _suffix_is_verified() 와 _STEM_GENERIC 이
#: 데이터로 뒷받침되는 경우만 남기고 나머지를 추정 매칭으로 낮춘다.
#: 'notation' 은 공급사가 적용범위를 직접 적은 표기(1M~5M, 7XX, SERIES)라 확정으로 본다.
CONFIDENT_REASONS = ('exact', 'suffix', 'member', 'stem', 'notation')

#: KC 적합등록은 모델 단위 법적 등록이다. 앞부분이 같다고 남의 등록을 보유했다고 답할 수 없다.
KC_CONFIDENT_REASONS = ('exact', 'suffix')


def normalize_model(raw: str) -> str:
    """사용자 입력을 조회 키로 다듬는다(전각·유니코드 하이픈·대소문자·구분자·접두 하이픈 누락)."""
    q = unicodedata.normalize('NFKC', raw or '')
    q = q.translate(_DASHES)
    q = q.upper().strip()
    q = q.strip('.,;:()[]{}"\'')
    q = q.replace('_', '-')
    q = re.sub(r'\s+', ' ', q).strip()
    q = q.strip('.-_ ')
    if '-' not in q:
        m = _PREFIX_FIX.match(q)
        if m:
            q = f'{m.group(1)}-{m.group(2)}'
    return q


def extract_models(text: str) -> List[str]:
    """자유 문장에서 모델 코드 후보를 순서대로 뽑는다(중복 제거)."""
    seen: List[str] = []
    # 정규식을 돌리기 전에 문장 전체를 먼저 되돌린다. 전각 'ＬＳ－' 는 MODEL_RX 에 안 걸린다.
    text = unicodedata.normalize('NFKC', text or '').translate(_DASHES)
    for m in MODEL_RX.finditer(text):
        q = normalize_model(m.group(1))
        if q and q not in seen:
            seen.append(q)
    return seen


@dataclass
class Resolution:
    """모델 해석 결과. ambiguous 면 답하지 말고 candidates 로 되물어야 한다."""
    query: str
    normalized: str
    index: str                                   # 'cert' | 'kc'
    key: Optional[str] = None                    # 대표 제품군/모델 키
    keys: List[str] = field(default_factory=list)  # 접두 충돌 보정 포함 전체
    reason: str = 'none'                         # exact/suffix/prefix/stem/stem_ambiguous/none
    ambiguous: bool = False
    confident: bool = False                      # 확정 답변에 써도 되는가
    candidates: List[str] = field(default_factory=list)   # 되묻기용 후보
    suggestions: List[str] = field(default_factory=list)  # none 일 때 유사 모델

    @property
    def found(self) -> bool:
        """확정이든 추정이든 붙일 자료가 있는가."""
        return bool(self.key) and not self.ambiguous


#: stem 매칭에서 이 잔여만 같은 제품의 표기 차이로 본다(LS-LKPG -> 'LS-LKPG SERIES').
#: 그 밖의 잔여는 사양 토큰일 수 있다 — LS-USB-AM -> LS-USB-AM5P(미니5핀)는 AMAF/AMAM/AMBM 과
#: 커넥터 조합이 전혀 다른 별개 제품인데, stem 필터가 뒤 글자를 배제해 '유일 후보'를 만들어낸다.
_STEM_GENERIC = frozenset({'SERIES', 'SERIE', 'S'})

_WILDCARD_LEN = re.compile(r'^(?:X{1,2}|\*)$')


def _family_variants(keys: Sequence[str], kb: loader.KnowledgeBase):
    """제품군 members 에서 실제로 관측된 (색상코드, 길이, 와일드카드 유무)."""
    cols, lens, wild = set(), set(), False
    for k in keys:
        for m in ((kb.family(k) or {}).get('members') or []):
            c = mf_candidate(m)
            if not c:
                continue
            _base, length, col = c
            if _WILDCARD_LEN.match(length):
                wild = True
            else:
                lens.add(length)
            cols.add(col)
    return cols, lens, wild


def _suffix_is_verified(q: str, keys: Sequence[str], kb: loader.KnowledgeBase) -> bool:
    """길이/색상 접미사가 데이터로 뒷받침되는 변형인가.

    접미사 파싱은 '숫자+M+영문 0~3글자'면 무엇이든 색상으로 본다. 그래서 색상이 아닌 코드가
    조용히 병합된다 — LS-5UTP-100MRE 의 'RE'(커넥터 없는 원장)와 LS-6UTP-300MGN 의 'GN'이
    그 예로, 둘 다 형제 SKU 와 제품 형태가 다르다. members 에 실제로 나타난 색상·길이만
    확정으로 인정하고 나머지는 추정 매칭으로 낮춘다(자료는 그대로 주되 확인을 요구한다).
    """
    c = mf_candidate(q)
    if not c:
        return True
    _base, length, col = c
    cols, lens, wild = _family_variants(keys, kb)
    if col and col not in cols:
        return False
    if not wild and lens and length not in lens:
        return False
    return True


#: 'LS-7SD-BKXM' 처럼 길이 자리에 와일드카드를 쓴 제품군 키. model_family.candidate() 는
#: 길이를 숫자로만 읽어서 이 키를 해석하지 못한다(_lib 는 그대로 두고 여기서 보완한다).
#: 그 결과 같은 제품이 'LS-7SD-BK'(문서 1건)와 'LS-7SD-BKXM'(9건)으로 갈라진 채,
#: 길이를 붙여 조회하면 1건짜리 쪽만 나오고 나머지를 '미보유'라고 단언한다.
_WILD_KEY = re.compile(r'^(?P<base>.*[A-Z])(?:X{1,2}|\*)M(?P<col>[A-Z]{0,3})$')

_DIGITS = re.compile(r'\d+')


# ---------------------------------------------------------------- 공급사 표기법
#
# 제품군 키에는 공급사가 메일에 쓴 표기가 그대로 들어와 있다. 실판매 모델명으로는 도달할 수
# 없어서, 자료가 있는데도 '미보유'라고 답하게 된다(LS-HF7 시리즈 인증 12건이 LS-HF7XX 키에만
# 매달려 있던 것이 그 예다). 파이프라인 중간 산출물이 남아 있지 않아 manifest 를 재생성할 수
# 없으므로, 조회 시점에 표기를 해석한다. 데이터·_lib 는 건드리지 않는다.

#: 범위 표기: LS-HDMI-EXT-10~15M / LS-HDMT-0.3M~5M / LS-HDMI-2MM-10-15MC
_RANGE_KEY = re.compile(
    r'^(?P<base>.+?)[-_ ](?P<lo>\d+(?:\.\d+)?)\s*M?\s*[~-]\s*(?P<hi>\d+(?:\.\d+)?)\s*M'
    r'(?P<col>[A-Z]{0,3})$')
#: 자릿수 와일드카드: LS-HF7XX / LS-EXT3XX / LS-PRT-25C-XX
_DIGIT_WILD_KEY = re.compile(r'^(?P<base>.*?[A-Z0-9])(?P<x>X{2,})$')
#: 시리즈 표기: 'LS-HF SERIES'
_SERIES_KEY = re.compile(r'^(?P<base>.+?)\s+SERIES$')


def _notation_patterns(kb: loader.KnowledgeBase) -> List[tuple]:
    """제품군 키의 공급사 표기를 실모델명 매칭 규칙으로 바꾼다. [(판정함수, 키)]"""
    out: List[tuple] = []
    for f in kb.family_keys:
        k = f.upper()

        if '/' in k:                                  # 'LS-FMD / LS-FMP'
            names = {p.strip() for p in k.split('/') if p.strip()}
            out.append((lambda q, n=names: q in n, f))
            continue

        if ',' in k:                                  # 'LS-HD21-1M,1.5M,2M,3M'
            parts = [p.strip() for p in k.split(',') if p.strip()]
            head = parts[0]
            stem = head.rsplit('-', 1)[0] if '-' in head else head
            names = {head} | {f'{stem}-{p}' for p in parts[1:]}
            out.append((lambda q, n=names: q in n, f))
            continue

        m = _RANGE_KEY.match(k)
        if m:
            base, lo, hi = m.group('base'), float(m.group('lo')), float(m.group('hi'))
            col = m.group('col')

            def in_range(q, base=base, lo=lo, hi=hi, col=col):
                c = mf_candidate(q)
                if not c or c[0] != base or c[2] != col:
                    return False
                try:
                    return lo <= float(c[1]) <= hi
                except ValueError:
                    return False
            out.append((in_range, f))
            continue

        m = _SERIES_KEY.match(k)
        if m:
            base = m.group('base')
            out.append((lambda q, b=base: q == b or q.startswith(b + '-') or
                        (q.startswith(b) and len(q) > len(b) and q[len(b)].isdigit()), f))
            continue

        m = _DIGIT_WILD_KEY.match(k)
        if m and not _WILD_KEY.match(k):               # 길이 와일드카드는 아래에서 따로 처리
            rx = re.compile('^' + re.escape(m.group('base')) + r'\d{1,%d}$' % len(m.group('x')))
            out.append((lambda q, rx=rx: bool(rx.match(q)), f))
    return out


#: 인덱스는 제품군 키에서만 파생되므로 KB 인스턴스당 한 번만 만든다.
_ALIAS_CACHE: Dict[int, tuple] = {}


def _alias_indexes(kb: loader.KnowledgeBase):
    hit = _ALIAS_CACHE.get(id(kb))
    if hit is None:
        hit = (_wildcard_index(kb), _dot_index(kb), _notation_patterns(kb))
        _ALIAS_CACHE[id(kb)] = hit
    return hit


def _wildcard_index(kb: loader.KnowledgeBase) -> Dict[tuple, List[str]]:
    idx: Dict[tuple, List[str]] = {}
    for f in kb.family_keys:
        m = _WILD_KEY.match(f.upper())
        if m:
            idx.setdefault((m.group('base').rstrip('-_ '), m.group('col')), []).append(f)
    return idx


def _dot_index(kb: loader.KnowledgeBase) -> Dict[str, List[str]]:
    """점 표기만 다른 중복 키(LS-SSTP-CAT.6A-300M / LS-SSTP-CAT6A-300M)를 잇는다."""
    idx: Dict[str, List[str]] = {}
    for f in kb.family_keys:
        idx.setdefault(f.upper().replace('.', ''), []).append(f)
    return {k: v for k, v in idx.items() if len(v) > 1}


def _skeleton(name: str) -> str:
    """숫자를 지운 이름. 랙처럼 크기만 다른 같은 라인은 같은 스켈레톤을 갖는다."""
    return _DIGITS.sub('#', (name or '').upper())


def _is_separate_line(q: str, key: str, kb: loader.KnowledgeBase) -> bool:
    """질의가 매칭된 키와 다른 제품 라인인가 — KB 자신의 명명 규칙으로 판정한다.

    LS-300HO(오픈랙)는 prefix 로 LS-300H(밀폐 캐비닛)에 붙는다. 그러나 KB 에는
    LS-1600HO/1800HO/2000HO 가 각각 독립 제품군으로 있고 문서가 '2단(2-post) 오픈랙'이라
    적고 있다. 즉 꼬리 O 는 색상이 아니라 라인 구분자다. 크기만 다른 같은 라인이
    이미 제품군으로 존재하면, 그 라인의 자료가 없는 것이지 옆 라인 자료를 줄 일이 아니다.
    """
    sq = _skeleton(q)
    if sq == _skeleton(key):
        return False
    return any(_skeleton(f) == sq for f in kb.family_keys)


def _stem_candidates(q: str, keys: Sequence[str]) -> List[str]:
    """model_family.resolve() 의 stem 분기와 같은 조건으로 후보를 재현한다(결정적 정렬)."""
    if len(q) < 6:
        return []
    out = [f for f in keys if f.startswith(q)
           and (len(f) == len(q) or not f[len(q)].isalnum() or f[len(q)].isdigit())]
    return sorted(out, key=lambda f: (len(f), f))


def _suggest(q: str, pool: Sequence[str], n: int = 5) -> List[str]:
    return difflib.get_close_matches(q, list(pool), n=n, cutoff=0.6)


#: 키 자체가 '숫자+성별코드'로 끝나는가 (LS-SER-9MF, LS-RGB-15MM). 이때만 뒤에 붙는
#: 성별 글자가 진짜 성별 차이를 뜻한다. LS-6STPD 처럼 그렇지 않은 키 뒤의 M 은 길이(meter)다.
_GENDER_KEY = re.compile(r'\d(?:' + '|'.join(sorted(GENDER, key=len, reverse=True)) + r')$')


def _gender_conflict(q: str, key: str) -> bool:
    """prefix 로 붙은 잔여 문자가 커넥터 성별 코드인지.

    model_family.candidate() 는 GENDER 집합으로 성별 병합을 막지만 resolve() 의 prefix 분기에는
    그 가드가 없다. LS-SER-9MFF(9핀 M-F-F)가 LS-SER-9MF(9핀 M-F)에 붙으면 성별이 다른 제품의
    인증서를 주게 된다 — 명세서가 '절대 병합하면 안 되는 것'으로 못박은 항목이다.

    단 키가 성별로 끝나지 않으면 뒤의 M 은 길이 자리표시자다. KB 자신이 members 에
    'LS-6STPD-*M' 처럼 쓰고 있으므로 이것까지 막으면 있는 자료를 못 주게 된다.
    """
    tail = q[len(key):].lstrip('-_ ')
    return tail in GENDER and bool(_GENDER_KEY.search(key))


def resolve_cert(query: str, kb: Optional[loader.KnowledgeBase] = None) -> Resolution:
    """인증자료 제품군 인덱스에서 모델을 해석한다."""
    kb = kb or loader.load_kb()
    q = normalize_model(query)
    res = Resolution(query=query, normalized=q, index='cert')
    if not q or not kb.family_keys:
        res.suggestions = []
        return res

    keys = kb.family_keys
    fam, reason = mf_resolve(q, keys)

    # 성별이 다르거나 아예 다른 제품 라인이면 매칭이 아니라 오답이다.
    # 자료를 붙이지 않고, 같은 라인의 실재 모델을 제안한다.
    if fam and reason == 'prefix' and (_gender_conflict(q, fam) or _is_separate_line(q, fam, kb)):
        res.reason = 'none'
        sq = _skeleton(q)
        same_line = sorted(f for f in kb.family_keys if _skeleton(f) == sq)
        res.suggestions = same_line[:5] or _suggest(q, kb.all_model_names())
        return res

    if reason == 'stem_ambiguous':
        res.reason = reason
        res.ambiguous = True
        res.candidates = _stem_candidates(q, keys)
        return res

    hits: Set[str] = set()
    if fam:
        hits.add(fam)
    # LS-HDMT-10M 처럼 family 키이면서 다른 family 의 member 인 모델을 놓치지 않기 위해
    hits |= kb.member_index.get(q, set())

    # 같은 제품이 길이 표기 때문에 두 키로 갈라져 있으면 함께 본다.
    wild_idx, dot_idx, notations = _alias_indexes(kb)
    c0 = mf_candidate(q)
    if c0:
        hits.update(wild_idx.get((c0[0], c0[2]), []))
    hits.update(dot_idx.get(q.replace('.', ''), []))
    # 공급사 표기(범위·와일드카드·SERIES·열거)로만 도달 가능한 제품군
    notation_hits = {f for match, f in notations if match(q)}
    if notation_hits:
        hits |= notation_hits
        if not fam:
            reason = 'notation'

    if not hits:
        c = mf_candidate(q)
        if c:                       # 길이 접미사를 떼면 member 로 등록돼 있을 수 있다
            hits |= kb.member_index.get(c[0], set())
            if hits:
                reason = 'suffix'
    if not hits:
        res.reason = 'none'
        res.suggestions = _suggest(q, kb.all_model_names())
        return res

    if not fam and hits:
        reason = 'member'
    res.reason = reason
    res.keys = sorted(hits, key=lambda k: (-len(kb.documents(k)), k))
    res.key = res.keys[0]
    res.confident = reason in CONFIDENT_REASONS
    if reason == 'suffix' and not _suffix_is_verified(q, res.keys, kb):
        res.confident = False
    elif reason == 'stem' and res.key[len(q):].strip('-_ ').upper() not in _STEM_GENERIC:
        # stem 은 질의가 키보다 짧다. 키의 잔여가 사양 토큰이면 별개 제품일 수 있다
        # (LS-SER-9F -> LS-SER-9F25M 은 9핀 F-F 가 아니라 9핀F-25핀M).
        # 잔여가 사양 토큰이면 별개 제품일 수 있다(LS-SER-9F -> LS-SER-9F25M).
        res.confident = False
    if not res.confident:
        # 자료는 붙여 주되 확정으로 답하지 않는다. render 가 추정 매칭 문구를 강제한다.
        res.candidates = res.keys
    return res


def resolve_kc(query: str, kb: Optional[loader.KnowledgeBase] = None) -> Resolution:
    """KC 인덱스에서 모델을 해석한다.

    KC 적합등록은 모델 단위 법적 등록이라 prefix/stem 추정을 확정으로 답하면 안 된다.
    exact/suffix 만 confident=True 로 표시한다.
    """
    kb = kb or loader.load_kb()
    q = normalize_model(query)
    res = Resolution(query=query, normalized=q, index='kc')
    if not q or not kb.kc_models:
        return res

    if q in kb.kc:
        res.key, res.keys, res.reason, res.confident = q, [q], 'exact', True
        return res

    model, reason = mf_resolve(q, kb.kc_models)
    if reason == 'stem_ambiguous':
        res.reason = reason
        res.ambiguous = True
        res.candidates = _stem_candidates(q, kb.kc_models)
        return res
    if not model:
        res.reason = 'none'
        res.suggestions = _suggest(q, kb.kc_models, n=3)
        return res

    res.key, res.keys, res.reason = model, [model], reason
    res.confident = reason in KC_CONFIDENT_REASONS
    if not res.confident:
        res.candidates = [model]
    return res


def resolve_all(query: str, kb: Optional[loader.KnowledgeBase] = None) -> Dict[str, Resolution]:
    """두 인덱스는 교집합이 4건뿐인 사실상 별개 집합이라 항상 함께 조회한다."""
    kb = kb or loader.load_kb()
    return {'cert': resolve_cert(query, kb), 'kc': resolve_kc(query, kb)}
