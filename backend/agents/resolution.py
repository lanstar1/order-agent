"""
상품 매칭(Resolution) 에이전트
추출된 상품 힌트를 실제 PROD_CD와 매칭

매칭 파이프라인:
  0단계: 글로벌 별칭 DB (거래처 무관, 과거 성공 매핑 재활용)
  0.5단계: 거래처별 학습 데이터 (few-shot item_map)
  1단계: 완전일치 (LS- 접두어 자동 포함)
  2단계: 스펙 분리 매칭 (카테고리/스펙 필터 → 키워드 검색)
  3단계: 다중 전략 키워드 검색 (부분문자열 + 퍼지 + 토큰)
  4단계: LLM Judge (상위 후보 중 최적 선택)
"""
import logging
import re
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import sys, json
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL, TOP_K_RESULTS, CONFIDENCE_THRESHOLD
from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)
client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)


# ─────────────────────────────────────────
#  간단한 In-Memory 상품 DB (CSV 로드)
# ─────────────────────────────────────────
import csv
from config import PRODUCTS_CSV

_product_cache: List[dict] = []


def load_products() -> List[dict]:
    global _product_cache
    if _product_cache:
        return _product_cache

    if not PRODUCTS_CSV.exists():
        logger.warning(f"[Resolution] 상품 파일 없음: {PRODUCTS_CSV}")
        return []

    with open(PRODUCTS_CSV, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        _product_cache = list(reader)
    logger.info(f"[Resolution] 상품 {len(_product_cache)}건 로드")
    return _product_cache


def reload_products():
    """상품 파일 변경 시 캐시 갱신"""
    global _product_cache
    _product_cache = []
    return load_products()


# ═════════════════════════════════════════
#  글로벌 별칭 DB (Cross-Customer Alias)
#  한번 매칭 성공한 표현→품목코드를 거래처 무관 재활용
# ═════════════════════════════════════════
from db.database import get_connection

def _ensure_alias_table():
    """글로벌 별칭 테이블 생성"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS product_aliases (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            alias_key   TEXT NOT NULL,
            prod_cd     TEXT NOT NULL,
            prod_name   TEXT DEFAULT '',
            model_name  TEXT DEFAULT '',
            hit_count   INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            updated_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alias_key_prodcd
            ON product_aliases(alias_key, prod_cd);
        CREATE INDEX IF NOT EXISTS idx_alias_key
            ON product_aliases(alias_key);
    """)
    conn.close()

_ensure_alias_table()


def save_alias(alias_text: str, prod_cd: str, prod_name: str = "", model_name: str = ""):
    """매칭 성공 시 별칭 저장 (거래처 무관)"""
    key = alias_text.strip().lower()
    if not key or not prod_cd:
        return
    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT id, hit_count FROM product_aliases WHERE alias_key = ? AND prod_cd = ?",
            (key, prod_cd)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE product_aliases SET hit_count = hit_count + 1, updated_at = datetime('now','localtime') WHERE id = ?",
                (existing["id"],)
            )
        else:
            conn.execute(
                "INSERT INTO product_aliases(alias_key, prod_cd, prod_name, model_name) VALUES(?,?,?,?)",
                (key, prod_cd, prod_name, model_name)
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"[Alias] 저장 실패: {e}")
    finally:
        conn.close()


def lookup_alias(hint: str) -> Optional[dict]:
    """별칭 DB에서 과거 매칭 검색 (hit_count 높은 순)"""
    key = hint.strip().lower()
    if not key:
        return None
    conn = get_connection()
    # 정확 매칭
    row = conn.execute(
        "SELECT prod_cd, prod_name, model_name, hit_count FROM product_aliases WHERE alias_key = ? ORDER BY hit_count DESC LIMIT 1",
        (key,)
    ).fetchone()
    if row:
        conn.close()
        return dict(row)

    # 정규화 후 완전 일치만 허용 (부분 매칭 금지)
    # "exodd"가 "exodd-n", "exoddc" 등 다른 모델에 오매칭되는 문제 방지
    rows = conn.execute(
        "SELECT alias_key, prod_cd, prod_name, model_name, hit_count FROM product_aliases ORDER BY hit_count DESC LIMIT 500"
    ).fetchall()
    conn.close()

    key_norm = re.sub(r'[\s\-_.]', '', key)
    for r in rows:
        ak = r["alias_key"]
        if len(ak) < 3:
            continue
        ak_norm = re.sub(r'[\s\-_.]', '', ak)
        # 정규화 후 완전 일치 (하이픈/공백/점 무시)
        if ak_norm == key_norm:
            return dict(r)
    return None


def get_global_alias_map() -> Dict[str, dict]:
    """전체 별칭 맵 조회 (부팅 시 캐시용)"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT alias_key, prod_cd, prod_name, model_name, hit_count FROM product_aliases ORDER BY hit_count DESC"
    ).fetchall()
    conn.close()
    return {r["alias_key"]: dict(r) for r in rows}


# ═════════════════════════════════════════
#  스펙 분리 엔진 (Spec Parser)
#  "CAT.6 UTP 2M 블루" → {category, type, length, color}
# ═════════════════════════════════════════
# 알려진 제조사 키워드
KNOWN_MANUFACTURERS = {
    "lanstar": ["lanstar", "랜스타", "lan star"],
    "coms": ["coms", "컴스"],
    "netmate": ["netmate", "넷메이트"],
    "현대일렉트릭": ["현대일렉트릭", "현대전기", "hyundai"],
    "asrock": ["asrock"],
    "gigabyte": ["gigabyte", "기가바이트"],
}

# 상품 카테고리 패턴 (prod_cd 접두어 기반)
CATEGORY_PREFIXES = {
    "CAB-U6": "CAT.6 UTP 케이블",
    "CAB-U5": "CAT.5 UTP 케이블",
    "CAB-U7": "CAT.7 케이블",
    "CAB-SF": "STP/FTP 케이블",
    "CAB-HD": "HDMI 케이블",
    "CAB-DP": "DP 케이블",
    "CAB-US": "USB 케이블",
    "CAB-APP": "변환/어댑터",
    "COM-외장형": "외장형 ODD/HDD",
    "COM-허브": "허브/스위치",
    "A-멀티탭": "멀티탭",
}

# 색상 키워드
COLOR_MAP = {
    "블루": ["blue", "블루", "b", "mb"],
    "그레이": ["gray", "grey", "그레이", "g", "mg"],
    "화이트": ["white", "화이트", "w", "mw"],
    "블랙": ["black", "블랙", "bk"],
    "레드": ["red", "레드", "r"],
    "옐로우": ["yellow", "옐로우", "y"],
    "오렌지": ["orange", "오렌지"],
    "그린": ["green", "그린"],
}

# 길이 패턴
LENGTH_PATTERN = re.compile(r'(\d+(?:\.\d+)?)\s*[mM미](?:[터])?', re.IGNORECASE)

# 카테고리 키워드
CATEGORY_KEYWORDS = {
    "cat6": ["cat.6", "cat6", "카테고리6", "캣6"],
    "cat5": ["cat.5", "cat5", "cat5e", "카테고리5"],
    "cat7": ["cat.7", "cat7", "카테고리7"],
    "utp": ["utp"],
    "stp": ["stp", "ftp", "sftp"],
    "hdmi": ["hdmi", "에이치디엠아이"],
    "dp": ["dp", "displayport", "디스플레이포트"],
    "usb": ["usb", "유에스비"],
    "랜케이블": ["랜케이블", "랜선", "lan cable", "lan케이블", "랜케"],
    "멀티탭": ["멀티탭", "멀탭", "전원탭"],
    "odd": ["odd", "dvd", "cd-rom", "외장odd"],
    "허브": ["허브", "hub", "스위치허브", "스위칭허브"],
    "젠더": ["젠더", "gender", "변환젠더", "컨버터", "어댑터"],
}

# 약어 사전 (업계 용어)
ABBREVIATION_DICT = {
    "랜케": "랜케이블",
    "멀탭": "멀티탭",
    "허브": "스위칭허브",
    "젠더": "변환젠더",
    "패치": "패치코드",
    "서버랙": "서버 랙",
    "콘솔": "KVM 콘솔",
}


def parse_specs(hint: str) -> dict:
    """
    힌트 텍스트에서 스펙 정보 분리 추출
    Returns: {
        "manufacturer": "lanstar" | None,
        "category": "cat6" | None,
        "cable_type": "utp" | None,
        "length": "2M" | None,
        "color": "블루" | None,
        "raw_model": "6utpd-2mg" (모델번호 패턴),
        "remaining": "기타 텍스트",
    }
    """
    h = hint.strip()
    h_lower = h.lower()
    specs = {
        "manufacturer": None,
        "category": None,
        "cable_type": None,
        "length": None,
        "color": None,
        "raw_model": None,
        "remaining": h,
    }

    # 제조사 감지
    for mfr, keywords in KNOWN_MANUFACTURERS.items():
        for kw in keywords:
            if kw in h_lower:
                specs["manufacturer"] = mfr
                break
        if specs["manufacturer"]:
            break

    # 카테고리 감지
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in h_lower:
                specs["category"] = cat
                break
        if specs["category"]:
            break

    # 케이블 타입
    if "utp" in h_lower:
        specs["cable_type"] = "utp"
    elif any(x in h_lower for x in ["stp", "ftp", "sftp"]):
        specs["cable_type"] = "stp"

    # 길이 추출
    length_match = LENGTH_PATTERN.search(h)
    if length_match:
        specs["length"] = length_match.group(1) + "M"

    # 색상 감지 (단독 단어로만 매칭, 모델번호 내부 문자열 제외)
    h_tokens_for_color = set(re.split(r'[\s,./;:()\[\]]+', h_lower))
    for color_name, keywords in COLOR_MAP.items():
        for kw in keywords:
            if len(kw) <= 2:
                # 짧은 키워드(b, g, mb 등)는 단독 토큰일 때만
                if kw in h_tokens_for_color:
                    specs["color"] = color_name
                    break
            else:
                if kw in h_lower:
                    specs["color"] = color_name
                    break
        if specs["color"]:
            break

    # 모델번호 패턴 추출 (영문+숫자+하이픈 조합)
    model_match = re.search(r'[a-zA-Z]{1,4}[\-]?[a-zA-Z0-9]{2,}[\-]?[a-zA-Z0-9]*', h)
    if model_match:
        specs["raw_model"] = model_match.group(0)

    return specs


def filter_by_specs(products: List[dict], specs: dict) -> List[dict]:
    """
    파싱된 스펙으로 상품 목록 사전 필터링
    제조사가 감지되지 않으면 LANstar 상품 우선
    """
    filtered = []
    manufacturer = specs.get("manufacturer")
    category = specs.get("category")
    length = specs.get("length")
    color = specs.get("color")

    for p in products:
        pn = p.get("prod_name", "").lower()
        model = p.get("model", "").lower()
        kw = p.get("keywords", "").lower()
        pc = p.get("prod_cd", "").lower()
        text = f"{pn} {model} {kw} {pc}"

        score = 0

        # 제조사 필터 (감지 안되면 LANstar 우선)
        if manufacturer:
            mfr_keywords = KNOWN_MANUFACTURERS.get(manufacturer, [])
            if any(mk in text for mk in mfr_keywords):
                score += 3
            else:
                continue  # 다른 제조사 제외
        else:
            # 제조사 미지정 → LANstar 제품 가산점
            if any(mk in text for mk in ["lanstar", "랜스타"]):
                score += 2

        # 카테고리 필터
        if category:
            cat_keywords = CATEGORY_KEYWORDS.get(category, [])
            if any(ck in text for ck in cat_keywords):
                score += 2
            else:
                score -= 1

        # 길이 필터
        if length:
            if length.lower() in text or length.lower().replace("m", "") + "m" in text:
                score += 1

        # 색상 필터
        if color:
            color_keywords = COLOR_MAP.get(color, [])
            if any(ck in text for ck in color_keywords if len(ck) > 1):
                score += 1

        if score > 0:
            filtered.append((score, p))

    filtered.sort(key=lambda x: -x[0])
    return [p for _, p in filtered]


# ═════════════════════════════════════════
#  유틸: 모델명 정규화 / 토큰화
# ═════════════════════════════════════════
def _normalize(text: str) -> str:
    """소문자 변환 + 특수문자를 공백으로"""
    return re.sub(r'[^a-z0-9가-힣]', ' ', text.lower()).strip()


def _tokenize(text: str) -> List[str]:
    """하이픈/공백/특수문자 기준으로 토큰 분리"""
    return [t for t in re.split(r'[\s\-_/.,;:()]+', text.lower()) if t]


def _expand_hint(hint: str) -> List[str]:
    """
    힌트에서 검색 변형을 생성:
    - 원본
    - LS- 접두어 추가 버전
    - 숫자+알파벳 끝자리 변형 (2mg↔2mb 등)
    - 약어 확장
    """
    variants = [hint.strip()]
    h = hint.strip()

    # 약어 확장
    h_lower = h.lower()
    for abbr, full in ABBREVIATION_DICT.items():
        if abbr in h_lower:
            expanded = h_lower.replace(abbr, full)
            variants.append(expanded)

    # LS- 접두어 자동 추가
    if not h.upper().startswith("LS-") and not h.upper().startswith("LS "):
        variants.append(f"LS-{h}")

    # 끝자리 알파벳 변형 (b↔g, m↔n 등 오타/유사 패턴)
    similar_chars = {'b': ['g', 'p', 'd'], 'g': ['b', 'q'], 'm': ['n', 'rn'],
                     'n': ['m'], 'p': ['b', 'd'], 'd': ['b', 'p'],
                     '0': ['o'], 'o': ['0'], '1': ['l', 'i'], 'l': ['1', 'i']}
    if len(h_lower) > 2 and h_lower[-1] in similar_chars:
        for alt in similar_chars[h_lower[-1]]:
            variants.append(h_lower[:-1] + alt)
            if not h.upper().startswith("LS-"):
                variants.append(f"LS-{h_lower[:-1]}{alt}")

    return list(set(v.lower() for v in variants if v.strip()))


def _levenshtein(s1: str, s2: str) -> int:
    """편집 거리 계산 (Levenshtein distance)"""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row
    return prev_row[-1]


# ═════════════════════════════════════════
#  1단계: 완전 일치 (PROD_CD / PROD_NAME / MODEL)
#  + LS- 접두어 자동 추가 + 모델 필드 매칭
# ═════════════════════════════════════════
def exact_match(hint: str, products: List[dict]) -> Optional[dict]:
    variants = _expand_hint(hint)
    for variant in variants:
        v = variant.strip().lower()
        for p in products:
            if (p.get("prod_cd", "").lower() == v or
                p.get("prod_name", "").lower() == v or
                p.get("model", "").strip().lower() == v):
                return p
    return None


# ═════════════════════════════════════════
#  2단계: 다중 전략 키워드 검색
#  - 토큰 오버랩 + 부분문자열 매칭 + 퍼지(편집거리)
# ═════════════════════════════════════════
def keyword_search(hint: str, products: List[dict], top_k: int = 20) -> List[dict]:
    variants = _expand_hint(hint)
    hint_tokens = set()
    for v in variants:
        hint_tokens.update(_tokenize(v))

    # 힌트 전체 문자열들 (부분문자열 검색용)
    hint_normalized = [_normalize(v) for v in variants]

    scored = []
    for p in products:
        prod_cd   = p.get("prod_cd", "").lower()
        prod_name = p.get("prod_name", "").lower()
        model     = p.get("model", "").lower().strip()
        kw        = p.get("keywords", "").lower()
        text      = f"{prod_cd} {prod_name} {model} {kw}"

        score = 0.0

        # 전략 1: 토큰 오버랩
        text_tokens = set(_tokenize(text))
        overlap = len(hint_tokens & text_tokens)
        if overlap > 0:
            score = max(score, overlap / (len(hint_tokens) + 1))

        # 전략 2: 부분문자열 매칭
        text_norm = _normalize(text)
        for hn in hint_normalized:
            hn_compact = hn.replace(' ', '')
            text_compact = text_norm.replace(' ', '')
            # 힌트가 상품 텍스트에 포함
            if hn_compact and len(hn_compact) >= 3 and hn_compact in text_compact:
                sub_score = min(0.9, len(hn_compact) / max(len(text_compact), 1) + 0.5)
                score = max(score, sub_score)
            # 상품 모델명이 힌트에 포함
            if model:
                model_compact = _normalize(model).replace(' ', '')
                if model_compact and model_compact in hn_compact:
                    # 모델명이 힌트와 완전 일치하면 높은 점수, 부분 포함이면 낮은 점수
                    if model_compact == hn_compact:
                        score = max(score, 0.95)  # 완전 일치
                    elif len(model_compact) / max(len(hn_compact), 1) > 0.85:
                        score = max(score, 0.85)  # 거의 일치 (길이 85% 이상)
                    else:
                        score = max(score, 0.55)  # 부분 포함 (짧은 모델이 긴 힌트에 포함)

        # 전략 3: 개별 힌트 토큰의 부분문자열 매칭
        for ht in hint_tokens:
            if len(ht) < 3:
                continue
            for tt in text_tokens:
                if ht in tt or tt in ht:
                    ratio = min(len(ht), len(tt)) / max(len(ht), len(tt))
                    sub_score = ratio * 0.6
                    score = max(score, sub_score)

        # 전략 4: 편집 거리 기반 퍼지 매칭 (모델명 한정)
        if model and score < 0.3:
            model_norm = _normalize(model).replace(' ', '')
            for hn in hint_normalized:
                hn_compact = hn.replace(' ', '')
                if len(hn_compact) >= 3 and len(model_norm) >= 3:
                    dist = _levenshtein(hn_compact, model_norm)
                    max_len = max(len(hn_compact), len(model_norm))
                    if max_len > 0:
                        similarity = 1.0 - (dist / max_len)
                        if similarity >= 0.65:  # 65% 이상 유사하면 후보
                            score = max(score, similarity * 0.5)

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: -x[0])
    return [p for _, p in scored[:top_k]]


# ═════════════════════════════════════════
#  LLM Judge (상위 후보 중 최적 선택)
# ═════════════════════════════════════════
JUDGE_SYSTEM = """당신은 B2B 발주서 상품 매칭 전문가입니다.
거래처가 기재한 상품 표현과 후보 상품 목록을 받아 가장 적합한 항목을 선택합니다.

## 판단 기준
1. 모델명/품번 유사도 (가장 중요: LS-6UTPD-2MG 와 6utpd-2mg 는 동일 상품)
2. 스펙 일치 (길이, 색상, 규격 등)
3. 카테고리 일치 (케이블 종류, 커넥터 타입 등)
4. 제조사가 불분명하면 LANstar(랜스타) 제품을 우선 고려

## 주의
- 모델명에서 LS- 접두어 유무는 무시하고 비교
- 끝자리 b/g, m/n 차이는 색상 코드(Blue/Gray, Male 등)일 수 있으므로 다른 상품일 수 있음
- 길이(2M, 3M 등)가 다르면 다른 상품

반드시 아래 JSON 형식만 반환하세요:
{
  "selected_idx": 0,
  "confidence": 0.95,
  "reason": "매칭 근거 한 줄"
}
selected_idx: 후보 목록에서의 인덱스 (0부터), 적합한 것 없으면 -1
confidence: 0.0~1.0"""


async def llm_judge(hint: str, candidates: List[dict], specs: dict = None) -> dict:
    """LLM이 후보 중 최적 상품 선택"""
    if not candidates:
        return {"selected_idx": -1, "confidence": 0.0, "reason": "후보 없음"}

    cand_text = "\n".join(
        f"{i}. [{p['prod_cd']}] {p.get('prod_name','')} (모델: {p.get('model', p.get('model_name', ''))})"
        for i, p in enumerate(candidates)
    )

    # 스펙 컨텍스트 추가
    spec_context = ""
    if specs:
        spec_parts = []
        if specs.get("manufacturer"): spec_parts.append(f"제조사: {specs['manufacturer']}")
        if specs.get("category"): spec_parts.append(f"카테고리: {specs['category']}")
        if specs.get("length"): spec_parts.append(f"길이: {specs['length']}")
        if specs.get("color"): spec_parts.append(f"색상: {specs['color']}")
        if spec_parts:
            spec_context = f"\n파싱된 스펙: {', '.join(spec_parts)}"

    try:
        r = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=300,
            system=JUDGE_SYSTEM,
            messages=[{
                "role": "user",
                "content": f"발주서 상품 표현: {hint}{spec_context}\n\n후보 목록:\n{cand_text}"
            }]
        )
        text = r.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
        return json.loads(text)
    except Exception as e:
        logger.error(f"[LLM Judge] 오류: {e}")
        return {"selected_idx": 0, "confidence": 0.5, "reason": "LLM 판단 실패, 상위 후보 반환"}


# ═════════════════════════════════════════
#  메인 매칭 함수
# ═════════════════════════════════════════
async def resolve_product(
    product_hint: str,
    implicit_notes: str = "",
    cust_code: str = "",
    normalized_hints: List[str] = None,
    detected_specs: dict = None,
) -> List[dict]:
    """
    상품 힌트를 받아 상위 후보 목록을 반환합니다.

    매칭 파이프라인:
      0단계: 글로벌 별칭 DB
      0.5단계: 거래처별 학습 데이터
      1단계: 완전일치 (LS- 자동 포함)
      2단계: 스펙 분리 → 필터 → 키워드 검색
      3단계: 전체 키워드 검색 (fallback)
      4단계: LLM Judge

    Returns: [{"prod_cd":..., "prod_name":..., "score":..., "confidence":..., "match_reason":...}]
    """
    from models.schemas import MatchConfidence

    products = load_products()
    if not products:
        logger.warning("[Resolution] 상품 데이터 없음")
        return []

    full_hint = f"{product_hint} {implicit_notes}".strip()

    # normalized_hints가 있으면 추가 검색 변형으로 활용
    extra_hints = normalized_hints or []

    # ── 0단계: 글로벌 별칭 DB 조회 ──
    alias = lookup_alias(product_hint)
    if not alias and extra_hints:
        for nh in extra_hints:
            alias = lookup_alias(nh)
            if alias:
                break
    if alias:
        # 별칭 DB에 있으면 products.csv에서 정확한 정보 보완
        for p in products:
            if p.get("prod_cd", "").lower() == alias["prod_cd"].lower():
                logger.info(f"[Resolution] 글로벌 별칭 매칭: {product_hint} → {alias['prod_cd']} (hit:{alias.get('hit_count',1)})")
                return [{
                    "prod_cd": p["prod_cd"],
                    "prod_name": p.get("prod_name", ""),
                    "model_name": p.get("model", "").strip(),
                    "score": 0.96,
                    "confidence": MatchConfidence.HIGH,
                    "match_reason": f"글로벌 별칭 매칭 (과거 {alias.get('hit_count',1)}회 성공)",
                }]

    # ── 0.5단계: 거래처별 학습 데이터 ──
    if cust_code:
        try:
            from services.training_service import get_fewshot_item_map
            item_map = get_fewshot_item_map(cust_code)
            if item_map:
                hint_lower = product_hint.strip().lower()
                trained = item_map.get(hint_lower)
                if not trained:
                    # 정규화 후 완전 일치만 허용 (부분 매칭 금지)
                    hint_norm = re.sub(r'[\s\-_.]', '', hint_lower)
                    for key, val in item_map.items():
                        key_norm = re.sub(r'[\s\-_.]', '', key)
                        if key_norm == hint_norm:
                            trained = val
                            break

                if trained:
                    matched_product = None
                    for p in products:
                        if p.get("prod_cd", "").lower() == trained["item_code"].lower():
                            matched_product = p
                            break

                    if matched_product:
                        logger.info(f"[Resolution] 학습 데이터 매칭 성공: {product_hint} → {trained['item_code']}")
                        # 성공한 매핑을 글로벌 별칭에도 저장
                        save_alias(product_hint, matched_product["prod_cd"],
                                   matched_product.get("prod_name", ""),
                                   matched_product.get("model", ""))
                        return [{
                            "prod_cd": matched_product["prod_cd"],
                            "prod_name": matched_product.get("prod_name", trained.get("product_name", "")),
                            "model_name": matched_product.get("model", "").strip() or trained.get("model_name", ""),
                            "score": 0.98,
                            "confidence": MatchConfidence.HIGH,
                            "match_reason": f"학습 데이터 매칭 (거래처 과거 이력)",
                        }]
                    else:
                        logger.info(f"[Resolution] 학습 데이터 품목코드 직접 사용: {trained['item_code']}")
                        return [{
                            "prod_cd": trained["item_code"],
                            "prod_name": trained.get("product_name", ""),
                            "model_name": trained.get("model_name", ""),
                            "score": 0.95,
                            "confidence": MatchConfidence.HIGH,
                            "match_reason": f"학습 데이터 직접 매칭 (품목코드: {trained['item_code']})",
                        }]
        except Exception as e:
            logger.warning(f"[Resolution] 학습 데이터 매칭 실패: {e}")

    # ── 1단계: 완전 일치 (product_hint + normalized_hints) ──
    all_hints_to_try = [product_hint] + extra_hints
    for try_hint in all_hints_to_try:
        exact = exact_match(try_hint, products)
        if exact:
            save_alias(product_hint, exact["prod_cd"], exact.get("prod_name", ""), exact.get("model", ""))
            return [{
                "prod_cd":     exact["prod_cd"],
                "prod_name":   exact.get("prod_name", ""),
                "model_name":  exact.get("model", "").strip(),
                "score":       1.0,
                "confidence":  MatchConfidence.HIGH,
                "match_reason": "완전 일치",
            }]

    # ── 2단계: 스펙 분리 매칭 ──
    # 추출 단계에서 감지된 스펙이 있으면 활용, 없으면 직접 파싱
    specs = detected_specs if detected_specs else parse_specs(full_hint)
    logger.info(f"[Resolution] 스펙 파싱: {specs}")

    # 스펙 기반 필터링 → 필터된 상품에서 키워드 검색
    spec_filtered = filter_by_specs(products, specs)
    kw_results = []
    if spec_filtered:
        logger.info(f"[Resolution] 스펙 필터: {len(products)} → {len(spec_filtered)}건")
        kw_results = keyword_search(full_hint, spec_filtered, top_k=15)

    # ── 3단계: 전체 검색 (스펙 필터 결과가 부족하면) ──
    if len(kw_results) < 5:
        logger.info(f"[Resolution] 스펙 필터 결과 부족({len(kw_results)}건), 전체 검색 보강")
        all_results = keyword_search(full_hint, products, top_k=20)
        # 기존 결과에 없는 것만 추가
        existing_cds = {p.get("prod_cd") for p in kw_results}
        for p in all_results:
            if p.get("prod_cd") not in existing_cds:
                kw_results.append(p)
                existing_cds.add(p.get("prod_cd"))
            if len(kw_results) >= 20:
                break

    # 3.5단계: 여전히 부족하면 fallback
    if len(kw_results) < 3:
        logger.info(f"[Resolution] 키워드 검색 결과 부족({len(kw_results)}건), fallback 검색 실행")
        model_patterns = re.findall(r'[a-z0-9]{3,}', full_hint.lower())
        existing_cds = {p.get("prod_cd") for p in kw_results}
        for pattern in model_patterns:
            for p in products:
                if p.get("prod_cd") in existing_cds:
                    continue
                searchable = f"{p.get('prod_cd','')} {p.get('prod_name','')} {p.get('model','')} {p.get('keywords','')}".lower()
                if pattern in searchable:
                    kw_results.append(p)
                    existing_cds.add(p.get("prod_cd"))
            if len(kw_results) >= 10:
                break

    # ── 4단계: LLM Judge ──
    top_candidates = kw_results[:10]
    judge = await llm_judge(full_hint, top_candidates, specs)
    idx   = judge.get("selected_idx", -1)
    conf  = float(judge.get("confidence", 0.5))
    reason = judge.get("reason", "")

    # LLM이 선택한 항목을 글로벌 별칭에 저장 (높은 신뢰도일 때만)
    if idx >= 0 and idx < len(top_candidates) and conf >= CONFIDENCE_THRESHOLD:
        selected = top_candidates[idx]
        save_alias(product_hint, selected["prod_cd"],
                   selected.get("prod_name", ""), selected.get("model", ""))

    # 결과 구성
    results = []
    for i, p in enumerate(top_candidates[:TOP_K_RESULTS]):
        score = conf if i == idx else max(0.1, conf - 0.1 * (i + 1))
        if conf >= CONFIDENCE_THRESHOLD:
            confidence = MatchConfidence.HIGH
        elif conf >= 0.7:
            confidence = MatchConfidence.MEDIUM
        else:
            confidence = MatchConfidence.LOW

        results.append({
            "prod_cd":     p["prod_cd"],
            "prod_name":   p.get("prod_name", ""),
            "model_name":  p.get("model", "").strip(),
            "score":       round(score, 3),
            "confidence":  confidence if i == idx else MatchConfidence.LOW,
            "match_reason": reason if i == idx else "유사 후보",
        })

    results.sort(key=lambda x: -x["score"])
    return results
