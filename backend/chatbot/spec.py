#!/usr/bin/env python3
"""2단계 제품 사양 컨텍스트 — 화이트리스트 강제.

제품분석(`제품분석` 필드)은 **이미지 한 장을 보고 만든 AI 추론**이다. 근거로 쓰기 전에
두 개의 서로 다른 게이트를 통과시켜야 한다. 두 위험은 별개다.

  게이트 1 — 인증 환각. 제품분석의 인증 주장 581건 중 KB 마크가 'O' 로 실제 일치한 것은
    29건(5.0%)뿐이다. RoHS 주장 338건 중 296건(87.6%)이 근거 없음, UL 주장 71건 중
    모델별 인증서로 답할 수 있는 건 0건('전사' 12건은 제조사 단위 인증), KC 주장 77건 중
    96%가 KC 인덱스에 없음. KB 로 실시간 대조하는 전략은 제품분석 보유 모델의 73.5%에서
    작동조차 하지 않는다 → **검증 시도 없이 문장 단위로 삭제**한다(config.strip_cert_claims).
    오염은 소구점 448 / 주요사양 145 / 재질 7 / 포트구성 1 에 집중되고,
    제품유형·외형·사용방법·사용용도는 0건이다.

  게이트 2 — 제품 오인식. LS-300HO(벽부형 오픈 허브랙)가 "로봇 청소기 충전 스테이션",
    LS-AS301(3:1 HDMI 선택기)이 "외장형 SSD", LS-C5U305BK(305M 랜케이블)가 "멀티탭"으로
    적혀 있다. 검증 가능 범위 기준 오류율 13~17%(하한 10%). 그리고 제품유형이 틀리면
    주요사양 61.4% / 사용방법 61.4% / 사용용도 59.1% / 소구점 59.1% 가 같이 틀린다 →
    '제품유형만 빼고 나머지는 안전' 전략은 성립하지 않는다. 그래서 **8개 필드 전부를
    제품유형 검증 게이트 뒤에 둔다.**

필드 정책
  · 항상 제외: 소구점(마케팅 문구 + 인증 환각 집합소), 사용방법(오류 전파 최고)
  · 조건부: 제품유형·외형·재질·포트구성·사용용도 (검증 통과/불가 + 인증 마스킹 + 정량 검증)
  · 게이트: 주요사양은 검증 통과 모델에서 정량 검증을 통과한 항목만 개별 추출
"""
from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from . import config

# ---------------------------------------------------------------- 필드 접근

#: 키 이름이 깨진 모델이 있다: '사용용처' 11건, '사용용태' 1건, '사용용표' 1건.
#: '소구점' 키 자체가 없는 모델도 6건. rec['사용용도'] 직접 접근은 KeyError 를 낸다.
FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    '제품유형': ('제품유형',),
    '외형': ('외형',),
    '재질': ('재질',),
    '포트구성': ('포트구성',),
    '주요사양': ('주요사양',),
    '사용방법': ('사용방법',),
    '사용용도': ('사용용도', '사용용처', '사용용태', '사용용표'),
    '소구점': ('소구점',),
}

#: 사용 금지. 소구점 = 인증문구 469모델·위험률 88.3%·잘림률 최고, 사용방법 = 오류 전파 61.4%.
BLOCKED_FIELDS: Tuple[str, ...] = ('소구점', '사용방법')
#: 조건부 허용(검증 게이트 + 인증 마스킹 + 정량 검증 통과분만)
CONDITIONAL_FIELDS: Tuple[str, ...] = ('제품유형', '외형', '재질', '포트구성', '사용용도')
#: 제품유형 검증을 통과한 모델에서만, 그것도 항목 단위로만
GATED_FIELDS: Tuple[str, ...] = ('주요사양',)

ALL_FIELDS = CONDITIONAL_FIELDS + GATED_FIELDS + BLOCKED_FIELDS


def get_field(rec: Dict[str, Any], name: str) -> str:
    """별칭 폴백 getter. 직접 인덱싱하면 13~19개 모델에서 KeyError 가 난다."""
    for key in FIELD_ALIASES.get(name, (name,)):
        v = (rec or {}).get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ''


# ---------------------------------------------------------------- 제품 클래스 사전

#: AI 제품유형과 사람이 쓴 제품명/카테고리를 같은 클래스 공간으로 접어 비교한다.
PRODUCT_CLASSES: Dict[str, Tuple[str, ...]] = {
    'RACK': ('랙', '캐비닛', '서버랙', '허브랙', '랙케이스', 'rack', '전산랙', '오픈랙'),
    'RACK_ACC': ('선반', '트레이', '블랭크', '케이블타이', '랙 악세서리'),
    'LANCABLE': ('utp', 'stp', 'ftp', 'cat.', 'cat5', 'cat6', 'cat7', 'cat8', '랜케이블',
                 '랜 케이블', '네트워크 케이블', '이더넷 케이블', '패치코드'),
    'HDMICABLE': ('hdmi 케이블', 'hdmi케이블', 'dp 케이블', '디스플레이포트 케이블',
                  'dvi 케이블', 'aoc'),
    'USBCABLE': ('usb 케이블', 'usb케이블', '충전 케이블', '데이터 케이블', '연장 케이블'),
    'HUB': ('허브', 'hub', '도킹', 'docking', '독', '멀티포트'),
    'SELECTOR': ('선택기', '셀렉터', 'selector', 'switcher', '스위처', '전환기'),
    'SPLITTER': ('분배기', 'splitter', '분배'),
    'CONVERTER': ('컨버터', '변환', 'converter', '미러링', '리피터', 'extender', '익스텐더'),
    'KVM': ('kvm',),
    'CONNECTOR': ('커넥터', '콘넥터', '커플러', '젠더', '어댑터잭', '모듈러', '키스톤',
                  '패치판넬', '패치패널', 'rj45'),
    'ODD': ('odd', 'dvd', 'cd', '블루레이', '외장 odd', '광학 드라이브'),
    'STORAGE': ('ssd', 'hdd', '외장하드', '스토리지', 'm.2', 'sata', 'nvme', '하드케이스'),
    'POWER': ('멀티탭', '콘센트', '어댑터', '충전기', 'ups', '전원', '파워'),
    'STAND': ('거치대', '스탠드', '받침', '홀더', '자바라', '모니터암'),
    'FAN': ('쿨링팬', '팬', 'fan', '쿨러'),
    'OPTIC': ('광케이블', '광점퍼', '광컨버터', 'sfp', '광모듈', 'om3', 'om4', '광 컨버터'),
    'CAPTURE': ('캡쳐보드', '캡처보드', 'capture'),
    'DOORLOCK': ('도어락', '도어록', '잠금장치', '문고리', '지문인식'),
    'TOOL': ('테스터', '탈피기', '툴', '압착', '공구', '측정기'),
    'WASHER': ('컵세척기', '세척기'),
    'SCALE': ('저울', '캐리어저울'),
    'AUDIO': ('스피커', '이어폰', '헤드셋', '마이크', '오디오', 'rca'),
    'CAMERA': ('웹캠', '카메라', 'cctv'),
    'NIC': ('랜카드', '네트워크 카드', 'pcie', 'pci'),
    'GAMING': ('게이밍', '마우스', '키보드', '패드'),
}

#: AI 제품유형에만 등장하고 사람 표기에는 없는 '이물질 명사'.
#: 클래스 교집합 검출기는 LS-300HO 를 놓친다('도킹 스테이션'이 HUB 와 겹친다) —
#: 이 목록이 그 구멍을 막는다.
ALIEN_NOUNS: Tuple[str, ...] = (
    '로봇 청소기', '로봇청소기', '청소기', '충전 스테이션', '도킹 스테이션',
    '높이 조절 책상', '책상', '데스크 프레임', 'dj 컨트롤러', '미디 컨트롤러',
    'ups', '무정전', '멀티탭', '파워스트립', 'led 디스플레이 제어',
    '열화상', '적외선 열감지', '액션 카메라', '웹캠', '보안 카메라',
    '외장형 ssd', '외장형 하드', 'portable hdd', 'nvme ssd 어댑터',
    '라이트닝', 'lightning', '정수기', '가습기', '공기청정기',
)

_WORD_RX = re.compile(r'[^0-9a-z가-힣.]+')


def _classes(text: str) -> Set[str]:
    t = _WORD_RX.sub(' ', (text or '').lower())
    return {cls for cls, kws in PRODUCT_CLASSES.items() if any(k in t for k in kws)}


def _aliens(ai_text: str, human_text: str) -> List[str]:
    a = (ai_text or '').lower()
    h = (human_text or '').lower()
    return [n for n in ALIEN_NOUNS if n in a and n not in h]


# ---------------------------------------------------------------- 정량 검증

_U_RX = re.compile(r'(\d{1,2})\s*U(?![A-Za-z])')
_MM_RX = re.compile(r'(\d{3,4})\s*mm', re.IGNORECASE)
_CM_RX = re.compile(r'(\d{2,4})\s*cm', re.IGNORECASE)
_M_RX = re.compile(r'(\d+(?:\.\d+)?)\s*(?:m|미터)(?![a-z])', re.IGNORECASE)
_MODEL_LEN_RX = re.compile(r'-(\d+(?:\.\d+)?)M(?:[A-Z]{1,3})?$', re.IGNORECASE)
_U_HEIGHT_MM = 44.45


def _check_rack_u(text: str) -> Optional[str]:
    """1U = 44.45mm. 두 값이 모두 기재된 38개 중 8개(21%)가 물리적으로 불가능했다.

    U 값과 높이가 서로 다른 열거 항목에 적히는 일이 많아('랙 공간: 42U 규격' /
    '높이: 1800mm') 조각 단위가 아니라 **필드 전체**를 보고 판정한다.
    """
    um = _U_RX.search(text or '')
    if not um:
        return None
    u = int(um.group(1))
    if u < 2:
        return None
    heights = [int(x) for x in _MM_RX.findall(text)]
    heights += [int(x) * 10 for x in _CM_RX.findall(text)]
    heights = [h for h in heights if h >= 100]
    if not heights:
        return None
    need = u * _U_HEIGHT_MM
    if max(heights) < need - 1:
        return (f'{u}U 인데 표기 높이 최대 {max(heights)}mm — '
                f'1U=44.45mm 기준 최소 {need:.0f}mm 가 필요해 물리적으로 불가능')
    return None


def _check_cable_length(model: str, segment: str) -> Optional[str]:
    """LS-6UTP-300MBK(300M 박스)의 주요사양이 '길이: 300cm (3m)' 로 적힌 사례가 있다."""
    mm = _MODEL_LEN_RX.search(model or '')
    if not mm:
        return None
    want = float(mm.group(1))
    found = [float(x) for x in _M_RX.findall(segment)]
    found += [float(x) / 100.0 for x in _CM_RX.findall(segment)]
    if not found:
        return None
    if all(abs(f - want) > max(0.2, want * 0.2) for f in found):
        return (f'모델명 길이 {want:g}M 과 본문 표기 '
                f'{", ".join(f"{f:g}m" for f in found)} 가 불일치')
    return None


_DIM_RX = re.compile(r'\d\s*(?:mm|cm)|높이|치수|규격|사이즈', re.IGNORECASE)


def _quant_filter(model: str, text: str) -> Tuple[str, List[str]]:
    """정량 모순이 있는 조각만 버린다. 정규식 + 산술로 잡히는 명백한 오류들."""
    if not text:
        return '', []
    rack_bad = _check_rack_u(text)
    kept: List[Tuple[str, str]] = []
    flags: List[str] = []
    for seg, sep in config.split_segments(text):
        s = seg.strip()
        if not s:
            continue
        why = None
        if rack_bad and (_U_RX.search(s) or _DIM_RX.search(s)):
            why = rack_bad                     # U 값과 치수 표기를 함께 버린다
        elif _check_cable_length(model, s):
            why = _check_cable_length(model, s)
        if why:
            flags.append(f'{s} → {why}')
            continue
        kept.append((seg, sep))
    return config.join_segments(kept), flags


# ---------------------------------------------------------------- 마스터 캐시

_MASTER: Optional[Dict[str, dict]] = None
_LOCK = threading.Lock()


def load_master(path=None) -> Dict[str, dict]:
    global _MASTER
    with _LOCK:
        if _MASTER is None:
            with open(path or config.MASTER_PATH, encoding='utf-8') as f:
                _MASTER = json.load(f)
        return _MASTER


def reset_cache() -> None:
    global _MASTER
    with _LOCK:
        _MASTER = None


def _record(model: str) -> Tuple[str, Optional[dict]]:
    """모델 키를 찾는다(대소문자·언더스코어 관용)."""
    master = load_master()
    if not model:
        return '', None
    if model in master:
        return model, master[model]
    key = model.strip().upper().replace('_', '-')
    if key in master:
        return key, master[key]
    for k in master:
        if k.upper().replace('_', '-') == key:
            return k, master[k]
    return key, None


# ---------------------------------------------------------------- 이미지

def product_image(model: str) -> Dict[str, Optional[str]]:
    """{모델명}.jpg 로 98.5% 가 정확 일치, 나머지 26개는 {모델}_*.jpg 폴백으로 100% 커버.

    파일이 평균 2.3MB / 최대 11MB 라 base64 인라인 금지 — URL·경로만 돌려준다.
    """
    key, rec = _record(model)
    if rec is None or not (rec or {}).get('제품분석'):
        return {'filename': None, 'url': None, 'path': None}
    fname = f'{key}.jpg'
    p = config.IMAGE_DIR / fname
    if not p.exists():
        cands = sorted(config.IMAGE_DIR.glob(f'{key}_*.jpg'))
        if cands:
            p = cands[0]
            fname = p.name
        else:
            p = None
    # URL 은 쇼핑몰이 공개 서빙하는 주소라 로컬 파일 유무와 무관하게 유효하다.
    # 배포 번들에는 이미지 3.9GB 를 넣지 않으므로, 파일 존재를 URL 조건으로 걸면
    # 서버에서만 이미지 안내가 통째로 사라진다(로컬↔서버 응답이 달라진다).
    return {'filename': fname,
            'url': config.IMAGE_URL_BASE + fname,
            'path': str(p) if p else None}


# ---------------------------------------------------------------- 검증

@dataclass
class Verification:
    status: str                       # 'pass' | 'unknown' | 'conflict'
    reason: str = ''
    ai_classes: List[str] = field(default_factory=list)
    human_classes: List[str] = field(default_factory=list)
    aliens: List[str] = field(default_factory=list)
    human_text: str = ''

    def as_dict(self) -> Dict[str, Any]:
        return {'status': self.status, 'reason': self.reason,
                'ai_classes': self.ai_classes, 'human_classes': self.human_classes,
                'aliens': self.aliens}


def _kb_product_text(model: str) -> str:
    """인증 KB 문서가 말하는 제품 설명. 공급사·시험기관이 쓴 것이라 AI 분석보다 신뢰도가 높다."""
    try:
        from cert_lookup import loader as _cl, resolver as _cr
        kb = _cl.load_kb()
        res = _cr.resolve_cert(model, kb)
        if not res.keys:
            return ''
        bits: List[str] = []
        for key in res.keys[:2]:
            for rec in kb.documents(key)[:6]:
                for f in ('product_in_doc', 'title_ko', 'scope_target'):
                    v = (rec.get(f) or '').strip()
                    if v:
                        bits.append(v)
        return ' '.join(bits)[:2000]
    except Exception:                       # 교차검증 실패는 차단 사유가 아니다
        return ''


def verify_product_type(model: str) -> Verification:
    """AI 제품유형 vs 사람이 쓴 제품명+카테고리.

    '기타' 카테고리가 414건 중 152건(36.7%)이라 검증 불가가 정상 경로다 —
    검증 실패(conflict)와 검증 불가(unknown)를 반드시 구분한다.
    카테고리 단독은 오답이 있으므로(LS-CATCH4) 제품명 + 카테고리를 합쳐 본다.
    """
    _key, rec = _record(model)
    if rec is None:
        return Verification('unknown', '마스터에 없는 모델')
    pa = rec.get('제품분석') or {}
    cs = rec.get('고객상담') or {}
    if not pa:
        return Verification('unknown', '제품분석 없음(이미지 미보유 모델 299개)')
    ai_text = get_field(pa, '제품유형')
    human_text = ' '.join(x for x in [(cs.get('제품명') or ''), (cs.get('카테고리') or '')]
                          if x).strip()
    if not human_text or human_text.strip() == '기타':
        # 상담이력이 없어도 KB 문서(공급사가 작성한 제품 설명)로 교차검증할 수 있다.
        # 이 폴백이 없으면 alien 검사 자체를 건너뛰어, 'LS-C5U305G(CAT.5 UTP 305M 케이블)
        # = 전원 관리 멀티탭' 같은 오기재가 그대로 사양 답변으로 나간다(실측 48개 모델).
        human_text = _kb_product_text(model)
        if not human_text:
            # 교차검증이 불가능한데 오인식 지표가 있으면 틀렸다고 단정할 수도, 맞다고 볼 수도
            # 없다. 제품유형을 감추고 경고를 붙인다 — 확정 서술이 나가는 것보다 안전하다.
            blind = _aliens(ai_text, '')
            if blind:
                return Verification('unverified_alien',
                                    f'교차검증 불가 + 오인식 지표: {", ".join(blind)}',
                                    sorted(_classes(ai_text)), [], blind, '')
            return Verification('unknown',
                                '교차검증할 사람 작성 표기 없음(상담이력·KB 문서 모두 없음)',
                                sorted(_classes(ai_text)), [], [], '')
    aliens = _aliens(ai_text, human_text)
    ac, hc = _classes(ai_text), _classes(human_text)
    if aliens:
        return Verification('conflict', f'AI 제품유형에만 등장하는 이물질 명사: {", ".join(aliens)}',
                            sorted(ac), sorted(hc), aliens, human_text)
    if not ac or not hc:
        return Verification('unknown', '제품클래스로 접히지 않는 표기',
                            sorted(ac), sorted(hc), [], human_text)
    if ac & hc:
        return Verification('pass', f'제품클래스 교집합 {sorted(ac & hc)}',
                            sorted(ac), sorted(hc), [], human_text)
    return Verification('conflict', f'제품클래스 충돌 AI{sorted(ac)} vs 표기{sorted(hc)}',
                        sorted(ac), sorted(hc), [], human_text)


def is_cert_contaminated(model: str) -> bool:
    """제품분석 8필드 어디에든 인증 문구가 있으면 True (전체 1,739 중 474개 = 27.3%)."""
    _key, rec = _record(model)
    pa = (rec or {}).get('제품분석') or {}
    return any(config.CERT_TOKEN_RX.search(get_field(pa, f)) for f in ALL_FIELDS)


def cert_fragments(model: str) -> Dict[str, List[str]]:
    """어떤 필드의 어떤 문장이 인증 주장인지(디버깅·KB 갱신 참고용)."""
    _key, rec = _record(model)
    pa = (rec or {}).get('제품분석') or {}
    out: Dict[str, List[str]] = {}
    for f in ALL_FIELDS:
        _clean, dropped = config.strip_cert_claims(get_field(pa, f))
        if dropped:
            out[f] = dropped
    return out


# ---------------------------------------------------------------- 공개 컨텍스트

def spec_context(model: str) -> Dict[str, Any]:
    """LLM/렌더에 넘길 수 있는 형태의 사양 컨텍스트.

    반환 dict 의 `usable_as_spec` 이 False 면 사양 답변 근거로 쓰면 안 된다.
    `source` 는 항상 '제품분석(이미지 기반 AI)' 이고 `notices` 를 반드시 함께 출력해야 한다.
    """
    key, rec = _record(model)
    out: Dict[str, Any] = {
        'model': key, 'found': rec is not None, 'has_analysis': False,
        'verification': 'unknown', 'verification_reason': '',
        'fields': {}, 'dropped_cert': {}, 'quant_flags': [],
        'blocked_fields': list(BLOCKED_FIELDS), 'excluded_fields': [],
        'cert_contaminated': False, 'usable_as_spec': False,
        'source': config.SOURCE_SPEC, 'ai_generated': True,
        'image': {'filename': None, 'url': None, 'path': None},
        'category': None, 'product_name': None,
        'notices': [],
    }
    if rec is None:
        return out
    pa = rec.get('제품분석') or {}
    cs = rec.get('고객상담') or {}
    out['category'] = cs.get('카테고리')
    out['product_name'] = cs.get('제품명')
    if not pa:
        out['notices'].append('이 모델은 제품 이미지가 없어 사양 참고 정보 자체가 없습니다.')
        return out

    out['has_analysis'] = True
    out['image'] = product_image(key)
    ver = verify_product_type(key)
    out['verification'] = ver.status
    out['verification_reason'] = ver.reason
    out['cert_contaminated'] = is_cert_contaminated(key)

    if ver.status == 'conflict':
        # 제품유형이 틀리면 나머지 필드도 같은 환각 위에 세워진다(전파율 최대 61.4%)
        out['excluded_fields'] = list(ALL_FIELDS)
        out['notices'].append(config.SPEC_CONFLICT_NOTICE)
        return out

    fields: Dict[str, str] = {}
    dropped: Dict[str, List[str]] = {}
    flags: List[str] = []
    names = list(CONDITIONAL_FIELDS) + (list(GATED_FIELDS) if ver.status == 'pass' else [])
    if ver.status == 'unverified_alien':
        # 제품 종류 자체가 의심스러운데 교차검증할 근거가 없다. 제품유형은 감추고
        # 나머지 필드만 경고와 함께 낸다 — 확정 서술이 나가는 것보다 안전하다.
        names = [n for n in names if n != '제품유형']
    for name in names:
        raw = get_field(pa, name)
        if not raw:
            continue
        cleaned, cert_dropped = config.strip_cert_claims(raw)
        if cert_dropped:
            dropped[name] = cert_dropped
        cleaned, qflags = _quant_filter(key, cleaned)
        if qflags:
            flags.extend(f'[{name}] {x}' for x in qflags)
        if cleaned:
            fields[name] = cleaned
    out['fields'] = fields
    out['dropped_cert'] = dropped
    out['quant_flags'] = flags
    out['excluded_fields'] = [f for f in ALL_FIELDS if f not in fields]
    out['usable_as_spec'] = bool(fields)
    out['notices'].append(config.SPEC_NOTICE)
    if ver.status == 'unknown':
        out['notices'].append(config.SPEC_UNVERIFIED_NOTICE)
    elif ver.status == 'unverified_alien':
        out['notices'].append(
            '※ 이 모델은 제품 종류를 교차검증할 근거(상담이력·인증 문서)가 없고, '
            f'AI 분석 결과에 다른 품목({", ".join(ver.aliens)}) 표현이 섞여 있습니다. '
            '제품 종류가 실제와 다를 수 있으니 담당자 확인이 필요합니다.')
    if flags:
        out['notices'].append('※ 아래 항목은 수치 모순이 확인돼 제외했습니다: '
                              + ' / '.join(flags))
    return out


def render_spec(ctx: Dict[str, Any]) -> str:
    """규칙 기반 사양 요약(LLM 없이 동작하는 기본 경로)."""
    if not ctx.get('fields'):
        return ''
    order = [f for f in (list(CONDITIONAL_FIELDS) + list(GATED_FIELDS)) if f in ctx['fields']]
    return '\n'.join(f'· {f}: {ctx["fields"][f]}' for f in order)
