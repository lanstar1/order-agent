#!/usr/bin/env python3
"""상담봇(assistant) 탭용 데이터 번들 생성.

무엇을 만드는가
    <repo>/data/assistant/
        제품인증자료/manifest.jsonl            (그대로 복사)
        제품인증자료/manifest_family.jsonl     (그대로 복사)
        제품인증자료/drive_folders.json        (그대로 복사)
        제품인증자료/자료요청이력.csv          (그대로 복사)
        제품인증자료/자료보유매트릭스.csv      (그대로 복사)
        제품인증자료/RoHS요청이력.csv          (그대로 복사)
        제품인증자료/_lib/model_family.py      (그대로 복사)
        KC적합등록자료/manifest.jsonl          (그대로 복사)
        lanstar_product_master.json            ★ PII 마스킹본
        BUNDLE.json                            (생성 메타데이터)

    디렉터리 이름을 원본 저장소와 똑같이 맞춘 이유: cert_lookup.loader 의 경로 상수가
    전부 CERT_KB_REPO 에서 파생되므로 `CERT_KB_REPO=<repo>/data/assistant` 하나만
    주면 6개 경로가 한 번에 맞는다.

왜 마스터를 그대로 넣지 않는가
    상담내역 원문에 다른 고객의 배송주소·주문번호·휴대폰·이메일이 그대로 들어 있다.
    chatbot.config.mask_pii() 를 question/answer 양쪽에 적용한 사본만 배포한다.
    제품분석(8필드)은 **한 글자도 건드리지 않는다** — 상담봇의 spec 게이트가 원문 기준으로
    캘리브레이션돼 있어서 여기서 손대면 판정이 어긋난다.

검증(하나라도 실패하면 exit 1)
    1. 마스킹본 상담내역에서 PII 패턴 잔존 0 건
    2. 제품분석 8필드가 원본과 문자 단위 동일
    3. 모델 수 / 상담내역 건수가 원본과 동일
    4. 그대로 복사한 파일들의 SHA256 이 원본과 동일

사용법
    python3 backend/scripts/prepare_assistant_data.py            # 기본 경로
    python3 backend/scripts/prepare_assistant_data.py --check    # 재생성 없이 검증만
    LANSTAR_MASTER_SRC=... CERT_KB_SRC_REPO=... python3 ...      # 소스 경로 재지정

멱등하다. 몇 번을 돌려도 같은 결과가 나오고, 중간에 죽어도 원본을 건드리지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

BACKEND = Path(__file__).resolve().parent.parent          # <repo>/backend
REPO = BACKEND.parent                                     # <repo>
DEST = Path(os.environ.get('ASSISTANT_DATA_DIR') or (REPO / 'data' / 'assistant'))

# ---------------------------------------------------------------- 소스 경로(이 맥 기준 기본값)

SRC_REPO = Path(os.environ.get('CERT_KB_SRC_REPO') or '/Users/lanstar/mail2')
SRC_MASTER = Path(os.environ.get('LANSTAR_MASTER_SRC')
                  or '/Users/lanstar/lanmart/lanstar_product_master.json')

CERT_SUB = '제품인증자료'
KC_SUB = 'KC적합등록자료'

#: (소스 상대경로, 번들 상대경로). 전부 원문 그대로 복사한다.
COPY_FILES: Tuple[Tuple[str, str], ...] = (
    (f'{CERT_SUB}/manifest.jsonl',          f'{CERT_SUB}/manifest.jsonl'),
    (f'{CERT_SUB}/manifest_family.jsonl',   f'{CERT_SUB}/manifest_family.jsonl'),
    (f'{CERT_SUB}/drive_folders.json',      f'{CERT_SUB}/drive_folders.json'),
    (f'{CERT_SUB}/자료요청이력.csv',         f'{CERT_SUB}/자료요청이력.csv'),
    (f'{CERT_SUB}/자료보유매트릭스.csv',     f'{CERT_SUB}/자료보유매트릭스.csv'),
    # loader 가 '자료 없음'의 사유(요청했지만 미수령)를 가르는 데 쓴다. 3KB.
    (f'{CERT_SUB}/RoHS요청이력.csv',         f'{CERT_SUB}/RoHS요청이력.csv'),
    (f'{CERT_SUB}/_lib/model_family.py',    f'{CERT_SUB}/_lib/model_family.py'),
    (f'{KC_SUB}/manifest.jsonl',            f'{KC_SUB}/manifest.jsonl'),
)

MASTER_NAME = 'lanstar_product_master.json'
BUNDLE_META = 'BUNDLE.json'

#: 제품분석 필드. 마스킹 대상이 아니며 원본과 완전히 같아야 한다.
SPEC_FIELDS = ('제품유형', '외형', '재질', '포트구성', '주요사양', '사용방법',
               '사용용도', '소구점', '사용용처', '사용용태', '사용용표')


# ---------------------------------------------------------------- 유틸

class Fail(Exception):
    """검증 실패. main 이 잡아서 exit 1 한다."""


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def load_masker() -> Tuple[Callable[[str], str], List[Tuple[Any, str]]]:
    """backend/chatbot/config.py 의 mask_pii 와 패턴 목록을 가져온다.

    패키지 import 가 되면 그것을 쓴다(운영과 완전히 같은 코드 경로). cert_lookup 의
    _lib 의존 때문에 막히는 상황(번들을 처음 만드는 순간이 그렇다)에서는 같은 파일을
    단독 로드한다 — 소스가 동일하므로 결과도 동일하다.
    """
    if str(BACKEND) not in sys.path:
        sys.path.insert(0, str(BACKEND))
    cfg_path = BACKEND / 'chatbot' / 'config.py'
    if not cfg_path.exists():
        raise Fail(f'chatbot/config.py 가 없습니다: {cfg_path}')
    try:
        from chatbot import config as cfg           # type: ignore
    except Exception:                               # noqa: BLE001 - 단독 로드로 폴백
        import importlib.util
        spec = importlib.util.spec_from_file_location('_assistant_chatbot_config', cfg_path)
        assert spec and spec.loader
        cfg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cfg)                # type: ignore[union-attr]
    patterns = [(rx, repl) for rx, repl in cfg._PII_PATTERNS]
    patterns.append((cfg._PII_LABEL_RX, '[이름]'))
    return cfg.mask_pii, patterns


def spec_blob(rec: Dict[str, Any]) -> str:
    """제품분석 하위 전체를 문자 단위 비교용 문자열로 만든다."""
    return json.dumps((rec or {}).get('제품분석'), ensure_ascii=False, sort_keys=True)


def consult_rows(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    cs = (rec or {}).get('고객상담') or {}
    rows = cs.get('상담내역') or []
    return rows if isinstance(rows, list) else []


# ---------------------------------------------------------------- 복사

def copy_static(dry: bool) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for src_rel, dst_rel in COPY_FILES:
        src = SRC_REPO / src_rel
        dst = DEST / dst_rel
        if not src.exists():
            raise Fail(f'소스 파일이 없습니다: {src}')
        if not dry:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
        if not dst.exists():
            raise Fail(f'번들 파일이 없습니다: {dst}')
        s, d = sha256(src), sha256(dst)
        if s != d:
            raise Fail(f'복사본 해시 불일치: {dst_rel}')
        out.append({'path': dst_rel, 'sha256': d, 'bytes': str(dst.stat().st_size)})
    return out


# ---------------------------------------------------------------- 마스킹

def build_master(dry: bool) -> Dict[str, Any]:
    mask_pii, patterns = load_masker()

    if not SRC_MASTER.exists():
        raise Fail(f'마스터가 없습니다: {SRC_MASTER}')
    with open(SRC_MASTER, encoding='utf-8') as fh:
        original: Dict[str, Any] = json.load(fh)

    dst = DEST / MASTER_NAME
    if dry:
        if not dst.exists():
            raise Fail(f'마스킹본이 없습니다: {dst}')
        with open(dst, encoding='utf-8') as fh:
            masked: Dict[str, Any] = json.load(fh)
    else:
        masked = json.loads(json.dumps(original, ensure_ascii=False))  # 깊은 복사
        for rec in masked.values():
            for row in consult_rows(rec):
                for field in ('question', 'answer'):
                    v = row.get(field)
                    if isinstance(v, str) and v:
                        row[field] = mask_pii(v)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix('.json.tmp')
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump(masked, fh, ensure_ascii=False)
        tmp.replace(dst)

    return verify_master(original, masked, patterns)


def verify_master(original: Dict[str, Any], masked: Dict[str, Any],
                  patterns: List[Tuple[Any, str]]) -> Dict[str, Any]:
    problems: List[str] = []

    # (3) 모델 수 / 상담내역 건수 -----------------------------------
    if len(original) != len(masked):
        problems.append(f'모델 수 불일치: 원본 {len(original)} / 마스킹본 {len(masked)}')
    if set(original) != set(masked):
        problems.append('모델 키 집합 불일치')

    n_rows_o = sum(len(consult_rows(r)) for r in original.values())
    n_rows_m = sum(len(consult_rows(r)) for r in masked.values())
    if n_rows_o != n_rows_m:
        problems.append(f'상담내역 건수 불일치: 원본 {n_rows_o} / 마스킹본 {n_rows_m}')

    # (2) 제품분석 8필드 문자 단위 동일 ------------------------------
    spec_diff = [k for k in original
                 if spec_blob(original[k]) != spec_blob(masked.get(k, {}))]
    if spec_diff:
        problems.append(f'제품분석이 변형된 모델 {len(spec_diff)}건: {spec_diff[:5]}')
    n_spec_fields = sum(1 for k in original
                        for f in SPEC_FIELDS
                        if (original[k].get('제품분석') or {}).get(f))

    # 상담내역의 비마스킹 필드(제품명/카테고리/source 등)도 그대로여야 한다
    meta_diff = 0
    for k, rec_o in original.items():
        rec_m = masked.get(k) or {}
        cs_o = (rec_o.get('고객상담') or {})
        cs_m = (rec_m.get('고객상담') or {})
        for key in set(cs_o) | set(cs_m):
            if key == '상담내역':
                continue
            if cs_o.get(key) != cs_m.get(key):
                meta_diff += 1
        for ro, rm in zip(consult_rows(rec_o), consult_rows(rec_m)):
            for key in set(ro) | set(rm):
                if key in ('question', 'answer'):
                    continue
                if ro.get(key) != rm.get(key):
                    meta_diff += 1
    if meta_diff:
        problems.append(f'상담내역 비마스킹 필드가 변형됨: {meta_diff}칸')

    # (1) PII 잔존 0 건 ---------------------------------------------
    before: Dict[str, int] = {}
    after: Dict[str, int] = {}
    for rec in original.values():
        for row in consult_rows(rec):
            for field in ('question', 'answer'):
                text = row.get(field) or ''
                for rx, _ in patterns:
                    before[rx.pattern[:28]] = before.get(rx.pattern[:28], 0) + \
                        len(rx.findall(text))
    for rec in masked.values():
        for row in consult_rows(rec):
            for field in ('question', 'answer'):
                text = row.get(field) or ''
                for rx, _ in patterns:
                    hits = rx.findall(text)
                    if hits:
                        after[rx.pattern[:28]] = after.get(rx.pattern[:28], 0) + len(hits)
    if after:
        problems.append(f'마스킹 후 PII 잔존: {after}')

    changed = 0
    for k, rec_o in original.items():
        for ro, rm in zip(consult_rows(rec_o), consult_rows(masked.get(k) or {})):
            if ro.get('question') != rm.get('question') or ro.get('answer') != rm.get('answer'):
                changed += 1

    if problems:
        raise Fail(' / '.join(problems))

    return {
        'models': len(masked),
        'consult_rows': n_rows_m,
        'consult_rows_masked': changed,
        'spec_fields_unchanged': n_spec_fields,
        'pii_hits_before': {k: v for k, v in sorted(before.items()) if v},
        'pii_hits_after': after,
    }


# ---------------------------------------------------------------- main

def main() -> int:
    global SRC_REPO, SRC_MASTER, DEST

    ap = argparse.ArgumentParser(description='상담봇 데이터 번들 생성/검증')
    ap.add_argument('--src-repo', default=None, help=f'기본: {SRC_REPO}')
    ap.add_argument('--master', default=None, help=f'기본: {SRC_MASTER}')
    ap.add_argument('--dest', default=None, help=f'기본: {DEST}')
    ap.add_argument('--check', action='store_true', help='재생성 없이 기존 번들만 검증')
    args = ap.parse_args()

    if args.src_repo:
        SRC_REPO = Path(args.src_repo)
    if args.master:
        SRC_MASTER = Path(args.master)
    if args.dest:
        DEST = Path(args.dest)

    try:
        files = copy_static(dry=args.check)
        stats = build_master(dry=args.check)
    except Fail as exc:
        print(f'[FAIL] {exc}', file=sys.stderr)
        return 1

    master_path = DEST / MASTER_NAME
    files.append({'path': MASTER_NAME, 'sha256': sha256(master_path),
                  'bytes': str(master_path.stat().st_size)})

    if not args.check:
        meta = {
            'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'source_repo': str(SRC_REPO),
            'source_master': str(SRC_MASTER),
            'source_master_sha256': sha256(SRC_MASTER),
            'masking': 'chatbot.config.mask_pii() applied to 고객상담.상담내역[].question/answer',
            'stats': stats,
            'files': files,
        }
        with open(DEST / BUNDLE_META, 'w', encoding='utf-8') as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)

    total = sum(int(f['bytes']) for f in files)
    print(f'[OK] 번들: {DEST}')
    print(f'     파일 {len(files)}개 / {total / 1024 / 1024:.2f} MB')
    print(f'     모델 {stats["models"]}개 / 상담내역 {stats["consult_rows"]}건 '
          f'(마스킹 적용 {stats["consult_rows_masked"]}건)')
    print(f'     제품분석 필드 {stats["spec_fields_unchanged"]}칸 원본과 동일')
    print(f'     PII 검출 원본 {sum(stats["pii_hits_before"].values())}건 → '
          f'마스킹본 {sum(stats["pii_hits_after"].values())}건')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
