#!/usr/bin/env python3
"""한국어 응답 생성.

톤은 기존 상담 이력을 그대로 따른다 — "랜스타입니다." 로 시작하고, 없는 자료는 돌려 말하지
않는다. caveat_ko 는 사람이 전달용으로 써 둔 문구라 요약·재작성하지 않고 그대로 붙인다.

모든 렌더 함수는 (사람이 읽는 텍스트, API 가 그대로 내보낼 수 있는 dict) 를 함께 만든다.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from . import loader, policy as policy_mod, search
from . import drive as drive_mod
from .drive import DriveRef, DriveResolver
from .policy import Policy
from .search import (DocView, FamilyResult, KCResult, LookupResult, TypeResult,
                     KC_FULL, KC_NONE, KC_NOT_INDEXED, KC_PARTIAL)

GREETING = '랜스타입니다.'
MAX_DOCS = 6

MARK_LABEL = {
    'O': '보유 (완제품 단위 문서)',
    'O*': '보유 (추론 매핑 — 적용범위 확인 필요)',
    '자재': '보유 (자재별 성적서 묶음)',
    '자재*': '보유 (자재별 묶음, 추론 매핑 — 확인 필요)',
    '전사': '보유 (제조사 전사 인증 — 모델별 인증서 아님)',
    'X': '미보유',
}

def _drive() -> DriveResolver:
    return drive_mod.get_resolver()


@dataclass
class Answer:
    """응답 텍스트 + 구조화 데이터."""
    text: str
    data: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:                     # print(answer) 가 바로 읽히도록
        return self.text


# ---------------------------------------------------------------- 조각

def _human_size(n: int) -> str:
    if not n:
        return ''
    mb = n / 1024 / 1024
    return f'{mb:.1f}MB' if mb >= 1 else f'{n / 1024:.0f}KB'


def _source_line(doc: DocView) -> str:
    s = doc.source or {}
    if not s:
        return ''
    subject = s.get('subject', '')
    date = s.get('date', '')
    return f"출처: {s.get('sender', '')} / \"{subject}\" / {date} 수신".strip()


#: issuer 원문은 '기관명 (풀네임) — 부연설명, 주소' 처럼 길다. 문서를 특정하는 데 필요한 건
#: 앞의 기관명뿐이라 부연을 자른다(원문은 data['issuer'] 에 그대로 있다).
_ISSUER_TAIL_RX = re.compile(r'\s*[—(,/].*$')


def _issuer_line(doc: DocView) -> str:
    bits = []
    if doc.issuer:
        name = doc.issuer.strip()
        if len(name) > 34:
            name = _ISSUER_TAIL_RX.sub('', name).strip() or name[:34]
        bits.append(name)
    if doc.doc_number:
        bits.append(doc.doc_number)
    if doc.issue_date:
        bits.append(doc.issue_date)
    # '유효기간 표기 없음' 은 890건 중 885건이라 정보가 아니다. 표기가 있을 때만 알린다.
    if doc.valid_until:
        bits.append(f'유효기간 {doc.valid_until}')
    return ' · '.join(bits)


def _location(doc: DocView, pol: Policy) -> Optional[DriveRef]:
    """파일 위치. 사내 모드에서는 '단독 전달 불가' 자료도 위치를 알려준다.

    담당자는 그 자료를 열어 보고 판단해야 하는 사람이다. 위치를 감추면 '자료는 있는데
    어디 있는지는 안 알려주는' 상태가 된다 — 전달 여부 경고는 이미 따로 붙는다.
    고객 모드에서는 종전대로 막는다.
    """
    if not pol.show_file_location:
        return None
    if pol.mode == policy_mod.CUSTOMER and not pol.may_share_file(doc):
        return None
    if doc.attribution_rejected:              # 출처가 반박된 문서는 사내에서도 안내하지 않는다
        return None
    return _drive().resolve_document(doc)


def _doc_block(doc: DocView, pol: Policy, idx: int) -> Dict[str, Any]:
    """문서 한 건. 읽는 사람은 사내 담당자이고, 알아야 할 것은 세 가지뿐이다 —
    이게 무슨 자료인지 / 써도 되는지 / 파일이 어디 있는지.

    메일 출처(발신인·제목·수신일)는 본문에서 뺀다. 자료를 어떻게 받았는지는 담당자가
    판단에 쓰지 않는 정보다. 필요하면 data['source'] 에 그대로 있다.
    발급기관·문서번호·발급일은 남긴다 — 고객사 제출 시 문서를 특정하는 값이다.
    """
    lines = [f'{idx}. {doc.title_ko or doc.filename}']
    lines.append(f'   {_issuer_line(doc)}')
    caveat = policy_mod.effective_caveat(doc)
    # 적용범위는 caveat 과 상당 부분 겹친다. caveat 이 있으면 그쪽이 더 정확하므로 생략한다.
    if not caveat:
        lines.append(f'   적용범위: {doc.scope_ko} — {doc.scope_target}')
    if caveat:
        lines.append(f'   안내: {caveat}')
    if doc.material_cats:
        lines.append(f"   자재구분: {'/'.join(doc.material_cats)}")
    if not doc.text_extractable:
        lines.append('   (스캔 문서 — 원문 인용 불가, 파일 확인 필요)')
    if not doc.deliverable:
        lines.append('   (단독 전달 불가 — 담당자 확인 필요)')

    ref = _location(doc, pol)
    if ref is not None:
        if ref.status == 'api_link':
            lines.append(f'   링크: {ref.url}')
        elif ref.status == 'local_path':
            lines.append(f'   파일: {ref.path}')
        else:
            lines.append(f'   위치: {ref.folder_url} 내 {ref.filename}')

    data = {
        'doc_id': doc.doc_id, 'doc_type': doc.doc_type, 'all_types': doc.all_types,
        'title_ko': doc.title_ko, 'scope': doc.scope, 'scope_ko': doc.scope_ko,
        'scope_target': doc.scope_target, 'provision': doc.provision,
        'material_cats': doc.material_cats, 'issuer': doc.issuer,
        'doc_number': doc.doc_number, 'issue_date': doc.issue_date,
        'valid_until': doc.valid_until, 'expired': doc.expired, 'standards': doc.standards,
        'caveat_ko': policy_mod.effective_caveat(doc), 'caveat_from_default': doc.caveat_missing,
        'deliverable': doc.deliverable, 'text_extractable': doc.text_extractable,
        'quotable': doc.text_extractable, 'names_query': doc.names_query,
        'secondary_type': doc.secondary_type, 'source': doc.source,
        'source_count': doc.source_count,
        'file': asdict(ref) if ref is not None else None,
    }
    # 내부 저장 파일명에는 다른 모델코드가 박혀 있다("이 모델 RoHS 는 사실 남의 문서"가 드러난다).
    if pol.show_file_location:
        data['path'], data['filename'] = doc.path, doc.filename
    return {'text': '\n'.join(lines), 'data': data}


#: 응답 본문에 들어가는 질의어. 원문을 그대로 넣으면 사내 웹UI 에서 마크업이 렌더되고,
#: 이 텍스트를 LLM 컨텍스트에 싣는 챗봇에서는 프롬프트 주입 경로가 된다. 원문은 data 에만 남긴다.
_UNSAFE_RX = re.compile(r'[<>&\r\n\t]')


def _safe_query(text: str, limit: int = 64) -> str:
    t = _UNSAFE_RX.sub(' ', text or '').strip()
    return t[:limit] + ('…' if len(t) > limit else '')


def _tentative_notice(resolution) -> List[str]:
    """확정 경로가 아닌 매칭이면 추정임을 반드시 알린다(자료는 붙여 주되 단정하지 않는다)."""
    if resolution.confident or not resolution.key:
        return []
    return [policy_mod.TENTATIVE_MATCH_NOTICE.format(
        q=_safe_query(resolution.normalized), key=resolution.key)]


def _doc_section(docs: Sequence[DocView], pol: Policy, inferred: bool = False,
                 bundle: bool = False) -> Dict[str, Any]:
    visible, hidden = pol.filter_documents(docs, inferred=inferred)
    # 묶음은 전체가 곧 근거다. 일부만 나열하면 그 자체로 근거가 성립하지 않으므로 자르지 않는다.
    limit = len(visible) if bundle else MAX_DOCS
    blocks = [_doc_block(d, pol, i + 1) for i, d in enumerate(visible[:limit])]
    text = '\n'.join(b['text'] for b in blocks)
    if len(visible) > limit:
        text += f'\n… 외 {len(visible) - limit}건'
    return {'text': text, 'documents': [b['data'] for b in blocks],
            'shown': len(blocks), 'total': len(visible), 'hidden': len(hidden)}


# ---------------------------------------------------------------- 자료유형 응답

def render_type(res: LookupResult, pol: Policy) -> Answer:
    """'LS-6UTPD-3MG RoHS 주세요' 형태의 응답."""
    cert, tr = res.cert, res.type_result
    assert tr is not None
    head = f'{GREETING} {_safe_query(res.query)}'
    if cert.families and cert.families[0].upper() != cert.resolution.normalized:
        head += f'(제품군 {", ".join(cert.families)})'
    head += f' {tr.doc_type} 자료 안내드립니다.'

    lines = [head, '']
    notices = pol.notices_for_type(tr, cert.sibling_only, cert.match_rule)
    notices = _tentative_notice(cert.resolution) + notices

    if not tr.held:
        lines.append(pol.not_held_text(tr))
        for h in tr.request_history:
            lines.append(f"  · {h['date']} \"{h['subject']}\" ({h['model']})")
        section: Dict[str, Any] = {'text': '', 'documents': [], 'shown': 0,
                                   'total': 0, 'hidden': 0}
    else:
        section = _doc_section(tr.documents, pol, inferred=tr.inferred, bundle=tr.bundle)
        if section['shown'] == 0:
            lines.append(policy_mod.CUSTOMER_INFERRED_REPLACEMENT if tr.inferred
                         else '보유 자료가 있으나 그대로 전달하기 어려운 자료입니다. '
                              '담당자 확인이 필요합니다.')
        else:
            label = (f'{tr.doc_type} 항목을 포함한 다른 유형 문서' if tr.secondary_only
                     else MARK_LABEL.get(tr.mark, tr.mark))
            lines.append(f'{tr.doc_type}: {label} — {section["total"]}건')
            if tr.bundle and tr.material_covered:
                lines.append(f"확보 자재: {', '.join(tr.material_covered)}")
            if tr.material_missing:
                lines.append(f"미확보 자재: {', '.join(tr.material_missing)}")
            lines.append('')
            lines.append(section['text'])

    notices = [n for n in notices if n not in lines]   # 본문에 이미 쓴 문구는 반복하지 않는다
    if notices:
        lines.append('')
        lines.extend(notices)

    data = {
        'kind': 'type', 'query': res.query, 'mode': pol.mode,
        'model_normalized': cert.resolution.normalized,
        'families': cert.families, 'members': cert.members,
        'match_reason': cert.resolution.reason, 'confident': cert.resolution.confident,
        'doc_type': tr.doc_type, 'mark': tr.mark, 'held': tr.held,
        'inferred': tr.inferred, 'bundle': tr.bundle,
        'material_covered': tr.material_covered, 'material_missing': tr.material_missing,
        'material_coverage_known': tr.material_coverage_known,
        'not_held_reason': tr.not_held_reason, 'request_history': tr.request_history,
        'documents': section['documents'], 'documents_total': section['total'],
        'documents_hidden': section['hidden'],
        'sibling_only': cert.sibling_only, 'notices': notices,
    }
    return Answer('\n'.join(l for l in lines if l is not None).strip(), data)


# ---------------------------------------------------------------- 보유현황 응답

def render_family(cert: FamilyResult, pol: Policy, kc: Optional[KCResult] = None) -> Answer:
    """'LS-7UTP 뭐뭐 있어?' 형태의 응답."""
    head = f'{GREETING} {_safe_query(cert.query)}'
    if cert.families and cert.families[0].upper() != cert.resolution.normalized:
        head += f'(제품군 {", ".join(cert.families)})'
    head += f' 보유 자료 현황입니다. (문서 {cert.document_count}건)'

    lines = [head, '']
    rows: List[Dict[str, Any]] = []
    notices: List[str] = _tentative_notice(cert.resolution)
    not_held: List[TypeResult] = []
    for col in loader.MATRIX_COLS:
        tr = cert.types.get(col)
        if tr is None:
            continue
        label = MARK_LABEL.get(tr.mark, tr.mark)
        if not tr.held:
            lines.append(f'· {col}: 미보유')
            not_held.append(tr)
        else:
            extra = ''
            if tr.material_missing:
                extra = f" (미확보 자재: {', '.join(tr.material_missing)})"
            visible, _ = pol.filter_documents(tr.documents, inferred=tr.inferred)
            if not visible:
                lines.append(f'· {col}: 확인 후 회신')
            elif tr.secondary_only:
                lines.append(f'· {col}: 매트릭스 기준 미보유 — {col} 항목을 포함한 '
                             f'다른 유형 문서 {len(visible)}건')
            else:
                lines.append(f'· {col}: {label} {len(visible)}건{extra}')
        rows.append({'doc_type': col, 'mark': tr.mark, 'held': tr.held,
                     'inferred': tr.inferred, 'bundle': tr.bundle,
                     'documents': len(tr.documents),
                     'material_covered': tr.material_covered,
                     'material_missing': tr.material_missing})
        if tr.held:
            notices.extend(pol.notices_for_type(tr, cert.sibling_only, cert.match_rule))

    if cert.other_types:
        lines.append(f"· 그 외 보유: {', '.join(cert.other_types)}")
        # 매트릭스 밖 유형도 만료·스캔·자재묶음 주의문구가 필요하다. 요약만 보고
        # 만료된 인증서를 유효한 보유 자료로 오인하면 그대로 고객사에 나간다.
        for col in cert.other_types:
            tr = cert.types.get(col) or search.type_result(
                cert.families, col, kb=loader.load_kb(),
                queried=[cert.resolution.normalized])
            if tr.material_missing:
                lines.append(f"   {col} 미확보 자재: {', '.join(tr.material_missing)}")
            notices.extend(pol.notices_for_type(tr, cert.sibling_only, cert.match_rule))

    # '미보유' 한 단어로 끝내면 담당자가 '자료 없음'으로 회신한다 — 명세서가 금지한 표현이다.
    if not_held:
        cols = ', '.join(t.doc_type for t in not_held)
        lines.append('')
        lines.append(f'· 미보유: {cols} — {policy_mod.NOT_HELD_BASE}')
        for t in not_held:
            if t.request_history:
                lines.append(f'   {t.doc_type}: {pol.not_held_text(t)}')
    # KC 는 별개 인덱스라 인증자료가 있어도 따로 알려줘야 한다(교집합 4건뿐).
    kc_label = {KC_FULL: 'KC 적합등록 완료자료 보유', KC_PARTIAL: 'KC 적합등록필증 PDF만 보유',
                KC_NONE: 'KC 완료자료 미수신 (인증원 재요청 필요)'}
    if kc is not None and kc.status in kc_label:
        lines.append(f'· {kc_label[kc.status]} ({kc.model})')
    if cert.company_wide:
        lines.append(f'· 제조사 전사 인증 {len(cert.company_wide)}건 (모델별 인증서가 아닙니다)')
    if len(cert.members) > 1:
        lines.append('')
        lines.append(f"적용 모델: {', '.join(cert.members[:12])}"
                     + (f" 외 {len(cert.members) - 12}개" if len(cert.members) > 12 else ''))
        if cert.match_rule:
            lines.append(f'※ {cert.match_rule}')

    notices = policy_mod.dedupe(notices)
    if notices:
        lines.append('')
        lines.extend(notices)

    data = {
        'kind': 'family', 'query': cert.query, 'mode': pol.mode,
        'model_normalized': cert.resolution.normalized,
        'families': cert.families, 'members': cert.members,
        'match_reason': cert.resolution.reason, 'confident': cert.resolution.confident,
        'coverage': cert.coverage, 'rows': rows, 'other_types': cert.other_types,
        'company_wide': len(cert.company_wide), 'document_count': cert.document_count,
        'kc_status': kc.status if kc is not None else None,
        'sibling_only': cert.sibling_only, 'notices': notices,
    }
    return Answer('\n'.join(lines).strip(), data)


# ---------------------------------------------------------------- KC 응답

def render_kc(kc: KCResult, pol: Policy) -> Answer:
    """'LS-OVERC KC 자료' 형태의 응답."""
    lines = [f'{GREETING} {_safe_query(kc.query)} KC 적합등록 자료 안내드립니다.', '']
    notices: List[str] = []
    file_ref: Optional[DriveRef] = None

    if kc.tentative and kc.model:
        notices.append(policy_mod.KC_TENTATIVE_NOTICE.format(
            q=_safe_query(kc.resolution.normalized), base=kc.model))

    if kc.status == KC_FULL:
        drive = kc.record.get('drive') or {}
        lines.append(f"KC 적합등록 완료자료를 보유하고 있습니다. (수신 {kc.received_date}"
                     f"{', ' + kc.sender if kc.sender else ''})")
        lines.append(f"파일: {drive.get('filename', '')} "
                     f"({_human_size(kc.size_bytes)}, 내부 문서 {len(kc.contents)}건)")
        lines.append('')
        lines.append('동봉 서류:')
        for kind, files in kc.kinds.items():
            lines.append(f"· {kind} {len(files)}건 — {files[0]}"
                         + (f' 외 {len(files) - 1}건' if len(files) > 1 else ''))
        if pol.show_file_location:
            file_ref = _drive().resolve_kc(kc.record)
            if file_ref.status == 'api_link':
                lines.append(f'\n링크: {file_ref.url}')
            elif file_ref.status == 'local_path':
                lines.append(f'\n파일: {file_ref.path}')
            else:
                lines.append(f'\n위치: {file_ref.folder_url} — {file_ref.note}')
        else:
            notices.append(policy_mod.CUSTOMER_FILE_NOTICE)
        notices.append(policy_mod.KC_NO_TEXT_NOTICE)
    elif kc.status == KC_PARTIAL:
        lines.append(policy_mod.KC_PARTIAL_NOTICE)
        if kc.received_date:
            lines.append(f'수신 {kc.received_date}')
        if pol.show_file_location:
            file_ref = _drive().resolve_kc(kc.record)
            if file_ref.status == 'local_path':
                lines.append(f'파일: {file_ref.path}')
            elif file_ref.status == 'api_link':
                lines.append(f'링크: {file_ref.url}')
        notices.append(policy_mod.KC_NO_TEXT_NOTICE)
    elif kc.status == KC_NONE:
        when = f"(최종 언급 {kc.last_mentioned})" if kc.last_mentioned else ''
        lines.append(policy_mod.KC_NONE_NOTICE.format(when=when))
    else:
        lines.append(policy_mod.KC_NOT_INDEXED_NOTICE)
        if kc.resolution.suggestions:
            lines.append(f"유사 등록모델: {', '.join(kc.resolution.suggestions)}")

    if notices:
        lines.append('')
        lines.extend(policy_mod.dedupe(notices))

    data = {
        'kind': 'kc', 'query': kc.query, 'mode': pol.mode, 'status': kc.status,
        'model': kc.model, 'tentative': kc.tentative,
        'match_reason': kc.resolution.reason,
        'received_date': kc.received_date, 'last_mentioned': kc.last_mentioned,
        'sender': kc.sender, 'mail_subject': kc.mail_subject,
        'size_bytes': kc.size_bytes, 'versions': kc.versions,
        'contents': kc.contents, 'kinds': kc.kinds,
        'has_certificate': kc.has_certificate,
        'file': asdict(file_ref) if file_ref is not None else None,
        'notices': notices,
    }
    return Answer('\n'.join(lines).strip(), data)


def render_kc_contains(kc: KCResult, info: Dict[str, Any], query: str,
                       pol: Policy) -> Answer:
    """'필증 들어있나요?' — zip 을 열지 않고 contents[] 로 답한다.

    KC 자료 자체가 없는 모델에 '포함되어 있지 않습니다' 라고 답하면 'KC 자료는 있는데
    그 서류만 빠졌다'는 없는 사실을 만들어낸다. 상태를 먼저 가른다.
    """
    label = info['kind'] or info['item']
    model = kc.model or query
    if kc.status == KC_FULL:
        state = '포함되어 있습니다' if info['contains'] else '포함되어 있지 않습니다'
        text = f'{GREETING} {model} KC 완료자료에 {label} 은(는) {state}.'
        if info['files']:
            text += '\n' + '\n'.join(f'· {f}' for f in info['files'][:10])
        text += '\n\n' + policy_mod.KC_NO_TEXT_NOTICE
    elif kc.status == KC_PARTIAL:
        text = (f'{GREETING} {model} 는 적합등록필증 PDF 1건만 보유하고 있어 '
                f'{label} 포함 여부를 확인할 수 없습니다.\n\n' + policy_mod.KC_PARTIAL_NOTICE)
    elif kc.status == KC_NONE:
        text = (f'{GREETING} {model} 는 완료자료를 수신한 기록이 없어 '
                f'{label} 포함 여부를 확인할 수 없습니다.\n\n'
                + policy_mod.KC_NONE_NOTICE.format(
                    when=f'(최종 언급 {kc.last_mentioned})' if kc.last_mentioned else ''))
    else:
        text = f'{GREETING} {query} — ' + policy_mod.KC_NOT_INDEXED_NOTICE
    if kc.tentative and kc.model:
        text += '\n' + policy_mod.KC_TENTATIVE_NOTICE.format(
            q=_safe_query(kc.resolution.normalized), base=kc.model)
    # info['kind'] 는 '필증' 같은 서류 종류다. 봉투의 kind(응답 종류)와 이름이 겹치므로
    # matched_kind 로 바꿔 싣는다 — 겹친 채로 두면 클라이언트가 응답 종류를 오판한다.
    return Answer(text, {
        'kind': 'kc_contains', 'query': query, 'mode': pol.mode,
        'status': kc.status, 'model': kc.model, 'kinds': list(kc.kinds),
        'notices': [], 'item': info['item'], 'matched_kind': info['kind'],
        'contains': info['contains'], 'files': info['files'],
        'matched_by': info['matched_by'], 'tentative': kc.tentative})


# ---------------------------------------------------------------- 인용 가능성

def render_content(res: LookupResult, pol: Policy) -> Answer:
    """"이 성적서 납 수치 얼마예요?" 류 — 원문 인용 가능 여부를 먼저 답한다."""
    tr = res.type_result
    # 같은 조건의 intent='files' 는 정책 게이트를 지나는데 여기만 빠져 있으면
    # 고객 모드에서 추론매핑 문서의 제목·발급기관이 그대로 나열된다.
    docs, hidden = ((pol.filter_documents(tr.documents, inferred=tr.inferred))
                    if tr else ([], []))
    lines = [f'{GREETING} {_safe_query(res.query)} 자료 내용 확인 관련 안내드립니다.', '']
    rows: List[Dict[str, Any]] = []
    notices: List[str] = _tentative_notice(res.cert.resolution)
    if not docs:
        if hidden:
            lines.append(policy_mod.CUSTOMER_INFERRED_REPLACEMENT)
        else:
            lines.append('해당 유형으로 확보된 자료가 없어 내용을 확인해 드릴 수 없습니다. '
                         + (pol.not_held_text(tr) if tr else policy_mod.NOT_HELD_BASE))
    for d in docs[:MAX_DOCS]:
        state = '원문 확인 가능' if pol.may_quote(d) else '원문 인용 불가 (스캔 이미지)'
        lines.append(f'· {d.title_ko or d.filename} — {state}')
        rows.append({'doc_id': d.doc_id, 'title_ko': d.title_ko,
                     'quotable': pol.may_quote(d), 'text_extractable': d.text_extractable})
    if any(not d.text_extractable for d in docs):
        notices.append(policy_mod.SCAN_NOTICE)
    if tr is not None and docs:
        notices.extend(pol.notices_for_type(tr, res.cert.sibling_only, res.cert.match_rule))
    notices = policy_mod.dedupe(notices)
    if notices:
        lines.append('')
        lines.extend(notices)
    return Answer('\n'.join(lines).strip(),
                  {'kind': 'content', 'query': res.query, 'mode': pol.mode,
                   'documents': rows, 'notices': notices})


# ---------------------------------------------------------------- 실패 응답

def render_ambiguous(res: LookupResult) -> Answer:
    """stem_ambiguous — 확정 답변 대신 되묻는다."""
    r = res.cert.resolution if res.cert.resolution.ambiguous else res.kc.resolution
    cands = r.candidates[:8]
    lines = [f'{GREETING} "{_safe_query(res.query)}" 는 여러 제품군에 해당합니다. 어느 것인지 알려주십시오.', '']
    kb = loader.load_kb()
    for c in cands:
        fam = kb.family(c)
        if fam:
            lines.append(f'· {c} (모델 {len(fam.get("members") or [])}개, '
                         f'문서 {len(fam.get("documents") or [])}건)')
        else:
            lines.append(f'· {c}')
    if len(r.candidates) > len(cands):
        lines.append(f'… 외 {len(r.candidates) - len(cands)}개')
    return Answer('\n'.join(lines),
                  {'kind': 'ambiguous', 'query': res.query, 'index': r.index,
                   'candidates': r.candidates, 'notices': []})


def render_not_found(res: LookupResult) -> Answer:
    """자료 미보유와 모델명 오류를 구분해서 답한다."""
    r = res.cert.resolution
    lines = [f'{GREETING} "{_safe_query(res.query)}" 로는 확보된 인증·기술자료를 찾지 못했습니다.']
    if res.kc.status not in (KC_NOT_INDEXED,):
        lines.append('KC 적합등록 자료는 별도로 확인됩니다 — 아래 KC 안내를 참고해 주십시오.')
    else:
        lines.append('인증자료 색인과 KC 색인 모두에 해당 모델이 없습니다. '
                     '제품이 실재한다면 자료를 확보한 적이 없는 것이므로 공급사 요청이 필요합니다.')
    if r.suggestions:
        lines.append(f"혹시 다음 모델을 찾으셨습니까? {', '.join(r.suggestions)}")
    return Answer('\n'.join(lines),
                  {'kind': 'not_found', 'query': res.query,
                   'model_normalized': r.normalized, 'suggestions': r.suggestions,
                   'kc_status': res.kc.status, 'notices': []})


# ---------------------------------------------------------------- 통합 진입점

def respond(query: str, doc_type: Optional[str] = None, mode: str = policy_mod.INTERNAL,
            intent: str = 'files', kb: Optional[loader.KnowledgeBase] = None,
            today: Optional[str] = None) -> Answer:
    """모델명(+자료유형)으로 최종 응답을 만든다.

    intent: 'files' 자료 안내 / 'content' 원문 인용 가능 여부 / 'kc' KC 자료.
    """
    kb = kb or loader.load_kb()
    pol = policy_mod.get_policy(mode)
    res = search.lookup(query, doc_type, kb, today)

    if res.ambiguous:
        return render_ambiguous(res)
    if intent == 'kc' or (not res.cert.found and res.kc.status != KC_NOT_INDEXED):
        ans = render_kc(res.kc, pol)
        # KC 로 빠지면서 요청받은 자료유형을 통째로 삼키면, RoHS 를 물은 사람에게
        # KC 패키지를 보내면서 요청에 답한 것처럼 보인다.
        if doc_type and intent != 'kc':
            dt = search.normalize_doc_type(doc_type) or doc_type
            head = (f'{GREETING} {_safe_query(res.query)} {dt} 자료는 현재 확보된 것이 없습니다. '
                    f'공급사에 요청이 필요합니다.\n'
                    f'KC 적합등록 자료는 별도로 보유하고 있어 함께 안내드립니다.\n')
            ans = Answer(head + ans.text.replace(GREETING + ' ', '', 1),
                         dict(ans.data, requested_doc_type=dt, cert_held=False,
                              not_held_reason=search.X_NO_EVIDENCE))
        return ans
    if not res.found:
        return render_not_found(res)
    if intent == 'content' and res.type_result is not None:
        return render_content(res, pol)
    if res.type_result is not None:
        return render_type(res, pol)
    return render_family(res.cert, pol, res.kc)
