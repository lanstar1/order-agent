"""상담봇(assistant) API 라우터 — 사내 영업·CS 인증/자료 상담.

order-agent 안에서 `cert_lookup`(인증·기술자료 KB) + `chatbot`(상담 엔진)을 노출한다.
프론트 새 탭 `상담봇`(data-page="assistant") 이 이 라우터만 호출한다.

  POST /api/assistant/chat        {message, session_id?, mode?}
  POST /api/assistant/clarify     {session_id, choice, mode?}
  GET  /api/assistant/health      KB·마스터 로딩 / LLM 가용 / 데이터 번들 경로 상태
  GET  /api/assistant/spec        ?model=   제품분석(화이트리스트 + 오염 플래그)
  GET  /api/assistant/history     ?model=   모델별 과거 상담이력
  GET  /api/assistant/file/{id}   드라이브 링크 / 로컬 경로 해석

이 계층이 지키는 것 (원본 chatbot/api.py 의 규칙을 order-agent 에서도 깨지 않기 위한 것들)
  * **모든 엔드포인트에 `Depends(get_current_user)`.** 사내 전용 자료 경로가 나가는 화면이다.
  * **응답 텍스트를 손대지 않는다.** engine 이 만든 text 와 notices[] 를 그대로 실어 나른다.
  * **근거 배지는 서버가 계산한다** — `chatbot.api.badges()` 를 그대로 재사용한다.
    배지 사전을 여기 복사하면 독립 서버 UI 와 order-agent 탭의 배지가 언젠가 갈라진다.
  * **모드는 조용히 넘어가지 않는다.** internal/customer 외의 값은 Enum 검증으로 422.
    ('customerr' 를 internal 로 흡수하면 고객에게 사내 전용 정보가 나간다 — fail closed.)
  * **세션은 사원별로 격리한다.** order-agent 는 `--workers 1` 이라 세션 저장소가
    프로세스 전역이다. 클라이언트가 보낸 session_id 를 그대로 키로 쓰면 두 직원이
    같은 문자열을 보냈을 때 남의 대화 맥락(직전 모델·되묻기)을 이어받는다.
    → 저장 키는 항상 `{emp_cd}:{session_id}` 이고, 응답에는 원래 값만 되돌려준다.

★ import 순서 주의: `config` 가 `os.environ.setdefault()` 로 데이터 번들 경로를 심는데
  `cert_lookup.loader` / `chatbot.config` 는 그 경로를 **import 시점에** 읽는다.
  그래서 `import config` 가 반드시 `cert_lookup`/`chatbot` 보다 먼저 와야 한다.

★ 예열 정책 (Render 콜드스타트 고려):
  startup 훅에 예열을 **넣지 않았다**. 실측 최초 로드 0.58초
  (KB 0.03 + 상담이력 인덱스 0.42 + 마스터 0.03 + import 0.10).
  startup 에 넣으면 상담봇을 아무도 안 쓰는 배포에서도 매 콜드스타트가 0.58초 느려진다.
  대신 프론트가 탭을 열 때 `GET /health` 를 먼저 호출하고, 그 호출이 예열을 끝낸다
  (`_warm()`). 사용자가 첫 질문을 입력하는 사이에 로딩이 끝나므로 체감 지연이 없다.
  이후는 프로세스 캐시라 실측 chat 0.087초 → 0.001초.

★ 라우터 함수는 전부 `def`(async 아님)다. cert_lookup/chatbot 호출은 동기 블로킹이라
  `async def` 로 두면 이벤트 루프를 막아 다른 API 까지 같이 느려진다.
  `def` 로 두면 FastAPI 가 스레드풀에서 돌린다.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# ★ 반드시 cert_lookup/chatbot 보다 먼저 — 데이터 번들 경로 기본값을 환경변수에 심는다
import config as oa_config  # noqa: F401  (import 부작용이 목적)
from security import get_current_user

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from cert_lookup import cli as cert_cli
from cert_lookup import loader as cert_loader
from chatbot import config as chat_config
from chatbot import engine, history, llm, session, spec
from chatbot import router as chat_router
# 근거 배지·파일카드·응답 봉투는 원본 API 계층과 **같은 코드**를 쓴다(단일 진실 원천).
from chatbot.api import ROUTE_LABEL, BADGE_SPEC, Mode, badges, collect_files, reply_envelope
from services import assistant_drive

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/assistant", tags=["assistant"])

MODE_Q = Query(Mode.internal, description="internal(기본, 사내 전용) | customer")

#: 예열 결과. /health 가 읽는다.
_STATE: Dict[str, Any] = {"warmed_at": None, "kb": {}, "master": {}, "errors": []}
_WARM_LOCK = threading.Lock()


# ─────────────────────────────────────────
#  예열 (프로세스당 1회)
# ─────────────────────────────────────────
def _warm() -> None:
    """KB·상담이력 인덱스·마스터를 프로세스당 1회 로드한다.

    로더들이 각자 모듈 캐시를 갖고 있어 두 번째 호출부터는 사실상 무료지만,
    첫 요청이 동시에 두 건 들어오면 6MB JSON 을 두 번 파싱하게 되므로 락으로 막는다.
    KB 가 없어도 예외를 올리지 않는다 — /health 로 원인을 봐야 하기 때문이다.
    """
    if _STATE["warmed_at"] is not None:
        return
    with _WARM_LOCK:
        if _STATE["warmed_at"] is not None:
            return
        t0 = time.time()
        errors: List[str] = []
        # 서비스 계정이 있으면 드라이브 직링크·다운로드가 열린다. 없으면 폴더 링크로 폴백.
        try:
            assistant_drive.setup()
        except Exception as exc:                 # noqa: BLE001 - 자격증명 문제로 상담봇이 죽으면 안 된다
            errors.append(f"드라이브 연결 실패: {type(exc).__name__}: {exc}")
        try:
            kb = cert_loader.load_kb()
            cert_cli.drive_resolver(kb)          # 동기화 루트 탐색도 프로세스당 1회
            _STATE["kb"] = {
                "loaded": True,
                "documents": len(kb.docs), "families": len(kb.families),
                "kc_models": len(kb.kc), "rohs_requests": len(kb.rohs_requests),
                "problems": list(kb.problems or []),
            }
        except Exception as exc:                 # noqa: BLE001 — 진단이 목적
            _STATE["kb"] = {"loaded": False, "problems": [f"{type(exc).__name__}: {exc}"]}
            errors.append(f"인증KB 로드 실패: {exc}")
            logger.warning(f"[assistant] 인증KB 로드 실패: {exc}")
        try:
            idx = history.get_index()
            master = spec.load_master()
            _STATE["master"] = {
                "loaded": True,
                "path": str(chat_config.MASTER_PATH),
                "models": len(master),
                "qa_rows": len(idx.rows),
                "models_with_history": len(idx.models),
                "dropped_template": idx.dropped_template,
                "aliases": len(idx.aliases),
            }
        except Exception as exc:                 # noqa: BLE001
            _STATE["master"] = {"loaded": False, "path": str(chat_config.MASTER_PATH),
                                "error": f"{type(exc).__name__}: {exc}"}
            errors.append(f"제품 마스터 로드 실패: {exc}")
            logger.warning(f"[assistant] 제품 마스터 로드 실패: {exc}")
        _STATE["errors"] = errors
        _STATE["boot_sec"] = round(time.time() - t0, 3)
        _STATE["warmed_at"] = time.time()
        logger.info(f"[assistant] 예열 완료 {_STATE['boot_sec']}초 "
                    f"KB={_STATE['kb'].get('documents', 0)}건 "
                    f"상담={_STATE['master'].get('qa_rows', 0)}행")


def _guard(fn, *args, **kwargs):
    """코어 예외를 500 대신 의미 있는 HTTP 로 바꾼다(원본 API 계층과 동일 규칙)."""
    try:
        return fn(*args, **kwargs)
    except ValueError as exc:                    # 모드 오타·모델 스코프 누락 = 잘못된 요청
        raise HTTPException(status_code=422, detail={"message": str(exc)}) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail={
            "message": f"데이터 파일을 열 수 없습니다: {exc}",
            "hint": "GET /api/assistant/health 의 problems 를 확인하십시오."}) from exc
    except HTTPException:
        raise
    except Exception as exc:                     # noqa: BLE001
        logger.error(f"[assistant] {type(exc).__name__}: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail={
            "message": f"{type(exc).__name__}: {exc}",
            "trace": traceback.format_exc(limit=4).splitlines()[-3:]}) from exc


def _scoped(user: dict, session_id: str) -> str:
    """세션 저장 키를 사원 코드로 네임스페이스한다. 빈 값이면 무상태(맥락 없음)."""
    sid = (session_id or "").strip()
    if not sid:
        return ""
    emp = (user or {}).get("emp_cd") or "?"
    return f"{emp}:{sid}"


# ─────────────────────────────────────────
#  요청 모델
# ─────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000,
                         description='사용자 문장. 예: "LS-6UTPD-3MG RoHS 인증서 주세요"')
    session_id: str = Field("", max_length=120,
                            description="대화 맥락 키. 비우면 상태 없는 1회성 응답.")
    mode: Mode = Field(Mode.internal, description="internal(기본) | customer. 오타는 422.")


class ClarifyRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=120,
                            description="되묻기를 받은 그 세션.")
    choice: str = Field(..., min_length=1, max_length=200,
                        description="후보 선택. '2번' / '두번째' / 'LS-6UTPD' 전부 인식한다.")
    mode: Mode = Field(Mode.internal)


# ─────────────────────────────────────────
#  대화
# ─────────────────────────────────────────
@router.post("/chat", summary="상담 한 턴")
def assistant_chat(body: ChatRequest = Body(...),
                   user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """근거 배지 + 주의문구 + 파일 위치까지 한 번에 돌려준다.

    `route` 가 `cert` 면 cert_lookup 이 유일 근거이며 제품분석·상담이력을 섞지 않는다.
    UI 에서 두 경로를 한 문단으로 합치면 안 된다.
    """
    _warm()
    sid = _scoped(user, body.session_id)
    t0 = time.time()
    reply = _guard(engine.chat, body.message, sid, body.mode.value)
    # 응답에는 클라이언트가 보낸 원래 session_id 를 그대로 되돌려준다(내부 키 비노출)
    return reply_envelope(reply, body.session_id, body.mode.value, (time.time() - t0) * 1000)


@router.post("/clarify", summary="되묻기 응답 처리")
def assistant_clarify(body: ClarifyRequest = Body(...),
                      user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """대기 중인 되묻기에 후보를 골라 답한다.

    후보를 특정하지 못하면 **임의로 고르지 않는다** — ok=false 로 다시 물어본다.
    (모델을 잘못 고르면 다른 제품의 인증 자료를 자신 있게 답하게 된다.)
    """
    _warm()
    sid = _scoped(user, body.session_id)
    sess = session.get(sid)
    pending = sess.pending_clarification
    if not pending:
        raise HTTPException(status_code=409, detail={
            "message": "이 세션에는 대기 중인 되묻기가 없습니다.",
            "session_id": body.session_id,
            "hint": "POST /api/assistant/chat 응답의 needs_clarification=true 일 때만 사용합니다."})
    cands = list(pending.get("candidates") or [])
    chosen = session.resolve_choice(body.choice, cands)
    if not chosen:
        return {"ok": False, "resolved": None, "candidates": cands,
                "session_id": body.session_id, "mode": body.mode.value,
                "needs_clarification": True,
                "answer": (f"{chat_config.GREETING} 선택하신 항목을 특정하지 못했습니다. "
                           f"후보 중 하나를 정확히 알려주십시오: {', '.join(cands)}"),
                "message": "후보를 특정하지 못했습니다."}
    t0 = time.time()
    reply = _guard(engine.chat, body.choice, sid, body.mode.value)
    out = reply_envelope(reply, body.session_id, body.mode.value, (time.time() - t0) * 1000)
    out["resolved"] = chosen
    out["original_message"] = pending.get("message")
    return out


@router.post("/reset", summary="대화 맥락 비우기")
def assistant_reset(body: Dict[str, str] = Body(default={}),
                    user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """직전 모델·되묻기 대기 상태를 버린다. 세션 키가 없으면 아무 일도 하지 않는다."""
    sid = _scoped(user, (body or {}).get("session_id", ""))
    if sid:
        session.STORE.drop(sid)
    return {"ok": True, "session_id": (body or {}).get("session_id", "")}


# ─────────────────────────────────────────
#  상태
# ─────────────────────────────────────────
def _bundle_status() -> Dict[str, Any]:
    """데이터 번들 파일 존재 여부. '왜 답이 비었는가'를 화면에서 바로 보기 위한 것."""
    files = {
        "문서 manifest": cert_loader.DOC_MANIFEST,
        "패밀리 manifest": cert_loader.FAMILY_MANIFEST,
        "KC manifest": cert_loader.KC_MANIFEST,
        "자료보유매트릭스": cert_loader.MODEL_MATRIX,
        "자료요청이력": cert_loader.REQUEST_LEDGER,
        "RoHS요청이력": cert_loader.ROHS_HISTORY,
        "드라이브 폴더맵": cert_loader.DRIVE_FOLDERS,
        "제품 마스터(마스킹본)": str(chat_config.MASTER_PATH),
    }
    out = []
    for label, path in files.items():
        p = Path(path)
        exists = p.exists()
        out.append({"label": label, "path": str(path), "exists": exists,
                    "size": (p.stat().st_size if exists else 0)})
    img = Path(chat_config.IMAGE_DIR)
    return {
        "repo": cert_loader.REPO,
        "files": out,
        "missing": [f["label"] for f in out if not f["exists"]],
        # 4GB 제품 이미지는 번들에 넣지 않는다 — 없으면 응답에서 이미지 링크만 빠진다
        "image_dir": {"path": str(img), "exists": img.exists(),
                      "note": "없어도 정상입니다. 응답에서 이미지 URL 줄만 빠집니다."},
    }


@router.get("/health", summary="KB 로딩 · 마스터 로딩 · LLM 가용 · 번들 경로")
def assistant_health(user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """프론트가 탭을 열 때 제일 먼저 부른다 — 이 호출이 예열(_warm)을 겸한다."""
    _warm()
    kb_state = dict(_STATE.get("kb") or {})
    master_state = dict(_STATE.get("master") or {})
    bundle = _bundle_status()

    problems: List[str] = list(_STATE.get("errors") or [])
    problems += [f"인증KB: {p}" for p in (kb_state.get("problems") or [])]
    if not kb_state.get("loaded"):
        problems.append("인증KB 미로드 — 인증 문의에 답할 수 없습니다.")
    if not master_state.get("loaded"):
        problems.append("제품 마스터 미로드 — 상담이력·제품분석 경로가 동작하지 않습니다.")
    for label in bundle["missing"]:
        problems.append(f"데이터 번들 누락: {label}")

    try:
        drive_state = assistant_drive.status()
    except Exception as exc:                     # noqa: BLE001
        drive_state = {"error": f"{type(exc).__name__}: {exc}"}
    if not drive_state.get("service_account_configured"):
        problems.append(
            "드라이브 서비스 계정 미설정 — 파일을 서버에서 직접 내려줄 수 없어 폴더 링크로만 "
            "안내합니다. GOOGLE_SERVICE_ACCOUNT_JSON 을 설정하고 인증자료 폴더를 그 계정에 "
            "뷰어로 공유하십시오.")
    elif not drive_state.get("folder_readable"):
        problems.append(
            "드라이브 서비스 계정은 설정됐으나 인증자료 폴더를 읽지 못합니다 — "
            f"폴더({drive_state.get('folder_id')})를 {drive_state.get('client_email')} 에 "
            "뷰어로 공유했는지 확인하십시오.")

    return {
        "ok": not problems,
        "cert_kb": kb_state,
        "master": master_state,
        "bundle": bundle,
        "drive": drive_state,
        "llm": {
            "available": llm.available(),
            "api_key_env": chat_config.LLM_API_KEY_ENV,
            "api_key_present": bool(os.environ.get(chat_config.LLM_API_KEY_ENV)),
            "model": chat_config.LLM_MODEL,
            "note": ("LLM 은 문장 다듬기 보조입니다. 키가 없어도 챗봇 전체가 규칙 기반으로 "
                     "정상 동작합니다(기본 동작)."),
        },
        "boot_sec": _STATE.get("boot_sec"),
        "warmed": _STATE.get("warmed_at") is not None,
        "sessions": len(session.STORE),
        "modes": [m.value for m in Mode],
        "badge_legend": [dict(v, kind=k) for k, v in BADGE_SPEC.items()],
        "route_labels": ROUTE_LABEL,
        "problems": problems,
    }


# ─────────────────────────────────────────
#  근거 직접 조회
# ─────────────────────────────────────────
@router.get("/spec", summary="제품 사양 참고 정보(화이트리스트 + 오염 플래그)")
def assistant_spec(model: str = Query(..., min_length=1, description="모델명. 예: LS-300HO"),
                   user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """★ 이 정보는 제품 이미지 한 장을 보고 만든 AI 추정이다. 확정 사양이 아니다.

    `usable_as_spec=false` 면 사양 답변 근거로 쓰면 안 되고, `cert_contaminated=true` 면
    원본 제품분석에 근거 없는 인증 주장이 있었다는 뜻이다(본 응답에서는 문장 단위로 삭제됨).
    """
    _warm()
    ctx = _guard(spec.spec_context, model)
    frags = _guard(spec.cert_fragments, ctx.get("model") or model)
    return {
        "ok": True,
        "model": ctx.get("model"),
        "requested": model,
        "found": ctx.get("found"),
        "has_analysis": ctx.get("has_analysis"),
        "usable_as_spec": ctx.get("usable_as_spec"),
        "verification": ctx.get("verification"),
        "verification_reason": ctx.get("verification_reason"),
        "cert_contaminated": ctx.get("cert_contaminated"),
        "cert_claims_removed": frags,
        "dropped_cert": ctx.get("dropped_cert"),
        "quant_flags": ctx.get("quant_flags"),
        "blocked_fields": ctx.get("blocked_fields"),
        "excluded_fields": ctx.get("excluded_fields"),
        "fields": ctx.get("fields"),
        "rendered": spec.render_spec(ctx),
        "image": ctx.get("image"),
        "category": ctx.get("category"),
        "product_name": ctx.get("product_name"),
        "source": chat_config.SOURCE_SPEC,
        "ai_generated": True,
        "notices": ctx.get("notices"),
        "warning": ("제품분석은 제품 이미지 기반 AI 추정입니다. 확정 사실이 아니며 "
                    "인증·자료 문의의 근거로 사용해서는 안 됩니다."),
    }


@router.get("/history", summary="모델별 과거 상담 이력(담당자 직접 확인용)")
def assistant_history(model: str = Query(..., min_length=1, description="모델명. 예: LS-UC314"),
                      query: Optional[str] = Query(None, description="주면 유사도 순으로 정렬"),
                      include_siblings: bool = Query(True, description="형제 모델 이력 포함"),
                      limit: int = Query(200, ge=1, le=2000),
                      user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """PII 는 인덱스 빌드 시점에 이미 마스킹돼 있다. URL 은 인용 직전에 제거한다.

    `quotable=false` 인 행은 그대로 고객에게 복사하면 안 된다(시점 의존 정보 등).
    """
    _warm()
    key = history.normalize_model_key(model)
    if query:
        hits = _guard(history.search_qa, chat_router.mask_models(query).strip(), key,
                      limit=limit, include_siblings=include_siblings)
        ranked = True
    else:
        hits = _guard(history.history_for_model, key, include_siblings)[:limit]
        ranked = False
    rows = []
    for h in hits:
        row = h.as_dict()
        row["answer_clean"] = history.clean_answer(row["answer"])
        row["answer_cert_stripped"], row["cert_dropped"] = chat_config.strip_cert_claims(
            row["answer_clean"])
        rows.append(row)
    return {
        "ok": True, "model": key, "requested": model,
        "ranked_by_query": ranked, "query": query,
        "count": len(rows), "exact_rows": history.model_row_count(key),
        "include_siblings": include_siblings,
        "source": chat_config.SOURCE_HISTORY,
        "notices": [chat_config.HISTORY_NOTICE] if rows else [],
        "rows": rows,
    }


@router.get("/file/{doc_id}", summary="드라이브 링크/로컬 경로 해석")
def assistant_file(doc_id: str, mode: Mode = MODE_Q,
                   user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """doc_id 는 /chat 응답 files[].doc_id 값이다.

    `allowed=false` 는 오류가 아니라 정책 판정이다(단독 전달 불가 자료 등) — 200 으로 준다.
    """
    _warm()
    ref = _guard(cert_cli.file_ref, doc_id, mode.value)
    if ref is None:
        raise HTTPException(status_code=404, detail={
            "message": f"문서를 찾을 수 없습니다: {doc_id}",
            "hint": "doc_id 는 /api/assistant/chat 응답의 files[].doc_id 값입니다."})
    return {"ok": True, **ref}


@router.get("/file/{doc_id}/download", summary="PDF 원본 내려받기")
def assistant_file_download(doc_id: str, mode: Mode = MODE_Q,
                            user: dict = Depends(get_current_user)):
    """서버가 드라이브에서 받아 그대로 흘려보낸다.

    담당자가 890개 파일이 든 폴더를 눈으로 뒤지지 않게 하려는 것이 목적이다.
    직원에게 드라이브 권한을 따로 주지 않아도 되는 부수 효과도 있다(챗봇 로그인이 곧 권한).
    자격증명이 없으면 503 + 폴더 링크 — 오류가 아니라 '아직 설정 안 됨' 안내다.
    """
    from fastapi.responses import StreamingResponse
    from urllib.parse import quote

    _warm()
    ref = _guard(cert_cli.file_ref, doc_id, mode.value)
    if ref is None:
        raise HTTPException(status_code=404, detail={
            "message": f"문서를 찾을 수 없습니다: {doc_id}"})
    if not ref.get("allowed"):
        raise HTTPException(status_code=403, detail={
            "message": "이 자료는 현재 모드에서 파일 제공이 제한됩니다.",
            "hint": ref.get("reason") or "고객 모드에서는 자료요청서 접수 후 제공합니다."})

    from cert_lookup import drive as cert_drive

    file_info = ref.get("file") or {}
    dref = cert_drive.DriveRef(
        status=file_info.get("status", ""), filename=file_info.get("filename", ""),
        folder_id=file_info.get("folder_id", ""), folder_url=file_info.get("folder_url", ""),
        url=file_info.get("url", ""), file_id=file_info.get("file_id", ""),
        path=file_info.get("path", ""))
    stream, size = cert_drive.get_resolver().open_stream(dref)
    if stream is None:
        raise HTTPException(status_code=503, detail={
            "message": "서버에서 파일을 직접 내려받을 수 없습니다.",
            "hint": "드라이브 서비스 계정이 설정되지 않았거나 해당 폴더에 접근 권한이 없습니다. "
                    "아래 폴더 링크에서 파일명으로 찾아 주십시오.",
            "folder_url": dref.folder_url, "filename": dref.filename})

    name = dref.filename or f"{doc_id}.pdf"
    headers = {"Content-Disposition":
               f"attachment; filename*=UTF-8\'\'{quote(name)}"}
    if size:
        headers["Content-Length"] = str(size)

    def _chunks():
        try:
            while True:
                buf = stream.read(64 * 1024)
                if not buf:
                    break
                yield buf
        finally:
            try:
                stream.close()
            except Exception:                    # noqa: BLE001
                pass

    media = "application/pdf" if name.lower().endswith(".pdf") else "application/octet-stream"
    return StreamingResponse(_chunks(), media_type=media, headers=headers)
