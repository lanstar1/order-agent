# 랜스타 사내 영업·CS 상담 챗봇

모델명이 섞인 한국어 문의 한 줄을 받아 **무엇을 근거로 한 답인지 라벨을 붙여** 돌려주는
사내 전용 상담 보조. FastAPI REST API + 단일 파일 웹 UI.

```
사용자 문장
   │
   ├─ 0단계 라우터 (chatbot/router.py)  인증·자료 의도인가?
   │      │
   │      ├─ cert   → cert_lookup 이 **유일한 근거**. 응답 원문 그대로 출력. LLM 미사용.
   │      ├─ mixed  → 두 경로를 각각 태워 **섹션으로 분리**. 근거를 한 문단에 섞지 않는다.
   │      └─ legacy → 1단계 상담이력(모델 스코프 필수) → 2단계 제품분석(화이트리스트)
   │                  → 둘 다 없으면 담당자 에스컬레이션
   └─ 응답 = 본문 + 주의문구(notices) + 근거 배지(badges) + 파일 위치(files)
```

**LLM 없이 전체가 동작하는 것이 기본 동작이다.** `ANTHROPIC_API_KEY` 는 선택이며, 있으면
legacy 경로의 문장 다듬기에만 쓴다(§LLM).

---

## 1. 기동

의존성 없이 동작 확인(표준 라이브러리만):

```bash
cd /Users/lanstar/mail2
python3 -c "from chatbot import chat; print(chat('LS-6UTPD-3MG RoHS 인증서 주세요').text)"
python3 -m unittest tests.test_chatbot tests.test_cert_lookup
```

API 서버 + 웹 UI:

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn chatbot.api:app --host 0.0.0.0 --port 8080
```

| 주소 | 내용 |
|---|---|
| `http://localhost:8080/` | 사내 웹 UI(단일 HTML, 외부 CDN 없음) |
| `http://localhost:8080/docs` | OpenAPI 문서 |
| `http://localhost:8080/health` | KB·마스터·LLM 상태 |
| `http://localhost:8080/cert/` | 인증 조회 API(`cert_lookup.api`) 를 그대로 마운트 |

인덱스는 **기동 시 1회** 로드한다(인증KB 890문서 + 제품마스터 6.3MB, 실측 0.5초).
마운트된 `/cert` 서브앱의 lifespan 은 Starlette 가 실행하지 않으므로 `chatbot.api` 의
lifespan 이 KB 를 열어 `cert_app.state.kb` 에 같은 인스턴스를 넣어 준다.

전체 테스트(143개):

```bash
python3 -m unittest tests.test_chatbot tests.test_api tests.test_cert_lookup
```

`tests/test_api.py` 는 fastapi/httpx 가 없으면 통째로 skip 한다 — 코어 테스트는 의존성
없는 PC 에서도 돌아야 한다.

---

## 2. 엔드포인트

### 챗봇

| 메서드 | 경로 | 설명 |
|---|---|---|
| `GET` | `/` | 사내 웹 UI (HTML) |
| `GET` | `/api` | 엔드포인트 목록 + 배지 범례 |
| `GET` | `/health` | KB 로딩 / 마스터 로딩 / LLM 가용 / `problems[]` |
| `POST` | `/chat` | `{message, session_id?, mode?}` → 응답 봉투(아래) |
| `POST` | `/clarify` | `{session_id, choice, mode?}` 되묻기 응답 처리 |
| `GET` | `/history?model=&query=&include_siblings=&limit=` | 그 모델의 과거 상담 이력(담당자 직접 확인용) |
| `GET` | `/spec?model=` | 제품분석(화이트리스트 적용 + 오염 플래그) |
| `GET` | `/models/suggest?text=` | 상품명으로 모델 후보 제안(자동 선택 금지) |
| `GET` | `/route?message=` | 0단계 라우팅 판정만(디버깅) |
| `GET` | `/session/{session_id}` | 직전 모델·되묻기 대기 상태 |

### 인증 조회 (마운트)

`GET /cert/health` · `GET /cert/lookup` · `GET /cert/lookup/{model}/{doc_type}` ·
`GET /cert/coverage` · `GET /cert/kc` · `GET /cert/kc/{model}/contains` ·
`GET /cert/file/{doc_id}` · `POST /cert/ask` — 상세는 `cert_lookup/README.md`.

### `POST /chat` 응답 봉투

```jsonc
{
  "ok": true,
  "answer": "랜스타입니다. …",   // 사람이 읽는 본문. API 는 이 문자열을 재작성하지 않는다
  "text":   "…",                // answer 와 동일(호환용)
  "route": "cert|legacy|mixed",
  "route_label": "인증·기술자료",
  "route_reason": "HARD 자료유형 ['RoHS']",
  "badges": [ { "key":"cert", "label":"인증KB", "level":"strong",
                "hint":"…", "count":1, "models":["LS-6UTPD-3MG"], "sections":[] } ],
  "sources": [ … ],             // 배지의 원본. kind = 상담이력 / 제품분석(이미지 기반 AI) / …
  "notices": [ "※ 스캔 이미지 문서라 …" ],
  "needs_clarification": false,
  "candidates": [],             // 되묻기 후보(모델명)
  "files": [ { "doc_id","doc_type","title_ko","filename","path","url","folder_url",
               "status","deliverable","quotable","text_extractable" } ],
  "models": ["LS-6UTPD-3MG"],
  "model_source": "message|gazetteer|session|clarification|none",
  "llm_used": false,
  "elapsed_ms": 3.3,
  "data": { … }                 // 근거 원본(cert 조회 결과, 상담이력 히트, 사양 컨텍스트)
}
```

### 되묻기 흐름

```
POST /chat  {"message":"LS-HD2 인증서 주세요","session_id":"s1"}
  → needs_clarification=true, candidates=["LS-HD21","LS-HD21R","LS-HD21-1M,1.5M,2M,3M"]
POST /clarify {"session_id":"s1","choice":"2번"}      # '2번' / '두번째' / 'LS-HD21R' 모두 인식
  → resolved="LS-HD21R", 원래 문의를 그 모델로 다시 처리한 응답
```

* 후보를 특정하지 못하면 **임의로 고르지 않는다** → `200 {"ok":false, "needs_clarification":true}`.
  (모델을 잘못 고르면 다른 제품의 인증 자료를 자신 있게 답하게 된다.)
* 대기 중인 되묻기가 없는 세션에 `/clarify` 를 호출하면 `409`.
* `session_id` 를 비우면 상태 없는 1회성 응답이 되고 되묻기 이어받기가 불가능하다.

---

## 3. 근거 배지 — 이 UI 의 핵심

담당자가 **무엇을 근거로 만든 문장인지** 모른 채 고객에게 복사하는 것을 막기 위한 장치다.
`sources[].kind` → `badges[]` 변환은 서버(`chatbot/api.py: BADGE_SPEC`)가 한 곳에서만 한다.

| 배지 | `sources[].kind` | level | 뜻과 취급 |
|---|---|---|---|
| **인증KB** | `인증KB(cert_lookup)` | strong | 메일·드라이브에서 실제로 확보한 인증·기술자료. **인증 문의의 유일한 근거.** 이 배지가 붙은 응답 본문은 조회 엔진 원문 그대로이며 요약·재작성 금지 |
| **상담이력** | `상담이력` | weak | 2023~2025년 쇼핑몰 문의/카카오톡 상담 기록. 과거 시점 사실이므로 재고·배송·가격은 그대로 쓰면 안 된다 |
| **제품분석(AI추정)** | `제품분석(이미지 기반 AI)` | ai | **제품 이미지 한 장을 보고 만든 AI 추론.** 확정 사양이 아니다(§5) |
| **LLM생성** | `LLM생성` | ai | 문장 다듬기에만 LLM 사용. 근거 원문이 `— 근거 원문 —` 아래에 항상 함께 남는다 |
| **규칙기반** | `규칙기반` | weak | LLM 없이 규칙으로 조립. 되묻기·에스컬레이션 응답도 여기 |

* `mixed` 라우트에서는 각 배지에 `sections:[1]`(인증) / `[2]`(일반)이 붙는다.
* 웹 UI 는 **인증KB 배지가 없고 제품분석 배지만 있는 응답**의 본문에 주황색 세로선을 그어
  "확정 사양 아님"을 시각적으로 구분한다.

### 주의문구(notices)

`notices[]` 는 `cert_lookup/policy.py` 와 `chatbot/config.py` 가 강제하는 문구다
(O* 추론매핑 / 자재 묶음 / 스캔 문서 인용 불가 / 만료 / 과거 상담 / AI 추정 / 시점 의존).
UI 는 이것을 **본문 위 별도 박스**에 다시 보여준다. 본문에서 떼어내지는 않는다 —
정책이 붙인 문구가 UI 버그 하나로 사라지면 안 되기 때문이다(같은 문구가 본문 끝에도 남는다).

---

## 4. 모드: internal / customer

| | `internal` (기본) | `customer` |
|---|---|---|
| 대상 | 사내 영업·CS 담당자 | 고객에게 그대로 노출 |
| 파일 경로 | 로컬/드라이브 경로 표시 | **숨김** (`files[].path` 없음) |
| O* 추론매핑 | 표시 + 주의문구 필수 | 숨김(대체 문구) |
| 단독 전달 불가 자료 | 목록에 표시 + 경고 | 필터링 |

* 이 챗봇은 **사내 전용**으로 설계됐다. `internal` 응답에는 로컬 파일 경로가 그대로
  들어가므로 화면·응답을 고객에게 그대로 전달하면 안 된다(웹 UI 상단에 상시 경고 표시).
* 모드 오타는 **조용히 통과하지 않는다.** `'customerr'`, `'Internal'`, `''` 는 전부 `422`.
  (`'customerr'` 를 `internal` 로 흡수하면 고객에게 사내 전용 정보가 나간다.)

---

## 5. ★ 제품분석은 이미지 기반 AI 추정이다 — 확정 사실이 아니다

`lanstar_product_master.json` 의 `제품분석` 필드(제품유형·외형·재질·포트구성·주요사양·
사용방법·사용용도·소구점)는 **제품 이미지 한 장을 보고 AI 가 작성한 추론**이다.
실제로 틀린 사례가 확인됐다.

* `LS-300HO` — 실제로는 사방이 뚫린 **오픈랙(벽부형 허브랙)** 인데
  "로봇 청소기 충전 스테이션 / 도킹 스테이션, 먼지통·물탱크 내장, 자동 세척·건조"로 적혀 있다.
* `LS-AS301`(3:1 HDMI 선택기) → "외장형 SSD", `LS-C5U305BK`(305M 랜케이블) → "멀티탭".
* 제품유형이 틀리면 주요사양 61.4% / 사용방법 61.4% / 사용용도 59.1% / 소구점 59.1% 가
  같이 틀린다 → "제품유형만 빼고 나머지는 안전" 전략은 성립하지 않는다.

그래서 `chatbot/spec.py` 가 두 개의 게이트를 건다.

1. **인증 환각 게이트** — 제품분석의 인증 주장 581건 중 KB 와 실제로 일치한 것은 29건(5.0%).
   검증 시도 없이 **문장 단위로 삭제**한다(`config.strip_cert_claims`).
2. **제품 오인식 게이트** — AI 제품유형을 사람이 쓴 제품명·카테고리와 교차검증한다.
   `conflict` 면 8개 필드 전부를 버리고, `unknown`(검증 불가, 36.7%)이면 참고용 표시를 붙인다.
   `소구점`·`사용방법` 은 검증 결과와 무관하게 **항상 제외**한다.

**따라서 응답·API 에서 제품분석 계열 정보는 언제나 "참고 정보"로만 다뤄야 하고,
확정 사실처럼 단정해서는 안 된다.** `GET /spec` 응답의 `ai_generated`,
`usable_as_spec`, `verification`, `warning` 필드가 이 판정을 그대로 노출한다.

### 인증 질문에 제품분석을 절대 쓰지 않는 이유

`소구점`·`재질`·`주요사양` 에 "RoHS 인증", "UL 인증" 같은 문구가 있는 모델이 519개인데,
상당수는 인증KB coverage 가 `X`(자료 없음)다. 이걸 컨텍스트에 넣으면 **없는 자료를 있다고
답하는 사고**가 난다. 그래서 라우터가 인증·자료 의도로 판정하면 **cert_lookup 만이 근거**이고,
제품분석·상담이력은 그 경로에 아예 태우지 않는다.

> 실측 검증: 오염 모델 519개 × 질의 4종 = **2,076 API 호출 → 근거 누출 0건**
> (`sources` 전건 인증KB 단독, `data` 에 `spec`/`history_hits` 없음).

---

## 6. LLM (선택)

```bash
export ANTHROPIC_API_KEY=sk-ant-...          # 없어도 된다
export LANSTAR_CHAT_MODEL=claude-sonnet-5    # 기본값
pip install anthropic
```

* 키가 없거나 `anthropic` 패키지가 없거나 API 가 죽어도 **예외를 올리지 않는다.**
  `llm.available()` 이 `False` 가 되고 규칙 기반 응답이 나간다.
  `GET /health` 의 `llm.available` 로 확인할 수 있고, 키가 없어도 `ok:true` 다.
* **인증(cert) 경로에는 LLM 을 태우지 않는다.** 정책이 붙인 주의문구를 LLM 이 요약하면
  사라지기 때문이다.
* legacy 경로에서만 근거 블록을 사람 문장으로 다듬고, 결과 아래에 `— 근거 원문 —` 으로
  원본을 항상 함께 남긴다. LLM 출력에 인증 어휘가 섞이면 출력단에서 폐기하고 규칙 기반으로
  되돌린다(`llm.output_is_safe`).
* 실측: LLM 없이 실제 상담 질문 5,313건 × internal 모드 → 예외 0 / 인사말 누락 0 / 7.9초.

---

## 7. 모듈

| 파일 | 역할 |
|---|---|
| `config.py` | 경로·임계값·환경변수, PII 마스킹, 인증 문구 삭제, 세그먼트 분할 |
| `router.py` | 0단계 의도 분기(HARD/SOFT 자료유형, 드라이버·미지원자료 라벨) |
| `history.py` | 1단계 상담이력 — 문자 2+3-gram IDF 코사인. **`search_qa(query, model)` 는 model 필수** |
| `spec.py` | 2단계 제품분석 — 화이트리스트 + 이중 게이트 + 정량 검증 |
| `llm.py` | 선택적 LLM. 전 실패 경로에서 `None` |
| `session.py` | TTL 세션, 지시어 해석, 되묻기 후보 선택 |
| `engine.py` | `chat(message, session_id, mode) -> ChatReply` |
| `api.py` | FastAPI. 배지 계산 · 파일 목록 추출 · `/cert` 마운트 |
| `web/index.html` | 단일 파일 사내 웹 UI(인라인 CSS/JS, 라이트/다크, 반응형) |

### 환경변수

| 이름 | 기본값 | 용도 |
|---|---|---|
| `LANSTAR_MASTER_PATH` | `/Users/lanstar/lanmart/lanstar_product_master.json` | 제품 마스터 |
| `LANSTAR_IMAGE_DIR` / `LANSTAR_IMAGE_URL_BASE` | `…/product_images` / `https://lanmart.co.kr/shop/product/` | 제품 이미지 |
| `LANSTAR_CHAT_MODEL` / `LANSTAR_CHAT_MAX_TOKENS` / `LANSTAR_CHAT_TIMEOUT` | `claude-sonnet-5` / `800` / `30` | LLM |
| `LANSTAR_CHAT_TTL` | `1800` | 세션 TTL(초) |
| `ANTHROPIC_API_KEY` | (없음) | 있으면 LLM 사용 |
| `CERT_KB_CORS_ORIGINS` | `*` | 외부 노출 시 좁힐 것 |
| `CERT_KB_TODAY` | (없음) | 만료 판정 기준일(테스트 전용) |

---

## 8. 알려진 제약

* **사내망 전용으로 운영할 것.** `internal` 모드 응답에 로컬 파일 경로가 그대로 들어간다.
  외부 노출 시 `CERT_KB_CORS_ORIGINS` 를 좁히고 앞단에 인증을 두어야 한다(현재 인증 없음).
* **드라이버·설치파일·펌웨어, 카탈로그·품질보증서·단종확인서·MFi** 는 인증KB 에 유형 자체가
  없어 에스컬레이션 문구로만 처리한다. 실제 담당자 연결(메일·티켓)은 붙어 있지 않다.
* 세션은 프로세스 메모리 + TTL 이다. 재시작하면 비는 것이 정상이며 워커를 2개 이상 띄우면
  되묻기 이어받기가 워커 간에 공유되지 않는다(단일 워커 권장, 또는 세션 스토어 외부화 필요).
* 라우터 정밀도 94.8% / 재현율 100%(상담 5,313건 전수). 오탐 3건은
  `원산지가 중국인가요`, `매뉴얼이 정보량이 적다`, `품번변경 자료있을까요` 유형이다.
* 상담이력 검색은 **모델 스코프 필수**다. 모델을 특정하지 못하면 검색하지 않고 되묻는다
  (모델 없이 검색하면 top-1 의 78.5%가 다른 모델이다).
* `POST /chat` 의 `files[].url` 은 드라이브 API 자격증명이 있을 때만 채워진다. 현재는
  로컬 동기화 경로(`status:"local_path"`)로 폴백하며, 이는 정상 동작이다.
