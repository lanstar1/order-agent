# 상담봇 (assistant 탭) — order-agent 운영 문서

order-agent 안에서 `cert_lookup`(인증·기술자료 조회 엔진) + `chatbot`(상담 엔진)을
**새 탭 하나**로 노출한 것에 대한 운영 문서다. 엔진 내부 설계는 각 패키지의 README 를 본다.

- `backend/cert_lookup/README.md` — 인증·기술자료 조회 엔진 (드라이브 폴백 4단 포함)
- `backend/chatbot/README.md` — 상담 챗봇 코어 (0단계 라우팅 / 상담이력 / 제품분석 게이트)

---

## 1. 구성

| 무엇 | 어디 |
|---|---|
| 라우터 | `backend/api/routes/assistant.py` |
| 등록 | `backend/main.py` — `_HAS_ASSISTANT` 가드 + `app.include_router(assistant_router)` |
| 경로 기본값 | `backend/config.py` 말미 (`os.environ.setdefault`) |
| 엔진 | `backend/cert_lookup/`, `backend/chatbot/` |
| 데이터 번들 | `data/assistant/` (약 9.6MB, 이미지에 포함) |
| 번들 생성기 | `backend/scripts/prepare_assistant_data.py` |
| 화면 | `frontend/index.html` `#page-assistant` + `frontend/static/js/app.js` `initAssistantPage()` |

### 엔드포인트 (전부 JWT 필수 — `Depends(get_current_user)`)

```
POST /api/assistant/chat        {message, session_id?, mode?}   상담 한 턴
POST /api/assistant/clarify     {session_id, choice, mode?}     되묻기 후보 선택
POST /api/assistant/reset       {session_id}                    대화 맥락 비우기
GET  /api/assistant/health                                      KB·마스터·LLM·번들 상태
GET  /api/assistant/spec        ?model=                         제품분석(AI추정)
GET  /api/assistant/history     ?model=&query=&limit=           모델별 과거 상담이력
GET  /api/assistant/file/{doc_id}?mode=                         드라이브 링크/경로 해석
```

`mode` 는 `internal` | `customer` **두 값만** 받는다. 오타는 조용히 넘어가지 않고 422 다
(`customerr` 를 `internal` 로 흡수하면 고객에게 사내 전용 정보가 나간다 — fail closed).

---

## 2. ★ 기존 `자료검색` 탭과 무엇이 다른가

**둘은 다른 자료다. 절대 섞지 말 것.**

| | 자료검색 (기존) | 상담봇 (신규) |
|---|---|---|
| 백엔드 | `/api/materials/drive/*` | `/api/assistant/*` |
| 원본 | Google Drive 폴더 `103m-Rj22HUpWyEwUlWle3FuUBnU6Y2hl` | 인증자료 폴더 `1_MpX-bxWIvFuS1u8CM2rhUjdU9SbRKm1` |
| 저장 | SQLite `drive_documents` 테이블 (동기화 필요) | `data/assistant/` 정적 번들 (manifest) |
| 하는 일 | 파일명 검색 → 파일 목록 | 자연어 질의 → 보유/미보유 판정 + 근거 + 파일 위치 |
| 판정 | 없음(있는 파일만 보여줌) | 있음(자재 커버리지·단독전달 가부·스캔 인용 가부) |

같은 종류의 문서라도 두 탭이 서로 다른 파일을 가리킬 수 있다. **의도된 것이다.**
한쪽 결과를 다른 쪽 근거로 인용하지 말고, 통합하지도 말 것.

---

## 3. 근거 배지의 의미

응답 카드 머리에 붙는 배지는 **서버가 계산해서** `badges[]` 로 내려준다
(정의는 `chatbot/api.py` 의 `BADGE_SPEC` 한 곳뿐 — 화면에서 다시 판정하지 않는다).

| 배지 | 색 | 뜻 | 고객에게 그대로 써도 되나 |
|---|---|---|---|
| **인증KB** | 초록 | `cert_lookup` 인증·기술자료 KB. 확정 근거. | ○ (주의문구 확인 후) |
| **상담이력** | 노랑 | 2023~2025 과거 상담 기록. | △ 현재 사양·정책과 다를 수 있음 |
| **제품분석(AI추정)** | 빨강 | **제품 이미지 한 장을 보고 만든 AI 추론.** | ✕ 확정 사양이 아님 |
| **LLM생성** | 보라 | 문장 다듬기에만 LLM 사용. 근거 원문은 함께 표시됨. | △ |
| **규칙기반** | 회색 | LLM 없이 규칙으로 조립. | △ |

`×2` 같은 숫자는 그 근거가 몇 건 쓰였는지다. 배지에 마우스를 올리면 근거 모델·섹션이 보인다.

### ★★ 제품분석은 확정 사양이 아니다

`제품분석(AI추정)` 배지가 붙은 내용은 **제품 이미지를 AI가 보고 추론한 것**이다.
실측 오류율이 제품유형 기준 10~17% 이고, 제품유형이 틀리면 나머지 필드도 같은 환각 위에서
작성돼 최대 61% 까지 전파된다. 그래서 코어가 다음을 강제한다.

- 화이트리스트 필드만 노출 (전체 필드를 그대로 뿌리지 않는다)
- 제품유형 교차검증 게이트 (사람 표기와 충돌하면 사양 근거로 쓰지 않는다)
- **제품분석에 들어 있던 인증 주장은 문장 단위로 삭제한다** — 실측 95%가 근거 없음
  (581건 중 KB `O` 일치 29건)

**인증·자료 문의의 근거로는 절대 쓰지 않는다.** 라우터가 인증 의도를 감지하면
상담이력·제품분석 단계를 아예 **건너뛴다**(점수 필터가 아니라 호출 차단). 화면에서
`route=cert` 응답에 제품분석을 덧붙이는 식으로 이 구조를 깨지 말 것.

### 주의문구(notices)

본문과 **분리된 노란 박스**로 전부 띄운다. 요약·생략 금지.
정책상 본문 문자열은 조회 엔진 원문 그대로 출력하므로 같은 문구가 본문 끝에도 남아 있을 수
있다 — 중복은 의도된 것이고, 박스는 "한 번 더" 보여주는 것이지 본문의 대체가 아니다.

### 파일 카드 플래그

| 플래그 | 뜻 |
|---|---|
| `단독 전달 불가` | 이 자료만 따로 보내면 안 된다. 고객 제공 전 담당자 확인 필요. |
| `스캔 — 원문 인용 불가` | 스캔 이미지라 텍스트 추출 불가. 파일 전달은 되지만 수치·문구는 원본 확인 필요. |
| `인용 불가` | 시점 의존 정보 등으로 그대로 복사하면 안 되는 행. |

`customer` 모드에서는 파일 **경로가 아예 내려오지 않는다**(정책). 파일 카드가 비어 보이는
것은 버그가 아니다.

---

## 4. 환경변수

**필수는 사실상 0개다.** `backend/config.py` 가 `os.environ.setdefault()` 로 기본값을
심어 두므로 아무것도 설정하지 않아도 번들을 본다. 아래는 덮어쓰고 싶을 때만.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `ASSISTANT_DATA_DIR` | `<repo>/data/assistant` | 번들 루트. 아래 두 개가 여기서 파생된다. |
| `CERT_KB_REPO` | `ASSISTANT_DATA_DIR` | manifest 3종·CSV 3종·drive_folders·`_lib` 경로가 전부 파생 |
| `LANSTAR_MASTER_PATH` | `<번들>/lanstar_product_master.json` | 상담이력 인덱스 + 제품분석 (★ 마스킹본) |
| `LANSTAR_IMAGE_DIR` | `<번들>/product_images` | 제품 이미지 4GB. **번들에 없다.** 없으면 응답에서 이미지 URL 줄만 빠지고 정상 동작. |
| `ANTHROPIC_API_KEY` | Render 기설정 | 없으면 규칙 기반 폴백(정상 동작). LLM 은 문장 다듬기 보조일 뿐. |
| `LANSTAR_CHAT_MODEL` | `claude-sonnet-5` | 상담봇 LLM 모델 |
| `GOOGLE_API_KEY` / `CERT_KB_DRIVE_API_KEY` | — | 드라이브 파일 링크 조회용 |
| `CERT_KB_DRIVE_TIMEOUT` | `4` | 드라이브 API 타임아웃(초) |
| `CERT_KB_DRIVE_DISABLE_API` | — | `1` 이면 드라이브 API 를 아예 호출하지 않는다 |
| `LANSTAR_CHAT_TTL` | `1800` | 대화 세션 TTL(초) |

### 드라이브 링크에 대해

파일 위치 해석은 **4단 폴백**이다: 서비스계정/토큰 → API 키 → 로컬 동기화 폴더 → 폴더 URL.
API 키로는 **"링크가 있는 모든 사용자"로 공유된 폴더·파일만** 읽힌다. 비공개면 403/404 가
나고 오류 없이 다음 단으로 내려간다(3연속 실패 시 회로 차단 — 상담 응답이 느려지지 않도록).
Render 에는 로컬 동기화 폴더가 없으므로, 링크가 필요하면 서비스 계정(뷰어 공유)을 쓰는 편이
낫다. 링크가 없어도 파일명·경로·보유 판정은 그대로 나온다.

---

## 5. 데이터 번들 갱신 (manifest 를 다시 만들었을 때)

번들은 원본 저장소(`/Users/lanstar/mail2`)에서 **생성해서 커밋**하는 정적 자산이다.
운영 서버에서 만들지 않는다.

```bash
# 1) 원본 저장소에서 manifest 를 재생성한 뒤(각 저장소 절차대로),
# 2) order-agent 저장소에서 번들을 다시 만든다
cd <order-agent>
python3 backend/scripts/prepare_assistant_data.py

# 검증만 하고 싶으면 (재생성 없음)
python3 backend/scripts/prepare_assistant_data.py --check

# 소스 경로가 다르면
LANSTAR_MASTER_SRC=/path/to/lanstar_product_master.json \
CERT_KB_SRC_REPO=/path/to/mail2 \
python3 backend/scripts/prepare_assistant_data.py
```

스크립트는 멱등하고, 아래 4가지를 검증해 하나라도 실패하면 **exit 1** 이다.

1. 마스킹본 상담내역에서 PII 패턴 잔존 **0건**
2. 제품분석 8필드가 원본과 **문자 단위 동일**
3. 모델 수 / 상담내역 건수가 원본과 동일
4. 그대로 복사한 파일들의 SHA256 이 원본과 동일

생성 메타와 전 파일 SHA256 은 `data/assistant/BUNDLE.json` 에 남는다.

### ★★ 원본 마스터를 그대로 넣지 말 것

`lanstar_product_master.json` 상담내역 원문에는 **다른 고객의 배송주소·주문번호·휴대폰·
이메일·계좌번호**가 그대로 들어 있다(실측 695건: 휴대폰 189 / 운송장 116 / 주소 105 /
이메일 101 / 주문번호 107 / 이름 32 / 계좌 30 / 전화 13 / 사업자 2).
반드시 `prepare_assistant_data.py` 가 만든 마스킹본만 커밋한다.
제품분석 필드는 **한 글자도 건드리지 않는다** — spec 게이트가 원문 기준으로 캘리브레이션돼
있어서 손대면 판정이 어긋난다.

번들을 갱신한 뒤 배포 전에 확인:

```bash
python3 backend/scripts/prepare_assistant_data.py --check   # exit 0 이어야 한다
curl -H "Authorization: Bearer <JWT>" .../api/assistant/health | jq '.ok, .problems'
```

---

## 6. 운영 메모

### 예열 (콜드스타트)

`main.py` 의 startup 훅에 예열을 **넣지 않았다**. 실측 최초 로드 0.58초
(KB 0.03 + 상담이력 인덱스 0.42 + 마스터 0.03 + import 0.10).
startup 에 넣으면 상담봇을 쓰지 않는 배포에서도 매 콜드스타트가 0.58초 느려진다.
대신 화면이 탭을 열 때 `GET /api/assistant/health` 를 먼저 부르고, 그 호출이 예열을 끝낸다.
사용자가 첫 질문을 타이핑하는 사이에 로딩이 끝나므로 체감 지연이 없다.
예열 이후는 프로세스 캐시라 실측 chat 0.087초 → 0.001초.

인덱스를 다시 읽게 하려면 **프로세스를 재시작**한다(재배포). 런타임 리로드 API 는 없다.

### 세션

메모리 딕셔너리 + TTL 30분. `--workers 1` 전제다. 워커를 늘리면 대화 맥락(직전 모델·되묻기)이
요청마다 다른 워커로 흩어져 되묻기가 깨진다 — 늘리려면 세션 저장소를 먼저 외부화해야 한다.
저장 키는 `{emp_cd}:{session_id}` 로 **사원별 격리**한다(같은 session_id 문자열을 두 직원이
보내도 서로의 맥락을 이어받지 않는다).

### 진단 순서

1. `GET /api/assistant/health` → `ok` / `problems` / `bundle.missing`
2. `bundle.missing` 이 비어 있지 않다 → `data/assistant/` 가 이미지에 안 들어갔다.
   `.dockerignore` 와 Dockerfile 의 `COPY data/ /app/data/` 확인.
3. `cert_kb.loaded=false` → 인증 문의에 답할 수 없다. `CERT_KB_REPO` 확인.
4. `master.loaded=false` → 상담이력·제품분석 경로가 죽는다. `LANSTAR_MASTER_PATH` 확인.
5. `llm.available=false` 는 **정상 동작**이다(규칙 기반 폴백). 장애가 아니다.

### 라우터 모듈이 죽어도 order-agent 는 뜬다

`main.py` 가 `from api.routes.assistant import router` 를 try/except 로 감싼다
(기존 sourcing 모듈과 같은 관례). 번들 누락·import 오류가 나면 상담봇 탭만 404 가 되고
나머지 기능은 그대로 동작한다. 로그에 `상담봇 모듈 로드 실패` 가 남는다.

### 프론트 캐시

`frontend/index.html` 의 `app.js?v=` / `api.js?v=` 버전을 올리지 않으면 브라우저가 옛 JS 를
계속 쓴다. 화면 코드를 고쳤으면 **반드시 버전을 올린다.**
