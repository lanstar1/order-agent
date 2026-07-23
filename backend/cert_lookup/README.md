# cert_lookup — 랜스타 제품 인증·기술자료 조회

모델명 하나로 **어떤 인증·기술자료를 갖고 있는지**, **파일이 어디 있는지**, **고객에게
그대로 보내도 되는지**를 답한다. 근거는 저장소의 manifest 3종이며, 없는 자료는 만들어내지
않고 "미보유 + 공급사 요청 필요"로 답한다.

| 색인 | 건수 | 파일 |
|---|---|---|
| 문서 | 890 | `제품인증자료/manifest.jsonl` |
| 제품군 | 427 | `제품인증자료/manifest_family.jsonl` |
| KC 적합등록 | 118 | `KC적합등록자료/manifest.jsonl` |
| RoHS 요청이력 | 33 | `제품인증자료/RoHS요청이력.csv` |

---

## 1. 빠른 시작

### CLI (설치 불필요 — 표준 라이브러리만)

```bash
cd /Users/lanstar/mail2

python3 -m cert_lookup.cli LS-6UTPD-3MG RoHS       # 자료유형 지정
python3 -m cert_lookup.cli LS-7UTP                 # 보유현황 요약
python3 -m cert_lookup.cli LS-OVERC --kc           # KC 자료
python3 -m cert_lookup.cli LS-OVERC --contains 필증 # zip 안 열고 포함 여부
python3 -m cert_lookup.cli --ask "LS-HDAOC-30M 로하스 성적서 주세요"
python3 -m cert_lookup.cli --file b5728b739902     # 파일 위치 해석
python3 -m cert_lookup.cli LS-1600HQ CE --mode customer
python3 -m cert_lookup.cli LS-6UTPD-3MG RoHS --json
python3 -m cert_lookup.cli --selftest              # 필수 8개 케이스
```

주요 옵션: `--mode internal|customer` · `--intent files|content|kc|coverage` · `--json` ·
`--today YYYY-MM-DD`(만료 판정 기준일, 테스트 전용 — API 에는 이 파라미터가 없다).

### API 서버

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn cert_lookup.api:app --host 0.0.0.0 --port 8080
```

`http://localhost:8080/docs` 에 Swagger UI, `/` 에 엔드포인트 목록이 있다.
인덱스는 **기동 시 1회** 읽고 프로세스 내내 재사용한다(요청마다 재파싱하지 않는다).

---

## 2. 엔드포인트

모든 응답은 같은 봉투다 — 사람이 읽을 `answer`(문자열)와 기계가 쓸 `data`(구조화 JSON)를
**둘 다** 담는다.

```jsonc
{
  "ok": true,
  "kind": "type",              // type | family | kc | kc_contains | content
                               // | ambiguous | not_found | need_model
  "query": "LS-6UTPD-3MG",
  "mode": "internal",
  "found": true,
  "needs_clarification": false, // true 면 candidates 로 되물어야 한다
  "candidates": [],             // 되묻기 후보 (제품군이 여럿일 때)
  "suggestions": [],            // 모델 미발견 시 유사 모델
  "notices": ["※ …"],          // 반드시 함께 노출해야 하는 주의문구
  "answer": "랜스타입니다. …",
  "data": { }                   // 렌더러가 만든 상세 (documents 는 최대 6건)
}
```

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/health` | 인덱스 규모·드라이브 상태·로딩 문제 |
| GET | `/lookup?model=&mode=&doc_type=&intent=` | 제품군 해석 + 보유현황 + 문서 목록 + 응답문 |
| GET | `/lookup/{model}/{doc_type}` | 자료유형 지정 조회. 자재 묶음이면 묶음 전체 + 미확보 자재 |
| GET | `/coverage?model=` | 유형별 `O / O* / 자재 / 자재* / X` 요약 매트릭스 |
| GET | `/kc?model=` | KC 완료자료 + `contents[]` 내부 서류 목록 |
| GET | `/kc/{model}/contains?item=필증` | zip 을 열지 않고 포함 여부 |
| GET | `/file/{doc_id}` | 드라이브 링크 / 로컬 경로 해석 |
| POST | `/ask` | 자연어 한 줄 → 위 기능으로 라우팅 (챗봇 단일 진입점) |

공통 쿼리 파라미터 `mode` 는 기본 `internal`. **`internal`/`customer` 외의 값은 422 로
거절한다** — 오타(`customerr`)를 internal 로 흡수하면 고객에게 사내 전용 정보가 나가기 때문.

### 예시

```bash
curl 'localhost:8080/lookup?model=LS-6UTPD-3MG'
curl 'localhost:8080/lookup/LS-HDAOC-30M/RoHS'
curl 'localhost:8080/coverage?model=LS-1600HQ'
curl 'localhost:8080/kc?model=LS-OVERC'
curl -G --data-urlencode 'item=필증' 'localhost:8080/kc/LS-OVERC/contains'
curl 'localhost:8080/file/b5728b739902'
curl -X POST localhost:8080/ask -H 'Content-Type: application/json' \
     -d '{"message":"LS-6UTPD-3MG RoHS 인증서 주세요","mode":"internal"}'
```

### 응답 규칙 세 가지

* **되묻기는 에러가 아니다.** `LS-SHE` 처럼 제품군이 17개로 갈리면 HTTP **200** +
  `needs_clarification: true` + `candidates[]` 로 답한다.
* **모델 미발견도 404 가 아니다.** HTTP **200** + `found: false` + `suggestions[]`.
  챗봇이 "혹시 이 모델인가요?"로 이어가야 하기 때문이다.
  (404 는 `/file/{doc_id}` 에 없는 doc_id 를 넣었을 때만 — 사용자가 타이핑하는 값이 아니다.)
* **자료유형 오타는 400.** `known_doc_types` 목록을 함께 돌려준다.

### 봉투 밖 추가 필드

`/lookup` 계열은 `data.documents`(응답문에 실린 최대 6건, 파일 위치 포함)와 별도로
최상위 `documents` 에 **정책 필터를 통과한 전체 문서 목록**을 싣는다. 전체 목록에는 파일
위치를 해석하지 않는다(문서마다 파일시스템을 두드리게 되므로) — 필요한 건만
`/file/{doc_id}` 로 따로 해석한다.

### 경로에 못 넣는 값

* `MSDS/SDS` 는 슬래시 때문에 경로에 못 쓴다 → `/lookup/{model}/MSDS` 로 요청한다.
* 제품군 키 `LS-FMD / LS-FMP` 처럼 이름에 슬래시가 든 경우가 하나 있다. 경로 라우팅은
  `{model:path}` 로 처리해 두었지만, 확실하게 하려면 쿼리 형식
  `/lookup?model=LS-FMD / LS-FMP&doc_type=RoHS` 를 쓴다.


### 만료 판정 기준일

API 에는 `today` 파라미터가 **없다**. 과거 날짜를 넣으면 만료 경고가 통째로 사라져
만료된 인증서가 유효한 것처럼 응답되기 때문이다. 테스트에서 시점을 바꿔야 하면
환경변수 `CERT_KB_TODAY=2020-01-01` 로 서버를 기동한다.

### 추정 매칭

`prefix` 로 붙은 결과(`LS-300HO` → `LS-300H`)는 **확정으로 답하지 않는다**.
`data.confident=false` 와 함께 응답문에 "추정 매칭입니다 — 동일 제품 확인 필요"가
반드시 붙는다. 커넥터 성별 변형(`LS-SER-9MFF` 등)은 아예 매칭하지 않고 유사 모델을 제안한다.


### 공급사 표기법 해석

제품군 키에는 공급사가 메일에 쓴 표기가 그대로 들어와 있어, 실판매 모델명으로는 도달할 수
없는 자료가 있었다(`LS-HF7` 시리즈 인증 12건이 `LS-HF7XX` 키에만 매달려 있는 식). 파이프라인
중간 산출물이 남아 있지 않아 manifest 를 재생성할 수 없으므로, **조회 시점에 표기를 해석한다.**
데이터와 `_lib/model_family.py` 는 건드리지 않으므로 manifest 를 재생성해도 그대로 동작한다.

| 표기 | 키 예시 | 도달 가능해진 질의 |
|---|---|---|
| 자릿수 와일드카드 | `LS-HF7XX`, `LS-EXT3XX` | `LS-HF710` (1건 → 20건) |
| 길이 와일드카드 | `LS-7SD-BKXM` | `LS-7SD-BK2M` (1건 → 10건) |
| 범위 | `LS-HDMI-EXT-10~15M`, `LS-HDMT-0.3-5M` | `LS-HDMI-EXT-15M` (0건 → 1건) |
| 콤마 열거 | `LS-HD21-1M,1.5M,2M,3M` | `LS-HD21-1.5M` |
| SERIES | `LS-HF SERIES`, `LS-LKPG SERIES` | `LS-LKPG-BK` |
| 슬래시 | `LS-FMD / LS-FMP` | 두 모델 각각 |
| 점 표기 중복키 | `LS-SSTP-CAT.6A-300M` | `LS-SSTP-CAT6A-300M` (1건 → 13건) |

매칭 경로는 `notation` 으로 표시되며, 공급사가 적용범위를 직접 적은 것이라 확정으로 취급한다.
범위는 경계를 지킨다 — `LS-HDMI-EXT-50M` 은 `10~15M`·`20~30M` 어디에도 붙지 않는다.

### 자료 요청 이력

`제품인증자료/자료요청이력.csv`(발송메일 전수 판독, 455행)를 함께 싣는다. 미보유 응답에
**요청 이력이 있으면 일자·수신처와 함께** 안내한다. 이력이 없을 때 "요청한 적 없다"고
단정하지는 않는다 — 판독 대상이 발송메일 전체가 아니므로 그렇게 말할 근거가 없다.

---

## 3. `/ask` 자연어 라우팅

```jsonc
POST /ask
{ "message": "LS-6UTPD-3MG RoHS 인증서 주세요",
  "mode": "internal",
  "override_model": null, "override_doc_type": null }
```

추출 결과는 응답의 `parsed` 에 그대로 실린다.

* **모델명** — `cert_lookup.resolver.MODEL_RX`. 기존 챗봇의
  `r'(LS[P]?-[A-Za-z0-9\-\.]+)'` 를 `LSN-` 접두와 비-LS 모델(`DP12MM-10M`, `OM4-*`,
  `USB2.0-*`)까지 넓힌 것으로, 제품군 키 427개 + KC 모델 118개를 **전부** 잡는다.
  대소문자·언더스코어·접두 하이픈 누락(`ls_6utpd 3mg`, `LS6UTPD`)은 정규화가 흡수한다.
* **자료유형** — `RoHS/로하스`, `CE/CE인증서`, `원산지증명서→CoC`, `데이터시트/사양서→Data
  sheet`, `플루크/채널테스트→Fluke test`, `MSDS/SDS`, `REACH/리치` 등.
  `"RoHS 성적서"` 의 `성적서` 는 앞 유형을 꾸미는 말로 보고 별개 유형(`Test report`)으로
  세지 않는다. `"시험성적서 주세요"` 처럼 단독으로 쓰이면 `Test report` 로 잡는다.
* **의도** — `KC`·`적합등록`·`필증` → KC 조회 / `수치`·`함량`·`얼마`·`기재` → 인용 가능
  여부 안내 / `어떤 자료`·`보유현황` → 보유현황 요약 / 그 외 → 자료 안내.
* 모델을 못 찾으면 `kind: "need_model"` + `needs_clarification: true` 로 되묻는다.

---

## 4. internal / customer 모드

| | `internal` (기본) | `customer` |
|---|---|---|
| 대상 | 사내 영업·CS 담당자 | 고객 직접 노출 |
| 추론 매핑 `O*`/`자재*` 자료 | **노출** + 주의문구 강제 | 목록에서 제외, "담당자 확인 후 회신" |
| 파일 위치(드라이브 링크·로컬 경로) | 노출 | 노출 안 함 → 자료요청서 양식 안내 |
| 단독 전달 불가(`deliverable=false`) | 보유 사실은 표시(전달은 차단) | 목록에서 제외 |
| 자재·부품 성적서 원본 | 전달 가능 | 완제품 단위 문서만 |

두 모드 공통 하드 게이트(정책으로 강제, 모드로 못 푼다):

* `deliverable=false` → 파일 링크 금지
* 출처 매핑이 반박된 문서(`attribution_rejected`) → 근거에서 제외
* 스캔 문서(`text_extractable=false`) → **원문 인용 금지**(파일 전달만 가능)
* `O*` / `자재*` → "대표 모델 시험 기반이며 적용범위 확인 필요" 주의문구 동반
* `coverage = X` → "확보된 자료 없음 → 공급사 요청 필요". RoHS 는 요청이력으로 사유를
  가른다(`no_request_history` = 미요청 / `requested_not_provided` = 요청했으나 미제공)
* 자재 성적서 묶음을 완제품 인증서처럼 답하지 않는다. `caveat_ko` 는 **가공 없이 그대로**

---

## 5. 드라이브 자격증명 (없어도 동작한다)

파일 위치는 **드라이브 자격증명 → 드라이브 API 키 → 로컬 동기화 폴더 → 폴더 URL** 순으로
4단 폴백하며, `DriveRef.status` 로 어느 단계가 답했는지 항상 알 수 있다.

| status | 뜻 |
|---|---|
| `api_link` | 드라이브 API 로 파일 직링크를 만들었다 |
| `local_path` | 동기화 폴더에서 실물을 찾았다 (자격증명 없을 때의 정상 경로) |
| `folder_fallback` | 파일을 못 찾아 폴더 URL 만 안내한다 |
| `missing` | 전달할 파일 자체가 없다 |

manifest 에는 **일부러 file ID 가 없다**(재업로드하면 바뀌므로). 안정적인 폴더 ID + 파일명만
있어서 링크는 런타임에 만든다.

### 자격증명을 붙이려면 (선택)

```bash
pip install google-api-python-client google-auth google-auth-oauthlib
export CERT_KB_DRIVE_TOKEN=/path/to/token.json          # OAuth 사용자 토큰
# 또는
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json  # 서비스 계정
```

필요 스코프는 `drive.readonly` 하나. 서비스 계정을 쓸 때는 대상 폴더 4개를 그 계정
이메일에 **뷰어로 공유**해야 한다.

| 폴더 | ID |
|---|---|
| 문서 | `1UqdvlWWvhBIXUAkaFZLzsRYUJi6b66Ii` |
| KC | `1bZ0BJv3HOiXp9yjROAJ7i5L5LPzDDbYf` |
| _미확정 | `11bp2bgzxD5p0crEythRLCvYlHhCsD3iF` |
| KC_필증PDF만 | `1Tev66aFBUvkFD5VA_Qb_hieRvZatsso8` |

자격증명이 없거나 google 라이브러리가 없으면 **조용히** 로컬 폴백으로 내려간다(예외 없음).
현재 이 PC 는 동기화 폴더가 있어 전건 `local_path` 로 해석된다.
`/health` 의 `drive.sync_root_found` / `drive.api_credentials` 로 확인할 수 있다.

### API 키만 있을 때 (order-agent 통합 경로)

자격증명이 없어도 `GOOGLE_API_KEY`(또는 전용 `CERT_KB_DRIVE_API_KEY`)가 있으면
Drive v3 `files.list` 를 **표준 라이브러리 urllib 로 직접** 호출한다. `googleapiclient` 도
`httpx` 도 필요 없다 — 이 패키지의 '표준 라이브러리만' 원칙을 유지하기 위한 선택이다.

```bash
export GOOGLE_API_KEY=AIza...          # order-agent 에 이미 설정돼 있음
export CERT_KB_DRIVE_TIMEOUT=4         # 선택, 초 단위(기본 4)
```

> ⚠️ **API 키로는 '링크가 있는 모든 사용자' 로 공유된 폴더·파일만 읽힌다.**
> API 키에는 OAuth 주체가 없어서 비공개 항목은 존재조차 보이지 않는다(403/404).
> 위 표의 4개 폴더를 링크 공유로 열지 않으면 이 단계는 항상 빈손으로 끝나고 —
> 오류 없이 — 로컬 동기화 폴더 → 폴더 URL 순으로 그대로 폴백한다.
> 사내 자료를 링크 공유로 여는 것이 부담이라면 서비스 계정(뷰어 공유) 쪽을 쓰는 편이 낫다.

키가 틀렸거나 폴더가 비공개면 조회마다 왕복이 생기므로, **연속 실패 3회면 그 프로세스에서는
API 키 단계를 끈다**(응답 지연 방지). `CERT_KB_DRIVE_DISABLE_API=1` 은 자격증명·API 키
두 단계를 모두 건너뛴다.

---

## 6. 환경변수

| 변수 | 기본값 | 용도 |
|---|---|---|
| `CERT_KB_REPO` | 이 패키지의 상위 디렉터리 | 저장소 루트 |
| `CERT_KB_DOC_MANIFEST` | `제품인증자료/manifest.jsonl` | 문서 색인 |
| `CERT_KB_FAMILY_MANIFEST` | `제품인증자료/manifest_family.jsonl` | 제품군 색인 |
| `CERT_KB_KC_MANIFEST` | `KC적합등록자료/manifest.jsonl` | KC 색인 |
| `CERT_KB_ROHS_HISTORY` | `제품인증자료/RoHS요청이력.csv` | 미보유 사유 근거 |
| `CERT_KB_DRIVE_FOLDERS` | `제품인증자료/drive_folders.json` | 폴더 ID |
| `CERT_KB_LIB_DIR` | `제품인증자료/_lib` | `model_family.py` 위치 |
| `CERT_KB_DRIVE_ROOT` | 자동 탐색 | 동기화 루트 직접 지정 |
| `CERT_KB_DRIVE_ROOT_NAME` | `랜스타_인증자료` | 동기화 폴더 이름 |
| `CERT_KB_DRIVE_TOKEN` / `GOOGLE_APPLICATION_CREDENTIALS` | 없음 | 드라이브 API 자격증명 |
| `CERT_KB_DRIVE_API_KEY` / `GOOGLE_API_KEY` | 없음 | 드라이브 API 키(**링크 공유된 항목만** 조회 가능) |
| `CERT_KB_DRIVE_TIMEOUT` | `4` | API 키 호출 타임아웃(초) |
| `CERT_KB_DRIVE_DISABLE_API` | 없음 | `1` 이면 자격증명·API 키 단계를 모두 건너뛴다 |
| `CERT_KB_CERT_DIR` | `<CERT_KB_REPO>/제품인증자료` | 인증자료 디렉터리(하위 경로가 전부 여기서 파생) |
| `CERT_KB_KC_DIR` | `<CERT_KB_REPO>/KC적합등록자료` | KC 디렉터리 |
| `CERT_KB_MODEL_MATRIX` | `제품인증자료/자료보유매트릭스.csv` | 모델 단위 보유 마크 |
| `CERT_KB_REQUEST_LEDGER` | `제품인증자료/자료요청이력.csv` | 자료요청 대장 |
| `CERT_KB_CORS_ORIGINS` | `*` | 쉼표 구분 허용 오리진. 사내망 밖에 노출하면 반드시 좁힐 것 |

`CERT_KB_CORS_ORIGINS` 가 `*` 이면 `allow_credentials` 는 자동으로 꺼진다(브라우저가
`*` + credentials 조합을 거부하므로).

---

## 7. 구성

```
cert_lookup/
  loader.py    경로 상수 · manifest 적재 · 인덱스 · 모듈 레벨 캐시
  resolver.py  모델명 정규화 → 제품군/KC 해석 (_lib/model_family.py 를 import 해 사용)
  search.py    보유 판정 (coverage 마크는 재계산하지 않고 그대로 읽는다)
  policy.py    internal/customer 노출 규칙 + 주의문구 상수
  drive.py     파일 위치 3단 폴백
  render.py    한국어 응답 생성 ("랜스타입니다." 톤, caveat_ko 무가공)
  cli.py       터미널 조회 + 자연어 파서 (표준 라이브러리만)
  api.py       FastAPI 계층 (얇은 전달자 — 판단하지 않는다)
```

`api.py` 는 `cli.py` 의 자연어 파서와 직렬화 헬퍼를 가져다 쓴다. 반대 방향 의존은 없으므로
**fastapi 가 없어도 CLI 는 그대로 동작한다.**

엔진 코어(`loader`~`render`)는 표준 라이브러리만 쓴다. google 라이브러리는 있으면 쓰고
없으면 폴백한다.

---

## 8. 검증

```bash
python3 -m cert_lookup.cli --selftest    # 명세서 5장 필수 8개 케이스
```

| 입력 | 기대 |
|---|---|
| `LS-6UTPD-3MG` | LS-6UTPD 제품군 자료 |
| `LS-RGB-15MM` | LS-RGB 로 병합되지 **않음** |
| `LS-LKPG` | `LS-LKPG SERIES` 로 연결 |
| `LS-1600HQ` RoHS | **미보유 + 공급사 요청 필요** (RoHS 문서 0건. `O*` 주의문구는 CE 에 붙는다) |
| `LS-HDAOC-30M` RoHS | 자재 묶음 7건 + **절연 미확보** |
| `LS-OVERC` KC | zip + 내부 서류 6건 |
| `LS-HDMI-AD20-1M` RoHS | 미보유 + 2020-12 요청 이력 3건 제시 |
| 스캔 문서 내용 질문 | 원문 인용 불가 안내 |

> 명세서 5장 테스트표와 4장 예시응답의 `LS-1600HQ RoHS = 추론 매핑` 은 데이터와 다르다.
> 실제 coverage 는 `RoHS=X, CE=O*` 이고 RoHS 문서는 0건이다. **데이터대로** 답한다.

---

## 9. 알려진 한계

* 드라이브 API 경로는 자격증명이 없어 **코드 경로만** 검증했다. 실제 `files.list` 응답은
  미검증이며 현재는 전건 `local_path` 로 해석된다.
* KC zip 내부 문서 **본문은 색인돼 있지 않다**. 필증 번호·유효기간·시험규격은 파일을 직접
  열어야 한다(`/kc/{model}/contains` 는 파일명 기준 판정이다).
* 890건 중 **222건이 스캔 문서**라 내용 인용이 불가능하다(`quotable: false`).
* 제품 마스터(`lanstar_product_master.json`)는 **연결하지 않았다.** 제품분석의
  `소구점/재질` 에 KB 의 `coverage=X` 와 충돌하는 인증 문구가 62건 있어 인증 응답 경로에
  넣으면 "없는 자료를 만들지 않는다" 규칙이 깨진다. 사양 질의용으로 쓰려면 상위 계층에서
  화이트리스트로 주입할 것.
* `resolve()` 의 `prefix` 분기는 색상·길이 접미사를 관대하게 흡수한다(`LS-6UTPDD` →
  `LS-6UTPD`). 응답의 `data.confident` / `match_reason` 으로 판별할 수 있다.
