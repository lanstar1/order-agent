# AI 발주서 자동화 시스템 - Synology NAS 배포 가이드

## 준비물

- Synology DS718+ (Docker 패키지 설치 필요)
- order-agent 프로젝트 폴더 전체
- PC에서 NAS에 SSH 접속 가능 (또는 File Station 사용)

---

## 1단계: NAS에 Docker 패키지 설치

1. DSM 웹 접속 (http://NAS주소:5000)
2. **패키지 센터** → "Docker" 검색 → **설치**
3. 설치 완료 후 Docker 아이콘이 메인 메뉴에 나타남

---

## 2단계: 프로젝트 파일을 NAS에 복사

### 방법 A: File Station (GUI)

1. DSM → **File Station** → `docker` 공유폴더 열기
2. `order-agent` 폴더째 업로드 (드래그앤드롭)
3. 최종 경로: `/volume1/docker/order-agent/`

### 방법 B: SSH (터미널)

```bash
# PC에서 NAS로 복사 (Mac/Linux)
scp -r ./order-agent admin@NAS주소:/volume1/docker/

# Windows (PowerShell)
scp -r .\order-agent admin@NAS주소:/volume1/docker/
```

### 폴더 구조 확인

```
/volume1/docker/order-agent/
├── .env                ← API 키 설정 (중요!)
├── Dockerfile
├── docker-compose.yml
├── backend/
├── frontend/
└── data/
    ├── products/
    │   └── products.csv
    ├── order_agent.db   ← (자동 생성됨)
    └── uploads/
```

---

## 3단계: .env 파일 확인

`.env` 파일에 아래 키들이 설정되어 있는지 확인:

```
ANTHROPIC_API_KEY=sk-ant-xxxxx
GOOGLE_API_KEY=AIzaSyxxxxx
ERP_COM_CODE=89356
ERP_USER_ID=TIGER
ERP_ZONE=CD
ERP_API_KEY=1d667exxxxx
```

---

## 4단계: Docker 이미지 빌드 및 실행

### NAS에 SSH 접속

```bash
ssh admin@NAS주소
```

> DSM → 제어판 → 터미널 및 SNMP → "SSH 서비스 활성화" 체크 필요

### 빌드 및 실행

```bash
cd /volume1/docker/order-agent

# 이미지 빌드 (최초 1회, 약 2~3분)
sudo docker-compose build

# 컨테이너 시작 (백그라운드)
sudo docker-compose up -d

# 상태 확인
sudo docker-compose ps

# 로그 확인
sudo docker-compose logs -f --tail=50
```

### 정상 동작 확인

```bash
# NAS 내부에서 테스트
curl http://localhost:8000/api/health
```

브라우저에서 `http://NAS주소:8000` 접속 → 대시보드 표시되면 성공!

---

## 5단계: 외부 접속 설정 (3곳 사무실 접근)

### 방법 A: Synology QuickConnect (가장 간단)

1. DSM → **제어판** → **외부 액세스** → **QuickConnect**
2. QuickConnect 활성화 + ID 설정 (예: `lanstar-nas`)
3. **고급 설정** → 포트포워딩 규칙에 **포트 8000** 추가

> 접속: `http://QuickConnect주소:8000`
> QuickConnect는 HTTP 리버스 프록시라 포트 직접 접속이 안 될 수 있음 → 방법 B 권장

### 방법 B: DDNS + 공유기 포트포워딩 (추천)

**① NAS DDNS 설정**

1. DSM → **제어판** → **외부 액세스** → **DDNS**
2. 추가 → 서비스 제공자: **Synology** 선택
3. 호스트 이름 설정 (예: `lanstar.synology.me`)

**② 공유기 포트포워딩**

1. 공유기 관리페이지 접속 (보통 192.168.0.1)
2. 포트포워딩 설정:
   - 외부 포트: `8000`
   - 내부 IP: NAS 내부 IP (예: 192.168.0.100)
   - 내부 포트: `8000`
   - 프로토콜: TCP

**③ 접속 테스트**

```
http://lanstar.synology.me:8000
```

3곳 사무실 모두 이 주소로 접속 가능!

### 방법 C: Synology 역방향 프록시 (HTTPS 적용)

보안 강화가 필요하면:

1. DSM → **제어판** → **로그인 포털** → **고급** → **역방향 프록시**
2. 생성:
   - 소스: `https://order.lanstar.synology.me` (포트 443)
   - 대상: `http://localhost:8000`
3. DSM → **보안** → **인증서** → Let's Encrypt 무료 인증서 발급

---

## 운영 명령어

```bash
# SSH 접속 후
cd /volume1/docker/order-agent

# 컨테이너 중지
sudo docker-compose down

# 컨테이너 재시작
sudo docker-compose restart

# 코드 업데이트 후 재빌드
sudo docker-compose build --no-cache
sudo docker-compose up -d

# 로그 실시간 확인
sudo docker-compose logs -f

# 메모리 사용량 확인
sudo docker stats order-agent
```

---

## NAS 부팅 시 자동 시작

Docker 패키지가 설치되어 있으면 `restart: unless-stopped` 설정에 의해 NAS 재부팅 후 자동으로 컨테이너가 시작됩니다. 별도 설정 불필요.

---

## 문제 해결

| 증상 | 해결 |
|------|------|
| 빌드 실패 | `sudo docker-compose build --no-cache` 재시도 |
| 포트 8000 접속 안 됨 | `sudo docker-compose ps`로 상태 확인, `logs`로 에러 확인 |
| 외부에서 접속 안 됨 | 공유기 포트포워딩 확인 + 방화벽에서 8000 허용 |
| 메모리 부족 | `docker stats`로 확인, 필요시 RAM 증설 |
| DB 초기화 필요 | `data/order_agent.db` 삭제 후 재시작 |
| 상품 CSV 업데이트 | `data/products/products.csv` 교체 후 재시작 |
