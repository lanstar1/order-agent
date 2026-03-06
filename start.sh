#!/bin/bash
echo "======================================="
echo "   AI 발주서 자동화 시스템 시작"
echo "======================================="

# 스크립트 위치로 이동
cd "$(dirname "$0")"

# Python 확인
if ! command -v python3 &> /dev/null; then
    echo "[오류] python3가 없습니다. https://www.python.org 에서 설치해주세요."
    exit 1
fi
echo "Python: $(python3 --version)"

# 가상환경 생성 (최초 실행 시)
if [ ! -d "venv" ]; then
    echo "[1/3] 가상환경 생성 중..."
    python3 -m venv venv
fi

# 가상환경 활성화
source venv/bin/activate

# 패키지 설치
echo "[2/3] 패키지 설치 중..."
pip install -r backend/requirements.txt -q

# .env 확인
if [ ! -f ".env" ]; then
    echo "[주의] .env 파일이 없습니다. .env.example을 복사합니다..."
    cp .env.example .env
    echo "ANTHROPIC_API_KEY를 .env 파일에 입력 후 다시 실행하세요."
    open .env 2>/dev/null || nano .env
    exit 1
fi

# 환경변수 로드
export $(grep -v '^#' .env | xargs)

# 서버 시작
echo "[3/3] 서버 시작..."
echo ""
echo "  접속 주소: http://localhost:8000"
echo "  종료: Ctrl+C"
echo ""
cd backend
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
