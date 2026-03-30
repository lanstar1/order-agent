#!/bin/bash
# ============================================
#  SmartStore API 로컬 테스트 서버
#  더블클릭으로 서버 시작 + 브라우저 자동 오픈
# ============================================

# 스크립트 위치로 이동
cd "$(dirname "$0")"

echo "============================================"
echo "  🧪 SmartStore API Test Server"
echo "============================================"
echo ""

# .env 파일 확인
if [ ! -f ".env" ]; then
    echo "⚠️  .env 파일이 없습니다!"
    echo ""
    echo "아래 내용으로 .env 파일을 생성하세요:"
    echo "  NAVER_COMMERCE_CLIENT_ID=여기에_클라이언트ID"
    echo "  NAVER_COMMERCE_CLIENT_SECRET=여기에_클라이언트시크릿"
    echo ""
    echo ".env.example 파일을 참고하세요."
    echo ""
    read -p "Enter를 누르면 종료합니다..."
    exit 1
fi

# Python 확인
if command -v python3 &> /dev/null; then
    PYTHON=python3
elif command -v python &> /dev/null; then
    PYTHON=python
else
    echo "❌ Python이 설치되어 있지 않습니다!"
    read -p "Enter를 누르면 종료합니다..."
    exit 1
fi

# 필요한 패키지 확인 및 설치
echo "📦 패키지 확인 중..."
$PYTHON -c "import fastapi, uvicorn, httpx, bcrypt, dotenv" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "📦 필요한 패키지를 설치합니다..."
    $PYTHON -m pip install fastapi uvicorn httpx bcrypt python-dotenv --quiet
fi

echo ""
echo "🚀 서버 시작 중... (http://localhost:8888)"
echo "   종료: Ctrl+C 또는 터미널 창 닫기"
echo ""

# 2초 후 브라우저 자동 오픈
(sleep 2 && open "http://localhost:8888") &

# 서버 실행
$PYTHON test_smartstore.py
