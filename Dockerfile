# ── 경량 Python 이미지 (RAM 절약) ──
FROM python:3.11-slim

# 시스템 패키지 최소 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리
WORKDIR /app

# 의존성 먼저 설치 (캐시 활용)
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt python-dotenv

# 소스 코드 복사
COPY backend/ /app/backend/
COPY frontend/ /app/frontend/

# data 디렉토리 복사 (products.csv 포함)
COPY data/ /app/data/

# 업로드/피드백 디렉토리 보장
RUN mkdir -p /app/data/uploads /app/data/feedback

# 포트
EXPOSE 8000

# 헬스체크
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 서버 실행 (reload 없이 - 프로덕션)
WORKDIR /app/backend
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
