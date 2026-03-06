"""
Order Agent - FastAPI 메인 앱
"""
import asyncio
import logging
import time
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import sys
sys.path.insert(0, str(Path(__file__).parent))

from db.database import init_db
from api.routes.orders import router as orders_router
from api.routes.customers import router as customers_router
from api.routes.auth import router as auth_router
from api.routes.settings import router as settings_router
from api.routes.inventory import router as inventory_router
from api.routes.sale_orders import router as sale_orders_router
from api.routes.materials import router as materials_router
from api.routes.training import router as training_router
from api.routes.orderlist import router as orderlist_router

# ─────────────────────────────────────────
#  로깅 설정
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  앱 초기화
# ─────────────────────────────────────────
app = FastAPI(
    title="AI 발주서 자동화 시스템",
    description="거래처 발주서를 AI로 자동 분석하여 ECOUNT ERP 판매 전표를 생성합니다.",
    version="0.2.0",
    docs_url="/docs" if __import__("config").DEBUG else None,
    redoc_url=None,
)

# CORS 설정 (환경변수에서 허용 도메인 지정)
from config import ALLOWED_ORIGINS, DEBUG
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization"],
)


# ─────────────────────────────────────────
#  보안 헤더 미들웨어
# ─────────────────────────────────────────
@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)

    # 보안 헤더 추가
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if not DEBUG:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

    # 요청 처리 시간 로깅 (API 엔드포인트만)
    if request.url.path.startswith("/api/"):
        duration = (time.time() - start_time) * 1000
        if duration > 5000:  # 5초 이상이면 경고
            logger.warning(
                f"[Slow API] {request.method} {request.url.path} "
                f"- {duration:.0f}ms"
            )

    return response


# ─────────────────────────────────────────
#  글로벌 예외 핸들러
# ─────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"[Unhandled] {request.method} {request.url.path}: {exc}", exc_info=True)
    detail = str(exc) if DEBUG else "서버 내부 오류가 발생했습니다."
    return JSONResponse(status_code=500, content={"detail": detail})


# 라우터 등록
app.include_router(orders_router)
app.include_router(customers_router)
app.include_router(auth_router)
app.include_router(settings_router)
app.include_router(inventory_router)
app.include_router(sale_orders_router)
app.include_router(materials_router)
app.include_router(training_router)
app.include_router(orderlist_router)

# AI 대시보드 라우터
try:
    from api.routes.dashboard import router as dashboard_router
    app.include_router(dashboard_router)
except ImportError:
    pass

# 정적 파일 (프론트엔드)
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
STATIC_DIR   = FRONTEND_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ─────────────────────────────────────────
#  시작 이벤트
# ─────────────────────────────────────────
@app.on_event("startup")
async def startup():
    logger.info("=== Order Agent 시작 (v0.2.0) ===")
    init_db()
    logger.info("데이터베이스 초기화 완료")

    # DB에 저장된 모델 설정 복원
    try:
        from api.routes.settings import ensure_settings_table
        ensure_settings_table()
        from db.database import get_connection
        conn = get_connection()
        row = conn.execute("SELECT value FROM app_settings WHERE key='claude_model'").fetchone()
        conn.close()
        if row:
            import config as cfg
            cfg.CLAUDE_MODEL = row["value"]
            logger.info(f"Claude 모델 복원: {row['value']}")
    except Exception as e:
        logger.warning(f"모델 설정 복원 실패 (기본값 사용): {e}")

    # AI 메트릭 테이블 초기화
    try:
        from services.ai_metrics import ensure_metrics_tables
        ensure_metrics_tables()
        logger.info("AI 메트릭 테이블 초기화 완료")
    except Exception as e:
        logger.warning(f"AI 메트릭 테이블 초기화 실패: {e}")

    # 자료관리 자동 동기화 스케줄러 시작 (매일 오전 9시)
    asyncio.create_task(_materials_scheduler())


# ─────────────────────────────────────────
#  자료관리 자동 동기화 스케줄러
# ─────────────────────────────────────────
async def _materials_scheduler():
    """매일 오전 9시에 Google Sheets 단가표 자동 동기화"""
    from datetime import datetime, timedelta
    from services.materials_service import sync_all_sheets

    while True:
        try:
            now = datetime.now()
            # 다음 오전 9시 계산
            target = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()

            logger.info(
                f"[Materials 스케줄러] 다음 자동 동기화: {target.strftime('%Y-%m-%d %H:%M')} "
                f"({int(wait_seconds)}초 후)"
            )
            await asyncio.sleep(wait_seconds)

            # 동기화 실행
            logger.info("[Materials 스케줄러] 자동 동기화 시작")
            from services.materials_service import sync_all as sync_all_sources
            result = await sync_all_sources()
            sheets = result.get("sheets", {})
            drive = result.get("drive", {})
            logger.info(
                f"[Materials 스케줄러] 자동 동기화 완료: "
                f"시트 {sheets.get('success_count',0)}/{sheets.get('total_sources',0)}개, "
                f"총 {sheets.get('total_rows',0)}행 / "
                f"Drive {drive.get('success_count',0)}/{drive.get('total_sources',0)}개, "
                f"총 {drive.get('total_files',0)}파일"
            )
        except asyncio.CancelledError:
            logger.info("[Materials 스케줄러] 종료")
            break
        except Exception as e:
            logger.error(f"[Materials 스케줄러] 오류: {e}", exc_info=True)
            # 오류 시 1시간 후 재시도
            await asyncio.sleep(3600)


# ─────────────────────────────────────────
#  프론트엔드 서빙
# ─────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def serve_index():
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "Order Agent API is running. Visit /docs for API documentation."}

@app.get("/health")
async def health():
    """상세 헬스체크 (DB, 외부 API 상태 포함)"""
    from datetime import datetime
    checks = {"status": "ok", "version": "0.2.0", "timestamp": datetime.now().isoformat()}

    # DB 연결 확인
    try:
        from db.database import get_connection
        conn = get_connection()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"
        checks["status"] = "degraded"

    return checks


# ─────────────────────────────────────────
#  직접 실행 시
# ─────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    from config import HOST, PORT, DEBUG
    uvicorn.run("main:app", host=HOST, port=PORT, reload=DEBUG)
