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
    if DEBUG:
        detail = str(exc)
    else:
        # 프로덕션: 에러 유형별 사용자 친화적 메시지
        exc_str = str(exc).lower()
        if "connection" in exc_str or "timeout" in exc_str:
            detail = "데이터베이스 연결에 실패했습니다. 잠시 후 다시 시도해주세요."
        elif "permission" in exc_str or "access" in exc_str:
            detail = "접근 권한이 없습니다."
        elif "not found" in exc_str:
            detail = "요청한 데이터를 찾을 수 없습니다."
        else:
            detail = "서버 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
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

    # 자료관리: 서버 시작 시 마지막 동기화가 오래됐으면 자동 동기화
    asyncio.create_task(_auto_sync_on_startup())


# ─────────────────────────────────────────
#  자료관리 자동 동기화 (서버 시작 시)
# ─────────────────────────────────────────
async def _auto_sync_on_startup():
    """
    서버 시작 시 마지막 동기화가 6시간 이상 지났으면 자동 동기화.
    Render 무료 플랜은 5분 후 서버가 꺼지므로, 오전 9시 스케줄러 방식은 작동하지 않음.
    대신 서버가 깨어날 때마다 동기화 필요 여부를 체크.
    """
    from datetime import datetime, timedelta

    # 서버 시작 직후 약간의 딜레이 (DB 초기화 완료 대기)
    await asyncio.sleep(5)

    try:
        from db.database import get_connection
        conn = get_connection()

        # 가장 최근 동기화 시간 확인
        row = conn.execute(
            "SELECT MAX(last_synced) as latest FROM material_sources WHERE last_synced != ''"
        ).fetchone()
        conn.close()

        latest_sync = row["latest"] if row and row["latest"] else None
        need_sync = True

        if latest_sync:
            try:
                last_dt = datetime.strptime(str(latest_sync)[:19], "%Y-%m-%d %H:%M:%S")
                hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                logger.info(f"[자동동기화] 마지막 동기화: {latest_sync} ({hours_ago:.1f}시간 전)")
                if hours_ago < 6:
                    need_sync = False
                    logger.info("[자동동기화] 최근 동기화됨 → 스킵")
            except (ValueError, TypeError) as e:
                logger.warning(f"[자동동기화] 날짜 파싱 실패: {e}")

        if need_sync:
            logger.info("[자동동기화] 동기화 시작...")
            from services.materials_service import sync_all as sync_all_sources
            result = await sync_all_sources()
            sheets = result.get("sheets", {})
            drive = result.get("drive", {})
            logger.info(
                f"[자동동기화] 완료: "
                f"시트 {sheets.get('success_count',0)}/{sheets.get('total_sources',0)}개, "
                f"총 {sheets.get('total_rows',0)}행 / "
                f"Drive {drive.get('success_count',0)}/{drive.get('total_sources',0)}개, "
                f"총 {drive.get('total_files',0)}파일"
            )

    except Exception as e:
        logger.error(f"[자동동기화] 오류: {e}", exc_info=True)


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
