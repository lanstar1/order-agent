"""
Order Agent - FastAPI (Smartstore Only - Demo)
"""
import asyncio
import logging
import os
import time
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
import sys
sys.path.insert(0, str(Path(__file__).parent))

from db.database import init_db
from api.routes.smartstore import router as smartstore_router

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
    title="스마트스토어 주문 자동화 (Demo)",
    version="0.3.0-demo",
    docs_url="/docs" if __import__("config").DEBUG else None,
    redoc_url=None,
)

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
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if not DEBUG:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    if request.url.path.startswith("/api/"):
        duration = (time.time() - start_time) * 1000
        if duration > 5000:
            logger.warning(f"[Slow API] {request.method} {request.url.path} - {duration:.0f}ms")
    return response


# ─────────────────────────────────────────
#  에러 핸들러
# ─────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "서버 처리 중 오류가 발생했습니다."})


# ─────────────────────────────────────────
#  라우터 등록 (스마트스토어만)
# ─────────────────────────────────────────
app.include_router(smartstore_router)


# ─────────────────────────────────────────
#  정적 파일 & 페이지
# ─────────────────────────────────────────
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
STATIC_DIR   = FRONTEND_DIR / "static"

@app.get("/static/js/{filename}")
async def serve_js_no_cache(filename: str):
    file_path = STATIC_DIR / "js" / filename
    if not file_path.exists() or not file_path.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(
        file_path,
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/smartstore", include_in_schema=False)
async def serve_smartstore_page():
    ss_html = FRONTEND_DIR / "smartstore.html"
    if not ss_html.exists():
        return JSONResponse({"error": "smartstore.html not found"}, status_code=404)
    return FileResponse(
        ss_html,
        media_type="text/html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

# 루트를 스마트스토어로 리다이렉트
@app.get("/", include_in_schema=False)
async def root_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/smartstore")


# ─────────────────────────────────────────
#  시작 이벤트
# ─────────────────────────────────────────
@app.on_event("startup")
async def startup():
    logger.info("=== Smartstore Demo 시작 (v0.3.0-demo) ===")
    init_db()
    logger.info("데이터베이스 초기화 완료")

    # DB에 저장된 API 키를 환경변수에 로드
    try:
        from api.routes.settings import ensure_settings_table, API_KEY_DEFINITIONS
        from db.database import get_connection as _gc
        ensure_settings_table()
        _conn = _gc()
        _db_keys = {}
        for api_def in API_KEY_DEFINITIONS:
            try:
                _row = _conn.execute(
                    "SELECT value FROM app_settings WHERE key = ?",
                    (api_def["key"],)
                ).fetchone()
                if _row and _row[0]:
                    _db_keys[api_def["key"]] = _row[0]
            except Exception:
                pass
        _conn.close()
        loaded_count = 0
        for api_def in API_KEY_DEFINITIONS:
            val = _db_keys.get(api_def["key"], "")
            if val:
                os.environ[api_def["env_var"]] = val
                loaded_count += 1
                logger.info(f"[Startup] DB에서 {api_def['label']} API 키 로드 완료")
            elif os.environ.get(api_def["env_var"]):
                logger.info(f"[Startup] {api_def['label']} 환경변수에서 이미 설정됨")
        logger.info(f"[Startup] API 키 로드 완료: DB {loaded_count}개")
    except Exception as e:
        logger.warning(f"API 키 로드 실패: {e}", exc_info=True)


# ─────────────────────────────────────────
#  헬스 체크
# ─────────────────────────────────────────
@app.get("/api/health")
async def health_check():
    checks = {"status": "ok", "version": "0.3.0-demo"}
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
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=DEBUG)
