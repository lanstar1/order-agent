"""
Super Agent 설정
환경변수 기반 설정, 기본값 포함 + DB 설정 연동
"""
import os

# ─── LLM 설정 (기본값) ───
SA_DEFAULT_MODEL = os.getenv("SA_DEFAULT_MODEL", "claude-sonnet")
SA_FAST_MODEL = os.getenv("SA_FAST_MODEL", "claude-haiku")


def get_sa_default_model() -> str:
    """DB 설정에서 SA 기본 모델 조회"""
    try:
        from api.routes.settings import get_llm_setting
        return get_llm_setting("llm_sa_default", SA_DEFAULT_MODEL)
    except Exception:
        return SA_DEFAULT_MODEL


def get_sa_fast_model() -> str:
    """DB 설정에서 SA 빠른 모델 조회"""
    try:
        from api.routes.settings import get_llm_setting
        return get_llm_setting("llm_sa_fast", SA_FAST_MODEL)
    except Exception:
        return SA_FAST_MODEL
SA_MAX_CONCURRENT = int(os.getenv("SA_MAX_CONCURRENT", "6"))
SA_MAX_TOKENS = int(os.getenv("SA_MAX_TOKENS", "4096"))

# ─── 비용 제한 ───
SA_MONTHLY_BUDGET = float(os.getenv("SA_MONTHLY_BUDGET", "50.0"))
SA_MAX_COST_PER_JOB = float(os.getenv("SA_MAX_COST_PER_JOB", "1.0"))

# ─── 파일 제한 ───
SA_MAX_FILE_SIZE_MB = int(os.getenv("SA_MAX_FILE_SIZE_MB", "50"))
SA_ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".txt", ".md", ".pdf", ".tsv"}

# ─── 타임아웃 ───
SA_TASK_TIMEOUT_SEC = int(os.getenv("SA_TASK_TIMEOUT_SEC", "120"))
SA_JOB_TIMEOUT_SEC = int(os.getenv("SA_JOB_TIMEOUT_SEC", "600"))

# ─── 디버그 ───
SA_DEBUG = os.getenv("SA_DEBUG", "false").lower() in ("true", "1", "yes")
SA_LOG_LEVEL = os.getenv("SA_LOG_LEVEL", "INFO")

# ─── 외부 API 키 ───
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# ─── 출력 ───
SA_OUTPUT_DIR = os.getenv("SA_OUTPUT_DIR", "")  # 비어있으면 기본 data/sa_outputs
