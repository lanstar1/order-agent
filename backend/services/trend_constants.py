"""
트렌드 분석 공통 상수/헬퍼 (shared/src/trends.ts 변환)
"""
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

TREND_MONTHLY_START_PERIOD = "2021-01"
TREND_TIMEZONE = "Asia/Seoul"
TREND_DEFAULT_RESULT_COUNT = 20

# 기본 브랜드 제외 목록
DEFAULT_BRAND_EXCLUDE = [
    "삼성", "samsung", "lg", "엘지", "애플", "apple", "소니", "sony",
    "나이키", "nike", "아디다스", "adidas", "뉴발란스", "구찌", "gucci",
    "샤넬", "chanel", "루이비통", "프라다", "다이슨", "dyson",
    "필립스", "philips", "보쉬", "bosch", "브라운", "braun",
    "로레알", "이니스프리", "설화수", "라네즈", "아모레퍼시픽",
    "쿠팡", "무신사", "마켓컬리", "SSG", "롯데", "현대",
    "유니클로", "자라", "zara", "H&M", "이케아", "ikea",
    "스타벅스", "맥도날드",
]

# 이벤트 라벨 (월별)
EVENT_LABELS = {
    "01": "신년/겨울 준비",
    "02": "발렌타인/설날",
    "03": "봄맞이/입학",
    "04": "벚꽃/야외활동",
    "05": "가정의 달/어버이날",
    "06": "여름 준비/중간결산",
    "07": "여름 성수기/휴가",
    "08": "여름 마무리/개학",
    "09": "추석/가을 준비",
    "10": "가을 시즌/할로윈",
    "11": "연말/블랙프라이데이",
    "12": "크리스마스/연말정산",
}

# 네이버 카테고리 폴백 (루트)
NAVER_ROOT_CATEGORIES = [
    {"cid": "50000000", "name": "패션의류"},
    {"cid": "50000001", "name": "패션잡화"},
    {"cid": "50000002", "name": "화장품/미용"},
    {"cid": "50000003", "name": "디지털/가전"},
    {"cid": "50000004", "name": "가구/인테리어"},
    {"cid": "50000005", "name": "출산/육아"},
    {"cid": "50000006", "name": "식품"},
    {"cid": "50000007", "name": "스포츠/레저"},
    {"cid": "50000008", "name": "생활/건강"},
    {"cid": "50000009", "name": "여가/생활편의"},
    {"cid": "50000010", "name": "면세점"},
    {"cid": "50000011", "name": "도서"},
]


def now_kst() -> datetime:
    return datetime.now(KST)


def now_kst_str() -> str:
    return now_kst().strftime("%Y-%m-%dT%H:%M:%S")


def get_latest_collectible_period() -> str:
    """수집 가능한 최신 월 (현재 월의 전월)"""
    n = now_kst()
    first_of_month = n.replace(day=1)
    prev = first_of_month - timedelta(days=1)
    return prev.strftime("%Y-%m")


def list_monthly_periods(start: str, end: str) -> list[str]:
    """시작~종료 월 사이의 모든 월 리스트 반환"""
    periods = []
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        periods.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return periods


def normalize_spreadsheet_id(raw: str) -> str:
    """구글 시트 URL이나 ID에서 spreadsheetId만 추출"""
    raw = raw.strip()
    if not raw:
        return ""
    if "/spreadsheets/d/" in raw:
        parts = raw.split("/spreadsheets/d/")[1]
        return parts.split("/")[0].split("?")[0]
    if "?" in raw:
        raw = raw.split("?")[0]
    return raw


def build_sheet_url(spreadsheet_id: str) -> str:
    if not spreadsheet_id:
        return ""
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
