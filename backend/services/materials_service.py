"""
자료관리 서비스 - Google Sheets 단가표 + Google Drive 문서 동기화 및 검색

구조:
1. material_sources: 등록된 Google Sheets/Drive 소스 목록
2. price_data: Google Sheets에서 동기화한 단가표 데이터 (CSV → SQLite)
3. drive_documents: Google Drive 파일 메타데이터 (KC인증서, 데이터시트 등)

동기화 방식:
- Google Sheets: 공개 CSV export URL로 다운로드 → SQLite 저장
  URL: https://docs.google.com/spreadsheets/d/{ID}/export?format=csv&gid={GID}
- Google Drive: API로 파일 목록 조회 → 메타데이터 저장 + 다운로드 URL 제공
"""
import csv
import io
import json
import logging
import httpx
import asyncio
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.database import get_connection, column_exists, now_kst
from config import GOOGLE_API_KEY

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  DB 테이블 생성
# ─────────────────────────────────────────
def _ensure_tables():
    conn = get_connection()
    conn.executescript("""
        -- 자료 소스 등록 (Google Sheet / Drive Folder)
        CREATE TABLE IF NOT EXISTS material_sources (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL DEFAULT 'sheet',  -- 'sheet' | 'drive_folder'
            name        TEXT NOT NULL,                  -- 표시명 (예: "IP TIME 단가표")
            url         TEXT NOT NULL,                  -- 원본 URL
            sheet_id    TEXT DEFAULT '',                 -- Google Sheet ID
            folder_id   TEXT DEFAULT '',                 -- Google Drive Folder ID
            gid         TEXT DEFAULT '0',               -- Sheet 탭 GID (기본 첫 탭)
            category    TEXT DEFAULT 'price',           -- 'price' | 'certificate' | 'datasheet' | 'other'
            vendor      TEXT DEFAULT '',                -- 제조사/유통사명
            last_synced TEXT DEFAULT '',
            is_active   INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        -- 단가표 데이터 (Google Sheets에서 동기화)
        CREATE TABLE IF NOT EXISTS price_data (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id   INTEGER NOT NULL,
            row_data    TEXT NOT NULL,    -- JSON 형태의 행 데이터
            row_colors  TEXT DEFAULT '',  -- JSON: 셀별 배경색 {"컬럼명": "#hex", ...}
            -- 주요 검색 필드 (시트마다 컬럼명이 다를 수 있으므로 추출하여 저장)
            product_name TEXT DEFAULT '',
            model_name   TEXT DEFAULT '',
            price        TEXT DEFAULT '',
            category     TEXT DEFAULT '',
            vendor       TEXT DEFAULT '',
            raw_text     TEXT DEFAULT '',  -- 전체 행을 텍스트로 합친 검색용
            sheet_tab    TEXT DEFAULT '',
            synced_at    TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (source_id) REFERENCES material_sources(id)
        );

        -- 검색 성능을 위한 인덱스
        CREATE INDEX IF NOT EXISTS idx_price_data_search
            ON price_data(source_id, raw_text);
        CREATE INDEX IF NOT EXISTS idx_price_data_model
            ON price_data(model_name);
        CREATE INDEX IF NOT EXISTS idx_price_data_vendor
            ON price_data(vendor);

        -- Google Drive 문서 메타데이터 (KC인증서, 데이터시트 등)
        CREATE TABLE IF NOT EXISTS drive_documents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id   INTEGER,
            file_id     TEXT NOT NULL,     -- Google Drive file ID
            file_name   TEXT NOT NULL,
            file_type   TEXT DEFAULT '',   -- MIME type
            folder_path TEXT DEFAULT '',   -- 상위 폴더 경로 (예: "2024-01")
            file_url    TEXT DEFAULT '',   -- 다운로드/보기 URL
            category    TEXT DEFAULT '',   -- 'certificate' | 'datasheet' | 'manual' | 'other'
            vendor      TEXT DEFAULT '',
            synced_at   TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (source_id) REFERENCES material_sources(id)
        );
        CREATE INDEX IF NOT EXISTS idx_drive_docs_name
            ON drive_documents(file_name);
    """)
    conn.close()

_ensure_tables()

# sheet_tab 컬럼 마이그레이션 (기존 DB 호환, SQLite/PG 모두 지원)
def _migrate_sheet_tab():
    conn = get_connection()
    if not column_exists(conn, 'price_data', 'sheet_tab'):
        conn.execute("ALTER TABLE price_data ADD COLUMN sheet_tab TEXT DEFAULT ''")
        conn.commit()
        logger.info("[Materials] price_data.sheet_tab 컬럼 추가 완료")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_price_data_tab ON price_data(source_id, sheet_tab)")
    conn.commit()
    conn.close()

_migrate_sheet_tab()

# row_colors 컬럼 마이그레이션 (기존 DB 호환, SQLite/PG 모두 지원)
def _migrate_row_colors():
    conn = get_connection()
    if not column_exists(conn, 'price_data', 'row_colors'):
        conn.execute("ALTER TABLE price_data ADD COLUMN row_colors TEXT DEFAULT ''")
        conn.commit()
        logger.info("[Materials] price_data.row_colors 컬럼 추가 완료")
    conn.close()

_migrate_row_colors()


# ─────────────────────────────────────────
#  기본 소스 등록 (초기 설정)
# ─────────────────────────────────────────
DEFAULT_SOURCES = [
    {
        "source_type": "sheet",
        "name": "(주)액티웍스 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1OeeDbYvIlNlIk-Ld260br4iG5a3hA3ZLMxhEBi-uzhk/edit",
        "sheet_id": "1OeeDbYvIlNlIk-Ld260br4iG5a3hA3ZLMxhEBi-uzhk",
        "category": "price",
        "vendor": "액티웍스",
    },
    {
        "source_type": "sheet",
        "name": "IP TIME 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1zTUfre_PJ93ToY--kJA33xSVWf58NTP9YIQSRnvZLMg/edit",
        "sheet_id": "1zTUfre_PJ93ToY--kJA33xSVWf58NTP9YIQSRnvZLMg",
        "category": "price",
        "vendor": "IP TIME",
    },
    {
        "source_type": "sheet",
        "name": "(주)엘디네트웍스 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1FhV-p40LSE_eU0_UcSd5bCHUlgYSvAWwDLTD4NznNjE/edit",
        "sheet_id": "1FhV-p40LSE_eU0_UcSd5bCHUlgYSvAWwDLTD4NznNjE",
        "category": "price",
        "vendor": "엘디네트웍스",
    },
    {
        "source_type": "sheet",
        "name": "파워네트정보통신 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1Mtr_Mc9AFzwgl-P4l0FcR0iXgR477t7Kw0prQhoEt2I/edit",
        "sheet_id": "1Mtr_Mc9AFzwgl-P4l0FcR0iXgR477t7Kw0prQhoEt2I",
        "gid": "65358359",
        "category": "price",
        "vendor": "파워네트정보통신",
    },
    {
        "source_type": "sheet",
        "name": "에이스정보통신 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1qIPNjabb7kTo4UvC5KTsSexQGlD5P2h__YUwHQwGIJo/edit",
        "sheet_id": "1qIPNjabb7kTo4UvC5KTsSexQGlD5P2h__YUwHQwGIJo",
        "category": "price",
        "vendor": "에이스정보통신",
    },
    {
        "source_type": "sheet",
        "name": "인네트워크 & 다유즈 통합 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1YELqEsRbjIc_l1F_F1gvgWVFoLk0wdkqsdR9OgLyNTg/edit",
        "sheet_id": "1YELqEsRbjIc_l1F_F1gvgWVFoLk0wdkqsdR9OgLyNTg",
        "category": "price",
        "vendor": "인네트워크/다유즈",
    },
    {
        "source_type": "sheet",
        "name": "NEXT 통합 단가표",
        "url": "https://docs.google.com/spreadsheets/d/1Vn73xaNhX1hvTtZDed5suY6qoVz1rKABxbiI0FVG55E/edit",
        "sheet_id": "1Vn73xaNhX1hvTtZDed5suY6qoVz1rKABxbiI0FVG55E",
        "category": "price",
        "vendor": "NEXT",
    },
    {
        "source_type": "sheet",
        "name": "스타링크_유비큐넷 단가표",
        "url": "https://docs.google.com/spreadsheets/d/13sVE4_A26zKr69lrFomrk9Ide9CbAAy72p2p3_pmV9Y/edit",
        "sheet_id": "13sVE4_A26zKr69lrFomrk9Ide9CbAAy72p2p3_pmV9Y",
        "category": "price",
        "vendor": "스타링크/유비큐넷",
    },
    {
        "source_type": "sheet",
        "name": "TP-Link 가격표 2026Q1",
        "url": "https://docs.google.com/spreadsheets/d/19YiQDR7Ru3lYe4qejCVPCqlfceZZyKl6aBDnnNnuycY/edit",
        "sheet_id": "19YiQDR7Ru3lYe4qejCVPCqlfceZZyKl6aBDnnNnuycY",
        "category": "price",
        "vendor": "TP-Link",
    },
    {
        "source_type": "drive_folder",
        "name": "자료검색 (데이터시트/KC인증서/ROHS/UL/Fluke/Test리포트)",
        "url": "https://drive.google.com/drive/folders/103m-Rj22HUpWyEwUlWle3FuUBnU6Y2hl",
        "folder_id": "103m-Rj22HUpWyEwUlWle3FuUBnU6Y2hl",
        "category": "all",
        "vendor": "",
    },
]


def init_default_sources():
    """기본 소스가 없으면 자동 등록"""
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM material_sources").fetchone()[0]
    if count == 0:
        for src in DEFAULT_SOURCES:
            conn.execute(
                """INSERT INTO material_sources(source_type,name,url,sheet_id,folder_id,category,vendor)
                   VALUES(?,?,?,?,?,?,?)""",
                (src["source_type"], src["name"], src["url"],
                 src.get("sheet_id", ""), src.get("folder_id", ""),
                 src.get("category", "price"), src.get("vendor", ""))
            )
        conn.commit()
        logger.info(f"[Materials] 기본 소스 {len(DEFAULT_SOURCES)}개 등록 완료")
    conn.close()

init_default_sources()


# ─────────────────────────────────────────
#  Google Sheets CSV 동기화
# ─────────────────────────────────────────

# 헤더로 인식할 수 있는 컬럼명 패턴들
_HEADER_KEYWORDS = [
    "모델", "모 델", "model", "품명", "품목", "제품", "상품",
    "가격", "단가", "공급가", "판매가", "대리점", "출고가", "매입가", "price",
    "분류", "카테고리", "category", "규격", "spec", "사양",
    "no.", "no ", "번호", "상품코드",
    "오픈마켓", "카드몰", "description", "스토어팜",
    "업링크", "마운트", "공지사항",
]


def _rgba_to_hex(color_obj: dict) -> str:
    """Google Sheets API의 rgba color 객체를 #hex 문자열로 변환. 흰색/기본색은 빈 문자열 반환."""
    if not color_obj:
        return ""
    r = int(color_obj.get("red", 1) * 255)
    g = int(color_obj.get("green", 1) * 255)
    b = int(color_obj.get("blue", 1) * 255)
    # 흰색/거의 흰색 무시
    if r >= 250 and g >= 250 and b >= 250:
        return ""
    return f"#{r:02x}{g:02x}{b:02x}"


async def _fetch_sheet_colors(sheet_id: str, gid: str, tab_title: str) -> dict:
    """
    Google Sheets API로 특정 탭의 셀 배경색을 조회합니다.
    Returns: {row_index: {col_index: "#hex"}} (0-based, 헤더 포함)
    """
    # Sheets API에서 gridData를 가져오기 (배경색 정보 포함)
    if not GOOGLE_API_KEY:
        return {}
    ranges_param = f"&ranges='{tab_title}'" if tab_title else ""
    api_url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
        f"?key={GOOGLE_API_KEY}"
        f"{ranges_param}"
        f"&fields=sheets.data.rowData.values.effectiveFormat.backgroundColor"
    )
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(api_url, timeout=30)
            r.raise_for_status()
        data = r.json()

        colors = {}
        sheets = data.get("sheets", [])
        if not sheets:
            return colors

        grid_data = sheets[0].get("data", [])
        if not grid_data:
            return colors

        for row_idx, row_data in enumerate(grid_data[0].get("rowData", [])):
            row_colors = {}
            for col_idx, cell in enumerate(row_data.get("values", [])):
                ef = cell.get("effectiveFormat", {})
                bg = ef.get("backgroundColor")
                hex_color = _rgba_to_hex(bg)
                if hex_color:
                    row_colors[col_idx] = hex_color
            if row_colors:
                colors[row_idx] = row_colors

        logger.info(f"[Materials] 색상 데이터 로드: 탭='{tab_title}', 색상 있는 행={len(colors)}개")
        return colors
    except Exception as e:
        logger.warning(f"[Materials] 셀 색상 조회 실패 (탭='{tab_title}'): {e}")
        return {}


async def _discover_sheet_tabs(sheet_id: str) -> list:
    """
    Google Sheets API로 스프레드시트의 모든 탭(시트) 정보를 조회합니다.
    Returns: [{"title": "LD랙", "gid": 0, "index": 0}, ...]
    """
    api_url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
        f"?key={GOOGLE_API_KEY}&fields=sheets.properties(sheetId,title,index)"
    )
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(api_url, timeout=15)
            r.raise_for_status()
        data = r.json()
        tabs = []
        for s in data.get("sheets", []):
            props = s.get("properties", {})
            tabs.append({
                "title": props.get("title", ""),
                "gid": str(props.get("sheetId", 0)),
                "index": props.get("index", 0),
            })
        tabs.sort(key=lambda t: t["index"])
        return tabs
    except Exception as e:
        logger.warning(f"[Materials] 시트 탭 조회 실패 (sheet_id={sheet_id}): {e}")
        return []


def _detect_header_row(rows: list, max_scan: int = 20) -> int:
    """
    CSV 행들에서 실제 헤더 행 인덱스를 찾습니다.
    - 첫 max_scan 행을 스캔하여 헤더 키워드가 가장 많이 매칭되는 행을 반환
    - 매칭이 없으면 0 (첫 행) 반환
    """
    best_idx = 0
    best_score = 0

    scan_limit = min(len(rows), max_scan)
    for i in range(scan_limit):
        row_text = " ".join(str(c).strip().lower() for c in rows[i])
        # 빈 행 스킵
        non_empty = sum(1 for c in rows[i] if str(c).strip())
        if non_empty < 2:
            continue

        score = 0
        for kw in _HEADER_KEYWORDS:
            if kw.lower() in row_text:
                score += 1

        if score > best_score:
            best_score = score
            best_idx = i

    return best_idx


def _clean_header(h: str) -> str:
    """헤더 텍스트 정리 (줄바꿈, 공백 정리)"""
    import re
    h = h.strip().replace("\n", " ").replace("\r", " ")
    h = re.sub(r'\s+', ' ', h)
    return h


def _build_composite_headers(rows: list, header_idx: int) -> list:
    """
    복합 헤더 구성: 멀티행 헤더(그룹 헤더 + 세부 헤더)를 하나로 합침.

    예: NEXT 시트
      Row 4: (스토어팜,쿠팡,독립몰) | ... | 지마켓,옥션,11번가... | 딜러 | 원윈/셀프로 | 대리점
      Row 5: 노출지도가 | ... | (등록가) | 지도가 | 공급가 | 공급가
      Row 6: NO | 상품코드 | 모델명 | <유료 배송> | <무료 배송> | ...

    결과: "스토어팜,쿠팡,독립몰 노출지도가 <유료 배송>", "딜러 지도가", "대리점 공급가" 등
    """
    main_headers = [_clean_header(str(h)) for h in rows[header_idx]]
    num_cols = len(main_headers)

    # 헤더 위 3행까지 스캔하여 그룹 헤더 수집
    # ★ 그룹 헤더 판별 기준: 데이터 영역(열 5 이후)에 비어있지 않은 셀이 3개 이상
    #    - 앞쪽 열(0~4)에만 있는 행은 제목/날짜/참고사항일 가능성이 높음
    #    - 그룹 헤더는 가격 컬럼들 위에 채널명 등이 배치되므로 뒤쪽 열에 분산됨
    group_rows = []
    data_col_start = min(5, num_cols // 3)  # 데이터 영역 시작 (보통 5열 이후)
    for i in range(max(0, header_idx - 3), header_idx):
        row = rows[i]
        cells = []
        for j in range(num_cols):
            cells.append(_clean_header(str(row[j])) if j < len(row) else "")

        # 데이터 영역(뒤쪽 열)에 3개 이상 비어있지 않은 셀 → 그룹 헤더
        data_area_non_empty = sum(1 for j, c in enumerate(cells) if j >= data_col_start and c.strip())
        if data_area_non_empty >= 3:
            group_rows.append(cells)

    # 병합 셀 대응: 비어있는 셀에 왼쪽 값 전파 (fill-right)
    # ★ 순수 숫자 셀은 그룹 헤더가 아니므로 전파하지 않음
    def fill_right(row):
        filled = list(row)
        last_val = ""
        for i in range(len(filled)):
            cell = filled[i].strip()
            if cell:
                # 순수 숫자는 그룹 헤더가 아님 (NO 열의 인덱스 등)
                if cell.isdigit():
                    filled[i] = ""
                else:
                    last_val = cell
            else:
                filled[i] = last_val if last_val else ""
        return filled

    if not group_rows:
        # 그룹 헤더가 없어도 서브헤더 체크는 필요하므로 메인 헤더를 composite로 사용
        composite = list(main_headers)
    else:
        filled_groups = [fill_right(r) for r in group_rows]

        # 각 컬럼별 복합 헤더 구성: 그룹 헤더들 + 메인 헤더
        composite = []
        for col_idx in range(num_cols):
            parts = []
            seen = set()
            for fg in filled_groups:
                if col_idx < len(fg) and fg[col_idx].strip():
                    val = fg[col_idx].strip()
                    # 순수 숫자만 있는 값은 스킵 (시트 내부 인덱스 등)
                    if val.isdigit():
                        continue
                    if val not in seen:
                        parts.append(val)
                        seen.add(val)

            main_h = main_headers[col_idx].strip() if col_idx < len(main_headers) else ""
            if main_h and main_h not in seen:
                # 순수 숫자만 있는 메인 헤더도 스킵
                if not main_h.isdigit():
                    parts.append(main_h)

            composite.append(" ".join(parts) if parts else "")

    # ★ 헤더 아래의 서브헤더도 확인하여 합병
    # IP TIME 등: 메인 헤더 아래에 채널 세부 이름이 있는 경우
    sub_row_idx = header_idx + 1
    if sub_row_idx < len(rows):
        sub_cells = []
        for j in range(num_cols):
            sub_cells.append(_clean_header(str(rows[sub_row_idx][j])) if j < len(rows[sub_row_idx]) else "")

        # 서브헤더 판별: 텍스트 셀 ≥ 3개, 숫자 셀보다 많고, 가격 패턴이 거의 없어야 함
        import re as _re
        text_cells = sum(1 for c in sub_cells if c.strip() and not c.strip().replace(",", "").replace(".", "").isdigit())
        numeric_cells = sum(1 for c in sub_cells if c.strip() and c.strip().replace(",", "").replace(".", "").isdigit())
        # 가격 패턴 (예: "20,405", "7,700") → 데이터 행에서 흔함
        price_pattern_cells = sum(1 for c in sub_cells if _re.match(r'^\d{1,3}(?:,\d{3})+$', c.strip()))

        if text_cells >= 3 and text_cells > numeric_cells and price_pattern_cells < 2:
            for col_idx in range(num_cols):
                if col_idx < len(sub_cells) and sub_cells[col_idx].strip():
                    sub_val = sub_cells[col_idx].strip()
                    # 순수 숫자 스킵
                    if sub_val.isdigit():
                        continue
                    if composite[col_idx].strip():
                        # 기존 헤더에 서브헤더 추가 (중복이 아닌 경우만)
                        if sub_val.lower() not in composite[col_idx].lower():
                            composite[col_idx] += " " + sub_val
                    else:
                        composite[col_idx] = sub_val

    logger.info(f"[Materials] 복합 헤더 구성: {[h for h in composite if h][:15]}")
    return composite


async def _sync_single_tab(conn, source_id: int, sheet_id: str, gid: str,
                            tab_title: str, vendor: str, colors: dict = None) -> int:
    """단일 시트 탭을 CSV로 다운로드하여 price_data에 저장. 삽입된 행 수 반환.
    colors: {row_index: {col_index: "#hex"}} - Sheets API에서 가져온 배경색 데이터"""
    if colors is None:
        colors = {}
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    if gid and gid != "0":
        csv_url += f"&gid={gid}"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        r = await client.get(csv_url, timeout=30)
        r.raise_for_status()

    reader = csv.reader(io.StringIO(r.text))
    rows = list(reader)
    if len(rows) < 2:
        return 0

    header_idx = _detect_header_row(rows)
    headers = _build_composite_headers(rows, header_idx)
    data_rows = rows[header_idx + 1:]

    _sub_header_words = ["품절", "현금", "카드", "착불", "무료배송", "배송비"]
    while data_rows:
        first_text = " ".join(str(c).strip() for c in data_rows[0]).lower()
        if any(sw in first_text for sw in _sub_header_words) and not any(
            c.strip().replace(",", "").isdigit() and len(c.strip()) > 3 for c in data_rows[0]
        ):
            data_rows = data_rows[1:]
        else:
            break

    logger.info(f"[Materials] 탭 '{tab_title}' 헤더행={header_idx}: {[h for h in headers if h][:10]}")

    # data_rows의 실제 스프레드시트 행 인덱스 오프셋 계산
    data_start_row = header_idx + 1 + (len(rows) - len(data_rows) - header_idx - 1)
    # 더 정확하게: rows에서 data_rows 시작 위치 역추적
    skipped_sub = len(rows[header_idx + 1:]) - len(data_rows)
    data_start_row = header_idx + 1 + skipped_sub

    count = 0
    for row_offset, row in enumerate(data_rows):
        if not any(cell.strip() for cell in row):
            continue

        actual_row_idx = data_start_row + row_offset  # 스프레드시트 원본 행 인덱스 (0-based)

        row_dict = {}
        for i, val in enumerate(row):
            if i < len(headers) and headers[i]:
                key = headers[i]
                val_str = val.strip()
                if key in row_dict:
                    if not val_str:
                        continue
                    if not row_dict[key]:
                        row_dict[key] = val_str
                    else:
                        suffix = 2
                        new_key = f"{key} ({suffix})"
                        while new_key in row_dict:
                            suffix += 1
                            new_key = f"{key} ({suffix})"
                        row_dict[new_key] = val_str
                else:
                    row_dict[key] = val_str

        if not any(v for v in row_dict.values()):
            continue

        # 의미 있는 값이 2개 미만이면 스킵 (빈 행/구분선 등 제거)
        non_empty_count = sum(1 for v in row_dict.values() if v.strip())
        if non_empty_count < 2:
            continue

        product_name = _extract_field(row_dict, ["품명", "품목명", "제품명", "상품명", "상품제목", "Product", "제품", "품목", "제품 사양"])
        model_name = _extract_field(row_dict, ["모델명", "모델", "모 델 명", "모 델", "Model", "규격", "모델번호", "MODEL", "상품명"])
        price = _extract_field(row_dict, [
            "대리점 공급가", "대리점 권장매입가", "대리점 권장도매가",
            "공급가(부가세포함)", "공급가 (부가세포함)", "공급가 (부가세 포함)",
            "공급가(부가세 별도)", "공급가 (부가세 별도)",
            "대리점가", "공급가",
            "단가", "가격", "Price", "판매가", "출고가", "매입가",
            "권장도매가", "도매가",
        ])
        category = _extract_field(row_dict, ["분류", "카테고리", "Category", "종류", "구분", "제품군"])
        raw_text = " ".join(val.strip() for val in row if val.strip())

        if not model_name and not product_name and raw_text:
            first_word = raw_text.split()[0] if raw_text.split() else ""
            if len(first_word) >= 2 and not first_word.startswith("※"):
                model_name = first_word

        # 셀 배경색 데이터 매핑 (col_index → header_name)
        row_color_dict = {}
        if colors and actual_row_idx in colors:
            col_colors = colors[actual_row_idx]
            for col_idx, hex_color in col_colors.items():
                if col_idx < len(headers) and headers[col_idx]:
                    row_color_dict[headers[col_idx]] = hex_color
                elif col_idx == 0 or (col_idx < len(headers)):
                    # 행 전체 색상으로 취급 (첫 번째 컬럼 색상이 있으면)
                    if "_row" not in row_color_dict:
                        row_color_dict["_row"] = hex_color

        conn.execute(
            """INSERT INTO price_data(source_id, row_data, row_colors, product_name, model_name,
                                      price, category, vendor, raw_text, sheet_tab)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (source_id, json.dumps(row_dict, ensure_ascii=False),
             json.dumps(row_color_dict, ensure_ascii=False) if row_color_dict else "",
             product_name, model_name, price, category, vendor, raw_text, tab_title)
        )
        count += 1

    return count


async def sync_sheet(source_id: int) -> dict:
    """
    Google Sheet를 CSV로 다운로드하여 price_data 테이블에 저장
    - 모든 시트 탭을 자동 탐지하여 각각 동기화
    - 특정 gid가 지정된 경우 해당 탭만 동기화
    Returns: {"success": bool, "rows_synced": int, "error": str}
    """
    conn = get_connection()
    source = conn.execute("SELECT * FROM material_sources WHERE id=?", (source_id,)).fetchone()
    if not source:
        conn.close()
        return {"success": False, "error": "소스를 찾을 수 없습니다.", "rows_synced": 0}

    sheet_id = source["sheet_id"]
    gid = source["gid"] or ""
    vendor = source["vendor"] or source["name"]

    logger.info(f"[Materials] 시트 동기화 시작: {source['name']} (ID={sheet_id})")

    try:
        # 기존 데이터 삭제 (전체 재동기화)
        conn.execute("DELETE FROM price_data WHERE source_id=?", (source_id,))

        total_count = 0
        tab_results = []

        # 특정 gid가 지정된 경우 → 해당 탭만 동기화
        if gid and gid != "0":
            tab_colors = await _fetch_sheet_colors(sheet_id, gid, "")
            count = await _sync_single_tab(conn, source_id, sheet_id, gid, "", vendor, colors=tab_colors)
            total_count = count
            tab_results.append({"tab": gid, "rows": count})
        else:
            # 모든 탭 자동 탐지 → 각각 동기화
            tabs = await _discover_sheet_tabs(sheet_id)
            if not tabs:
                # 탭 조회 실패 시 기본 탭(gid=0)만 동기화
                tab_colors = await _fetch_sheet_colors(sheet_id, "0", "Sheet1")
                count = await _sync_single_tab(conn, source_id, sheet_id, "0", "", vendor, colors=tab_colors)
                total_count = count
                tab_results.append({"tab": "(기본)", "rows": count})
            else:
                for tab in tabs:
                    try:
                        tab_colors = await _fetch_sheet_colors(sheet_id, tab["gid"], tab["title"])
                        count = await _sync_single_tab(
                            conn, source_id, sheet_id,
                            tab["gid"], tab["title"], vendor, colors=tab_colors
                        )
                        total_count += count
                        tab_results.append({"tab": tab["title"], "rows": count})
                        logger.info(f"[Materials]   탭 '{tab['title']}': {count}행")
                    except Exception as tab_err:
                        logger.warning(f"[Materials]   탭 '{tab['title']}' 동기화 실패: {tab_err}")
                        tab_results.append({"tab": tab["title"], "rows": 0, "error": str(tab_err)})

        conn.execute(
            "UPDATE material_sources SET last_synced=? WHERE id=?",
            (now_kst(), source_id)
        )
        conn.commit()
        conn.close()

        logger.info(f"[Materials] 시트 동기화 완료: {source['name']}, 총 {total_count}행 ({len(tab_results)}탭)")
        return {"success": True, "rows_synced": total_count, "tabs": tab_results}

    except httpx.HTTPStatusError as e:
        conn.close()
        detail = ""
        try:
            err_body = e.response.json()
            detail = err_body.get("error", {}).get("message", "")
        except Exception:
            detail = e.response.text[:200] if e.response.text else ""

        if e.response.status_code == 403:
            err = (
                f"HTTP 403: Google Sheets API 접근이 거부되었습니다. "
                f"Google Cloud Console에서 'Google Sheets API'를 활성화했는지 확인하세요. "
                f"또한 시트가 '링크가 있는 모든 사용자'에게 공개되어 있어야 합니다."
            )
            if detail:
                err += f" [상세: {detail}]"
        else:
            err = f"HTTP 오류 {e.response.status_code}: 시트 접근 실패. [상세: {detail}]"
        logger.error(f"[Materials] {err}")
        return {"success": False, "error": err, "rows_synced": 0}
    except Exception as e:
        conn.close()
        logger.error(f"[Materials] 동기화 오류: {e}", exc_info=True)
        return {"success": False, "error": str(e), "rows_synced": 0}


async def sync_all_sheets() -> dict:
    """모든 활성 시트 소스를 동기화"""
    conn = get_connection()
    sources = conn.execute(
        "SELECT id, name FROM material_sources WHERE source_type='sheet' AND is_active=1"
    ).fetchall()
    conn.close()

    results = []
    for src in sources:
        result = await sync_sheet(src["id"])
        result["name"] = src["name"]
        result["source_id"] = src["id"]
        results.append(result)

    total_rows = sum(r["rows_synced"] for r in results)
    success_count = sum(1 for r in results if r["success"])
    return {
        "total_sources": len(sources),
        "success_count": success_count,
        "total_rows": total_rows,
        "details": results,
    }


# ─────────────────────────────────────────
#  Google Drive 폴더 동기화
# ─────────────────────────────────────────
DRIVE_API_BASE = "https://www.googleapis.com/drive/v3/files"


async def _list_drive_folder(folder_id: str, api_key: str) -> list:
    """
    Google Drive API v3로 폴더 내 파일 목록 조회 (재귀적으로 하위 폴더 탐색)
    Returns: [{"id", "name", "mimeType", "parents", "folder_path"}, ...]
    """
    all_files = []

    async def _crawl(fid: str, path: str = ""):
        page_token = None
        while True:
            params = {
                "q": f"'{fid}' in parents and trashed=false",
                "key": api_key,
                "fields": "nextPageToken,files(id,name,mimeType,webViewLink,webContentLink)",
                "pageSize": 100,
            }
            if page_token:
                params["pageToken"] = page_token

            async with httpx.AsyncClient() as client:
                r = await client.get(DRIVE_API_BASE, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()

            for f in data.get("files", []):
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    # 하위 폴더 → 재귀 탐색
                    sub_path = f"{path}/{f['name']}" if path else f["name"]
                    await _crawl(f["id"], sub_path)
                else:
                    all_files.append({
                        "file_id": f["id"],
                        "file_name": f["name"],
                        "file_type": f["mimeType"],
                        "folder_path": path,
                        "file_url": f.get("webViewLink", ""),
                        "download_url": f.get("webContentLink", ""),
                    })

            page_token = data.get("nextPageToken")
            if not page_token:
                break

    await _crawl(folder_id)
    return all_files


def _detect_category_from_path(folder_path: str, file_name: str) -> str:
    """
    폴더 경로와 파일명에서 자료 카테고리를 자동 감지
    카테고리: 데이터시트, Fluke, KC인증서, ROHS, Test리포트, UL
    """
    text = (folder_path + " " + file_name).lower()
    if "fluke" in text:
        return "Fluke"
    if "kc" in text or "인증서" in text or "certificate" in text:
        return "KC인증서"
    if "rohs" in text or "rosh" in text:
        return "ROHS"
    if "test" in text and ("리포트" in text or "report" in text):
        return "Test리포트"
    if "ul" in text and ("인증" in text or "cert" in text or "/" in folder_path):
        return "UL"
    if "데이터시트" in text or "datasheet" in text or "data sheet" in text or "spec" in text:
        return "데이터시트"
    # 폴더 최상위 이름을 카테고리로 사용
    top_folder = folder_path.split("/")[0].strip() if folder_path else ""
    if top_folder:
        for cat in ["데이터시트", "Fluke", "KC인증서", "ROHS", "Test리포트", "UL"]:
            if cat.lower() in top_folder.lower():
                return cat
    return "기타"


async def sync_drive_folder(source_id: int) -> dict:
    """
    Google Drive 폴더의 파일 목록을 동기화하여 drive_documents 테이블에 저장
    - 하위 폴더명으로 카테고리 자동 분류
    Returns: {"success": bool, "files_synced": int, "error": str}
    """
    if not GOOGLE_API_KEY:
        return {
            "success": False,
            "files_synced": 0,
            "error": "GOOGLE_API_KEY가 설정되지 않았습니다. .env에 Google API Key를 추가하세요.",
        }

    conn = get_connection()
    source = conn.execute("SELECT * FROM material_sources WHERE id=?", (source_id,)).fetchone()
    if not source:
        conn.close()
        return {"success": False, "files_synced": 0, "error": "소스를 찾을 수 없습니다."}

    folder_id = source["folder_id"]
    vendor = source["vendor"] or ""

    logger.info(f"[Materials] Drive 폴더 동기화 시작: {source['name']} (folder={folder_id})")

    try:
        files = await _list_drive_folder(folder_id, GOOGLE_API_KEY)

        # 기존 데이터 삭제 (전체 재동기화)
        conn.execute("DELETE FROM drive_documents WHERE source_id=?", (source_id,))

        count = 0
        for f in files:
            # 파일 보기 URL (webViewLink 또는 직접 구성)
            view_url = f.get("file_url", "")
            if not view_url:
                view_url = f"https://drive.google.com/file/d/{f['file_id']}/view"

            # ★ 하위 폴더명에서 카테고리 자동 감지
            category = _detect_category_from_path(f.get("folder_path", ""), f["file_name"])

            conn.execute(
                """INSERT INTO drive_documents(source_id, file_id, file_name, file_type,
                   folder_path, file_url, category, vendor)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (source_id, f["file_id"], f["file_name"], f["file_type"],
                 f["folder_path"], view_url, category, vendor)
            )
            count += 1

        # 마지막 동기화 시간 업데이트
        conn.execute(
            "UPDATE material_sources SET last_synced=? WHERE id=?",
            (now_kst(), source_id)
        )
        conn.commit()
        conn.close()

        logger.info(f"[Materials] Drive 폴더 동기화 완료: {source['name']}, {count}개 파일")
        return {"success": True, "files_synced": count}

    except httpx.HTTPStatusError as e:
        conn.close()
        # Google API 에러 응답 본문에서 구체적 원인 추출
        detail = ""
        try:
            err_body = e.response.json()
            err_info = err_body.get("error", {})
            detail = err_info.get("message", "")
            err_status = err_info.get("status", "")
            if err_status:
                detail = f"{detail} ({err_status})"
        except Exception:
            detail = e.response.text[:200] if e.response.text else ""

        if e.response.status_code == 403:
            err = (
                f"HTTP 403 Forbidden: Google Drive API 접근이 거부되었습니다. "
                f"Google Cloud Console에서 'Google Drive API'를 활성화했는지 확인하세요. "
                f"또한 Drive 폴더가 '링크가 있는 모든 사용자'로 공유되어 있어야 합니다."
            )
            if detail:
                err += f" [상세: {detail}]"
        elif e.response.status_code == 400:
            err = f"HTTP 400: 잘못된 요청입니다. folder_id가 올바른지 확인하세요. [상세: {detail}]"
        elif e.response.status_code == 404:
            err = f"HTTP 404: 폴더를 찾을 수 없습니다. folder_id가 올바른지, 폴더가 삭제되지 않았는지 확인하세요. [상세: {detail}]"
        else:
            err = f"HTTP 오류 {e.response.status_code}: Drive API 접근 실패. [상세: {detail}]"
        logger.error(f"[Materials] {err}")
        return {"success": False, "files_synced": 0, "error": err}
    except Exception as e:
        conn.close()
        logger.error(f"[Materials] Drive 동기화 오류: {e}", exc_info=True)
        return {"success": False, "files_synced": 0, "error": str(e)}


async def sync_all_drive_folders() -> dict:
    """모든 활성 Drive 폴더 소스를 동기화"""
    conn = get_connection()
    sources = conn.execute(
        "SELECT id, name FROM material_sources WHERE source_type='drive_folder' AND is_active=1"
    ).fetchall()
    conn.close()

    results = []
    for src in sources:
        result = await sync_drive_folder(src["id"])
        result["name"] = src["name"]
        result["source_id"] = src["id"]
        results.append(result)

    total_files = sum(r.get("files_synced", 0) for r in results)
    success_count = sum(1 for r in results if r["success"])
    return {
        "total_sources": len(sources),
        "success_count": success_count,
        "total_files": total_files,
        "details": results,
    }


async def sync_all() -> dict:
    """모든 소스 동기화 (Sheets + Drive 폴더)"""
    sheets_result = await sync_all_sheets()
    drive_result = await sync_all_drive_folders()
    return {
        "sheets": sheets_result,
        "drive": drive_result,
    }


# ─────────────────────────────────────────
#  자료 검색
# ─────────────────────────────────────────
def search_materials(query: str, vendor: str = "", category: str = "",
                     price_type: str = "", limit: int = 20) -> list:
    """
    단가표 데이터에서 검색 (정확도 우선, 멀티 가격 컬럼 지원)

    query: 검색어 (품명, 모델명, 원문에서 LIKE 검색)
    vendor: 특정 제조사/유통사 필터
    category: 카테고리 필터
    price_type: 가격 유형 필터 (예: "공급가", "대리점", "채널별" → 모든 가격)
    Returns: [{"source_name", "vendor", "model_name", "price", "all_prices", "row_data", ...}]
    """
    import re
    conn = get_connection()

    conditions = []
    params = []

    if query:
        keywords = query.strip().split()
        for kw in keywords:
            conditions.append("(p.raw_text LIKE ? OR p.model_name LIKE ? OR p.product_name LIKE ?)")
            like = f"%{kw}%"
            params.extend([like, like, like])

    if vendor:
        conditions.append("p.vendor LIKE ?")
        params.append(f"%{vendor}%")

    if category:
        conditions.append("(p.category LIKE ? OR s.category LIKE ?)")
        params.extend([f"%{category}%", f"%{category}%"])

    where = " AND ".join(conditions) if conditions else "1=1"

    sql = f"""
        SELECT p.*, s.name as source_name, s.url as source_url
        FROM price_data p
        JOIN material_sources s ON p.source_id = s.id
        WHERE {where}
        LIMIT ?
    """
    params.append(limit * 5)  # 넉넉히 가져와서 재정렬

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    query_lower = query.lower().strip() if query else ""
    query_keywords = query_lower.split() if query_lower else []
    price_type_lower = price_type.lower().strip() if price_type else ""

    results = []
    for r in rows:
        try:
            row_data = json.loads(r["row_data"]) if r["row_data"] else {}
        except (json.JSONDecodeError, TypeError):
            row_data = {}

        model = r["model_name"] or ""
        product = r["product_name"] or ""
        raw = r["raw_text"] or ""
        price = r["price"] or ""

        # ★ 정확도 점수 계산
        score = 0
        model_lower = model.lower()
        product_lower = product.lower()

        if query_lower and query_lower == model_lower:
            score = 100
        elif query_lower and query_lower in model_lower:
            score = 80
        elif query_lower and model_lower and model_lower in query_lower:
            score = 70
        elif query_lower and query_lower in product_lower:
            score = 60
        elif query_keywords and all(kw in model_lower or kw in product_lower for kw in query_keywords):
            score = 50
        else:
            score = 10

        # ★ row_data에서 모든 가격 관련 컬럼 추출
        all_prices = _extract_all_prices(row_data, price_type_lower)

        # raw_text 가격 추출 (멀티 제품 행 대응)
        extracted_price = ""
        if score <= 30 and raw and query_lower:
            extracted_price = _extract_price_near_keyword(raw, query_lower)

        item = {
            "source_name": r["source_name"],
            "source_url": r["source_url"],
            "vendor": r["vendor"],
            "product_name": product,
            "model_name": model,
            "price": extracted_price if extracted_price else price,
            "all_prices": all_prices,
            "category": r["category"],
            "row_data": row_data,
            "raw_text": raw,
            "_score": score,
        }
        results.append(item)

    # 점수 높은 순 정렬
    results.sort(key=lambda x: (-x["_score"], x.get("model_name", "")))

    for item in results:
        item.pop("_score", None)

    return results[:limit]


# 가격 관련 컬럼을 식별하는 키워드
_PRICE_COL_KEYWORDS = [
    "가격", "단가", "공급가", "판매가", "대리점", "출고가", "매입가", "도매가",
    "지도가", "배송", "등록가", "권장", "딜러", "마진", "원가",
    "스토어팜", "쿠팡", "독립몰", "오픈마켓", "카드몰",
    "지마켓", "옥션", "11번가", "인터파크", "위메프", "티몬",
    "윈윈", "셀프로", "네이버",
]

# 가격이 아닌 컬럼을 제외하는 키워드
_NON_PRICE_COL_KEYWORDS = [
    "모델", "model", "품명", "품목", "상품코드", "코드", "no.", "no ",
    "분류", "카테고리", "category", "비고", "설명", "규격", "spec",
    "사양", "단위", "수량", "등록여부", "박스",
    "등록불가", "오류시",  # NEXT O열 제외
]


def _extract_all_prices(row_data: dict, price_type: str = "") -> dict:
    """
    row_data에서 모든 가격 관련 컬럼을 추출합니다.

    price_type이 지정되면 해당 키워드가 포함된 컬럼만 필터링:
      - "공급가" → 공급가가 포함된 컬럼만
      - "대리점" → 대리점이 포함된 컬럼만
      - "채널별" 또는 "" → 모든 가격 컬럼

    Returns: {"컬럼명": "가격값", ...}
    """
    import re
    prices = {}

    for col_name, val in row_data.items():
        val_str = str(val).strip()
        if not val_str:
            continue

        col_lower = col_name.lower().strip()

        # ★ 순수 숫자 헤더 제외 (복합 헤더가 깨져서 "1", "2", "3" 등으로 된 경우)
        if col_lower.isdigit():
            continue

        # 비가격 컬럼 제외
        if any(nk in col_lower for nk in _NON_PRICE_COL_KEYWORDS):
            continue

        # 가격 컬럼 판별: 1) 컬럼명에 가격 키워드 포함, 또는 2) 값이 숫자/가격 패턴
        is_price_col = any(pk in col_lower for pk in _PRICE_COL_KEYWORDS)
        is_price_val = bool(re.match(r'^[\d,]+\.?\d*$', val_str.replace(",", "").replace("원", ""))) and len(val_str) >= 3
        is_special_val = val_str in ("불가", "품절", "단종", "-")

        if is_price_col or is_price_val or is_special_val:
            # price_type 필터 적용
            if price_type and price_type != "채널별" and price_type != "전체":
                if price_type not in col_lower:
                    continue

            prices[col_name] = val_str

    return prices


def _extract_price_near_keyword(raw_text: str, keyword: str) -> str:
    """
    raw_text에서 특정 키워드 근처에 있는 가격(숫자) 추출
    한 행에 여러 제품이 나열된 시트(예: IP TIME)에서 정확한 가격 매칭용
    """
    import re
    text_lower = raw_text.lower()
    kw_pos = text_lower.find(keyword)
    if kw_pos < 0:
        return ""

    # 키워드 뒤의 텍스트에서 가격 패턴 찾기
    after_text = raw_text[kw_pos:]

    # 쉼표 포함 가격 패턴: "20,405" 또는 "1,234,567" 형식
    for m in re.finditer(r'(\d{1,3}(?:,\d{3})+)', after_text[:80]):
        price_str = m.group()
        # 가격 시작 위치 확인 - 바로 앞에 날짜 구분자(-/)가 없어야 함
        start_in_after = m.start()
        before_char = after_text[start_in_after - 1] if start_in_after > 0 else ""
        if before_char in ("-", "/", "."):
            continue  # 날짜의 일부 (예: 2025-02-24의 "024" 부분)
        # 바로 뒤에 날짜 구분자가 없어야 함
        end_pos = m.end()
        after_char = after_text[end_pos] if end_pos < len(after_text) else ""
        if after_char in ("-", "/"):
            continue  # 날짜 패턴 (예: 2025- 의 앞부분)
        return price_str

    return ""


def search_drive_documents(query: str, category: str = "", limit: int = 20) -> list:
    """
    Google Drive 문서 검색 (KC인증서, 데이터시트 등)
    - 문서유형 키워드 자동 제거 (데이터시트, 인증서 등)
    - 키워드 변형 생성 (대소문자, 구두점 제거, 부분 분리)
    - OR 검색으로 폭넓게 매칭 후 관련도순 정렬
    """
    import re as _re
    conn = get_connection()

    # ── 1) 문서유형 키워드 제거
    _DOC_TYPE_WORDS = [
        "데이터시트", "datasheet", "인증서", "certificate", "시험성적서",
        "test report", "자료", "문서", "파일", "보내줘", "필요해", "필요",
        "찾아줘", "있어", "줘", "요청", "다운로드", "보내", "찾아",
        "알려줘", "있나요", "있나", "해줘",
    ]
    cleaned = query.strip()
    cleaned_lower = cleaned.lower()
    for dtw in _DOC_TYPE_WORDS:
        cleaned_lower = cleaned_lower.replace(dtw.lower(), " ")
    cleaned = cleaned_lower.strip()

    # ── 2) 키워드 분리 및 변형 생성
    raw_keywords = [kw for kw in cleaned.split() if len(kw) >= 2]
    # 단일 문자도 허용 (예: "6" 같은 숫자는 cat.6에서 중요)
    if not raw_keywords:
        raw_keywords = [kw for kw in cleaned.split() if kw.strip()]

    all_search_terms = set()
    for kw in raw_keywords:
        all_search_terms.add(kw)
        all_search_terms.add(kw.upper())
        all_search_terms.add(kw.lower())
        # 점/하이픈/밑줄 제거 버전 (예: "cat.6" → "cat6")
        no_punct = _re.sub(r'[.\-_,]', '', kw)
        if len(no_punct) >= 2:
            all_search_terms.add(no_punct)
            all_search_terms.add(no_punct.upper())
        # 점/하이픈으로 분리된 부분 (예: "cat.6" → "cat", "6")
        parts = _re.split(r'[-._,]', kw)
        for p in parts:
            if len(p) >= 2:
                all_search_terms.add(p)
                all_search_terms.add(p.upper())
        # 접두사 "LS-" 추가 (랜스타 자체 모델명 패턴)
        if not kw.upper().startswith("LS-"):
            all_search_terms.add(f"LS-{kw.upper()}")

    if not all_search_terms and not category:
        conn.close()
        return []

    # ── 3) OR 검색
    conditions = []
    params = []

    if all_search_terms:
        kw_conditions = []
        for term in all_search_terms:
            kw_conditions.append("d.file_name LIKE ?")
            params.append(f"%{term}%")
        conditions.append(f"({' OR '.join(kw_conditions)})")

    if category:
        conditions.append("d.category LIKE ?")
        params.append(f"%{category}%")

    where = " AND ".join(conditions) if conditions else "1=1"

    sql = f"""
        SELECT d.*, s.name as source_name
        FROM drive_documents d
        LEFT JOIN material_sources s ON d.source_id = s.id
        WHERE {where}
        ORDER BY d.file_name
        LIMIT ?
    """
    params.append(limit * 3)  # 넉넉히 가져와서 정렬

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    # ── 4) 매칭 점수 계산 (원본 키워드 기준)
    results = []
    for r in rows:
        doc = dict(r)
        fname_lower = doc["file_name"].lower()
        # 원본 키워드 전부 매칭 = 높은 점수
        match_count = sum(1 for kw in raw_keywords if kw.lower() in fname_lower)
        # 보너스: 전체 키워드 매칭이면 +10
        if raw_keywords and match_count == len(raw_keywords):
            match_count += 10
        doc["_match_score"] = match_count
        results.append(doc)

    # 매칭 점수 높은 순 → 파일명 순으로 정렬
    results.sort(key=lambda x: (-x["_match_score"], x["file_name"]))

    # _match_score 제거 후 반환
    for doc in results:
        doc.pop("_match_score", None)

    return results[:limit]


def get_sync_status() -> dict:
    """전체 동기화 상태 조회"""
    conn = get_connection()
    sources = conn.execute("""
        SELECT s.*,
               (SELECT COUNT(*) FROM price_data WHERE source_id=s.id) as row_count,
               (SELECT COUNT(*) FROM drive_documents WHERE source_id=s.id) as doc_count
        FROM material_sources s
        WHERE s.is_active=1
        ORDER BY s.source_type, s.name
    """).fetchall()
    conn.close()

    return {
        "sources": [dict(s) for s in sources],
        "total_sources": len(sources),
    }


# ─────────────────────────────────────────
#  유틸
# ─────────────────────────────────────────
def get_drive_categories() -> list:
    """Drive 문서의 카테고리 목록 반환"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT category, COUNT(*) as cnt
        FROM drive_documents
        GROUP BY category
        ORDER BY cnt DESC
    """).fetchall()
    conn.close()
    return [{"category": r["category"], "count": r["cnt"]} for r in rows]


def list_drive_documents(category: str = "", query: str = "",
                         limit: int = 200, offset: int = 0) -> dict:
    """
    Drive 문서 목록 조회 (카테고리 필터 + 검색)
    - category가 있으면 해당 카테고리만 반환
    - query가 있으면 file_name LIKE 검색
    - 페이지네이션 지원
    """
    conn = get_connection()

    conditions = []
    params = []

    if category:
        conditions.append("d.category = ?")
        params.append(category)

    if query:
        keywords = query.strip().split()
        for kw in keywords:
            conditions.append("d.file_name LIKE ?")
            params.append(f"%{kw}%")

    where = " AND ".join(conditions) if conditions else "1=1"

    # 총 건수
    count_sql = f"SELECT COUNT(*) as total FROM drive_documents d WHERE {where}"
    total = conn.execute(count_sql, params).fetchone()["total"]

    # 데이터 조회
    sql = f"""
        SELECT d.*, s.name as source_name
        FROM drive_documents d
        LEFT JOIN material_sources s ON d.source_id = s.id
        WHERE {where}
        ORDER BY d.category, d.file_name
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    return {
        "total": total,
        "documents": [dict(r) for r in rows],
    }


def get_price_sheet_vendors() -> list:
    """단가표가 있는 거래처(vendor) 목록 반환"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT s.id as source_id, s.name, s.vendor, s.last_synced,
               COUNT(p.id) as row_count
        FROM material_sources s
        LEFT JOIN price_data p ON p.source_id = s.id
        WHERE s.source_type = 'sheet' AND s.is_active = 1
        GROUP BY s.id, s.name, s.vendor, s.last_synced
        ORDER BY s.vendor
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_price_sheet_tabs(source_id: int) -> list:
    """특정 소스의 시트 탭 목록 반환 (동기화된 데이터 기준)"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT sheet_tab, COUNT(*) as row_count
        FROM price_data
        WHERE source_id = ? AND sheet_tab != ''
        GROUP BY sheet_tab
        ORDER BY MIN(id)
    """, (source_id,)).fetchall()
    conn.close()
    return [{"tab": r["sheet_tab"], "row_count": r["row_count"]} for r in rows]


def get_price_sheet_data(source_id: int, query: str = "",
                         tab: str = "", limit: int = 500, offset: int = 0) -> dict:
    """
    특정 거래처의 단가표 전체 데이터 조회
    - tab: 시트 탭 필터 (빈 문자열이면 전체)
    - query가 있으면 raw_text LIKE 검색 (Ctrl+F 처럼)
    - 페이지네이션 지원
    """
    conn = get_connection()

    source = conn.execute(
        "SELECT * FROM material_sources WHERE id=?", (source_id,)
    ).fetchone()
    if not source:
        conn.close()
        return {"error": "소스를 찾을 수 없습니다.", "total": 0, "rows": [], "headers": []}

    conditions = ["p.source_id = ?"]
    params = [source_id]

    # 빈 행 제외: model_name 또는 product_name 또는 price 중 하나라도 있는 행만
    conditions.append("(p.model_name != '' OR p.product_name != '' OR p.price != '')")

    if tab:
        conditions.append("p.sheet_tab = ?")
        params.append(tab)

    if query:
        keywords = query.strip().split()
        for kw in keywords:
            conditions.append("p.raw_text LIKE ?")
            params.append(f"%{kw}%")

    where = " AND ".join(conditions)

    count_sql = f"SELECT COUNT(*) as total FROM price_data p WHERE {where}"
    total = conn.execute(count_sql, params).fetchone()["total"]

    sql = f"""
        SELECT p.id, p.row_data, p.row_colors, p.product_name, p.model_name, p.price,
               p.vendor, p.raw_text, p.sheet_tab
        FROM price_data p
        WHERE {where}
        ORDER BY p.id
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    headers = []
    parsed_rows = []
    for r in rows:
        try:
            rd = json.loads(r["row_data"]) if r["row_data"] else {}
        except (json.JSONDecodeError, TypeError):
            rd = {}
        if not headers and rd:
            headers = list(rd.keys())
        # 셀 색상 파싱
        try:
            rc = json.loads(r["row_colors"]) if r["row_colors"] else {}
        except (json.JSONDecodeError, TypeError):
            rc = {}
        parsed_rows.append({
            "id": r["id"],
            "row_data": rd,
            "row_colors": rc,
            "product_name": r["product_name"],
            "model_name": r["model_name"],
            "price": r["price"],
            "sheet_tab": r["sheet_tab"] if r["sheet_tab"] else "",
        })

    # 이 결과에 포함된 탭 목록도 반환
    tabs = get_price_sheet_tabs(source_id)

    return {
        "source_name": source["name"],
        "vendor": source["vendor"],
        "total": total,
        "headers": headers,
        "rows": parsed_rows,
        "tabs": tabs,
    }


def _extract_field(row_dict: dict, candidate_keys: list) -> str:
    """여러 후보 컬럼명 중 첫 매칭 값 반환 (순수 숫자 컬럼, 등록불가 컬럼 제외)"""
    for key in candidate_keys:
        for col_name, val in row_dict.items():
            # 순수 숫자 컬럼 스킵
            if col_name.strip().isdigit():
                continue
            # 등록불가 컬럼 스킵
            if "등록불가" in col_name or "오류시" in col_name:
                continue
            if key.lower() in col_name.lower() and val.strip():
                return val.strip()
    return ""
