"""
ECOUNT ERP 판매현황 xlsx 파서
- Row 1: 메타데이터 (회사명 / 거래처명 / 기간)  — A1 셀 하나에 슬래시 구분
- Row 2: 실제 컬럼 헤더 (오타 포함: '판1매처명', '합 계' 등)
- ECOUNT 날짜: MM/DD-전표번호
- 소계행(XX 계)/합계행(총합계)/타임스탬프행 자동 스킵
- 스타일시트 손상 자동 복구 (ECOUNT 특유의 aRGB 오류)
"""
from __future__ import annotations
import os, re, logging, tempfile
from zipfile import ZipFile
from typing import Optional
from .schemas import SalesData, AnalysisMode

logger = logging.getLogger(__name__)

# ── 컬럼 매핑 정의 (ECOUNT 오타/공백 변형 포함) ──
COLUMN_MAP = {
    "transaction_date": ["월/일", "일자", "날짜", "거래일"],
    "product_code":     ["품목코드", "품목 코드", "자재코드"],
    "customer_name":    ["판매처명", "판1매처명", "거래처명", "고객명", "거래처"],
    "product_name":     ["품명 및 규격", "품명", "품목명", "상품명"],
    "model_name":       ["모델명"],
    "quantity":         ["수량", "판매수량", "출고수량"],
    "unit_price":       ["단가", "판매단가"],
    "supply_price":     ["공급가액", "공급가", "금액"],
    "vat":              ["부가세", "세액", "VAT"],
    "total_amount":     ["합 계", "합계금액", "합계", "총액"],
    "category":         ["분류", "카테고리", "품목분류", "품목그룹1명"],
    "warehouse":        ["창고"],
}

# ── 스타일시트 복구용 tmp 파일 추적 ──
_tmp_files: list[str] = []


def _safe_load_workbook(path: str):
    """openpyxl 스타일시트 손상 자동 복구 (ECOUNT aRGB 오류 대응)"""
    import openpyxl
    try:
        return openpyxl.load_workbook(path, data_only=True)
    except Exception as e:
        logger.warning(f"[xlsx_parser] 스타일시트 손상 감지, 복구 시도: {e}")
        tmp = tempfile.mktemp(suffix=".xlsx")
        with ZipFile(path, "r") as zin, ZipFile(tmp, "w") as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == "xl/styles.xml":
                    data = _minimal_styles_xml()
                zout.writestr(item, data)
        # tmp 파일은 workbook close 후 삭제해야 하므로 추적 리스트에 보관
        _tmp_files.append(tmp)
        return openpyxl.load_workbook(tmp, data_only=True)


def _cleanup_tmp():
    """임시 파일 정리"""
    for p in _tmp_files:
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
    _tmp_files.clear()


def _minimal_styles_xml() -> bytes:
    xfs = "".join('<xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>' for _ in range(100))
    xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellXfs count="100">{xfs}</cellXfs>
</styleSheet>'''
    return xml.encode("utf-8")


def _detect_ecount_format(ws) -> Optional[dict]:
    """Row 1에서 ECOUNT 메타데이터 감지
    예: '회사명 : 라인업시스템(주) / (주)컴퓨존 / 2026/01/01  ~ 2026/03/12'
    """
    row1_vals = []
    for cell in ws[1]:
        v = str(cell.value).strip() if cell.value else ""
        if v:
            row1_vals.append(v)
    full = " ".join(row1_vals)

    if "회사명" not in full:
        return None

    row2_vals = []
    for cell in ws[2]:
        v = str(cell.value).strip() if cell.value else ""
        row2_vals.append(v)

    if "월/일" not in row2_vals and "품목코드" not in row2_vals:
        return None

    # 메타데이터 파싱
    meta = {"company": "", "customer_name": "", "period_start": "", "period_end": ""}

    # 기간 추출 (전체 텍스트에서)
    period_match = re.search(r"(\d{4}/\d{2}/\d{2})\s*~\s*(\d{4}/\d{2}/\d{2})", full)
    if period_match:
        meta["period_start"] = period_match.group(1).replace("/", "-")
        meta["period_end"] = period_match.group(2).replace("/", "-")

    # 회사명 추출
    company_match = re.search(r"회사명\s*[:：]\s*(.+?)(?:\s*/|$)", full)
    if company_match:
        meta["company"] = company_match.group(1).strip()

    # 거래처명 추출: "회사명 : XXX / 거래처명 / 기간"
    # 슬래시로 분리 후, 회사명 뒤 & 기간(YYYY/) 앞의 부분
    slash_parts = [p.strip() for p in full.split("/") if p.strip()]
    # 날짜 부분이 아닌 슬래시 구분을 찾아야 함
    # "회사명 : 라인업시스템(주) / (주)컴퓨존 / 2026/01/01  ~ 2026/03/12"
    # → split("/") = ["회사명 : 라인업시스템(주) ", " (주)컴퓨존 ", " 2026", "01", "01  ~ 2026", "03", "12"]
    # 정규식으로 추출: 회사명 다음부터 날짜 시작 전까지
    cust_match = re.search(
        r"회사명\s*[:：]\s*.+?\)\s*/\s*(.+?)\s*/\s*\d{4}",
        full
    )
    if cust_match:
        meta["customer_name"] = cust_match.group(1).strip()
    else:
        # 다른 패턴 시도: "회사명 : XXX / YYY / 기간"
        cust_match2 = re.search(
            r"회사명\s*[:：]\s*[^/]+/\s*([^/]+?)\s*/\s*\d{4}",
            full
        )
        if cust_match2:
            meta["customer_name"] = cust_match2.group(1).strip()

    return meta


def _find_column(headers: list[str], aliases: list[str], all_maps: dict) -> int:
    """2패스 컬럼 매칭: 정확 매치 → 부분 매치 (충돌 방지)
    공백/특수문자 정규화 후 비교도 수행
    """
    # 정규화 함수
    def norm(s: str) -> str:
        return re.sub(r"\s+", "", s).lower()

    # Pass 1: 정확 매치 (공백 정규화 포함)
    for i, h in enumerate(headers):
        if not h:
            continue
        h_norm = norm(h)
        for alias in aliases:
            if h == alias or h_norm == norm(alias):
                return i

    # Pass 2: 부분 매치 (충돌 체크)
    for i, h in enumerate(headers):
        if not h:
            continue
        h_norm = norm(h)
        for alias in aliases:
            a_norm = norm(alias)
            if a_norm in h_norm or h_norm in a_norm:
                collision = False
                for other_key, other_aliases in all_maps.items():
                    if other_aliases is aliases:
                        continue
                    for oa in other_aliases:
                        if norm(oa) == h_norm:
                            collision = True
                            break
                    if collision:
                        break
                if not collision:
                    return i
    return -1


def _parse_ecount_date(cell_val: str, year: str) -> str:
    """ECOUNT 날짜 변환: '01/02-27' → '2026-01-02'"""
    m = re.match(r"(\d{2})/(\d{2})", str(cell_val).strip())
    if m:
        return f"{year}-{m.group(1)}-{m.group(2)}"
    return ""


def _safe_numeric(val) -> float:
    """숫자 변환 (None, 문자열, 콤마 포함 대응)"""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0


def parse_xlsx(file_path: str, mode: str = "multi",
               target_customer_code: Optional[str] = None) -> SalesData:
    """xlsx 파일을 파싱하여 SalesData 반환"""
    wb = _safe_load_workbook(file_path)
    try:
        ws = wb.active
        analysis_mode = AnalysisMode.SINGLE if mode == "single" else AnalysisMode.MULTI

        # ECOUNT 포맷 감지
        ecount_meta = _detect_ecount_format(ws)
        is_ecount = ecount_meta is not None

        if is_ecount:
            result = _parse_ecount(ws, ecount_meta, analysis_mode, target_customer_code, file_path)
        else:
            result = _parse_standard(ws, analysis_mode, target_customer_code, file_path)

        return result
    finally:
        wb.close()
        _cleanup_tmp()


def _parse_ecount(ws, meta: dict, mode: AnalysisMode,
                  target_code: Optional[str], file_path: str) -> SalesData:
    """ECOUNT ERP 판매현황 포맷 파싱

    핵심 컬럼:
    A=월/일, B=품목코드, C=판1매처명(거래처), D=품명 및 규격, E=모델명,
    F=수량, G=단가, H=공급가액, I=부가세, J=합 계(매출액)
    """
    # Row 2 = 헤더
    headers = [str(c.value).strip() if c.value else "" for c in ws[2]]
    logger.info(f"[xlsx_parser] ECOUNT 헤더: {headers[:12]}")

    col = {}
    for key, aliases in COLUMN_MAP.items():
        idx = _find_column(headers, aliases, COLUMN_MAP)
        if idx >= 0:
            col[key] = idx
    logger.info(f"[xlsx_parser] 컬럼 매핑: {col}")

    # 연도 추출
    year = meta.get("period_start", "")[:4] or "2026"

    # 데이터에서 거래처명을 읽을 수 있는지 (C열 매핑 여부)
    has_customer_col = "customer_name" in col

    transactions = []
    skip_patterns = re.compile(r"계$|합계|총합계|^\d{4}/\d{2}/\d{2}\s*\(")

    for row in ws.iter_rows(min_row=3):
        vals = [c.value for c in row]
        if not vals or not vals[0]:
            continue
        a_val = str(vals[0]).strip()
        if skip_patterns.search(a_val):
            continue

        date_val = _parse_ecount_date(a_val, year)
        if not date_val:
            continue

        tx = {"transaction_date": date_val}

        for key, idx in col.items():
            if key == "transaction_date":
                continue
            v = vals[idx] if idx < len(vals) else None

            if key in ("quantity", "unit_price", "supply_price", "vat", "total_amount"):
                tx[key] = _safe_numeric(v)
            else:
                tx[key] = str(v).strip() if v else ""

        # 거래처명이 컬럼에 없으면 메타데이터에서 가져오기
        if not has_customer_col or not tx.get("customer_name"):
            tx["customer_name"] = meta.get("customer_name", "")

        transactions.append(tx)

    # 거래처/품목 집계
    customers = []
    customer_set = set()
    product_set = set()

    for tx in transactions:
        cn = tx.get("customer_name", "")
        if cn and cn not in customer_set:
            customer_set.add(cn)
            customers.append({"customer_name": cn, "customer_code": cn})
        pc = tx.get("product_code", "")
        if pc:
            product_set.add(pc)

    # 총매출액: '총합계' 행에서 추출 (J열 = total_amount, 없으면 H열 = supply_price)
    total_amount = 0
    ta_idx = col.get("total_amount", -1)     # J열 (합 계)
    sp_idx = col.get("supply_price", -1)     # H열 (공급가액)

    for row in ws.iter_rows(min_row=3):
        vals = [c.value for c in row]
        if not vals or not vals[0]:
            continue
        a_val = str(vals[0]).strip()
        if a_val == "총합계":
            # J열 우선, H열 폴백
            if ta_idx >= 0 and ta_idx < len(vals) and vals[ta_idx]:
                total_amount = int(_safe_numeric(vals[ta_idx]))
            elif sp_idx >= 0 and sp_idx < len(vals) and vals[sp_idx]:
                total_amount = int(_safe_numeric(vals[sp_idx]))
            break

    # 총합계 행이 없으면 트랜잭션에서 합산
    if total_amount == 0:
        total_amount = int(sum(
            tx.get("total_amount", tx.get("supply_price", 0))
            for tx in transactions
        ))

    logger.info(f"[xlsx_parser] ECOUNT 파싱 완료: {len(transactions)}건, "
                f"거래처 {len(customer_set)}개, 품목 {len(product_set)}개, "
                f"매출액 {total_amount:,}원")

    return SalesData(
        transactions=transactions,
        customers=customers,
        products=[{"product_code": pc} for pc in product_set],
        period_start=meta.get("period_start", ""),
        period_end=meta.get("period_end", ""),
        analysis_mode=mode,
        target_customer_code=target_code,
        target_customer_name=meta.get("customer_name", "") if mode == AnalysisMode.SINGLE else None,
        file_name=os.path.basename(file_path),
        total_rows=len(transactions),
        total_customers=len(customer_set),
        total_products=len(product_set),
        total_amount=total_amount,
    )


def _parse_standard(ws, mode: AnalysisMode,
                    target_code: Optional[str], file_path: str) -> SalesData:
    """표준 포맷 파싱 (Row 1 = 헤더)"""
    headers = [str(c.value).strip() if c.value else "" for c in ws[1]]
    col = {}
    for key, aliases in COLUMN_MAP.items():
        idx = _find_column(headers, aliases, COLUMN_MAP)
        if idx >= 0:
            col[key] = idx

    transactions = []
    customer_set = set()
    product_set = set()

    for row in ws.iter_rows(min_row=2):
        vals = [c.value for c in row]
        if not vals or not any(vals):
            continue
        tx = {}
        for key, idx in col.items():
            v = vals[idx] if idx < len(vals) else None
            if key in ("quantity", "unit_price", "supply_price", "vat", "total_amount"):
                tx[key] = _safe_numeric(v)
            else:
                tx[key] = str(v).strip() if v is not None else ""
        transactions.append(tx)

        cn = tx.get("customer_name", "")
        if cn:
            customer_set.add(cn)
        pc = tx.get("product_code", "")
        if pc:
            product_set.add(pc)

    total_amount = int(sum(
        tx.get("total_amount", tx.get("supply_price", 0))
        for tx in transactions
    ))

    customers = [{"customer_name": cn, "customer_code": cn} for cn in customer_set]

    return SalesData(
        transactions=transactions,
        customers=customers,
        products=[{"product_code": pc} for pc in product_set],
        period_start="",
        period_end="",
        analysis_mode=mode,
        target_customer_code=target_code,
        file_name=os.path.basename(file_path),
        total_rows=len(transactions),
        total_customers=len(customer_set),
        total_products=len(product_set),
        total_amount=total_amount,
    )
