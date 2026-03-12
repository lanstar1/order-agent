"""
ECOUNT ERP 판매현황 xlsx 파서
- Row 1: 메타데이터 (회사명 / 거래처명 / 기간)
- Row 2: 실제 컬럼 헤더
- ECOUNT 날짜: MM/DD-전표번호
- 소계행/합계행/타임스탬프행 자동 스킵
- 스타일시트 손상 자동 복구
"""
from __future__ import annotations
import os, re, logging, shutil, tempfile
from zipfile import ZipFile
from typing import Optional
from .schemas import SalesData, AnalysisMode

logger = logging.getLogger(__name__)

# ── 컬럼 매핑 정의 ──
COLUMN_MAP = {
    "transaction_date": ["월/일", "일자", "날짜", "거래일"],
    "product_code":     ["품목코드", "품목 코드", "자재코드"],
    "customer_name":    ["판1매처명", "거래처명", "판매처명", "고객명"],
    "product_name":     ["품명 및 규격", "품명", "품목명", "상품명"],
    "quantity":         ["수량", "판매수량", "출고수량"],
    "unit_price":       ["단가", "판매단가"],
    "supply_price":     ["공급가액", "공급가", "금액"],
    "vat":              ["부가세", "세액", "VAT"],
    "total_amount":     ["합계금액", "합계", "총액"],
    "category":         ["분류", "카테고리", "품목분류"],
}


def _safe_load_workbook(path: str):
    """openpyxl 스타일시트 손상 자동 복구"""
    import openpyxl
    try:
        return openpyxl.load_workbook(path, data_only=True, read_only=True)
    except Exception:
        logger.warning("[xlsx_parser] 스타일시트 복구 시도")
        tmp = tempfile.mktemp(suffix=".xlsx")
        try:
            with ZipFile(path, "r") as zin, ZipFile(tmp, "w") as zout:
                for item in zin.infolist():
                    data = zin.read(item.filename)
                    if item.filename == "xl/styles.xml":
                        data = _minimal_styles_xml()
                    zout.writestr(item, data)
            return openpyxl.load_workbook(tmp, data_only=True, read_only=True)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)


def _minimal_styles_xml() -> bytes:
    xfs = "".join(f'<xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>' for _ in range(100))
    xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellXfs count="100">{xfs}</cellXfs>
</styleSheet>'''
    return xml.encode("utf-8")


def _detect_ecount_format(ws) -> Optional[dict]:
    """Row 1에서 ECOUNT 메타데이터 감지"""
    row1_vals = []
    for cell in ws[1]:
        v = str(cell.value).strip() if cell.value else ""
        if v:
            row1_vals.append(v)
    full = " ".join(row1_vals)

    if "회사명" not in full and "/" not in full:
        return None

    row2_vals = []
    for cell in ws[2]:
        v = str(cell.value).strip() if cell.value else ""
        row2_vals.append(v)

    if "월/일" not in row2_vals and "품목코드" not in row2_vals:
        return None

    # 메타데이터 파싱
    meta = {"company": "", "customer_name": "", "period_start": "", "period_end": ""}
    parts = full.split("/")
    if len(parts) >= 2:
        # "회사명 : 라인업시스템(주) / (주)컴퓨존 / 2026/01/01 ~ 2026/03/12"
        for p in parts:
            p = p.strip()
            if "회사명" in p:
                m = re.search(r"회사명\s*[:：]\s*(.+)", p)
                if m:
                    meta["company"] = m.group(1).strip()
            elif "~" in p:
                # 기간 부분
                period_match = re.search(r"(\d{4}/\d{2}/\d{2})\s*~\s*(\d{4}/\d{2}/\d{2})", full)
                if period_match:
                    meta["period_start"] = period_match.group(1).replace("/", "-")
                    meta["period_end"] = period_match.group(2).replace("/", "-")

    # 기간 매치 (전체 텍스트에서)
    if not meta["period_start"]:
        period_match = re.search(r"(\d{4}/\d{2}/\d{2})\s*~\s*(\d{4}/\d{2}/\d{2})", full)
        if period_match:
            meta["period_start"] = period_match.group(1).replace("/", "-")
            meta["period_end"] = period_match.group(2).replace("/", "-")

    # 거래처명 추출: 회사명 다음 / 와 기간 사이
    cust_match = re.search(r"회사명\s*[:：]\s*[^/]+/\s*([^/]+?)(?:\s*/\s*\d{4}|\s*$)", full)
    if cust_match:
        meta["customer_name"] = cust_match.group(1).strip()

    return meta


def _find_column(headers: list[str], aliases: list[str], all_maps: dict) -> int:
    """2패스 컬럼 매칭: 정확 매치 → 부분 매치 (충돌 방지)"""
    # Pass 1: 정확 매치
    for i, h in enumerate(headers):
        if h in aliases:
            return i
    # Pass 2: 부분 매치 (충돌 체크)
    for i, h in enumerate(headers):
        for alias in aliases:
            if alias in h or h in alias:
                # 다른 필드의 정확 매치가 이 컬럼을 차지하지 않는지 확인
                collision = False
                for other_key, other_aliases in all_maps.items():
                    if other_aliases is aliases:
                        continue
                    if h in other_aliases:
                        collision = True
                        break
                if not collision:
                    return i
    return -1


def _parse_ecount_date(cell_val: str, year: str) -> str:
    """ECOUNT 날짜 변환: '03/12-1234' → '2026-03-12'"""
    m = re.match(r"(\d{2})/(\d{2})", str(cell_val).strip())
    if m:
        return f"{year}-{m.group(1)}-{m.group(2)}"
    return ""


def parse_xlsx(file_path: str, mode: str = "multi",
               target_customer_code: Optional[str] = None) -> SalesData:
    """xlsx 파일을 파싱하여 SalesData 반환"""
    wb = _safe_load_workbook(file_path)
    ws = wb.active

    analysis_mode = AnalysisMode.SINGLE if mode == "single" else AnalysisMode.MULTI

    # ECOUNT 포맷 감지
    ecount_meta = _detect_ecount_format(ws)
    is_ecount = ecount_meta is not None

    if is_ecount:
        return _parse_ecount(ws, ecount_meta, analysis_mode, target_customer_code, file_path)
    else:
        return _parse_standard(ws, analysis_mode, target_customer_code, file_path)


def _parse_ecount(ws, meta: dict, mode: AnalysisMode,
                  target_code: Optional[str], file_path: str) -> SalesData:
    """ECOUNT ERP 판매현황 포맷 파싱"""
    # Row 2 = 헤더
    headers = [str(c.value).strip() if c.value else "" for c in ws[2]]
    col = {}
    for key, aliases in COLUMN_MAP.items():
        idx = _find_column(headers, aliases, COLUMN_MAP)
        if idx >= 0:
            col[key] = idx

    # 연도 추출
    year = meta.get("period_start", "")[:4] or "2026"

    transactions = []
    skip_patterns = re.compile(r"계$|합계|^\d{4}/\d{2}/\d{2}\s*\(")

    for row_idx, row in enumerate(ws.iter_rows(min_row=3), start=3):
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
            if v is None:
                v = "" if key in ("product_code", "customer_name", "product_name", "category") else 0
            tx[key] = v

        # supply_price → total_amount 매핑 (ECOUNT H열)
        if "supply_price" in tx and "total_amount" not in tx:
            tx["total_amount"] = tx["supply_price"]

        transactions.append(tx)

    # 거래처 정보 구성
    customer_name = meta.get("customer_name", "")
    customers = []
    customer_set = set()
    product_set = set()

    for tx in transactions:
        cn = tx.get("customer_name", "") or customer_name
        if cn:
            tx["customer_name"] = cn
        if cn and cn not in customer_set:
            customer_set.add(cn)
            customers.append({"customer_name": cn, "customer_code": cn})
        pc = tx.get("product_code", "")
        if pc:
            product_set.add(pc)

    # 총매출액: 마지막 합계행에서 추출
    total_amount = 0
    for row in ws.iter_rows(min_row=3):
        vals = [c.value for c in row]
        if vals and vals[0] and "합계" in str(vals[0]):
            sp_idx = col.get("supply_price", col.get("total_amount", -1))
            if sp_idx >= 0 and sp_idx < len(vals) and vals[sp_idx]:
                try:
                    total_amount = int(float(str(vals[sp_idx]).replace(",", "")))
                except (ValueError, TypeError):
                    pass

    if total_amount == 0:
        total_amount = sum(
            int(float(str(tx.get("total_amount", tx.get("supply_price", 0))).replace(",", "")))
            for tx in transactions
            if tx.get("total_amount") or tx.get("supply_price")
        )

    return SalesData(
        transactions=transactions,
        customers=customers,
        products=list({"product_code": pc} for pc in product_set),
        period_start=meta.get("period_start", ""),
        period_end=meta.get("period_end", ""),
        analysis_mode=mode,
        target_customer_code=target_code,
        target_customer_name=customer_name if mode == AnalysisMode.SINGLE else None,
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
            tx[key] = v if v is not None else ("" if key in ("product_code", "customer_name", "product_name") else 0)
        transactions.append(tx)
        cn = tx.get("customer_name", "")
        if cn:
            customer_set.add(cn)
        pc = tx.get("product_code", "")
        if pc:
            product_set.add(pc)

    total_amount = sum(
        int(float(str(tx.get("total_amount", tx.get("supply_price", 0))).replace(",", "")))
        for tx in transactions
        if tx.get("total_amount") or tx.get("supply_price")
    )

    customers = [{"customer_name": cn, "customer_code": cn} for cn in customer_set]

    return SalesData(
        transactions=transactions,
        customers=customers,
        products=list({"product_code": pc} for pc in product_set),
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
