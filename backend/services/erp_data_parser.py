"""ECOUNT ERP 구매현황/판매현황 엑셀·CSV 파서
구매현황: xlsx (첫 행 메타데이터, 2행부터 데이터)
판매현황: csv (UTF-8-BOM, 마지막 합계행 제거)
"""
import re
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_erp_purchase(filepath: str) -> dict:
    """구매현황 엑셀 파싱 (.xlsx)
    Returns: {"items": [...], "total": int, "meta": str}
    """
    import pandas as pd

    ext = Path(filepath).suffix.lower()

    if ext == '.csv':
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(filepath, encoding='euc-kr')
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, encoding='cp949')
        # CSV has headers in row 0 directly
        header_row = 0
        meta_info = ""
    else:
        # xlsx: row 0 = metadata, row 1 = headers
        try:
            df = pd.read_excel(filepath, engine='calamine', header=None)
        except Exception:
            df = pd.read_excel(filepath, header=None)

        meta = str(df.iloc[0, 0]) if len(df) > 0 else ""
        # Find header row (row with '품목코드')
        header_idx = None
        for i in range(min(5, len(df))):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v)]
            if '품목코드' in row_vals:
                header_idx = i
                break

        if header_idx is None:
            header_idx = 1  # default

        df.columns = df.iloc[header_idx].values
        df = df.iloc[header_idx + 1:].reset_index(drop=True)
        meta_info = meta

    # Normalize column names (strip whitespace)
    df.columns = [str(c).strip() if pd.notna(c) else f"col_{i}" for i, c in enumerate(df.columns)]

    # Remove summary/empty rows
    df = df[df['품목코드'].notna() & (df['품목코드'] != '')]
    # Remove rows where 품목코드 looks like a summary
    df = df[~df['품목코드'].astype(str).str.contains('계|합계|총', na=False)]

    items = []
    for _, row in df.iterrows():
        date_raw = str(row.get('월/일', '')).strip()
        if not date_raw or date_raw == 'nan':
            continue

        items.append({
            "date": date_raw,
            "prod_cd": str(row.get('품목코드', '')).strip(),
            "prod_name": str(row.get('품명 및 모델', row.get('품명 및 규격', ''))).strip(),
            "qty": _safe_int(row.get('수량', 0)),
            "unit_price": _safe_float(row.get('단가', 0)),
            "partner_price": _safe_float(row.get('파트너가', 0)),
            "inbound_price": _safe_float(row.get('입고단가', 0)),
            "supply_amt": _safe_float(row.get('공급가액', 0)),
            "vat": _safe_float(row.get('부가세', 0)),
            "total": _safe_float(row.get('합 계', row.get('합계', 0))),
            "cust_name": str(row.get('구매처명', '')).strip(),
            "group1": str(row.get('품목그룹1명', '')).strip(),
            "group2": str(row.get('품목그룹2명', '')).strip(),
            "warehouse": str(row.get('창고명', row.get('창고', ''))).strip(),
            "model_name": str(row.get('모델명', '')).strip(),
            "remarks": str(row.get('비고사항', '')).strip(),
        })

    return {
        "items": items,
        "total": len(items),
        "meta": meta_info if ext != '.csv' else "",
        "type": "purchase",
    }


def parse_erp_sales(filepath: str) -> dict:
    """판매현황 CSV/엑셀 파싱
    Returns: {"items": [...], "total": int}
    """
    import pandas as pd

    ext = Path(filepath).suffix.lower()

    if ext == '.csv':
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(filepath, encoding='euc-kr')
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, encoding='cp949')
    else:
        try:
            df = pd.read_excel(filepath, engine='calamine', header=None)
        except Exception:
            df = pd.read_excel(filepath, header=None)

        # Find header row
        header_idx = None
        for i in range(min(5, len(df))):
            row_vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v)]
            if '품목코드' in row_vals:
                header_idx = i
                break
        if header_idx is None:
            header_idx = 0

        df.columns = df.iloc[header_idx].values
        df = df.iloc[header_idx + 1:].reset_index(drop=True)

    df.columns = [str(c).strip() if pd.notna(c) else f"col_{i}" for i, c in enumerate(df.columns)]

    # Remove summary rows (합계, 총합계, timestamp rows)
    df = df[df['품목코드'].notna() & (df['품목코드'] != '')]
    # The date column '연/월/일' has format like "20260302-1", summary rows have "2026/03 계" etc
    date_col = '연/월/일' if '연/월/일' in df.columns else '월/일'
    if date_col in df.columns:
        df = df[df[date_col].astype(str).str.match(r'^\d{8}-\d+$|^\d{2}/\d{2}-\d+$', na=False)]

    items = []
    for _, row in df.iterrows():
        date_raw = str(row.get(date_col, '')).strip()
        if not date_raw or date_raw == 'nan':
            continue

        items.append({
            "date": date_raw,
            "prod_cd": str(row.get('품목코드', '')).strip(),
            "prod_name": str(row.get('품명 및 규격', row.get('품명 및 모델', ''))).strip(),
            "model_name": str(row.get('모델명', '')).strip(),
            "cust_name": str(row.get('거래처명', row.get('판매처명', ''))).strip(),
            "qty": _safe_int(row.get('수량', 0)),
            "unit_price": _safe_float(row.get('단가', 0)),
            "supply_amt": _safe_float(row.get('공급가액', 0)),
            "vat": _safe_float(row.get('부가세', 0)),
            "total": _safe_float(row.get('합 계', row.get('합계', 0))),
            "inbound_price": _safe_float(row.get('입고단가', 0)),
            "warehouse": str(row.get('창고', row.get('창고명', ''))).strip(),
            "group1": str(row.get('품목그룹1명', '')).strip(),
            "remarks": str(row.get('비고사항', '')).strip(),
        })

    return {
        "items": items,
        "total": len(items),
        "type": "sales",
    }


def _safe_float(val) -> float:
    if val is None:
        return 0.0
    try:
        import math
        v = float(val)
        return 0.0 if math.isnan(v) else v
    except (ValueError, TypeError):
        try:
            return float(str(val).replace(",", ""))
        except:
            return 0.0


def _safe_int(val) -> int:
    f = _safe_float(val)
    return int(f)
