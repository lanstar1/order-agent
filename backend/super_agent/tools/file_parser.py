"""
파일 파서 — CSV, Excel, PDF, 텍스트 등 다양한 포맷 파싱
"""
import os
import json
import logging
import csv
import io
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_file(file_path: str, mime_type: str = "") -> Dict[str, Any]:
    """
    파일을 파싱하여 구조화된 데이터 반환
    Returns: {type, row_count, columns, data_preview, full_text, raw_data}
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"파일 없음: {file_path}", "type": "error"}

    ext = path.suffix.lower()

    try:
        if ext in ('.csv', '.tsv'):
            return _parse_csv(path, ext)
        elif ext in ('.xlsx', '.xls'):
            return _parse_excel(path)
        elif ext == '.json':
            return _parse_json(path)
        elif ext in ('.txt', '.md', '.log'):
            return _parse_text(path)
        elif ext == '.pdf':
            return _parse_pdf(path)
        else:
            return _parse_text(path)
    except Exception as e:
        logger.error(f"[FileParser] 파싱 실패 {file_path}: {e}")
        return {"error": str(e), "type": "error"}


def _parse_csv(path: Path, ext: str) -> Dict[str, Any]:
    """CSV/TSV 파싱"""
    delimiter = '\t' if ext == '.tsv' else ','

    # 인코딩 시도
    for encoding in ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig']:
        try:
            with open(path, 'r', encoding=encoding) as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue
    else:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    rows = list(reader)

    if not rows:
        return {"type": "csv", "row_count": 0, "columns": [], "data": [], "full_text": ""}

    columns = rows[0]
    data_rows = rows[1:]

    # 데이터를 딕셔너리 리스트로 변환
    data = []
    for row in data_rows:
        if len(row) >= len(columns):
            data.append(dict(zip(columns, row[:len(columns)])))
        else:
            padded = row + [''] * (len(columns) - len(row))
            data.append(dict(zip(columns, padded)))

    # 미리보기 (처음 5행)
    preview = data[:5]

    # 숫자 컬럼 통계
    stats = _compute_column_stats(columns, data)

    return {
        "type": "csv",
        "row_count": len(data),
        "columns": columns,
        "data": data,
        "data_preview": preview,
        "column_stats": stats,
        "full_text": content[:50000],  # 최대 50KB 텍스트
    }


def _parse_excel(path: Path) -> Dict[str, Any]:
    """Excel 파싱"""
    try:
        import openpyxl
    except ImportError:
        return {"error": "openpyxl 미설치", "type": "error"}

    wb = openpyxl.load_workbook(str(path), data_only=True, read_only=True)
    sheet = wb.active

    rows = []
    for row in sheet.iter_rows(values_only=True):
        rows.append([str(cell) if cell is not None else '' for cell in row])
    wb.close()

    if not rows:
        return {"type": "excel", "row_count": 0, "columns": [], "data": []}

    columns = rows[0]
    data_rows = rows[1:]

    data = []
    for row in data_rows:
        if any(cell.strip() for cell in row):  # 빈 행 건너뛰기
            d = dict(zip(columns, row[:len(columns)]))
            data.append(d)

    stats = _compute_column_stats(columns, data)

    return {
        "type": "excel",
        "row_count": len(data),
        "columns": columns,
        "data": data,
        "data_preview": data[:5],
        "column_stats": stats,
        "full_text": _data_to_text(columns, data[:100]),
    }


def _parse_json(path: Path) -> Dict[str, Any]:
    """JSON 파싱"""
    with open(path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    if isinstance(raw, list):
        columns = list(raw[0].keys()) if raw else []
        return {
            "type": "json",
            "row_count": len(raw),
            "columns": columns,
            "data": raw,
            "data_preview": raw[:5],
            "full_text": json.dumps(raw[:100], ensure_ascii=False, indent=2),
        }
    else:
        return {
            "type": "json",
            "row_count": 1,
            "columns": list(raw.keys()) if isinstance(raw, dict) else [],
            "data": [raw] if isinstance(raw, dict) else [{"value": raw}],
            "full_text": json.dumps(raw, ensure_ascii=False, indent=2),
        }


def _parse_text(path: Path) -> Dict[str, Any]:
    """텍스트 파싱"""
    for encoding in ['utf-8', 'cp949', 'euc-kr']:
        try:
            with open(path, 'r', encoding=encoding) as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue
    else:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

    lines = content.strip().split('\n')
    return {
        "type": "text",
        "row_count": len(lines),
        "columns": [],
        "data": [],
        "full_text": content[:100000],
    }


def _parse_pdf(path: Path) -> Dict[str, Any]:
    """PDF 텍스트 추출"""
    try:
        import pdfplumber
        text_parts = []
        with pdfplumber.open(str(path)) as pdf:
            for page in pdf.pages[:50]:  # 최대 50페이지
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        full_text = '\n\n'.join(text_parts)
        return {
            "type": "pdf",
            "row_count": len(text_parts),
            "columns": [],
            "data": [],
            "full_text": full_text[:100000],
        }
    except ImportError:
        return {"type": "pdf", "error": "pdfplumber 미설치", "full_text": ""}


def _compute_column_stats(columns: List[str], data: List[Dict]) -> Dict[str, Any]:
    """컬럼별 간단한 통계"""
    stats = {}
    for col in columns:
        values = [row.get(col, '') for row in data if row.get(col, '')]
        numeric_vals = []
        for v in values:
            try:
                numeric_vals.append(float(str(v).replace(',', '')))
            except (ValueError, TypeError):
                pass

        col_stat = {"count": len(values), "unique": len(set(values))}
        if numeric_vals:
            col_stat["is_numeric"] = True
            col_stat["min"] = min(numeric_vals)
            col_stat["max"] = max(numeric_vals)
            col_stat["sum"] = sum(numeric_vals)
            col_stat["avg"] = sum(numeric_vals) / len(numeric_vals)
        else:
            col_stat["is_numeric"] = False
            col_stat["sample_values"] = list(set(values))[:5]

        stats[col] = col_stat
    return stats


def _data_to_text(columns: List[str], data: List[Dict]) -> str:
    """데이터를 텍스트로 변환"""
    lines = ['\t'.join(columns)]
    for row in data:
        lines.append('\t'.join(str(row.get(c, '')) for c in columns))
    return '\n'.join(lines)
