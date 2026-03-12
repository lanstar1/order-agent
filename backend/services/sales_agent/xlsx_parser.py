"""
판매 데이터 xlsx 파싱 및 전처리
- 다양한 컬럼명 자동 매핑
- 거래처/품목 마스터 자동 추출
- SalesData 구조로 변환
"""
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional
import openpyxl

logger = logging.getLogger(__name__)

# ── 컬럼명 매핑 (다양한 한글/영문 컬럼명 지원) ──
COLUMN_ALIASES = {
    "transaction_date": ["거래일", "판매일", "일자", "날짜", "date", "거래일자", "판매일자", "전표일자", "매출일", "매출일자"],
    "customer_code": ["거래처코드", "고객코드", "업체코드", "cust_code", "거래처 코드", "거래처CD"],
    "customer_name": ["거래처명", "고객명", "업체명", "거래처", "고객", "cust_name", "거래처 명"],
    "product_code": ["품목코드", "상품코드", "제품코드", "item_code", "품목 코드", "품목CD"],
    "product_name": ["품목명", "상품명", "제품명", "품목", "item_name", "품명", "품목 명"],
    "category": ["카테고리", "분류", "대분류", "품목분류", "구분", "품목군"],
    "quantity": ["수량", "판매수량", "qty", "수 량", "판매 수량"],
    "unit_price": ["단가", "판매단가", "price", "판매가", "매출단가"],
    "supply_price": ["공급가", "원가", "매입가", "cost", "공급가액", "매입단가"],
    "total_amount": ["합계금액", "합계", "판매금액", "금액", "amount", "합계 금액", "매출액", "공급가액합계"],
    "vat": ["부가세", "세액", "vat", "부가가치세"],
    "grand_total": ["총액", "총합계", "합산금액", "총 금액"],
    "sales_rep": ["담당영업", "담당자", "영업담당", "담당", "영업사원", "영업담당자"],
    "payment_terms": ["결제조건", "결제방식", "지급조건"],
    "notes": ["비고", "메모", "참고", "note", "비 고"],
    # 거래처 마스터 추가 필드
    "industry": ["업종", "업종분류", "산업"],
    "company_size": ["규모", "기업규모", "회사규모"],
    "region": ["지역", "소재지", "지역구분"],
    "contact_name": ["담당자명", "구매담당", "구매담당자"],
    "contact_phone": ["연락처", "전화번호", "전화"],
    "contact_email": ["이메일", "email", "메일"],
    "contract_type": ["계약유형", "거래유형", "계약형태"],
    "first_deal_date": ["거래시작일", "최초거래일", "첫거래일"],
    # 품목 마스터 추가 필드
    "sub_category": ["소분류", "서브카테고리", "세분류"],
    "brand": ["브랜드", "제조사", "메이커", "벤더"],
    "standard_cost": ["표준원가", "기준원가", "최신매입가"],
    "current_stock": ["현재고", "재고수량", "재고"],
    "lead_time_days": ["리드타임", "입고일수", "발주리드타임"],
}


def _find_column(header_row: list, target_key: str) -> Optional[int]:
    """헤더 행에서 대상 키에 매핑되는 컬럼 인덱스를 찾는다."""
    aliases = COLUMN_ALIASES.get(target_key, [target_key])
    for idx, cell_val in enumerate(header_row):
        if cell_val is None:
            continue
        cell_str = str(cell_val).strip()
        # 정확한 매칭
        if cell_str in aliases or cell_str.lower() == target_key:
            return idx
        # 부분 매칭 (포함 검사)
        for alias in aliases:
            if alias in cell_str or cell_str in alias:
                return idx
    return None


def _parse_date(val) -> Optional[str]:
    """다양한 날짜 형식을 YYYY-MM-DD 문자열로 변환"""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%m/%d/%Y", "%d/%m/%Y"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def _parse_int(val) -> int:
    """숫자값을 int로 변환 (쉼표, 원 기호 등 제거)"""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip().replace(",", "").replace("원", "").replace("₩", "").replace(" ", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def parse_xlsx(file_path: str) -> dict:
    """
    xlsx 파일을 파싱하여 판매 데이터 구조로 변환한다.

    Returns:
        {
            "transactions": [...],
            "customers": [...],
            "products": [...],
            "period_start": "YYYY-MM-DD",
            "period_end": "YYYY-MM-DD",
            "file_name": "...",
            "summary": {"total_rows": N, "total_customers": N, "total_products": N, ...}
        }
    """
    fp = Path(file_path)
    if not fp.exists():
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

    wb = openpyxl.load_workbook(str(fp), data_only=True, read_only=True)
    result = {
        "transactions": [],
        "customers": {},
        "products": {},
        "file_name": fp.name,
    }

    # ── 시트 탐색: 판매 트랜잭션, 거래처 마스터, 품목 마스터 ──
    tx_sheet = None
    cust_sheet = None
    prod_sheet = None

    tx_keywords = ["판매", "매출", "트랜잭션", "거래", "sales", "transaction"]
    cust_keywords = ["거래처", "고객", "customer", "업체"]
    prod_keywords = ["품목", "상품", "제품", "product", "item"]

    for sn in wb.sheetnames:
        sn_lower = sn.lower().strip()
        if any(k in sn_lower for k in cust_keywords) and "판매" not in sn_lower:
            cust_sheet = wb[sn]
        elif any(k in sn_lower for k in prod_keywords) and "판매" not in sn_lower:
            prod_sheet = wb[sn]
        elif tx_sheet is None:
            # 첫 번째 시트이거나 판매 관련 키워드 포함 시
            if any(k in sn_lower for k in tx_keywords) or tx_sheet is None:
                tx_sheet = wb[sn]

    if tx_sheet is None:
        tx_sheet = wb[wb.sheetnames[0]]

    # ── 판매 트랜잭션 파싱 ──
    rows = list(tx_sheet.iter_rows(values_only=True))
    if len(rows) < 2:
        wb.close()
        raise ValueError("데이터가 부족합니다 (최소 헤더 + 1행 필요)")

    header = [str(c).strip() if c else "" for c in rows[0]]

    # 컬럼 매핑
    col_map = {}
    for key in ["transaction_date", "customer_code", "customer_name", "product_code",
                 "product_name", "category", "quantity", "unit_price", "supply_price",
                 "total_amount", "vat", "grand_total", "sales_rep", "payment_terms", "notes"]:
        idx = _find_column(header, key)
        if idx is not None:
            col_map[key] = idx

    logger.info(f"[xlsx_parser] 컬럼 매핑: {list(col_map.keys())} ({len(col_map)}개)")

    # 필수 컬럼 체크
    required = ["customer_name", "product_name"]
    missing = [k for k in required if k not in col_map]
    if missing:
        wb.close()
        raise ValueError(f"필수 컬럼을 찾을 수 없습니다: {missing}. 헤더: {header}")

    dates = []
    for row_idx, row in enumerate(rows[1:], start=2):
        if all(c is None for c in row):
            continue

        def _get(key):
            idx = col_map.get(key)
            return row[idx] if idx is not None and idx < len(row) else None

        tx_date = _parse_date(_get("transaction_date"))
        cust_code = str(_get("customer_code") or "").strip()
        cust_name = str(_get("customer_name") or "").strip()
        prod_code = str(_get("product_code") or "").strip()
        prod_name = str(_get("product_name") or "").strip()
        category = str(_get("category") or "").strip()
        quantity = _parse_int(_get("quantity")) or 1
        unit_price = _parse_int(_get("unit_price"))
        supply_price = _parse_int(_get("supply_price"))
        total_amount = _parse_int(_get("total_amount"))
        sales_rep = str(_get("sales_rep") or "").strip()

        if not cust_name and not prod_name:
            continue

        # 자동 계산 보완
        if total_amount == 0 and unit_price > 0:
            total_amount = unit_price * quantity
        if unit_price == 0 and total_amount > 0 and quantity > 0:
            unit_price = total_amount // quantity

        # 거래처코드 자동 생성
        if not cust_code and cust_name:
            cust_code = f"C{hash(cust_name) % 10000:04d}"
        # 품목코드 자동 생성
        if not prod_code and prod_name:
            prod_code = f"P{hash(prod_name) % 10000:04d}"

        tx = {
            "transaction_date": tx_date or "",
            "customer_code": cust_code,
            "customer_name": cust_name,
            "product_code": prod_code,
            "product_name": prod_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "supply_price": supply_price,
            "total_amount": total_amount,
            "sales_rep": sales_rep,
            "notes": str(_get("notes") or "").strip(),
        }
        result["transactions"].append(tx)

        if tx_date:
            dates.append(tx_date)

        # 거래처 자동 수집
        if cust_code and cust_code not in result["customers"]:
            result["customers"][cust_code] = {
                "customer_code": cust_code,
                "customer_name": cust_name,
                "industry": "",
                "company_size": "",
                "region": "",
                "contract_type": "",
            }

        # 품목 자동 수집
        if prod_code and prod_code not in result["products"]:
            result["products"][prod_code] = {
                "product_code": prod_code,
                "product_name": prod_name,
                "category": category,
                "brand": "",
                "standard_cost": supply_price,
            }

    # ── 거래처 마스터 시트 파싱 (있으면) ──
    if cust_sheet:
        try:
            c_rows = list(cust_sheet.iter_rows(values_only=True))
            if len(c_rows) >= 2:
                c_header = [str(c).strip() if c else "" for c in c_rows[0]]
                c_map = {}
                for key in ["customer_code", "customer_name", "industry", "company_size",
                            "region", "contact_name", "contact_phone", "contact_email",
                            "contract_type", "first_deal_date"]:
                    idx = _find_column(c_header, key)
                    if idx is not None:
                        c_map[key] = idx

                for crow in c_rows[1:]:
                    if all(c is None for c in crow):
                        continue
                    cc = str(crow[c_map["customer_code"]] if "customer_code" in c_map else "").strip()
                    if cc and cc in result["customers"]:
                        for k, cidx in c_map.items():
                            if k != "customer_code" and cidx < len(crow) and crow[cidx]:
                                result["customers"][cc][k] = str(crow[cidx]).strip()
        except Exception as e:
            logger.warning(f"[xlsx_parser] 거래처 마스터 파싱 실패: {e}")

    # ── 품목 마스터 시트 파싱 (있으면) ──
    if prod_sheet:
        try:
            p_rows = list(prod_sheet.iter_rows(values_only=True))
            if len(p_rows) >= 2:
                p_header = [str(c).strip() if c else "" for c in p_rows[0]]
                p_map = {}
                for key in ["product_code", "product_name", "category", "sub_category",
                            "brand", "standard_cost", "current_stock", "lead_time_days"]:
                    idx = _find_column(p_header, key)
                    if idx is not None:
                        p_map[key] = idx

                for prow in p_rows[1:]:
                    if all(c is None for c in prow):
                        continue
                    pc = str(prow[p_map["product_code"]] if "product_code" in p_map else "").strip()
                    if pc and pc in result["products"]:
                        for k, pidx in p_map.items():
                            if k != "product_code" and pidx < len(prow) and prow[pidx]:
                                result["products"][pc][k] = str(prow[pidx]).strip()
        except Exception as e:
            logger.warning(f"[xlsx_parser] 품목 마스터 파싱 실패: {e}")

    wb.close()

    # ── 결과 정리 ──
    result["customers"] = list(result["customers"].values())
    result["products"] = list(result["products"].values())

    if dates:
        dates_sorted = sorted(dates)
        result["period_start"] = dates_sorted[0]
        result["period_end"] = dates_sorted[-1]
    else:
        result["period_start"] = ""
        result["period_end"] = ""

    result["summary"] = {
        "total_rows": len(result["transactions"]),
        "total_customers": len(result["customers"]),
        "total_products": len(result["products"]),
        "total_amount": sum(t["total_amount"] for t in result["transactions"]),
        "period": f"{result['period_start']} ~ {result['period_end']}",
    }

    logger.info(
        f"[xlsx_parser] 파싱 완료: {result['summary']['total_rows']}건, "
        f"거래처 {result['summary']['total_customers']}개, "
        f"품목 {result['summary']['total_products']}개, "
        f"총액 {result['summary']['total_amount']:,}원"
    )

    return result
