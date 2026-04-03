"""
바코드 ERP Bridge 서비스
- 쿠팡 PO 파일 → 바코드→품목코드 변환 → 이카운트 판매전표 자동 등록
- master_data.xlsx 기반 매핑 (PO매핑, 주문서매핑, 단종, 매입가인상)
"""
import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd

from config import (
    ERP_COM_CODE, ERP_USER_ID, ERP_API_KEY, ERP_ZONE,
    BARCODE_CUST_CODE, BARCODE_WH_CD, BARCODE_MASTER_PATH,
)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 마스터 데이터 경로
# ──────────────────────────────────────────────
_DEFAULT_MASTER = Path(__file__).parent.parent.parent / "data" / "barcode" / "master_data.xlsx"
MASTER_PATH = BARCODE_MASTER_PATH if BARCODE_MASTER_PATH else str(_DEFAULT_MASTER)


# ──────────────────────────────────────────────
# 이카운트 세션 발급 (독립 — erp_client와 별도)
# ──────────────────────────────────────────────
async def get_ecount_session() -> tuple[str, str]:
    """이카운트 세션 ID 발급 (Zone → OAPILogin 2단계)"""
    async with httpx.AsyncClient(timeout=30) as client:
        # 1단계: Zone 조회
        zone_resp = await client.post(
            "https://oapi.ecount.com/OAPI/V2/Zone",
            json={"COM_CODE": ERP_COM_CODE},
        )
        zone_data = zone_resp.json()
        if str(zone_data.get("Status")) != "200" or not zone_data.get("Data"):
            raise Exception(f"Zone 조회 실패: {zone_data}")
        zone = zone_data["Data"]["ZONE"]
        logger.info(f"[바코드] Zone 확인: {zone}")

        # 2단계: 로그인
        login_resp = await client.post(
            f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/OAPILogin",
            json={
                "COM_CODE": ERP_COM_CODE,
                "USER_ID": ERP_USER_ID.upper(),
                "API_CERT_KEY": ERP_API_KEY,
                "LAN_TYPE": "ko-KR",
                "ZONE": zone,
            },
        )
        login_data = login_resp.json()
        if login_data.get("Error"):
            raise Exception(f"이카운트 로그인 실패: {login_data['Error']}")
        if str(login_data.get("Status")) != "200":
            raise Exception(f"이카운트 로그인 실패: {login_data}")

        session_id = login_data["Data"]["Datas"]["SESSION_ID"]
        logger.info(f"[바코드] 세션 발급 성공: {session_id[:10]}...")
        return session_id, zone


# ──────────────────────────────────────────────
# 마스터 데이터 로드
# ──────────────────────────────────────────────
def load_master() -> tuple[dict, dict, set, set, set]:
    """master_data.xlsx에서 매핑 및 단종/매입가인상/바코드부착 목록을 로드한다.

    Returns:
        (barcode_to_code, code_to_barcode, discontinued, price_up, needs_label)
    """
    barcode_to_code: dict[str, str] = {}
    code_to_barcode: dict[str, str] = {}
    discontinued: set[str] = set()
    price_up: set[str] = set()
    needs_label: set[str] = set()  # 바코드 부착 필요 바코드/품목코드 집합

    if not os.path.exists(MASTER_PATH):
        logger.warning(f"[바코드] master_data.xlsx 없음: {MASTER_PATH}")
        return barcode_to_code, code_to_barcode, discontinued, price_up, needs_label

    with open(MASTER_PATH, "rb") as f:
        raw = io.BytesIO(f.read())

    xl = pd.ExcelFile(raw)

    # ── PO 매핑 ──
    po_sheet = "PO매핑" if "PO매핑" in xl.sheet_names else xl.sheet_names[0]
    raw.seek(0)
    df_po = pd.read_excel(raw, sheet_name=po_sheet, dtype=str)
    df_po.columns = df_po.columns.str.strip()
    if "바코드" in df_po.columns and "상품코드" in df_po.columns:
        df_po["바코드"] = df_po["바코드"].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
        df_po["상품코드"] = df_po["상품코드"].fillna("").str.strip()
        barcode_to_code = {
            row["바코드"]: row["상품코드"]
            for _, row in df_po.iterrows()
            if row["바코드"]
        }
        logger.info(f"[바코드] PO 매핑 로드: {len(barcode_to_code)}개")

    # ── 주문서 매핑 ──
    if "주문서매핑" in xl.sheet_names:
        raw.seek(0)
        df_ord = pd.read_excel(raw, sheet_name="주문서매핑", dtype=str)
        df_ord.columns = df_ord.columns.str.strip()
        if "품목코드" in df_ord.columns and "lineup11 바코드" in df_ord.columns:
            df_ord["품목코드"] = df_ord["품목코드"].fillna("").str.strip()
            df_ord["lineup11 바코드"] = df_ord["lineup11 바코드"].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
            code_to_barcode = {
                row["품목코드"]: row["lineup11 바코드"]
                for _, row in df_ord.iterrows()
                if row["품목코드"]
            }
            with_bc = sum(1 for v in code_to_barcode.values() if v)
            logger.info(f"[바코드] 주문서 매핑 로드: {len(code_to_barcode)}개 (바코드 있음: {with_bc}개)")

    # ── 단종 목록 ──
    if "단종" in xl.sheet_names:
        raw.seek(0)
        df_d = pd.read_excel(raw, sheet_name="단종", dtype=str).fillna("")
        df_d.columns = df_d.columns.str.strip()
        for col in ["바코드", "품목코드"]:
            if col in df_d.columns:
                vals = df_d[col].str.strip().str.replace(r"\.0$", "", regex=True)
                discontinued |= set(v for v in vals if v)
        logger.info(f"[바코드] 단종 목록: {len(discontinued)}개")

    # ── 매입가인상 목록 ──
    if "매입가인상" in xl.sheet_names:
        raw.seek(0)
        df_p = pd.read_excel(raw, sheet_name="매입가인상", dtype=str).fillna("")
        df_p.columns = df_p.columns.str.strip()
        for col in ["바코드", "품목코드"]:
            if col in df_p.columns:
                vals = df_p[col].str.strip().str.replace(r"\.0$", "", regex=True)
                price_up |= set(v for v in vals if v)
        logger.info(f"[바코드] 매입가인상 목록: {len(price_up)}개")

    # ── 바코드부착 목록 (단종/매입가인상과 동일 구조)
    if "바코드부착" in xl.sheet_names:
        raw.seek(0)
        df_l = pd.read_excel(raw, sheet_name="바코드부착", dtype=str).fillna("")
        df_l.columns = df_l.columns.str.strip()
        for col in ["바코드", "품목코드"]:
            if col in df_l.columns:
                vals = df_l[col].str.strip().str.replace(r"\.0$", "", regex=True)
                needs_label |= set(v for v in vals if v)
        logger.info(f"[바코드] 바코드부착 목록: {len(needs_label)}개")

    return barcode_to_code, code_to_barcode, discontinued, price_up, needs_label


# ──────────────────────────────────────────────
# PO 파일 파싱 (미리보기 테이블용)
# ──────────────────────────────────────────────
def parse_po_items(contents: bytes) -> list[dict]:
    """PO 엑셀 → 항목 리스트 (납품부족사유 자동 감지 포함)"""
    df = pd.read_excel(io.BytesIO(contents), dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    barcode_to_code, _, discontinued, price_up, needs_label = load_master()

    items = []
    for i, row in df.iterrows():
        bc = str(row.get("상품바코드", "")).strip().replace(".0", "")
        prod_cd = barcode_to_code.get(bc, "")

        existing_reason = str(row.get("납품부족사유", "")).strip()
        if existing_reason:
            auto_reason = existing_reason
        elif bc in discontinued or (prod_cd and prod_cd in discontinued):
            auto_reason = "제조사 생산중단 혹은 공급사 취급중단 - 시장 단종"
        elif bc in price_up or (prod_cd and prod_cd in price_up):
            auto_reason = "가격 이슈 (Price) - 매입가 인상 협상 중"
        else:
            auto_reason = ""

        # 바코드부착 필요 여부
        label_needed = bc in needs_label or (prod_cd and prod_cd in needs_label)

        items.append({
            "idx": i,
            "발주번호": str(row.get("발주번호", "")).strip(),
            "물류센터": str(row.get("물류센터", "")).strip(),
            "상품이름": str(row.get("상품이름", "")).strip()[:40],
            "발주수량": str(row.get("발주수량", "")).strip(),
            "확정수량": str(row.get("확정수량", "")).strip(),
            "매핑여부": "✅" if prod_cd else "❌",
            "사유": auto_reason,
            "바코드부착": label_needed,
        })
    return items


# ──────────────────────────────────────────────
# PO 파일 다운로드 (납품부족사유 반영)
# ──────────────────────────────────────────────
def fill_shortage_reasons(contents: bytes, shortage_reasons: dict) -> io.BytesIO:
    """납품부족사유를 채워서 엑셀 반환"""
    df = pd.read_excel(io.BytesIO(contents), dtype=str).fillna("")
    df.columns = df.columns.str.strip()

    # 확정수량을 0으로 설정해야 하는 사유 키워드 (단종/품절/인상)
    ZERO_QTY_KEYWORDS = ["시장 단종", "생산중단", "재고부족", "입고지연", "매입가 인상", "가격 이슈"]

    for idx_str, reason in shortage_reasons.items():
        idx = int(idx_str)
        if idx < len(df):
            df.at[idx, "납품부족사유"] = reason
            # 단종·품절·인상 사유인 경우 확정수량(I열) → 0
            if any(kw in reason for kw in ZERO_QTY_KEYWORDS):
                if "확정수량" in df.columns:
                    df.at[idx, "확정수량"] = "0"

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output


# ──────────────────────────────────────────────
# 이카운트 판매 전표 등록
# ──────────────────────────────────────────────
async def send_to_ecount(
    contents: bytes,
    staff_code: str = "",
    io_date: str = "",
    shortage_reasons_json: str = "{}",
) -> dict:
    """PO 파일 → 이카운트 판매전표 BulkSave"""
    today = io_date.strip() if io_date.strip() else datetime.today().strftime("%Y%m%d")
    barcode_to_code, _, discontinued, price_up, needs_label = load_master()

    df = pd.read_excel(io.BytesIO(contents), dtype=str)
    df.columns = df.columns.str.strip()
    df = df.fillna("")

    # 필수 컬럼 확인
    for col in ["상품바코드", "발주번호", "물류센터"]:
        if col not in df.columns:
            raise ValueError(f"'{col}' 열이 없습니다. PO 파일인지 확인하세요.")

    # ① 바코드 → 품목코드 변환
    df["상품바코드"] = df["상품바코드"].str.strip().str.replace(r"\.0$", "", regex=True)
    df["_품목코드"] = df["상품바코드"].map(lambda bc: barcode_to_code.get(bc, ""))

    # ② 납품부족사유 필터 — 단종/인상은 이카운트 제외
    reasons: dict = json.loads(shortage_reasons_json)
    excluded = {int(k) for k, v in reasons.items()
                if "시장 단종" in v or "생산중단" in v or "매입가 인상" in v or "가격 이슈" in v}

    # 마스터 자동 감지로도 제외
    for i, row in df.iterrows():
        if i not in excluded:
            bc = str(row.get("상품바코드", "")).strip().replace(".0", "")
            prod_cd = barcode_to_code.get(bc, "")
            if bc in discontinued or (prod_cd and prod_cd in discontinued):
                excluded.add(i)
            elif bc in price_up or (prod_cd and prod_cd in price_up):
                excluded.add(i)

    excluded_cnt = len(excluded)
    df = df[~df.index.isin(excluded)].copy()

    # ③ 수량 컬럼 선택 (확정수량 우선)
    qty_col = "확정수량" if "확정수량" in df.columns else "발주수량"
    df[qty_col] = df[qty_col].str.strip().str.replace(",", "")

    valid = df[
        (df["_품목코드"] != "") &
        (df[qty_col] != "") &
        (df[qty_col] != "0")
    ].copy()

    unmatched = int((df["_품목코드"] == "").sum())

    if valid.empty:
        raise ValueError(f"전송할 유효한 데이터가 없습니다. (바코드 미매칭: {unmatched}건)")

    # ④ 물류센터 ㄱ~ㅎ 정렬 → 발주번호 순 (원본 인덱스 보존)
    valid["_orig_idx"] = valid.index
    valid = valid.sort_values(["물류센터", "발주번호"]).reset_index(drop=True)

    # ⑤ 발주번호별 순번 할당
    doc_to_ser: dict = {}
    ser_counter = 1
    for doc_no in valid["발주번호"]:
        if doc_no not in doc_to_ser:
            doc_to_ser[doc_no] = str(ser_counter)
            ser_counter += 1

    # ⑥ BulkDatas 구성
    bulk_list = []
    orig_indices = []  # 원본 PO 행 인덱스 추적
    label_flags = []  # 바코드부착 필요 여부 추적
    for _, row in valid.iterrows():
        doc_no = str(row["발주번호"]).strip()
        warehouse = str(row["물류센터"]).strip()
        qty_str = str(row[qty_col]).replace(",", "").strip()
        supply_str = str(row.get("총발주 매입금", "")).replace(",", "").strip()

        try:
            price_val = round(float(supply_str) / float(qty_str)) if supply_str and qty_str and float(qty_str) != 0 else 0
        except Exception:
            price_val = 0

        bc_val = str(row.get("상품바코드", "")).strip()
        cd_val = str(row.get("_품목코드", "")).strip()
        is_label = bc_val in needs_label or (cd_val and cd_val in needs_label)
        label_flags.append(is_label)

        bulk_list.append({"BulkDatas": {
            "UPLOAD_SER_NO": doc_to_ser[doc_no],
            "IO_DATE": today,
            "CUST": BARCODE_CUST_CODE,
            "WH_CD": BARCODE_WH_CD,
            "EMP_CD": staff_code,
            "PROD_CD": str(row["_품목코드"]).strip(),
            "PROD_DES": "★ 바코드 부착 필요" if is_label else "",
            "QTY": qty_str,
            "PRICE": str(price_val),
            "SUPPLY_AMT": supply_str,
            "VAT_AMT": "",
            "REMARKS": f"{warehouse} - {doc_no}",
            "U_MEMO5": f"{warehouse} - {doc_no}",
        }})
        orig_indices.append(int(row["_orig_idx"]))

    logger.info(f"[바코드] 전송 항목: {len(bulk_list)}개 | 미매칭: {unmatched}개 | 제외: {excluded_cnt}개")

    # ⑦ 세션 발급 및 전송
    session_id, zone = await get_ecount_session()
    sale_url = f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(sale_url, json={"SaleList": bulk_list})
        result = resp.json()

        # ⑧ 재고 조회 — 10번(용산) + 30번(통진) 두 창고
        inv_10: dict = {}  # 용산창고
        inv_30: dict = {}  # 통진창고
        inv_checked = False
        try:
            inv_url = (
                f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/"
                f"InventoryBalance/GetListInventoryBalanceStatusByLocation"
                f"?SESSION_ID={session_id}"
            )
            # WH_CD 빈값이면 전체 창고 조회
            inv_resp = await client.post(inv_url, json={
                "BASE_DATE": today,
                "WH_CD": "",
                "PROD_CD": "",
            })
            inv_data = inv_resp.json()
            for r in ((inv_data.get("Data") or {}).get("Result") or []):
                pc = str(r.get("PROD_CD", "")).strip()
                wh = str(r.get("WH_CD", "")).strip()
                try:
                    bq = float(str(r.get("BAL_QTY", "0") or "0"))
                except Exception:
                    bq = 0.0
                if pc and wh == "10":
                    inv_10[pc] = bq
                elif pc and wh == "30":
                    inv_30[pc] = bq
            inv_checked = True
            logger.info(f"[바코드] 재고 조회 완료: 용산 {len(inv_10)}건, 통진 {len(inv_30)}건")
        except Exception as e:
            logger.warning(f"[바코드] 재고 조회 실패: {e}")

    # ⑨ 결과 구성
    status = result.get("Status")
    data = result.get("Data") or {}
    success = data.get("SuccessCnt", 0)
    fail = data.get("FailCnt", 0)
    slip_nos = data.get("SlipNos", [])
    errors = []
    for rd in (data.get("ResultDetails") or []):
        if not rd.get("IsSuccess"):
            errors.append(rd.get("TotalError", ""))

    items_result = []
    for i, item in enumerate(bulk_list):
        bd = item["BulkDatas"]
        prod_cd = bd["PROD_CD"]
        bal_10 = inv_10.get(prod_cd, None)  # 용산
        bal_30 = inv_30.get(prod_cd, None)  # 통진

        # 재고 상태 판별:
        # - 통진(30) 재고 > 0 → 정상 (ok)
        # - 통진(30) 재고 ≤ 0 이지만 용산(10) 재고 > 0 → 주황 (wh10_has)
        # - 둘 다 없음 → 빨강 (low_stock)
        has_30 = bal_30 is not None and bal_30 > 0
        has_10 = bal_10 is not None and bal_10 > 0

        if has_30:
            stock_status = "ok"
        elif has_10:
            stock_status = "wh10_has"  # 용산에만 재고 있음
        else:
            stock_status = "low_stock"  # 둘 다 재고 없음

        items_result.append({
            "upload_ser_no": bd["UPLOAD_SER_NO"],
            "remarks": bd["U_MEMO5"],
            "prod_cd": prod_cd,
            "qty": bd["QTY"],
            "bal_10": round(bal_10) if bal_10 is not None else None,
            "bal_30": round(bal_30) if bal_30 is not None else None,
            "stock_status": stock_status,
            "low_stock": stock_status == "low_stock",  # 하위호환
            "orig_idx": orig_indices[i],
            "needs_label": label_flags[i],  # 바코드 부착 필요 여부
        })

    # 바코드 부착 필요 항목 별도 추출
    label_items = [it for it in items_result if it.get("needs_label")]

    return {
        "status": status,
        "total": len(bulk_list),
        "success": success,
        "fail": fail,
        "slip_nos": slip_nos,
        "errors": errors,
        "unmatched": unmatched,
        "excluded": excluded_cnt,
        "items_result": items_result,
        "label_items": label_items,
        "inv_checked": inv_checked,
    }
