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

    # ⑤ 세션 발급 후 재고 먼저 조회 → 품목별 WH_CD 결정
    session_id, zone = await get_ecount_session()

    inv_10: dict = {}  # 용산(10) 재고
    inv_30: dict = {}  # 통진(30) 재고
    inv_checked = False
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            inv_url = (
                f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/"
                f"InventoryBalance/GetListInventoryBalanceStatusByLocation"
                f"?SESSION_ID={session_id}"
            )
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
            logger.info(f"[바코드] 재고 사전조회 완료: 용산 {len(inv_10)}건, 통진 {len(inv_30)}건")
        except Exception as e:
            logger.warning(f"[바코드] 재고 사전조회 실패 (기본창고로 진행): {e}")

        # ⑥ 품목별 WH_CD 결정 후 통진/용산 두 그룹으로 분리
        #    → 통진(30) 전체 먼저, 용산(10) 전체 나중에 전송해야
        #      같은 발주번호가 창고별로 각각 하나의 전표로 묶임
        rows_30 = []       # (row, orig_idx, is_label) — 통진(30)
        rows_10 = []       # 용산(10) 전환
        rows_no_stock = [] # 둘 다 재고 없음 → 전표 제외

        # 순번별 수량 차감 추적 (원본 inv_30/inv_10은 표시용으로 보존)
        rem_30: dict = dict(inv_30)  # 통진 남은 재고 (처리하면서 차감)
        rem_10: dict = dict(inv_10)  # 용산 남은 재고 (처리하면서 차감)

        for _, row in valid.iterrows():
            prod_cd = str(row["_품목코드"]).strip()
            bc_val = str(row.get("상품바코드", "")).strip()
            is_label = bc_val in needs_label or (prod_cd and prod_cd in needs_label)

            # 발주 수량
            try:
                order_qty = float(
                    str(row.get(qty_col, "0") or "0").replace(",", "").strip()
                )
            except Exception:
                order_qty = 0.0

            if not inv_checked:
                # 재고 조회 실패 → 기본창고(통진)로 처리
                rows_30.append((row, int(row["_orig_idx"]), is_label))
                continue

            stock_30 = rem_30.get(prod_cd, 0.0)
            stock_10 = rem_10.get(prod_cd, 0.0)

            if order_qty > 0 and stock_30 >= order_qty:
                # 통진 재고 충분 → 통진 전표, 수량 차감
                rows_30.append((row, int(row["_orig_idx"]), is_label))
                rem_30[prod_cd] = stock_30 - order_qty
                logger.debug(f"[바코드] {prod_cd} 통진 배정: {order_qty} (잔여 {rem_30[prod_cd]})")
            elif order_qty > 0 and stock_10 >= order_qty:
                # 통진 부족 → 용산 재고 충분 → 용산 전표, 수량 차감
                rows_10.append((row, int(row["_orig_idx"]), is_label))
                rem_10[prod_cd] = stock_10 - order_qty
                logger.info(f"[바코드] {prod_cd} 통진부족({stock_30}<{order_qty}) → 용산 전환 (잔여 {rem_10[prod_cd]})")
            else:
                # 둘 다 부족 → 전표 제외
                rows_no_stock.append((row, int(row["_orig_idx"]), is_label))
                logger.info(f"[바코드] {prod_cd} 재고부족(통진:{stock_30}, 용산:{stock_10}, 필요:{order_qty}) → 전표제외")

        # ⑦ 통진 그룹 순번 할당 → 용산 그룹 순번 이어서 할당
        def assign_ser_nos(rows):
            doc_to_ser: dict = {}
            counter = [1]
            def get_ser(doc_no):
                if doc_no not in doc_to_ser:
                    doc_to_ser[doc_no] = str(counter[0])
                    counter[0] += 1
                return doc_to_ser[doc_no]
            return get_ser, counter

        get_ser_30, cnt_30 = assign_ser_nos(rows_30)
        # 용산 순번은 통진 마지막 순번 이후부터 시작하지 않고 1부터 독립적으로
        # (ERP에서 창고가 다르면 별도 전표이므로 순번 중복 무방)
        doc_to_ser_30: dict = {}
        ser_30 = 1
        for row, _, _ in rows_30:
            doc_no = str(row["발주번호"]).strip()
            if doc_no not in doc_to_ser_30:
                doc_to_ser_30[doc_no] = str(ser_30)
                ser_30 += 1

        doc_to_ser_10: dict = {}
        ser_10 = 1
        for row, _, _ in rows_10:
            doc_no = str(row["발주번호"]).strip()
            if doc_no not in doc_to_ser_10:
                doc_to_ser_10[doc_no] = str(ser_10)
                ser_10 += 1

        # ⑧ BulkDatas 구성 — 통진 그룹 먼저, 용산 그룹 나중
        bulk_list = []
        orig_indices = []
        label_flags = []
        item_wh_list = []

        def build_entry(row, orig_idx, is_label, wh_cd, doc_to_ser):
            doc_no = str(row["발주번호"]).strip()
            warehouse = str(row["물류센터"]).strip()
            qty_str = str(row[qty_col]).replace(",", "").strip()
            supply_str = str(row.get("총발주 매입금", "")).replace(",", "").strip()
            try:
                price_val = round(float(supply_str) / float(qty_str)) if supply_str and qty_str and float(qty_str) != 0 else 0
            except Exception:
                price_val = 0
            return {"BulkDatas": {
                "UPLOAD_SER_NO": doc_to_ser[doc_no],
                "IO_DATE": today,
                "CUST": BARCODE_CUST_CODE,
                "WH_CD": wh_cd,
                "EMP_CD": staff_code,
                "PROD_CD": str(row["_품목코드"]).strip(),
                "PROD_DES": "★ 바코드 부착 필요" if is_label else "",
                "QTY": qty_str,
                "PRICE": str(price_val),
                "SUPPLY_AMT": supply_str,
                "VAT_AMT": "",
                "REMARKS": f"{warehouse} - {doc_no}",
                "U_MEMO5": f"{warehouse} - {doc_no}",
            }}

        for row, orig_idx, is_label in rows_30:
            bulk_list.append(build_entry(row, orig_idx, is_label, BARCODE_WH_CD, doc_to_ser_30))
            orig_indices.append(orig_idx)
            label_flags.append(is_label)
            item_wh_list.append(BARCODE_WH_CD)

        for row, orig_idx, is_label in rows_10:
            bulk_list.append(build_entry(row, orig_idx, is_label, "10", doc_to_ser_10))
            orig_indices.append(orig_idx)
            label_flags.append(is_label)
            item_wh_list.append("10")

        yongsan_cnt = len(rows_10)
        no_stock_cnt = len(rows_no_stock)
        logger.info(f"[바코드] 전송 항목: {len(bulk_list)}개 | 통진: {len(rows_30)}개 | 용산: {yongsan_cnt}개 | 재고없음(전표제외): {no_stock_cnt}개 | 미매칭: {unmatched}개 | 제외: {excluded_cnt}개")
        if yongsan_cnt:
            logger.info(f"[바코드] 통진→용산 전환 {yongsan_cnt}건 (용산 전표 후순위 전송)")
        if no_stock_cnt:
            logger.info(f"[바코드] 재고 없음 전표 제외 {no_stock_cnt}건")

        # ⑧ 전표 전송
        sale_url = f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}"
        resp = await client.post(sale_url, json={"SaleList": bulk_list})
        result = resp.json()

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

    # (WH_CD, UPLOAD_SER_NO) 조합 → slip_no 매핑
    # Ecount SlipNos 순서 = bulk_list 내 고유 (wh, ser_no) 조합 순서
    seen_wh_ser: list = []
    for item in bulk_list:
        bd = item["BulkDatas"]
        key = (bd["WH_CD"], bd["UPLOAD_SER_NO"])
        if key not in seen_wh_ser:
            seen_wh_ser.append(key)
    wh_ser_to_slip: dict = {}
    for idx, key in enumerate(seen_wh_ser):
        if idx < len(slip_nos):
            wh_ser_to_slip[key] = slip_nos[idx]

    items_result = []
    for i, item in enumerate(bulk_list):
        bd = item["BulkDatas"]
        prod_cd = bd["PROD_CD"]
        bal_10 = inv_10.get(prod_cd, None)  # 용산
        bal_30 = inv_30.get(prod_cd, None)  # 통진
        used_wh = item_wh_list[i]  # 실제 전송된 창고

        has_30 = bal_30 is not None and bal_30 > 0
        has_10 = bal_10 is not None and bal_10 > 0

        # 재고 상태:
        # - 통진(30) 있음 → ok (통진 전표)
        # - 통진(30) 없고 용산(10) 있음 → wh10_used (용산으로 자동 전환됨)
        # - 둘 다 없음 → low_stock (납품부족사유 자동 입력 대상)
        if has_30:
            stock_status = "ok"
        elif has_10:
            stock_status = "wh10_used"  # 용산으로 전표 전환됨
        else:
            stock_status = "low_stock"

        slip_no = wh_ser_to_slip.get((used_wh, bd["UPLOAD_SER_NO"]), "")
        # 차감 후 잔여 재고
        rem_30_after = rem_30.get(prod_cd, None)
        rem_10_after = rem_10.get(prod_cd, None)

        items_result.append({
            "upload_ser_no": bd["UPLOAD_SER_NO"],
            "slip_no": slip_no,           # 실제 이카운트 전표번호
            "remarks": bd["U_MEMO5"],
            "prod_cd": prod_cd,
            "qty": bd["QTY"],
            "bal_10": round(bal_10) if bal_10 is not None else None,   # 조회 시 원본 재고
            "bal_30": round(bal_30) if bal_30 is not None else None,   # 조회 시 원본 재고
            "rem_10": round(rem_10_after) if rem_10_after is not None else None,  # 배정 후 잔여
            "rem_30": round(rem_30_after) if rem_30_after is not None else None,  # 배정 후 잔여
            "stock_status": stock_status,
            "used_wh": used_wh,           # 실제 전표 창고 (10=용산, 30=통진)
            "low_stock": stock_status == "low_stock",  # 하위호환
            "orig_idx": orig_indices[i],
            "needs_label": label_flags[i],
        })

    # 재고 없어 전표 제외된 품목 → items_result에 low_stock으로 추가 (납품부족사유 자동 입력용)
    for row, orig_idx, is_label in rows_no_stock:
        prod_cd = str(row["_품목코드"]).strip()
        bal_10 = inv_10.get(prod_cd, None)
        bal_30 = inv_30.get(prod_cd, None)
        items_result.append({
            "upload_ser_no": "-",
            "slip_no": "",                 # 전표 미생성
            "remarks": f"{str(row['물류센터']).strip()} - {str(row['발주번호']).strip()}",
            "prod_cd": prod_cd,
            "qty": str(row[qty_col]).replace(",", "").strip(),
            "bal_10": round(bal_10) if bal_10 is not None else None,
            "bal_30": round(bal_30) if bal_30 is not None else None,
            "stock_status": "low_stock",
            "used_wh": None,
            "low_stock": True,
            "orig_idx": orig_idx,
            "needs_label": is_label,
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
        "no_stock": len(rows_no_stock),
        "items_result": items_result,
        "label_items": label_items,
        "inv_checked": inv_checked,
    }
