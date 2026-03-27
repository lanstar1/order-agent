"""
스마트스토어 주문 자동화 API
- 네이버 커머스 API로 주문 수집
- ERP 판매입력 엑셀 생성/다운로드
- 택배 업로드 엑셀 생성/다운로드
- 송장번호 발송처리 (네이버 API)
"""
import io
import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from security import get_current_user, verify_token
from db.database import get_connection, now_kst

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/smartstore", tags=["smartstore"])

KST = timezone(timedelta(hours=9))


# ─── Request / Response 모델 ───────────────────
class FetchOrdersRequest(BaseModel):
    from_date: str = ""      # YYYY-MM-DD (비어있으면 오늘)
    to_date: str = ""        # YYYY-MM-DD (비어있으면 from_date+1)


class DispatchItem(BaseModel):
    product_order_id: str
    tracking_number: str


class DispatchRequest(BaseModel):
    items: list[DispatchItem]


class ProductMappingItem(BaseModel):
    naver_product_no: str
    item_code: str
    model_name: str = ""


# ─── 품목코드 매칭 유틸 ───────────────────
_MODEL_PATTERN = re.compile(r'(LS[PNE]?-[\w\-]+|ZOT-[\w\-]+)', re.IGNORECASE)


def _extract_model_code(option_info: str, product_name: str) -> str:
    """옵션정보 → 상품명 순서로 모델코드 추출"""
    if option_info:
        m = _MODEL_PATTERN.search(option_info)
        if m:
            return m.group(1).upper()
    if product_name:
        m = _MODEL_PATTERN.search(product_name)
        if m:
            return m.group(1).upper()
    return ""


def _load_product_mapping() -> tuple[dict, dict]:
    """DB에서 품목코드 매칭 데이터 로드
    Returns: (product_no_map, model_code_map)
    """
    conn = get_connection()
    product_no_map = {}  # 상품번호 → 품목코드
    model_code_map = {}  # 모델코드 → 품목코드

    try:
        # smartstore_product_map 테이블에서 로드
        rows = conn.execute(
            "SELECT naver_product_no, item_code, model_name FROM smartstore_product_map"
        ).fetchall()
        for row in rows:
            pno = str(row["naver_product_no"] if hasattr(row, '__getitem__') else row[0]).strip()
            item_code = str(row["item_code"] if hasattr(row, '__getitem__') else row[1]).strip()
            model_name = str(row["model_name"] if hasattr(row, '__getitem__') else row[2]).strip()
            if pno and item_code:
                product_no_map[pno] = item_code
            if model_name and item_code:
                model_code_map[model_name.upper()] = item_code
    except Exception as e:
        logger.warning(f"[SmartStore] 품목매칭 로드 실패: {e}")
    finally:
        conn.close()

    return product_no_map, model_code_map


def _match_item_code(product_no: str, option_info: str, product_name: str,
                     product_no_map: dict, model_code_map: dict) -> tuple[str, str]:
    """상품번호/옵션/상품명으로 품목코드 매칭
    Returns: (item_code, model_code)
    """
    # 1순위: 모델코드 추출 (옵션 → 상품명)
    model_code = _extract_model_code(option_info, product_name)

    # 2순위: 모델코드로 품목코드 조회
    if model_code and model_code in model_code_map:
        return model_code_map[model_code], model_code

    # 3순위: 상품번호로 품목코드 조회
    if product_no in product_no_map:
        return product_no_map[product_no], model_code or product_no

    return "", model_code or ""


# ─── 네이버 클라이언트 인스턴스 ───────────────────
def _get_naver_client():
    """환경변수에서 네이버 커머스 API 클라이언트 생성"""
    import os
    client_id = os.getenv("NAVER_COMMERCE_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise Exception("네이버 커머스 API 키가 설정되지 않았습니다. 설정 페이지에서 등록해주세요.")
    from services.naver_commerce_client import NaverCommerceClient
    return NaverCommerceClient(client_id, client_secret)


# ─── API 엔드포인트 ───────────────────

@router.post("/fetch-orders")
async def fetch_orders(req: FetchOrdersRequest, user=Depends(get_current_user)):
    """네이버 커머스 API로 발송대기(PAYED) 주문 수집 → DB 저장"""
    try:
        client = _get_naver_client()

        # 날짜 설정
        now = datetime.now(KST)
        if req.from_date:
            from_dt = req.from_date + "T00:00:00.000+09:00"
        else:
            from_dt = now.strftime("%Y-%m-%d") + "T00:00:00.000+09:00"

        if req.to_date:
            to_dt = req.to_date + "T23:59:59.999+09:00"
        else:
            to_dt = now.strftime("%Y-%m-%d") + "T23:59:59.999+09:00"

        # 주문 수집
        raw_orders = await client.fetch_new_orders(from_dt, to_dt)

        if not raw_orders:
            return {"success": True, "message": "수집된 주문이 없습니다.", "count": 0, "orders": []}

        # 상품주문 상세 조회를 위해 productOrderId 목록 추출
        product_order_ids = []
        for order in raw_orders:
            poid = order.get("productOrderId", "")
            if poid:
                product_order_ids.append(poid)

        # 상품주문 상세 조회 (30건씩 분할)
        all_details = []
        for i in range(0, len(product_order_ids), 30):
            batch = product_order_ids[i:i+30]
            details = await _fetch_product_order_details(client, batch)
            all_details.extend(details)

        # DB 저장
        saved_count = _save_orders_to_db(all_details)

        # 응답용 요약 생성
        orders_summary = _build_orders_summary(all_details)

        return {
            "success": True,
            "message": f"주문 {saved_count}건 수집 완료",
            "count": saved_count,
            "orders": orders_summary,
        }
    except Exception as e:
        logger.error(f"[SmartStore] 주문 수집 실패: {e}", exc_info=True)
        return {"success": False, "message": str(e), "count": 0, "orders": []}


async def _fetch_product_order_details(client, product_order_ids: list[str]) -> list[dict]:
    """상품주문번호 목록으로 상세 정보 조회"""
    import httpx

    headers = await client._headers()
    async with httpx.AsyncClient() as http:
        resp = await http.post(
            f"https://api.commerce.naver.com/external/v1/pay-order/seller/product-orders/query",
            headers=headers,
            json={"productOrderIds": product_order_ids},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.error(f"[SmartStore] 상세 조회 실패: {resp.status_code} {resp.text}")
            raise Exception(f"상품주문 상세 조회 실패: {resp.status_code}")
        data = resp.json()
        return data.get("data", [])


def _save_orders_to_db(orders: list[dict]) -> int:
    """주문 데이터를 DB에 저장 (UPSERT)"""
    conn = get_connection()
    saved = 0
    try:
        for order in orders:
            po = order.get("productOrder", {})
            product_order_id = po.get("productOrderId", "")
            order_id = po.get("orderId", "")
            product_no = str(po.get("productId", ""))
            product_name = po.get("productName", "")
            option_info = po.get("optionManageCode", "") or po.get("optionCode", "")

            # 옵션 정보가 없으면 상품옵션명에서 추출
            if not option_info:
                option_info = po.get("productOption", "") or ""

            qty = po.get("quantity", 1)
            price = po.get("totalPaymentAmount", 0)
            settlement_amount = po.get("expectedSettlementAmount", 0) or price

            # 수취인 정보
            shipping = order.get("order", {}).get("shippingAddress", {}) or {}
            if not shipping:
                shipping = po.get("shippingAddress", {}) or {}

            rcv_name = shipping.get("name", "")
            rcv_tel1 = shipping.get("tel1", "")
            rcv_tel2 = shipping.get("tel2", "")
            rcv_addr = shipping.get("baseAddress", "")
            rcv_addr_detail = shipping.get("detailAddress", "")
            rcv_zip = shipping.get("zipCode", "")

            # 배송비 정보
            delivery_fee_type = po.get("deliveryFeeType", "")  # FREE, PAID, CONDITIONAL_FREE
            delivery_fee = po.get("deliveryFeeAmount", 0) or 0

            status = po.get("productOrderStatus", "PAYED")

            conn.execute("""
                INSERT OR IGNORE INTO smartstore_orders (
                    product_order_id, order_id, product_no, product_name, option_info,
                    qty, price, settlement_amount, rcv_name, rcv_tel1, rcv_tel2,
                    rcv_addr, rcv_addr_detail, rcv_zip,
                    delivery_fee_type, delivery_fee, status, collected_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                product_order_id, order_id, product_no, product_name, option_info,
                qty, price, settlement_amount, rcv_name, rcv_tel1, rcv_tel2,
                rcv_addr, rcv_addr_detail, rcv_zip,
                delivery_fee_type, delivery_fee, status, now_kst(),
            ))
            saved += 1

        conn.commit()
    except Exception as e:
        logger.error(f"[SmartStore] DB 저장 오류: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()
    return saved


def _build_orders_summary(orders: list[dict]) -> list[dict]:
    """주문 데이터 → 프론트엔드용 요약"""
    result = []
    for order in orders:
        po = order.get("productOrder", {})
        shipping = order.get("order", {}).get("shippingAddress", {}) or {}
        if not shipping:
            shipping = po.get("shippingAddress", {}) or {}

        result.append({
            "productOrderId": po.get("productOrderId", ""),
            "orderId": po.get("orderId", ""),
            "productNo": str(po.get("productId", "")),
            "productName": po.get("productName", ""),
            "optionInfo": po.get("productOption", "") or po.get("optionManageCode", ""),
            "qty": po.get("quantity", 1),
            "price": po.get("totalPaymentAmount", 0),
            "settlementAmount": po.get("expectedSettlementAmount", 0),
            "rcvName": shipping.get("name", ""),
            "rcvAddr": shipping.get("baseAddress", ""),
            "deliveryFeeType": po.get("deliveryFeeType", ""),
            "deliveryFee": po.get("deliveryFeeAmount", 0),
            "status": po.get("productOrderStatus", ""),
        })
    return result


@router.get("/orders")
async def get_orders(
    status: str = Query("PAYED", description="주문 상태 필터"),
    date: str = Query("", description="수집일 필터 (YYYY-MM-DD)"),
    user=Depends(get_current_user),
):
    """DB에서 스마트스토어 주문 목록 조회"""
    conn = get_connection()
    try:
        sql = "SELECT * FROM smartstore_orders WHERE 1=1"
        params = []

        if status:
            sql += " AND status = ?"
            params.append(status)

        if date:
            sql += " AND collected_at LIKE ?"
            params.append(f"{date}%")

        sql += " ORDER BY collected_at DESC"

        rows = conn.execute(sql, params).fetchall()

        # 품목코드 매칭 정보 로드
        product_no_map, model_code_map = _load_product_mapping()

        orders = []
        for row in rows:
            r = dict(row) if hasattr(row, 'keys') else {
                "product_order_id": row[1], "order_id": row[2],
                "product_no": row[3], "product_name": row[4],
                "option_info": row[5], "qty": row[6], "price": row[7],
                "settlement_amount": row[8], "rcv_name": row[9],
                "rcv_tel1": row[10], "rcv_tel2": row[11],
                "rcv_addr": row[12], "rcv_addr_detail": row[13],
                "rcv_zip": row[14], "delivery_fee_type": row[15],
                "delivery_fee": row[16], "status": row[17],
                "tracking_number": row[18], "item_code": row[19],
                "collected_at": row[20],
            }

            # 품목코드 매칭 시도
            item_code = r.get("item_code", "") or ""
            model_code = ""
            if not item_code:
                item_code, model_code = _match_item_code(
                    r.get("product_no", ""),
                    r.get("option_info", ""),
                    r.get("product_name", ""),
                    product_no_map, model_code_map,
                )
            else:
                model_code = _extract_model_code(
                    r.get("option_info", ""), r.get("product_name", "")
                )

            r["item_code_matched"] = item_code
            r["model_code"] = model_code
            orders.append(r)

        return {"success": True, "orders": orders, "count": len(orders)}
    except Exception as e:
        logger.error(f"[SmartStore] 주문 조회 실패: {e}", exc_info=True)
        return {"success": False, "message": str(e), "orders": [], "count": 0}
    finally:
        conn.close()


@router.get("/download-erp")
async def download_erp(
    date: str = Query("", description="수집일 필터"),
    token: str = Query("", description="JWT 토큰 (브라우저 다운로드용)"),
):
    """ERP 판매입력 엑셀 다운로드"""
    if not token or not verify_token(token):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=401, content={"detail": "인증이 필요합니다."})

    import openpyxl

    conn = get_connection()
    try:
        sql = "SELECT * FROM smartstore_orders WHERE status = 'PAYED'"
        params = []
        if date:
            sql += " AND collected_at LIKE ?"
            params.append(f"{date}%")
        sql += " ORDER BY order_id, product_order_id"

        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return {"success": False, "message": "다운로드할 주문이 없습니다."}

        product_no_map, model_code_map = _load_product_mapping()

        # ERP 엑셀 생성
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "판매입력"

        # 헤더 (마스터파일 양식 기준)
        headers = ["상품번호", "일자", "", "거래처코드", "", "담당자", "출하창고",
                    "", "", "", "", "", "", "품목코드", "", "", "수량", "단가", "", "공급가액"]
        ws.append(headers)

        today = datetime.now(KST).strftime("%Y%m%d")

        # 설정값 로드
        import os
        cust_code = os.getenv("SMARTSTORE_CUST_CODE", "")
        emp_code = os.getenv("SMARTSTORE_EMP_CODE", "")
        wh_code = os.getenv("SMARTSTORE_WH_CODE", "30")

        for row in rows:
            r = dict(row) if hasattr(row, 'keys') else {}
            product_no = r.get("product_no", "")
            option_info = r.get("option_info", "")
            product_name = r.get("product_name", "")
            qty = r.get("qty", 1)
            settlement = r.get("settlement_amount", 0) or r.get("price", 0)

            item_code, _ = _match_item_code(
                product_no, option_info, product_name,
                product_no_map, model_code_map,
            )

            unit_price = round(settlement / qty) if qty else 0

            ws.append([
                product_no,    # A: 상품번호
                today,         # B: 일자
                "",            # C: 순번 (빈값)
                cust_code,     # D: 거래처코드
                "",            # E
                emp_code,      # F: 담당자
                wh_code,       # G: 출하창고
                "", "", "", "", "", "",
                item_code,     # N: 품목코드
                "", "",
                qty,           # Q: 수량
                unit_price,    # R: 단가
                "",
                settlement,    # T: 공급가액
            ])

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        filename = f"ERP_판매입력_{today}.xlsx"
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error(f"[SmartStore] ERP 다운로드 실패: {e}", exc_info=True)
        return {"success": False, "message": str(e)}
    finally:
        conn.close()


@router.get("/download-delivery")
async def download_delivery(
    date: str = Query("", description="수집일 필터"),
    token: str = Query("", description="JWT 토큰 (브라우저 다운로드용)"),
):
    """택배 업로드 엑셀 다운로드 (주문번호 기준 묶음)"""
    if not token or not verify_token(token):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=401, content={"detail": "인증이 필요합니다."})

    import openpyxl

    conn = get_connection()
    try:
        sql = "SELECT * FROM smartstore_orders WHERE status = 'PAYED'"
        params = []
        if date:
            sql += " AND collected_at LIKE ?"
            params.append(f"{date}%")
        sql += " ORDER BY order_id, product_order_id"

        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return {"success": False, "message": "다운로드할 주문이 없습니다."}

        product_no_map, model_code_map = _load_product_mapping()

        # 주문번호별 그룹핑
        order_groups = {}
        for row in rows:
            r = dict(row) if hasattr(row, 'keys') else {}
            order_id = r.get("order_id", "")
            if order_id not in order_groups:
                order_groups[order_id] = []
            order_groups[order_id].append(r)

        # 택배 업로드 엑셀 생성
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "택배업로드"

        # 헤더 (파일변환예제문서.xlsx 양식)
        headers = ["수하인명", "", "수하인주소1", "수하인주소2", "수하인전화번호",
                    "수하인핸드폰번호", "택배수량", "택배운임", "운임구분", "품목명", "", "배송메세지"]
        ws.append(headers)

        for order_id, items in order_groups.items():
            first = items[0]
            rcv_name = first.get("rcv_name", "")
            rcv_addr = first.get("rcv_addr", "")
            rcv_addr_detail = first.get("rcv_addr_detail", "")
            rcv_tel1 = first.get("rcv_tel1", "")
            rcv_tel2 = first.get("rcv_tel2", "")

            # 배송비 (첫 행에서만)
            delivery_fee = first.get("delivery_fee", 0) or 0
            delivery_fee_type = first.get("delivery_fee_type", "")

            # 운임구분: 착불 → 020, 그 외 → 030
            if delivery_fee_type == "PAID":  # 착불
                fare_code = "020"
            else:
                fare_code = "030"

            # 품목명: 모델명 x 수량 형태
            goods_parts = []
            for item in items:
                _, model = _match_item_code(
                    item.get("product_no", ""),
                    item.get("option_info", ""),
                    item.get("product_name", ""),
                    product_no_map, model_code_map,
                )
                if not model:
                    model = item.get("product_name", "")[:20]
                qty = item.get("qty", 1)
                goods_parts.append(f"{model} x{qty}")
            goods_nm = ", ".join(goods_parts)

            ws.append([
                rcv_name,        # A: 수하인명
                "",              # B: (빈값)
                rcv_addr,        # C: 주소1
                rcv_addr_detail,  # D: 주소2
                rcv_tel2 or "",  # E: 전화번호
                rcv_tel1 or "",  # F: 핸드폰번호
                1,               # G: 택배수량
                delivery_fee,    # H: 택배운임
                fare_code,       # I: 운임구분
                goods_nm,        # J: 품목명
                "",              # K: (빈값)
                "",              # L: 배송메세지
            ])

        today = datetime.now(KST).strftime("%Y%m%d")
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        filename = f"택배업로드_{today}.xlsx"
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error(f"[SmartStore] 택배 다운로드 실패: {e}", exc_info=True)
        return {"success": False, "message": str(e)}
    finally:
        conn.close()


@router.post("/dispatch")
async def dispatch_orders(req: DispatchRequest, user=Depends(get_current_user)):
    """송장번호 발송처리 (네이버 커머스 API)"""
    try:
        client = _get_naver_client()

        # 30건씩 분할 처리
        dispatches = [
            {
                "productOrderId": item.product_order_id,
                "deliveryMethod": "DELIVERY",
                "deliveryCompanyCode": "KGB",  # 로젠택배
                "trackingNumber": item.tracking_number,
            }
            for item in req.items
        ]

        results = []
        for i in range(0, len(dispatches), 30):
            batch = dispatches[i:i+30]
            result = await client.dispatch_orders(batch)
            results.append(result)

        # DB 업데이트: 송장번호 + 상태
        conn = get_connection()
        try:
            for item in req.items:
                conn.execute(
                    "UPDATE smartstore_orders SET tracking_number = ?, status = 'DISPATCHED' WHERE product_order_id = ?",
                    (item.tracking_number, item.product_order_id),
                )
            conn.commit()
        finally:
            conn.close()

        return {"success": True, "message": f"발송처리 {len(dispatches)}건 완료", "results": results}
    except Exception as e:
        logger.error(f"[SmartStore] 발송처리 실패: {e}", exc_info=True)
        return {"success": False, "message": str(e)}


@router.post("/upload-tracking")
async def upload_tracking(file: UploadFile = File(...), user=Depends(get_current_user)):
    """택배사 결과 엑셀 업로드 → 송장번호 매칭"""
    import openpyxl

    try:
        content = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(content))
        ws = wb.active

        # 택배 결과 파일에서 수하인명, 운송장번호 추출
        tracking_map = {}  # 수하인명 → 운송장번호
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                continue  # 헤더 스킵
            rcv_name = str(row[0] or "").strip() if row[0] else ""
            slip_no = str(row[10] or "").strip() if len(row) > 10 and row[10] else ""
            if not slip_no and len(row) > 11:
                slip_no = str(row[11] or "").strip() if row[11] else ""
            if rcv_name and slip_no:
                tracking_map[rcv_name] = slip_no

        if not tracking_map:
            return {"success": False, "message": "운송장번호를 찾을 수 없습니다."}

        # DB에서 PAYED 주문 로드 → 수취인명 기준 매칭
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT product_order_id, order_id, rcv_name FROM smartstore_orders WHERE status = 'PAYED'"
            ).fetchall()

            matched = []
            for row in rows:
                poid = row["product_order_id"] if hasattr(row, '__getitem__') else row[0]
                oid = row["order_id"] if hasattr(row, '__getitem__') else row[1]
                name = row["rcv_name"] if hasattr(row, '__getitem__') else row[2]

                if name in tracking_map:
                    matched.append({
                        "productOrderId": poid,
                        "orderId": oid,
                        "rcvName": name,
                        "trackingNumber": tracking_map[name],
                    })

            return {
                "success": True,
                "message": f"매칭 완료: {len(matched)}건 / 전체 {len(rows)}건",
                "matched": matched,
                "total": len(rows),
            }
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"[SmartStore] 송장 업로드 실패: {e}", exc_info=True)
        return {"success": False, "message": str(e)}


@router.get("/product-map")
async def get_product_map(user=Depends(get_current_user)):
    """품목코드 매칭 테이블 조회"""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM smartstore_product_map ORDER BY naver_product_no"
        ).fetchall()
        items = []
        for row in rows:
            items.append(dict(row) if hasattr(row, 'keys') else {
                "naver_product_no": row[0],
                "item_code": row[1],
                "model_name": row[2],
            })
        return {"success": True, "items": items, "count": len(items)}
    except Exception as e:
        return {"success": False, "message": str(e), "items": []}
    finally:
        conn.close()


@router.post("/product-map")
async def save_product_map(items: list[ProductMappingItem], user=Depends(get_current_user)):
    """품목코드 매칭 저장 (UPSERT)"""
    conn = get_connection()
    try:
        saved = 0
        for item in items:
            conn.execute("""
                INSERT OR IGNORE INTO smartstore_product_map (naver_product_no, item_code, model_name)
                VALUES (?, ?, ?)
            """, (item.naver_product_no, item.item_code, item.model_name))

            conn.execute("""
                UPDATE smartstore_product_map
                SET item_code = ?, model_name = ?
                WHERE naver_product_no = ?
            """, (item.item_code, item.model_name, item.naver_product_no))
            saved += 1

        conn.commit()
        return {"success": True, "message": f"{saved}건 저장 완료"}
    except Exception as e:
        conn.rollback()
        return {"success": False, "message": str(e)}
    finally:
        conn.close()


@router.delete("/orders")
async def delete_orders(
    status: str = Query("DISPATCHED", description="삭제할 상태"),
    user=Depends(get_current_user),
):
    """처리 완료된 주문 삭제"""
    conn = get_connection()
    try:
        result = conn.execute(
            "DELETE FROM smartstore_orders WHERE status = ?", (status,)
        )
        conn.commit()
        return {"success": True, "message": "삭제 완료"}
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        conn.close()


@router.get("/settings")
async def get_settings(user=Depends(get_current_user)):
    """스마트스토어 설정값 조회"""
    import os
    return {
        "success": True,
        "settings": {
            "naverClientId": os.getenv("NAVER_COMMERCE_CLIENT_ID", ""),
            "naverClientSecretSet": bool(os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "")),
            "custCode": os.getenv("SMARTSTORE_CUST_CODE", ""),
            "empCode": os.getenv("SMARTSTORE_EMP_CODE", ""),
            "whCode": os.getenv("SMARTSTORE_WH_CODE", "30"),
        },
    }
