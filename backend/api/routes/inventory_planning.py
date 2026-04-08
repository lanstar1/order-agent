"""
적정재고 관리 (온라인관리품목) API 라우터
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List

from db.database import get_connection
from services.inventory_planning_service import (
    get_planning_targets, add_planning_target, update_planning_target,
    remove_planning_target, bulk_add_planning_targets,
    analyze_all_targets, analyze_single_product,
    get_daily_sales, get_pending_orders, search_products_master,
    get_all_pending_orders_map,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/inventory-planning", tags=["inventory-planning"])
KST = timezone(timedelta(hours=9))


# ─── Pydantic 모델 ──────────────────────────────────────

class TargetAdd(BaseModel):
    prod_cd: str
    model_name: str = ""
    prod_name: str = ""
    lead_time_days: int = 40
    safety_stock_days: int = 10
    moq: int = 0
    supplier_group: str = ""

class TargetUpdate(BaseModel):
    lead_time_days: Optional[int] = None
    safety_stock_days: Optional[int] = None
    moq: Optional[int] = None
    supplier_group: Optional[str] = None
    is_active: Optional[int] = None

class BulkAddRequest(BaseModel):
    items: List[TargetAdd]


# ─── 분석 (핵심) ─────────────────────────────────────────

@router.get("/analysis")
async def get_analysis():
    """전체 관리품목 적정재고 분석"""
    conn = get_connection()
    try:
        result = analyze_all_targets(conn)
        # daily_sales는 상세 조회에서만 반환 (목록에서는 제외)
        for item in result["items"]:
            item.pop("daily_sales", None)
        return result
    finally:
        conn.close()


@router.get("/analysis/{target_id}")
async def get_analysis_detail(target_id: int):
    """단일 품목 상세 분석 (일별 판매 데이터 포함)"""
    conn = get_connection()
    try:
        targets = get_planning_targets(conn, active_only=False)
        target = next((t for t in targets if t["id"] == target_id), None)
        if not target:
            raise HTTPException(404, "관리품목을 찾을 수 없습니다")
        order_map = get_all_pending_orders_map(conn)
        return analyze_single_product(conn, target, order_map)
    finally:
        conn.close()


# ─── 관리품목 CRUD ───────────────────────────────────────

@router.get("/targets")
async def list_targets(active_only: bool = True):
    conn = get_connection()
    try:
        items = get_planning_targets(conn, active_only)
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


@router.post("/targets")
async def add_target(body: TargetAdd):
    conn = get_connection()
    try:
        add_planning_target(conn, body.prod_cd, body.model_name, body.prod_name,
                           body.lead_time_days, body.safety_stock_days,
                           body.moq, body.supplier_group)
        return {"status": "ok", "prod_cd": body.prod_cd}
    finally:
        conn.close()


@router.post("/targets/bulk")
async def bulk_add_targets(body: BulkAddRequest):
    conn = get_connection()
    try:
        items = [i.dict() for i in body.items]
        added = bulk_add_planning_targets(conn, items)
        return {"status": "ok", "added": added}
    finally:
        conn.close()


@router.put("/targets/{target_id}")
async def update_target(target_id: int, body: TargetUpdate):
    conn = get_connection()
    try:
        update_planning_target(conn, target_id, **body.dict())
        return {"status": "ok"}
    finally:
        conn.close()


@router.delete("/targets/{target_id}")
async def delete_target(target_id: int):
    conn = get_connection()
    try:
        remove_planning_target(conn, target_id)
        return {"status": "ok"}
    finally:
        conn.close()


# ─── 품목 검색 (등록 시 자동완성) ─────────────────────────

@router.get("/search")
async def search_products(q: str = "", limit: int = 20):
    if not q or len(q) < 2:
        return {"items": []}
    conn = get_connection()
    try:
        results = search_products_master(conn, q, limit)
        return {"items": results}
    finally:
        conn.close()


# ─── 일별 판매 이력 ──────────────────────────────────────

@router.get("/daily-sales/{prod_cd}")
async def get_daily_sales_api(prod_cd: str, days: int = 60):
    conn = get_connection()
    try:
        daily = get_daily_sales(conn, prod_cd, days)
        return {"prod_cd": prod_cd, "days": days, "data": daily}
    finally:
        conn.close()


# ─── 오더리스트 확인 ──────────────────────────────────────

@router.get("/orders/{model_name}")
async def check_pending_orders(model_name: str):
    conn = get_connection()
    try:
        orders = get_pending_orders(conn, model_name)
        return {"model_name": model_name, "orders": orders, "has_orders": len(orders) > 0}
    finally:
        conn.close()

# ─── 선적 메일 스캔 ──────────────────────────────────────

@router.post("/shipping/scan")
async def scan_shipping_mails(days_back: int = 90):
    """메일서버에서 선적 메일 스캔 → BOR 첨부파일 파싱 → DB 저장"""
    from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD
    from services.shipping_mail_service import scan_shipping_emails, save_shipping_info

    if not MAIL_USER or not MAIL_PASSWORD:
        raise HTTPException(400, "메일 설정이 없습니다 (MAIL_USER, MAIL_PASSWORD 환경변수 필요)")

    try:
        results = scan_shipping_emails(
            imap_server=MAIL_IMAP_SERVER,
            imap_user=MAIL_USER,
            imap_password=MAIL_PASSWORD,
            imap_port=MAIL_IMAP_PORT,
            days_back=days_back,
        )

        conn = get_connection()
        try:
            saved = save_shipping_info(conn, results)
        finally:
            conn.close()

        return {
            "status": "ok",
            "scanned": len(results),
            "saved": saved,
            "items": [{
                "bor_number": r["bor_number"],
                "shipping_date": r["shipping_date"],
                "arrival_date": r["arrival_date"],
                "model_count": r["model_count"],
                "filename": r["filename"],
            } for r in results],
        }
    except Exception as e:
        raise HTTPException(500, f"메일 스캔 실패: {str(e)}")


@router.get("/shipping/list")
async def list_shipping_info():
    """저장된 선적 정보 목록"""
    from services.shipping_mail_service import get_all_shipping_info
    conn = get_connection()
    try:
        items = get_all_shipping_info(conn)
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


@router.post("/shipping/scan-nam")
async def scan_nam_shipping():
    """NAM 거래처 메일(네이버) 스캔 → PI 엑셀 파싱 → DB + 구글시트 저장"""
    from config import MAIL2_IMAP_SERVER, MAIL2_IMAP_PORT, MAIL2_USER, MAIL2_PASSWORD, MAIL2_SENDER_FILTER
    from services.shipping_mail_service import scan_nam_shipping_emails, save_nam_shipping_info, write_nam_orders_to_sheet

    if not MAIL2_USER or not MAIL2_PASSWORD:
        raise HTTPException(400, "네이버 메일 설정이 없습니다 (MAIL2_USER, MAIL2_PASSWORD)")

    try:
        results = scan_nam_shipping_emails(
            imap_server=MAIL2_IMAP_SERVER,
            imap_user=MAIL2_USER,
            imap_password=MAIL2_PASSWORD,
            sender_filter=MAIL2_SENDER_FILTER,
            imap_port=MAIL2_IMAP_PORT,
            days_back=180,
        )

        # DB 저장 (orderlist_items + shipping_mail_info)
        conn = get_connection()
        try:
            saved = save_nam_shipping_info(conn, results)
        finally:
            conn.close()

        # 구글시트 자동 기록
        sheet_result = {"status": "skipped"}
        try:
            sheet_result = write_nam_orders_to_sheet(results)
        except Exception as e:
            sheet_result = {"status": "error", "error": str(e)}

        return {
            "status": "ok",
            "source": "NAM (Naver)",
            "scanned": len(results),
            "saved": saved,
            "sheet": sheet_result,
            "items": [{
                "pi_number": r["pi_number"],
                "email_date": r["email_date"],
                "item_count": len(r["items"]),
                "filename": r["filename"],
            } for r in results],
        }
    except Exception as e:
        raise HTTPException(500, f"NAM 메일 스캔 실패: {str(e)}")


@router.post("/shipping/scan-all")
async def scan_all_shipping():
    """모든 메일소스 통합 스캔"""
    results = {"sources": []}

    # 1. BOR 메일 (Ecount)
    try:
        from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD
        if MAIL_USER and MAIL_PASSWORD:
            from services.shipping_mail_service import scan_shipping_emails, save_shipping_info
            bor_results = scan_shipping_emails(
                MAIL_IMAP_SERVER, MAIL_USER, MAIL_PASSWORD, MAIL_IMAP_PORT, days_back=90)
            conn = get_connection()
            try:
                save_shipping_info(conn, bor_results)
            finally:
                conn.close()
            results["sources"].append({"name": "BOR (Ecount)", "count": len(bor_results)})
    except Exception as e:
        results["sources"].append({"name": "BOR (Ecount)", "error": str(e)})

    # 2. NAM 메일 (Naver)
    try:
        from config import MAIL2_IMAP_SERVER, MAIL2_IMAP_PORT, MAIL2_USER, MAIL2_PASSWORD, MAIL2_SENDER_FILTER
        if MAIL2_USER and MAIL2_PASSWORD:
            from services.shipping_mail_service import scan_nam_shipping_emails, save_nam_shipping_info
            nam_results = scan_nam_shipping_emails(
                MAIL2_IMAP_SERVER, MAIL2_USER, MAIL2_PASSWORD, MAIL2_SENDER_FILTER, MAIL2_IMAP_PORT, days_back=180)
            conn = get_connection()
            try:
                save_nam_shipping_info(conn, nam_results)
            finally:
                conn.close()
            results["sources"].append({"name": "NAM (Naver)", "count": len(nam_results)})
    except Exception as e:
        results["sources"].append({"name": "NAM (Naver)", "error": str(e)})

    results["status"] = "ok"
    return results


@router.post("/orderlist/sync-bor")
async def sync_bor_orderlist():
    """BOR 거래처 메일에서 REST 엑셀 → 구글시트 오더리스트 덮어쓰기"""
    from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD
    from services.shipping_mail_service import scan_bor_orderlist_emails, sync_bor_orderlist_to_sheet

    if not MAIL_USER or not MAIL_PASSWORD:
        raise HTTPException(400, "Ecount 메일 설정이 없습니다")

    try:
        # 1. 메일에서 REST 파일 스캔
        results = scan_bor_orderlist_emails(
            MAIL_IMAP_SERVER, MAIL_USER, MAIL_PASSWORD, MAIL_IMAP_PORT,
            days_back=90, sender_filter="guzhiyi@bor-cable.com"
        )

        if not results:
            return {"status": "ok", "message": "REST 파일이 있는 메일을 찾지 못했습니다"}

        # 가장 최근 파일만 사용 (덮어쓰기)
        latest = results[0]

        # 2. 구글시트에 덮어쓰기
        sheet_result = sync_bor_orderlist_to_sheet([latest])

        # 3. DB orderlist_items도 갱신 (오더리스트 동기화)
        try:
            from services.orderlist_service import sync_orderlist
            sync_orderlist()
        except Exception as e:
            logger.warning(f"[BOR오더] DB 동기화 실패: {e}")

        return {
            "status": "ok",
            "email_date": latest["email_date"],
            "filename": latest["filename"],
            "sheet": sheet_result,
        }
    except Exception as e:
        raise HTTPException(500, f"BOR 오더리스트 동기화 실패: {str(e)}")


@router.get("/shipping/debug-mail")
async def debug_mail_connection():
    """메일 서버 디버그 — 폴더 목록 + 검색 테스트"""
    import imaplib
    results = {}

    # 1. Ecount 메일
    try:
        from config import MAIL_IMAP_SERVER, MAIL_IMAP_PORT, MAIL_USER, MAIL_PASSWORD
        if MAIL_USER and MAIL_PASSWORD:
            mail = imaplib.IMAP4_SSL(MAIL_IMAP_SERVER, MAIL_IMAP_PORT)
            mail.login(MAIL_USER, MAIL_PASSWORD)

            # 폴더 목록
            status, folders = mail.list()
            folder_names = []
            for f in (folders or []):
                try:
                    folder_names.append(f.decode("utf-8", errors="replace"))
                except:
                    folder_names.append(str(f))

            # INBOX 검색 테스트
            mail.select("INBOX", readonly=True)
            tests = {}

            # 전체 메일 수
            status, data = mail.search(None, "ALL")
            total = len(data[0].split()) if data[0] else 0
            tests["total_inbox"] = total

            # 최근 30일
            from datetime import datetime, timedelta
            since = (datetime.now() - timedelta(days=30)).strftime("%d-%b-%Y")
            status, data = mail.search(None, f"(SINCE {since})")
            tests["last_30d"] = len(data[0].split()) if data[0] else 0

            # shipping 키워드
            for kw in ["shipping", "final", "BOR"]:
                try:
                    status, data = mail.search(None, f'(SUBJECT "{kw}")')
                    tests[f"subject_{kw}"] = len(data[0].split()) if data[0] else 0
                except Exception as e:
                    tests[f"subject_{kw}"] = f"error: {e}"

            # FROM bor-cable
            try:
                status, data = mail.search(None, '(FROM "guzhiyi@bor-cable.com")')
                tests["from_bor"] = len(data[0].split()) if data[0] else 0
            except Exception as e:
                tests["from_bor"] = f"error: {e}"

            # FROM bor-cable (90일)
            try:
                status, data = mail.search(None, f'(SINCE {since} FROM "guzhiyi")')
                tests["from_guzhiyi_30d"] = len(data[0].split()) if data[0] else 0
            except Exception as e:
                tests["from_guzhiyi_30d"] = f"error: {e}"

            mail.logout()
            results["ecount"] = {"folders": folder_names[:20], "tests": tests}
    except Exception as e:
        results["ecount"] = {"error": str(e)}

    # 2. Naver 메일
    try:
        from config import MAIL2_IMAP_SERVER, MAIL2_IMAP_PORT, MAIL2_USER, MAIL2_PASSWORD
        if MAIL2_USER and MAIL2_PASSWORD:
            mail = imaplib.IMAP4_SSL(MAIL2_IMAP_SERVER, MAIL2_IMAP_PORT)
            mail.login(MAIL2_USER, MAIL2_PASSWORD)

            status, folders = mail.list()
            folder_names = []
            for f in (folders or []):
                try:
                    folder_names.append(f.decode("utf-8", errors="replace"))
                except:
                    folder_names.append(str(f))

            mail.select("INBOX", readonly=True)
            tests = {}

            status, data = mail.search(None, "ALL")
            tests["total_inbox"] = len(data[0].split()) if data[0] else 0

            since = (datetime.now() - timedelta(days=180)).strftime("%d-%b-%Y")
            status, data = mail.search(None, f"(SINCE {since})")
            tests["last_180d"] = len(data[0].split()) if data[0] else 0

            # FROM 163.com
            try:
                status, data = mail.search(None, '(FROM "13428934642@163.com")')
                tests["from_163"] = len(data[0].split()) if data[0] else 0
            except Exception as e:
                tests["from_163"] = f"error: {e}"

            try:
                status, data = mail.search(None, '(FROM "163.com")')
                tests["from_163_domain"] = len(data[0].split()) if data[0] else 0
            except Exception as e:
                tests["from_163_domain"] = f"error: {e}"

            mail.logout()
            results["naver"] = {"folders": folder_names[:20], "tests": tests}
    except Exception as e:
        results["naver"] = {"error": str(e)}

    return results
