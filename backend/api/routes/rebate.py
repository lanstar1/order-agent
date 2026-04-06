"""리베이트 계산/미리보기/ERP 제출 API"""
import csv
import io
import json
import logging
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel

from services.rebate_config import load_rebate_settings, save_rebate_settings, ERP_COM_CODE, ERP_USER_ID, ERP_API_KEY, ERP_ZONE
from services.rebate_service import calculate_rebates, update_customer_codes
from services.rebate_erp_client import RebateERPClient
from db.database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/rebate", tags=["rebate"])

_cache: dict[int, dict] = {}


def _get_rebate_db():
    """리베이트용 DB 연결 (order-agent의 기존 DB 사용)."""
    return get_connection()


def _ensure_rebate_tables():
    """리베이트 테이블 존재 확인 및 생성."""
    conn = _get_rebate_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rebate_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                target_month TEXT NOT NULL,
                total_customers INTEGER DEFAULT 0,
                total_rebate INTEGER DEFAULT 0,
                status TEXT DEFAULT 'calculated',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                submitted_at TIMESTAMP,
                csv_filename TEXT,
                approved_by TEXT,
                approved_at TIMESTAMP,
                approval_status TEXT DEFAULT 'pending',
                approval_note TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rebate_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                customer_code TEXT,
                tier TEXT NOT NULL,
                total_sales INTEGER DEFAULT 0,
                main_sales INTEGER DEFAULT 0,
                lanstar3_sales INTEGER DEFAULT 0,
                lanstar5_sales INTEGER DEFAULT 0,
                printer_sales INTEGER DEFAULT 0,
                main_rebate INTEGER DEFAULT 0,
                lanstar3_rebate INTEGER DEFAULT 0,
                lanstar5_rebate INTEGER DEFAULT 0,
                printer_rebate INTEGER DEFAULT 0,
                total_rebate INTEGER DEFAULT 0,
                is_exception INTEGER DEFAULT 0,
                is_excluded INTEGER DEFAULT 0,
                manual_adjustment INTEGER DEFAULT 0,
                erp_status TEXT DEFAULT 'pending',
                erp_slip_no TEXT,
                emp_cd TEXT,
                returns_amount INTEGER DEFAULT 0,
                FOREIGN KEY (run_id) REFERENCES rebate_runs(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rebate_customer_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_name TEXT UNIQUE NOT NULL,
                customer_code TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        # Add missing columns to existing tables
        _add_column_if_not_exists(conn, "rebate_runs", "approved_by", "TEXT")
        _add_column_if_not_exists(conn, "rebate_runs", "approved_at", "TIMESTAMP")
        _add_column_if_not_exists(conn, "rebate_runs", "approval_status", "TEXT DEFAULT 'pending'")
        _add_column_if_not_exists(conn, "rebate_runs", "approval_note", "TEXT")
        _add_column_if_not_exists(conn, "rebate_details", "returns_amount", "INTEGER DEFAULT 0")
    finally:
        conn.close()


def _add_column_if_not_exists(conn, table_name: str, column_name: str, column_def: str):
    """컬럼이 없으면 추가."""
    try:
        # SQLite와 PostgreSQL 모두 지원
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        conn.commit()
    except Exception:
        # 컬럼이 이미 있거나 다른 오류 - 무시
        pass


# 모듈 로드 시 테이블 확인
try:
    _ensure_rebate_tables()
except Exception:
    pass


@router.post("/calculate")
async def calculate(file: UploadFile = File(...)):
    if not file.filename.endswith((".csv", ".CSV")):
        raise HTTPException(400, "CSV 파일만 업로드 가능합니다.")

    content = await file.read()

    for encoding in ["utf-8-sig", "utf-8", "cp949", "euc-kr"]:
        try:
            csv_text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise HTTPException(400, "파일 인코딩을 인식할 수 없습니다. (UTF-8 또는 CP949)")

    settings = load_rebate_settings()

    try:
        result = calculate_rebates(csv_text, settings)
    except ValueError as e:
        raise HTTPException(400, str(e))

    # 거래처 마스터에서 코드 매핑
    db = _get_rebate_db()
    try:
        rows = db.execute("SELECT customer_name, customer_code FROM rebate_customer_master").fetchall()
        master = {}
        for r in rows:
            if hasattr(r, '__getitem__'):
                master[r["customer_name"]] = r["customer_code"]
            else:
                master[r[0]] = r[1]
        result = update_customer_codes(result, master)
    finally:
        db.close()

    # Feature 1: 이중 지급 방지 - 기존 제출 리베이트 확인
    db = _get_rebate_db()
    duplicate_warning = None
    try:
        existing_run = db.execute(
            "SELECT id, submitted_at FROM rebate_runs WHERE target_month = ? AND status = 'submitted' ORDER BY submitted_at DESC LIMIT 1",
            (result["target_month"],)
        ).fetchone()
        if existing_run:
            duplicate_warning = {
                "existing_run_id": existing_run["id"],
                "submitted_at": existing_run["submitted_at"],
                "message": "해당 월은 이미 ERP 전표가 제출되었습니다."
            }

        # 계산된 상태의 리베이트도 경고
        calculated_run = db.execute(
            "SELECT id, created_at FROM rebate_runs WHERE target_month = ? AND status = 'calculated' ORDER BY created_at DESC LIMIT 1",
            (result["target_month"],)
        ).fetchone()
        if calculated_run and not duplicate_warning:
            duplicate_warning = {
                "existing_run_id": calculated_run["id"],
                "created_at": calculated_run["created_at"],
                "message": "해당 월의 이전 계산 결과가 있습니다. (아직 제출되지 않음)"
            }
    finally:
        db.close()

    # DB에 실행 이력 저장
    db = _get_rebate_db()
    try:
        cursor = db.execute(
            """INSERT INTO rebate_runs (run_date, target_month, total_customers, total_rebate, status, csv_filename)
               VALUES (?, ?, ?, ?, 'calculated', ?)""",
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                result["target_month"],
                result["summary"]["total_customers"],
                result["summary"]["total_rebate"],
                file.filename,
            ),
        )
        run_id = cursor.lastrowid

        for cust in result["customers"]:
            returns_amt = cust.get("returns_amount", 0)
            db.execute(
                """INSERT INTO rebate_details
                   (run_id, customer_name, customer_code, tier, total_sales,
                    main_sales, lanstar3_sales, lanstar5_sales, printer_sales,
                    main_rebate, lanstar3_rebate, lanstar5_rebate, printer_rebate,
                    total_rebate, is_exception, is_excluded, manual_adjustment, emp_cd, returns_amount)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id,
                    cust["customer_name"],
                    cust["customer_code"],
                    cust["tier"],
                    cust["total_sales"],
                    cust["main_sales"],
                    cust["lanstar3_sales"],
                    cust["lanstar5_sales"],
                    cust["printer_sales"],
                    cust["main_rebate"],
                    cust["lanstar3_rebate"],
                    cust["lanstar5_rebate"],
                    cust["printer_rebate"],
                    cust["total_rebate"],
                    1 if cust["is_exception"] else 0,
                    0,
                    0,
                    settings.get("customer_employees", {}).get(cust["customer_name"], ""),
                    returns_amt,
                ),
            )

        db.commit()
    finally:
        db.close()

    result["run_id"] = run_id
    result["status"] = "calculated"
    if duplicate_warning:
        result["duplicate_warning"] = duplicate_warning
    _cache[run_id] = result

    return result


@router.get("/preview/{run_id}")
async def preview(run_id: int):
    if run_id in _cache:
        return _cache[run_id]

    db = _get_rebate_db()
    try:
        run = db.execute("SELECT * FROM rebate_runs WHERE id = ?", (run_id,)).fetchone()
        if not run:
            raise HTTPException(404, "계산 결과를 찾을 수 없습니다.")

        details = db.execute(
            "SELECT * FROM rebate_details WHERE run_id = ? ORDER BY total_sales DESC",
            (run_id,),
        ).fetchall()

        customers = []
        for d in details:
            customers.append({
                "id": d["id"],
                "customer_name": d["customer_name"],
                "customer_code": d["customer_code"],
                "tier": d["tier"],
                "total_sales": d["total_sales"],
                "main_sales": d["main_sales"],
                "lanstar3_sales": d["lanstar3_sales"],
                "lanstar5_sales": d["lanstar5_sales"],
                "printer_sales": d["printer_sales"],
                "main_rebate": d["main_rebate"],
                "lanstar3_rebate": d["lanstar3_rebate"],
                "lanstar5_rebate": d["lanstar5_rebate"],
                "printer_rebate": d["printer_rebate"],
                "total_rebate": d["total_rebate"],
                "is_exception": bool(d["is_exception"]),
                "is_excluded": bool(d["is_excluded"]),
                "manual_adjustment": d["manual_adjustment"],
                "erp_status": d["erp_status"],
                "emp_cd": d["emp_cd"] or "",
                "returns_amount": d.get("returns_amount", 0),  # Feature 5
            })

        active = [c for c in customers if not c["is_excluded"]]
        tier_10 = [c for c in active if c["tier"] == "10%"]
        tier_5 = [c for c in active if c["tier"] == "5%"]

        result = {
            "run_id": run_id,
            "target_month": run["target_month"],
            "status": run["status"],
            "approval_status": run.get("approval_status", "pending"),  # Feature 3
            "approved_by": run.get("approved_by"),  # Feature 3
            "approved_at": run.get("approved_at"),  # Feature 3
            "summary": {
                "total_customers": len(active),
                "tier_10_count": len(tier_10),
                "tier_5_count": len(tier_5),
                "total_rebate": sum(c["total_rebate"] + c["manual_adjustment"] for c in active),
                "tier_10_rebate": sum(c["total_rebate"] + c["manual_adjustment"] for c in tier_10),
                "tier_5_rebate": sum(c["total_rebate"] + c["manual_adjustment"] for c in tier_5),
            },
            "customers": customers,
        }

        _cache[run_id] = result
        return result
    finally:
        db.close()


class UpdateDetailRequest(BaseModel):
    is_excluded: bool | None = None
    manual_adjustment: int | None = None
    emp_cd: str | None = None


@router.put("/detail/{detail_id}")
async def update_detail(detail_id: int, req: UpdateDetailRequest):
    db = _get_rebate_db()
    try:
        detail = db.execute("SELECT * FROM rebate_details WHERE id = ?", (detail_id,)).fetchone()
        if not detail:
            raise HTTPException(404, "상세 데이터를 찾을 수 없습니다.")

        updates = []
        params = []
        if req.is_excluded is not None:
            updates.append("is_excluded = ?")
            params.append(1 if req.is_excluded else 0)
        if req.manual_adjustment is not None:
            updates.append("manual_adjustment = ?")
            params.append(req.manual_adjustment)
        if req.emp_cd is not None:
            updates.append("emp_cd = ?")
            params.append(req.emp_cd)

        if updates:
            params.append(detail_id)
            db.execute(
                f"UPDATE rebate_details SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            db.commit()

            run_id = detail["run_id"]
            _cache.pop(run_id, None)

        return {"status": "ok"}
    finally:
        db.close()


class SubmitRequest(BaseModel):
    run_id: int
    io_date: str
    force: bool = False


@router.post("/submit")
async def submit_to_erp(req: SubmitRequest):
    if not ERP_API_KEY:
        raise HTTPException(400, "ERP API 키가 설정되지 않았습니다. (.env 파일 확인)")

    db = _get_rebate_db()
    try:
        run = db.execute("SELECT * FROM rebate_runs WHERE id = ?", (req.run_id,)).fetchone()
        if not run:
            raise HTTPException(404, "계산 결과를 찾을 수 없습니다.")
        if run["status"] == "submitted":
            raise HTTPException(400, "이미 제출된 리베이트입니다.")

        # Feature 3: 승인 워크플로우 - 승인 상태 확인
        if run.get("approval_status") != "approved":
            raise HTTPException(400, "승인되지 않은 리베이트입니다. 먼저 승인을 요청하세요.")

        # Feature 1: 이중 지급 방지 - 같은 월의 다른 제출된 리베이트 확인
        if not req.force:
            other_submitted = db.execute(
                "SELECT id, submitted_at FROM rebate_runs WHERE target_month = ? AND status = 'submitted' AND id != ? LIMIT 1",
                (run["target_month"], req.run_id)
            ).fetchone()
            if other_submitted:
                raise HTTPException(
                    400,
                    f"다른 리베이트가 이미 제출되었습니다. (ID: {other_submitted['id']}, 제출: {other_submitted['submitted_at']})"
                )

        details = db.execute(
            """SELECT * FROM rebate_details
               WHERE run_id = ? AND is_excluded = 0 AND total_rebate > 0
               ORDER BY total_sales DESC""",
            (req.run_id,),
        ).fetchall()

        if not details:
            raise HTTPException(400, "제출할 리베이트 데이터가 없습니다.")

        missing_codes = [d["customer_name"] for d in details if not d["customer_code"]]
        if missing_codes:
            raise HTTPException(
                400,
                f"거래처코드가 없는 거래처가 있습니다: {', '.join(missing_codes[:5])}... "
                "설정 > 거래처 마스터에서 코드를 등록해주세요.",
            )
    finally:
        db.close()

    settings = load_rebate_settings()
    erp_defaults = settings["erp_defaults"]

    erp = RebateERPClient(ERP_COM_CODE, ERP_USER_ID, ERP_API_KEY, ERP_ZONE)

    results = []
    success_count = 0
    fail_count = 0

    try:
        for d in details:
            final_rebate = d["total_rebate"] + d["manual_adjustment"]
            if final_rebate <= 0:
                continue

            month_str = run["target_month"].split("-")[1] if "-" in run["target_month"] else ""
            remarks = erp_defaults["remarks_format"].format(month=month_str.lstrip("0"))

            try:
                resp = erp.create_rebate_slip(
                    io_date=req.io_date,
                    customer_code=d["customer_code"],
                    customer_name=d["customer_name"],
                    rebate_amount=final_rebate,
                    emp_cd=d["emp_cd"] or "",
                    wh_cd=erp_defaults["wh_cd"],
                    prod_cd=erp_defaults["prod_cd"],
                    prod_des=erp_defaults["prod_des"],
                    remarks=remarks,
                    io_type=erp_defaults["io_type"],
                )

                erp_status = "success" if resp.get("Status") == "200" else "failed"
                slip_no = ""
                if erp_status == "success":
                    success_count += 1
                    try:
                        slip_no = resp.get("Data", {}).get("Datas", [{}])[0].get("SLIP_NO", "")
                    except (IndexError, AttributeError):
                        pass
                else:
                    fail_count += 1
                    logger.error(f"ERP 전표 실패 [{d['customer_name']}]: {resp}")

            except Exception as e:
                erp_status = "error"
                slip_no = ""
                fail_count += 1
                logger.error(f"ERP 전표 에러 [{d['customer_name']}]: {e}")

            results.append({
                "customer_name": d["customer_name"],
                "rebate_amount": final_rebate,
                "erp_status": erp_status,
                "slip_no": slip_no,
            })

            db2 = _get_rebate_db()
            try:
                db2.execute(
                    "UPDATE rebate_details SET erp_status = ?, erp_slip_no = ? WHERE id = ?",
                    (erp_status, slip_no, d["id"]),
                )
                db2.commit()
            finally:
                db2.close()
    finally:
        erp.close()

    db = _get_rebate_db()
    try:
        db.execute(
            "UPDATE rebate_runs SET status = 'submitted', submitted_at = ? WHERE id = ?",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), req.run_id),
        )
        db.commit()
    finally:
        db.close()

    _cache.pop(req.run_id, None)

    return {
        "status": "ok",
        "success_count": success_count,
        "fail_count": fail_count,
        "total_rebate": sum(r["rebate_amount"] for r in results),
        "details": results,
    }


@router.get("/history")
async def history():
    db = _get_rebate_db()
    try:
        runs = db.execute(
            "SELECT * FROM rebate_runs ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
        return [dict(r) for r in runs]
    finally:
        db.close()


# ═══════════════════════════════════════════
# Feature 3: 승인 워크플로우
# ═══════════════════════════════════════════

class ApprovalRequest(BaseModel):
    run_id: int
    action: str  # "approve" or "reject"
    note: str = ""
    emp_cd: str


@router.post("/approve")
async def approve_rebate(req: ApprovalRequest):
    if req.action not in ("approve", "reject"):
        raise HTTPException(400, "action은 'approve' 또는 'reject'만 가능합니다.")

    db = _get_rebate_db()
    try:
        run = db.execute("SELECT * FROM rebate_runs WHERE id = ?", (req.run_id,)).fetchone()
        if not run:
            raise HTTPException(404, "계산 결과를 찾을 수 없습니다.")

        approval_status = "approved" if req.action == "approve" else "rejected"
        db.execute(
            """UPDATE rebate_runs
               SET approval_status = ?, approved_by = ?, approved_at = ?, approval_note = ?
               WHERE id = ?""",
            (
                approval_status,
                req.emp_cd,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                req.note,
                req.run_id,
            ),
        )
        db.commit()

        _cache.pop(req.run_id, None)
        return {"status": "ok", "approval_status": approval_status}
    finally:
        db.close()


# ═══════════════════════════════════════════
# Feature 2: 전월 대비 이상치 감지
# ═══════════════════════════════════════════

@router.get("/anomalies/{run_id}")
async def detect_anomalies(run_id: int):
    db = _get_rebate_db()
    try:
        run = db.execute("SELECT * FROM rebate_runs WHERE id = ?", (run_id,)).fetchone()
        if not run:
            raise HTTPException(404, "계산 결과를 찾을 수 없습니다.")

        current_details = db.execute(
            "SELECT * FROM rebate_details WHERE run_id = ?",
            (run_id,),
        ).fetchall()

        # 이전 월 데이터 찾기
        prev_run = db.execute(
            "SELECT * FROM rebate_runs WHERE target_month < ? AND status IN ('calculated', 'submitted') ORDER BY target_month DESC LIMIT 1",
            (run["target_month"],)
        ).fetchone()

        if not prev_run:
            return {"anomalies": [], "prev_month": None}

        # 이전 월 거래처별 판매액
        prev_details = db.execute(
            "SELECT customer_name, total_sales FROM rebate_details WHERE run_id = ?",
            (prev_run["id"],),
        ).fetchall()
        prev_sales_map = {d["customer_name"]: d["total_sales"] for d in prev_details}

        # 이상치 감지 (30% 이상 변경)
        anomalies = []
        for curr in current_details:
            cust_name = curr["customer_name"]
            curr_sales = curr["total_sales"]
            if cust_name in prev_sales_map:
                prev_sales = prev_sales_map[cust_name]
                if prev_sales > 0:
                    change_pct = ((curr_sales - prev_sales) / prev_sales) * 100
                    if abs(change_pct) >= 30:
                        anomalies.append({
                            "customer_name": cust_name,
                            "current_sales": curr_sales,
                            "prev_sales": prev_sales,
                            "change_pct": round(change_pct, 1),
                            "direction": "up" if change_pct > 0 else "down",
                        })

        return {
            "anomalies": sorted(anomalies, key=lambda x: abs(x["change_pct"]), reverse=True),
            "prev_month": prev_run["target_month"]
        }
    finally:
        db.close()


# ═══════════════════════════════════════════
# Feature 4: 거래처별 정산내역서 PDF (데이터 제공)
# ═══════════════════════════════════════════

@router.get("/statement/{run_id}")
async def get_statement_data(run_id: int, customer_ids: str = "all"):
    db = _get_rebate_db()
    try:
        run = db.execute("SELECT * FROM rebate_runs WHERE id = ?", (run_id,)).fetchone()
        if not run:
            raise HTTPException(404, "계산 결과를 찾을 수 없습니다.")

        if customer_ids == "all":
            details = db.execute(
                "SELECT * FROM rebate_details WHERE run_id = ? ORDER BY customer_name",
                (run_id,),
            ).fetchall()
        else:
            ids = [int(x.strip()) for x in customer_ids.split(",")]
            placeholders = ",".join(["?"] * len(ids))
            details = db.execute(
                f"SELECT * FROM rebate_details WHERE run_id = ? AND id IN ({placeholders}) ORDER BY customer_name",
                [run_id] + ids,
            ).fetchall()

        customers = [dict(d) for d in details]
        return {
            "run_id": run_id,
            "target_month": run["target_month"],
            "created_at": run["created_at"],
            "customers": customers,
        }
    finally:
        db.close()


# ═══════════════════════════════════════════
# 리베이트 설정 API
# ═══════════════════════════════════════════

@router.get("/settings")
async def get_rebate_settings():
    return load_rebate_settings()


@router.put("/settings")
async def update_rebate_settings_all(settings: dict):
    save_rebate_settings(settings)
    return {"status": "ok"}


class TierThresholdsRequest(BaseModel):
    tier_10_min: int
    tier_5_min: int

@router.put("/settings/tier-thresholds")
async def update_tier_thresholds(req: TierThresholdsRequest):
    settings = load_rebate_settings()
    settings["tier_thresholds"] = {"tier_10_min": req.tier_10_min, "tier_5_min": req.tier_5_min}
    save_rebate_settings(settings)
    return {"status": "ok"}


class ExceptionCustomer(BaseModel):
    name: str
    code: str
    min_tier: str

@router.put("/settings/exceptions")
async def update_exceptions(exceptions: list[ExceptionCustomer]):
    settings = load_rebate_settings()
    settings["exception_customers"] = [e.model_dump() for e in exceptions]
    save_rebate_settings(settings)
    return {"status": "ok"}


@router.put("/settings/excluded-customers")
async def update_excluded_customers(customers: list[str]):
    settings = load_rebate_settings()
    settings["excluded_customers"] = customers
    save_rebate_settings(settings)
    return {"status": "ok"}


class DiscountRateUpdate(BaseModel):
    category: str
    rate_5: float
    rate_10: float

@router.put("/settings/discount-rates")
async def update_discount_rates(rates: list[DiscountRateUpdate]):
    settings = load_rebate_settings()
    for rate in rates:
        if rate.category in settings["discount_rates"]:
            settings["discount_rates"][rate.category] = {
                "5%": rate.rate_5,
                "10%": rate.rate_10,
            }
    save_rebate_settings(settings)
    return {"status": "ok"}


class AllowedCustomersUpdate(BaseModel):
    use_allowed_list: bool
    customers: list[str]
    aliases: dict[str, str] = {}

@router.put("/settings/allowed-customers")
async def update_allowed_customers(req: AllowedCustomersUpdate):
    settings = load_rebate_settings()
    settings["use_allowed_list"] = req.use_allowed_list
    settings["allowed_customers"] = req.customers
    settings["allowed_customer_aliases"] = req.aliases
    save_rebate_settings(settings)
    return {"status": "ok"}


class RateUpgradeCustomer(BaseModel):
    name: str
    description: str = ""
    upgrades: dict[str, float]

@router.put("/settings/rate-upgrade-customers")
async def update_rate_upgrade_customers(entries: list[RateUpgradeCustomer]):
    settings = load_rebate_settings()
    settings["rate_upgrade_customers"] = [e.model_dump() for e in entries]
    save_rebate_settings(settings)
    return {"status": "ok"}


class EmployeeMapping(BaseModel):
    customer_name: str
    emp_cd: str

@router.put("/settings/customer-employees")
async def update_customer_employees(mappings: list[EmployeeMapping]):
    settings = load_rebate_settings()
    settings["customer_employees"] = {m.customer_name: m.emp_cd for m in mappings}
    save_rebate_settings(settings)
    return {"status": "ok"}


@router.get("/settings/customer-master")
async def get_customer_master():
    db = _get_rebate_db()
    try:
        rows = db.execute("SELECT * FROM rebate_customer_master ORDER BY customer_name").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


@router.post("/settings/customer-master/upload")
async def upload_customer_master(file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename.lower()

    if filename.endswith((".xlsx", ".xls")):
        try:
            import openpyxl
            import io as _io

            wb = openpyxl.load_workbook(_io.BytesIO(content), read_only=True)
            ws = wb.active
            rows_data = list(ws.iter_rows(values_only=True))
            wb.close()

            if not rows_data:
                raise HTTPException(400, "빈 파일입니다.")

            header_row_idx = None
            code_idx = None
            name_idx = None
            for row_idx, row in enumerate(rows_data):
                row_strs = [str(h).strip() if h else "" for h in row]
                for i, h in enumerate(row_strs):
                    if "거래처코드" in h:
                        code_idx = i
                    if "거래처명" in h:
                        name_idx = i
                if code_idx is not None and name_idx is not None:
                    header_row_idx = row_idx
                    break
                code_idx = None
                name_idx = None

            if header_row_idx is None:
                raise HTTPException(400, "'거래처코드'와 '거래처명' 컬럼이 필요합니다.")

            records = []
            for row in rows_data[header_row_idx + 1:]:
                code = str(row[code_idx]).strip() if row[code_idx] else ""
                name = str(row[name_idx]).strip() if row[name_idx] else ""
                if code and name:
                    records.append((name, code))

        except ImportError:
            raise HTTPException(500, "openpyxl 패키지가 필요합니다.")

    elif filename.endswith(".csv"):
        for encoding in ["utf-8-sig", "utf-8", "cp949"]:
            try:
                csv_text = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise HTTPException(400, "파일 인코딩 오류")

        reader = csv.DictReader(io.StringIO(csv_text))
        records = []
        for row in reader:
            code = row.get("거래처코드", "").strip()
            name = row.get("거래처명", "").strip()
            if code and name:
                records.append((name, code))
    else:
        raise HTTPException(400, "CSV 또는 Excel 파일만 지원합니다.")

    if not records:
        raise HTTPException(400, "유효한 데이터가 없습니다.")

    db = _get_rebate_db()
    try:
        for name, code in records:
            db.execute(
                """INSERT INTO rebate_customer_master (customer_name, customer_code)
                   VALUES (?, ?)
                   ON CONFLICT(customer_name) DO UPDATE SET customer_code = ?, updated_at = CURRENT_TIMESTAMP""",
                (name, code, code),
            )
        db.commit()
    finally:
        db.close()

    return {"status": "ok", "count": len(records)}


@router.put("/settings/erp-defaults")
async def update_erp_defaults(defaults: dict):
    settings = load_rebate_settings()
    settings["erp_defaults"] = defaults
    save_rebate_settings(settings)
    return {"status": "ok"}
