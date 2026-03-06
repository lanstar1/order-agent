"""
직원 인증 API
- GET  /api/auth/employees  : 직원 목록 (이름만, 비밀번호 제외)
- POST /api/auth/login      : 로그인 (이름 선택 + 비밀번호)
- POST /api/auth/change-password : 비밀번호 변경
"""
import hashlib
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from db.database import get_connection
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

router = APIRouter(prefix="/api/auth", tags=["auth"])
logger = logging.getLogger(__name__)


def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


class LoginRequest(BaseModel):
    emp_cd: str
    password: str


class ChangePasswordRequest(BaseModel):
    emp_cd: str
    old_password: str
    new_password: str


@router.get("/employees")
async def list_employees():
    """로그인 화면용 직원 목록 (emp_cd, name만 반환)"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT emp_cd, name FROM employees ORDER BY name"
    ).fetchall()
    conn.close()
    return {"employees": [{"emp_cd": r["emp_cd"], "name": r["name"]} for r in rows]}


@router.post("/login")
async def login(req: LoginRequest):
    """
    로그인: emp_cd + 비밀번호 검증
    성공 시 {emp_cd, name} 반환 → 프론트에서 localStorage 저장
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT emp_cd, name, password_hash FROM employees WHERE emp_cd=?",
        (req.emp_cd,)
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(401, "등록되지 않은 직원입니다.")

    if row["password_hash"] != _hash(req.password):
        raise HTTPException(401, "비밀번호가 틀렸습니다.")

    logger.info(f"[Auth] 로그인: [{row['emp_cd']}] {row['name']}")
    return {
        "success": True,
        "emp_cd": row["emp_cd"],
        "name": row["name"],
    }


@router.post("/change-password")
async def change_password(req: ChangePasswordRequest):
    """비밀번호 변경"""
    conn = get_connection()
    row = conn.execute(
        "SELECT password_hash FROM employees WHERE emp_cd=?",
        (req.emp_cd,)
    ).fetchone()

    if not row:
        conn.close()
        raise HTTPException(404, "직원을 찾을 수 없습니다.")

    if row["password_hash"] != _hash(req.old_password):
        conn.close()
        raise HTTPException(401, "현재 비밀번호가 틀렸습니다.")

    conn.execute(
        "UPDATE employees SET password_hash=? WHERE emp_cd=?",
        (_hash(req.new_password), req.emp_cd)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "비밀번호가 변경되었습니다."}
