"""
직원 인증 API (JWT + bcrypt)
- GET  /api/auth/employees       : 직원 목록 (이름만)
- POST /api/auth/login           : 로그인 → JWT 토큰 반환
- POST /api/auth/change-password : 비밀번호 변경
- POST /api/auth/reset-password  : 관리자 비밀번호 초기화
- GET  /api/auth/me              : 현재 로그인 사용자 정보
- POST /api/auth/refresh         : 토큰 갱신
"""
import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from db.database import get_connection
from security import (
    hash_password, verify_password, needs_rehash,
    create_token, get_current_user,
    validate_code, sanitize_text,
)
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

router = APIRouter(prefix="/api/auth", tags=["auth"])
logger = logging.getLogger(__name__)


class LoginRequest(BaseModel):
    emp_cd: str = Field(..., min_length=1, max_length=20)
    password: str = Field(..., min_length=1, max_length=128)


class ChangePasswordRequest(BaseModel):
    emp_cd: str = Field(..., min_length=1, max_length=20)
    old_password: str = Field(..., min_length=1, max_length=128)
    new_password: str = Field(..., min_length=4, max_length=128)


class ResetPasswordRequest(BaseModel):
    emp_cd: str = Field(..., min_length=1, max_length=20)
    admin_emp_cd: str = Field(..., min_length=1, max_length=20)


# 관리자 코드 목록 (비밀번호 초기화 권한)
ADMIN_EMP_CDS = {"28", "01"}


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
    성공 시 JWT 토큰 + 사용자 정보 반환
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT emp_cd, name, password_hash FROM employees WHERE emp_cd=?",
        (req.emp_cd,)
    ).fetchone()

    if not row:
        conn.close()
        raise HTTPException(401, "등록되지 않은 직원입니다.")

    if not verify_password(req.password, row["password_hash"]):
        conn.close()
        raise HTTPException(401, "비밀번호가 틀렸습니다.")

    # SHA256 레거시 해시 → bcrypt 자동 마이그레이션
    if needs_rehash(row["password_hash"]):
        new_hash = hash_password(req.password)
        conn.execute(
            "UPDATE employees SET password_hash=? WHERE emp_cd=?",
            (new_hash, req.emp_cd)
        )
        conn.commit()
        logger.info(f"[Auth] 비밀번호 해시 마이그레이션: {row['name']}({req.emp_cd})")

    conn.close()

    token = create_token(row["emp_cd"], row["name"])
    logger.info(f"[Auth] 로그인: [{row['emp_cd']}] {row['name']}")

    return {
        "success": True,
        "emp_cd": row["emp_cd"],
        "name": row["name"],
        "token": token,
    }


@router.get("/me")
async def get_me(user: dict = Depends(get_current_user)):
    """현재 로그인된 사용자 정보"""
    return {"emp_cd": user["emp_cd"], "name": user["name"]}


@router.post("/refresh")
async def refresh_token(user: dict = Depends(get_current_user)):
    """토큰 갱신 (기존 토큰이 유효할 때만)"""
    new_token = create_token(user["emp_cd"], user["name"])
    return {"token": new_token, "emp_cd": user["emp_cd"], "name": user["name"]}


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

    if not verify_password(req.old_password, row["password_hash"]):
        conn.close()
        raise HTTPException(401, "현재 비밀번호가 틀렸습니다.")

    new_hash = hash_password(req.new_password)
    conn.execute(
        "UPDATE employees SET password_hash=? WHERE emp_cd=?",
        (new_hash, req.emp_cd)
    )
    conn.commit()
    conn.close()
    return {"success": True, "message": "비밀번호가 변경되었습니다."}


@router.post("/reset-password")
async def reset_password(req: ResetPasswordRequest):
    """관리자가 직원 비밀번호를 담당자코드로 초기화"""
    if req.admin_emp_cd not in ADMIN_EMP_CDS:
        raise HTTPException(403, "비밀번호 초기화 권한이 없습니다.")

    conn = get_connection()
    row = conn.execute(
        "SELECT emp_cd, name FROM employees WHERE emp_cd=?",
        (req.emp_cd,)
    ).fetchone()

    if not row:
        conn.close()
        raise HTTPException(404, "직원을 찾을 수 없습니다.")

    # 비밀번호를 담당자 코드로 초기화 (bcrypt)
    new_hash = hash_password(req.emp_cd)
    conn.execute(
        "UPDATE employees SET password_hash=? WHERE emp_cd=?",
        (new_hash, req.emp_cd)
    )
    conn.commit()
    conn.close()

    logger.info(f"[Auth] 비밀번호 초기화: {row['name']}({req.emp_cd}) by {req.admin_emp_cd}")
    return {"success": True, "message": f"{row['name']}님의 비밀번호가 초기화되었습니다."}
