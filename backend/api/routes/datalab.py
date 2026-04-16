"""
데이터랩 - 네이버 쇼핑인사이트 AI 소싱 시스템 API
"""
import json
import logging
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Header, Query, Body
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datalab", tags=["DataLab"])


# ── 인증 헬퍼 ──
def _get_user(authorization: str = Header("")):
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "인증이 필요합니다")
    from security import verify_token
    payload = verify_token(authorization.replace("Bearer ", ""))
    if not payload:
        raise HTTPException(401, "토큰이 유효하지 않습니다")
    return payload


# ── Request Models ──
class AnalyzeRequest(BaseModel):
    category_code: str
    category_name: str = ""
    keywords: List[str]
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD
    time_unit: str = "month"  # date, week, month
    device: str = ""   # "", "pc", "mo"
    gender: str = ""   # "", "m", "f"
    ages: List[str] = []  # ["10","20","30","40","50","60"]


class BrandBlacklistRequest(BaseModel):
    brand_name: str


# ── 카테고리 ──
@router.get("/categories")
async def get_categories(parent_cid: str = Query("", description="상위 카테고리 코드 (빈값=1분류)")):
    """카테고리 트리 조회 (1분류/2분류/3분류 캐스케이드)"""
    from services.datalab_service import get_categories, get_subcategories
    if not parent_cid:
        return {"categories": get_categories()}
    else:
        subs = await get_subcategories(parent_cid)
        return {"categories": subs}


# ── 트렌드 분석 ──
@router.post("/analyze")
async def analyze_trends(req: AnalyzeRequest, authorization: str = Header("")):
    """키워드 트렌드 분석 실행"""
    user = _get_user(authorization)

    if not req.keywords:
        raise HTTPException(400, "키워드를 1개 이상 입력해주세요")
    if len(req.keywords) > 20:
        raise HTTPException(400, "키워드는 최대 20개까지 입력 가능합니다")

    from services.datalab_service import run_full_analysis

    try:
        result = await run_full_analysis(
            category_code=req.category_code,
            category_name=req.category_name,
            keywords=req.keywords,
            start_date=req.start_date,
            end_date=req.end_date,
            time_unit=req.time_unit,
            device=req.device,
            gender=req.gender,
            ages=req.ages,
        )

        # Save to history
        try:
            from db.database import get_connection
            conn = get_connection()
            conn.execute(
                """INSERT INTO datalab_analysis_history
                   (emp_cd, category_code, category_name, keywords, filters, trend_data)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    user.get("emp_cd", ""),
                    req.category_code,
                    req.category_name,
                    json.dumps(req.keywords, ensure_ascii=False),
                    json.dumps({"device": req.device, "gender": req.gender, "ages": req.ages}, ensure_ascii=False),
                    json.dumps(result, ensure_ascii=False),
                )
            )
            conn.commit()
            history_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.close()
            result["history_id"] = history_id
        except Exception as e:
            logger.warning(f"[DataLab] 이력 저장 실패: {e}")

        # Update seed keywords
        try:
            from services.datalab_service import update_seed_keywords
            update_seed_keywords(req.category_code, result.get("keywords", []))
        except Exception as e:
            logger.warning(f"[DataLab] 시드 키워드 업데이트 실패: {e}")

        return result

    except Exception as e:
        logger.error(f"[DataLab] 분석 실패: {e}", exc_info=True)
        raise HTTPException(500, f"분석 중 오류가 발생했습니다: {str(e)}")


# ── AI 인사이트 ──
@router.post("/ai-insight")
async def generate_ai_insight(
    history_id: int = Body(..., embed=True),
    authorization: str = Header("")
):
    """분석 결과 기반 Claude AI 인사이트 생성"""
    user = _get_user(authorization)

    from db.database import get_connection
    conn = get_connection()
    row = conn.execute("SELECT * FROM datalab_analysis_history WHERE id = ?", (history_id,)).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "분석 이력을 찾을 수 없습니다")

    trend_data = json.loads(row["trend_data"] if isinstance(row, dict) else row[6])

    from services.datalab_ai_service import generate_datalab_insight
    insight = await generate_datalab_insight(trend_data)

    # Save AI insight to history
    try:
        conn = get_connection()
        conn.execute(
            "UPDATE datalab_analysis_history SET ai_insight = ? WHERE id = ?",
            (json.dumps(insight, ensure_ascii=False), history_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[DataLab] AI 인사이트 저장 실패: {e}")

    return insight


# ── 분석 이력 ──
@router.get("/history")
async def get_history(
    limit: int = Query(20, ge=1, le=100),
    authorization: str = Header("")
):
    """분석 이력 목록 조회"""
    user = _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, category_code, category_name, keywords, filters, created_at
           FROM datalab_analysis_history
           WHERE emp_cd = ?
           ORDER BY created_at DESC LIMIT ?""",
        (user.get("emp_cd", ""), limit)
    ).fetchall()
    conn.close()

    result = []
    for r in rows:
        result.append({
            "id": r[0],
            "category_code": r[1],
            "category_name": r[2],
            "keywords": json.loads(r[3]) if r[3] else [],
            "filters": json.loads(r[4]) if r[4] else {},
            "created_at": r[5],
        })
    return {"history": result}


@router.get("/history/{history_id}")
async def get_history_detail(history_id: int, authorization: str = Header("")):
    """분석 이력 상세 조회"""
    user = _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM datalab_analysis_history WHERE id = ? AND emp_cd = ?",
        (history_id, user.get("emp_cd", ""))
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "분석 이력을 찾을 수 없습니다")

    return {
        "id": row[0],
        "emp_cd": row[1],
        "category_code": row[2],
        "category_name": row[3],
        "keywords": json.loads(row[4]) if row[4] else [],
        "filters": json.loads(row[5]) if row[5] else {},
        "trend_data": json.loads(row[6]) if row[6] else {},
        "ai_insight": json.loads(row[7]) if row[7] else None,
        "created_at": row[8],
    }


# ── 연관 키워드 추천 ──
@router.get("/suggest-keywords")
async def suggest_keywords(
    query: str = Query(..., min_length=1),
    authorization: str = Header("")
):
    """연관 키워드 추천 (네이버 검색 API 활용)"""
    _get_user(authorization)
    from services.datalab_service import suggest_keywords
    keywords = await suggest_keywords(query)
    return {"suggestions": keywords}


# ── 시드 키워드 ──
@router.get("/seed-keywords")
async def get_seed_keywords(
    category_code: str = Query(...),
    limit: int = Query(20, ge=1, le=50),
    authorization: str = Header("")
):
    """카테고리별 인기 키워드 목록 (축적된 시드 DB)"""
    _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    rows = conn.execute(
        """SELECT keyword, search_count, last_score, last_momentum, updated_at
           FROM datalab_seed_keywords
           WHERE category_code = ?
           ORDER BY search_count DESC LIMIT ?""",
        (category_code, limit)
    ).fetchall()
    conn.close()

    return {"keywords": [
        {"keyword": r[0], "count": r[1], "score": r[2], "momentum": r[3], "updated": r[4]}
        for r in rows
    ]}


# ── 브랜드 블랙리스트 ──
@router.get("/brand-blacklist")
async def get_brand_blacklist(authorization: str = Header("")):
    """제외 브랜드 목록"""
    user = _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, brand_name, created_at FROM datalab_brand_blacklist WHERE emp_cd = ? ORDER BY brand_name",
        (user.get("emp_cd", ""),)
    ).fetchall()
    conn.close()
    return {"blacklist": [{"id": r[0], "name": r[1], "created_at": r[2]} for r in rows]}


@router.post("/brand-blacklist")
async def add_brand_blacklist(req: BrandBlacklistRequest, authorization: str = Header("")):
    """제외 브랜드 추가"""
    user = _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO datalab_brand_blacklist (emp_cd, brand_name) VALUES (?, ?)",
            (user.get("emp_cd", ""), req.brand_name.strip())
        )
        conn.commit()
    finally:
        conn.close()
    return {"success": True}


@router.delete("/brand-blacklist/{item_id}")
async def delete_brand_blacklist(item_id: int, authorization: str = Header("")):
    """제외 브랜드 삭제"""
    user = _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    conn.execute(
        "DELETE FROM datalab_brand_blacklist WHERE id = ? AND emp_cd = ?",
        (item_id, user.get("emp_cd", ""))
    )
    conn.commit()
    conn.close()
    return {"success": True}


# ── 엑셀 내보내기 ──
@router.post("/export-excel")
async def export_excel(history_id: int = Body(..., embed=True), authorization: str = Header("")):
    """분석 결과 엑셀 다운로드"""
    user = _get_user(authorization)
    from db.database import get_connection
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM datalab_analysis_history WHERE id = ?", (history_id,)
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "분석 이력을 찾을 수 없습니다")

    import io
    import openpyxl
    from fastapi.responses import StreamingResponse

    trend_data = json.loads(row[6]) if row[6] else {}

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "트렌드 분석"

    # Header
    ws.append(["키워드", "신뢰도 점수", "모멘텀", "모멘텀(%)", "추천 시즌", "주의 시즌"])

    for kw in trend_data.get("keywords", []):
        ws.append([
            kw.get("keyword", ""),
            kw.get("trust_score", 0),
            kw.get("momentum", ""),
            kw.get("momentum_pct", 0),
            ", ".join([f"{m}월" for m in kw.get("peak_months", [])]),
            ", ".join([f"{m}월" for m in kw.get("low_months", [])]),
        ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"datalab_analysis_{history_id}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
