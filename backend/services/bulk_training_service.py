"""
대량 학습 서비스 (Bulk Training)

여러 발주서 이미지 + 판매전표 엑셀 1개를 업로드하면
AI가 발주서에서 날짜/품명/수량을 추출하고
엑셀 행과 자동 매칭하여 학습 데이터로 저장
"""
import json
import uuid
import re
import logging
import base64
import io
from difflib import SequenceMatcher
from datetime import datetime
from typing import List, Optional
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.database import get_connection, now_kst
from services.training_service import parse_sales_slip_excel, save_training_pair
from anthropic import AsyncAnthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)
client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)


# ─────────────────────────────────────────
#  날짜 정규화
# ─────────────────────────────────────────
def normalize_date(date_str: str) -> str:
    """
    다양한 날짜 형식을 MM/DD로 정규화
    지원: "1/6", "01/06", "2024-01-06", "2024/01/06", "1월6일", "01-06"
    """
    if not date_str:
        return ""
    s = str(date_str).strip()

    # YYYY-MM-DD 또는 YYYY/MM/DD
    m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        return f"{int(m.group(2)):02d}/{int(m.group(3)):02d}"

    # MM-DD 또는 MM/DD
    m = re.match(r"(\d{1,2})[-/](\d{1,2})$", s)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"

    # 한국어: 1월6일, 1월 6일
    m = re.match(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"

    return s


# ─────────────────────────────────────────
#  품명 유사도 매칭
# ─────────────────────────────────────────
def _tokenize(s: str) -> set:
    """문자열을 토큰 set으로 분리"""
    return set(re.split(r'[\s\-_/,]+', s.lower().strip()))


def fuzzy_match_score(po_text: str, excel_name: str) -> float:
    """
    발주서 품명과 엑셀 품명의 유사도 (0.0 ~ 1.0)
    토큰 겹침 50% + 문자열 유사도 50%
    """
    if not po_text or not excel_name:
        return 0.0

    po_lower = po_text.lower().strip()
    ex_lower = excel_name.lower().strip()

    # 완전 일치
    if po_lower == ex_lower:
        return 1.0

    # 부분 문자열 포함
    if po_lower in ex_lower or ex_lower in po_lower:
        return 0.9

    # 토큰 겹침
    po_tokens = _tokenize(po_text)
    ex_tokens = _tokenize(excel_name)
    if po_tokens and ex_tokens:
        overlap = len(po_tokens & ex_tokens)
        total = max(len(po_tokens), len(ex_tokens))
        token_score = overlap / total if total > 0 else 0
    else:
        token_score = 0

    # difflib 유사도
    seq_score = SequenceMatcher(None, po_lower, ex_lower).ratio()

    return token_score * 0.5 + seq_score * 0.5


# ─────────────────────────────────────────
#  세션 관리
# ─────────────────────────────────────────
def create_session(cust_code: str, cust_name: str,
                   excel_bytes: bytes, excel_filename: str) -> dict:
    """
    대량 학습 세션 생성 + 엑셀 파싱
    Returns: {session_id, excel_items: [...], total_items}
    """
    # 엑셀 파싱
    parsed = parse_sales_slip_excel(excel_bytes, excel_filename)
    items = parsed.get("items", [])

    session_id = str(uuid.uuid4())[:12]
    conn = get_connection()
    conn.execute(
        """INSERT INTO bulk_training_sessions
           (session_id, cust_code, cust_name, excel_data, status)
           VALUES(?,?,?,?,?)""",
        (session_id, cust_code, cust_name, json.dumps(items, ensure_ascii=False), "extracting")
    )
    conn.commit()
    conn.close()

    logger.info(f"[BulkTrain] 세션 생성: {session_id}, 엑셀 {len(items)}건")
    return {
        "session_id": session_id,
        "excel_items": items,
        "total_items": len(items),
    }


def get_session(session_id: str) -> Optional[dict]:
    """세션 정보 조회"""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM bulk_training_sessions WHERE session_id=?",
        (session_id,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    result = dict(row)
    result["excel_items"] = json.loads(result.get("excel_data", "[]"))
    return result


# ─────────────────────────────────────────
#  발주서 AI 추출
# ─────────────────────────────────────────
BULK_OCR_SYSTEM = """당신은 B2B 발주서 이미지에서 주문 정보를 추출하는 전문 AI입니다.

## 역할
발주서 이미지에서 다음을 추출합니다:
1. 주문 날짜 (order_date) - MM/DD 형식으로 정규화
2. 각 주문 항목의 품명과 수량

## 출력 형식 (반드시 JSON만 반환)
{
  "order_date": "01/06",
  "items": [
    {
      "product_hint": "상품명/모델명/규격",
      "qty": 10,
      "unit": "EA"
    }
  ]
}

## 규칙
1. order_date는 발주서에 기재된 날짜를 MM/DD 형식으로 변환 (예: 1월 6일 → 01/06)
2. 날짜를 찾을 수 없으면 order_date를 빈 문자열("")로
3. 상품 항목이 아닌 내용(인사말, 배송지, 연락처)은 items에서 제외
4. qty는 숫자(float), 파악 불가 시 null
5. product_hint에는 모델명, 규격, 색상 등 특징적인 표현 포함
6. JSON 외 다른 텍스트는 절대 출력하지 말 것"""


async def extract_po_image(session_id: str, filename: str,
                           image_bytes: bytes, media_type: str) -> dict:
    """
    발주서 이미지에서 AI로 날짜+품명+수량 추출
    Returns: {extraction_id, order_date, items: [...]}
    """
    b64_data = base64.standard_b64encode(image_bytes).decode("utf-8")

    user_content = [
        {
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": b64_data},
        },
        {"type": "text", "text": "이 발주서에서 주문 날짜와 각 주문 항목(품명, 수량)을 추출해주세요."},
    ]

    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            system=BULK_OCR_SYSTEM,
            messages=[{"role": "user", "content": user_content}],
        )

        text = response.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        order_date = normalize_date(result.get("order_date", ""))
        items = result.get("items", [])

    except Exception as e:
        logger.error(f"[BulkTrain] AI 추출 실패 ({filename}): {e}")
        order_date = ""
        items = []

    # DB 저장
    conn = get_connection()
    cur = conn.execute(
        """INSERT INTO bulk_training_extractions
           (session_id, po_filename, po_image, po_image_type,
            order_date, extracted_lines, raw_text, status)
           VALUES(?,?,?,?,?,?,?,?)""",
        (session_id, filename, image_bytes, media_type,
         order_date, json.dumps(items, ensure_ascii=False),
         json.dumps(result, ensure_ascii=False) if items else "",
         "success" if items else "failed")
    )
    extraction_id = cur.lastrowid
    conn.commit()
    conn.close()

    logger.info(f"[BulkTrain] 추출 완료: {filename}, 날짜={order_date}, {len(items)}건")
    return {
        "extraction_id": extraction_id,
        "filename": filename,
        "order_date": order_date,
        "items": items,
        "status": "success" if items else "failed",
    }


# ─────────────────────────────────────────
#  매칭 제안
# ─────────────────────────────────────────
def suggest_matches(session_id: str) -> dict:
    """
    추출된 발주서와 엑셀 데이터를 날짜+품명 기반으로 매칭
    Returns: {
        extractions: [{extraction_id, filename, order_date, items, matches: [...]}],
        unmatched_excel: [...]
    }
    """
    conn = get_connection()

    # 세션 정보
    session = conn.execute(
        "SELECT * FROM bulk_training_sessions WHERE session_id=?",
        (session_id,)
    ).fetchone()
    if not session:
        conn.close()
        return {"error": "세션을 찾을 수 없습니다."}

    excel_items = json.loads(session["excel_data"] or "[]")

    # 엑셀 항목에 날짜 정규화 적용
    for idx, item in enumerate(excel_items):
        item["_idx"] = idx
        item["_norm_date"] = normalize_date(item.get("date", ""))

    # 추출된 발주서들
    extractions = conn.execute(
        """SELECT * FROM bulk_training_extractions
           WHERE session_id=? AND status='success'
           ORDER BY order_date, id""",
        (session_id,)
    ).fetchall()
    conn.close()

    used_excel_indices = set()
    result_extractions = []

    for ext in extractions:
        ext_data = dict(ext)
        po_items = json.loads(ext_data.get("extracted_lines", "[]"))
        po_date = normalize_date(ext_data.get("order_date", ""))

        # 같은 날짜의 엑셀 항목 필터
        date_candidates = [
            ei for ei in excel_items
            if ei["_norm_date"] == po_date and ei["_idx"] not in used_excel_indices
        ]

        # 날짜 ±1일 허용 (정확 매칭이 없을 때)
        if not date_candidates and po_date:
            try:
                po_m, po_d = map(int, po_date.split("/"))
                for ei in excel_items:
                    if ei["_idx"] in used_excel_indices:
                        continue
                    ei_date = ei["_norm_date"]
                    if ei_date:
                        ei_m, ei_d = map(int, ei_date.split("/"))
                        if po_m == ei_m and abs(po_d - ei_d) <= 1:
                            date_candidates.append(ei)
            except (ValueError, AttributeError):
                pass

        # 발주서 각 항목별 매칭
        matches = []
        for po_item in po_items:
            po_hint = po_item.get("product_hint", "")
            po_qty = po_item.get("qty")

            best_match = None
            best_score = 0

            for ei in date_candidates:
                # 품명 유사도
                excel_name = ei.get("product_name", "") or ei.get("model_name", "")
                name_score = fuzzy_match_score(po_hint, excel_name)

                # 수량 일치 보너스
                qty_score = 0
                if po_qty and ei.get("qty"):
                    try:
                        diff = abs(float(po_qty) - float(ei["qty"]))
                        if diff == 0:
                            qty_score = 1.0
                        elif diff / max(float(po_qty), 1) < 0.05:
                            qty_score = 0.8
                        elif diff / max(float(po_qty), 1) < 0.2:
                            qty_score = 0.5
                    except (ValueError, TypeError):
                        pass

                # 종합 점수: 품명 80% + 수량 20%
                total_score = name_score * 0.8 + qty_score * 0.2

                if total_score > best_score:
                    best_score = total_score
                    best_match = ei

            confidence = round(best_score * 100)
            match_entry = {
                "po_item": po_item,
                "confidence": confidence,
            }

            if best_match and confidence >= 30:
                match_entry["excel_item"] = best_match
                match_entry["excel_idx"] = best_match["_idx"]
                if confidence >= 70:
                    used_excel_indices.add(best_match["_idx"])
            else:
                match_entry["excel_item"] = None
                match_entry["excel_idx"] = None

            matches.append(match_entry)

        result_extractions.append({
            "extraction_id": ext_data["id"],
            "filename": ext_data["po_filename"],
            "order_date": po_date,
            "po_items": po_items,
            "matches": matches,
        })

    # 미매칭 엑셀 항목
    unmatched = [ei for ei in excel_items if ei["_idx"] not in used_excel_indices]

    return {
        "extractions": result_extractions,
        "unmatched_excel_count": len(unmatched),
        "total_matched": sum(
            1 for ext in result_extractions
            for m in ext["matches"]
            if m.get("excel_item") and m["confidence"] >= 70
        ),
    }


# ─────────────────────────────────────────
#  확인 및 저장
# ─────────────────────────────────────────
def confirm_and_save(session_id: str, confirmations: list) -> dict:
    """
    사용자가 확인한 매칭을 학습 데이터로 저장

    confirmations: [
        {
            "extraction_id": 1,
            "matches": [
                {"po_item": {...}, "excel_item": {...}}
            ]
        }
    ]
    """
    conn = get_connection()
    session = conn.execute(
        "SELECT * FROM bulk_training_sessions WHERE session_id=?",
        (session_id,)
    ).fetchone()
    if not session:
        conn.close()
        return {"success": False, "error": "세션을 찾을 수 없습니다."}

    cust_code = session["cust_code"]
    cust_name = session["cust_name"]
    conn.close()

    saved_pairs = 0
    saved_items = 0

    for conf in confirmations:
        extraction_id = conf.get("extraction_id")
        matched_items = conf.get("matches", [])

        if not matched_items:
            continue

        # 추출 정보 가져오기
        conn = get_connection()
        ext = conn.execute(
            "SELECT * FROM bulk_training_extractions WHERE id=?",
            (extraction_id,)
        ).fetchone()
        conn.close()

        if not ext:
            continue

        # po_training_items 형식으로 변환
        training_items = []
        raw_po_lines = []
        for m in matched_items:
            po_item = m.get("po_item", {})
            excel_item = m.get("excel_item", {})

            if not excel_item or not excel_item.get("item_code"):
                continue

            raw_po_lines.append(po_item.get("product_hint", ""))

            training_items.append({
                "item_code": excel_item["item_code"],
                "product_name": excel_item.get("product_name", ""),
                "model_name": excel_item.get("model_name", ""),
                "spec": excel_item.get("spec", ""),
                "qty": float(excel_item.get("qty", 0) or 0),
                "unit": excel_item.get("unit", "EA"),
                "unit_price": float(excel_item.get("unit_price", 0) or 0),
                "supply_price": float(excel_item.get("supply_price", 0) or 0),
                "tax": float(excel_item.get("tax", 0) or 0),
                "raw_line_text": po_item.get("product_hint", ""),
            })

        if not training_items:
            continue

        # 발주서 원문 텍스트 구성
        raw_po_text = "\n".join(raw_po_lines)

        # 이미지 데이터
        po_image = ext["po_image"] if ext["po_image"] else None
        po_image_type = ext["po_image_type"] or ""

        # 기존 save_training_pair 함수 활용
        result = save_training_pair(
            cust_code=cust_code,
            cust_name=cust_name,
            raw_po_text=raw_po_text,
            items=training_items,
            memo=f"[대량학습] {ext['po_filename']} ({ext['order_date']})",
            raw_po_image=po_image,
            raw_po_image_type=po_image_type,
        )

        if result.get("success"):
            saved_pairs += 1
            saved_items += result.get("item_count", 0)

    # 세션 상태 업데이트
    conn = get_connection()
    conn.execute(
        "UPDATE bulk_training_sessions SET status='completed', updated_at=? WHERE session_id=?",
        (now_kst(), session_id)
    )
    conn.commit()
    conn.close()

    logger.info(f"[BulkTrain] 저장 완료: {saved_pairs}쌍, {saved_items}품목")
    return {
        "success": True,
        "saved_pairs": saved_pairs,
        "saved_items": saved_items,
    }
