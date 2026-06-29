"""
Pydantic 모델 정의 - API 요청/응답 스키마
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum
from datetime import datetime


# ─────────────────────────────────────────
#  열거형
# ─────────────────────────────────────────
class OrderStatus(str, Enum):
    PENDING    = "pending"     # 대기
    PROCESSING = "processing"  # AI 처리중
    REVIEWING  = "reviewing"   # 검토 필요
    CONFIRMED  = "confirmed"   # 확인 완료
    SUBMITTED  = "submitted"   # ERP 전송 완료
    FAILED     = "failed"      # 실패


class MatchConfidence(str, Enum):
    HIGH   = "high"    # 90% 이상 - 자동처리
    MEDIUM = "medium"  # 70~90%   - 검토 권고
    LOW    = "low"     # 70% 미만 - 반드시 검토


# ─────────────────────────────────────────
#  거래처
# ─────────────────────────────────────────
class Customer(BaseModel):
    cust_code: str
    cust_name: str

class CustomerListResponse(BaseModel):
    customers: List[Customer]


# ─────────────────────────────────────────
#  발주서 입력
# ─────────────────────────────────────────
class OrderCreateRequest(BaseModel):
    cust_code:  str = Field(..., description="거래처 코드 (ECOUNT CUST 코드)")
    cust_name:  str = Field(..., description="거래처명")
    raw_text:   Optional[str] = Field(None, description="원문 발주서 텍스트 (붙여넣기)")
    memo:       Optional[str] = Field(None, description="메모")


# ─────────────────────────────────────────
#  상품 매칭 후보
# ─────────────────────────────────────────
class ProductCandidate(BaseModel):
    prod_cd:    str
    prod_name:  str
    model_name: str = Field("", description="모델명")
    score:      float = Field(..., description="유사도 점수 0~1")
    confidence: MatchConfidence
    match_reason: str = Field("", description="매칭 근거 설명")


# ─────────────────────────────────────────
#  추출된 주문 라인
# ─────────────────────────────────────────
class OrderLineExtracted(BaseModel):
    line_no:     int
    raw_text:    str              # 원문 그대로
    qty:         Optional[float] = None
    unit:        Optional[str]   = None
    candidates:  List[ProductCandidate] = []
    selected_cd: Optional[str]   = None   # 사용자가 선택한 PROD_CD
    is_confirmed: bool = False
    price:       Optional[float] = None   # ERP 출고단가 (자동 조회)
    model_name:  Optional[str]  = None   # 선택된 상품의 모델명


# ─────────────────────────────────────────
#  발주서 처리 결과
# ─────────────────────────────────────────
class OrderProcessResponse(BaseModel):
    order_id:    str
    cust_code:   str
    cust_name:   str
    status:      OrderStatus
    lines:       List[OrderLineExtracted]
    created_at:  datetime
    message:     str = ""


# ─────────────────────────────────────────
#  사용자 확인 요청 (라인별 선택)
# ─────────────────────────────────────────
class LineConfirmation(BaseModel):
    line_no:    int
    prod_cd:    str
    qty:        float
    unit:       Optional[str]  = None
    price:      Optional[float] = None   # 사용자가 확인한 단가


class OrderConfirmRequest(BaseModel):
    order_id:    str
    lines:       List[LineConfirmation]


# ─────────────────────────────────────────
#  ERP 전송 결과
# ─────────────────────────────────────────
class ERPSubmitResponse(BaseModel):
    order_id:   str
    success:    bool
    erp_slip_no: Optional[str] = None   # ERP 전표번호
    message:    str


# ─────────────────────────────────────────
#  실시간 처리 상태 (SSE용)
# ─────────────────────────────────────────
class ProcessingEvent(BaseModel):
    event:   str    # "ocr" | "extract" | "match" | "done" | "error"
    message: str
    data:    Optional[dict] = None
    progress: int = 0   # 0~100
