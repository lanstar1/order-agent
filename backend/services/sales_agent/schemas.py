"""
판매 에이전트 데이터 스키마
- AnalysisMode: MULTI(Mode A) / SINGLE(Mode B)
- SalesData: 파싱된 판매 데이터 컨테이너
- AnalysisResult: 에이전트 분석 결과
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


class AnalysisMode(str, Enum):
    MULTI = "multi"    # Mode A: 다중 거래처 비교 분석
    SINGLE = "single"  # Mode B: 단일 거래처 심층 분석


@dataclass
class SalesData:
    """파싱된 판매 데이터"""
    transactions: list[dict] = field(default_factory=list)
    customers: list[dict] = field(default_factory=list)
    products: list[dict] = field(default_factory=list)
    period_start: str = ""
    period_end: str = ""
    analysis_mode: AnalysisMode = AnalysisMode.MULTI
    target_customer_code: Optional[str] = None
    target_customer_name: Optional[str] = None
    # 원본 파일 메타데이터
    file_name: str = ""
    total_rows: int = 0
    total_customers: int = 0
    total_products: int = 0
    total_amount: int = 0


@dataclass
class AnalysisResult:
    """에이전트 분석 결과"""
    agent_name: str = ""
    analysis_mode: AnalysisMode = AnalysisMode.MULTI
    summary: str = ""
    report_markdown: str = ""
    metrics: dict = field(default_factory=dict)
    visuals: dict = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "agent_name": self.agent_name,
            "analysis_mode": self.analysis_mode.value,
            "summary": self.summary,
            "report_markdown": self.report_markdown,
            "metrics": self.metrics,
            "visuals": self.visuals,
            "recommendations": self.recommendations,
            "raw_data": self.raw_data,
        }
