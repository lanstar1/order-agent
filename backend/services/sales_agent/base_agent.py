"""
BaseAgent - 모든 판매 에이전트의 추상 기반 클래스
"""
from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from .schemas import SalesData, AnalysisResult, AnalysisMode

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """판매 에이전트 기반 클래스"""

    name: str = "base"
    description: str = ""

    def is_single_mode(self, data: SalesData) -> bool:
        return data.analysis_mode == AnalysisMode.SINGLE

    async def analyze(self, data: SalesData) -> AnalysisResult:
        """분석 실행 (모드에 따라 분기)"""
        logger.info(f"[{self.name}] 분석 시작 (mode={data.analysis_mode.value})")
        try:
            if self.is_single_mode(data):
                result = await self._analyze_single(data)
            else:
                result = await self._analyze_multi(data)
            result.agent_name = self.name
            result.analysis_mode = data.analysis_mode
            logger.info(f"[{self.name}] 분석 완료")
            return result
        except Exception as e:
            logger.error(f"[{self.name}] 분석 실패: {e}", exc_info=True)
            return AnalysisResult(
                agent_name=self.name,
                analysis_mode=data.analysis_mode,
                summary=f"분석 중 오류 발생: {str(e)}",
            )

    @abstractmethod
    async def _analyze_multi(self, data: SalesData) -> AnalysisResult:
        """Mode A: 다중 거래처 비교 분석"""
        ...

    @abstractmethod
    async def _analyze_single(self, data: SalesData) -> AnalysisResult:
        """Mode B: 단일 거래처 심층 분석"""
        ...
