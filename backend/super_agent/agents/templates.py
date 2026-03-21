"""
프롬프트 템플릿 라이브러리
자주 사용되는 분석 요청을 원클릭으로 실행
"""

TEMPLATES = [
    {
        "id": "monthly_sales",
        "category": "매출분석",
        "title": "월간 매출 분석 보고서",
        "description": "이번 달 매출을 분석하여 경영진 보고서를 생성합니다",
        "prompt": "이번 달 매출 데이터를 전월 대비 분석하여 경영진 보고서를 작성해줘. 거래처별 매출 변동, 주요 제품 성과, 이상치를 포함해줘.",
        "deliverable_type": "report",
        "icon": "📊",
        "requires_file": False,
    },
    {
        "id": "client_health",
        "category": "거래처관리",
        "title": "거래처 건강도 점검",
        "description": "거래처별 매출 추이와 이탈 위험을 분석합니다",
        "prompt": "거래처별 최근 3개월 매출 추이를 분석하고, RFM 기반으로 이탈 위험 거래처를 식별해줘. 각 등급별 관리 전략도 제안해줘.",
        "deliverable_type": "report",
        "icon": "🏢",
        "requires_file": False,
    },
    {
        "id": "inventory_abc",
        "category": "재고관리",
        "title": "재고 ABC 분석",
        "description": "재고를 ABC 분류하고 품절 위험 품목을 식별합니다",
        "prompt": "현재 재고 데이터를 ABC 분석하여 A급(매출 기여 상위 70%), B급(20%), C급(10%)으로 분류하고, 품절 위험 품목과 과잉 재고 품목을 식별해줘.",
        "deliverable_type": "sheet",
        "icon": "📦",
        "requires_file": True,
    },
    {
        "id": "pricing_margin",
        "category": "가격전략",
        "title": "마진 분석 리포트",
        "description": "품목별/거래처별 마진율을 분석합니다",
        "prompt": "품목별, 거래처별 매출이익률을 분석하고, 마진이 낮은 거래를 식별해줘. 가격 조정이 필요한 품목과 거래처를 제안해줘.",
        "deliverable_type": "report",
        "icon": "💰",
        "requires_file": True,
    },
    {
        "id": "market_trend",
        "category": "시장조사",
        "title": "시장 동향 브리핑",
        "description": "업계 동향과 경쟁 환경을 분석합니다",
        "prompt": "우리 업계의 최근 시장 동향, 주요 경쟁사 동향, 신규 트렌드를 조사하여 임원 브리핑 자료를 만들어줘.",
        "deliverable_type": "slides",
        "icon": "🔍",
        "requires_file": False,
    },
    {
        "id": "meeting_brief",
        "category": "미팅준비",
        "title": "거래처 미팅 브리핑",
        "description": "거래처 미팅을 위한 준비 자료를 생성합니다",
        "prompt": "거래처 미팅을 위한 브리핑 자료를 만들어줘. 해당 거래처의 최근 거래 내역, 주요 이슈, 미팅 어젠다, 예상 질문과 대응 방안을 포함해줘.",
        "deliverable_type": "slides",
        "icon": "🤝",
        "requires_file": False,
    },
    {
        "id": "cs_report",
        "category": "CS관리",
        "title": "CS/반품 분석 리포트",
        "description": "반품 및 불량 패턴을 분석합니다",
        "prompt": "최근 반품/불량 데이터를 분석하여 주요 원인별 파레토 분석, 거래처별 반품률, 제품별 불량 패턴을 정리해줘. 개선 액션 플랜도 제안해줘.",
        "deliverable_type": "report",
        "icon": "🔧",
        "requires_file": True,
    },
    {
        "id": "exec_monthly",
        "category": "경영보고",
        "title": "월간 경영 보고서",
        "description": "종합적인 월간 경영 보고서를 생성합니다",
        "prompt": "이번 달 경영 성과를 종합 분석하여 임원 보고서를 작성해줘. 매출/이익 KPI, 주요 이슈, 리스크, 다음 달 전략 제안을 포함해줘.",
        "deliverable_type": "report",
        "icon": "📋",
        "requires_file": False,
    },
    {
        "id": "data_analysis",
        "category": "데이터분석",
        "title": "데이터 분석 (파일 업로드)",
        "description": "업로드한 파일의 데이터를 심층 분석합니다",
        "prompt": "첨부한 데이터를 심층 분석하여 주요 인사이트를 도출하고, 추이/패턴/이상치를 식별해줘. 시각화 가능한 차트 데이터도 포함해줘.",
        "deliverable_type": "report",
        "icon": "📎",
        "requires_file": True,
    },
    {
        "id": "b2c_performance",
        "category": "B2C채널",
        "title": "온라인 채널 성과 분석",
        "description": "B2C 온라인 채널별 성과를 비교 분석합니다",
        "prompt": "온라인 판매 채널(쿠팡/네이버/자사몰 등)별 매출, 수수료, 순이익을 비교 분석하고, 채널별 최적화 전략을 제안해줘.",
        "deliverable_type": "sheet",
        "icon": "🛒",
        "requires_file": True,
    },
]


def get_templates(category: str = None) -> list:
    """템플릿 목록 조회"""
    if category:
        return [t for t in TEMPLATES if t["category"] == category]
    return TEMPLATES


def get_template_by_id(template_id: str) -> dict:
    """ID로 템플릿 조회"""
    for t in TEMPLATES:
        if t["id"] == template_id:
            return t
    return None


def get_categories() -> list:
    """카테고리 목록"""
    cats = list(dict.fromkeys(t["category"] for t in TEMPLATES))
    return cats
