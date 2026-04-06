"""리베이트 설정 관리"""
import os
import json
from pathlib import Path

# 데이터 디렉토리 (order-agent의 data/ 폴더 활용)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
REBATE_SETTINGS_FILE = DATA_DIR / "rebate_settings.json"

# ERP 설정 (order-agent의 기존 환경변수 공유)
ERP_COM_CODE = os.getenv("ERP_COM_CODE", "89356")
ERP_USER_ID = os.getenv("ERP_USER_ID", "TIGER")
ERP_API_KEY = os.getenv("ERP_API_KEY", "")
ERP_ZONE = os.getenv("ERP_ZONE", "CD")

DEFAULT_REBATE_SETTINGS = {
    "tier_thresholds": {
        "tier_10_min": 10000000,
        "tier_5_min": 5000000
    },
    "discount_rates": {
        "main": {"5%": 0.05, "10%": 0.10},
        "lanstar_3": {"5%": 0.03, "10%": 0.03},
        "lanstar_5": {"5%": 0.05, "10%": 0.05},
        "printer": {"5%": 0.07, "10%": 0.07}
    },
    "discount_groups": {
        "main": [
            "수입제품 5% / 10%할인품목",
            "수입제품(심천시장) 5% / 10%할인품목",
            "수입제품(plus제품)",
            "수입(단종)",
            "★★재고 소진후 단종★★"
        ],
        "lanstar_3": ["수입제품(랜스타)★ 3%할인품목"],
        "lanstar_5": ["수입제품(랜스타)★ 5%할인품목"],
        "printer": ["프린터서버류(매출별할인) 5% / 7% 할인품목"]
    },
    "excluded_product_groups": [
        "기타01-판매장려금할인",
        "기타02-매출배송료(택배비)"
    ],
    "use_allowed_list": True,
    "allowed_customers": [
        "주식회사 리더샵",
        "주식회사 인네트워크",
        "주식회사 네트워크 다모일",
        "누리시스템",
        "대원티엠티(주)",
        "(주)랜마스터",
        "레알몰",
        "에이플러그",
        "엠아이시스템",
        "주식회사 엠알오솔루션",
        "연승",
        "영원에스티",
        "태영전기",
        "파워네트정보통신(주)",
        "(주)현대모아컴",
        "제이피엘프라자",
        "에이치엘프라자",
        "강원정보통신",
        "칼스정보",
        "(주)케이블가이",
        "(주)유니정보통신",
        "(주) 영원이앤디",
        "주식회사 가가전자",
        "주식회사 루피하루",
        "주식회사 한국코스모",
        "주식회사 유진정보통신"
    ],
    "allowed_customer_aliases": {
        "리더샵": "주식회사 리더샵",
        "인네트워크": "주식회사 인네트워크",
        "유진정보통신": "주식회사 유진정보통신"
    },
    "excluded_customers": [
        "(주)포워드벤처스(쿠팡로켓)",
        "(주)포워드벤처스(쿠팡일반)",
        "샵N(엔에이치엔 비즈니스플랫폼)",
        "랜마트(온라인)",
        "랜마트(네이버페이)",
        "오픈마켓(지마켓라인)",
        "지마켓_스타배송",
        "십일번가 주식회사"
    ],
    "exception_customers": [
        {"name": "주식회사 리더샵", "code": "1068703761", "min_tier": "5%"},
        {"name": "주식회사 네트워크 다모일", "code": "1068649251", "min_tier": "5%"},
        {"name": "주식회사 인네트워크", "code": "8228800257", "min_tier": "10%"}
    ],
    "rate_upgrade_customers": [
        {
            "name": "레알몰",
            "description": "한 단계 상향 (5%→10%, 3%→5%)",
            "upgrades": {"0.03": 0.05, "0.05": 0.10}
        }
    ],
    "erp_defaults": {
        "wh_cd": "10",
        "io_type": "1Z",
        "prod_cd": "판매장려금할인006",
        "prod_des": "리베이트",
        "remarks_format": "{month}월분 / 수고하셨습니다"
    },
    "customer_employees": {}
}


def load_rebate_settings() -> dict:
    if REBATE_SETTINGS_FILE.exists():
        with open(REBATE_SETTINGS_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        merged = {**DEFAULT_REBATE_SETTINGS, **saved}
        return merged
    else:
        save_rebate_settings(DEFAULT_REBATE_SETTINGS)
        return DEFAULT_REBATE_SETTINGS.copy()


def save_rebate_settings(settings: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(REBATE_SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)
