"""
iLOGEN API 연동 테스트 스크립트
실행: python test_ilogen_api.py
"""
import requests
import json
from datetime import datetime

# ── 설정 ──
API_TOKEN = "UWVuoDFvxpBEhQSRViOnNJ_mhcxRGCZvGG_ToeKvVAc"
USER_ID = "34850417"
CUST_CD = "34850417"

# 개발계 URL (테스트용)
BASE_URL = "https://topenapi.ilogen.com/lrm02b-edi/edi"
# 운영계 URL (실제 운영시)
# BASE_URL = "https://openapi.ilogen.com/lrm02b-edi/edi"

TODAY = datetime.today().strftime("%Y%m%d")


def test_register_order():
    """1단계: 주문 정보 일괄 등록 테스트"""
    url = f"{BASE_URL}/registerOrderData"

    # 인증 방식 시도 (Bearer / apiKey 헤더 / 파라미터)
    headers_options = [
        {"Content-Type": "application/json", "Authorization": f"Bearer {API_TOKEN}"},
        {"Content-Type": "application/json", "apiKey": API_TOKEN},
        {"Content-Type": "application/json", "x-api-key": API_TOKEN},
        {"Content-Type": "application/json"},  # 토큰 없이 (userId로만 인증)
    ]

    payload = {
        "userId": USER_ID,
        "data": [{
            "custCd": CUST_CD,
            "takeDt": TODAY,
            "fixTakeNo": f"TEST-{TODAY}-001",
            "sndCustNm": "라인업시스템",
            "sndCustAddr": "서울시 용산구",
            "sndTelNo": "0212345678",
            "rcvCustNm": "테스트",
            "rcvCustAddr": "서울시 강남구 테스트주소 123",
            "rcvTelNo": "01000000000",
            "fareTy": "030",   # 030 = 선불
            "qty": 1,
            "dlvFare": 4000,
            "goodsNm": "테스트상품",
            "sndMsg": "API 연동 테스트"
        }]
    }

    # 토큰을 body에 포함하는 버전도 시도
    payload_with_token = {**payload, "apiKey": API_TOKEN}

    print("=" * 60)
    print("iLOGEN 주문 정보 일괄 등록 테스트")
    print(f"URL: {url}")
    print(f"날짜: {TODAY}")
    print("=" * 60)

    for i, headers in enumerate(headers_options, 1):
        p = payload_with_token if i == 4 else payload
        auth_desc = {
            1: "Bearer 토큰",
            2: "apiKey 헤더",
            3: "x-api-key 헤더",
            4: "body에 apiKey 포함 (헤더 없음)",
        }
        print(f"\n--- 시도 {i}: {auth_desc[i]} ---")
        try:
            resp = requests.post(url, json=p, headers=headers, timeout=15)
            print(f"HTTP 상태: {resp.status_code}")
            print(f"응답: {resp.text[:500]}")
            if resp.status_code == 200:
                data = resp.json()
                print(f"\n✅ 성공! sttsCd={data.get('sttsCd')}, sttsMsg={data.get('sttsMsg')}")
                return data
        except Exception as e:
            print(f"❌ 에러: {e}")

    return None


def test_inquiry_slip(fix_take_no: str = None):
    """2단계: 출력 송장번호 조회 테스트"""
    url = f"{BASE_URL}/inquirySlipNoMulti"
    fix_take_no = fix_take_no or f"TEST-{TODAY}-001"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }

    payload = {
        "userId": USER_ID,
        "data": [{
            "custCd": CUST_CD,
            "fixTakeNo": fix_take_no,
        }]
    }

    print("\n" + "=" * 60)
    print("iLOGEN 출력 송장번호 조회 테스트")
    print(f"URL: {url}")
    print(f"주문번호: {fix_take_no}")
    print("=" * 60)

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        print(f"HTTP 상태: {resp.status_code}")
        print(f"응답: {resp.text[:500]}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"\n✅ sttsCd={data.get('sttsCd')}, sttsMsg={data.get('sttsMsg')}")
            return data
    except Exception as e:
        print(f"❌ 에러: {e}")

    return None


if __name__ == "__main__":
    result = test_register_order()
    print("\n")
    test_inquiry_slip()
