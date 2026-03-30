"""
스마트스토어 API 로컬 테스트 서버
- 네이버 커머스 API 직접 호출 테스트
- 응답 원본(raw) 확인
- 주문수집 → 상세조회 단계별 디버깅
- 별도 포트(8888)에서 테스트 전용 페이지 서빙

사용법:
  1. .env 파일에 NAVER_COMMERCE_CLIENT_ID, NAVER_COMMERCE_CLIENT_SECRET 설정
  2. python test_smartstore.py
  3. 브라우저에서 http://localhost:8888 접속
"""
import os
import sys
import json
import time
import hashlib
import base64
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

# .env 로딩
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import bcrypt
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
NAVER_API_BASE = "https://api.commerce.naver.com/external"

app = FastAPI(title="SmartStore API Test Server")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── 네이버 토큰 캐시 ───
_token_cache = {"token": None, "expires": 0}


def _make_signature(client_id: str, client_secret: str, timestamp: int) -> str:
    password = f"{client_id}_{timestamp}"
    hashed = bcrypt.hashpw(password.encode("utf-8"), client_secret.encode("utf-8"))
    return base64.b64encode(hashed).decode("utf-8")


async def get_token() -> dict:
    """토큰 발급 (캐시)"""
    client_id = os.getenv("NAVER_COMMERCE_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        return {"error": "NAVER_COMMERCE_CLIENT_ID 또는 NAVER_COMMERCE_CLIENT_SECRET 미설정"}

    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires"] - 60:
        return {"token": _token_cache["token"], "cached": True}

    timestamp = int(now * 1000)
    sign = _make_signature(client_id, client_secret, timestamp)

    async with httpx.AsyncClient() as http:
        resp = await http.post(
            f"{NAVER_API_BASE}/v1/oauth2/token",
            data={
                "client_id": client_id,
                "timestamp": timestamp,
                "grant_type": "client_credentials",
                "client_secret_sign": sign,
                "type": "SELF",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        body = resp.json()
        if resp.status_code != 200:
            return {"error": f"토큰 발급 실패: {resp.status_code}", "response": body}

        _token_cache["token"] = body["access_token"]
        _token_cache["expires"] = now + body.get("expires_in", 10800)
        return {"token": body["access_token"], "expires_in": body.get("expires_in"), "cached": False}


# ─── API 엔드포인트 ───

@app.get("/api/test/token")
async def test_token():
    """토큰 발급 테스트"""
    result = await get_token()
    return result


@app.get("/api/test/orders-raw")
async def test_orders_raw(
    from_date: str = "",
    to_date: str = "",
    status: str = "PAYED",
    range_type: str = "PAYED_DATETIME",
):
    """주문 조회 - 원본(raw) 응답 반환"""
    token_result = await get_token()
    if "error" in token_result:
        return token_result

    now = datetime.now(KST)
    if not from_date:
        week_ago = now - timedelta(days=7)
        from_date = week_ago.strftime("%Y-%m-%d")
    if not to_date:
        to_date = now.strftime("%Y-%m-%d")

    from_dt = f"{from_date}T00:00:00.000+09:00"
    to_dt = f"{to_date}T23:59:59.999+09:00"

    headers = {
        "Authorization": f"Bearer {token_result['token']}",
        "Content-Type": "application/json",
    }
    params = {
        "from": from_dt,
        "to": to_dt,
        "rangeType": range_type,
        "productOrderStatuses": status,
        "pageSize": 100,
        "page": 1,
    }

    logger.info(f"[TEST] 주문 조회 요청: params={json.dumps(params, ensure_ascii=False)}")

    async with httpx.AsyncClient() as http:
        resp = await http.get(
            f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders",
            headers=headers,
            params=params,
            timeout=30,
        )
        raw = resp.json()
        logger.info(f"[TEST] 주문 조회 응답: status={resp.status_code}, body_keys={list(raw.keys()) if isinstance(raw, dict) else type(raw)}")

        return {
            "request": {
                "url": str(resp.url),
                "params": params,
            },
            "response": {
                "status_code": resp.status_code,
                "body": raw,
            },
            "analysis": _analyze_response(raw),
        }


@app.get("/api/test/orders-all-statuses")
async def test_all_statuses(from_date: str = "", to_date: str = ""):
    """모든 상태로 주문 조회 (어떤 상태에 주문이 있는지 확인)"""
    token_result = await get_token()
    if "error" in token_result:
        return token_result

    now = datetime.now(KST)
    if not from_date:
        week_ago = now - timedelta(days=7)
        from_date = week_ago.strftime("%Y-%m-%d")
    if not to_date:
        to_date = now.strftime("%Y-%m-%d")

    statuses = [
        "PAYMENT_WAITING", "PAYED", "DELIVERING", "DELIVERED",
        "PURCHASE_DECIDED", "EXCHANGED", "CANCELED", "RETURNED",
        "CANCEL_REQUEST",
    ]
    range_types = ["PAYED_DATETIME", "ORDER_DATETIME"]

    results = {}
    headers = {
        "Authorization": f"Bearer {token_result['token']}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as http:
        for rt in range_types:
            for st in statuses:
                params = {
                    "from": f"{from_date}T00:00:00.000+09:00",
                    "to": f"{to_date}T23:59:59.999+09:00",
                    "rangeType": rt,
                    "productOrderStatuses": st,
                    "pageSize": 10,
                    "page": 1,
                }
                try:
                    resp = await http.get(
                        f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders",
                        headers=headers,
                        params=params,
                        timeout=15,
                    )
                    body = resp.json()
                    data = body.get("data", {})
                    count = data.get("count", 0) if isinstance(data, dict) else len(data) if isinstance(data, list) else 0
                    ids = data.get("productOrderIds", []) if isinstance(data, dict) else data if isinstance(data, list) else []

                    key = f"{rt}/{st}"
                    results[key] = {
                        "status_code": resp.status_code,
                        "count": count,
                        "sample_ids": ids[:3],
                        "raw_data_type": type(data).__name__,
                    }
                    if count > 0:
                        logger.info(f"[TEST] ✅ {key}: {count}건 발견!")
                except Exception as e:
                    results[f"{rt}/{st}"] = {"error": str(e)}

    # 결과가 있는 것만 하이라이트
    found = {k: v for k, v in results.items() if v.get("count", 0) > 0}

    return {
        "from_date": from_date,
        "to_date": to_date,
        "found_orders": found,
        "all_results": results,
        "summary": f"총 {sum(v.get('count', 0) for v in found.values())}건 발견 ({len(found)}개 상태/범위 조합)",
    }


@app.get("/api/test/order-detail")
async def test_order_detail(product_order_ids: str = ""):
    """상품주문 상세 조회 (쉼표 구분 ID)"""
    if not product_order_ids:
        return {"error": "product_order_ids 파라미터 필요 (쉼표 구분)"}

    token_result = await get_token()
    if "error" in token_result:
        return token_result

    ids = [x.strip() for x in product_order_ids.split(",") if x.strip()]

    headers = {
        "Authorization": f"Bearer {token_result['token']}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as http:
        resp = await http.post(
            f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders/query",
            headers=headers,
            json={"productOrderIds": ids},
            timeout=30,
        )
        raw = resp.json()
        logger.info(f"[TEST] 상세 조회 응답: status={resp.status_code}")

        return {
            "request": {"productOrderIds": ids},
            "response": {
                "status_code": resp.status_code,
                "body": raw,
            },
        }


@app.get("/api/test/changed-orders")
async def test_changed_orders(from_date: str = "", to_date: str = ""):
    """변경 주문 조회 (lastChangedStatuses API)"""
    token_result = await get_token()
    if "error" in token_result:
        return token_result

    now = datetime.now(KST)
    if not from_date:
        week_ago = now - timedelta(days=7)
        from_date = week_ago.strftime("%Y-%m-%d")
    if not to_date:
        to_date = now.strftime("%Y-%m-%d")

    headers = {
        "Authorization": f"Bearer {token_result['token']}",
        "Content-Type": "application/json",
    }
    params = {
        "lastChangedFrom": f"{from_date}T00:00:00.000+09:00",
        "lastChangedTo": f"{to_date}T23:59:59.999+09:00",
        "limitCount": 100,
    }

    async with httpx.AsyncClient() as http:
        resp = await http.get(
            f"{NAVER_API_BASE}/v1/pay-order/seller/product-orders/last-changed-statuses",
            headers=headers,
            params=params,
            timeout=30,
        )
        raw = resp.json()
        return {
            "request": params,
            "response": {
                "status_code": resp.status_code,
                "body": raw,
            },
        }


@app.get("/api/test/env")
async def test_env():
    """환경변수 확인 (키 값은 마스킹)"""
    cid = os.getenv("NAVER_COMMERCE_CLIENT_ID", "")
    csecret = os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "")
    return {
        "NAVER_COMMERCE_CLIENT_ID": cid[:8] + "..." if len(cid) > 8 else ("SET" if cid else "NOT SET"),
        "NAVER_COMMERCE_CLIENT_SECRET": csecret[:4] + "..." if len(csecret) > 4 else ("SET" if csecret else "NOT SET"),
        "cwd": os.getcwd(),
        "env_file": str(Path(__file__).parent / ".env"),
        "env_exists": (Path(__file__).parent / ".env").exists(),
    }


def _analyze_response(raw):
    """응답 구조 분석"""
    if not isinstance(raw, dict):
        return {"type": type(raw).__name__, "note": "응답이 dict가 아님"}

    data = raw.get("data")
    if data is None:
        return {"note": "data 키 없음", "keys": list(raw.keys())}

    if isinstance(data, dict):
        return {
            "data_type": "dict",
            "data_keys": list(data.keys()),
            "count": data.get("count", "없음"),
            "has_productOrderIds": "productOrderIds" in data,
            "productOrderIds_count": len(data.get("productOrderIds", [])),
            "sample_ids": data.get("productOrderIds", [])[:3],
        }
    elif isinstance(data, list):
        return {
            "data_type": "list",
            "length": len(data),
            "sample": data[:2] if data else [],
            "item_type": type(data[0]).__name__ if data else "empty",
        }
    else:
        return {"data_type": type(data).__name__, "value": str(data)[:200]}


# ─── 테스트 페이지 ───

@app.get("/", response_class=HTMLResponse)
async def test_page():
    return TEST_HTML


TEST_HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SmartStore API Test</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:#f0f2f5; color:#1a1a1a; }
.header { background:linear-gradient(135deg, #03c75a, #00a843); color:#fff; padding:20px 24px; }
.header h1 { font-size:22px; font-weight:700; }
.header p { font-size:13px; opacity:0.85; margin-top:4px; }
.container { max-width:1100px; margin:0 auto; padding:20px; }
.card { background:#fff; border-radius:12px; box-shadow:0 1px 3px rgba(0,0,0,0.08); padding:20px; margin-bottom:16px; }
.card h2 { font-size:16px; font-weight:700; color:#1a1a1a; margin-bottom:12px; padding-bottom:8px; border-bottom:1px solid #e5e7eb; }
.card h2 span { font-size:12px; color:#6b7280; font-weight:400; margin-left:8px; }
.form-row { display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin-bottom:12px; }
.form-group { display:flex; flex-direction:column; gap:4px; }
.form-group label { font-size:12px; font-weight:600; color:#4b5563; }
.form-group input, .form-group select { padding:8px 12px; border:1px solid #d1d5db; border-radius:6px; font-size:14px; }
.btn { padding:8px 20px; border:none; border-radius:6px; font-size:14px; font-weight:600; cursor:pointer; transition:all 0.15s; }
.btn-green { background:#03c75a; color:#fff; }
.btn-green:hover { background:#00a843; }
.btn-blue { background:#2563eb; color:#fff; }
.btn-blue:hover { background:#1d4ed8; }
.btn-orange { background:#ea580c; color:#fff; }
.btn-orange:hover { background:#c2410c; }
.btn-purple { background:#7c3aed; color:#fff; }
.btn-purple:hover { background:#6d28d9; }
.btn-gray { background:#6b7280; color:#fff; }
.btn-gray:hover { background:#4b5563; }
.result-box { margin-top:12px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; overflow:hidden; }
.result-header { display:flex; justify-content:space-between; align-items:center; padding:8px 12px; background:#f1f5f9; border-bottom:1px solid #e2e8f0; }
.result-header span { font-size:12px; font-weight:600; color:#475569; }
.result-status { font-size:12px; padding:2px 8px; border-radius:4px; font-weight:600; }
.result-status.ok { background:#dcfce7; color:#166534; }
.result-status.err { background:#fef2f2; color:#991b1b; }
.result-status.loading { background:#fef3c7; color:#92400e; }
pre { padding:12px; font-size:12px; line-height:1.5; overflow-x:auto; white-space:pre-wrap; word-break:break-all; max-height:500px; overflow-y:auto; }
.highlight { background:#fefce8; padding:2px 4px; border-radius:3px; }
.badge { display:inline-block; font-size:11px; padding:2px 8px; border-radius:10px; font-weight:600; }
.badge-found { background:#dcfce7; color:#166534; }
.badge-empty { background:#f3f4f6; color:#6b7280; }
.summary-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:8px; margin-top:12px; }
.summary-item { padding:10px 12px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; font-size:13px; }
.summary-item.found { border-left:3px solid #03c75a; }
.summary-item .count { font-size:20px; font-weight:700; color:#03c75a; }
.summary-item .label { font-size:11px; color:#6b7280; }
.info-bar { padding:12px 16px; background:#eff6ff; border:1px solid #bfdbfe; border-radius:8px; margin-bottom:16px; font-size:13px; color:#1e40af; }
</style>
</head>
<body>

<div class="header">
  <h1>🧪 SmartStore API Test Server</h1>
  <p>네이버 커머스 API 로컬 디버깅 도구 &nbsp;|&nbsp; http://localhost:8888</p>
</div>

<div class="container">

  <div class="info-bar" id="env-info">환경변수 확인 중...</div>

  <!-- 1. 토큰 테스트 -->
  <div class="card">
    <h2>1. 토큰 발급 테스트 <span>OAuth2 BCrypt 전자서명</span></h2>
    <button class="btn btn-green" onclick="testToken()">🔑 토큰 발급</button>
    <div class="result-box" id="result-token" style="display:none"></div>
  </div>

  <!-- 2. 전체 상태 스캔 -->
  <div class="card">
    <h2>2. 전체 상태 스캔 <span>어떤 상태/범위에 주문이 있는지 확인</span></h2>
    <div class="form-row">
      <div class="form-group">
        <label>시작일</label>
        <input type="date" id="scan-from">
      </div>
      <div class="form-group">
        <label>종료일</label>
        <input type="date" id="scan-to">
      </div>
      <button class="btn btn-orange" onclick="scanAllStatuses()">🔍 전체 상태 스캔</button>
    </div>
    <div id="scan-summary"></div>
    <div class="result-box" id="result-scan" style="display:none"></div>
  </div>

  <!-- 3. 단일 상태 조회 -->
  <div class="card">
    <h2>3. 주문 조회 (단일 상태) <span>raw 응답 확인</span></h2>
    <div class="form-row">
      <div class="form-group">
        <label>시작일</label>
        <input type="date" id="raw-from">
      </div>
      <div class="form-group">
        <label>종료일</label>
        <input type="date" id="raw-to">
      </div>
      <div class="form-group">
        <label>주문 상태</label>
        <select id="raw-status">
          <option value="PAYED">PAYED (발송대기)</option>
          <option value="DELIVERING">DELIVERING (배송중)</option>
          <option value="DELIVERED">DELIVERED (배송완료)</option>
          <option value="PURCHASE_DECIDED">PURCHASE_DECIDED (구매확정)</option>
          <option value="PAYMENT_WAITING">PAYMENT_WAITING (입금대기)</option>
          <option value="CANCELED">CANCELED (취소)</option>
          <option value="CANCEL_REQUEST">CANCEL_REQUEST (취소요청)</option>
        </select>
      </div>
      <div class="form-group">
        <label>범위 타입</label>
        <select id="raw-range">
          <option value="PAYED_DATETIME">PAYED_DATETIME</option>
          <option value="ORDER_DATETIME">ORDER_DATETIME</option>
        </select>
      </div>
      <button class="btn btn-blue" onclick="fetchOrdersRaw()">📋 조회</button>
    </div>
    <div class="result-box" id="result-raw" style="display:none"></div>
  </div>

  <!-- 4. 변경 주문 조회 -->
  <div class="card">
    <h2>4. 변경 주문 조회 <span>lastChangedStatuses API</span></h2>
    <div class="form-row">
      <div class="form-group">
        <label>시작일</label>
        <input type="date" id="changed-from">
      </div>
      <div class="form-group">
        <label>종료일</label>
        <input type="date" id="changed-to">
      </div>
      <button class="btn btn-purple" onclick="fetchChangedOrders()">📋 변경 주문 조회</button>
    </div>
    <div class="result-box" id="result-changed" style="display:none"></div>
  </div>

  <!-- 5. 상세 조회 -->
  <div class="card">
    <h2>5. 상품주문 상세 조회 <span>productOrderIds → 상세 정보</span></h2>
    <div class="form-row">
      <div class="form-group" style="flex:1">
        <label>상품주문번호 (쉼표 구분)</label>
        <input type="text" id="detail-ids" placeholder="예: 2026032712345001, 2026032712345002" style="width:100%">
      </div>
      <button class="btn btn-gray" onclick="fetchOrderDetail()">📦 상세 조회</button>
    </div>
    <div class="result-box" id="result-detail" style="display:none"></div>
  </div>

</div>

<script>
// 날짜 기본값: 7일전~오늘
const today = new Date();
const weekAgo = new Date(today);
weekAgo.setDate(weekAgo.getDate() - 7);
const fmt = d => d.toISOString().split('T')[0];

document.querySelectorAll('input[type=date]').forEach(el => {
  if (el.id.includes('from')) el.value = fmt(weekAgo);
  else el.value = fmt(today);
});

// 환경변수 확인
fetch('/api/test/env').then(r=>r.json()).then(d => {
  const el = document.getElementById('env-info');
  const idOk = !d.NAVER_COMMERCE_CLIENT_ID.includes('NOT SET');
  const secOk = !d.NAVER_COMMERCE_CLIENT_SECRET.includes('NOT SET');
  if (idOk && secOk) {
    el.innerHTML = `✅ API 키 설정됨 | CLIENT_ID: ${d.NAVER_COMMERCE_CLIENT_ID} | SECRET: ${d.NAVER_COMMERCE_CLIENT_SECRET} | .env: ${d.env_exists ? '있음' : '없음'}`;
    el.style.background = '#dcfce7'; el.style.borderColor = '#86efac'; el.style.color = '#166534';
  } else {
    el.innerHTML = `❌ API 키 미설정 | CLIENT_ID: ${d.NAVER_COMMERCE_CLIENT_ID} | SECRET: ${d.NAVER_COMMERCE_CLIENT_SECRET} | .env 경로: ${d.env_file}`;
    el.style.background = '#fef2f2'; el.style.borderColor = '#fecaca'; el.style.color = '#991b1b';
  }
});

function showResult(id, data, ok) {
  const el = document.getElementById(id);
  el.style.display = 'block';
  const statusClass = ok ? 'ok' : 'err';
  const statusText = ok ? 'OK' : 'ERROR';
  el.innerHTML = `
    <div class="result-header">
      <span>응답 결과</span>
      <span class="result-status ${statusClass}">${statusText}</span>
    </div>
    <pre>${JSON.stringify(data, null, 2)}</pre>
  `;
}

function showLoading(id) {
  const el = document.getElementById(id);
  el.style.display = 'block';
  el.innerHTML = `
    <div class="result-header">
      <span>요청 중...</span>
      <span class="result-status loading">⏳ LOADING</span>
    </div>
    <pre>API 호출 중입니다...</pre>
  `;
}

async function testToken() {
  showLoading('result-token');
  const res = await fetch('/api/test/token');
  const data = await res.json();
  showResult('result-token', data, !data.error);
}

async function scanAllStatuses() {
  const from = document.getElementById('scan-from').value;
  const to = document.getElementById('scan-to').value;
  showLoading('result-scan');
  document.getElementById('scan-summary').innerHTML = '<div style="padding:12px;color:#92400e">⏳ 전체 상태 스캔 중... (약 20초 소요)</div>';

  const res = await fetch(`/api/test/orders-all-statuses?from_date=${from}&to_date=${to}`);
  const data = await res.json();

  // 요약 표시
  const found = data.found_orders || {};
  let html = `<div style="padding:8px 0;font-weight:600;font-size:14px">${data.summary || ''}</div>`;
  html += '<div class="summary-grid">';
  for (const [key, val] of Object.entries(data.all_results || {})) {
    const count = val.count || 0;
    const cls = count > 0 ? 'found' : '';
    html += `<div class="summary-item ${cls}">
      <div class="count">${count}</div>
      <div class="label">${key}</div>
      ${val.sample_ids?.length ? '<div style="font-size:10px;color:#9ca3af;margin-top:4px">' + val.sample_ids[0] + '...</div>' : ''}
    </div>`;
  }
  html += '</div>';
  document.getElementById('scan-summary').innerHTML = html;

  showResult('result-scan', data, !data.error);
}

async function fetchOrdersRaw() {
  const from = document.getElementById('raw-from').value;
  const to = document.getElementById('raw-to').value;
  const status = document.getElementById('raw-status').value;
  const range = document.getElementById('raw-range').value;
  showLoading('result-raw');
  const res = await fetch(`/api/test/orders-raw?from_date=${from}&to_date=${to}&status=${status}&range_type=${range}`);
  const data = await res.json();
  showResult('result-raw', data, data.response?.status_code === 200);
}

async function fetchChangedOrders() {
  const from = document.getElementById('changed-from').value;
  const to = document.getElementById('changed-to').value;
  showLoading('result-changed');
  const res = await fetch(`/api/test/changed-orders?from_date=${from}&to_date=${to}`);
  const data = await res.json();
  showResult('result-changed', data, data.response?.status_code === 200);
}

async function fetchOrderDetail() {
  const ids = document.getElementById('detail-ids').value;
  if (!ids) { alert('상품주문번호를 입력하세요'); return; }
  showLoading('result-detail');
  const res = await fetch(`/api/test/order-detail?product_order_ids=${encodeURIComponent(ids)}`);
  const data = await res.json();
  showResult('result-detail', data, data.response?.status_code === 200);
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    print("\n" + "="*60)
    print("  🧪 SmartStore API Test Server")
    print("  http://localhost:8888")
    print("="*60 + "\n")
    uvicorn.run(app, host="127.0.0.1", port=8888, log_level="info")
