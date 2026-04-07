"""
MAP Price Collector Service
네이버 쇼핑 API + 오픈마켓 크롤링으로 가격 수집 및 위반 판정
"""
import httpx
import json
import os
import logging
import asyncio
from datetime import datetime
from typing import List, Optional
from bs4 import BeautifulSoup

from db.database import get_connection

logger = logging.getLogger("map_collector")

NAVER_SEARCH_URL = "https://openapi.naver.com/v1/search/shop.json"


# ═══════════════════════════════════════════════════════
# 네이버 쇼핑 API
# ═══════════════════════════════════════════════════════

async def collect_naver(product: dict) -> list:
    cid = os.getenv("NAVER_CLIENT_ID", "")
    csec = os.getenv("NAVER_CLIENT_SECRET", "")
    if not cid:
        logger.warning("네이버 API 키 미설정")
        return []

    query = product.get("search_keywords") or f"{product['brand']} {product['model_name']}"
    results = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(NAVER_SEARCH_URL, headers={
                "X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec,
            }, params={"query": query, "display": 20, "sort": "asc"})
            resp.raise_for_status()
            for item in resp.json().get("items", []):
                title = item.get("title", "").replace("<b>", "").replace("</b>", "")
                if product["model_name"].upper() not in title.upper():
                    continue
                lprice = int(item.get("lprice", 0))
                if lprice <= 0:
                    continue
                results.append({
                    "product_id": product["id"], "platform": "네이버",
                    "seller_name": item.get("mallName", "알 수 없음"),
                    "product_url": item.get("link", ""),
                    "display_price": lprice, "sale_price": lprice,
                    "coupon_name": "", "coupon_discount": 0, "coupon_price": None,
                    "point_reward": 0, "effective_price": lprice, "free_shipping": 0,
                })
        await asyncio.sleep(0.2)
    except Exception as e:
        logger.error(f"네이버 수집 오류 [{product['model_name']}]: {e}")
    return results


# ═══════════════════════════════════════════════════════
# 쿠팡 크롤링
# ═══════════════════════════════════════════════════════

async def collect_coupang(product: dict) -> list:
    query = product.get("search_keywords") or f"{product['brand']} {product['model_name']}"
    url = f"https://www.coupang.com/np/search?q={query}&channel=user"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    results = []
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200: return results
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select("li.search-product")[:10]:
                try:
                    name_el = item.select_one(".name")
                    price_el = item.select_one(".price-value")
                    if not name_el or not price_el: continue
                    name = name_el.get_text(strip=True)
                    if product["model_name"].upper() not in name.upper(): continue
                    price = int(price_el.get_text(strip=True).replace(",", ""))
                    link = item.select_one("a.search-product-link")
                    purl = f"https://www.coupang.com{link['href']}" if link else ""
                    rocket = item.select_one(".badge.rocket")
                    results.append({
                        "product_id": product["id"], "platform": "쿠팡",
                        "seller_name": "쿠팡 로켓배송" if rocket else "쿠팡 마켓플레이스",
                        "product_url": purl, "display_price": price, "sale_price": price,
                        "coupon_name": "", "coupon_discount": 0, "coupon_price": None,
                        "point_reward": 0, "effective_price": price,
                        "free_shipping": 1 if rocket else 0,
                    })
                except: continue
        await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"쿠팡 오류 [{product['model_name']}]: {e}")
    return results


# ═══════════════════════════════════════════════════════
# G마켓 / 옥션
# ═══════════════════════════════════════════════════════

async def collect_gmarket(product: dict, platform: str = "G마켓") -> list:
    query = product.get("search_keywords") or f"{product['brand']} {product['model_name']}"
    base = "browse.gmarket.co.kr" if platform == "G마켓" else "browse.auction.co.kr"
    url = f"https://{base}/search?keyword={query}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    results = []
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200: return results
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select(".box__item-container")[:10]:
                try:
                    name_el = item.select_one(".text__item")
                    price_el = item.select_one(".text__value")
                    if not name_el or not price_el: continue
                    name = name_el.get_text(strip=True)
                    if product["model_name"].upper() not in name.upper(): continue
                    price = int(price_el.get_text(strip=True).replace(",", "").replace("원", ""))
                    link = item.select_one("a")
                    seller_el = item.select_one(".text__seller")
                    results.append({
                        "product_id": product["id"], "platform": platform,
                        "seller_name": seller_el.get_text(strip=True) if seller_el else "알 수 없음",
                        "product_url": link.get("href", "") if link else "",
                        "display_price": price, "sale_price": price,
                        "coupon_name": "", "coupon_discount": 0, "coupon_price": None,
                        "point_reward": 0, "effective_price": price, "free_shipping": 0,
                    })
                except: continue
        await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"{platform} 오류 [{product['model_name']}]: {e}")
    return results


# ═══════════════════════════════════════════════════════
# 11번가
# ═══════════════════════════════════════════════════════

async def collect_11st(product: dict) -> list:
    query = product.get("search_keywords") or f"{product['brand']} {product['model_name']}"
    url = f"https://search.11st.co.kr/Search.tmall?kwd={query}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
    results = []
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200: return results
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select(".list_item")[:10]:
                try:
                    name_el = item.select_one(".info_tit a")
                    price_el = item.select_one(".price_detail .value")
                    if not name_el or not price_el: continue
                    name = name_el.get_text(strip=True)
                    if product["model_name"].upper() not in name.upper(): continue
                    price = int(price_el.get_text(strip=True).replace(",", ""))
                    seller_el = item.select_one(".store a")
                    results.append({
                        "product_id": product["id"], "platform": "11번가",
                        "seller_name": seller_el.get_text(strip=True) if seller_el else "알 수 없음",
                        "product_url": name_el.get("href", ""),
                        "display_price": price, "sale_price": price,
                        "coupon_name": "", "coupon_discount": 0, "coupon_price": None,
                        "point_reward": 0, "effective_price": price, "free_shipping": 0,
                    })
                except: continue
        await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"11번가 오류 [{product['model_name']}]: {e}")
    return results


# ═══════════════════════════════════════════════════════
# 위반 판정
# ═══════════════════════════════════════════════════════

def check_violation(product: dict, price: dict, global_tol: float = 5.0) -> Optional[dict]:
    map_p = product["map_price"]
    if map_p <= 0: return None
    tol = product.get("tolerance_pct") or global_tol
    min_allowed = map_p * (1 - tol / 100)
    eff = price["effective_price"]
    if eff >= min_allowed: return None

    dev = round((map_p - eff) / map_p * 100, 1)
    if price.get("coupon_price") and price["coupon_price"] < min_allowed:
        vtype = "쿠폰 할인"
    elif price.get("point_reward", 0) > 0:
        vtype = "적립금 과다"
    else:
        vtype = "직접 인하"
    sev = "CRITICAL" if dev >= 15 else "HIGH" if dev >= 10 else "MEDIUM" if dev >= 5 else "LOW"
    return {"product_id": product["id"], "platform": price["platform"],
            "seller_name": price["seller_name"], "violation_type": vtype,
            "severity": sev, "map_price": map_p, "violated_price": eff,
            "deviation_pct": dev, "evidence_url": price.get("product_url", "")}


# ═══════════════════════════════════════════════════════
# 메인 수집 실행
# ═══════════════════════════════════════════════════════

COLLECTORS = {
    "네이버 쇼핑": collect_naver,
    "쿠팡": collect_coupang,
    "G마켓": lambda p: collect_gmarket(p, "G마켓"),
    "옥션": lambda p: collect_gmarket(p, "옥션"),
    "11번가": collect_11st,
}


async def run_price_collection(
    platforms: list = None,
    product_ids: list = None,
    collection_type: str = "manual"
) -> dict:
    conn = get_connection()

    # 수집 로그 시작
    cur = conn.execute(
        "INSERT INTO map_collection_logs (collection_type, status) VALUES (?, 'running')",
        (collection_type,))
    log_id = cur.lastrowid
    conn.commit()

    try:
        # 설정 로드
        s = conn.execute("SELECT * FROM map_settings WHERE id=1").fetchone()
        s = dict(s) if s else {}
        min_price = s.get("min_price", 5000)
        global_tol = s.get("tolerance_pct", 5.0)

        if not platforms:
            plat_str = s.get("platforms", '["네이버 쇼핑"]')
            platforms = json.loads(plat_str) if isinstance(plat_str, str) else plat_str

        # 제품 로드
        if product_ids:
            placeholders = ",".join(["?"] * len(product_ids))
            rows = conn.execute(
                f"SELECT * FROM map_products WHERE id IN ({placeholders}) AND is_active=1",
                product_ids).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM map_products WHERE is_active=1 AND map_price>=?",
                (min_price,)).fetchall()

        products = [dict(r) for r in rows]
        total_prices = total_violations = 0
        errors = []

        logger.info(f"MAP 수집 시작: {len(products)}개 제품 × {len(platforms)}개 플랫폼")

        for product in products:
            for pname in platforms:
                collector = COLLECTORS.get(pname)
                if not collector: continue
                try:
                    price_results = await collector(product)
                    for pd in price_results:
                        # 셀러 upsert
                        existing = conn.execute(
                            "SELECT id FROM map_sellers WHERE seller_name=? AND platform=?",
                            (pd["seller_name"], pd["platform"])).fetchone()
                        if existing:
                            seller_id = existing["id"]
                        else:
                            c = conn.execute(
                                "INSERT INTO map_sellers (seller_name, platform) VALUES (?,?)",
                                (pd["seller_name"], pd["platform"]))
                            seller_id = c.lastrowid

                        # 가격 기록 저장
                        c = conn.execute("""INSERT INTO map_price_records
                            (product_id, seller_id, platform, seller_name, product_url,
                             display_price, sale_price, coupon_name, coupon_discount,
                             coupon_price, point_reward, effective_price, free_shipping)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (pd["product_id"], seller_id, pd["platform"], pd["seller_name"],
                             pd.get("product_url",""), pd["display_price"], pd.get("sale_price"),
                             pd.get("coupon_name",""), pd.get("coupon_discount",0),
                             pd.get("coupon_price"), pd.get("point_reward",0),
                             pd["effective_price"], pd.get("free_shipping",0)))
                        record_id = c.lastrowid
                        total_prices += 1

                        # 위반 체크
                        vio = check_violation(product, pd, global_tol)
                        if vio:
                            conn.execute("""INSERT INTO map_violations
                                (price_record_id, product_id, seller_id, platform, seller_name,
                                 violation_type, severity, map_price, violated_price, deviation_pct, evidence_url)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                                (record_id, vio["product_id"], seller_id,
                                 vio["platform"], vio["seller_name"], vio["violation_type"],
                                 vio["severity"], vio["map_price"], vio["violated_price"],
                                 vio["deviation_pct"], vio.get("evidence_url","")))
                            total_violations += 1

                            # 셀러 위반 카운트
                            conn.execute("""UPDATE map_sellers SET total_violations=total_violations+1,
                                last_violation_at=datetime('now','localtime'),
                                risk_level=CASE WHEN total_violations+1>=10 THEN 'high'
                                    WHEN total_violations+1>=5 THEN 'medium' ELSE 'low' END
                                WHERE id=?""", (seller_id,))

                            conn.execute("UPDATE map_price_records SET is_violation=1 WHERE id=?", (record_id,))

                            logger.warning(f"🚨 위반: {product['model_name']} | {vio['platform']} | "
                                f"{vio['seller_name']} | {vio['violated_price']:,}원 (지도가 {vio['map_price']:,}원 -{vio['deviation_pct']}%)")

                except Exception as e:
                    err = f"{pname}/{product['model_name']}: {e}"
                    errors.append(err)
                    logger.error(f"수집 오류: {err}")

        # 로그 완료
        conn.execute("""UPDATE map_collection_logs SET
            platforms_searched=?, products_checked=?, prices_collected=?,
            violations_found=?, errors=?, finished_at=datetime('now','localtime'), status='completed'
            WHERE id=?""",
            (json.dumps(platforms, ensure_ascii=False), len(products),
             total_prices, total_violations, json.dumps(errors[:50], ensure_ascii=False), log_id))
        conn.commit()
        conn.close()

        msg = f"수집 완료: {len(products)}개 제품, {total_prices}건 가격, {total_violations}건 위반"
        logger.info(msg)
        return {"message": msg, "products_checked": len(products),
                "prices_collected": total_prices, "violations_found": total_violations, "errors": errors[:10]}

    except Exception as e:
        conn.execute("UPDATE map_collection_logs SET status='error', errors=?, finished_at=datetime('now','localtime') WHERE id=?",
                     (str(e), log_id))
        conn.commit(); conn.close()
        raise
