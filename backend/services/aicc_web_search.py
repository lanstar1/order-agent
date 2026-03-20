"""
AICC 웹 검색 서비스 — 네이버 블로그 검색 API
DB/QnA/리뷰에 없는 정보를 보충하기 위한 3차 소스
"""
import os
import re
import urllib.parse
import httpx

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "yJoeRzSxXZJiN8amAOrY")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "ccpO9tf7b7")

BLOG_API_URL = "https://openapi.naver.com/v1/search/blog.json"


def _clean_html(text: str) -> str:
    """HTML 태그 제거"""
    return re.sub(r'<[^>]+>', '', text).strip()


async def search_naver_blog(query: str, display: int = 5) -> list[dict]:
    """
    네이버 블로그 검색.
    Returns: [{"title", "description", "link", "bloggername", "postdate"}, ...]
    """
    if not query.strip():
        return []

    params = {
        "query": query,
        "display": display,
        "sort": "sim",
    }

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(BLOG_API_URL, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        results = []
        for item in data.get("items", []):
            results.append({
                "title": _clean_html(item.get("title", "")),
                "description": _clean_html(item.get("description", "")),
                "link": item.get("link", ""),
                "bloggername": item.get("bloggername", ""),
                "postdate": item.get("postdate", ""),
            })
        return results

    except Exception as e:
        print(f"[AICC] 네이버 블로그 검색 오류: {e}")
        return []


async def search_product_blog(model: str, user_question: str = "", max_results: int = 3) -> list[dict]:
    """
    제품 모델명 + 질문 키워드로 블로그 검색.
    랜스타 관련 글 우선, 경쟁사 필터링.
    """
    # 검색어 구성: "랜스타 모델명" 또는 "모델명 키워드"
    search_query = f"랜스타 {model}"
    if user_question:
        # 질문에서 핵심 키워드 추출 (짧게)
        keywords = user_question[:30]
        search_query = f"{model} {keywords}"

    results = await search_naver_blog(search_query, display=8)

    # 경쟁사/무관한 결과 필터링
    exclude_keywords = ["유니콘", "벨킨", "앤커", "애플", "삼성전자", "엘지전자"]
    filtered = []
    for r in results:
        text = (r["title"] + " " + r["description"]).lower()
        if any(kw in text for kw in exclude_keywords):
            continue
        filtered.append(r)

    return filtered[:max_results]
