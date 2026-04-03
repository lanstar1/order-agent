"""
ECOUNT ERP 웹 스크래퍼 (Playwright 기반)
─────────────────────────────────────────
ECOUNT OAPI에 구매/판매 현황 조회 API가 없으므로,
Playwright headless 브라우저로 웹에 로그인하여 데이터를 가져온다.

흐름:
  1) https://login.ecount.com/ 로그인 (회사코드 + ID + PW)
  2) 로그인 후 zone 기반 URL로 리다이렉트 (예: logincd.ecount.com)
  3) 구매현황/판매현황 페이지를 **브라우저 탭 2개**로 병렬 접근
  4) 검색 조건 설정 (거래처, 기간)
  5) 검색 실행 후 테이블 데이터 파싱
"""
import asyncio
import logging
import re
import time
from datetime import datetime
from typing import Optional
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import ERP_COM_CODE, ERP_WEB_USER_ID, ERP_WEB_USER_PW, ERP_ZONE

logger = logging.getLogger(__name__)

# ECOUNT 메뉴 URL 해시 패턴
PURCHASE_STATUS_HASH = "menuType=MENUTREE_000004&menuSeq=MENUTREE_000513&groupSeq=MENUTREE_000031&prgId=E040305&depth=4"
SALE_STATUS_HASH = "menuType=MENUTREE_000004&menuSeq=MENUTREE_000494&groupSeq=MENUTREE_000030&prgId=E040207&depth=4"


class ERPWebScraper:
    """ECOUNT ERP 웹 스크래퍼 (Playwright headless browser, 병렬 탭 지원)"""

    def __init__(self):
        self._browser = None
        self._context = None
        self._page = None          # 로그인 전용 페이지
        self._logged_in = False
        self._base_url = ""
        self._ec_req_sid = ""
        self._lock = asyncio.Lock()  # 로그인 동시성 보호
        self._last_used: float = 0
        self._idle_timeout: int = 300  # 5 minutes
        self._cleanup_task: Optional[asyncio.Task] = None

    async def _ensure_browser(self):
        """Playwright 브라우저 인스턴스 보장"""
        if self._browser and self._browser.is_connected():
            return

        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
        )
        self._page = await self._context.new_page()
        self._logged_in = False
        self._last_used = time.time()
        await self._start_cleanup_timer()
        logger.info("[ERPWebScraper] 브라우저 시작")

    async def _start_cleanup_timer(self):
        """Idle timeout: close browser after 5 minutes of inactivity"""
        if self._cleanup_task and not self._cleanup_task.done():
            return  # already running
        self._cleanup_task = asyncio.create_task(self._idle_cleanup_loop())

    async def _idle_cleanup_loop(self):
        """Background task that checks idle time and closes browser"""
        try:
            while True:
                await asyncio.sleep(60)  # check every minute
                if self._browser and self._last_used > 0:
                    elapsed = time.time() - self._last_used
                    if elapsed > self._idle_timeout:
                        logger.info(f"[ERPWebScraper] Browser idle for {elapsed:.0f}s, closing to free memory")
                        await self.close()
                        break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"[ERPWebScraper] Cleanup timer error: {e}")

    async def _login(self) -> bool:
        """ECOUNT 웹 로그인 (self._page 사용)"""
        if not ERP_COM_CODE or not ERP_WEB_USER_ID or not ERP_WEB_USER_PW:
            logger.error("[ERPWebScraper] 웹 로그인 정보 미설정")
            return False

        await self._ensure_browser()
        page = self._page

        try:
            await page.goto("https://login.ecount.com/", wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(1000)

            com_input = page.locator('input[name="COM_CODE"], input#COM_CODE, input[placeholder*="회사"]').first
            await com_input.fill(ERP_COM_CODE)

            id_input = page.locator('input[name="USER_ID"], input#USER_ID, input[placeholder*="아이디"]').first
            await id_input.fill(ERP_WEB_USER_ID)

            pw_input = page.locator('input[name="USER_PW"], input#USER_PW, input[type="password"]').first
            await pw_input.fill(ERP_WEB_USER_PW)

            login_btn = page.locator('button[type="submit"], input[type="submit"], .login-btn, #loginBtn, button:has-text("로그인")').first
            await login_btn.click()

            await page.wait_for_url("**/ec5/view/erp**", timeout=15000)
            await page.wait_for_timeout(2000)

            current_url = page.url
            self._base_url = re.match(r"(https://[^/]+)", current_url).group(1)
            sid_match = re.search(r"ec_req_sid=([^&#]+)", current_url)
            if sid_match:
                self._ec_req_sid = sid_match.group(1)

            self._logged_in = True
            logger.info(f"[ERPWebScraper] 로그인 성공 - base: {self._base_url}")

            # 팝업 닫기
            try:
                close_btns = page.locator('.ec-notification-close, .popup-close, [class*="close"]')
                for i in range(await close_btns.count()):
                    await close_btns.nth(i).click()
                    await page.wait_for_timeout(300)
            except Exception:
                pass

            return True

        except Exception as e:
            logger.error(f"[ERPWebScraper] 로그인 실패: {e}", exc_info=True)
            self._logged_in = False
            return False

    async def _ensure_logged_in(self) -> bool:
        """로그인 상태 확인 및 필요시 재로그인 (lock으로 동시성 보호)"""
        async with self._lock:
            if self._logged_in and self._page and not self._page.is_closed():
                try:
                    current_url = self._page.url
                    if "login.ecount.com" in current_url or "ec5/view/erp" not in current_url:
                        self._logged_in = False
                except Exception:
                    self._logged_in = False

            if not self._logged_in:
                return await self._login()
            return True

    async def _create_tab(self):
        """새 브라우저 탭 생성 (로그인 세션 쿠키 공유)"""
        return await self._context.new_page()

    # ──────────────────────────────────────────────
    #  페이지 조작 메서드 (page 파라미터를 받아 독립 실행)
    # ──────────────────────────────────────────────

    async def _navigate_to_page(self, page, page_hash: str) -> bool:
        """특정 ERP 메뉴 페이지로 이동"""
        target_url = f"{self._base_url}/ec5/view/erp?w_flag=1&ec_req_sid={self._ec_req_sid}#{page_hash}"

        try:
            await page.goto(target_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            if "login.ecount.com" in page.url:
                logger.warning("[ERPWebScraper] 세션 만료 - 재로그인 시도")
                self._logged_in = False
                if not await self._login():
                    return False
                await page.goto(target_url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(3000)

            return True
        except Exception as e:
            logger.error(f"[ERPWebScraper] 페이지 이동 실패: {e}")
            return False

    async def _set_search_params_and_execute(self, page, from_date: str, to_date: str, cust_code: str = "") -> bool:
        """
        검색 조건 설정 후 검색 실행.
        F3 → 날짜/거래처 설정 → F8 검색.
        """
        try:
            await page.keyboard.press("F3")
            await page.wait_for_timeout(2000)

            if from_date and to_date:
                from_year = from_date[:4]
                from_month = str(int(from_date[4:6]))
                from_day = from_date[6:8]
                to_year = to_date[:4]
                to_month = str(int(to_date[4:6]))
                to_day = to_date[6:8]

                await self._set_ecount_datepicker(page, from_year, from_month, from_day, to_year, to_month, to_day)

            if cust_code:
                cust_input = page.locator('input[placeholder*="거래처"]').first
                if await cust_input.count() > 0:
                    await cust_input.click()
                    await cust_input.fill(cust_code)
                    await page.wait_for_timeout(1000)
                    await cust_input.press("Enter")
                    await page.wait_for_timeout(500)

            await page.keyboard.press("F8")
            await page.wait_for_timeout(5000)

            try:
                await page.wait_for_selector(
                    'table tbody tr, [class*="grid"] tr',
                    timeout=15000
                )
            except Exception:
                logger.warning("[ERPWebScraper] 테이블 로딩 타임아웃 - 데이터 없거나 로딩 중")

            return True
        except Exception as e:
            logger.error(f"[ERPWebScraper] 검색 설정/실행 실패: {e}", exc_info=True)
            return False

    async def _set_ecount_datepicker(self, page, from_year, from_month, from_day, to_year, to_month, to_day):
        """ECOUNT 커스텀 datepicker 위젯에 날짜 설정"""
        # 방법 1: 버튼 기반 datepicker
        wrappers = page.locator('.wrapper-datepicker > .wrapper-datepicker')
        wrapper_count = await wrappers.count()

        if wrapper_count >= 2:
            logger.info(f"[ERPWebScraper] 버튼 기반 datepicker 감지 ({wrapper_count}개)")
            await self._set_datepicker_wrapper(page, wrappers.nth(0), from_year, from_month, from_day)
            await self._set_datepicker_wrapper(page, wrappers.nth(1), to_year, to_month, to_day)
            return

        # 방법 2: select 기반 datepicker (fallback)
        logger.info("[ERPWebScraper] select 기반 datepicker fallback")
        await page.evaluate("""(dates) => {
            const selects = document.querySelectorAll('select');
            const yearSelects = [];
            const monthSelects = [];
            const daySelects = [];

            selects.forEach(s => {
                const opts = Array.from(s.options).map(o => o.value);
                if (opts.some(v => v === '2026' || v === '2025')) yearSelects.push(s);
                else if (opts.length >= 2 && opts.length <= 13 && opts.some(v => /^0?[1-9]$/.test(v) || v === '10' || v === '11' || v === '12')) monthSelects.push(s);
                else if (opts.length >= 2 && opts.length <= 32 && opts.some(v => /^0?[1-9]$/.test(v) || v === '28' || v === '30' || v === '31')) daySelects.push(s);
            });

            function setSelect(sel, val) {
                if (!sel) return;
                sel.value = val;
                sel.dispatchEvent(new Event('change', {bubbles: true}));
            }

            setSelect(yearSelects[0], dates.fy);
            setSelect(monthSelects[0], dates.fm);
            setSelect(daySelects[0], dates.fd);
            setSelect(yearSelects[1], dates.ty);
            setSelect(monthSelects[1], dates.tm);
            setSelect(daySelects[1], dates.td);
        }""", {
            "fy": from_year, "fm": from_month, "fd": from_day,
            "ty": to_year, "tm": to_month, "td": to_day,
        })
        await page.wait_for_timeout(500)

    async def _set_datepicker_wrapper(self, page, wrapper, year: str, month: str, day: str):
        """개별 datepicker wrapper 의 년/월/일 설정"""
        month_padded = month.zfill(2)

        try:
            # 년 설정
            year_btn = wrapper.locator('button[data-cid="year"], button[data-id="year"]').first
            if await year_btn.count() > 0:
                current_year = (await year_btn.locator('.selectbox-label').text_content()).strip()
                if current_year != year:
                    await self._click_selectbox_option(page, year_btn, year)

            # 월 설정
            month_btn = wrapper.locator('button[data-cid="month"], button[data-id="month"]').first
            if await month_btn.count() > 0:
                current_month = (await month_btn.locator('.selectbox-label').text_content()).strip()
                if current_month != month_padded and current_month != month:
                    await self._click_selectbox_option(page, month_btn, month_padded)

            # 일 설정
            day_input = wrapper.locator('input#day, input[data-cid="day"]').first
            if await day_input.count() > 0:
                await day_input.click()
                await day_input.fill("")
                await day_input.type(day.zfill(2))
                await day_input.press("Tab")
                await page.wait_for_timeout(200)

        except Exception as e:
            logger.warning(f"[ERPWebScraper] datepicker 설정 오류: {e}")

    async def _click_selectbox_option(self, page, button, target_text: str):
        """ECOUNT 커스텀 selectbox 버튼 클릭 → 드롭다운에서 옵션 선택"""
        await button.click()
        await page.wait_for_timeout(600)

        dropdown = page.locator('.dropdown-menu.show')
        if await dropdown.count() == 0:
            logger.warning("[ERPWebScraper] selectbox 드롭다운 미출현")
            return

        options = dropdown.locator('li a')
        count = await options.count()
        clicked = False
        for i in range(count):
            opt = options.nth(i)
            text = (await opt.text_content()).strip()
            if text == target_text:
                await opt.click()
                clicked = True
                break

        if not clicked:
            logger.warning(f"[ERPWebScraper] selectbox 옵션 '{target_text}' 미발견 (총 {count}개)")
            await page.keyboard.press("Escape")

        await page.wait_for_timeout(300)

    async def _parse_grid_data(self, page, page_type: str = "purchase") -> list[dict]:
        """그리드 데이터를 JavaScript로 파싱"""
        js_code = """
        () => {
            const results = [];
            const tables = document.querySelectorAll('table');
            let dataTable = null;

            for (const table of tables) {
                const headers = table.querySelectorAll('th');
                if (headers.length >= 5) {
                    const headerText = Array.from(headers).map(h => h.textContent.trim()).join('|');
                    if (headerText.includes('품') || headerText.includes('수량') || headerText.includes('단가')) {
                        dataTable = table;
                        break;
                    }
                }
            }

            if (!dataTable) {
                const gridBody = document.querySelector('.ec-grid-body, [class*="grid-body"], [class*="gridBody"]');
                if (gridBody) {
                    dataTable = gridBody.querySelector('table') || gridBody;
                }
            }

            if (!dataTable) {
                return { error: 'no_table_found', html_preview: document.body?.innerHTML?.substring(0, 500) || '' };
            }

            const headerCells = dataTable.querySelectorAll('thead th, tr:first-child th');
            const headers = Array.from(headerCells).map(h => h.textContent.trim());

            const rows = dataTable.querySelectorAll('tbody tr, tr');
            for (const row of rows) {
                if (row.querySelector('th')) continue;
                const cells = row.querySelectorAll('td');
                if (cells.length < 3) continue;

                const rowData = {};
                cells.forEach((cell, i) => {
                    const key = headers[i] || `col_${i}`;
                    rowData[key] = cell.textContent.trim();
                });

                const values = Object.values(rowData).filter(v => v);
                if (values.length >= 3) {
                    results.push(rowData);
                }
            }

            return { success: true, headers, data: results, count: results.length };
        }
        """

        try:
            result = await page.evaluate(js_code)

            if isinstance(result, dict) and result.get("error"):
                logger.warning(f"[ERPWebScraper] 테이블 파싱 실패: {result.get('error')}")
                return []

            items = result.get("data", [])
            headers = result.get("headers", [])
            logger.info(f"[ERPWebScraper] 파싱 완료: {len(items)}개 항목, 헤더: {headers}")

            return self._normalize_items(items, headers, page_type)

        except Exception as e:
            logger.error(f"[ERPWebScraper] 그리드 파싱 오류: {e}", exc_info=True)
            return []

    def _normalize_items(self, raw_items: list[dict], headers: list[str], page_type: str) -> list[dict]:
        """ECOUNT 그리드 데이터를 표준 형식으로 변환"""
        normalized = []
        current_year = datetime.now().strftime("%Y")

        for item in raw_items:
            try:
                # 날짜
                date_raw = (
                    item.get("연/월/일", "")
                    or item.get("월/일", "")
                    or item.get("일자", "")
                    or item.get("col_0", "")
                )
                date_str = ""
                if date_raw:
                    date_clean = date_raw.split("-")[0].strip() if "-" in date_raw else date_raw.strip()
                    if "/" in date_clean:
                        parts = date_clean.split("/")
                        if len(parts) == 3:
                            year, month, day = parts[0].strip(), parts[1].strip(), parts[2].strip()
                            date_str = f"{year}{month.zfill(2)}{day.zfill(2)}"
                        elif len(parts) == 2:
                            month, day = parts[0].strip(), parts[1].strip()
                            date_str = f"{current_year}{month.zfill(2)}{day.zfill(2)}"

                prod_cd = item.get("품목코드", "") or item.get("col_1", "")
                prod_name = (
                    item.get("품명 및 규격", "")
                    or item.get("품명 및 모델", "")
                    or item.get("품명", "")
                    or item.get("col_2", "")
                )

                qty = self._parse_number(item.get("수량", "") or item.get("col_3", ""))
                price = self._parse_number(item.get("단가", "") or item.get("col_4", ""))
                supply_amt = self._parse_number(item.get("공급가액", "") or item.get("col_7", ""))
                vat_amt = self._parse_number(item.get("부가세", "") or item.get("col_8", ""))
                total = self._parse_number(item.get("합계", "") or item.get("합 계", "") or item.get("col_9", ""))

                if not prod_cd and not prod_name:
                    continue

                entry = {
                    "date": date_str,
                    "prod_cd": prod_cd.strip(),
                    "prod_name": prod_name.strip(),
                    "qty": int(qty) if qty else 0,
                    "price": price,
                    "supply_amt": supply_amt,
                    "vat_amt": vat_amt,
                    "total": total,
                }

                if page_type == "purchase":
                    partner_str = item.get("파트너가", "") or item.get("col_5", "")
                    entry["partner_price"] = self._parse_number(partner_str)
                    inbound_str = item.get("입고단가", "") or item.get("col_6", "")
                    entry["inbound_price"] = self._parse_number(inbound_str)
                    entry["cust_name"] = (item.get("구매처명", "") or "").strip()
                    entry["warehouse"] = (item.get("창고명", "") or "").strip()
                elif page_type == "sale":
                    entry["cust_name"] = (item.get("판매처명", "") or item.get("판1매처명", "")).strip()
                    entry["model_name"] = (item.get("모델명", "") or "").strip()
                    entry["warehouse"] = (item.get("창고", "") or "").strip()
                    entry["inbound_price"] = self._parse_number(item.get("입고단가", ""))
                    entry["remarks"] = (item.get("발송수단 및 비고사항", "") or "").strip()

                normalized.append(entry)

            except Exception as e:
                logger.warning(f"[ERPWebScraper] 항목 정규화 실패: {e} - raw: {item}")
                continue

        return normalized

    @staticmethod
    def _parse_number(s: str) -> float:
        """숫자 문자열 파싱 (콤마, 공백 제거)"""
        if not s:
            return 0.0
        s = s.strip().replace(",", "").replace(" ", "")
        try:
            return float(s)
        except ValueError:
            return 0.0

    # ──── 개별 탭에서 실행하는 내부 워커 ────

    async def _fetch_on_tab(self, page_hash: str, page_type: str,
                            from_date: str, to_date: str, cust_code: str) -> dict:
        """
        새 탭을 열고 → 메뉴 이동 → 검색 → 파싱 → 탭 닫기.
        병렬 실행 단위.
        """
        tab = await self._create_tab()
        label = "구매현황" if page_type == "purchase" else "판매현황"
        try:
            if not await self._navigate_to_page(tab, page_hash):
                return {"success": False, "items": [], "total": 0, "error": f"{label} 페이지 이동 실패"}

            if not await self._set_search_params_and_execute(tab, from_date, to_date, cust_code):
                return {"success": False, "items": [], "total": 0, "error": f"{label} 검색 실행 실패"}

            items = await self._parse_grid_data(tab, page_type)

            return {"success": True, "items": items, "total": len(items)}

        except Exception as e:
            logger.error(f"[ERPWebScraper] {label} 조회 오류: {e}", exc_info=True)
            return {"success": False, "items": [], "total": 0, "error": str(e)}
        finally:
            try:
                await tab.close()
            except Exception:
                pass

    # ──── 공개 API ────

    async def get_purchase_list(
        self,
        from_date: str = "",
        to_date: str = "",
        cust_code: str = "",
    ) -> dict:
        """ECOUNT 구매현황 데이터 조회 (단독 호출용)"""
        self._last_used = time.time()
        if not await self._ensure_logged_in():
            return {"success": False, "items": [], "total": 0, "error": "로그인 실패"}

        return await self._fetch_on_tab(
            PURCHASE_STATUS_HASH, "purchase", from_date, to_date, cust_code
        )

    async def get_sales_list(
        self,
        from_date: str = "",
        to_date: str = "",
        cust_code: str = "",
    ) -> dict:
        """ECOUNT 판매현황 데이터 조회 (단독 호출용)"""
        self._last_used = time.time()
        if not await self._ensure_logged_in():
            return {"success": False, "items": [], "total": 0, "error": "로그인 실패"}

        return await self._fetch_on_tab(
            SALE_STATUS_HASH, "sale", from_date, to_date, cust_code
        )

    async def get_both(
        self,
        from_date: str = "",
        to_date: str = "",
        purchase_cust_code: str = "",
        sales_cust_code: str = "",
    ) -> dict:
        """
        구매현황 + 판매현황을 **브라우저 탭 2개로 동시 조회**.
        순차 대비 약 40~50 % 시간 절약.

        Returns:
            {
                "success": bool,
                "purchase": {"success", "items", "total", "error?"},
                "sales":    {"success", "items", "total", "error?"},
            }
        """
        self._last_used = time.time()
        if not await self._ensure_logged_in():
            err = {"success": False, "items": [], "total": 0, "error": "로그인 실패"}
            return {"success": False, "purchase": err, "sales": err}

        logger.info("[ERPWebScraper] 구매+판매 병렬 조회 시작")

        purchase_task = self._fetch_on_tab(
            PURCHASE_STATUS_HASH, "purchase", from_date, to_date, purchase_cust_code,
        )
        sales_task = self._fetch_on_tab(
            SALE_STATUS_HASH, "sale", from_date, to_date, sales_cust_code,
        )

        purchase_result, sales_result = await asyncio.gather(
            purchase_task, sales_task, return_exceptions=True,
        )

        # 예외 처리
        if isinstance(purchase_result, Exception):
            purchase_result = {"success": False, "items": [], "total": 0, "error": str(purchase_result)}
        if isinstance(sales_result, Exception):
            sales_result = {"success": False, "items": [], "total": 0, "error": str(sales_result)}

        overall = purchase_result.get("success", False) or sales_result.get("success", False)

        logger.info(
            f"[ERPWebScraper] 병렬 조회 완료 - "
            f"구매 {purchase_result.get('total', 0)}건, "
            f"판매 {sales_result.get('total', 0)}건"
        )

        return {
            "success": overall,
            "purchase": purchase_result,
            "sales": sales_result,
        }

    async def close(self):
        """브라우저 정리"""
        try:
            if self._cleanup_task and not self._cleanup_task.done():
                self._cleanup_task.cancel()
            if self._browser:
                await self._browser.close()
            if hasattr(self, '_playwright') and self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        finally:
            self._browser = None
            self._context = None
            self._page = None
            self._logged_in = False
            self._cleanup_task = None
            logger.info("[ERPWebScraper] 브라우저 종료")


# 싱글톤 인스턴스
erp_web_scraper = ERPWebScraper()
