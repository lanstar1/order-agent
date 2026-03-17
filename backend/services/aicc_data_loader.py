"""
AICC 데이터 로더 — 서버 시작 시 1회 로드 후 메모리 유지
"""
import json, os, re
from typing import Dict, List, Optional, Set
import openpyxl

# Render에서는 cd backend로 시작하므로 ../data/aicc가 올바른 경로
_default_dir = "./data/aicc"
if not os.path.exists(_default_dir) and os.path.exists("../data/aicc"):
    _default_dir = "../data/aicc"
DATA_DIR = os.getenv("AICC_DATA_DIR", _default_dir)

# 드롭다운 제외 조건
EXCLUDE_KEYWORDS = {"주문제작", "취소", "반품불가", "제작케", "★", "제작/취소"}


class AICCDataLoader:
    def __init__(self):
        self.dropdown_models: List[dict] = []
        self.product_data: Dict[str, dict] = {}
        self.faq_list: List[dict] = []
        self.golden_answers: List[dict] = []
        self.wrong_answers_text: str = ""
        self.install_guide_text: str = ""
        self.compatibility_data: List[dict] = []
        self.error_data: List[dict] = []
        self.policy_as: str = ""
        self.policy_delivery: str = ""
        self.policy_return: str = ""
        self.price_restricted: Set[str] = set()
        self._erp_map: Dict[str, str] = {}  # model_name → erp_code

    def load_all(self):
        print("[AICC] 데이터 로딩 시작...")
        try:
            self._load_master()
            self._load_product_json()
            self._load_faq()
            self._load_golden()
            self._load_wrong()
            self._load_install()
            self._load_compat()
            self._load_errors()
            self._load_policies()
            self._load_price()
            print(f"[AICC] 완료 — 드롭다운:{len(self.dropdown_models)} 제품:{len(self.product_data)}")
        except Exception as e:
            print(f"[AICC] 로딩 오류: {e}")
            raise

    def _load_master(self):
        path = os.path.join(DATA_DIR, "product_master.xlsx")
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            erp_code, prod_name, model = row[0], row[1], row[2]
            if not model:
                continue
            m = str(model).strip()
            if any(kw in m for kw in EXCLUDE_KEYWORDS):
                continue
            item = {
                "erp_code": str(erp_code).strip() if erp_code else "",
                "product_name": str(prod_name).strip() if prod_name else "",
                "model_name": m,
            }
            self.dropdown_models.append(item)
            self._erp_map[m] = item["erp_code"]
        wb.close()

    def _load_product_json(self):
        path = os.path.join(DATA_DIR, "01_제품별_통합데이터.json")
        with open(path, "r", encoding="utf-8") as f:
            self.product_data = json.load(f)

    def _load_faq(self):
        path = os.path.join(DATA_DIR, "02_FAQ_Top150_카테고리별.xlsx")
        wb = openpyxl.load_workbook(path, read_only=True)
        for row in wb.active.iter_rows(min_row=2, values_only=True):
            if row[0]:
                self.faq_list.append({
                    "category": str(row[1] or ""),
                    "question": str(row[2] or ""),
                    "models": str(row[4] or ""),
                    "answer": str(row[5] or ""),
                })
        wb.close()

    def _load_golden(self):
        path = os.path.join(DATA_DIR, "03_모범답변_골든앤서.xlsx")
        wb = openpyxl.load_workbook(path, read_only=True)
        for row in wb.active.iter_rows(min_row=2, values_only=True):
            if row[0]:
                self.golden_answers.append({
                    "category": str(row[1] or ""),
                    "model": str(row[2] or ""),
                    "question": str(row[3] or ""),
                    "answer": str(row[4] or ""),
                    "keyword": str(row[5] or ""),
                    "warning": str(row[6] or "") if row[6] else "",
                })
        wb.close()

    def _load_wrong(self):
        path = os.path.join(DATA_DIR, "04_오답사례_주의목록.txt")
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        # 핵심 섹션만 추출 (토큰 절약)
        self.wrong_answers_text = text[:2000]

    def _load_install(self):
        # 실제 파일명에 맞춤 (정제 접미사 없을 수 있음)
        for fname in ["05_제품별_연결방법_설치가이드_정제.txt", "05_제품별_연결방법_설치가이드.txt"]:
            path = os.path.join(DATA_DIR, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    self.install_guide_text = f.read()
                return
        print("[AICC] 설치가이드 파일 없음 (스킵)")

    def _load_compat(self):
        # 실제 파일명에 맞춤
        for fname in ["06_호환성_매트릭스_정제.xlsx", "06_호환성_매트릭스.xlsx"]:
            path = os.path.join(DATA_DIR, fname)
            if os.path.exists(path):
                wb = openpyxl.load_workbook(path, read_only=True)
                for row in wb.active.iter_rows(min_row=2, values_only=True):
                    if row[1]:
                        self.compatibility_data.append({
                            "model": str(row[1]).strip(),
                            "question": str(row[3] or ""),
                            "answer": str(row[4] or ""),
                        })
                wb.close()
                return
        print("[AICC] 호환성 매트릭스 파일 없음 (스킵)")

    def _load_errors(self):
        path = os.path.join(DATA_DIR, "07_오류증상_대응표.xlsx")
        wb = openpyxl.load_workbook(path, read_only=True)
        for row in wb.active.iter_rows(min_row=2, values_only=True):
            if row[1]:
                self.error_data.append({
                    "model": str(row[1]).strip(),
                    "symptom_type": str(row[3] or ""),
                    "symptom": str(row[4] or ""),
                    "solution": str(row[5] or ""),
                })
        wb.close()

    def _load_policies(self):
        for attr, fname in [
            ("policy_as", "09_AS정책_전문.txt"),
            ("policy_delivery", "10_배송정책.txt"),
            ("policy_return", "11_교환반품_규정.txt"),
        ]:
            path = os.path.join(DATA_DIR, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    setattr(self, attr, f.read())

    def _load_price(self):
        path = os.path.join(DATA_DIR, "12_가격지도_적용품목.xlsx")
        wb = openpyxl.load_workbook(path, read_only=True)
        for row in wb.active.iter_rows(min_row=2, values_only=True):
            if row[1]:
                self.price_restricted.add(str(row[1]).strip())
        wb.close()

    # ── 조회 메서드 ──────────────────────────────────────────

    def search_models(self, query: str, limit: int = 15) -> List[dict]:
        q = query.upper().strip()
        if len(q) < 2:
            return []
        return [
            m for m in self.dropdown_models
            if q in m["model_name"].upper() or q in m["product_name"].upper()
        ][:limit]

    def get_product(self, model: str) -> dict:
        return self.product_data.get(model, {})

    def get_erp_code(self, model: str) -> Optional[str]:
        return self._erp_map.get(model)

    def get_install_section(self, model: str) -> str:
        pattern = rf'\[{re.escape(model)}\](.*?)(?=\n\[LS-|\Z)'
        m = re.search(pattern, self.install_guide_text, re.DOTALL)
        return m.group(1).strip()[:800] if m else ""

    def get_compat(self, model: str) -> List[dict]:
        return [d for d in self.compatibility_data if d["model"] == model][:4]

    def get_errors(self, model: str) -> List[dict]:
        return [d for d in self.error_data if d["model"] == model][:4]

    def get_golden_by_category(self, cat: str) -> List[dict]:
        return [g for g in self.golden_answers if g["category"] == cat][:3]

    def get_golden_by_model(self, model: str) -> List[dict]:
        """모델명으로 골든앤서 직접 검색 (최우선)"""
        return [g for g in self.golden_answers if g["model"] == model][:5]

    def get_faq_by_model(self, model: str) -> List[dict]:
        """모델명이 포함된 FAQ 검색"""
        return [f for f in self.faq_list if model in f.get("models", "")][:5]

    def get_driver_url(self, model: str) -> str:
        """드라이버 다운로드 URL 생성 (모델명에서 LS- 접두사 제거)"""
        # LS-UH319-W → uh319-w 형태로 검색어 생성
        search_word = model.replace("LS-", "").lower() if model.startswith("LS-") else model.lower()
        return f"https://www.lanstar.co.kr/board/list.php?bdId=lanstardownload&memNo=&noheader=&mypageFl=&searchField=subject&searchWord={search_word}"

    def get_product_url(self, model: str) -> str:
        """제품 검색 URL 생성"""
        return f"https://www.lanstar.co.kr/goods/goods_search.php?keyword={model}&recentCount=10"

    def is_price_restricted(self, model: str) -> bool:
        return model in self.price_restricted


data_loader = AICCDataLoader()
