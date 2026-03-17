"""
AICC 데이터 로더
서버 시작 시 한 번만 로드 → 전역 싱글톤으로 관리
"""
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Set
import openpyxl
from dotenv import load_dotenv

load_dotenv()

# 기본값: 프로젝트루트/data/aicc (backend의 상위 = 프로젝트루트)
_DEFAULT_DATA_DIR = str(Path(__file__).parent.parent.parent / "data" / "aicc")
DATA_DIR = os.getenv("AICC_DATA_DIR", _DEFAULT_DATA_DIR)


class AICCDataLoader:
    def __init__(self):
        self.dropdown_models: List[dict] = []      # 드롭다운용
        self.product_data: Dict[str, dict] = {}    # AI 답변용
        self.faq_list: List[dict] = []
        self.golden_answers: List[dict] = []
        self.wrong_answers_text: str = ""
        self.install_guide_text: str = ""
        self.compatibility_data: List[dict] = []
        self.error_data: List[dict] = []
        self.tech_urls_text: str = ""
        self.policy_as: str = ""
        self.policy_delivery: str = ""
        self.policy_return: str = ""
        self.price_restricted_models: Set[str] = set()
        self._loaded = False
        self._model_to_erp: Dict[str, str] = {}
        self._product_names: Dict[str, str] = {}

    def load_all(self):
        """서버 시작 시 호출 — 모든 데이터 파일 로드"""
        print("[AICC] 데이터 로딩 시작...")

        self._load_product_master()
        self._load_product_json()
        self._load_faq()
        self._load_golden_answers()
        self._load_wrong_answers()
        self._load_install_guide()
        self._load_compatibility()
        self._load_error_data()
        self._load_tech_urls()
        self._load_policies()
        self._load_price_restricted()

        self._loaded = True
        print(f"[AICC] 로딩 완료 — 드롭다운 모델: {len(self.dropdown_models)}개 / 제품데이터: {len(self.product_data)}개")

    def _load_product_master(self):
        """product_master.xlsx → 드롭다운 목록"""
        path = os.path.join(DATA_DIR, "product_master.xlsx")
        if not os.path.exists(path):
            print(f"[AICC] product_master.xlsx 파일 없음: {path}")
            return
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active

        EXCLUDE = {"주문제작", "취소", "반품불가", "제작케", "★"}

        for row in ws.iter_rows(min_row=2, values_only=True):
            erp_code, product_name, model_name = row[0], row[1], row[2]
            if not model_name:
                continue
            model_str = str(model_name).strip()
            # 필터링
            if any(kw in model_str for kw in EXCLUDE):
                continue
            if "★" in model_str:
                continue
            self.dropdown_models.append({
                "erp_code": str(erp_code) if erp_code else "",
                "product_name": str(product_name) if product_name else "",
                "model_name": model_str,
            })
        wb.close()

        # erp_code 빠른 조회용 dict
        self._model_to_erp = {m["model_name"]: m["erp_code"] for m in self.dropdown_models}
        self._product_names = {m["model_name"]: m["product_name"] for m in self.dropdown_models}

    def _load_product_json(self):
        """01_제품별_통합데이터.json"""
        path = os.path.join(DATA_DIR, "01_제품별_통합데이터.json")
        if not os.path.exists(path):
            print(f"[AICC] 통합데이터 파일 없음: {path}")
            return
        with open(path, "r", encoding="utf-8") as f:
            self.product_data = json.load(f)

    def _load_faq(self):
        path = os.path.join(DATA_DIR, "02_FAQ_Top150_카테고리별.xlsx")
        if not os.path.exists(path):
            return
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0]:
                self.faq_list.append({
                    "rank": row[0], "category": row[1], "question": row[2],
                    "frequency": row[3], "models": row[4], "answer": row[5],
                })
        wb.close()

    def _load_golden_answers(self):
        path = os.path.join(DATA_DIR, "03_모범답변_골든앤서.xlsx")
        if not os.path.exists(path):
            return
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0]:
                self.golden_answers.append({
                    "no": row[0], "category": row[1], "model": row[2],
                    "question": row[3], "answer": row[4],
                    "keyword": row[5] if len(row) > 5 else "",
                    "warning": row[6] if len(row) > 6 else "",
                })
        wb.close()

    def _load_wrong_answers(self):
        path = os.path.join(DATA_DIR, "04_오답사례_주의목록.txt")
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            full_text = f.read()
        # 앞 3000자만 시스템 프롬프트에 사용 (토큰 절약)
        self.wrong_answers_text = full_text[:3000]

    def _load_install_guide(self):
        path = os.path.join(DATA_DIR, "05_제품별_연결방법_설치가이드_정제.txt")
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            self.install_guide_text = f.read()

    def _load_compatibility(self):
        path = os.path.join(DATA_DIR, "06_호환성_매트릭스_정제.xlsx")
        if not os.path.exists(path):
            return
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if len(row) >= 5 and row[1]:
                self.compatibility_data.append({
                    "model": row[1], "category": row[2],
                    "question": row[3], "answer": row[4],
                })
        wb.close()

    def _load_error_data(self):
        path = os.path.join(DATA_DIR, "07_오류증상_대응표.xlsx")
        if not os.path.exists(path):
            return
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if len(row) >= 6 and row[1]:
                self.error_data.append({
                    "model": row[1], "category": row[2], "symptom_type": row[3],
                    "symptom": row[4], "solution": row[5],
                })
        wb.close()

    def _load_tech_urls(self):
        path = os.path.join(DATA_DIR, "08_기술자료실_파일목록_URL.txt")
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            self.tech_urls_text = f.read()

    def _load_policies(self):
        for attr, fname in [
            ("policy_as", "09_AS정책_전문.txt"),
            ("policy_delivery", "10_배송정책.txt"),
            ("policy_return", "11_교환반품_규정.txt"),
        ]:
            path = os.path.join(DATA_DIR, fname)
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as f:
                setattr(self, attr, f.read())

    def _load_price_restricted(self):
        path = os.path.join(DATA_DIR, "12_가격지도_적용품목.xlsx")
        if not os.path.exists(path):
            return
        wb = openpyxl.load_workbook(path, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[1]:
                self.price_restricted_models.add(str(row[1]).strip())
        wb.close()

    # ── 조회 메서드 ──────────────────────────────────────────

    def get_product(self, model_name: str) -> dict:
        return self.product_data.get(model_name, {})

    def get_erp_code(self, model_name: str) -> Optional[str]:
        return self._model_to_erp.get(model_name)

    def search_models(self, query: str, limit: int = 15) -> List[dict]:
        """드롭다운 자동완성 검색 (대소문자 무시)"""
        q = query.upper()
        return [
            m for m in self.dropdown_models
            if q in m["model_name"].upper() or q in m["product_name"].upper()
        ][:limit]

    def get_install_guide_section(self, model_name: str) -> str:
        """설치가이드에서 해당 모델 섹션만 추출"""
        pattern = rf'\[{re.escape(model_name)}\](.*?)(?=\n\[LS-|\Z)'
        match = re.search(pattern, self.install_guide_text, re.DOTALL)
        return match.group(1).strip()[:800] if match else ""

    def get_compatibility(self, model_name: str) -> List[dict]:
        return [d for d in self.compatibility_data if d["model"] == model_name][:5]

    def get_errors(self, model_name: str) -> List[dict]:
        return [d for d in self.error_data if d["model"] == model_name][:5]

    def get_golden_answers_by_category(self, category: str) -> List[dict]:
        return [g for g in self.golden_answers if g["category"] == category][:3]

    def is_price_restricted(self, model_name: str) -> bool:
        return model_name in self.price_restricted_models


# 전역 싱글톤
data_loader = AICCDataLoader()
