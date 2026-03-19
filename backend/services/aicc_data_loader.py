"""
AICC 데이터 로더 — 서버 시작 시 1회 로드 후 메모리 유지
기존 shop/aicc 시스템의 데이터 구조 + 검색 로직 완전 이식
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
        self._driver_models: Set[str] = set()  # 드라이버가 있는 모델 목록

        # 기존 shop/aicc 시스템 데이터 (searchRelevantQna용)
        self.technical_qna: List[dict] = []     # lanstar_technical_qna.json
        self.unidentified_qna: List[dict] = []  # lanstar_unidentified_qna.json

        # 품목가격정보 (제품문의용)
        self.price_info: Dict[str, dict] = {}   # model_name → {품목명, 딜러가, 온라인노출가}
        self._price_model_map: Dict[str, str] = {}  # 모델명(괄호 제거) → 원본 키

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
            self._load_driver_models()
            self._load_technical_qna()
            self._load_unidentified_qna()
            self._load_price_info()
            total_qna = sum(len(p.get("qna", [])) for p in self.technical_qna)
            print(f"[AICC] 완료 — 드롭다운:{len(self.dropdown_models)} 제품:{len(self.product_data)} "
                  f"기술QnA:{total_qna}건({len(self.technical_qna)}모델) 미분류QnA:{len(self.unidentified_qna)}건")
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
        self.wrong_answers_text = text[:2000]

    def _load_install(self):
        for fname in ["05_제품별_연결방법_설치가이드_정제.txt", "05_제품별_연결방법_설치가이드.txt"]:
            path = os.path.join(DATA_DIR, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    self.install_guide_text = f.read()
                return
        print("[AICC] 설치가이드 파일 없음 (스킵)")

    def _load_compat(self):
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

    def _load_driver_models(self):
        """08_기술자료실 파일에서 드라이버가 있는 모델 목록 파싱"""
        path = os.path.join(DATA_DIR, "08_기술자료실_파일목록_URL.txt")
        if not os.path.exists(path):
            print("[AICC] 기술자료실 파일 없음 (스킵)")
            return
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        # [모델별 빠른 검색 URL 전체 목록] 섹션에서 모델명 추출
        # 패턴: 줄 시작 공백 + LS-모델명 (URL: 줄 바로 위에 있는 모델명)
        for match in re.finditer(r'^\s+(LS-[\w\-]+)\s*$', text, re.MULTILINE):
            model = match.group(1).strip()
            self._driver_models.add(model)
        print(f"[AICC] 드라이버 모델 로드: {len(self._driver_models)}개")

    def _load_price_info(self):
        """품목가격정보.json 로드 — 제품문의 시 가격 비교용"""
        path = os.path.join(DATA_DIR, "품목가격정보.json")
        if not os.path.exists(path):
            print("[AICC] 품목가격정보.json 없음 (스킵)")
            return
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for full_key, val in raw.items():
            # 키: "LS-H21AOC-5M(AVO-HD선054)" → 모델명: "LS-H21AOC-5M"
            model_name = full_key.split("(")[0].strip()
            self.price_info[model_name] = {
                "품목명": val.get("품목명", ""),
                "딜러가": val.get("딜러가", 0),
                "온라인노출가": val.get("온라인노출가", 0),
                "원본키": full_key,
            }
            self._price_model_map[model_name.upper()] = model_name
        print(f"[AICC] 품목가격정보 로드: {len(self.price_info)}개")

    def search_products_for_recommendation(self, query: str, max_results: int = 15) -> List[dict]:
        """
        고객 질문에서 키워드를 추출하여 제품 추천용 데이터 검색.
        01_제품별_통합데이터.json + 품목가격정보.json 교차 검색.
        """
        upper = query.upper()
        words = [w for w in re.split(r'[\s,]+', upper) if len(w) >= 2]
        if not words:
            return []

        results = []
        seen_models = set()

        # 1. product_data (01_제품별_통합데이터.json) 검색
        for model_name, product in self.product_data.items():
            feat = product.get("제품특징", {})
            cat = product.get("카테고리", "")

            # 검색 대상 텍스트 구성
            if isinstance(feat, dict):
                search_text = " ".join(str(v) for v in feat.values()) + " " + cat + " " + model_name
            elif isinstance(feat, list):
                search_text = " ".join(feat) + " " + cat + " " + model_name
            else:
                search_text = str(feat) + " " + cat + " " + model_name
            search_text = search_text.upper()

            score = 0
            for w in words:
                if w in search_text:
                    score += 1

            if score > 0:
                # 가격 정보 매칭
                price_data = self.price_info.get(model_name, {})
                product_name = ""
                if isinstance(feat, dict):
                    product_name = feat.get("제품명_full", "") or feat.get("용도", "")
                if not product_name and price_data:
                    product_name = price_data.get("품목명", "")

                results.append({
                    "model_name": model_name,
                    "category": cat,
                    "product_name": product_name,
                    "features": feat,
                    "price_tier": price_data.get("온라인노출가", 0),
                    "score": score,
                })
                seen_models.add(model_name.upper())

        # 2. price_info에만 있는 제품도 검색 (product_data에 없는 것)
        for model_name, pinfo in self.price_info.items():
            if model_name.upper() in seen_models:
                continue
            search_text = (pinfo.get("품목명", "") + " " + model_name).upper()
            score = 0
            for w in words:
                if w in search_text:
                    score += 1
            if score > 0:
                results.append({
                    "model_name": model_name,
                    "category": "",
                    "product_name": pinfo.get("품목명", ""),
                    "features": {},
                    "price_tier": pinfo.get("온라인노출가", 0),
                    "score": score,
                })

        # 점수 내림차순 정렬
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:max_results]

    def get_price_rank(self, model_name: str) -> Optional[int]:
        """해당 모델의 온라인노출가 반환 (상대 비교용)"""
        pinfo = self.price_info.get(model_name)
        if pinfo:
            return pinfo.get("온라인노출가", 0)
        return None

    def _load_technical_qna(self):
        """기존 shop/aicc의 lanstar_technical_qna.json 로드"""
        path = os.path.join(DATA_DIR, "lanstar_technical_qna.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                self.technical_qna = json.load(f)
            print(f"[AICC] 기술QnA 로드: {len(self.technical_qna)}개 모델")
        else:
            print("[AICC] lanstar_technical_qna.json 없음 (스킵)")

    def _load_unidentified_qna(self):
        """기존 shop/aicc의 lanstar_unidentified_qna.json 로드"""
        path = os.path.join(DATA_DIR, "lanstar_unidentified_qna.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                self.unidentified_qna = json.load(f)
            print(f"[AICC] 미분류QnA 로드: {len(self.unidentified_qna)}건")
        else:
            print("[AICC] lanstar_unidentified_qna.json 없음 (스킵)")

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
        return [g for g in self.golden_answers if g["model"] == model][:5]

    def get_faq_by_model(self, model: str) -> List[dict]:
        return [f for f in self.faq_list if model in f.get("models", "")][:5]

    def has_driver(self, model: str) -> bool:
        """해당 모델에 드라이버가 있는지 확인"""
        return model in self._driver_models

    def get_driver_url(self, model: str) -> str:
        search_word = model.replace("LS-", "").lower() if model.startswith("LS-") else model.lower()
        return f"https://www.lanstar.co.kr/board/list.php?bdId=lanstardownload&memNo=&noheader=&mypageFl=&searchField=subject&searchWord={search_word}"

    def get_product_url(self, model: str) -> str:
        return f"https://www.lanstar.co.kr/goods/goods_search.php?keyword={model}&recentCount=10"

    def is_price_restricted(self, model: str) -> bool:
        return model in self.price_restricted

    # ── 기존 shop/aicc searchRelevantQna 완전 이식 ──────────

    def _get_related_models(self, model: str) -> set:
        """같은 제품군의 관련 모델명 반환 (ADOOR↔ANDOOR 등)"""
        if not model:
            return set()
        upper = model.upper()
        related = {upper}

        # ADOOR ↔ ANDOOR 매핑 (같은 도어락 제품군)
        if "ANDOOR" in upper:
            base = upper.replace("ANDOOR", "ADOOR")
            related.add(base)
            # LS-ANDOOR-S → LS-ADOOR, LS-ADOOR-S 등
            related.add(re.sub(r'-[SB]$', '', base))
            related.add(re.sub(r'-[SB]$', '', upper))
        elif "ADOOR" in upper:
            base = upper.replace("ADOOR", "ANDOOR")
            related.add(base)
            related.add(re.sub(r'-[SB]$', '', base))
            related.add(re.sub(r'-[SB]$', '', upper))

        # 같은 제품의 변형 (LS-XXX-S, LS-XXX-B → LS-XXX 공통)
        base_no_suffix = re.sub(r'-[SB]$', '', upper)
        if base_no_suffix != upper:
            related.add(base_no_suffix)

        return related

    def search_relevant_qna(self, query: str, session_model: str, max_results: int = 10) -> List[dict]:
        """
        1. lanstar_technical_qna.json의 모든 제품 QnA 검색
        2. lanstar_unidentified_qna.json (미분류 QnA) 검색
        3. 같은 모델 = +5점, 관련 모델(ADOOR↔ANDOOR) = +4점, 키워드 매칭 = +1점/단어
        """
        upper = query.upper()
        words = [w for w in re.split(r'[\s,]+', upper) if len(w) >= 2]
        results = []

        # 관련 모델 그룹 (ADOOR↔ANDOOR, -S↔-B 등)
        related_models = self._get_related_models(session_model)

        # 1. technical_qna (모델별 QnA)
        for product in self.technical_qna:
            product_model = product.get("model", "")
            product_model_upper = product_model.upper()
            for qna in product.get("qna", []):
                q_text = qna.get("question", "")
                a_text = qna.get("answer", "")
                text = (q_text + " " + a_text + " " + product_model).upper()

                score = 0
                # 정확히 같은 모델
                if session_model and product_model_upper == session_model.upper():
                    score += 5
                # 관련 모델 (ADOOR↔ANDOOR 등) — 같은 제품군이므로 높은 점수
                elif product_model_upper in related_models or any(
                    rm in product_model_upper or product_model_upper in rm
                    for rm in related_models
                ):
                    score += 4
                for w in words:
                    if w in text:
                        score += 1
                if score > 0:
                    results.append({
                        "model": product_model,
                        "category": product.get("category", ""),
                        "question": q_text,
                        "answer": a_text,
                        "score": score,
                    })

        # 2. unidentified_qna (미분류 QnA — 도어락 등 특수 제품 커버)
        for qna in self.unidentified_qna:
            q_text = qna.get("question", "")
            a_text = qna.get("answer", "")
            text = (q_text + " " + a_text).upper()

            score = 0
            for w in words:
                if w in text:
                    score += 1
            # 미분류지만 모델명이 포함되어 있으면 가산
            if session_model and session_model.upper() in text:
                score += 3
            if score > 0:
                results.append({
                    "model": qna.get("original_product_name", ""),
                    "category": "",
                    "question": q_text,
                    "answer": a_text,
                    "score": score,
                })

        # 점수 내림차순 정렬
        results.sort(key=lambda x: x["score"], reverse=True)

        # 중복 제거 (질문 앞 50자)
        seen = set()
        unique = []
        for r in results:
            key = r["question"][:50]
            if key not in seen:
                seen.add(key)
                unique.append(r)

        return unique[:max_results]


data_loader = AICCDataLoader()
