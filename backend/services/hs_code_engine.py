"""
HS코드 규칙 엔진
================
선적시트 Excel의 카테고리/모델명을 분석하여 HS코드를 자동 매칭합니다.
규칙 기반 (AI 불필요) - 카테고리 키워드 우선 매칭 방식.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class HSCodeResult:
    hs_code: Optional[str]  # None이면 입력 불필요
    rule_name: str           # 매칭된 규칙명
    confidence: str          # "exact" | "category" | "skip" | "unknown"
    note: str = ""           # 추가 메모


class HSCodeEngine:
    """
    HS코드 매칭 엔진
    
    판단 우선순위:
    1. SKIP 규칙 (HS코드 불필요 품목) - 가장 먼저 체크
    2. EXACT 카테고리 매칭 (8544.42, 8536.69, 9403.99, 8203.20)
    3. 미매칭 → unknown (대시보드 알림)
    """
    
    # ===== HS코드 미입력 (FTA 불필요) =====
    SKIP_RULES = [
        {
            "name": "patch_cord",
            "keywords": ["PATCH CORD"],
            "note": "패치코드 - FTA 불필요",
        },
        {
            "name": "network_cable_generic",
            # PATCH CORD/LAN CABLE 명시 없어도 Cat./UTP/FTP/SFTP 키워드면 네트워크 케이블
            "keywords": ["CAT.5", "CAT.6", "CAT.7", "CAT.8",
                         "CAT5", "CAT6", "CAT7", "CAT8",
                         "U/UTP", "F/UTP", "S/FTP", "SF/UTP",
                         "UTP ", "FTP ", "SFTP "],
            "note": "네트워크 케이블(Cat/UTP/FTP) - FTA 불필요",
        },
        {
            "name": "lan_cable",
            "keywords": ["LAN CABLE", "REEL CABLE"],
            "note": "랜케이블/릴케이블 - FTA 불필요",
        },
        {
            "name": "fiber_optic",
            "keywords": ["FIBER OPTIC", "FIBRE OPTIC"],
            "note": "광케이블 - FTA 불필요",
        },
        {
            "name": "rack_cabinet_body",
            # "RACK CABINET"만 매칭, "RACK CABINET ACCESSORIES"는 제외
            "keywords": ["RACK CABINET"],
            "exclude_keywords": ["ACCESSORIES"],
            "note": "랙캐비닛 본체 - FTA 불필요",
        },
        {
            "name": "open_rack",
            "keywords": ["OPEN RACK", "2 POST"],
            "note": "오픈랙 - FTA 불필요",
        },
        {
            "name": "high_rack",
            "keywords": ["HIGH RACK"],
            "note": "하이랙 - FTA 불필요",
        },
        {
            "name": "screw_cable_network",
            # 네트워크 스크류 케이블 (F/UTP, Cat.5e Screw)만 해당
            # USB Screw Cable은 8544.42 → HS코드 입력 규칙에서 먼저 캐치
            "keywords": ["SCREW CABLE"],
            "require_keywords": ["CAT", "UTP", "FTP"],  # 네트워크 관련 키워드 동반 필수
            "note": "네트워크 스크류케이블 - FTA 불필요",
        },
        {
            "name": "pallet",
            "keywords": ["PALLET"],
            "note": "팔레트 - FTA 불필요",
        },
        {
            "name": "car_project",
            "keywords": ["CAR PROJECT"],
            "note": "차량 프로젝트 - FTA 불필요",
        },
    ]
    
    # ===== HS코드 입력 대상 =====
    HS_CODE_RULES = [
        {
            "hs_code": "8544.42",
            "name": "av_data_cable",
            "category_keywords": [
                "USB CABLE", "USB 2.0 CABLE", "USB 3.0 CABLE",
                "USB 2.0 MINI", "USB 2.0 SCREW", "USB 3.0 SCREW",
                "USB 3.0 AOC",
                "HDMI", "DVI", "SERIAL CABLE",
                "DP CABLE", "DP 1.2", "DP 1.4",
                "DP 1.2 TO HDMI", "MDP 1.2 TO HDMI",
                "DVI TO HDMI",
                "A/V CABLE",
                "TYPE-C TO RJ45", "TYPE-C CABLE",
                "AOC CABLE",
                "ANTENNA CABLE", "KEYBOARD CABLE",
            ],
            "description_keywords": [
                "USB", "HDMI", "DVI", "DISPLAYPORT",
                "SERIAL", "RCA", "3.5 ST",
                "TYPE-C TO RJ45",
            ],
            "note": "영상/음향/데이터 케이블",
        },
        {
            "hs_code": "8536.69",
            "name": "network_accessories",
            "category_keywords": ["NETWORKS"],
            "description_keywords": [
                "CONNECTOR", "COUPLER", "KEYSTONE",
                "CONNECTION BOX", "SURFACE MOUNT",
                "OUTLET", "BACK BOX", "BACKBOX",
                "STR-", "JACK",
            ],
            "note": "네트워크 액세서리 (커넥터, 커플러 등)",
        },
        {
            "hs_code": "9403.99",
            "name": "rack_cabinet_accessories",
            "category_keywords": ["RACK CABINET ACCESSORIES"],
            "description_keywords": [
                "BLANK PANEL", "SHELF", "TRAY",
                "CASTER", "FAN UNIT",
            ],
            "note": "랙캐비닛 액세서리",
        },
        {
            "hs_code": "8203.20",
            "name": "crimping_tool",
            "category_keywords": ["CRIMPING TOOL"],
            "description_keywords": ["CRIMPING", "CRIMP TOOL"],
            "model_patterns": [r"LS-68R", r"LS-68RP"],
            "note": "압착공구",
        },
        {
            "hs_code": "8544.69",
            "name": "fiber_transceiver",
            "category_keywords": [],
            "description_keywords": [],
            "model_patterns": [r"LS-F850", r"LS-F1310", r"LS-SFP", r"LSN-SFP"],
            "note": "광트랜시버/SFP 모듈 (8544.69)",
        },
    ]
    
    def __init__(self):
        pass
    
    def match(self, category: str, model_desc: str) -> HSCodeResult:
        """
        HS코드 매칭 실행
        
        Args:
            category: B열 카테고리 텍스트 (예: "U/UTP CAT.6 PATCH CORD")
            model_desc: A열 모델명+설명 텍스트 (예: "LS-6UTPD-7MG, U/UTP Cat.6...")
        
        Returns:
            HSCodeResult with hs_code (None=불필요), rule_name, confidence
        """
        cat_upper = (category or "").upper().strip()
        desc_upper = (model_desc or "").upper().strip()
        combined = f"{cat_upper} {desc_upper}"
        
        # ===== 1단계: 모델명 패턴 최우선 매칭 =====
        # LS-68R은 NETWORKS 안에 있지만 CRIMPING TOOL → 8203.20
        for rule in self.HS_CODE_RULES:
            for pattern in rule.get("model_patterns", []):
                if re.search(pattern, desc_upper, re.IGNORECASE):
                    return HSCodeResult(
                        hs_code=rule["hs_code"],
                        rule_name=rule["name"],
                        confidence="exact",
                        note=rule["note"],
                    )
        
        # ===== 2단계: 카테고리 키워드 매칭 (설명보다 우선) =====
        # NETWORKS 카테고리 안 제품은 HDMI/Cable 등이 설명에 있어도 8536.69
        for rule in self.HS_CODE_RULES:
            for kw in rule.get("category_keywords", []):
                if kw in cat_upper:
                    return HSCodeResult(
                        hs_code=rule["hs_code"],
                        rule_name=rule["name"],
                        confidence="category",
                        note=rule["note"],
                    )
        
        # ===== 3단계: 설명 키워드 매칭 (카테고리에서 안 잡힌 경우만) =====
        for rule in self.HS_CODE_RULES:
            for kw in rule.get("description_keywords", []):
                if kw in desc_upper or kw in cat_upper:
                    return HSCodeResult(
                        hs_code=rule["hs_code"],
                        rule_name=rule["name"],
                        confidence="category",
                        note=f"{rule['note']} (설명 매칭)",
                    )
        
        # ===== 2단계: SKIP 규칙 (HS코드 불필요) =====
        for rule in self.SKIP_RULES:
            matched = False
            
            for kw in rule["keywords"]:
                if kw in cat_upper or kw in desc_upper:
                    matched = True
                    break
            
            if not matched:
                continue
            
            # exclude 키워드 체크
            if "exclude_keywords" in rule:
                excluded = False
                for ekw in rule["exclude_keywords"]:
                    if ekw in cat_upper or ekw in desc_upper:
                        excluded = True
                        break
                if excluded:
                    continue  # exclude에 해당하면 이 스킵 규칙 무시
            
            # require 키워드 체크 (있으면 반드시 동반해야 함)
            if "require_keywords" in rule:
                has_required = False
                for rkw in rule["require_keywords"]:
                    if rkw in combined:
                        has_required = True
                        break
                if not has_required:
                    continue  # require 미충족 → 이 스킵 규칙 무시
            
            return HSCodeResult(
                hs_code=None,
                rule_name=rule["name"],
                confidence="skip",
                note=rule["note"],
            )
        
        # ===== 3단계: 미매칭 =====
        return HSCodeResult(
            hs_code=None,
            rule_name="unknown",
            confidence="unknown",
            note=f"규칙 미매칭: cat='{category}' desc='{model_desc[:50]}'",
        )
    
    def extract_model_name(self, cell_value: str) -> Optional[str]:
        """
        A열에서 모델명만 추출 (쉼표 앞 부분)
        
        "LS-6UTPD-7MG, U/UTP Cat.6 Patch Cord, 24AWG, Grey, 7m, S"
        → "LS-6UTPD-7MG"
        
        ⚠️ 전체 모델명 매칭 (하이픈 뒤 끝까지)
        LS-5UTPD-2MG ≠ LS-5UTPD-2MR
        LS-1000H ≠ LS-1000HB
        """
        if not cell_value:
            return None
        
        text = str(cell_value).strip()
        
        # 쉼표로 분리하여 첫 번째 부분
        parts = text.split(",")
        model = parts[0].strip()
        
        # LS-, LSP-, LSN-, ZOT- 로 시작하는지 확인
        if re.match(r'^(LS|LSP|LSN|ZOT)-', model, re.IGNORECASE):
            return model
        
        # B열에 모델명이 있는 경우 (예: LS-68R)
        # 공백 없는 짧은 모델명
        if re.match(r'^(LS|LSP|LSN|ZOT)-\S+$', model, re.IGNORECASE):
            return model
        
        return None
    
    def is_erp_target(self, model_name: str) -> bool:
        """ERP 구매전표 입력 대상인지 확인"""
        if not model_name:
            return False
        return bool(re.match(r'^(LS|LSP|LSN|ZOT)-', model_name, re.IGNORECASE))


# ===== 테스트 =====
if __name__ == "__main__":
    engine = HSCodeEngine()
    
    test_cases = [
        # (카테고리, 모델명/설명, 기대 HS코드)
        ("U/UTP CAT.6 PATCH CORD", "LS-6UTPD-7MG, U/UTP Cat.6 Patch Cord", None),
        ("HDMI 2.0 CABLE", "LS-HDMI-HMM-20M, HDMI 1.4 Cable", "8544.42"),
        ("USB 2.0 SCREW CABLE", "LS-USB2.0-AMAF-S1M, USB 2.0 Screw Cable", "8544.42"),
        ("NETWORKS", "LSP-6IC-UJW, UTP Cat.6 Coupler", "8536.69"),
        ("RACK CABINET ACCESSORIES", "LS-BPA-S2UB, A-9, BLANK PANEL", "9403.99"),
        ("RACK CABINET", "LS-750H, 19\" Cabinet", None),
        ("NETWORKS", "LS-68R, HT-568B, Crimping Tool", "8203.20"),
        ("F/UTP CAT.5E SCREW CABLE", "LS-5FTPSD-BK2M, F/UTP Cat.5e Cable", None),
        ("U/UTP CAT.5E LAN CABLE", "LS-C5U305G, U/UTP Cat.5e Cable", None),
        ("A/V CABLE", "LS-ST-MM-5MN, 3.5 ST/M to 3.5 ST/M", "8544.42"),
        ("FIBER OPTIC CABLE", "LS-FMD-LCLC-10M, LC-LC, OM2", None),
        ("FLAT U/UTP CAT.6 PATCH CORD", "LS-F6UTPD-1MW, Flat Patch Cord", None),
        ("SLIM U/UTP CAT.6 PATCH CORD", "LS-SL6-3W, Slim Patch Cord", None),
        ("TYPE-C to RJ45 ETHERNET CABLE", "LS-UCLAN-10M, Type-C to RJ45", "8544.42"),
        ("DP 1.4 CABLE", "LS-DP14N-2M, DP 1.4 Cable", "8544.42"),
        ("DVI to HDMI CABLE", "LS-DVI19M-HDMI-1.5M, DVI to HDMI", "8544.42"),
    ]
    
    print("=" * 80)
    print("HS코드 엔진 테스트")
    print("=" * 80)
    
    all_pass = True
    for cat, desc, expected in test_cases:
        result = engine.match(cat, desc)
        status = "✅" if result.hs_code == expected else "❌"
        if result.hs_code != expected:
            all_pass = False
        
        print(f"{status} [{cat[:30]:30s}] → HS: {str(result.hs_code):10s} "
              f"(expect: {str(expected):10s}) rule: {result.rule_name}")
    
    print(f"\n{'모든 테스트 통과!' if all_pass else '⚠️ 실패한 테스트가 있습니다.'}")
    
    # 모델명 추출 테스트
    print("\n" + "=" * 80)
    print("모델명 추출 테스트")
    print("=" * 80)
    model_tests = [
        "LS-6UTPD-7MG, U/UTP Cat.6 Patch Cord, 24AWG, Grey, 7m, S",
        "LSP-7UTP-0.5M, U/UTP Cat.7 Patch Cord, 24AWG, CCA, Grey",
        "LS-68R",
        "LS-SHE-1000HG, The Shelf for LS-750H and LS-1000H",
        "BOR-2512012",
        None,
    ]
    for t in model_tests:
        m = engine.extract_model_name(t)
        erp = engine.is_erp_target(m) if m else False
        print(f"  Input: {str(t)[:60]:60s} → Model: {str(m):25s} ERP: {erp}")
