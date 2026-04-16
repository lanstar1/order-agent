"""
구글 시트 동기화 서비스 (index.ts 변환)
JWT 인증으로 구글 시트 API에 접근하여 트렌드 데이터를 동기화
"""
import os
import json
import time
import re
import base64
import httpx
from datetime import datetime, timedelta
from typing import Optional, Any
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

from db.database import get_connection
from services.trend_constants import (
    list_monthly_periods,
    normalize_spreadsheet_id,
    build_sheet_url,
    now_kst_str,
)

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
SHEETS_API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"


def _get_google_credentials() -> tuple[str, str]:
    """환경변수에서 구글 서비스 계정 자격정보 추출"""
    email = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_EMAIL", "").strip()
    private_key = os.getenv("GOOGLE_SHEETS_PRIVATE_KEY", "").strip()

    # 따옴표 제거
    if (email.startswith('"') and email.endswith('"')) or (
        email.startswith("'") and email.endswith("'")
    ):
        email = email[1:-1]
    if (private_key.startswith('"') and private_key.endswith('"')) or (
        private_key.startswith("'") and private_key.endswith("'")
    ):
        private_key = private_key[1:-1]

    # \n 이스케이프 처리
    private_key = private_key.replace("\\n", "\n").strip()

    if not email or not private_key:
        raise ValueError(
            "GOOGLE_SHEETS_SERVICE_ACCOUNT_EMAIL와 GOOGLE_SHEETS_PRIVATE_KEY 필요"
        )

    return email, private_key


def _base64_url_encode(data: bytes | str) -> str:
    """base64 URL-safe 인코딩"""
    if isinstance(data, str):
        data = data.encode("utf-8")
    encoded = base64.urlsafe_b64encode(data).decode("utf-8")
    return encoded.rstrip("=")


def _sign_jwt(input_str: str, private_key_pem: str) -> str:
    """RS256으로 JWT 서명"""
    # PEM 형식 제거
    pem = (
        private_key_pem.replace("-----BEGIN PRIVATE KEY-----", "")
        .replace("-----END PRIVATE KEY-----", "")
        .replace(" ", "")
        .replace("\n", "")
        .replace("\r", "")
    )

    # base64 디코드
    key_bytes = base64.b64decode(pem)

    # 개인키 로드
    private_key = serialization.load_der_private_key(
        key_bytes, password=None, backend=default_backend()
    )

    # 서명
    signature = private_key.sign(
        input_str.encode("utf-8"),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )

    return _base64_url_encode(signature)


async def _get_google_access_token() -> str:
    """구글 OAuth 토큰 획득 (JWT Bearer)"""
    email, private_key = _get_google_credentials()

    issued_at = int(time.time())
    exp_time = issued_at + 3600

    # JWT 헤더
    header = json.dumps({"alg": "RS256", "typ": "JWT"})
    header_b64 = _base64_url_encode(header)

    # JWT 페이로드
    payload = json.dumps(
        {
            "iss": email,
            "scope": SHEETS_SCOPE,
            "aud": GOOGLE_TOKEN_URL,
            "exp": exp_time,
            "iat": issued_at,
        }
    )
    payload_b64 = _base64_url_encode(payload)

    # 서명
    input_str = f"{header_b64}.{payload_b64}"
    signature = _sign_jwt(input_str, private_key)

    # JWT 조립
    assertion = f"{input_str}.{signature}"

    # 토큰 요청
    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            },
        )

    if response.status_code != 200:
        raise Exception(
            f"구글 토큰 교환 실패: {response.status_code} {response.text}"
        )

    token_data = response.json()
    return token_data["access_token"]


async def _sheets_api_fetch(
    access_token: str, method: str, url: str, body: Optional[dict] = None
) -> Any:
    """구글 시트 API 호출"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        if method == "GET":
            response = await client.get(url, headers=headers)
        elif method == "POST":
            response = await client.post(url, headers=headers, json=body)
        elif method == "PUT":
            response = await client.put(url, headers=headers, json=body)
        else:
            raise ValueError(f"Unsupported method: {method}")

    if response.status_code == 204:
        return None

    if not response.is_success:
        raise Exception(
            f"구글 시트 API 실패: {response.status_code} {response.text}"
        )

    return response.json()


def _sanitize_sheet_tab_name(value: str) -> str:
    """시트 탭 이름 정규화 (특수문자 제거)"""
    # 특수문자 제거
    sanitized = re.sub(r"[\\/?*\[\]:]", "-", value).strip()
    # 90자 제한
    sanitized = sanitized[:90]
    return sanitized or "sheet"


def _build_sheet_tabs(profile: dict, snapshots: list[dict]) -> list[dict]:
    """트렌드 프로필을 Google Sheets 탭으로 변환"""
    periods = list_monthly_periods(profile["start_period"], profile["end_period"])

    # 스냅샷을 기간별로 그룹화
    snapshots_by_period = {}
    for snapshot in snapshots:
        period = snapshot["period"]
        if period not in snapshots_by_period:
            snapshots_by_period[period] = {}
        snapshots_by_period[period][snapshot["rank"]] = snapshot

    # meta_{slug} 탭
    meta_rows = [
        ["field", "value"],
        ["profile_id", profile["id"]],
        ["name", profile["name"]],
        ["category_path", profile["category_path"]],
        ["category_cid", str(profile["category_cid"])],
        ["time_unit", profile["time_unit"]],
        ["devices", ",".join(profile.get("devices", [])) or "all"],
        ["genders", ",".join(profile.get("genders", [])) or "all"],
        ["ages", ",".join(profile.get("ages", [])) or "all"],
        ["result_count", str(profile["result_count"])],
        [
            "exclude_brand_products",
            "yes" if profile.get("exclude_brand_products") else "no",
        ],
        ["custom_excluded_terms", ",".join(profile.get("custom_excluded_terms", []))],
        ["start_period", profile["start_period"]],
        ["end_period", profile["end_period"]],
        ["last_collected_period", profile.get("last_collected_period") or ""],
        ["last_synced_at", profile.get("last_synced_at") or ""],
        ["sheet_url", build_sheet_url(profile["spreadsheet_id"])],
    ]

    # raw_{slug} 탭 - 모든 스냅샷 (기간, 순위, 키워드, 장치, 성별, 나이, 수집시간)
    raw_rows = [
        ["period", "rank", "keyword", "category_path", "device", "gender", "age", "collected_at"],
    ]
    sorted_snapshots = sorted(
        snapshots, key=lambda x: (x["period"], x["rank"])
    )
    for snapshot in sorted_snapshots:
        raw_rows.append([
            snapshot["period"],
            str(snapshot["rank"]),
            snapshot["keyword"],
            snapshot.get("category_path", ""),
            ",".join(snapshot.get("devices", [])),
            ",".join(snapshot.get("genders", [])),
            ",".join(snapshot.get("ages", [])),
            snapshot["collected_at"],
        ])

    # matrix_{slug} 탭 - 피벗 테이블 (행: 순위, 열: 월별)
    matrix_rows = [["rank"] + periods]
    for rank in range(1, profile["result_count"] + 1):
        row = [str(rank)]
        for period in periods:
            keyword = (
                snapshots_by_period.get(period, {})
                .get(rank, {})
                .get("keyword", "")
            )
            row.append(keyword)
        matrix_rows.append(row)

    return [
        {
            "title": _sanitize_sheet_tab_name(f"meta_{profile['slug']}"),
            "rows": meta_rows,
        },
        {
            "title": _sanitize_sheet_tab_name(f"raw_{profile['slug']}"),
            "rows": raw_rows,
        },
        {
            "title": _sanitize_sheet_tab_name(f"matrix_{profile['slug']}"),
            "rows": matrix_rows,
        },
    ]


async def _sync_profile_sheets(
    access_token: str, profile: dict, snapshots: list[dict]
) -> str:
    """프로필을 구글 시트로 동기화"""
    spreadsheet_id = normalize_spreadsheet_id(profile["spreadsheet_id"])
    tabs = _build_sheet_tabs(profile, snapshots)

    # 기존 시트 조회
    spreadsheet = await _sheets_api_fetch(
        access_token, "GET", f"{SHEETS_API_BASE}/{spreadsheet_id}"
    )
    existing_titles = set()
    if spreadsheet and "sheets" in spreadsheet:
        for sheet in spreadsheet["sheets"]:
            if "properties" in sheet and "title" in sheet["properties"]:
                existing_titles.add(sheet["properties"]["title"])

    # 없는 탭 추가
    add_requests = []
    for tab in tabs:
        if tab["title"] not in existing_titles:
            add_requests.append({"addSheet": {"properties": {"title": tab["title"]}}})

    if add_requests:
        await _sheets_api_fetch(
            access_token,
            "POST",
            f"{SHEETS_API_BASE}/{spreadsheet_id}:batchUpdate",
            {"requests": add_requests},
        )

    # 모든 탭의 값 초기화
    clear_ranges = [f"{tab['title']}!A1:ZZ" for tab in tabs]
    await _sheets_api_fetch(
        access_token,
        "POST",
        f"{SHEETS_API_BASE}/{spreadsheet_id}/values:batchClear",
        {"ranges": clear_ranges},
    )

    # 각 탭에 데이터 쓰기
    for tab in tabs:
        range_notation = f"{tab['title']}!A1"
        await _sheets_api_fetch(
            access_token,
            "PUT",
            f"{SHEETS_API_BASE}/{spreadsheet_id}/values/{range_notation}?valueInputOption=RAW",
            {"values": tab["rows"]},
        )

    return build_sheet_url(spreadsheet_id)


async def sync_profile_to_sheets(profile_id: str) -> dict:
    """
    프로필의 모든 스냅샷을 Google Sheets로 동기화
    - meta_{slug}: 프로필 메타데이터
    - raw_{slug}: 모든 스냅샷 (기간, 순위, 키워드, 장치, 성별, 나이)
    - matrix_{slug}: 피벗 테이블 (순위 행 x 월별 열)

    성공 시 sync_status='synced', last_synced_at 업데이트
    실패 시 sync_status='failed'
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        # 프로필 로드
        cur.execute(
            "SELECT * FROM trend_profiles WHERE id = ?",
            (profile_id,),
        )
        profile_row = cur.fetchone()

        if not profile_row:
            return {
                "ok": False,
                "code": "TREND_PROFILE_NOT_FOUND",
                "message": "profileId에 해당하는 트렌드 프로필이 없습니다.",
            }

        profile = dict(profile_row)

        # 브랜드 제외 필터 적용 여부 확인
        exclude_brand = profile.get("exclude_brand_products", 0)

        # 스냅샷 로드 (brand_excluded 필터링)
        if exclude_brand:
            cur.execute(
                """SELECT * FROM trend_snapshots
                   WHERE profile_id = ? AND brand_excluded = 0
                   ORDER BY period ASC, rank ASC""",
                (profile_id,),
            )
        else:
            cur.execute(
                """SELECT * FROM trend_snapshots
                   WHERE profile_id = ?
                   ORDER BY period ASC, rank ASC""",
                (profile_id,),
            )

        snapshot_rows = cur.fetchall()
        snapshots = [dict(row) for row in snapshot_rows]

        # JSON 필드 파싱
        if profile.get("devices"):
            profile["devices"] = json.loads(profile["devices"]) if isinstance(profile["devices"], str) else []
        else:
            profile["devices"] = []

        if profile.get("genders"):
            profile["genders"] = json.loads(profile["genders"]) if isinstance(profile["genders"], str) else []
        else:
            profile["genders"] = []

        if profile.get("ages"):
            profile["ages"] = json.loads(profile["ages"]) if isinstance(profile["ages"], str) else []
        else:
            profile["ages"] = []

        if profile.get("custom_excluded_terms"):
            profile["custom_excluded_terms"] = (
                json.loads(profile["custom_excluded_terms"])
                if isinstance(profile["custom_excluded_terms"], str)
                else []
            )
        else:
            profile["custom_excluded_terms"] = []

        # 스냅샷 JSON 필드 파싱
        for snapshot in snapshots:
            if snapshot.get("devices_json"):
                snapshot["devices"] = (
                    json.loads(snapshot["devices_json"])
                    if isinstance(snapshot["devices_json"], str)
                    else []
                )
            else:
                snapshot["devices"] = []

            if snapshot.get("genders_json"):
                snapshot["genders"] = (
                    json.loads(snapshot["genders_json"])
                    if isinstance(snapshot["genders_json"], str)
                    else []
                )
            else:
                snapshot["genders"] = []

            if snapshot.get("ages_json"):
                snapshot["ages"] = (
                    json.loads(snapshot["ages_json"])
                    if isinstance(snapshot["ages_json"], str)
                    else []
                )
            else:
                snapshot["ages"] = []

        # 구글 토큰 획득
        access_token = await _get_google_access_token()

        # 시트 동기화
        sheet_url = await _sync_profile_sheets(access_token, profile, snapshots)

        # 프로필 업데이트 (sync_status, last_synced_at)
        now = now_kst_str()
        cur.execute(
            """UPDATE trend_profiles
               SET sync_status = ?, last_synced_at = ?, updated_at = ?
               WHERE id = ?""",
            ("synced", now, now, profile_id),
        )
        conn.commit()

        return {
            "ok": True,
            "sheetUrl": sheet_url,
        }

    except Exception as e:
        # 실패 시 sync_status='failed'
        try:
            cur.execute(
                """UPDATE trend_profiles
                   SET sync_status = ?, updated_at = ?
                   WHERE id = ?""",
                ("failed", now_kst_str(), profile_id),
            )
            conn.commit()
        except Exception:
            pass

        return {
            "ok": False,
            "code": "SYNC_FAILED",
            "message": str(e),
        }

    finally:
        conn.close()
