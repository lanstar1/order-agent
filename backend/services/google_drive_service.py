"""
Google Drive 파일 업로드 서비스 (Service Account 인증)
- CS/RMA 첨부파일을 Google Drive에 업로드
- Render 배포 시 파일 유실 방지
"""
import json
import logging
import httpx
from config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_CS_FOLDER_ID

logger = logging.getLogger(__name__)

# 서비스 계정 토큰 캐시
_token_cache = {"access_token": "", "expires_at": 0}


def _is_configured() -> bool:
    """서비스 계정이 설정되었는지 확인"""
    return bool(GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_CS_FOLDER_ID)


async def _get_access_token() -> str:
    """
    서비스 계정 JSON에서 JWT를 생성하여 OAuth2 access_token 획득.
    google-auth 라이브러리 없이 직접 JWT 서명.
    """
    import time
    import base64
    import hashlib
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    now = int(time.time())

    # 캐시된 토큰이 유효하면 재사용
    if _token_cache["access_token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["access_token"]

    sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    client_email = sa_info["client_email"]
    private_key_pem = sa_info["private_key"]

    # JWT Header
    header = base64.urlsafe_b64encode(json.dumps({
        "alg": "RS256", "typ": "JWT"
    }).encode()).rstrip(b"=").decode()

    # JWT Claim
    claim = base64.urlsafe_b64encode(json.dumps({
        "iss": client_email,
        "scope": "https://www.googleapis.com/auth/drive",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode()).rstrip(b"=").decode()

    # Sign
    message = f"{header}.{claim}".encode()
    private_key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    signature = private_key.sign(message, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()

    jwt_token = f"{header}.{claim}.{sig_b64}"

    # Exchange JWT for access token
    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt_token,
            },
            timeout=10,
        )
        r.raise_for_status()
        token_data = r.json()

    _token_cache["access_token"] = token_data["access_token"]
    _token_cache["expires_at"] = now + token_data.get("expires_in", 3600)

    return _token_cache["access_token"]


async def upload_to_drive(
    file_content: bytes,
    filename: str,
    mime_type: str,
    subfolder_name: str = "",
) -> dict:
    """
    Google Drive에 파일 업로드.
    subfolder_name이 있으면 CS_FOLDER 하위에 해당 폴더를 찾거나 생성하여 업로드.

    Returns: {"file_id": str, "file_url": str, "web_view_link": str}
    """
    if not _is_configured():
        raise RuntimeError("Google Service Account 또는 CS 폴더 ID가 설정되지 않았습니다.")

    access_token = await _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    parent_id = GOOGLE_CS_FOLDER_ID

    # 하위 폴더 생성/조회 (티켓별 폴더)
    if subfolder_name:
        parent_id = await _get_or_create_folder(subfolder_name, GOOGLE_CS_FOLDER_ID, headers)

    # Multipart upload
    import io
    boundary = "---cs-upload-boundary---"
    metadata = json.dumps({
        "name": filename,
        "parents": [parent_id],
    })

    body = (
        f"--{boundary}\r\n"
        f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{metadata}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode() + file_content + f"\r\n--{boundary}--\r\n".encode()

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink",
            headers={
                **headers,
                "Content-Type": f"multipart/related; boundary={boundary}",
            },
            content=body,
            timeout=60,
        )
        if r.status_code != 200:
            error_body = ""
            try:
                error_body = r.text[:500]
            except Exception:
                pass
            logger.error(f"[GoogleDrive] 업로드 실패 ({r.status_code}): {error_body}")
            r.raise_for_status()
        data = r.json()

    file_id = data["id"]
    web_view_link = data.get("webViewLink", f"https://drive.google.com/file/d/{file_id}/view")

    # 파일을 '링크가 있는 모든 사용자'에게 읽기 권한 부여
    await _set_public_readable(file_id, headers)

    # 직접 표시/다운로드 가능한 URL
    direct_url = f"https://drive.google.com/uc?id={file_id}"

    logger.info(f"[GoogleDrive] 업로드 완료: {filename} → {file_id}")
    return {
        "file_id": file_id,
        "file_url": direct_url,
        "web_view_link": web_view_link,
    }


async def _get_or_create_folder(folder_name: str, parent_id: str, headers: dict) -> str:
    """Drive에서 폴더를 찾거나 없으면 생성"""
    async with httpx.AsyncClient() as client:
        # 기존 폴더 검색
        r = await client.get(
            "https://www.googleapis.com/drive/v3/files",
            params={
                "q": f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
                "fields": "files(id)",
                "pageSize": 1,
            },
            headers=headers,
            timeout=10,
        )
        r.raise_for_status()
        files = r.json().get("files", [])
        if files:
            return files[0]["id"]

        # 폴더 생성
        r = await client.post(
            "https://www.googleapis.com/drive/v3/files",
            headers={**headers, "Content-Type": "application/json"},
            json={
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            },
            timeout=10,
        )
        r.raise_for_status()
        new_id = r.json()["id"]
        logger.info(f"[GoogleDrive] 폴더 생성: {folder_name} → {new_id}")
        return new_id


async def _set_public_readable(file_id: str, headers: dict):
    """파일을 링크가 있는 모든 사용자에게 읽기 권한 부여"""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions",
                headers={**headers, "Content-Type": "application/json"},
                json={"type": "anyone", "role": "reader"},
                timeout=10,
            )
    except Exception as e:
        logger.warning(f"[GoogleDrive] 공개 권한 설정 실패 (file={file_id}): {e}")


async def delete_from_drive(file_id: str) -> bool:
    """Drive에서 파일 삭제"""
    if not _is_configured():
        return False
    try:
        access_token = await _get_access_token()
        async with httpx.AsyncClient() as client:
            r = await client.delete(
                f"https://www.googleapis.com/drive/v3/files/{file_id}",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
            return r.status_code in (200, 204)
    except Exception as e:
        logger.warning(f"[GoogleDrive] 파일 삭제 실패 (file={file_id}): {e}")
        return False
