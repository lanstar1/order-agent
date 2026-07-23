#!/usr/bin/env python3
"""파일 위치 해석 — 드라이브 API → 로컬 동기화 폴더 → 폴더 URL 순.

manifest 에는 일부러 file ID 가 없다(재업로드하면 바뀌므로). 안정적인 폴더 ID + 파일명만
있어서 링크는 런타임에 만들어야 한다. 자격증명이 없는 환경이 정상 상태이므로 API 단계는
절대 예외로 죽지 않고 조용히 다음 단으로 내려간다.

문서/ 는 하위 폴더가 없는 평면 폴더지만 KC/ 는 모델별 하위 폴더가 있다 — 같은 코드로
처리하면 파일을 못 찾으므로 경로를 나눠 놓았다.
"""
from __future__ import annotations

import os
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from . import loader

API_LINK, LOCAL_PATH, FOLDER_FALLBACK, MISSING = 'api_link', 'local_path', 'folder_fallback', 'missing'


@dataclass
class DriveRef:
    """파일 위치 해석 결과. status 로 어느 백엔드가 답했는지 항상 알 수 있다."""
    status: str
    filename: str = ''
    folder_id: str = ''
    folder_url: str = ''
    url: str = ''            # 파일 직링크(api_link 일 때만)
    path: str = ''           # 로컬 절대경로(local_path 일 때만)
    note: str = ''

    @property
    def ok(self) -> bool:
        return self.status in (API_LINK, LOCAL_PATH)


def _nfc(s: str) -> str:
    # macOS 는 파일명을 NFD 로 돌려준다. manifest 는 NFC 라 정규화 없이 비교하면 전건 오탐.
    return unicodedata.normalize('NFC', s or '')


def find_drive_root() -> Optional[str]:
    """~/Library/CloudStorage 를 훑어 동기화된 '랜스타_인증자료' 루트를 찾는다."""
    if loader.DRIVE_ROOT and os.path.isdir(loader.DRIVE_ROOT):
        return loader.DRIVE_ROOT
    base = os.path.expanduser('~/Library/CloudStorage')
    if not os.path.isdir(base):
        return None
    try:
        entries = sorted(os.listdir(base))
    except OSError:
        return None
    for d in entries:
        if not d.startswith('GoogleDrive-'):
            continue
        for my in ('My Drive', '내 드라이브'):
            p = os.path.join(base, d, my, loader.DRIVE_ROOT_NAME)
            if os.path.isdir(p):
                return p
    return None


def _find_in_dir(directory: str, filename: str) -> Optional[str]:
    if not directory or not os.path.isdir(directory):
        return None
    direct = os.path.join(directory, filename)
    if os.path.exists(direct):
        return direct
    target = _nfc(filename)
    try:
        for name in os.listdir(directory):
            if _nfc(name) == target:
                return os.path.join(directory, name)
    except OSError:
        return None
    return None


class DriveResolver:
    """문서/KC 파일의 전달 경로를 해석한다."""

    def __init__(self, kb: Optional[loader.KnowledgeBase] = None,
                 use_api: bool = True, drive_root: Optional[str] = None) -> None:
        self.drive_root = drive_root or find_drive_root()
        self.repo_cert_dir = loader.CERT_DIR
        self._use_api = use_api and os.environ.get('CERT_KB_DRIVE_DISABLE_API') != '1'
        self._service: Any = None
        self._api_tried = False

    # -- (1) Drive API -------------------------------------------------
    def _api(self) -> Any:
        """google 라이브러리·자격증명이 없으면 None. 실패는 폴백 신호일 뿐 오류가 아니다."""
        if self._api_tried or not self._use_api:
            return self._service
        self._api_tried = True
        try:
            from googleapiclient.discovery import build  # type: ignore

            creds = None
            token = os.environ.get('CERT_KB_DRIVE_TOKEN')
            sa = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            scopes = ['https://www.googleapis.com/auth/drive.readonly']
            if token and os.path.exists(token):
                from google.oauth2.credentials import Credentials  # type: ignore
                creds = Credentials.from_authorized_user_file(token, scopes)
            elif sa and os.path.exists(sa):
                from google.oauth2 import service_account  # type: ignore
                creds = service_account.Credentials.from_service_account_file(sa, scopes=scopes)
            if creds is None:
                return None
            self._service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        except Exception:                                   # noqa: BLE001 - 폴백이 정상 경로
            self._service = None
        return self._service

    def _api_find(self, parent_id: str, name: str, folder: bool = False) -> Optional[Dict[str, str]]:
        svc = self._api()
        if not svc or not parent_id or not name:
            return None
        # drive.lookup 은 v2 문법(parentId/title)이라 v3 로 재조립해야 한다.
        # 파일명에 ' 나 \\ 가 들어오면 쿼리가 깨져 400 이 난다(현재 데이터엔 없지만 잠복).
        esc = name.replace('\\', '\\\\').replace("'", "\\'")
        q = f"'{parent_id}' in parents and name = '{esc}' and trashed = false"
        if folder:
            q += " and mimeType = 'application/vnd.google-apps.folder'"
        try:
            r = svc.files().list(q=q, fields='files(id,name,webViewLink)',
                                 pageSize=5, supportsAllDrives=True,
                                 includeItemsFromAllDrives=True).execute()
            files = r.get('files') or []
            return files[0] if files else None
        except Exception:                                   # noqa: BLE001
            return None

    # -- (2)(3) 폴백 ---------------------------------------------------
    def _folder_url(self, folder_id: str) -> str:
        return f'https://drive.google.com/drive/folders/{folder_id}' if folder_id else ''

    def resolve_document(self, doc: Any) -> DriveRef:
        """인증자료 문서(문서/ 평면 폴더)의 위치를 해석한다.

        doc: manifest 레코드(dict) 또는 search.DocView.
        """
        rec = doc if isinstance(doc, dict) else None
        if rec is not None:
            drive = rec.get('drive') or {}
            filename = drive.get('filename') or os.path.basename(rec.get('path', ''))
            folder_id = drive.get('folder_id') or ''
            rel = rec.get('path') or ''
        else:
            filename = getattr(doc, 'filename', '') or os.path.basename(
                getattr(doc, 'path', '') or '')
            folder_id = getattr(doc, 'folder_id', '')
            rel = getattr(doc, 'path', '')
        folder = (rel.split('/', 1)[0] if '/' in rel else '문서') or '문서'
        folder_url = self._folder_url(folder_id)

        if not filename:
            return DriveRef(status=MISSING, folder_id=folder_id, folder_url=folder_url,
                            note='파일명 정보가 없습니다')

        hit = self._api_find(folder_id, filename)
        if hit:
            return DriveRef(status=API_LINK, filename=filename, folder_id=folder_id,
                            folder_url=folder_url,
                            url=hit.get('webViewLink')
                            or f"https://drive.google.com/file/d/{hit['id']}/view")

        for base in [os.path.join(self.drive_root, folder) if self.drive_root else '',
                     os.path.join(self.repo_cert_dir, folder)]:
            p = _find_in_dir(base, filename)
            if p:
                return DriveRef(status=LOCAL_PATH, filename=filename, folder_id=folder_id,
                                folder_url=folder_url, path=p)

        return DriveRef(status=FOLDER_FALLBACK, filename=filename, folder_id=folder_id,
                        folder_url=folder_url,
                        note='드라이브 폴더에서 파일명으로 찾아주십시오')

    def resolve_kc(self, rec: Dict[str, Any]) -> DriveRef:
        """KC 자료의 위치를 해석한다.

        완료자료 zip 은 KC/{모델}/ 하위에 있어 API 조회가 2단계다.
        필증 PDF 만 있는 2건은 KC_필증PDF만/ 평면 폴더에 있다(note 문구는 잘못돼 있으니 무시).
        """
        drive = rec.get('drive') or {}
        if not drive:
            return DriveRef(status=MISSING,
                            note='완료자료를 수신한 기록이 없어 전달할 파일이 없습니다')
        filename = drive.get('filename') or ''
        parent_id = drive.get('parent_folder_id') or ''
        folder = drive.get('folder') or ''          # 'KC/LS-OVERC' 또는 'KC_필증PDF만'
        folder_url = drive.get('folder_url') or self._folder_url(parent_id)
        nested = folder.startswith('KC/')

        hit = None
        if nested:
            model_dir = self._api_find(parent_id, folder.split('/', 1)[1], folder=True)
            if model_dir:
                hit = self._api_find(model_dir['id'], filename)
        else:
            hit = self._api_find(parent_id, filename)
        if hit:
            return DriveRef(status=API_LINK, filename=filename, folder_id=parent_id,
                            folder_url=folder_url,
                            url=hit.get('webViewLink')
                            or f"https://drive.google.com/file/d/{hit['id']}/view")

        # 저장소의 KC 실물은 .gitignore 대상이라 다른 머신에는 없다 — 동기화 폴더가 정본.
        candidates: List[str] = []
        if self.drive_root:
            candidates.append(os.path.join(self.drive_root, folder))
        repo_kc = os.path.join(loader.KC_DIR, '모델별' if nested else '_필증PDF만')
        candidates.append(os.path.join(repo_kc, folder.split('/', 1)[1]) if nested else repo_kc)
        for base in candidates:
            p = _find_in_dir(base, filename)
            if p:
                return DriveRef(status=LOCAL_PATH, filename=filename, folder_id=parent_id,
                                folder_url=folder_url, path=p)

        return DriveRef(status=FOLDER_FALLBACK, filename=filename, folder_id=parent_id,
                        folder_url=folder_url,
                        note=f"드라이브 '{folder}' 폴더에서 {filename} 를 찾아주십시오")


_shared: Optional['DriveResolver'] = None


def get_resolver() -> 'DriveResolver':
    """프로세스당 하나. 모듈마다 따로 만들면 ~/Library/CloudStorage 스캔이 중복된다."""
    global _shared
    if _shared is None:
        _shared = DriveResolver()
    return _shared
