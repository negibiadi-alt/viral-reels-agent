"""YouTube Shorts publisher via YouTube Data API v3.

Render'da tarayıcı açılamaz. Token'ı bootstrap scripti ile al:
    python scripts/bootstrap_yt_auth.py
Çıkan JSON'u Render'da YT_TOKEN_JSON env var'ına yapıştır.
"""
from __future__ import annotations

import json
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from loguru import logger

from src.config import settings

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


class YTPublishError(RuntimeError):
    pass


def _load_credentials() -> Credentials:
    creds: Credentials | None = None

    # 1. Env var'dan oku (Render production)
    if settings.yt_token_json:
        creds = Credentials.from_authorized_user_info(
            json.loads(settings.yt_token_json), SCOPES
        )

    # 2. Dosyadan oku (lokal geliştirme)
    elif settings.yt_token_file.exists():
        creds = Credentials.from_authorized_user_file(str(settings.yt_token_file), SCOPES)

    else:
        raise YTPublishError(
            "YouTube token bulunamadı. "
            "Laptop'ta çalıştır: python scripts/bootstrap_yt_auth.py"
        )

    if creds.valid:
        return creds

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        logger.info("YouTube token yenilendi.")
        return creds

    raise YTPublishError("YouTube token geçersiz ve refresh token yok. Bootstrap'ı tekrar yap.")


def publish_short(video_path: Path, title: str, description: str, tags: list[str] | None = None) -> str:
    """Upload a video as a YouTube Short. Returns the video id."""
    if settings.dry_run:
        logger.warning("DRY_RUN=true — skipping YT upload ({})", video_path)
        return "dry-run"

    creds = _load_credentials()
    youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)

    body = {
        "snippet": {
            "title": title[:100],
            "description": description[:4900],
            "tags": (tags or []) + ["Shorts"],
            "categoryId": "22",
        },
        "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False},
    }
    media = MediaFileUpload(str(video_path), mimetype="video/mp4", resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            logger.info("YT upload progress {:.0%}", status.progress())

    video_id = response.get("id")
    if not video_id:
        raise YTPublishError(f"upload failed: {response}")
    logger.info("YT published: https://youtube.com/shorts/{}", video_id)
    return video_id
