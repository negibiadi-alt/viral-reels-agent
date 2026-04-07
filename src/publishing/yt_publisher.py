"""YouTube Shorts publisher via YouTube Data API v3.

First run interactively (outside the scheduler) to create the OAuth token:
    python -m src.publishing.yt_publisher auth
"""
from __future__ import annotations

from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from loguru import logger

from src.config import settings

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


class YTPublishError(RuntimeError):
    pass


def _load_credentials() -> Credentials:
    token_path = settings.yt_token_file
    creds: Credentials | None = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json())
        return creds
    if not settings.yt_client_secrets_file.exists():
        raise YTPublishError(f"client secrets not found: {settings.yt_client_secrets_file}")
    flow = InstalledAppFlow.from_client_secrets_file(str(settings.yt_client_secrets_file), SCOPES)
    creds = flow.run_local_server(port=0)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())
    return creds


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


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "auth":
        _load_credentials()
        print("Auth complete. Token saved to:", settings.yt_token_file)
