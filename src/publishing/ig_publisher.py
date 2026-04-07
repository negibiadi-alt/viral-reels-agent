"""Instagram Reels publisher via Meta Graph API.

Flow:
  1. POST /{ig-user-id}/media    (media_type=REELS, video_url, caption)
  2. Poll GET /{container_id}?fields=status_code until FINISHED (or ERROR)
  3. POST /{ig-user-id}/media_publish (creation_id=container_id)

The `video_url` must be publicly reachable — in production the processed file
should be uploaded to object storage (S3, R2, ...). For dev use an ngrok tunnel
pointing at FastAPI's static file route, or swap in instagrapi as a fallback.
"""
from __future__ import annotations

import time

import httpx
from loguru import logger

from src.config import settings

GRAPH = "https://graph.facebook.com/v20.0"


class IGPublishError(RuntimeError):
    pass


def _require_creds() -> None:
    if not settings.ig_user_id or not settings.ig_access_token:
        raise IGPublishError("IG_USER_ID or IG_ACCESS_TOKEN missing")


def create_reel_container(video_url: str, caption: str) -> str:
    _require_creds()
    r = httpx.post(
        f"{GRAPH}/{settings.ig_user_id}/media",
        data={
            "media_type": "REELS",
            "video_url": video_url,
            "caption": caption,
            "access_token": settings.ig_access_token,
        },
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    container_id = data.get("id")
    if not container_id:
        raise IGPublishError(f"no container id: {data}")
    return container_id


def wait_for_container(container_id: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(
            f"{GRAPH}/{container_id}",
            params={"fields": "status_code", "access_token": settings.ig_access_token},
            timeout=30,
        )
        r.raise_for_status()
        status = r.json().get("status_code")
        logger.info("IG container {} status={}", container_id, status)
        if status == "FINISHED":
            return
        if status == "ERROR":
            raise IGPublishError(f"container {container_id} errored")
        time.sleep(5)
    raise IGPublishError("container timeout")


def publish_container(container_id: str) -> str:
    r = httpx.post(
        f"{GRAPH}/{settings.ig_user_id}/media_publish",
        data={"creation_id": container_id, "access_token": settings.ig_access_token},
        timeout=60,
    )
    r.raise_for_status()
    media_id = r.json().get("id")
    if not media_id:
        raise IGPublishError(f"publish failed: {r.json()}")
    return media_id


def publish_reel(video_url: str, caption: str) -> str:
    """End-to-end publish. Returns the published media id."""
    if settings.dry_run:
        logger.warning("DRY_RUN=true — skipping IG publish ({} chars caption)", len(caption))
        return "dry-run"
    container_id = create_reel_container(video_url, caption)
    wait_for_container(container_id)
    return publish_container(container_id)
