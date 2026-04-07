"""Instagram Reels publisher.

Öncelik sırası:
  1. instagrapi (IG_USERNAME + IG_PASSWORD) — kolay kurulum
  2. Meta Graph API (IG_USER_ID + IG_ACCESS_TOKEN) — resmi yol
"""
from __future__ import annotations

import time

import httpx
from loguru import logger

from src.config import settings

GRAPH = "https://graph.facebook.com/v20.0"

_instagrapi_client = None


def _get_instagrapi_client():
    global _instagrapi_client
    if _instagrapi_client is not None:
        return _instagrapi_client
    from instagrapi import Client
    cl = Client()
    cl.login(settings.ig_username, settings.ig_password)
    _instagrapi_client = cl
    logger.info("instagrapi: logged in as {}", settings.ig_username)
    return cl


class IGPublishError(RuntimeError):
    pass


def publish_reel(video_path: str, caption: str) -> str:
    """End-to-end publish. Returns the published media id."""
    if settings.dry_run:
        logger.warning("DRY_RUN=true — skipping IG publish ({} chars caption)", len(caption))
        return "dry-run"

    # instagrapi yolu
    if settings.ig_username and settings.ig_password:
        cl = _get_instagrapi_client()
        media = cl.clip_upload(video_path, caption)
        logger.info("IG reel published via instagrapi: {}", media.pk)
        return str(media.pk)

    # Graph API yolu
    if settings.ig_user_id and settings.ig_access_token:
        container_id = _create_reel_container(video_path, caption)
        _wait_for_container(container_id)
        return _publish_container(container_id)

    raise IGPublishError("Instagram credentials missing. Set IG_USERNAME+IG_PASSWORD or IG_USER_ID+IG_ACCESS_TOKEN")


def _create_reel_container(video_url: str, caption: str) -> str:
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


def _wait_for_container(container_id: str, timeout: int = 300) -> None:
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


def _publish_container(container_id: str) -> str:
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
