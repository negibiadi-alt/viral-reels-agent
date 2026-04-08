"""TikTok viral content discovery via Omkar Cloud TikTok Scraper.

Ücretsiz: 5.000 istek/ay — kayıt ve kredi kartı gerektirmez.
https://github.com/omkarcloud/tiktok-scraper
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
from loguru import logger

from src.config import settings
from src.db.models import Platform
from src.discovery.apify_client import RawCandidate

BASE_URL = "https://api.omkar.cloud/tiktok"


def fetch_tiktok_videos(keywords: list[str], limit: int = 20) -> list[RawCandidate]:
    if not settings.omkar_api_key:
        logger.warning("OMKAR_API_KEY not set — skipping TikTok discovery")
        return []

    results: list[RawCandidate] = []
    headers = {"x-api-key": settings.omkar_api_key}

    for keyword in keywords:
        try:
            r = httpx.get(
                f"{BASE_URL}/search/videos",
                params={"keyword": keyword, "count": min(limit, 20), "region": "TR"},
                headers=headers,
                timeout=30,
            )
            r.raise_for_status()
            videos = r.json().get("data", {}).get("videos", [])
            for v in videos:
                cand = _parse_video(v)
                if cand:
                    results.append(cand)
            logger.info("TikTok: {} videos found for '{}'", len(videos), keyword)
        except Exception as exc:
            logger.exception("TikTok fetch failed for '{}': {}", keyword, exc)

    return results


def fetch_tiktok_trending(limit: int = 20) -> list[RawCandidate]:
    """Trending feed — topic-agnostic, call once per discovery batch."""
    if not settings.omkar_api_key:
        return []
    headers = {"x-api-key": settings.omkar_api_key}
    try:
        r = httpx.get(f"{BASE_URL}/trending", headers=headers, timeout=30)
        r.raise_for_status()
        videos = r.json().get("data", {}).get("videos", [])
        results = [c for v in videos[:limit] if (c := _parse_video(v))]
        logger.info("TikTok: {} trending videos fetched", len(results))
        return results
    except Exception as exc:
        logger.warning("TikTok trending failed: {}", exc)
        return []


def _parse_video(item: dict[str, Any]) -> RawCandidate | None:
    video_id = item.get("id") or item.get("video_id", "")
    if not video_id:
        return None

    author = item.get("author", {})
    stats = item.get("stats", {}) or item.get("statistics", {})

    url = f"https://www.tiktok.com/@{author.get('uniqueId', 'unknown')}/video/{video_id}"
    views = int(stats.get("playCount") or stats.get("play_count") or 0)
    likes = int(stats.get("diggCount") or stats.get("like_count") or 0)
    comments = int(stats.get("commentCount") or stats.get("comment_count") or 0)

    create_time = item.get("createTime") or item.get("create_time")
    posted_at = None
    if create_time:
        try:
            posted_at = datetime.fromtimestamp(int(create_time), tz=timezone.utc)
        except (ValueError, OSError):
            pass

    return RawCandidate(
        platform=Platform.TIKTOK,
        source_url=url,
        thumbnail_url=item.get("video", {}).get("cover") or item.get("cover"),
        caption=item.get("desc") or item.get("description"),
        author=author.get("uniqueId") or author.get("nickname"),
        views=views,
        likes=likes,
        comments=comments,
        posted_at=posted_at,
        raw=item,
    )
