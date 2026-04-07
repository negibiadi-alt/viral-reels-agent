"""YouTube Shorts discovery via YouTube Data API v3.

Sadece API key gerekiyor — OAuth yok.
Ücretsiz: 10.000 kota/gün. Bir search = 100 kota.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from loguru import logger

from src.config import settings
from src.db.models import Platform
from src.discovery.apify_client import RawCandidate

YT_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YT_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"


def fetch_youtube_shorts(queries: list[str], limit: int = 20) -> list[RawCandidate]:
    if not settings.youtube_api_key:
        logger.warning("YOUTUBE_API_KEY not set — skipping YouTube discovery")
        return []

    results: list[RawCandidate] = []
    published_after = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    for query in queries:
        try:
            # Search for shorts
            r = httpx.get(YT_SEARCH_URL, params={
                "key": settings.youtube_api_key,
                "q": query,
                "part": "id",
                "type": "video",
                "videoDuration": "short",
                "order": "viewCount",
                "publishedAfter": published_after,
                "maxResults": min(limit, 50),
            }, timeout=30)
            r.raise_for_status()
            items = r.json().get("items", [])
            video_ids = [i["id"]["videoId"] for i in items if i.get("id", {}).get("videoId")]

            if not video_ids:
                continue

            # Get stats
            r2 = httpx.get(YT_VIDEOS_URL, params={
                "key": settings.youtube_api_key,
                "id": ",".join(video_ids),
                "part": "snippet,statistics,contentDetails",
            }, timeout=30)
            r2.raise_for_status()

            for video in r2.json().get("items", []):
                cand = _parse_video(video)
                if cand:
                    results.append(cand)

            logger.info("YouTube: {} shorts found for '{}'", len(video_ids), query)
        except Exception as exc:
            logger.exception("YouTube fetch failed for '{}': {}", query, exc)

    return results


def _parse_video(item: dict[str, Any]) -> RawCandidate | None:
    vid_id = item.get("id", "")
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})

    url = f"https://www.youtube.com/shorts/{vid_id}"
    views = int(stats.get("viewCount") or 0)
    likes = int(stats.get("likeCount") or 0)
    comments = int(stats.get("commentCount") or 0)

    published = snippet.get("publishedAt")
    posted_at = None
    if published:
        try:
            posted_at = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except ValueError:
            pass

    return RawCandidate(
        platform=Platform.YOUTUBE,
        source_url=url,
        thumbnail_url=(snippet.get("thumbnails") or {}).get("high", {}).get("url"),
        caption=snippet.get("title"),
        author=snippet.get("channelTitle"),
        views=views,
        likes=likes,
        comments=comments,
        posted_at=posted_at,
        raw=item,
    )
