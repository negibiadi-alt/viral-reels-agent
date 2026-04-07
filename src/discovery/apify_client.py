"""Apify actor wrappers for Instagram & YouTube scraping.

Uses the synchronous apify-client. Two actors:
- apify/instagram-scraper   — hashtag / profile reel listings
- streamers/youtube-scraper — search-term based Shorts discovery
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from apify_client import ApifyClient
from loguru import logger

from src.config import settings
from src.db.models import Platform

IG_ACTOR = "apidojo/instagram-scraper-api"


@dataclass
class RawCandidate:
    platform: Platform
    source_url: str
    thumbnail_url: str | None
    caption: str | None
    author: str | None
    views: int
    likes: int
    comments: int
    posted_at: datetime | None
    raw: dict[str, Any]


def _client() -> ApifyClient:
    if not settings.apify_token:
        raise RuntimeError("APIFY_TOKEN is not set")
    return ApifyClient(settings.apify_token)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def fetch_instagram_reels(hashtags: list[str], limit: int = 30) -> list[RawCandidate]:
    """Scrape Instagram reels by hashtag via apidojo/instagram-scraper-api."""
    client = _client()
    run_input = {
        "hashtags": hashtags,
        "resultsLimit": limit,
        "mediaType": "reels",
    }
    logger.info("Apify IG scraper (apidojo): hashtags={} limit={}", hashtags, limit)
    run = client.actor(IG_ACTOR).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    return [_ig_to_candidate(item) for item in items if _is_reel(item)]


def _is_reel(item: dict[str, Any]) -> bool:
    return item.get("type") == "Video" or item.get("productType") == "clips"


def _ig_to_candidate(item: dict[str, Any]) -> RawCandidate:
    return RawCandidate(
        platform=Platform.INSTAGRAM,
        source_url=item.get("url") or f"https://instagram.com/p/{item.get('shortCode', '')}",
        thumbnail_url=item.get("displayUrl"),
        caption=item.get("caption"),
        author=(item.get("ownerUsername") or item.get("owner", {}).get("username")),
        views=int(item.get("videoViewCount") or item.get("videoPlayCount") or 0),
        likes=int(item.get("likesCount") or 0),
        comments=int(item.get("commentsCount") or 0),
        posted_at=_parse_dt(item.get("timestamp")),
        raw=item,
    )


def fetch_youtube_shorts(queries: list[str], limit: int = 30) -> list[RawCandidate]:
    """Scrape YouTube Shorts by search term."""
    client = _client()
    run_input = {
        "searchKeywords": queries,
        "maxResults": limit,
        "maxResultsShorts": limit,
        "uploadDate": "week",
    }
    logger.info("Apify YT scraper: queries={} limit={}", queries, limit)
    run = client.actor(YT_ACTOR).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    return [_yt_to_candidate(item) for item in items if _is_short(item)]


def _is_short(item: dict[str, Any]) -> bool:
    url = (item.get("url") or "").lower()
    duration = item.get("duration") or ""
    return "shorts/" in url or (isinstance(duration, str) and _duration_seconds(duration) <= 60)


def _duration_seconds(s: str) -> int:
    try:
        parts = [int(p) for p in s.split(":")]
    except ValueError:
        return 999
    secs = 0
    for p in parts:
        secs = secs * 60 + p
    return secs


def _yt_to_candidate(item: dict[str, Any]) -> RawCandidate:
    return RawCandidate(
        platform=Platform.YOUTUBE,
        source_url=item.get("url", ""),
        thumbnail_url=item.get("thumbnailUrl"),
        caption=item.get("title"),
        author=item.get("channelName"),
        views=int(item.get("viewCount") or 0),
        likes=int(item.get("likes") or 0),
        comments=int(item.get("commentsCount") or 0),
        posted_at=_parse_dt(item.get("date")),
        raw=item,
    )
