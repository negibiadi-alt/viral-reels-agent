"""High-level discovery orchestration: topics -> scrapers -> filter -> DB."""
from __future__ import annotations

from datetime import datetime

from loguru import logger
from sqlalchemy import select

from src.config import settings
from src.db import SessionLocal
from src.db.models import Candidate, CandidateStatus, Platform, Topic
from src.discovery.apify_client import RawCandidate, fetch_instagram_reels
from src.discovery.youtube_client import fetch_youtube_shorts
from src.discovery.tiktok_client import fetch_tiktok_videos
from src.discovery.viral_filter import rank_and_filter


def _keywords_for(topic: Topic) -> list[str]:
    if topic.keywords:
        return [k.strip() for k in topic.keywords.split(",") if k.strip()]
    return [topic.name]


def discover_for_topic(topic: Topic) -> list[RawCandidate]:
    keywords = _keywords_for(topic)
    hashtags = [k.lstrip("#") for k in keywords]
    raw: list[RawCandidate] = []

    # TikTok (ücretsiz, en güvenilir)
    try:
        raw.extend(fetch_tiktok_videos(keywords, limit=20))
    except Exception as exc:
        logger.exception("TikTok scrape failed for {}: {}", topic.name, exc)

    # YouTube Shorts (ücretsiz API key ile)
    try:
        raw.extend(fetch_youtube_shorts(keywords, limit=20))
    except Exception as exc:
        logger.exception("YouTube scrape failed for {}: {}", topic.name, exc)

    # Instagram (Apify, yedek)
    if settings.apify_token:
        try:
            raw.extend(fetch_instagram_reels(hashtags, limit=20))
        except Exception as exc:
            logger.exception("IG scrape failed for {}: {}", topic.name, exc)

    logger.info("Topic '{}': {} raw candidates before filter", topic.name, len(raw))
    return rank_and_filter(raw, top_n=settings.daily_candidates * 3)


def _exists(session, source_url: str) -> bool:
    return session.execute(
        select(Candidate.id).where(Candidate.source_url == source_url)
    ).first() is not None


def run_discovery() -> list[Candidate]:
    session = SessionLocal()
    inserted: list[Candidate] = []
    try:
        topics = session.execute(select(Topic).where(Topic.active.is_(True))).scalars().all()
        if not topics:
            logger.warning("No active topics — add one first.")
            return []

        all_raw: list[tuple[Topic, RawCandidate]] = []
        for topic in topics:
            logger.info("Discovering for topic '{}'", topic.name)
            for rc in discover_for_topic(topic):
                all_raw.append((topic, rc))
            topic.last_run_at = datetime.utcnow()

        from src.discovery.viral_filter import viral_score
        all_raw.sort(key=lambda tr: viral_score(tr[1]), reverse=True)

        for topic, rc in all_raw:
            if len(inserted) >= settings.daily_candidates:
                break
            if _exists(session, rc.source_url):
                continue
            cand = Candidate(
                topic_id=topic.id,
                platform=rc.platform,
                source_url=rc.source_url,
                thumbnail_url=rc.thumbnail_url,
                caption=rc.caption,
                author=rc.author,
                views=rc.views,
                likes=rc.likes,
                comments=rc.comments,
                posted_at=rc.posted_at,
                metrics={"engagement_rate": (rc.likes + rc.comments) / max(rc.views, 1)},
                status=CandidateStatus.PENDING,
            )
            session.add(cand)
            inserted.append(cand)

        session.commit()
        for c in inserted:
            session.refresh(c)
        logger.info("Discovery done — {} new candidates", len(inserted))
        return inserted
    finally:
        session.close()
