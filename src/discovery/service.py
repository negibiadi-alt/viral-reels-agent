"""High-level discovery orchestration: topics -> scrapers -> filter -> DB."""
from __future__ import annotations

from datetime import datetime

from loguru import logger
from sqlalchemy import select

from src.config import settings
from src.db import SessionLocal
from src.db.models import Candidate, CandidateStatus, Topic
from src.discovery.apify_client import RawCandidate
from src.discovery.tiktok_client import fetch_tiktok_trending, fetch_tiktok_videos
from src.discovery.youtube_client import fetch_youtube_shorts
from src.discovery.viral_filter import rank_and_filter, viral_score


def _keywords_for(topic: Topic) -> list[str]:
    if topic.keywords:
        return [k.strip() for k in topic.keywords.split(",") if k.strip()]
    return [topic.name]


def discover_for_topic(topic: Topic) -> list[RawCandidate]:
    keywords = _keywords_for(topic)
    raw: list[RawCandidate] = []

    try:
        raw.extend(fetch_tiktok_videos(keywords, limit=20))
    except Exception as exc:
        logger.exception("TikTok scrape failed for {}: {}", topic.name, exc)

    try:
        raw.extend(fetch_youtube_shorts(keywords, limit=20))
    except Exception as exc:
        logger.exception("YouTube scrape failed for {}: {}", topic.name, exc)

    logger.info("Topic '{}': {} raw candidates before filter", topic.name, len(raw))
    return rank_and_filter(raw, top_n=settings.daily_candidates * 3)


def _exists(session, source_url: str) -> bool:
    return session.execute(
        select(Candidate.id).where(Candidate.source_url == source_url)
    ).first() is not None


def _raw_to_candidate(topic: Topic, rc: RawCandidate) -> Candidate:
    return Candidate(
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


def discover_and_save_for_topic(topic_id: int) -> list[Candidate]:
    """Discover + save candidates for a single topic. Used by Telegram bot."""
    session = SessionLocal()
    inserted: list[Candidate] = []
    try:
        topic = session.get(Topic, topic_id)
        if not topic:
            return []
        raw = discover_for_topic(topic)
        raw.sort(key=viral_score, reverse=True)
        for rc in raw:
            if _exists(session, rc.source_url):
                continue
            cand = _raw_to_candidate(topic, rc)
            session.add(cand)
            inserted.append(cand)
        topic.last_run_at = datetime.utcnow()
        session.commit()
        for c in inserted:
            session.refresh(c)
        logger.info("Topic '{}': {} new candidates saved", topic.name, len(inserted))
        return inserted
    finally:
        session.close()


def run_discovery() -> list[Candidate]:
    """Run discovery for every active topic and persist pending candidates."""
    session = SessionLocal()
    inserted: list[Candidate] = []
    try:
        topics = session.execute(select(Topic).where(Topic.active.is_(True))).scalars().all()
        if not topics:
            logger.warning("No active topics — add one first.")
            return []

        all_raw: list[tuple[Topic, RawCandidate]] = []

        # Trending bir kez çek, tüm topic'lere ait say
        trending = fetch_tiktok_trending(limit=20)
        for rc in trending:
            all_raw.append((topics[0], rc))

        for topic in topics:
            logger.info("Discovering for topic '{}'", topic.name)
            for rc in discover_for_topic(topic):
                all_raw.append((topic, rc))
            topic.last_run_at = datetime.utcnow()

        all_raw.sort(key=lambda tr: viral_score(tr[1]), reverse=True)

        for topic, rc in all_raw:
            if len(inserted) >= settings.daily_candidates:
                break
            if _exists(session, rc.source_url):
                continue
            cand = _raw_to_candidate(topic, rc)
            session.add(cand)
            inserted.append(cand)

        session.commit()
        for c in inserted:
            session.refresh(c)
        logger.info("Discovery done — {} new candidates", len(inserted))
        return inserted
    finally:
        session.close()
