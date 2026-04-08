"""Approved-candidate pipeline.

Kaynak platform (TikTok/YouTube/Instagram) ne olursa olsun,
tüm onaylanan videolar YouTube Shorts'a yüklenir.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db import SessionLocal
from src.db.models import Candidate, CandidateStatus, Platform, ScheduledPost
from src.processing.video_processor import process_candidate
from src.publishing.yt_publisher import publish_short
from src.scheduler.peak_hours import next_available_slot


def _pick_approved(session: Session, limit: int = 50) -> list[Candidate]:
    return (
        session.execute(
            select(Candidate)
            .where(Candidate.status == CandidateStatus.APPROVED)
            .order_by(Candidate.discovered_at.asc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


def enqueue_approved() -> list[ScheduledPost]:
    """Process every approved candidate and place it on a YouTube peak slot."""
    session = SessionLocal()
    scheduled: list[ScheduledPost] = []
    try:
        for cand in _pick_approved(session):
            try:
                processed = process_candidate(session, cand)
            except Exception as exc:
                logger.exception("processing failed for {}: {}", cand.id, exc)
                cand.status = CandidateStatus.FAILED
                session.commit()
                continue

            # Kaynak ne olursa olsun hedef YouTube
            run_at = next_available_slot(session, Platform.YOUTUBE).replace(tzinfo=None)
            post = ScheduledPost(
                candidate_id=cand.id,
                platform=Platform.YOUTUBE,
                run_at=run_at,
                status="queued",
            )
            session.add(post)
            session.commit()
            session.refresh(post)
            scheduled.append(post)
            logger.info(
                "Candidate {} processed -> {}  @ {}",
                cand.id, processed.processed_path, run_at,
            )
        return scheduled
    finally:
        session.close()


def run_scheduled_post(post_id: int) -> None:
    """Actually publish a ScheduledPost to YouTube. Called by APScheduler at run_at."""
    session = SessionLocal()
    try:
        post = session.get(ScheduledPost, post_id)
        if post is None or post.status != "queued":
            return
        cand = session.get(Candidate, post.candidate_id)
        if cand is None:
            return

        processed_path = settings.processed_dir / f"{cand.id}.mp4"
        if not processed_path.exists():
            logger.error("Processed file missing: {}", processed_path)
            post.status = "failed"
            post.error = "processed file missing"
            session.commit()
            return

        caption = _build_caption(cand)

        try:
            video_id = publish_short(
                processed_path,
                title=(cand.caption or "Shorts")[:90],
                description=caption,
                tags=[],
            )
            post.published_url = f"https://youtube.com/shorts/{video_id}"
            post.status = "published"
            cand.status = CandidateStatus.PUBLISHED
            logger.info("Published to YouTube: {}", post.published_url)
        except Exception as exc:
            logger.exception("publish failed for post {}: {}", post_id, exc)
            post.status = "failed"
            post.error = str(exc)
        finally:
            session.commit()
    finally:
        session.close()


def _build_caption(cand: Candidate) -> str:
    base = (cand.caption or "").strip()
    credit = f"\n\n🎬 credit: @{cand.author}" if cand.author else ""
    return f"{base}{credit}"[:2200]
