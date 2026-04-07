"""Approved-candidate pipeline.

Picks up APPROVED candidates, processes the video, and creates a ScheduledPost
at the next peak slot. The scheduler then fires `run_scheduled_post` at run_at.
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
from src.publishing.ig_publisher import publish_reel
from src.publishing.yt_publisher import publish_short
from src.scheduler.peak_hours import next_available_slot


def _pick_approved(session: Session) -> list[Candidate]:
    return (
        session.execute(
            select(Candidate)
            .where(Candidate.status == CandidateStatus.APPROVED)
            .order_by(Candidate.discovered_at.asc())
        )
        .scalars()
        .all()
    )


def enqueue_approved() -> list[ScheduledPost]:
    """Process every approved candidate and place it on a peak slot."""
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

            run_at = next_available_slot(session, cand.platform).replace(tzinfo=None)
            post = ScheduledPost(
                candidate_id=cand.id,
                platform=cand.platform,
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
    """Actually publish a ScheduledPost. Called by APScheduler at run_at."""
    session = SessionLocal()
    try:
        post = session.get(ScheduledPost, post_id)
        if post is None or post.status != "queued":
            return
        cand = session.get(Candidate, post.candidate_id)
        if cand is None:
            return

        processed_path = settings.processed_dir / f"{cand.id}.mp4"
        caption = _build_caption(cand)

        try:
            if post.platform == Platform.INSTAGRAM:
                # video_url must be a public URL; integrate object storage in prod
                video_url = _public_url_for(processed_path)
                media_id = publish_reel(video_url, caption)
                post.published_url = f"https://instagram.com/reel/{media_id}"
            elif post.platform == Platform.YOUTUBE:
                video_id = publish_short(
                    processed_path,
                    title=(cand.caption or "Shorts")[:90],
                    description=caption,
                    tags=[],
                )
                post.published_url = f"https://youtube.com/shorts/{video_id}"
            post.status = "published"
            cand.status = CandidateStatus.PUBLISHED
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


def _public_url_for(path: Path) -> str:
    """Stub — replace with S3/R2 upload returning a presigned URL."""
    return f"file://{path.resolve()}"
