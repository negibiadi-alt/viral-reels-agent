"""Rank and filter raw candidates into viral ones."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.config import settings
from src.discovery.apify_client import RawCandidate


def _hours_since(dt: datetime | None) -> float:
    if dt is None:
        return 24 * 365  # very old => low views/hour
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    return max(delta.total_seconds() / 3600, 1.0)


def engagement_rate(c: RawCandidate) -> float:
    if c.views <= 0:
        return 0.0
    return (c.likes + c.comments) / c.views


def views_per_hour(c: RawCandidate) -> float:
    return c.views / _hours_since(c.posted_at)


def viral_score(c: RawCandidate) -> float:
    """Composite score — higher is better."""
    vph = views_per_hour(c)
    er = engagement_rate(c)
    return vph * (1 + er * 10)


def is_viral(c: RawCandidate) -> bool:
    if c.views < settings.min_views:
        return False
    if engagement_rate(c) < settings.min_engagement_rate:
        return False
    if views_per_hour(c) < settings.min_views_per_hour:
        return False
    if c.posted_at:
        age = datetime.now(timezone.utc) - (
            c.posted_at if c.posted_at.tzinfo else c.posted_at.replace(tzinfo=timezone.utc)
        )
        if age > timedelta(days=settings.max_candidate_age_days):
            return False
    return True


def rank_and_filter(candidates: list[RawCandidate], top_n: int) -> list[RawCandidate]:
    viral = [c for c in candidates if is_viral(c)]
    viral.sort(key=viral_score, reverse=True)
    return viral[:top_n]
