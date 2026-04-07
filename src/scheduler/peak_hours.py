"""Peak posting slot selection.

Static baseline derived from general social-media engagement stats for Türkiye.
Will be replaced in v2 by account-specific analytics from IG Insights + YT Analytics.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import Platform, ScheduledPost

# TR local time peak slots
PEAK_SLOTS: dict[Platform, list[time]] = {
    Platform.INSTAGRAM: [time(11, 0), time(14, 0), time(19, 0), time(21, 0)],
    Platform.YOUTUBE: [time(12, 0), time(17, 0), time(20, 0), time(22, 0)],
}


def _tz() -> ZoneInfo:
    return ZoneInfo(settings.timezone)


def _candidate_slots(platform: Platform, start_from: datetime) -> list[datetime]:
    tz = _tz()
    slots: list[datetime] = []
    day = start_from.astimezone(tz).date()
    for offset in range(7):
        d: date = day + timedelta(days=offset)
        for t in PEAK_SLOTS[platform]:
            dt = datetime.combine(d, t, tzinfo=tz)
            if dt > start_from.astimezone(tz):
                slots.append(dt)
    return slots


def next_available_slot(session: Session, platform: Platform) -> datetime:
    """Pick the earliest peak slot that doesn't already have a scheduled post."""
    now = datetime.now(tz=_tz())
    taken = {
        row.run_at
        for row in session.execute(
            select(ScheduledPost).where(ScheduledPost.platform == platform)
        )
        .scalars()
        .all()
    }
    for slot in _candidate_slots(platform, now):
        if slot.replace(tzinfo=None) not in {t.replace(tzinfo=None) if t.tzinfo else t for t in taken}:
            return slot
    # Fallback: 7 days later at first slot
    return datetime.combine(
        now.date() + timedelta(days=7), PEAK_SLOTS[platform][0], tzinfo=_tz()
    )
