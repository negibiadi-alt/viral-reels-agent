from datetime import datetime
from zoneinfo import ZoneInfo

from src.config import settings
from src.db import Base, SessionLocal, engine
from src.db.models import Platform, ScheduledPost
from src.scheduler.peak_hours import PEAK_SLOTS, next_available_slot


def setup_module(_mod):
    # Force sqlite in-memory-ish file for tests
    Base.metadata.create_all(bind=engine)


def teardown_function(_fn):
    session = SessionLocal()
    try:
        session.query(ScheduledPost).delete()
        session.commit()
    finally:
        session.close()


def test_next_slot_returns_future_peak():
    session = SessionLocal()
    try:
        slot = next_available_slot(session, Platform.INSTAGRAM)
        assert slot > datetime.now(tz=ZoneInfo(settings.timezone))
        assert slot.time() in PEAK_SLOTS[Platform.INSTAGRAM]
    finally:
        session.close()


def test_next_slot_skips_taken():
    session = SessionLocal()
    try:
        first = next_available_slot(session, Platform.YOUTUBE)
        session.add(
            ScheduledPost(
                candidate_id=1,
                platform=Platform.YOUTUBE,
                run_at=first.replace(tzinfo=None),
            )
        )
        session.commit()
        second = next_available_slot(session, Platform.YOUTUBE)
        assert second > first
    finally:
        session.close()
