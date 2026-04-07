from datetime import datetime, timedelta, timezone

from src.db.models import Platform
from src.discovery.apify_client import RawCandidate
from src.discovery.viral_filter import (
    engagement_rate,
    is_viral,
    rank_and_filter,
    viral_score,
)


def _mk(views: int, likes: int, comments: int, age_hours: float) -> RawCandidate:
    return RawCandidate(
        platform=Platform.INSTAGRAM,
        source_url=f"https://x/{views}-{likes}-{age_hours}",
        thumbnail_url=None,
        caption="t",
        author="u",
        views=views,
        likes=likes,
        comments=comments,
        posted_at=datetime.now(timezone.utc) - timedelta(hours=age_hours),
        raw={},
    )


def test_engagement_rate_zero_views():
    c = _mk(0, 0, 0, 1)
    assert engagement_rate(c) == 0.0


def test_is_viral_happy_path():
    c = _mk(views=200_000, likes=10_000, comments=2_000, age_hours=24)
    assert is_viral(c)


def test_is_viral_rejects_low_views():
    c = _mk(views=1000, likes=500, comments=100, age_hours=24)
    assert not is_viral(c)


def test_is_viral_rejects_low_engagement():
    c = _mk(views=500_000, likes=100, comments=10, age_hours=24)
    assert not is_viral(c)


def test_rank_sorts_by_score_desc():
    hot = _mk(500_000, 40_000, 5_000, age_hours=5)
    warm = _mk(200_000, 10_000, 1_000, age_hours=24)
    stale = _mk(100_000, 8_000, 500, age_hours=100)
    ranked = rank_and_filter([warm, hot, stale], top_n=2)
    assert ranked[0] is hot
    assert viral_score(ranked[0]) >= viral_score(ranked[1])
