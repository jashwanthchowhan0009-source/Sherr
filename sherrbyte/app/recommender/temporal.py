"""
recommender/temporal.py — Freshness decay scorer.

News value decays fast. We use exponential half-life decay so a story is worth
~half as much every FRESHNESS_HALFLIFE_HOURS. Returns a value in (0, 1].
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

from app.config import settings


def freshness(published_at: datetime, now: datetime | None = None) -> float:
    """Exponential decay in (0,1] based on age vs the configured half-life."""
    if published_at is None:
        return 0.3
    now = now or datetime.now(timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600.0)
    halflife = max(0.5, settings.freshness_halflife_hours)
    return math.pow(0.5, age_hours / halflife)
