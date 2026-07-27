"""
adapters/weather.py — weather reading → Signal.

magnitude is the anomaly against a baseline when one is given (how far from
normal), else the raw value; direction is the sign of that anomaly. The location
becomes the entity so weather can chain to logistics / commodity signals sharing
the same place. Pure: no DB, no network.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.models.signal import Signal, SignalEntity
from app.spie.knowledge.adapters.base import direction, clamp

_WEATHER_CREDIBILITY = 0.85


def from_reading(location: str, value: float, *, baseline: Optional[float] = None,
                 metric: str = "rainfall", ts: Optional[datetime] = None,
                 source: str = "open-meteo") -> list[Signal]:
    value = float(value or 0.0)
    if baseline is None:
        magnitude, dirn = abs(value), direction(value)
    else:
        anomaly = value - float(baseline)
        magnitude, dirn = abs(anomaly), direction(anomaly)

    return [Signal(
        entities=[SignalEntity(name=location, type="GPE")] if location else [],
        domain="weather",
        ts=ts or datetime.now(timezone.utc),
        location=location or None,
        magnitude=magnitude,
        direction=dirn,
        sentiment=None,
        embedding=None,
        source_id=f"{source}:{metric}",
        credibility=_WEATHER_CREDIBILITY,
        confidence=clamp(0.7),
        novelty=0.0,
        ref_id=f"weather:{location}:{metric}",
    )]
