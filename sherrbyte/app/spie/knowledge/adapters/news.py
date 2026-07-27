"""
adapters/news.py — news domain → Signal.

Reuses the existing pipeline's understood output (InfoObject / InfoObjectIn or a
plain dict). One article → one Signal carrying all its entities; co-occurrence is
derived downstream from that entity list. Pure: no DB, no network.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.models.signal import Signal, SignalEntity
from app.spie.knowledge.adapters.base import direction, sentiment_to_float, clamp
from app.spie.knowledge.adapters.credibility import score as credibility_score


def _get(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def from_info_object(obj: Any) -> list[Signal]:
    """Convert one understood article into a news Signal (list for a uniform API)."""
    raw_entities = _get(obj, "entities", []) or []
    entities: list[SignalEntity] = []
    for e in raw_entities:
        name = _get(e, "name", "") or _get(e, "canonical", "")
        if name:
            entities.append(SignalEntity(name=name, type=_get(e, "type", "MISC") or "MISC"))

    sent = sentiment_to_float(_get(obj, "sentiment", "neutral"))
    importance = float(_get(obj, "importance", 0.0) or 0.0)
    ts = _get(obj, "published_at") or datetime.now(timezone.utc)
    source = _get(obj, "source_name", "") or ""

    return [Signal(
        entities=entities,
        domain="news",
        ts=ts,
        location=(_get(_get(obj, "wwww", {}) or {}, "where", None) or None),
        # magnitude = article presence (one unit of news volume for these entities);
        # editorial importance is a confidence weight, not a magnitude.
        magnitude=1.0,
        direction=direction(sent),
        sentiment=sent,
        embedding=_get(obj, "embedding", None),
        source_id=source,
        credibility=credibility_score(source),
        confidence=clamp(importance if importance > 0 else 0.5),
        novelty=float(_get(obj, "novelty", 0.0) or 0.0),
        ref_id=(str(_get(obj, "id")) if _get(obj, "id") is not None else None),
    )]
