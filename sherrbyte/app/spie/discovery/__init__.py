"""
detectors — domain-agnostic pattern detectors (Intelligence Engine V1, Step 4).

Each detector is a scheduled job that reads Signals / co-occurrence and writes to
the insights table with a mandatory explain_json. V1 ships two, both gated on the
materialized co-occurrence table (never brute-forced over all pairs):

    emergence            — entity pairs new in the last 7 days, unseen in the prior 90
    temporal_correlation — leading-indicator pairs (lag-window correlation)
    volume_anomaly       — per-entity daily story-volume spike (EWMA + MAD z-score)
    market_reaction      — news ↔ market: an unusual instrument move with related
                           news in the preceding (or following) window
    observation          — TIER 1: today's top movers with news context, no history
                           required (see observation.py)
"""

from app.spie.discovery import (
    emergence, observation, temporal, volume_anomaly, market_reaction)

REGISTRY = {
    # Tier 1 runs first: it needs only today's data, so it is the one detector
    # guaranteed to have something to say on a young corpus.
    "observation": observation.run,
    "emergence": emergence.run,
    "temporal_correlation": temporal.run,
    "volume_anomaly": volume_anomaly.run,
    "market_reaction": market_reaction.run,
}

__all__ = ["emergence", "observation", "temporal", "volume_anomaly",
           "market_reaction", "REGISTRY"]
