"""
detectors — domain-agnostic pattern detectors (Intelligence Engine V1, Step 4).

Each detector is a scheduled job that reads Signals / co-occurrence and writes to
the insights table with a mandatory explain_json. V1 ships two, both gated on the
materialized co-occurrence table (never brute-forced over all pairs):

    emergence            — entity pairs new in the last 7 days, unseen in the prior 90
    temporal_correlation — leading-indicator pairs (lag-window correlation)
"""

from app.spie.discovery import emergence, temporal

REGISTRY = {
    "emergence": emergence.run,
    "temporal_correlation": temporal.run,
}

__all__ = ["emergence", "temporal", "REGISTRY"]
