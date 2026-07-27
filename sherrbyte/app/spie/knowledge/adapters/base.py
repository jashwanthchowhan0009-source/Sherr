"""
adapters/base.py — pure, stdlib-only helpers shared by every domain adapter.

These carry the risk-bearing transform logic (how a raw number/label becomes a
Signal's magnitude / direction / sentiment) so it is unit-testable without a DB,
pydantic, or any domain library.
"""

from __future__ import annotations

_EPS = 1e-9


def direction(x: float, eps: float = _EPS) -> int:
    """Sign of a change as +1 / -1 / 0 — the universal 'direction' field."""
    if x is None:
        return 0
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def sentiment_to_float(s) -> float:
    """Map a sentiment label (or number) to -1..1. Unknown → 0.0 (neutral)."""
    if isinstance(s, (int, float)):
        return clamp(float(s), -1.0, 1.0)
    key = (s or "").strip().lower()
    return {"positive": 1.0, "pos": 1.0, "bullish": 1.0,
            "negative": -1.0, "neg": -1.0, "bearish": -1.0}.get(key, 0.0)


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))
