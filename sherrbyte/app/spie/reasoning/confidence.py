"""
reasoning/confidence.py — the documented confidence formula (SPIE Reasoning Engine).

Pure and deterministic. Confidence answers "how well-evidenced is this reasoning?",
NOT "how likely is a future move" — there is no forecast anywhere in SPIE.

    confidence = 0.30 · source_diversity
               + 0.25 · npmi_strength
               + 0.25 · historical_consistency
               + 0.20 · cross_market_breadth

  source_diversity      min(distinct sources, 6) / 6
                        One outlet repeating itself is weak; six independent
                        outlets covering the same thing is strong.
  npmi_strength         mean NPMI of the connected entities, clamped to 0..1.
                        NPMI already discounts hub entities that co-occur with
                        everything, so this measures association beyond chance.
  historical_consistency  followed / similar when similar >= 2, else 0.30.
                        A single prior instance is not evidence, so thin history
                        is capped at a deliberately low constant rather than 0
                        (the reasoning may still be sound) or 1 (it is unproven).
  cross_market_breadth  min(co-moving instruments, 3) / 3
                        Several asset classes moving on one shared news driver is
                        stronger corroboration than a single instrument.
"""

from __future__ import annotations

WEIGHTS = {
    "source_diversity": 0.30,
    "npmi_strength": 0.25,
    "historical_consistency": 0.25,
    "cross_market_breadth": 0.20,
}

_THIN_HISTORY = 0.30      # used when fewer than 2 comparable prior clusters exist
_MIN_HISTORY = 2


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def components(*, source_count: int, npmi_values: list[float],
               similar_count: int, followed_count: int,
               co_moving: int) -> dict:
    """The four normalized components, each 0..1 (returned for explainability)."""
    diversity = _clamp(min(int(source_count or 0), 6) / 6)

    vals = [v for v in (npmi_values or []) if v is not None]
    npmi = _clamp(sum(vals) / len(vals)) if vals else 0.0

    if int(similar_count or 0) >= _MIN_HISTORY:
        history = _clamp(int(followed_count or 0) / int(similar_count))
    else:
        history = _THIN_HISTORY

    breadth = _clamp(min(int(co_moving or 0), 3) / 3)

    return {"source_diversity": round(diversity, 3),
            "npmi_strength": round(npmi, 3),
            "historical_consistency": round(history, 3),
            "cross_market_breadth": round(breadth, 3)}


def score(**kwargs) -> float:
    """Weighted confidence in 0..1. Accepts the same keywords as components()."""
    c = components(**kwargs)
    return round(_clamp(sum(WEIGHTS[k] * v for k, v in c.items())), 3)
