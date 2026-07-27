"""
discovery/anomaly_math.py — robust anomaly primitives (SPIE Task 4).

Pure, stdlib-only, unit-testable. News volume is heavy-tailed, so deviation is
measured with the **Median Absolute Deviation (MAD)**, not the standard deviation
(σ-based z-scores false-alarm constantly on heavy tails). The baseline level is an
**EWMA** (exponentially weighted moving average) of the history.
"""

from __future__ import annotations

_MAD_TO_SIGMA = 0.6745   # modified z-score constant: MAD → σ-equivalent (normal)


def median(values: list[float]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    s = sorted(values)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def ewma(values: list[float], alpha: float = 0.3) -> float:
    """Exponentially weighted moving average — the smoothed 'expected level'.
    Recent days weigh more; returns the running average after the last point."""
    if not values:
        return 0.0
    e = float(values[0])
    for v in values[1:]:
        e = alpha * v + (1 - alpha) * e
    return e


def mad(values: list[float]) -> float:
    """Median absolute deviation around the median — a robust scale estimate."""
    if not values:
        return 0.0
    m = median(values)
    return median([abs(v - m) for v in values])


def mad_zscore(x: float, center: float, mad_scale: float) -> float:
    """Robust (modified) z-score of `x` given a `center` (EWMA baseline) and a MAD
    scale. MAD is floored at 1.0 (one story) so a near-flat baseline can't blow the
    score up on a one-off ±1 wobble."""
    scale = mad_scale if mad_scale and mad_scale > 1.0 else 1.0
    return _MAD_TO_SIGMA * (x - center) / scale
