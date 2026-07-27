"""
detectors/correlation_math.py — pure lag-correlation math (Intelligence Engine V1, Step 4).

No pandas / numpy / ML — plain Python so it is dependency-free and fully unit-testable.
Series are sparse {date: value} maps (only days the entity was active). Aligning A[t]
with B[t+lag] then correlating is exactly pandas' `A.corr(B.shift(-lag))`, done explicitly.
"""

from __future__ import annotations

from datetime import date, timedelta


def pearson(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation of two equal-length samples. 0.0 if undefined
    (n < 2 or a constant series → no variance)."""
    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs)
    dy = sum((y - my) ** 2 for y in ys)
    if dx <= 0 or dy <= 0:
        return 0.0
    return num / ((dx * dy) ** 0.5)


def aligned_at_lag(a: dict, b: dict, lag: int) -> tuple[list[float], list[float]]:
    """Pairs (A[d], B[d+lag]) over the days both series are active — A leading B
    by `lag` days. Deterministic (day-sorted)."""
    xs: list[float] = []
    ys: list[float] = []
    for d in sorted(a):
        d2 = d + timedelta(days=lag)
        if d2 in b:
            xs.append(a[d])
            ys.append(b[d2])
    return xs, ys


def best_lag_correlation(a: dict, b: dict, lags=(0, 1, 2, 3, 7)) -> tuple[int, float, int]:
    """Return (best_lag, r, n_overlap) — the lag maximizing |r| with A leading B."""
    best = (0, 0.0, 0)
    for lag in lags:
        xs, ys = aligned_at_lag(a, b, lag)
        if len(xs) < 2:
            continue
        r = pearson(xs, ys)
        if abs(r) > abs(best[1]):
            best = (lag, r, len(xs))
    return best


def two_period_consistent(a: dict, b: dict, lag: int, min_r: float = 0.3) -> bool:
    """The pattern must show up in ≥ 2 separate time periods, not once — split the
    lag-aligned pairs chronologically in half and require both halves to correlate
    with the SAME sign and |r| ≥ min_r (once = coincidence, twice = pattern)."""
    aligned = []
    for d in sorted(a):
        d2 = d + timedelta(days=lag)
        if d2 in b:
            aligned.append((a[d], b[d2]))
    if len(aligned) < 4:
        return False
    mid = len(aligned) // 2
    r1 = pearson([x for x, _ in aligned[:mid]], [y for _, y in aligned[:mid]])
    r2 = pearson([x for x, _ in aligned[mid:]], [y for _, y in aligned[mid:]])
    return abs(r1) >= min_r and abs(r2) >= min_r and (r1 > 0) == (r2 > 0)
