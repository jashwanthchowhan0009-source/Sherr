"""
Unit tests for detectors/correlation_math (Intelligence Engine V1, Step 4).

Pure math, DB-free. Verifies Pearson, lag alignment, best-lag detection, and the
two-period consistency guard — including the canonical leading-indicator scenario
(A spikes, B spikes ~3 days later, repeatedly).
"""

from datetime import date, timedelta

from app.spie.discovery.correlation_math import (
    pearson, aligned_at_lag, best_lag_correlation, two_period_consistent,
)


# ─── pearson ──────────────────────────────────────────────────────────────────
def test_pearson_perfect_and_inverse():
    assert round(pearson([1, 2, 3, 4], [2, 4, 6, 8]), 6) == 1.0
    assert round(pearson([1, 2, 3, 4], [8, 6, 4, 2]), 6) == -1.0


def test_pearson_constant_or_short_is_zero():
    assert pearson([5, 5, 5], [1, 2, 3]) == 0.0      # no variance in x
    assert pearson([1], [1]) == 0.0                   # n < 2
    assert pearson([1, 2], [1]) == 0.0                # mismatched lengths


# ─── aligned_at_lag ───────────────────────────────────────────────────────────
def test_aligned_at_lag_shifts_b_forward():
    d = date(2026, 1, 1)
    a = {d: 10.0, d + timedelta(days=1): 20.0}
    b = {d + timedelta(days=3): 11.0, d + timedelta(days=4): 21.0}
    xs, ys = aligned_at_lag(a, b, 3)                  # A[t] vs B[t+3]
    assert xs == [10.0, 20.0] and ys == [11.0, 21.0]


# ─── best_lag_correlation — the leading-indicator scenario ────────────────────
def _spike_series(start: date, spike_days: list[int], value: float, base_days: int) -> dict:
    """Sparse daily series: `value` on spike days, small baseline elsewhere."""
    s = {}
    for i in range(base_days):
        d = start + timedelta(days=i)
        s[d] = value if i in spike_days else 1.0
    return s


def test_best_lag_finds_three_day_leader():
    start = date(2026, 1, 1)
    # A spikes on days 0, 7, 14, 21; B spikes 3 days after each.
    a = _spike_series(start, [0, 7, 14, 21], 100.0, 28)
    b = _spike_series(start, [3, 10, 17, 24], 100.0, 28)
    lag, r, n = best_lag_correlation(a, b, (0, 1, 2, 3, 7))
    assert lag == 3
    assert r > 0.8
    assert n >= 8
    assert two_period_consistent(a, b, lag)


def test_no_lag_relationship_stays_weak():
    start = date(2026, 1, 1)
    a = _spike_series(start, [0, 7, 14, 21], 100.0, 28)
    # B spikes on unrelated days with no consistent offset from A.
    b = _spike_series(start, [1, 2, 15, 27], 100.0, 28)
    lag, r, n = best_lag_correlation(a, b, (0, 1, 2, 3, 7))
    assert abs(r) < 0.5 or not two_period_consistent(a, b, lag)


def test_two_period_rejects_first_half_only_relationship():
    start = date(2026, 1, 1)
    # A spikes across the whole span, but B only responds in the FIRST half
    # (days 3, 10). Second half has A-spikes with no B response → not a pattern.
    a = _spike_series(start, [0, 7, 14, 21], 100.0, 28)
    b = _spike_series(start, [3, 10], 100.0, 28)
    assert two_period_consistent(a, b, 3) is False
