"""
Unit tests for discovery/anomaly_math (Sherr-I Task 4) — pure, DB-free.

Verifies median / EWMA / MAD and the robust MAD z-score, plus the end-to-end
spike scenario the volume-anomaly detector relies on (a quiet entity that suddenly
appears in many stories fires; a mild day does not).
"""

from app.spie.discovery.anomaly_math import median, ewma, mad, mad_zscore


def test_median():
    assert median([1, 2, 3, 4]) == 2.5
    assert median([3, 1, 2]) == 2
    assert median([]) == 0.0


def test_ewma_flat_and_weighting():
    assert ewma([10, 10, 10], 0.3) == 10.0
    # Recent points pull the average up.
    assert ewma([0, 0, 0, 10], 0.3) > ewma([0, 0, 0, 1], 0.3)


def test_mad():
    assert mad([2, 4, 6, 8]) == 2.0            # median 5, devs [3,1,1,3] → median 2
    assert mad([5, 5, 5]) == 0.0               # no spread


def test_mad_zscore_floor_prevents_blowup():
    # Near-flat baseline (mad 0 → floored to 1): a +1 wobble stays small.
    assert mad_zscore(5, 4, 0.0) < 1.0
    # A big jump over a small scale is a large score.
    assert mad_zscore(15, 2, 1.0) > 5.0


def test_below_baseline_is_negative():
    assert mad_zscore(1, 5, 1.0) < 0


# ─── end-to-end spike scenario ───────────────────────────────────────────────
def test_spike_fires_and_mild_day_does_not():
    # A quiet entity: ~1-2 stories/day for two weeks.
    history = [1, 2, 1, 0, 1, 2, 1, 1, 0, 1, 2, 1, 1, 0]
    baseline = ewma(history, 0.3)
    scale = mad(history)

    spike = mad_zscore(12, baseline, scale)     # 12 stories today
    mild = mad_zscore(3, baseline, scale)       # 3 stories today
    assert spike >= 3.5, f"spike z={spike} should fire"
    assert mild < 3.5, f"mild z={mild} should not fire"
