"""The null: what does the analog engine score on data with nothing in it?

THIS IS A REGRESSION TEST FOR A BUG THAT ALREADY HAPPENED. Before the sqrt(h)
term was added, z divided an h-day return by a ONE-day volatility, so long
horizons looked violent for free. On pure random walks — no relationship
anywhere in the data — the engine reported signal_strength 42 at h=10 against 3
at h=1. Every one of those 42 points was manufactured.

Nothing in the output looked wrong. The counts were real counts, the medians
were real medians, and the whole thing was noise. So the null runs on every CI
run now: if any future change pushes a horizon back above its measured ceiling,
the build fails rather than a card shipping a confident number about nothing.

Re-derive the numbers with:
    python -m app.spie.analog.calibration --seeds 200
"""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sherrbyte"))

from app.spie.analog import calibration as C     # noqa: E402
from app.spie.analog import reaction as R        # noqa: E402

# Enough seeds to be meaningful, few enough to stay fast in CI. The published
# floors come from 200; this samples the same generator deterministically.
CI_SEEDS = 40


@pytest.fixture(scope="module")
def null_scores():
    """{horizon: [signal_strength, ...]} over the CI seed set."""
    per_h = {h: [] for h in R.HORIZONS}
    for seed in range(CI_SEEDS):
        for h, s in C.null_run(seed).items():
            per_h[h].append(s)
    return per_h


# ─── THE CEILING: the build fails if noise ever scores above this ────────────

@pytest.mark.parametrize("horizon", R.HORIZONS)
def test_noise_never_exceeds_its_measured_ceiling(null_scores, horizon):
    """If this fails, something is manufacturing signal out of nothing."""
    worst = max(null_scores[horizon])
    ceiling = C.NOISE_CEILING[horizon]
    assert worst <= ceiling, (
        f"h={horizon}: pure noise scored {worst}, above the measured ceiling "
        f"of {ceiling}. Something in the scoring path is inventing signal — "
        f"check the horizon scaling before shipping any card.")


def test_the_long_horizon_does_not_score_systematically_above_the_short_one():
    """THE ORIGINAL BUG, stated directly.

    With a one-day denominator, h=10 scored ~14x h=1 on identical noise. The
    horizons need not be equal — longer windows do accumulate more real
    exceedances — but a 3x mean gap on data with NOTHING in it is the scaling
    error coming back.
    """
    means = {}
    for h in R.HORIZONS:
        v = [C.null_run(seed)[h] for seed in range(CI_SEEDS)]
        means[h] = sum(v) / len(v)
    assert means[10] <= means[1] * 3.0, (
        f"h=10 averages {means[10]:.1f} on noise against h=1's {means[1]:.1f} "
        f"— long horizons are being inflated by construction")


# ─── THE FLOOR: the reader-facing bar ───────────────────────────────────────

def test_the_published_floor_matches_what_noise_actually_reaches(null_scores):
    """NOISE_FLOOR is the 95th percentile of noise. If the code drifts away
    from the published number, the bar shown to readers is a lie."""
    for h in R.HORIZONS:
        v = sorted(null_scores[h])
        p95 = v[min(len(v) - 1, int(0.95 * len(v)))]
        published = C.NOISE_FLOOR[h]
        assert abs(p95 - published) <= 4, (
            f"h={h}: measured p95 {p95}, published floor {published}. "
            f"Re-derive with calibration.py --seeds 200 and update both.")


def test_a_floor_exists_for_every_horizon_the_engine_computes():
    for h in R.HORIZONS:
        assert h in C.NOISE_FLOOR and h in C.NOISE_CEILING


def test_an_unknown_horizon_gets_the_strictest_floor_not_zero():
    """An unmeasured horizon must not be presented as a cleaner bar than a
    measured one."""
    assert C.noise_floor(999) == max(C.NOISE_FLOOR.values())


def test_the_ceiling_sits_above_the_floor_everywhere():
    for h in R.HORIZONS:
        assert C.NOISE_CEILING[h] > C.NOISE_FLOOR[h]


def test_clears_noise_is_strict():
    """Scoring exactly the noise floor is not clearing it."""
    h = 1
    floor = C.NOISE_FLOOR[h]
    assert not C.clears_noise(floor, h)
    assert C.clears_noise(floor + 1, h)


# ─── every stored row carries its own bar ───────────────────────────────────

def _cells(z, n, horizon):
    return [{"ok": True, "z": z, "r": z / 100.0, "mad_sigma": 0.01,
             "age_days": 10.0, "event_id": "e", "occurred_at": "2026-06-01"}
            for _ in range(n)]


def test_aggregate_ships_the_noise_floor_with_the_score():
    """A card must never be renderable without its bar."""
    agg = R.aggregate(_cells(3.0, 10, 5), horizon=5)
    assert agg["noise_floor"] == C.NOISE_FLOOR[5]
    assert agg["clears_noise"] is (agg["signal_strength"] > agg["noise_floor"])


def test_a_score_at_or_below_the_floor_is_marked_as_not_clearing_it():
    """The 11-against-a-floor-of-11 case: real arithmetic, no evidence."""
    agg = R.aggregate(_cells(0.2, 10, 3), horizon=3)
    assert agg["signal_strength"] <= agg["noise_floor"]
    assert agg["clears_noise"] is False


def test_a_strong_pattern_clears_the_floor():
    agg = R.aggregate(_cells(4.0, 20, 1), horizon=1)
    assert agg["signal_strength"] > agg["noise_floor"]
    assert agg["clears_noise"] is True


def test_the_migration_stores_the_floor_and_refuses_a_null_one():
    sql = open(os.path.join(
        _ROOT, "sherrbyte/app/db/migrations/023_analog_reactions.sql")).read()
    assert "noise_floor" in sql and "noise_floor     INTEGER NOT NULL" in sql


# ─── the sqrt(h) term itself ────────────────────────────────────────────────

def test_z_is_scaled_by_the_square_root_of_the_horizon():
    """Same series, same anchor: a 4-day horizon's denominator must be exactly
    2x a 1-day one for identical trailing volatility."""
    series = C.random_walk(7)
    ts = series[200][0]
    one = R.measure(series, ts, 1)
    assert one["ok"] and one["horizon_scale"] == 1.0
    four = R.measure(series, ts, 4)
    assert four["ok"] and four["horizon_scale"] == 2.0
    # mad_sigma is stored unscaled so z can be re-derived either way.
    assert one["mad_sigma"] == four["mad_sigma"]
    assert four["z"] == pytest.approx(four["r"] / (four["mad_sigma"] * 2.0))
