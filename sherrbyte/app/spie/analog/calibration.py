"""analog/calibration.py — what does this engine score on data with nothing in it?

Every scoring system answers SOME number for any input. The only way to know
whether 11 means anything is to know what pure noise scores. So: generate price
series that are random walks with no relationship to anything, run them through
the real measure/aggregate path, and record what comes out.

TWO NUMBERS, TWO JOBS
=====================
NOISE_FLOOR   the 95th percentile of noise at that horizon. This is the
              READER-FACING bar shipped beside every card: a card scoring 11
              against a floor of 11 is nothing, and one scoring 60 against a
              floor of 11 is something. Without it a number is unauditable.

NOISE_CEILING max observed over 200 seeds plus headroom. This is the CI bar.
              If a future change ever pushes the null above it, the build fails
              — that is the regression that put signal_strength 42 on random
              walks before the sqrt(h) term was added.

Both were MEASURED by this module's own null_run(), not chosen. Re-derive them
any time with:

    python -m app.spie.analog.calibration --seeds 200
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from app.spie.analog import reaction as R

UTC = timezone.utc

# Measured 2026-09-01 over 200 seeds x 140 events, sqrt(h) scaling in place.
#
#   h    mean   p50   p95   p99   max
#   1     3.3     3     6     6     7
#   3     6.9     7    11    13    15
#   5     7.6     8    12    16    17
#  10     7.3     7    13    18    21
#
# Before sqrt(h) the same null produced 42 at h=10. That is what these numbers
# exist to stop coming back.
NOISE_FLOOR = {1: 6, 3: 11, 5: 12, 10: 13}      # p95 — the reader's bar
NOISE_CEILING = {1: 11, 3: 19, 5: 21, 10: 25}   # max + headroom — the CI bar

# Fixed so CI is deterministic. Changing them invalidates the numbers above.
NULL_SESSIONS = 420
NULL_EVENTS = 140
NULL_DAILY_VOL = 0.012
NULL_SHOCK_PROB = 0.05


def noise_floor(horizon: int) -> int:
    """The score pure noise reaches at this horizon 95% of the time.

    Unknown horizons fall back to the highest measured floor rather than 0: an
    unmeasured horizon must not be presented as a cleaner bar than a measured
    one.
    """
    return NOISE_FLOOR.get(int(horizon), max(NOISE_FLOOR.values()))


def clears_noise(signal: int, horizon: int) -> bool:
    """Does this score exceed what noise reaches at the same horizon?"""
    return int(signal) > noise_floor(horizon)


def random_walk(seed: int, *, sessions: int = NULL_SESSIONS,
                vol: float = NULL_DAILY_VOL,
                shock_prob: float = NULL_SHOCK_PROB) -> list:
    """A price series with no relationship to any news, by construction.

    Occasional shock days are included deliberately: real instruments have fat
    tails, and a null built only from small moves would be an easier test than
    reality and would set the floor too low.
    """
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=UTC)
    out, price = [], 100.0
    for i in range(sessions):
        price *= 1.0 + rng.uniform(-vol, vol)
        if rng.random() < shock_prob:
            price *= 1.0 + rng.choice([-1, 1]) * rng.uniform(0.03, 0.07)
        out.append((base + timedelta(days=i), price))
    return out


def null_run(seed: int, *, n_events: int = NULL_EVENTS) -> dict:
    """{horizon: signal_strength} for one null series.

    Events are placed at random sessions with no relationship to the prices —
    which is the whole point. Anything above zero here is the engine scoring
    coincidence.
    """
    rng = random.Random(seed * 7919 + 13)
    series = random_walk(seed)
    lo, hi = R.VOL_WINDOW + 10, len(series) - max(R.HORIZONS) - 10
    idxs = sorted(rng.sample(range(lo, hi), min(n_events, hi - lo)))

    out = {}
    for h in R.HORIZONS:
        cells = []
        for i in idxs:
            cell = R.measure(series, series[i][0], h)
            if not cell.get("ok"):
                continue
            cell["age_days"] = rng.uniform(0, 368)
            cell["event_id"] = "null"
            cell["occurred_at"] = series[i][0]
            cells.append(cell)
        agg = R.aggregate(cells)
        out[h] = agg["signal_strength"] if agg else 0
    return out


def sweep(seeds: int = 200) -> dict:
    """Run the null across many seeds and summarise per horizon."""
    per_h = {h: [] for h in R.HORIZONS}
    for seed in range(seeds):
        for h, s in null_run(seed).items():
            per_h[h].append(s)

    def pct(v, q):
        s = sorted(v)
        return s[min(len(s) - 1, int(q * len(s)))]

    return {h: {"n": len(v), "mean": round(sum(v) / len(v), 2),
                "p50": pct(v, 0.50), "p95": pct(v, 0.95),
                "p99": pct(v, 0.99), "max": max(v)}
            for h, v in per_h.items()}


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="re-derive the noise floor")
    ap.add_argument("--seeds", type=int, default=200)
    args = ap.parse_args()

    stats = sweep(args.seeds)
    print(f"{'h':>3}{'n':>6}{'mean':>8}{'p50':>6}{'p95':>6}{'p99':>6}{'max':>6}"
          f"{'floor':>7}{'ceiling':>9}")
    for h in sorted(stats):
        s = stats[h]
        print(f"{h:>3}{s['n']:>6}{s['mean']:>8.1f}{s['p50']:>6}{s['p95']:>6}"
              f"{s['p99']:>6}{s['max']:>6}{NOISE_FLOOR.get(h, 0):>7}"
              f"{NOISE_CEILING.get(h, 0):>9}")
    print("\nNOISE_FLOOR should track p95; NOISE_CEILING should sit above max.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
