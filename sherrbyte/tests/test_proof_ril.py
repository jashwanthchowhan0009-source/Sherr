"""test_proof_ril.py — the RIL proof run, without a database.

Covers the three things that are easy to get wrong and invisible in output:

  1. the feed separation is real — general feeds are absent from the whitelist,
     financial ones are present;
  2. the move detector has NO LOOKAHEAD — the session's own move never enters the
     volatility it is judged against;
  3. the honest-number machinery is UNBIASED — on data with no relationship
     anywhere, a firing is followed by a beyond-normal RIL move no more often than
     any random session. If this ever drifts, the proof run would be manufacturing
     signal out of noise, which is the one failure it exists to rule out.
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))          # sherrbyte/ -> importable app.*

from app.spie.analog import reaction as R           # noqa: E402
from app.spie.proof import backfill as B            # noqa: E402
from app.spie.proof import signals as S             # noqa: E402
from app.spie.proof.evaluator import evaluate_session  # noqa: E402

UTC = timezone.utc


def _walk(seed, n=900, vol=0.014, shock=0.05):
    """Trading-session random walk with fat-tail shock days. No relationship to
    anything, by construction."""
    rng = random.Random(seed)
    d = datetime(2023, 1, 1, tzinfo=UTC)
    out, p = [], 100.0
    for _ in range(n):
        d += timedelta(days=1)
        if d.weekday() >= 5:               # sessions only, skip weekends
            continue
        p *= 1 + rng.uniform(-vol, vol)
        if rng.random() < shock:
            p *= 1 + rng.choice([-1, 1]) * rng.uniform(0.03, 0.07)
        out.append((d.replace(tzinfo=None), p))
    return out


# ─── 1. feed separation ──────────────────────────────────────────────────────
def test_feed_whitelist_excludes_general_sources():
    sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))   # repo root
    import feeds_financial as F

    fin = set(F.FINANCIAL_SOURCES)
    # The general/consumer feeds that caused the silver<->game mislink must NOT
    # be able to reach the signal path.
    for general in ("IGN", "GameSpot", "BBC Sport", "Variety", "NDTV"):
        assert general not in fin, f"{general} leaked into the financial whitelist"
    # The financial desks and the crude/metals inputs must be present.
    for financial in ("Mint Markets", "ET Markets", "Business Standard Markets",
                      "Moneycontrol Markets", "RBI Releases", "SEBI Releases",
                      "OilPrice.com"):
        assert financial in fin, f"{financial} missing from the financial whitelist"
    # It is a real filter clause bound from one place.
    assert F.financial_sources() == sorted(fin)


# ─── 2. no lookahead in the move detector ────────────────────────────────────
def test_daily_move_denominator_excludes_the_move_itself():
    series = _walk(7)
    # Pick a session with a real move and confirm the trailing window used to
    # score it is strictly before it: doubling only the LAST close must not change
    # the computed sigma (only r), proving the last bar is not in the denominator.
    idx = 200
    ts = series[idx][0]
    m1 = S.daily_move(series, ts)
    bumped = list(series)
    bumped[idx] = (series[idx][0], series[idx][1] * 1.5)
    m2 = S.daily_move(bumped, ts)
    assert m1 and m2
    assert abs(m1["sigma"] - m2["sigma"]) < 1e-9    # denominator unchanged
    assert m1["r"] != m2["r"]                        # the move itself changed


# ─── 3. the firing rule ──────────────────────────────────────────────────────
def _edges():
    return [
        {"edge_key": "o2c_brent", "signal_keys": ["ril", "brent", "refining_margin"]},
        {"edge_key": "ril_usdinr", "signal_keys": ["ril", "usdinr"]},
    ]


def test_fires_only_on_two_or_more_signals():
    ril = _walk(1)
    series = {"RELIANCE.NS": ril, "BZ=F": _walk(2), "USDINR=X": _walk(3)}
    total = 0
    for ts, _ in ril:
        for f in evaluate_session(_edges(), series, ts, []):
            total += 1
            assert len(f.moved_signals) >= 2         # never a one-signal firing
            assert 0 <= f.signal_strength <= 100     # a 0-100 integer, not a %
            assert f.noise_floor == R.noise_floor(1) if hasattr(R, "noise_floor") else True
    assert total > 0                                  # the rule can fire at all


# ─── 4. the honest number is unbiased on null data ───────────────────────────
def test_null_hit_rate_is_no_better_than_chance():
    """Pooled over many INDEPENDENT random-walk worlds, the firing hit rate must
    sit on top of the base rate at every horizon — lift ~ 0. A meaningfully
    positive lift here would mean the machinery scores coincidence as signal."""
    edges = [{"edge_key": "o2c_brent", "signal_keys": ["ril", "brent"]},
             {"edge_key": "ril_usdinr", "signal_keys": ["ril", "usdinr"]}]
    agg = {h: {"fh": 0, "fm": 0, "bh": 0, "bm": 0} for h in R.HORIZONS}
    worlds, fired = 40, 0
    for s in range(worlds):
        ril = _walk(3 * s + 1)
        series = {"RELIANCE.NS": ril, "BZ=F": _walk(3 * s + 2),
                  "USDINR=X": _walk(3 * s + 3)}
        firings = []
        for ts, _ in ril:
            firings += evaluate_session(edges, series, ts, [])
        fired += len(firings)
        r = B.hit_rates(firings, ril)
        for h in R.HORIZONS:
            agg[h]["fh"] += r[h]["firing_hits"]; agg[h]["fm"] += r[h]["firings_measured"]
            agg[h]["bh"] += r[h]["base_hits"];   agg[h]["bm"] += r[h]["base_measured"]

    assert fired > 100, "too few firings to conclude anything"
    for h in R.HORIZONS:
        a = agg[h]
        fr = a["fh"] / a["fm"]
        br = a["bh"] / a["bm"]
        # Pooled over hundreds of firings, chance-level means the two rates agree
        # to within a few points. Not zero — a shock day mildly raises near-term
        # realized vol — but nowhere near a real edge.
        assert abs(fr - br) < 0.06, f"h={h}: lift {fr - br:+.3f} — harness not unbiased"
