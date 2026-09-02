"""analog/reaction.py — what the instrument actually did after comparable events.

Pure arithmetic over stored prices. No model, no LLM, no provider call. Given a
set of analog events and a symbol's daily closes, measure the forward move at
each horizon, normalise it against that instrument's own normal range AT THE
TIME, and aggregate.

THE ONE THING THAT MUST NOT BE WRONG: NO LOOKAHEAD
==================================================
The volatility an analog is normalised by is computed from the 60 sessions
BEFORE that analog's date and nothing after it. Use a window that includes the
move itself and every event scores as unremarkable, because the move inflated
its own denominator. That failure is invisible in the output — the numbers look
plausible and are meaningless — so it is asserted in a test rather than trusted
to review.

WHAT signal_strength IS
=======================
A 0-100 integer for RANKING which past pattern is best evidenced. It is not a
confidence, not a probability, and must never be rendered as a percentage. Its
inputs are all frequencies and dispersions of things that already happened.

HORIZON SCALING: THE sqrt(h) TERM IS NOT OPTIONAL
=================================================
    z = r_h / (MAD_1day * sqrt(h))

An h-day return accumulates variance over h days, so dividing it by a ONE-day
volatility makes long horizons look violent for free. This was measured, not
argued: on a corpus of pure random walks with no relationship anywhere in the
data, the unscaled version reported signal_strength 42 at h=10 against 3 at
h=1. With sqrt(h) the same data stays at 13. Forty-two points of confidence
manufactured out of noise is exactly the failure this engine exists to avoid.

test_calibration_noise_floor.py runs that null on every CI run and fails if any
horizon ever climbs back above its measured ceiling.

The raw return and the unscaled MAD are both kept in the breakdown, so anything
downstream can re-derive either form without recomputing.
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timezone

log = logging.getLogger("sherbyte.analog.reaction")

HORIZONS = (1, 3, 5, 10)

VOL_WINDOW = int(os.getenv("SHERR_I_ANALOG_VOL_WINDOW", "60"))
# Fewer real sessions than this in the trailing window and the denominator is
# not trustworthy, so the whole cell is dropped rather than estimated.
MIN_VOL_SESSIONS = int(os.getenv("SHERR_I_ANALOG_MIN_VOL_SESSIONS", "45"))

Z_EXCEEDED = float(os.getenv("SHERR_I_ANALOG_Z_EXCEEDED", "2.5"))

# Phase 3's suppression rule. A thin sample is worse than silence.
MIN_ANALOGS = int(os.getenv("SHERR_I_ANALOG_MIN_ANALOGS", "5"))

# exp(-age_days / 540): an analog from 18 months ago carries about a third the
# weight of one from last week.
RECENCY_TAU_DAYS = float(os.getenv("SHERR_I_ANALOG_RECENCY_TAU", "540"))

# MAD -> standard-deviation-equivalent for a normal distribution.
_MAD_TO_SIGMA = 1.4826


# ─── small statistics, stdlib only ───────────────────────────────────────────

def median(xs: list) -> float:
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def mad(xs: list) -> float:
    """Median absolute deviation from the median."""
    if not xs:
        return 0.0
    m = median(xs)
    return median([abs(x - m) for x in xs])


def iqr(xs: list) -> float:
    """Interquartile range by linear interpolation."""
    if len(xs) < 2:
        return 0.0
    s = sorted(xs)

    def q(p):
        pos = p * (len(s) - 1)
        lo = int(math.floor(pos))
        hi = min(lo + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (pos - lo)

    return q(0.75) - q(0.25)


def log_return(a: float, b: float):
    """log(b / a), or None when either price is unusable."""
    if not a or not b or a <= 0 or b <= 0:
        return None
    return math.log(b / a)


# ─── the per-analog measurement ──────────────────────────────────────────────

def measure(series: list, event_ts, horizon: int) -> dict:
    """One (analog, symbol, horizon) cell, or a dict saying why there isn't one.

    `series` is [(ts, close), ...] ascending. Trading days are ROW OFFSETS: the
    table only holds sessions the market actually traded, so "3 trading days
    later" is three rows on, never three calendar days.
    """
    if not series:
        return {"ok": False, "reason": "no price series"}

    # The anchor is the last session AT OR BEFORE the event. An event on a
    # Saturday is anchored to Friday's close, not skipped.
    idx = _anchor_index(series, event_ts)
    if idx is None:
        return {"ok": False, "reason": "event predates the price series"}

    target = idx + horizon
    if target >= len(series):
        return {"ok": False, "reason": f"fewer than {horizon} sessions after the event"}

    # ── THE NO-LOOKAHEAD BOUNDARY ───────────────────────────────────────────
    # Trailing window is series[:idx + 1] — up to and INCLUDING the anchor, and
    # not one row further. series[idx + 1:] is the future at the moment the
    # event happened and must not touch the denominator.
    trailing = series[max(0, idx + 1 - VOL_WINDOW): idx + 1]
    rets = []
    for i in range(1, len(trailing)):
        r = log_return(trailing[i - 1][1], trailing[i][1])
        if r is not None:
            rets.append(r)

    if len(rets) < MIN_VOL_SESSIONS:
        return {"ok": False,
                "reason": f"only {len(rets)} of {VOL_WINDOW} trailing sessions "
                          f"(need {MIN_VOL_SESSIONS})"}

    sigma = _MAD_TO_SIGMA * mad(rets)
    if sigma <= 0:
        # A flat instrument has no normal range to be unusual against.
        return {"ok": False, "reason": "zero trailing volatility"}

    r_h = log_return(series[idx][1], series[target][1])
    if r_h is None:
        return {"ok": False, "reason": "unusable prices at the horizon"}

    # sqrt(h): an h-day return has h days of variance in it. Without this the
    # denominator is a one-day yardstick held against a ten-day move.
    scaled = sigma * math.sqrt(horizon)

    return {"ok": True, "r": r_h, "z": r_h / scaled,
            "mad_sigma": sigma,            # unscaled, so z can be re-derived
            "horizon_scale": math.sqrt(horizon),
            "anchor_ts": series[idx][0], "target_ts": series[target][0],
            "trailing_sessions": len(rets)}


def _anchor_index(series: list, event_ts):
    """Index of the last session at or before the event, or None."""
    if event_ts is None:
        return None
    lo, hi, found = 0, len(series) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if _ts(series[mid][0]) <= _ts(event_ts):
            found, lo = mid, mid + 1
        else:
            hi = mid - 1
    return found


def _ts(x) -> float:
    if isinstance(x, datetime):
        d = x if x.tzinfo else x.replace(tzinfo=timezone.utc)
        return d.timestamp()
    return float(x)


# ─── aggregation ─────────────────────────────────────────────────────────────

def recency_weight(age_days: float) -> float:
    return math.exp(-max(0.0, float(age_days)) / RECENCY_TAU_DAYS)


def signal_strength(*, n_analogs: int, n_exceeded: int, sign_agreement: float,
                    recency: float) -> int:
    """The 0-100 ranking integer. Math only — the LLM never touches this.

    Four multiplicative penalties, each answering one objection to the headline
    frequency:
      n_exceeded/n_analogs   how often it actually moved beyond its normal range
      sign_agreement         evidence that splits on direction is weak evidence
      n_analogs/15           a 5-analog sample must not score like a 20-analog one
      recency                a pattern last seen two years ago has decayed
    """
    if n_analogs <= 0:
        return 0
    base = 100.0 * (n_exceeded / n_analogs)
    base *= (0.5 + 0.5 * sign_agreement)
    base *= min(1.0, n_analogs / 15.0)
    base *= (0.6 + 0.4 * recency)
    return int(round(max(0.0, min(100.0, base))))


def aggregate(cells: list, *, min_analogs: int = None, horizon: int = None) -> dict:
    """Cells for one (symbol, horizon) into the stored statistics, or None.

    Returns None — not a zeroed row — when the sample is too thin. Silence is a
    valid output and the schema refuses to store anything below the floor.
    """
    floor = MIN_ANALOGS if min_analogs is None else int(min_analogs)
    usable = [c for c in cells if c.get("ok")]
    n = len(usable)
    if n < floor:
        return None

    zs = [c["z"] for c in usable]
    pos = sum(1 for z in zs if z > 0)
    neg = sum(1 for z in zs if z < 0)
    agreement = max(pos, neg) / n if n else 0.0
    recency = sum(recency_weight(c.get("age_days", 0.0)) for c in usable) / n
    n_exceeded = sum(1 for z in zs if abs(z) >= Z_EXCEEDED)
    strength = signal_strength(n_analogs=n, n_exceeded=n_exceeded,
                               sign_agreement=agreement, recency=recency)

    from app.spie.analog.calibration import noise_floor         # noqa: PLC0415
    floor = noise_floor(horizon) if horizon is not None else 0

    return {
        "n_analogs": n,
        "n_exceeded": n_exceeded,
        "sign_agreement": round(agreement, 6),
        "median_abs_z": round(median([abs(z) for z in zs]), 6),
        # IQR of z, not of |z|: dispersion should show a sample that split
        # between big rises and big falls as WIDE, and folding the sign would
        # hide exactly that.
        "dispersion": round(iqr(zs), 6),
        "recency_weight": round(recency, 6),
        "signal_strength": strength,
        # The bar this score has to clear to mean anything. Travels WITH the
        # row so no renderer can show the score without it.
        "noise_floor": floor,
        "clears_noise": strength > floor,
        "breakdown": [{
            "event_id": c.get("event_id"),
            "occurred_at": str(c.get("occurred_at")),
            "r": round(c["r"], 8),
            "z": round(c["z"], 6),
            "mad_sigma": round(c["mad_sigma"], 8),
            "age_days": round(float(c.get("age_days", 0.0)), 1),
        } for c in usable],
    }


# ─── database side ───────────────────────────────────────────────────────────

_SERIES_SQL = """
SELECT ts, price
  FROM sherrbyte_app.market_ticks
 WHERE symbol = $1
 ORDER BY ts
"""

_EVENTS_SQL = """
SELECT event_id, occurred_at, event_class, linked_symbols
  FROM hist_events
 ORDER BY occurred_at
"""

_UPSERT = """
INSERT INTO analog_reactions
    (symbol, event_class, horizon_days, n_analogs, n_exceeded, sign_agreement,
     median_abs_z, dispersion, recency_weight, signal_strength, noise_floor,
     breakdown)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
ON CONFLICT (symbol, event_class, horizon_days) DO UPDATE
   SET n_analogs = EXCLUDED.n_analogs,
       n_exceeded = EXCLUDED.n_exceeded,
       sign_agreement = EXCLUDED.sign_agreement,
       median_abs_z = EXCLUDED.median_abs_z,
       dispersion = EXCLUDED.dispersion,
       recency_weight = EXCLUDED.recency_weight,
       signal_strength = EXCLUDED.signal_strength,
       noise_floor = EXCLUDED.noise_floor,
       breakdown = EXCLUDED.breakdown,
       computed_at = now()
"""


async def compute(conn, *, write: bool = True, horizons=HORIZONS) -> dict:
    """Build every (symbol, event_class, horizon) cell the corpus supports.

    Returns a funnel plus the surviving rows. When nothing survives, the funnel
    says which gate did it — thin library, missing prices, or the 45-of-60
    trailing-session rule.
    """
    import json                                                    # noqa: PLC0415

    events = await conn.fetch(_EVENTS_SQL)
    funnel = {"events": len(events), "symbols_seen": 0, "cells_attempted": 0,
              "cells_ok": 0, "cells_dropped": 0, "drop_reasons": {},
              "groups": 0, "groups_written": 0, "groups_suppressed": 0}
    if not events:
        funnel["diagnosis"] = "the event library is empty — run Phase 1 first"
        return {"funnel": funnel, "rows": []}

    # Group analogs by (symbol, class); load each symbol's series once.
    groups: dict = {}
    for e in events:
        for sym in (e["linked_symbols"] or []):
            groups.setdefault((sym, e["event_class"]), []).append(e)

    symbols = sorted({s for s, _ in groups})
    funnel["symbols_seen"] = len(symbols)
    series_cache: dict = {}
    for sym in symbols:
        rows = await conn.fetch(_SERIES_SQL, sym)
        series_cache[sym] = [(r["ts"], float(r["price"])) for r in rows
                             if r["price"] is not None]

    now = datetime.now(timezone.utc)
    out = []
    for (sym, klass), evs in sorted(groups.items()):
        series = series_cache.get(sym) or []
        for h in horizons:
            funnel["groups"] += 1
            cells = []
            for e in evs:
                funnel["cells_attempted"] += 1
                cell = measure(series, e["occurred_at"], h)
                if not cell.get("ok"):
                    funnel["cells_dropped"] += 1
                    reason = cell.get("reason", "unknown")
                    funnel["drop_reasons"][reason] = \
                        funnel["drop_reasons"].get(reason, 0) + 1
                    continue
                funnel["cells_ok"] += 1
                cell["event_id"] = str(e["event_id"])
                cell["occurred_at"] = e["occurred_at"]
                cell["age_days"] = (now - e["occurred_at"]).total_seconds() / 86400.0
                cells.append(cell)

            agg = aggregate(cells, horizon=h)
            if agg is None:
                funnel["groups_suppressed"] += 1
                continue
            row = {"symbol": sym, "event_class": klass, "horizon_days": h, **agg}
            out.append(row)
            if write:
                await conn.execute(
                    _UPSERT, sym, klass, h, agg["n_analogs"], agg["n_exceeded"],
                    agg["sign_agreement"], agg["median_abs_z"], agg["dispersion"],
                    agg["recency_weight"], agg["signal_strength"],
                    agg["noise_floor"], json.dumps(agg["breakdown"]))
                funnel["groups_written"] += 1

    funnel["diagnosis"] = _diagnose(funnel)
    log.info("[ANALOG] reactions: %s", {k: v for k, v in funnel.items()
                                        if k != "drop_reasons"})
    return {"funnel": funnel, "rows": out}


def _diagnose(f: dict) -> str:
    if not f["events"]:
        return "the event library is empty"
    if not f["groups"]:
        return "no event carries a symbol — nothing to measure"
    if not f["cells_ok"]:
        top = max(f["drop_reasons"].items(), key=lambda kv: kv[1], default=None)
        return ("no analog could be measured at all"
                + (f"; commonest reason: {top[0]} ({top[1]})" if top else ""))
    if not f.get("groups_written") and f["groups_suppressed"]:
        return (f"every one of {f['groups_suppressed']} (symbol, class, horizon) "
                f"group fell below the {MIN_ANALOGS}-analog floor — the engine "
                f"has nothing to say yet, which is a valid output")
    return (f"{f['groups_written']} group(s) cleared the {MIN_ANALOGS}-analog "
            f"floor, {f['groups_suppressed']} suppressed")
