"""
discovery/tick_anomaly.py — robust anomaly detection over stored daily closes.

WHY THIS IS SEPARATE FROM observation.py. That detector scores news VOLUME out of
`domain_signals`: an EWMA baseline, a minimum of five buckets, and a threshold of
2.0. All three are right for story counts and wrong for prices.

  centre     EWMA follows a run of large moves, so a volatile week stops looking
             volatile exactly when it matters. The median does not move.
  input      A price LEVEL is not comparable across days; the daily RETURN is.
  window     Five observations is not a distribution. Thirty trading days is a
             baseline; forty stored closes is the least that reliably yields it
             once weekends and holidays are removed.
  threshold  2.5, not 2.0 — returns are fat-tailed, and 2.0 on a MAD scale fires
             often enough on ordinary days to make the page noise.

A SHORT SERIES RETURNS None, NEVER A SCORE. A z computed from eleven points is
arithmetic, not evidence, and the whole engine downstream treats a score as a
reason to call an LLM and render a card.

Stdlib only. Reads sherrbyte_app.market_ticks.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime

from app.spie.discovery.anomaly_math import mad, median, robust_z

log = logging.getLogger("sherbyte.detectors.tick_anomaly")

# ─── thresholds ──────────────────────────────────────────────────────────────
# |z| at or above this is a trigger.
Z_THRESHOLD = float(os.getenv("SHERR_I_TICK_Z_THRESHOLD", "2.5"))
# Trading days of history the z is measured against.
WINDOW = int(os.getenv("SHERR_I_TICK_WINDOW", "30"))
# Stored closes required before a symbol is scoreable at all. Below this the
# detector returns None — see the module docstring.
MIN_OBSERVATIONS = int(os.getenv("SHERR_I_TICK_MIN_OBS", "40"))
# A MAD at or below this means the window is essentially flat. Dividing by it
# turns rounding noise into a 40-sigma event, so the symbol is skipped instead.
MIN_MAD = 1e-9

TABLE = "sherrbyte_app.market_ticks"


@dataclass(frozen=True)
class AnomalyResult:
    symbol: str
    market_type: str
    value: float            # the close that triggered
    pct_change: float       # its daily return, in percent
    z: float                # modified z of that return against the window
    window_n: int           # observations the z was measured against
    direction: int          # +1 up, -1 down
    ts: datetime

    def as_dict(self) -> dict:
        d = asdict(self)
        d["ts"] = self.ts.isoformat() if self.ts else None
        return d


def daily_returns(closes: list) -> list:
    """Percent change between consecutive closes. n closes -> n-1 returns."""
    out = []
    prev = None
    for c in closes:
        c = float(c)
        if prev not in (None, 0):
            out.append((c - prev) / prev * 100.0)
        prev = c
    return out


def score_series(symbol: str, market_type: str, rows: list):
    """Score the most recent close in `rows` (oldest first).

    `rows` are (ts, price). Returns an AnomalyResult, or None when the series is
    too short, flat, or the move is not extreme enough.
    """
    if not rows or len(rows) < MIN_OBSERVATIONS:
        return None

    closes = [float(p) for _, p in rows]
    returns = daily_returns(closes)
    if len(returns) < 2:
        return None

    latest = returns[-1]
    # The window EXCLUDES the day being scored: a baseline that contains the
    # observation pulls itself toward it and understates the deviation.
    history = returns[-(WINDOW + 1):-1]
    if len(history) < 2 or mad(history) <= MIN_MAD:
        return None

    z = robust_z(latest, history)
    if abs(z) < Z_THRESHOLD:
        return None

    ts, price = rows[-1]
    return AnomalyResult(
        symbol=symbol, market_type=market_type, value=round(float(price), 4),
        pct_change=round(latest, 4), z=round(z, 3), window_n=len(history),
        direction=1 if latest > 0 else -1, ts=ts,
    )


# ─── data access ─────────────────────────────────────────────────────────────
_SERIES_SQL = f"""
SELECT symbol, market_type, ts, price
  FROM {TABLE}
 WHERE ts >= now() - ($1 || ' days')::interval
 ORDER BY symbol, ts
"""


async def load_series(conn, days: int = 200) -> dict:
    """{symbol: (market_type, [(ts, price), ...])} oldest first."""
    rows = await conn.fetch(_SERIES_SQL, str(int(days)))
    out: dict = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in out:
            out[sym] = (r["market_type"], [])
        out[sym][1].append((r["ts"], r["price"]))
    return out


async def scan(conn, *, days: int = 200) -> list:
    """Every symbol whose latest close triggers. Empty is the normal outcome."""
    series = await load_series(conn, days)
    hits = []
    for symbol, (market_type, rows) in series.items():
        r = score_series(symbol, market_type, rows)
        if r:
            hits.append(r)
    hits.sort(key=lambda a: abs(a.z), reverse=True)
    log.info("tick_anomaly: %d symbol(s) scanned, %d triggered", len(series), len(hits))
    return hits


async def coverage(conn, *, days: int = 200) -> dict:
    """Which symbols have enough history to be scored at all.

    This is the number to look at after a backfill: a symbol below
    MIN_OBSERVATIONS is not failing, it simply cannot be scored yet.
    """
    series = await load_series(conn, days)
    enough, short = [], []
    for symbol, (_, rows) in sorted(series.items()):
        (enough if len(rows) >= MIN_OBSERVATIONS else short).append(
            {"symbol": symbol, "closes": len(rows)})
    return {
        "symbols_total": len(series),
        "symbols_scoreable": len(enough),
        "min_observations": MIN_OBSERVATIONS,
        "too_short": short,
    }
