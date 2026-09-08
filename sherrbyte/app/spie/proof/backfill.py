"""proof/backfill.py — run the four edges BACKWARDS over the whole history.

We already hold ~600 days of ticks and a ~368-day corpus. So instead of waiting
30 days forward, this evaluates every RIL session we have, logs every firing with
its real date, and then — for each firing — measures what RIL ACTUALLY DID over
the next 1/3/5/10 sessions and reports the hit rate against RIL's own normal
range. The comparison that keeps it honest is the BASE RATE: how often ANY
session (firing or not) is followed by such a move. If the firing hit rate is no
better than the base rate, the edges are no better than chance, and this says so.

NO LOOKAHEAD anywhere it would matter:
  - a signal's move is scored only against sessions before it (signals.daily_move)
  - RIL's forward move is normalised by volatility before the firing
    (reaction.measure's trailing window). The denominator never contains the
    move it is judging.
"""

from __future__ import annotations

import bisect
import logging
import os
import sys
from datetime import timedelta

from app.spie.analog import reaction as R
from app.spie.proof import data as D
from app.spie.proof import signals as S
from app.spie.proof.evaluator import evaluate_session

log = logging.getLogger("sherbyte.proof.backfill")

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))

Z_EXCEEDED = R.Z_EXCEEDED           # "beyond RIL's own normal range", one meaning


# ─── ensure RIL has a price series ───────────────────────────────────────────
async def ensure_ril_ticks(conn, *, days: int = 800) -> dict:
    """Fetch RELIANCE.NS daily closes and upsert them into market_ticks.

    RIL is the one instrument the proof adds — Brent and USD/INR are already in
    the tick universe, but no Indian single stock was. It is stored as market_type
    'stocks' in the SAME table so reaction.measure reads it identically. Reuses
    market_ticks.yahoo_daily rather than carrying a second copy of the fetch.
    """
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    import httpx
    import market_ticks

    await market_ticks.ensure_schema(conn)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            series = await market_ticks.yahoo_daily(client, S.RIL_SYMBOL, days)
        except Exception as e:
            return {"ok": False, "symbol": S.RIL_SYMBOL, "error": str(e)}
    written = await market_ticks.write_ticks(conn, S.RIL_SYMBOL, "stocks", series)
    return {"ok": True, "symbol": S.RIL_SYMBOL, "written": written,
            "days_fetched": len(series)}


# ─── the backwards evaluation ────────────────────────────────────────────────
def _edge_symbols(edges: list) -> list:
    syms = set()
    for e in edges:
        for k in e["signal_keys"]:
            if S.is_price_signal(k):
                syms.add(S.PRICE_SIGNALS[k][0])
    return sorted(syms)


async def evaluate_history(conn) -> dict:
    """Walk every RIL session, evaluate the four edges, collect firings.

    Returns the firings (as evaluator.Firing) plus the RIL series and every
    session, so the caller can both write the log and measure forward reactions
    without re-reading anything.
    """
    edges = await D.load_edges(conn)
    if not edges:
        return {"ok": False, "detail": "ril_edges is empty — apply migration 024"}

    series_by_symbol = {sym: await D.load_series(conn, sym)
                        for sym in _edge_symbols(edges)}
    ril_series = series_by_symbol.get(S.RIL_SYMBOL) or []
    if not ril_series:
        return {"ok": False, "detail": "no RELIANCE.NS ticks — run ensure_ril_ticks first"}

    # The whole financial corpus over the RIL span, read ONCE and windowed in
    # memory. A per-session query would be ~600 round-trips for no benefit.
    lo = ril_series[0][0] - timedelta(hours=S.WINDOW_HOURS)
    hi = ril_series[-1][0] + timedelta(hours=S.WINDOW_HOURS)
    corpus = await D.financial_articles(conn, lo, hi)
    for a in corpus:
        a["_dt"] = _as_dt(a.get("published_at"))
    corpus = [a for a in corpus if a["_dt"] is not None]
    corpus.sort(key=lambda a: a["_dt"])
    corpus_dts = [a["_dt"] for a in corpus]

    firings = []
    for ts, _price in ril_series:
        window = _window_articles(corpus, corpus_dts, ts)
        firings.extend(evaluate_session(edges, series_by_symbol, ts, window))

    return {"ok": True, "edges": edges, "ril_series": ril_series,
            "sessions": len(ril_series), "corpus": len(corpus), "firings": firings}


def _window_articles(corpus: list, corpus_dts: list, ts) -> list:
    # Trailing, like the price window and for the same reason: the daily job can
    # only ever see news at or before the session, so the backwards run must not
    # give itself future headlines the live job would never have.
    lo = ts - timedelta(hours=S.WINDOW_HOURS)
    i = bisect.bisect_left(corpus_dts, lo)
    j = bisect.bisect_right(corpus_dts, ts)
    return corpus[i:j]


def _as_dt(v):
    """published_at -> naive UTC datetime, from either a timestamptz (post-018)
    or the ISO TEXT the sqlite-shaped schema stores. None if unparseable."""
    from datetime import datetime, timezone
    if isinstance(v, datetime):
        return (v.astimezone(timezone.utc).replace(tzinfo=None)
                if v.tzinfo else v)
    if not v:
        return None
    s = str(v).strip().replace("T", " ")
    for fmt, width in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16),
                       ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(s[:width], fmt)
        except ValueError:
            continue
    return None


# ─── forward reaction: what RIL did next, and the honest hit rate ────────────
def _forward(ril_series: list, event_ts) -> dict:
    """{horizon: {"z": float, "exceeded": bool}} for RIL after `event_ts`.

    Uses reaction.measure, so the volatility RIL is judged against is strictly
    pre-event. Horizons with too little future are simply absent.
    """
    out = {}
    for h in R.HORIZONS:
        cell = R.measure(ril_series, event_ts, h)
        if cell.get("ok"):
            out[h] = {"z": cell["z"], "exceeded": abs(cell["z"]) >= Z_EXCEEDED}
    return out


def hit_rates(firings: list, ril_series: list) -> dict:
    """Firing hit rate vs base rate, per horizon.

    firing hit rate   among firings measurable at h, fraction where RIL exceeded
    base rate         among ALL sessions measurable at h, fraction where RIL
                      exceeded — i.e. what a coin would score
    lift              hit_rate - base_rate (the honest number; ~0 means chance)
    """
    # Base rate over every session.
    base = {h: [0, 0] for h in R.HORIZONS}      # [exceeded, measurable]
    for ts, _p in ril_series:
        for h, cell in _forward(ril_series, ts).items():
            base[h][1] += 1
            base[h][0] += 1 if cell["exceeded"] else 0

    fire = {h: [0, 0] for h in R.HORIZONS}
    for f in firings:
        event_ts = _session_ts(ril_series, f.event_date)
        if event_ts is None:
            continue
        for h, cell in _forward(ril_series, event_ts).items():
            fire[h][1] += 1
            fire[h][0] += 1 if cell["exceeded"] else 0

    def rate(pair):
        return (pair[0] / pair[1]) if pair[1] else None

    out = {}
    for h in R.HORIZONS:
        fr, br = rate(fire[h]), rate(base[h])
        out[h] = {
            "firings_measured": fire[h][1], "firing_hits": fire[h][0],
            "firing_hit_rate": round(fr, 3) if fr is not None else None,
            "base_measured": base[h][1], "base_hits": base[h][0],
            "base_rate": round(br, 3) if br is not None else None,
            "lift": round(fr - br, 3) if (fr is not None and br is not None) else None,
        }
    return out


def _session_ts(ril_series: list, event_date):
    for ts, _p in ril_series:
        d = ts.date() if hasattr(ts, "date") else ts
        if d == event_date:
            return ts
    return None


# ─── writing the permanent log ───────────────────────────────────────────────
_INSERT = """
INSERT INTO sherrbyte_app.ril_proof_log
    (edge_key, event_date, run_kind, moved_signals, evidence_article_ids,
     signal_strength, noise_floor, rendered, fwd_z, fwd_exceeded)
VALUES ($1, $2::date, $3, $4::text[], $5::bigint[], $6, $7, $8, $9::jsonb, $10::jsonb)
ON CONFLICT (edge_key, event_date) DO NOTHING
"""


async def write_firings(conn, firings: list, ril_series: list, *,
                        run_kind: str = "backfill") -> int:
    """Append every firing. ON CONFLICT DO NOTHING — never overwrites, never
    prunes; a re-run of the same window is a no-op."""
    import json
    written = 0
    for f in firings:
        event_ts = _session_ts(ril_series, f.event_date)
        fwd = _forward(ril_series, event_ts) if event_ts is not None else {}
        fwd_z = {str(h): round(c["z"], 4) for h, c in fwd.items()}
        fwd_ex = {str(h): c["exceeded"] for h, c in fwd.items()}
        r = await conn.execute(
            _INSERT, f.edge_key, f.event_date, run_kind, f.moved_signals,
            f.evidence_article_ids, int(f.signal_strength), int(f.noise_floor),
            bool(f.rendered),
            json.dumps(fwd_z) if fwd_z else None,
            json.dumps(fwd_ex) if fwd_ex else None)
        written += 1 if r.endswith("1") else 0
    return written


async def run(conn, *, days: int = 800, fetch_ril: bool = True) -> dict:
    """Full backwards run: ensure RIL prices, evaluate history, log firings,
    and return the honest hit-rate report."""
    ril = await ensure_ril_ticks(conn, days=days) if fetch_ril else {"ok": True, "skipped": True}
    ev = await evaluate_history(conn)
    if not ev.get("ok"):
        return {"ok": False, "ril_ticks": ril, **ev}

    firings = ev["firings"]
    written = await write_firings(conn, firings, ev["ril_series"], run_kind="backfill")
    rates = hit_rates(firings, ev["ril_series"])

    per_edge = {}
    for f in firings:
        per_edge[f.edge_key] = per_edge.get(f.edge_key, 0) + 1

    return {
        "ok": True, "ril_ticks": ril, "sessions": ev["sessions"],
        "financial_articles": ev["corpus"], "firings": len(firings),
        "firings_written": written, "firings_per_edge": per_edge,
        "hit_rates": rates,
    }


async def run_daily(conn, *, on_date=None) -> dict:
    """The morning job: append firings for ONE session (the most recent by
    default), forward outcome left NULL because the future isn't in yet.

    Same evaluator, same log, same append-only guarantee. Distinct from the
    backwards run only in that it looks at one date and does not measure a
    forward reaction it cannot yet have.
    """
    ril = await ensure_ril_ticks(conn, days=120)
    ev = await evaluate_history(conn)
    if not ev.get("ok"):
        return {"ok": False, "ril_ticks": ril, **ev}

    ril_series = ev["ril_series"]
    if on_date is None:
        target_ts = ril_series[-1][0]           # the latest completed session
    else:
        target_ts = _session_ts(ril_series, on_date)
        if target_ts is None:
            return {"ok": False, "detail": f"no RIL session on {on_date}"}

    todays = [f for f in ev["firings"]
              if (f.event_date == (target_ts.date() if hasattr(target_ts, "date")
                                   else target_ts))]
    written = await write_firings(conn, todays, ril_series, run_kind="daily")
    return {"ok": True, "ril_ticks": ril,
            "session": str(target_ts.date() if hasattr(target_ts, "date") else target_ts),
            "firings": len(todays), "firings_written": written,
            "edges": [f.edge_key for f in todays]}
