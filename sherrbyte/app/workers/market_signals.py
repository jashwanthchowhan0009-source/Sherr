"""
workers/market_signals.py — write daily market moves into domain_signals (Sherr-I Part B).

Until now domain_signals held only news. Without market signals there is nothing for
a news↔market detector to join against, so this is the prerequisite for Part C.

Per instrument, one signal:
    domain    = "market"
    entity    = the instrument's display name ("NIFTY 50", "Gold", "WTI Crude")
    magnitude = abs(% change)
    direction = sign(% change)  (+1 up / -1 down / 0 flat)
    source_id = "yahoo:<asset_class>" so the asset class survives for later filtering
    ref_id    = "market:<SYMBOL>:<UTC date>"  → one signal per instrument per day

Quotes come from the same free endpoints the app's market cards use (Yahoo chart API
for everything except crypto, CoinGecko for crypto) — no new infrastructure, no keys.

One run writes ONE day. market_reaction needs min_history+1 = 6 daily
observations per instrument before it will test a move for significance, so a
fresh database is six days away from its first insight. --from-ticks closes that
gap: it replays the daily closes already stored in sherrbyte_app.market_ticks
(see market_ticks.py / scripts/backfill_ticks.py) as historical signals of
exactly the shape below, so the history is there immediately.

Standalone:
    python -m app.workers.market_signals            # fetch + persist today
    python -m app.workers.market_signals --dry-run  # fetch + print, write nothing
    python -m app.workers.market_signals --from-ticks           # replay 90 days
    python -m app.workers.market_signals --from-ticks --days 30
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import httpx

log = logging.getLogger("sherbyte.worker.market_signals")


def _db():
    """The engine's pool, imported on first use rather than at module import.

    `from app.db import db` pulls app.config, which needs pydantic-settings —
    not in the ROOT service's requirements.txt. Keeping it lazy is what lets
    main.py's /admin/replay-signals import this module and call
    backfill_from_ticks() with a connection of its own, instead of the endpoint
    reimplementing the Signal shape (which is the whole thing that must not
    drift between the two paths).
    """
    from app.db import db
    return db


# Live state of the replay in flight, so a caller with no shell can poll it.
# Same shape as market_ticks.progress(). Reset at the start of every run.
PROGRESS: dict = {}


def progress() -> dict:
    """A snapshot of the replay in flight (or the last one). Empty before any."""
    p = dict(PROGRESS)
    if p.get("started_at"):
        end = p.get("finished_at") or time.time()
        p["elapsed_s"] = round(end - p["started_at"], 1)
    return p

# (symbol, display name, asset class). Display names double as entity names, so they
# are chosen to be distinctive — the junk filter drops bare common nouns.
INSTRUMENTS: list[tuple[str, str, str]] = [
    ("^NSEI",     "NIFTY 50",     "stocks"),
    ("^BSESN",    "Sensex",       "stocks"),
    ("^IXIC",     "Nasdaq",       "stocks"),
    ("GC=F",      "Gold",         "metals"),
    ("SI=F",      "Silver",       "metals"),
    ("CL=F",      "WTI Crude",    "commodities"),
    ("BZ=F",      "Brent Crude",  "commodities"),
    ("NG=F",      "Natural Gas",  "commodities"),
    ("USDINR=X",  "USD/INR",      "forex"),
    ("EURUSD=X",  "EUR/USD",      "forex"),
    ("^TNX",      "US 10Y Yield", "rates"),
]

# CoinGecko id → display name (crypto isn't on the Yahoo chart endpoint we use).
CRYPTO = {"bitcoin": "Bitcoin", "ethereum": "Ethereum"}

_YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
_COINGECKO = "https://api.coingecko.com/api/v3/coins/markets"


async def _yahoo_change(client: httpx.AsyncClient, symbol: str) -> float | None:
    """Percent change vs the previous close, or None if unavailable."""
    try:
        r = await client.get(_YAHOO.format(sym=symbol),
                             params={"range": "5d", "interval": "1d"},
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if r.status_code != 200:
            return None
        meta = (r.json().get("chart", {}).get("result") or [{}])[0].get("meta", {})
        price = meta.get("regularMarketPrice")
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if not price or not prev:
            return None
        return (price - prev) / prev * 100.0
    except Exception as e:
        log.warning("yahoo %s failed: %s", symbol, e)
        return None


async def _crypto_changes(client: httpx.AsyncClient) -> dict[str, float]:
    try:
        r = await client.get(_COINGECKO, params={
            "vs_currency": "usd", "ids": ",".join(CRYPTO),
            "price_change_percentage": "24h"}, timeout=15)
        if r.status_code != 200:
            return {}
        return {c["id"]: float(c.get("price_change_percentage_24h") or 0.0)
                for c in r.json()}
    except Exception as e:
        log.warning("coingecko failed: %s", e)
        return {}


async def collect() -> list[dict]:
    """Fetch every instrument's daily move. Pure I/O — no DB writes."""
    out: list[dict] = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(
            *[_yahoo_change(client, sym) for sym, _, _ in INSTRUMENTS],
            return_exceptions=True)
        for (sym, name, cls), pct in zip(INSTRUMENTS, results):
            if isinstance(pct, (int, float)):
                out.append({"symbol": sym, "name": name, "asset_class": cls,
                            "change_pct": float(pct)})
        for cid, pct in (await _crypto_changes(client)).items():
            out.append({"symbol": cid.upper(), "name": CRYPTO[cid],
                        "asset_class": "crypto", "change_pct": float(pct)})
    return out


async def run(dry_run: bool = False) -> dict:
    from app.spie.knowledge.adapters.base import direction, clamp
    from app.spie.knowledge.signals import persist_signals
    from app.models.signal import Signal, SignalEntity

    quotes = await collect()
    if not quotes:
        return {"collected": 0, "written": 0,
                "detail": "no market data returned (upstream unreachable?)"}

    today = datetime.now(timezone.utc)
    sigs = []
    for q in quotes:
        pct = q["change_pct"]
        sigs.append(Signal(
            entities=[SignalEntity(name=q["name"], type="MISC")],
            domain="market",                       # one domain for all instruments
            ts=today,
            magnitude=abs(pct),
            direction=direction(pct),
            sentiment=None,
            source_id=f"yahoo:{q['asset_class']}",
            credibility=0.9,                       # mechanical feed, not editorial
            confidence=clamp(0.6 + min(abs(pct), 10.0) / 20.0),
            # One signal per instrument per UTC day — re-running is safe.
            ref_id=f"market:{q['symbol']}:{today.date().isoformat()}",
        ))

    if dry_run:
        return {"collected": len(quotes), "written": 0, "dry_run": True,
                "quotes": [{"name": q["name"], "change_pct": round(q["change_pct"], 2)}
                           for q in quotes]}

    db = _db()
    async with db.acquire() as conn:
        # Idempotent per day: drop today's rows for these instruments first.
        await conn.execute(
            "DELETE FROM domain_signals WHERE domain='market' AND ref_id = ANY($1::text[])",
            [s.ref_id for s in sigs])
        written = await persist_signals(conn, sigs, conn_factory=db.acquire)

    total = await db.fetchval("SELECT COUNT(*) FROM domain_signals WHERE domain='market'")
    return {
        "collected": len(quotes), "written": written,
        "market_signals_total": int(total or 0),
        "moves": sorted(({"name": q["name"], "change_pct": round(q["change_pct"], 2)}
                         for q in quotes),
                        key=lambda x: abs(x["change_pct"]), reverse=True)[:10],
    }


# ─── history replay from the price store ─────────────────────────────────────
# market_ticks stores every symbol /markets quotes; INSTRUMENTS/CRYPTO above name
# the subset the engine has entities for. Only the intersection is replayed —
# giving "^FTSE" or "XOM" a display name here would be inventing entities the
# daily path never creates, and the two would then resolve differently.
#
# Keys are the symbols as market_ticks stores them (Yahoo symbols; CoinGecko ids
# for coins). Values match what collect() emits, so the Signal built below is
# indistinguishable from the one the daily run writes — including ref_id, which
# is what makes the two paths idempotent against each other.
_TICK_SOURCES: dict[str, tuple[str, str, str]] = {
    **{sym: (sym, name, cls) for sym, name, cls in INSTRUMENTS},
    **{cid: (cid.upper(), name, "crypto") for cid, name in CRYPTO.items()},
}

# Chunk size for the persist loop — only so a poller sees the counter move.
_CHUNK = 100

_TICKS_SQL = """
    SELECT symbol, ts, change_24h
      FROM sherrbyte_app.market_ticks
     WHERE symbol = ANY($1::text[])
       AND change_24h IS NOT NULL
       AND ts >= now() - ($2 || ' days')::interval
     ORDER BY symbol, ts
"""


async def backfill_from_ticks(days: int = 90, dry_run: bool = False,
                              conn_factory=None) -> dict:
    """Replay stored daily closes as domain='market' signals.

    Same Signal shape, same ref_id, same delete-then-insert as run(), so the
    replay and the daily job can be run in any order any number of times — a day
    already present is replaced, never duplicated.

    Rows with change_24h IS NULL are skipped rather than treated as flat: that is
    the first bar of a series (no previous close) or an unrepresentable print,
    and a fabricated 0.0% would read to the detector as a genuinely flat day.

    `conn_factory` is an async context-manager factory (db.acquire, or an
    asyncpg pool's .acquire). It defaults to the engine's own pool; main.py's
    /admin/replay-signals passes the pool it already holds, so the endpoint runs
    THIS function rather than carrying a second copy of the Signal shape.
    """
    from app.spie.knowledge.adapters.base import direction, clamp
    from app.spie.knowledge.signals import persist_signals
    from app.models.signal import Signal, SignalEntity

    acquire = conn_factory or _db().acquire
    symbols = sorted(_TICK_SOURCES)

    PROGRESS.clear()
    PROGRESS.update({"running": True, "started_at": time.time(), "finished_at": None,
                     "phase": "reading market_ticks", "days": days, "dry_run": dry_run,
                     "signals": 0, "written": 0})

    try:
        async with acquire() as conn:
            try:
                rows = await conn.fetch(_TICKS_SQL, symbols, str(int(days)))
            except Exception as e:
                return {"collected": 0, "written": 0,
                        "detail": f"cannot read sherrbyte_app.market_ticks: {e} — "
                                  f"run scripts/backfill_ticks.py first"}

        PROGRESS["phase"] = "building signals"
        sigs, per_symbol = [], {}
        for r in rows:
            signal_symbol, name, cls = _TICK_SOURCES[r["symbol"]]
            pct = float(r["change_24h"])
            day = r["ts"].astimezone(timezone.utc)
            sigs.append(Signal(
                entities=[SignalEntity(name=name, type="MISC")],
                domain="market",
                ts=day,
                magnitude=abs(pct),
                direction=direction(pct),
                sentiment=None,
                source_id=f"yahoo:{cls}",
                credibility=0.9,
                confidence=clamp(0.6 + min(abs(pct), 10.0) / 20.0),
                ref_id=f"market:{signal_symbol}:{day.date().isoformat()}",
            ))
            per_symbol[name] = per_symbol.get(name, 0) + 1
        PROGRESS.update({"signals": len(sigs), "per_instrument": per_symbol})

        if not sigs:
            return {"collected": 0, "written": 0,
                    "detail": "market_ticks has no usable rows for the engine's "
                              "instruments — run scripts/backfill_ticks.py "
                              "(or GET /admin/backfill-ticks) first",
                    "symbols_looked_for": symbols}

        if dry_run:
            return {"collected": len(sigs), "written": 0, "dry_run": True,
                    "days": days, "per_instrument": per_symbol}

        written = 0
        async with acquire() as conn:
            # Same idempotency as run(), over the whole replayed window.
            PROGRESS["phase"] = "clearing replaced days"
            await conn.execute(
                "DELETE FROM domain_signals WHERE domain='market' AND ref_id = ANY($1::text[])",
                [s.ref_id for s in sigs])
            # Persisted in chunks purely so the counter moves for a poller;
            # persist_signals loops one signal at a time either way.
            PROGRESS["phase"] = "persisting"
            for i in range(0, len(sigs), _CHUNK):
                written += await persist_signals(
                    conn, sigs[i:i + _CHUNK], conn_factory=acquire)
                PROGRESS["written"] = written

        # What the detector actually gates on: distinct days per instrument.
        PROGRESS["phase"] = "measuring history depth"
        async with acquire() as conn:
            depth = await conn.fetch(
                """
                SELECT COUNT(DISTINCT (ts AT TIME ZONE 'UTC')::date) AS days
                  FROM domain_signals
                 WHERE domain='market'
                 GROUP BY entity_ids
                 ORDER BY 1 DESC
                """)
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM domain_signals WHERE domain='market'")
        deepest = int(depth[0]["days"]) if depth else 0
        result = {
            "collected": len(sigs), "written": written, "days": days,
            "per_instrument": per_symbol,
            "market_signals_total": int(total or 0),
            "deepest_instrument_days": deepest,
            # min_history + 1 in market_reaction.run().
            "enough_for_market_reaction": deepest >= 6,
        }
        PROGRESS["phase"] = "done"
        return result
    finally:
        PROGRESS["running"] = False
        PROGRESS["finished_at"] = time.time()


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Write market moves into domain_signals.")
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch and print quotes without writing signals")
    parser.add_argument("--from-ticks", action="store_true",
                        help="replay stored daily closes from sherrbyte_app.market_ticks "
                             "instead of fetching today's quotes")
    parser.add_argument("--days", type=int, default=90,
                        help="with --from-ticks: how far back to replay (default 90)")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        if args.from_ticks:
            result = await backfill_from_ticks(days=args.days, dry_run=args.dry_run)
            log.info("market_signals backfill: %s", result)
        else:
            result = await run(dry_run=args.dry_run)
            log.info("market_signals: %s", result)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
