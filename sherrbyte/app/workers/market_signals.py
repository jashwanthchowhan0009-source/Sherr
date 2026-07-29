"""
workers/market_signals.py — write daily market moves into domain_signals (SPIE Part B).

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

Standalone:
    python -m app.workers.market_signals            # fetch + persist
    python -m app.workers.market_signals --dry-run  # fetch + print, write nothing
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from app.db import db

log = logging.getLogger("sherbyte.worker.market_signals")

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


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Write market moves into domain_signals.")
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch and print quotes without writing signals")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(dry_run=args.dry_run)
        log.info("market_signals: %s", result)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
