"""
workers/instrument_map.py — seed and inspect the news↔market keyword links.

    python -m app.workers.instrument_map              # sync seeds, print summary
    python -m app.workers.instrument_map --report     # sync + per-instrument coverage
    python -m app.workers.instrument_map --dry-run    # print what would be seeded

The report is the diagnostic tool: for every instrument it shows how many mapped
keywords resolve to entities that actually exist in the corpus, and how many news
signals those entities appear in. That is what separates "no mappings" from
"mappings exist but no news overlapped the window" — the two look identical from a
detector that just returns 0.

Two tables are written, each in the role it was designed for:

  instrument_keywords — the RELATION. Many keywords ↔ many instruments
                        ("Iran", "OPEC", "Strait of Hormuz" → WTI Crude).
                        This is what the reasoning engine joins on.
  entity_ticker_map   — the IDENTITY. One symbol → one entity ("GC=F" *is* Gold).
                        Filled here too so the symbol linkage is real, but it
                        cannot express the relation above: its primary key is
                        (symbol, domain) with a single entity_id.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from app.db import db
from app.spie.knowledge import instrument_map
from app.spie.knowledge.entity_resolver import resolve
from app.workers.market_signals import CRYPTO, INSTRUMENTS

log = logging.getLogger("sherbyte.worker.instrument_map")


async def sync_ticker_map(conn) -> int:
    """Record symbol → instrument-entity identity for every tracked instrument.

    create=False: the entity exists only once market_signals has actually written
    a signal for that instrument. Until then the symbol stays unmapped rather than
    conjuring an entity that no observation backs.
    """
    rows = [(sym, cls, name) for sym, name, cls in INSTRUMENTS]
    rows += [(cid.upper(), "crypto", name) for cid, name in CRYPTO.items()]

    written = 0
    for symbol, domain, name in rows:
        eid = await resolve(conn, name, "MISC", create=False)
        if eid is None:
            continue
        await conn.execute(
            """
            INSERT INTO entity_ticker_map (symbol, domain, entity_id, display_name)
            VALUES ($1, $2, $3::uuid, $4)
            ON CONFLICT (symbol, domain) DO UPDATE
                SET entity_id = EXCLUDED.entity_id,
                    display_name = EXCLUDED.display_name
            """,
            symbol, domain, eid, name)
        written += 1
    return written


async def run(*, report: bool = False, dry_run: bool = False) -> dict:
    if dry_run:
        rows = instrument_map.seed_rows()
        return {"dry_run": True, "instruments": len(instrument_map.SEED),
                "keyword_links": len(rows),
                "sample": [{"instrument": i, "keyword": k} for i, k, _ in rows[:15]]}

    async with db.acquire() as conn:
        summary = await instrument_map.sync_seeds(conn)
        summary["ticker_map_rows"] = await sync_ticker_map(conn)
        if report:
            cov = await instrument_map.coverage(conn)
            summary["coverage"] = cov
            summary["instruments_with_zero_resolved"] = [
                c["instrument"] for c in cov if c["resolved_entities"] == 0]
            summary["instruments_with_reachable_news"] = [
                c["instrument"] for c in cov if c["news_signals_reachable"] > 0]
    return summary


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed and inspect news-keyword ↔ market-instrument links.")
    parser.add_argument("--report", action="store_true",
                        help="print per-instrument keyword resolution coverage")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the seed set without writing anything")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(report=args.report, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
