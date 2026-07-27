"""
workers/cooccurrence_backfill.py — one-off co-occurrence backfill (Engine V1, Step 3).

Rebuilds the trailing-window cooccurrence counts from domain_signals.

Standalone:
    python -m app.workers.cooccurrence_backfill                 # full 90-day rebuild
    python -m app.workers.cooccurrence_backfill --limit 100     # small verify batch first
    python -m app.workers.cooccurrence_backfill --days 30       # narrower window

--limit N processes only the N most-recent signals additively (for verifying a
small real-Postgres run); the full run (no --limit) clears the window and rebuilds
it idempotently, so any prior --limit experimentation is superseded.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from app.db import db
from app.spie.graph import cooccurrence

log = logging.getLogger("sherbyte.worker.cooc_backfill")


async def run(days: int = 90, limit: int | None = None, npmi: bool = True) -> dict:
    async with db.acquire() as conn:
        result = await cooccurrence.backfill(conn, days=days, limit=limit)
        if npmi:
            result["npmi_pairs"] = await cooccurrence.compute_npmi(conn, days=days)
        return result


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Backfill entity co-occurrence from domain_signals.")
    parser.add_argument("--days", type=int, default=90, help="trailing window in days (default 90)")
    parser.add_argument("--limit", type=int, default=None,
                        help="process only the N most-recent signals (additive verify batch)")
    parser.add_argument("--no-npmi", action="store_true", help="skip the NPMI recompute step")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(days=args.days, limit=args.limit, npmi=not args.no_npmi)
        log.info("backfill complete: %s", result)
        print(result)
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
