"""
workers/reset_spie.py — explicit, standalone reset of the SPIE derived tables.

Resetting is DESTRUCTIVE, so it lives in its own command instead of being a side
effect of a backfill: a backfill that crashes half-way (flaky pooler) must never
destroy the graph or the insights you already have.

What it clears (signal-derived, rebuildable from articles/info_objects):
    entities, entity_aliases, article_fingerprints, domain_signals,
    cooccurrence, cooccurrence_events   (+ restarts article_cluster_seq)

What it NEVER clears unless you ask explicitly:
    insights  — detector output and the user-visible product. Detectors rewrite
                their own rows via `signature`, so a rebuild refreshes them anyway.

Standalone:
    python -m app.workers.reset_spie --yes                 # signal tables only
    python -m app.workers.reset_spie --yes --with-insights # also wipe insights
    python -m app.workers.reset_spie                       # dry run: just counts
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from app.db import db
from app.workers.signals_backfill import _DERIVED_TABLES, reset_derived

log = logging.getLogger("sherbyte.worker.reset_spie")

_COUNT_TABLES = _DERIVED_TABLES + ["insights"]


async def counts() -> dict:
    out = {}
    for t in _COUNT_TABLES:
        try:
            out[t] = int(await db.fetchval(f"SELECT COUNT(*) FROM {t}") or 0)
        except Exception as e:            # table may not exist yet
            out[t] = f"n/a ({e.__class__.__name__})"
    return out


async def run(confirm: bool = False, with_insights: bool = False) -> dict:
    before = await counts()
    if not confirm:
        return {"dry_run": True, "would_truncate": _DERIVED_TABLES,
                "would_truncate_insights": with_insights, "counts": before,
                "hint": "re-run with --yes to actually reset"}

    async with db.acquire() as conn:
        await reset_derived(conn)
        if with_insights:
            log.warning("reset_spie: TRUNCATE insights (explicitly requested)")
            await conn.execute("TRUNCATE insights RESTART IDENTITY CASCADE")

    return {"dry_run": False, "truncated": _DERIVED_TABLES,
            "insights_truncated": with_insights,
            "counts_before": before, "counts_after": await counts()}


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset the SPIE derived tables (destructive; explicit by design).")
    parser.add_argument("--yes", action="store_true",
                        help="actually perform the reset (without it this is a dry run)")
    parser.add_argument("--with-insights", action="store_true",
                        help="ALSO wipe the insights table (detector output) — rarely wanted")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(confirm=args.yes, with_insights=args.with_insights)
        log.info("reset_spie: %s", result)
        print(result)
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
