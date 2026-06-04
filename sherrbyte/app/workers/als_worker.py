"""
workers/als_worker.py — Retrains the ALS collaborative-filter model.

Cron: nightly. Reads the signals matrix and rewrites als_user_factors /
als_item_factors, which the hybrid scorer consumes for the collab term.

Standalone:  python -m app.workers.als_worker
"""

from __future__ import annotations

import asyncio
import logging

from app.recommender import als

log = logging.getLogger("sherbyte.worker.als")


async def run(ctx=None) -> dict:
    return await als.train()


async def _main() -> None:
    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        log.info("ALS retrain: %s", await run())
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
