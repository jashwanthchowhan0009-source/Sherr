"""
workers/ingest_worker.py — Runs the full ingestion pipeline.

Cron: every 15 min (GitHub Actions or ARQ). Drives Collect → Dedup → Understand
→ Construct → Embed → Connect, then tops up any stragglers (embed/connect) so
the graph stays consistent even if a previous run was interrupted.

Standalone:  python -m app.workers.ingest_worker
"""

from __future__ import annotations

import asyncio
import logging

from app.pipeline import run_cycle
from app.pipeline.connector import connect_pending
from app.pipeline.embedder import embed_pending

log = logging.getLogger("sherbyte.worker.ingest")


async def run(ctx=None) -> dict:
    stats = await run_cycle()
    # Safety net for anything constructed-but-not-finished in earlier runs.
    stats["embedded_extra"] = await embed_pending(limit=256)
    stats["threaded_extra"] = await connect_pending(limit=256)
    return stats


async def _main() -> None:
    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run()
        log.info("ingest complete: %s", result)
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
