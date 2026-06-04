"""
workers/embed_worker.py — Vectorizes any info objects missing an embedding,
then threads them into the story graph.

Cron: every ~5 min. Keeps embedding/threading off the request path.

Standalone:  python -m app.workers.embed_worker
"""

from __future__ import annotations

import asyncio
import logging

from app.pipeline.connector import connect_pending
from app.pipeline.embedder import embed_pending

log = logging.getLogger("sherbyte.worker.embed")


async def run(ctx=None) -> dict:
    embedded = await embed_pending(limit=256)
    threaded = await connect_pending(limit=256)
    return {"embedded": embedded, "threaded": threaded}


async def _main() -> None:
    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        log.info("embed complete: %s", await run())
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
