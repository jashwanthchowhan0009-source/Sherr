"""
workers/detectors.py — run the pattern detectors (Intelligence Engine V1, Step 4).

Standalone:
    python -m app.workers.detectors                 # run all detectors
    python -m app.workers.detectors --only emergence
Also invoked nightly by the in-process APScheduler (see app/main.py).
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from app.db import db
from app.detectors import REGISTRY

log = logging.getLogger("sherbyte.worker.detectors")


async def run(only: str | None = None) -> dict:
    """Run detectors and return {name: insights_written}."""
    results: dict[str, int] = {}
    async with db.acquire() as conn:
        for name, fn in REGISTRY.items():
            if only and name != only:
                continue
            try:
                results[name] = await fn(conn)
            except Exception as e:
                log.error("detector %s failed: %s", name, e, exc_info=True)
                results[name] = -1
    return results


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Run Intelligence Engine detectors.")
    parser.add_argument("--only", choices=sorted(REGISTRY), default=None,
                        help="run a single detector instead of all")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(only=args.only)
        log.info("detectors complete: %s", result)
        print(result)
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
