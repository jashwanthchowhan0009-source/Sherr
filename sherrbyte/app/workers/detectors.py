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
from app.spie.discovery import REGISTRY
from app.spie.graph import cooccurrence
from app.spie.decision import rules as decision_rules

log = logging.getLogger("sherbyte.worker.detectors")

# All runnable jobs: discovery detectors + the decision-engine chain evaluator.
_CHAIN = "cross_domain_chain"


async def run(only: str | None = None) -> dict:
    """Run detectors and return {name: insights_written}."""
    results: dict[str, int] = {}
    async with db.acquire() as conn:
        # Refresh NPMI first so detector ranking uses current association strengths.
        try:
            await cooccurrence.compute_npmi(conn)
        except Exception as e:
            log.warning("npmi refresh failed: %s", e)
        for name, fn in REGISTRY.items():
            if only and name != only:
                continue
            try:
                results[name] = await fn(conn)
            except Exception as e:
                log.error("detector %s failed: %s", name, e, exc_info=True)
                results[name] = -1
        # Decision Engine: cross-domain chain rules.
        if not only or only == _CHAIN:
            try:
                results[_CHAIN] = await decision_rules.run(conn)
            except Exception as e:
                log.error("decision rules failed: %s", e, exc_info=True)
                results[_CHAIN] = -1
    return results


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Run Intelligence Engine detectors.")
    parser.add_argument("--only", choices=sorted(list(REGISTRY) + [_CHAIN]), default=None,
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
