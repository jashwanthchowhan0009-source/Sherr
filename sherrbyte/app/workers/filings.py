"""workers/filings.py — ingest exchange & regulator filings. The cron entry point.

    python -m app.workers.filings              # ingest all four sources
    python -m app.workers.filings --only BSE   # one source
    python -m app.workers.filings --doctor      # fetch + report, write nothing

--doctor is the standalone twin of GET /admin/filing-doctor: it fetches each
source live and prints the shape report (status, sample raw, parsed vs failed)
without touching the database. Use it from the cron runner (which is NOT blocked
by the app sandbox) to verify a shape against production.

Runs after the detector pass in cron_detectors.yml. Adds ZERO provider calls.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from app.db import db

log = logging.getLogger("sherbyte.worker.filings")


async def run(*, only: str | None = None, doctor: bool = False) -> dict:
    if doctor:
        from app.spie.filings import doctor as D
        return await D.run(only=only)
    from app.spie.filings import ingest
    async with db.acquire() as conn:
        return await ingest.run(conn, only=only)


async def _main() -> int:
    ap = argparse.ArgumentParser(description="Ingest BSE/NSE/RBI/SEBI filings.")
    ap.add_argument("--only", default=None,
                    help="ingest a single source by name (BSE|NSE|RBI|SEBI)")
    ap.add_argument("--doctor", action="store_true",
                    help="fetch + report each source's shape, write nothing")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(only=args.only, doctor=args.doctor)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("ok", True) else 1
    finally:
        await teardown()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
