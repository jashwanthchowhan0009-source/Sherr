"""workers/proof.py — the RIL proof run's entry point.

    python -m app.workers.proof --daily        # the morning job: log today's firings
    python -m app.workers.proof --backfill      # run the whole history backwards
    python -m app.workers.proof --backfill --days 600
    python -m app.workers.proof --report        # print the log's per-edge counts

--daily is what the cron schedules each morning. --backfill is the one-off that
walks ~600 sessions, logs every firing with its date, and prints the honest
forward hit rate against RIL's own normal range.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from app.db import db
from app.spie.proof import backfill

log = logging.getLogger("sherbyte.worker.proof")


_REPORT_SQL = """
SELECT edge_key, COUNT(*) AS firings,
       SUM((rendered)::int) AS rendered,
       MIN(event_date) AS first, MAX(event_date) AS last
  FROM sherrbyte_app.ril_proof_log
 GROUP BY edge_key
 ORDER BY edge_key
"""


async def _report() -> dict:
    async with db.acquire() as conn:
        rows = await conn.fetch(_REPORT_SQL)
        total = await conn.fetchval("SELECT COUNT(*) FROM sherrbyte_app.ril_proof_log")
    return {"total_firings": int(total or 0),
            "per_edge": [dict(r) for r in rows]}


async def run(*, daily: bool, do_backfill: bool, report: bool, days: int) -> dict:
    if report:
        return await _report()
    async with db.acquire() as conn:
        if do_backfill:
            return await backfill.run(conn, days=days)
        # default and --daily both run the morning job
        return await backfill.run_daily(conn)


async def _main() -> int:
    ap = argparse.ArgumentParser(description="RIL 30-day proof run.")
    ap.add_argument("--daily", action="store_true",
                    help="log firings for the most recent session (the cron job)")
    ap.add_argument("--backfill", action="store_true",
                    help="evaluate the whole history backwards and report hit rate")
    ap.add_argument("--report", action="store_true",
                    help="print the log's per-edge firing counts and exit")
    ap.add_argument("--days", type=int, default=800,
                    help="how many days of RIL prices to fetch for --backfill")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(daily=args.daily, do_backfill=args.backfill,
                           report=args.report, days=args.days)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("ok", True) else 1
    finally:
        await teardown()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
