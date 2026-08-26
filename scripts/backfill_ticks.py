"""
scripts/backfill_ticks.py — seed sherrbyte_app.market_ticks with 90 days of history.

Sherr-I cannot call a market move unusual without knowing what usual is, and the
app has never kept a price series — markets.py holds the latest quote in memory
and loses it on every redeploy. This script writes the baseline: 90 daily closes
for every symbol markets.py quotes, from Yahoo v8 for the instruments and
CoinGecko market_chart for the coins.

    python scripts/backfill_ticks.py                    # 90 days, everything
    python scripts/backfill_ticks.py --days 180
    python scripts/backfill_ticks.py --only crypto      # one market_type
    python scripts/backfill_ticks.py --dry-run          # fetch, count, write nothing
    python scripts/backfill_ticks.py --report           # just the stats, no fetching

IDEMPOTENT. Every write is ON CONFLICT (symbol, UTC day) DO UPDATE, so re-running
refreshes the days it already has instead of duplicating them — a re-run over the
same window changes nothing but the revised closes. It is safe to run on a
schedule, after a failed run, or twice by accident.

PARTIAL RESULTS ARE THE NORMAL CASE. A symbol whose upstream is down or rate
limited is reported under `failed` and costs that symbol only; the rest of the
run still writes. Re-run to pick the stragglers up.

Needs DATABASE_URL (or SHERR_I_DATABASE_URL) — market_ticks is Postgres-only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import market_ticks  # noqa: E402


def _print_report(rep: dict) -> None:
    if not rep.get("ok"):
        print(json.dumps(rep, indent=2))
        return
    t = rep["totals"]
    print()
    print(f"  total rows      {t['rows']:,}")
    print(f"  symbols         {t['symbols']}")
    print(f"  date span       {t['earliest']} -> {t['latest']}")
    print()
    print(f"  {'market_type':<16}{'rows':>8}{'symbols':>9}   "
          f"{'earliest':<12}{'latest':<12}")
    print(f"  {'-' * 15:<16}{'-' * 7:>8}{'-' * 8:>9}   {'-' * 10:<12}{'-' * 10:<12}")
    for r in rep["by_market_type"]:
        print(f"  {r['market_type']:<16}{r['rows']:>8,}{r['symbols']:>9}   "
              f"{str(r['earliest']):<12}{str(r['latest']):<12}")
    print()


async def _main() -> int:
    ap = argparse.ArgumentParser(
        description="Backfill daily closes into sherrbyte_app.market_ticks.")
    ap.add_argument("--days", type=int, default=market_ticks.BACKFILL_DAYS,
                    help="how many days of history to pull (default 90)")
    ap.add_argument("--only", default=None,
                    help=f"one market_type: {', '.join(market_ticks.market_types())}")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and count, write nothing")
    ap.add_argument("--report", action="store_true",
                    help="print the stored stats and exit, without fetching")
    ap.add_argument("--dsn", default=None,
                    help="override DATABASE_URL for this run")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if args.report:
        _print_report(await market_ticks.report(dsn=args.dsn))
        return 0

    targets = market_ticks.catalogue(args.only)
    if args.only and not targets:
        print(f"unknown market_type {args.only!r}; "
              f"known: {', '.join(market_ticks.market_types())}", file=sys.stderr)
        return 2
    print(f"backfilling {len(targets)} symbols x {args.days} days"
          f"{' (dry run)' if args.dry_run else ''} ...", file=sys.stderr)

    result = await market_ticks.backfill(
        args.days, only=args.only, dry_run=args.dry_run, dsn=args.dsn)
    print(json.dumps(result, indent=2, default=str))

    if not result.get("ok"):
        return 1
    if not args.dry_run:
        _print_report(await market_ticks.report(dsn=args.dsn))
    # A run where every single symbol failed is a failure, not a quiet success.
    return 1 if result["symbols_ok"] == 0 else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
