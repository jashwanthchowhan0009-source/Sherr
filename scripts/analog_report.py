#!/usr/bin/env python3
"""scripts/analog_report.py — does the analog engine have anything to say?

Runs the whole chain against a real database and prints the only numbers that
decide whether Phase 4 (the card) is worth building:

  * how many events the library holds, and of what class
  * how many (symbol, event_class, horizon) groups clear the 5-analog floor
  * for those, the actual statistics

If the answer is nothing, this says so plainly and names the gate that stopped
it. Nothing is a legitimate answer: silence is a valid output for this engine,
and a card built on four analogs would be worse than no card.

    python scripts/analog_report.py --dsn "$DATABASE_URL"
    python scripts/analog_report.py --dsn "$DATABASE_URL" --build   # Phase 1 first
    python scripts/analog_report.py --dsn "$DATABASE_URL" --dry-run # compute, don't write

Read-only unless --build is passed (writes hist_events) — and compute() writes
analog_reactions unless --dry-run.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for p in (_ROOT, os.path.join(_ROOT, "sherrbyte")):
    if p not in sys.path:
        sys.path.insert(0, p)


def _hr(title: str) -> None:
    print(f"\n{'─' * 4} {title} {'─' * max(0, 66 - len(title))}")


async def run(dsn: str, *, build: bool, dry_run: bool, migrate: bool) -> int:
    import asyncpg
    from app.spie.analog import event_library, reaction

    conn = await asyncpg.connect(dsn)
    try:
        if migrate:
            _hr("migrations")
            for name in ("022_event_library.sql", "023_analog_reactions.sql"):
                path = os.path.join(_ROOT, "sherrbyte/app/db/migrations", name)
                await conn.execute(open(path).read())
                print(f"applied {name}")

        if build:
            _hr("phase 1 — building the event library")
            funnel = await event_library.build(conn)
            print(json.dumps(funnel, indent=2, default=str))

        _hr("phase 1 — what the library holds")
        rep = await event_library.report(conn)
        print(json.dumps(rep, indent=2, default=str))

        if not rep["total_events"]:
            print("\nVERDICT: the event library is empty. Phase 3 cannot run.")
            print("Check /admin/body-audit first — an all-placeholder corpus")
            print("produces no events, because a placeholder is not evidence.")
            return 1

        _hr("phase 3 — reaction statistics")
        out = await reaction.compute(conn, write=not dry_run)
        funnel, rows = out["funnel"], out["rows"]
        print(json.dumps({k: v for k, v in funnel.items()
                          if k != "drop_reasons"}, indent=2, default=str))
        if funnel.get("drop_reasons"):
            print("\nwhy cells were dropped:")
            for reason, n in sorted(funnel["drop_reasons"].items(),
                                    key=lambda kv: -kv[1]):
                print(f"  {n:6d}  {reason}")

        _hr("THE ANSWER")
        pairs = {(r["symbol"], r["horizon_days"]) for r in rows}
        print(f"(symbol, horizon) pairs clearing n_analogs >= "
              f"{reaction.MIN_ANALOGS}: {len(pairs)}")
        print(f"(symbol, class, horizon) groups stored:            {len(rows)}")

        if not rows:
            print("\nVERDICT: nothing clears the floor. The engine has nothing")
            print("to say yet. That is a valid output, not a failure — but it")
            print("means Phase 4 has no card to render.")
            return 0

        print()
        print(f"{'symbol':<12}{'class':<22}{'h':>3}{'n':>5}{'exc':>5}"
              f"{'agree':>7}{'med|z|':>8}{'iqr':>8}{'rec':>6}"
              f"{'strength':>9}{'floor':>7}  verdict")
        clearing = 0
        for r in sorted(rows, key=lambda r: -r["signal_strength"]):
            clears = r["signal_strength"] > r["noise_floor"]
            clearing += bool(clears)
            print(f"{r['symbol']:<12}{r['event_class']:<22}"
                  f"{r['horizon_days']:>3}{r['n_analogs']:>5}{r['n_exceeded']:>5}"
                  f"{r['sign_agreement']:>7.2f}{r['median_abs_z']:>8.2f}"
                  f"{r['dispersion']:>8.2f}{r['recency_weight']:>6.2f}"
                  f"{r['signal_strength']:>9d}{r['noise_floor']:>7d}  "
                  + ("CLEARS NOISE" if clears else "at or below noise"))

        print(f"\n{clearing} of {len(rows)} group(s) score above what pure noise")
        print("reaches at the same horizon. The rest are arithmetic, not evidence.")
        print("\nsignal_strength is a 0-100 RANKING integer. It is not a")
        print("confidence and not a probability. Never render it as a percentage.")
        # ── phase 4 + 5: what a reader would actually be shown ──────────
        _hr("phase 4 + 5 — the cards a reader would see")
        from app.spie.analog import cards                          # noqa: PLC0415
        built = await cards.build(conn, limit=10)
        print("counts:", json.dumps(built["counts"]))
        for c in built["analogs"][:5]:
            mark = "CLEARS" if c["clears_noise"] else "below floor"
            print(f"  [analog/{mark}] {c['headline']}")
            print(f"      {c['detail']}")
        for c in built["observations"][:5]:
            print(f"  [observation] {c['headline']}")
            print(f"      {c['detail']}")
        if not built["analogs"] and not built["observations"]:
            print("  nothing renderable — no analog cleared the floor and no")
            print("  single-event move reached the significance bar.")

        print("\nThe floor column is the 95th percentile of signal_strength over")
        print("random walks with no relationship in them, measured by")
        print("app/spie/analog/calibration.py. z is scaled by sqrt(h), so the")
        print("horizons are comparable with each other.")
        return 0
    finally:
        await conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=os.getenv("DATABASE_URL"),
                    help="engine Postgres DSN (default: $DATABASE_URL)")
    ap.add_argument("--build", action="store_true",
                    help="run the Phase 1 backfill before reporting")
    ap.add_argument("--migrate", action="store_true",
                    help="apply migrations 022 and 023 first")
    ap.add_argument("--dry-run", action="store_true",
                    help="compute the statistics without writing analog_reactions")
    args = ap.parse_args()

    if not args.dsn:
        print("no DSN: pass --dsn or set DATABASE_URL", file=sys.stderr)
        return 2
    return asyncio.run(run(args.dsn, build=args.build, dry_run=args.dry_run,
                           migrate=args.migrate))


if __name__ == "__main__":
    raise SystemExit(main())
