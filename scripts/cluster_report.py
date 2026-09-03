#!/usr/bin/env python3
"""scripts/cluster_report.py — how many sources does an average event have?

THIS IS THE NUMBER THE SYNTHESIS PASS IS DESIGNED AROUND. Multi-source synthesis
only works if the corpus actually contains multiple articles per event. If most
events carry one source, a pass that fires at 3+ sources sits idle forever and
the honest answer is to run at 2 — which is why MIN_CLUSTER is 2, not 3.

Run it against whatever the app is connected to:

    DATABASE_URL=... python scripts/cluster_report.py
    python scripts/cluster_report.py --db sherbyte.db
    python scripts/cluster_report.py --window 24 --days 30

It reads nothing but headlines, tags, pillars and timestamps, and writes
nothing at all.
"""
from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import synthesis                                                  # noqa: E402


def _connect(db_path: str, dsn: str):
    if dsn:
        import pgcompat                                           # noqa: PLC0415
        return pgcompat.connect(dsn), "postgres"
    import sqlite3                                                # noqa: PLC0415
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn, f"sqlite:{db_path}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "sherbyte.db"))
    ap.add_argument("--dsn", default=(os.getenv("DATABASE_URL")
                                     or os.getenv("SHERR_I_DATABASE_URL") or ""))
    ap.add_argument("--window", type=int, default=synthesis.WINDOW_HOURS,
                    help="cluster window in hours (default 24)")
    ap.add_argument("--days", type=int, default=0,
                    help="only articles published in the last N days (0 = all)")
    ap.add_argument("--candidates-only", action="store_true",
                    help="restrict to rows the rewrite would actually pick up")
    args = ap.parse_args()

    try:
        conn, where = _connect(args.db, args.dsn.strip())
    except Exception as e:                                        # noqa: BLE001
        print(f"cannot connect: {type(e).__name__}: {e}")
        print("Set DATABASE_URL (Supabase pooler URI) or pass --db <sqlite file>.")
        return 2

    sql = ("SELECT id, headline, micro_tags, pillar_id, published_at "
           "FROM articles WHERE status='published'")
    params: list = []
    if args.days:
        sql += " AND published_at >= datetime('now', ?)"
        params.append(f"-{int(args.days)} days")
    sql += " ORDER BY id DESC LIMIT 20000"

    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
    except Exception as e:                                        # noqa: BLE001
        print(f"query failed: {type(e).__name__}: {e}")
        return 2

    if args.candidates_only:
        import body_state                                         # noqa: PLC0415
        rows = [r for r in conn.execute(
            body_state.SELECT_NEEDING_REWRITE, (20000,)).fetchall()]

    print(f"source: {where}")
    print(f"articles considered: {len(rows)}")
    if not rows:
        print("nothing to cluster.")
        return 1

    clusters = synthesis.cluster_events(rows, window_hours=args.window)
    hist = synthesis.size_histogram(clusters)
    total = len(clusters)
    multi = sum(n for size, n in hist.items() if size >= synthesis.MIN_CLUSTER)
    three = sum(n for size, n in hist.items() if size >= 3)
    covered = sum(size * n for size, n in hist.items()
                  if size >= synthesis.MIN_CLUSTER)

    print(f"window: {args.window}h   clusters: {total}")
    print("cluster size -> count")
    for size, n in hist.items():
        print(f"  {size:>3} source(s): {n}")
    print(f"\nclusters with >= 2 sources: {multi} "
          f"({100.0 * multi / total:.1f}% of clusters)")
    print(f"clusters with >= 3 sources: {three} "
          f"({100.0 * three / total:.1f}% of clusters)")
    print(f"articles inside a 2+ cluster: {covered} "
          f"({100.0 * covered / len(rows):.1f}% of the corpus)")
    print(f"\nrequests to clear these articles: "
          f"{total} (one per cluster) vs {len(rows)} one-at-a-time")
    if three < multi / 2:
        print("\nVERDICT: most events carry 1-2 sources. MIN_CLUSTER=2 is the "
              "setting that matters here; a 3-source floor would idle.")
    else:
        print("\nVERDICT: 3+ source events are common; synthesis has real "
              "corroboration to work with.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
