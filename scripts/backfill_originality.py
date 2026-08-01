"""
scripts/backfill_originality.py — re-run the originality gate over existing rows (P0.5).

Every article published before the gate existed is unchecked, and some of them are
verbatim copies. This walks the corpus, applies the same two gates the live pipeline
uses, and unpublishes anything that fails.

    python scripts/backfill_originality.py --dry-run     # report only, no writes
    python scripts/backfill_originality.py               # apply
    python scripts/backfill_originality.py --limit 500

UNPUBLISH IS IMMEDIATE AND NOT OPTIONAL. A failing row is a live copyright exposure,
so the default path writes. --dry-run exists to size the problem first, not to make
enforcement discretionary.

Rows with no stored source text cannot be compared. They are marked pending_rewrite
rather than passed: unverifiable is not the same as clean, and the honest state for
"we cannot prove this is ours" is off the feed.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from originality import headline_is_original, originality_check   # noqa: E402

DB_PATH = os.getenv("DB_PATH", "sherrbyte.db")


def gate(headline: str, body: str, source_headline: str, source_body: str):
    """Same routing as main._gate_article — kept in step deliberately."""
    head_ok, head_m = headline_is_original(headline, source_headline)
    body_ok, body_m = originality_check(body, source_body)
    if head_ok and body_ok:
        status = "published"
    elif not head_ok:
        status = "pending_rewrite"
    else:
        status = "blocked_originality"
    return status, {"status": status, "headline": head_m, "body": body_m}


def run(db_path: str = DB_PATH, *, dry_run: bool = False,
        limit: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    q = ("SELECT id, headline, source_headline, full_body, source_summary, status "
         "FROM articles ORDER BY id")
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = conn.execute(q).fetchall()

    counts = {"scanned": 0, "published": 0, "pending_rewrite": 0,
              "blocked_originality": 0, "unverifiable": 0, "changed": 0}
    samples: list[dict] = []

    for r in rows:
        counts["scanned"] += 1
        source_headline = r["source_headline"] or ""
        # source_summary is the stored excerpt of the publisher's text. Without it
        # there is nothing to compare against.
        source_body = r["source_summary"] or ""

        if not source_headline and not source_body:
            counts["unverifiable"] += 1
            new_status = "pending_rewrite"
            audit = {"status": new_status,
                     "reason": "no stored source text — cannot verify originality"}
        else:
            new_status, audit = gate(r["headline"] or "", r["full_body"] or "",
                                     source_headline, source_body)
            counts[new_status] += 1

        if new_status != (r["status"] or "published"):
            counts["changed"] += 1
            if len(samples) < 10 and new_status != "published":
                samples.append({"id": r["id"], "headline": (r["headline"] or "")[:70],
                                "was": r["status"], "now": new_status,
                                "why": (audit.get("body", {}).get("reasons")
                                        or audit.get("headline", {}).get("reasons")
                                        or [audit.get("reason", "")])[:1]})

        if not dry_run:
            body_m = audit.get("body", {})
            conn.execute(
                "UPDATE articles SET status=?, originality_json=?, "
                "originality_overlap=?, originality_run=?, originality_checked_at=? "
                "WHERE id=?",
                (new_status, json.dumps(audit),
                 body_m.get("overlap", -1), body_m.get("longest_run", -1),
                 datetime.now(timezone.utc).isoformat(), r["id"]))

    if not dry_run:
        conn.commit()
    conn.close()
    return {"dry_run": dry_run, "db": db_path, "counts": counts,
            "unpublished_samples": samples}


def main() -> None:
    ap = argparse.ArgumentParser(description="Re-run the originality gate over "
                                             "existing articles.")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    print(json.dumps(run(args.db, dry_run=args.dry_run, limit=args.limit), indent=2))


if __name__ == "__main__":
    main()
