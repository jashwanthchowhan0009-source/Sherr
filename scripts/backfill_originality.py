"""
scripts/backfill_originality.py — re-run the originality gate over existing rows (P0.5).

Every article published before the gate existed is unchecked, and some are verbatim
copies. This walks the corpus, applies the same two gates the live pipeline uses, and
unpublishes anything that fails.

    python scripts/backfill_originality.py --dry-run    # report only, no writes
    python scripts/backfill_originality.py             # apply
    python scripts/backfill_originality.py --limit 500

BACKEND IS CHOSEN FROM DATABASE_URL. A postgres:// or postgresql:// URL uses asyncpg
against Supabase; anything else (or no URL) falls back to the local sqlite file. The
two stores are genuinely different schemas, not one schema behind two drivers:

    sqlite      articles.headline        vs  articles.source_headline
                articles.full_body       vs  articles.source_summary
    postgres    info_objects.headline    vs  articles.title
                info_objects.body        vs  articles.body

On Postgres our rewritten text lives in info_objects and the publisher's original
stays in articles, joined on article_id. That join IS the comparison the gate needs,
and it is a better one than sqlite has: articles.body is the full source text, where
sqlite only kept a truncated source_summary.

UNPUBLISH IS IMMEDIATE AND NOT OPTIONAL. A failing row is a live copyright exposure,
so the default path writes. --dry-run exists to size the problem first, not to make
enforcement discretionary.

Rows with no stored source text cannot be compared. They are parked as
pending_rewrite rather than passed: unverifiable is not the same as clean.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from originality import headline_is_original, originality_check   # noqa: E402

DB_PATH = os.getenv("DB_PATH", "sherbyte.db")   # matches main.py's default
DATABASE_URL = (os.getenv("SHERR_I_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()

_PG_PREFIXES = ("postgres://", "postgresql://")
# Supabase's pooled connection string carries libpq-only options that asyncpg rejects.
_PGBOUNCER_ONLY = {"pgbouncer", "options", "sslmode", "connect_timeout",
                   "prepared_statement_cache_size", "statement_cache_size"}


def is_postgres(url: str) -> bool:
    return url.lower().startswith(_PG_PREFIXES)


def sanitize_pg_dsn(url: str) -> str:
    """Strip query params asyncpg does not understand.

    Supabase hands out a pooled URL with ?pgbouncer=true&sslmode=require; asyncpg
    raises on both, so a copy-pasted dashboard URL fails before it connects.
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    kept = [kv for kv in parts.query.split("&")
            if kv and kv.split("=", 1)[0].lower() not in _PGBOUNCER_ONLY]
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       "&".join(kept), parts.fragment))


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


def _new_counts() -> dict:
    return {"scanned": 0, "published": 0, "pending_rewrite": 0,
            "blocked_originality": 0, "unverifiable": 0, "changed": 0}


def _decide(row: dict, counts: dict) -> tuple[str, dict]:
    """Apply the gate to one row, or park it when there is nothing to compare."""
    counts["scanned"] += 1
    src_head = row.get("source_headline") or ""
    src_body = row.get("source_body") or ""
    if not src_head and not src_body:
        counts["unverifiable"] += 1
        return "pending_rewrite", {
            "status": "pending_rewrite",
            "reason": "no stored source text — cannot verify originality"}
    status, audit = gate(row.get("headline") or "", row.get("body") or "",
                         src_head, src_body)
    counts[status] += 1
    return status, audit


def _sample(samples: list, row: dict, was: str, now: str, audit: dict) -> None:
    if len(samples) >= 10 or now == "published":
        return
    samples.append({
        "id": str(row.get("id")),
        "headline": (row.get("headline") or "")[:70],
        "was": was, "now": now,
        "why": (audit.get("body", {}).get("reasons")
                or audit.get("headline", {}).get("reasons")
                or [audit.get("reason", "")])[:1],
    })


# ─── Postgres ─────────────────────────────────────────────────────────────────
_PG_COLUMNS = [
    "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'published'",
    "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS originality_json JSONB DEFAULT '{}'::jsonb",
    "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS originality_overlap REAL DEFAULT -1",
    "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS originality_run INTEGER DEFAULT -1",
    "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS originality_checked_at TIMESTAMPTZ",
]


async def run_postgres(url: str, *, dry_run: bool, limit: int | None) -> dict:
    import asyncpg

    conn = await asyncpg.connect(
        sanitize_pg_dsn(url),
        # The transaction pooler multiplexes sessions, so server-side prepared
        # statements are not safe to cache across them.
        statement_cache_size=0,
    )
    try:
        # The gate's columns live on info_objects (the served row), not articles.
        # Added here rather than in a migration file so the script is runnable
        # against a database that has not taken the app's migrations yet.
        for stmt in _PG_COLUMNS:
            await conn.execute(stmt)

        q = """
            SELECT io.id, io.headline, io.body, io.status,
                   a.title AS source_headline, a.body AS source_body
            FROM info_objects io
            LEFT JOIN articles a ON a.id = io.article_id
            ORDER BY io.created_at
        """
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = await conn.fetch(q)

        counts, samples = _new_counts(), []
        updates = []
        for r in rows:
            row = dict(r)
            was = row.get("status") or "published"
            status, audit = _decide(row, counts)
            if status != was:
                counts["changed"] += 1
                _sample(samples, row, was, status, audit)
            body_m = audit.get("body", {})
            updates.append((status, json.dumps(audit),
                            float(body_m.get("overlap", -1)),
                            int(body_m.get("longest_run", -1)),
                            datetime.now(timezone.utc), row["id"]))

        if not dry_run and updates:
            await conn.executemany(
                """
                UPDATE info_objects
                   SET status = $1, originality_json = $2::jsonb,
                       originality_overlap = $3, originality_run = $4,
                       originality_checked_at = $5
                 WHERE id = $6
                """, updates)
        return {"backend": "postgres", "dry_run": dry_run,
                "counts": counts, "unpublished_samples": samples}
    finally:
        await conn.close()


# ─── sqlite ───────────────────────────────────────────────────────────────────
def run_sqlite(db_path: str, *, dry_run: bool, limit: int | None) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    q = ("SELECT id, headline, source_headline, full_body, source_summary, status "
         "FROM articles ORDER BY id")
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = conn.execute(q).fetchall()

    counts, samples = _new_counts(), []
    for r in rows:
        row = {"id": r["id"], "headline": r["headline"], "body": r["full_body"],
               "source_headline": r["source_headline"],
               # sqlite only ever kept a truncated excerpt of the publisher's text.
               "source_body": r["source_summary"]}
        was = r["status"] or "published"
        status, audit = _decide(row, counts)
        if status != was:
            counts["changed"] += 1
            _sample(samples, row, was, status, audit)
        if not dry_run:
            body_m = audit.get("body", {})
            conn.execute(
                "UPDATE articles SET status=?, originality_json=?, "
                "originality_overlap=?, originality_run=?, originality_checked_at=? "
                "WHERE id=?",
                (status, json.dumps(audit), body_m.get("overlap", -1),
                 body_m.get("longest_run", -1),
                 datetime.now(timezone.utc).isoformat(), row["id"]))
    if not dry_run:
        conn.commit()
    conn.close()
    return {"backend": "sqlite", "db": db_path, "dry_run": dry_run,
            "counts": counts, "unpublished_samples": samples}


def run(*, dry_run: bool = False, limit: int | None = None,
        url: str | None = None, db_path: str | None = None) -> dict:
    url = DATABASE_URL if url is None else url
    if is_postgres(url):
        return asyncio.run(run_postgres(url, dry_run=dry_run, limit=limit))

    path = db_path or DB_PATH
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        # Silently reporting "0 scanned" against an empty file reads like a clean
        # corpus. It is not — it is the wrong database.
        return {"backend": "sqlite", "db": path, "error":
                f"{path} is missing or empty, and DATABASE_URL is not a postgres URL. "
                f"Set DATABASE_URL to the Supabase connection string to check "
                f"production data.", "counts": _new_counts()}
    return run_sqlite(path, dry_run=dry_run, limit=limit)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Re-run the originality gate over existing articles.")
    ap.add_argument("--database-url", default=None,
                    help="postgres DSN (defaults to $DATABASE_URL)")
    ap.add_argument("--db", default=None, help="sqlite path (fallback backend)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    result = run(dry_run=args.dry_run, limit=args.limit,
                 url=args.database_url, db_path=args.db)
    print(json.dumps(result, indent=2, default=str))
    if result.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
