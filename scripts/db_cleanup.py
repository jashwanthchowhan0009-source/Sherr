#!/usr/bin/env python3
"""db_cleanup.py — prune the Supabase database back under its free-tier cap.

WHY THIS EXISTS
───────────────
The database blew its quota (egress ~487%, size ~248%) and started returning
402. The egress leak is fixed in code (embeddings are no longer SELECTed into
Python); this script attacks the SIZE half: it prunes old rows, drops old
embeddings, and VACUUM FULLs to actually return the space to the OS.

IT IS DRY-RUN BY DEFAULT. With no flags it only MEASURES — current sizes, and
exactly how many rows each step WOULD touch — and prints the plan. Nothing is
deleted until you pass --apply, so you see the numbers first.

    python scripts/db_cleanup.py                 # measure + plan only
    python scripts/db_cleanup.py --apply         # actually prune + VACUUM FULL
    python scripts/db_cleanup.py --days 45 --embedding-days 60 --apply

Windows (all overridable):
  --days 60              age past which rows in {schema}.articles and
                         public.info_objects are deleted.
  --public-articles-days 30
                         public.articles is the ENGINE's copy (94k rows vs the
                         app's 47k); it does not need full history to match
                         against a ~368-day news corpus, so it is rolled to a
                         tighter window. Set equal to --days to disable.
  --embedding-days 90    age past which info_objects.embedding is set NULL
                         (the vector is the biggest column; the row stays).

Connects via DATABASE_URL (or SHERR_I_DATABASE_URL) — the same DSN the engine
uses. Requires asyncpg.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

try:
    import asyncpg
except ImportError:                                               # pragma: no cover
    sys.exit("asyncpg is required: pip install asyncpg")

APP_SCHEMA = os.getenv("APP_SCHEMA", "sherrbyte_app")

# (schema, table, timestamp column). articles lives in BOTH schemas; info_objects
# only in public. published_at is cast to timestamptz because migration 018 left
# it TEXT-or-timestamptz depending on the row's age (see CLAUDE.md).
PRUNE_TARGETS = [
    (APP_SCHEMA, "articles", "published_at"),
    ("public", "articles", "published_at"),
    ("public", "info_objects", "published_at"),
]


def _dsn() -> str:
    dsn = (os.getenv("DATABASE_URL") or os.getenv("SHERR_I_DATABASE_URL") or "").strip()
    if not dsn:
        sys.exit("Set DATABASE_URL (or SHERR_I_DATABASE_URL) first.")
    # Strip libpq-only params asyncpg rejects (sslmode, pgbouncer, …).
    from urllib.parse import urlsplit, urlunsplit
    p = urlsplit(dsn)
    if p.query:
        keep = "&".join(kv for kv in p.query.split("&")
                        if kv.split("=")[0] not in
                        ("sslmode", "pgbouncer", "options", "target_session_attrs"))
        dsn = urlunsplit((p.scheme, p.netloc, p.path, keep, p.fragment))
    return dsn


async def _table_exists(conn, schema, table) -> bool:
    return bool(await conn.fetchval(
        "SELECT to_regclass($1)", f"{schema}.{table}"))


async def _age_cast(conn, schema, table, col) -> str:
    """`col` compared as a timestamp, tolerant of the TEXT-or-timestamptz split.

    A plain `< now() - interval` fails on a TEXT column; casting the column to
    timestamptz is valid on both, and NULLIF guards the empty-string rows that
    predate the normalisation so they simply do not match rather than raising.
    """
    return f"NULLIF({col}::text, '')::timestamptz"


async def measure(conn) -> None:
    db = await conn.fetchval("SELECT current_database()")
    size = await conn.fetchval("SELECT pg_size_pretty(pg_database_size($1))", db)
    print(f"\n  database {db}: {size} total\n")
    print(f"  {'table':<34}{'rows':>12}{'total size':>14}")
    print("  " + "-" * 58)
    for schema, table, _ in PRUNE_TARGETS:
        if not await _table_exists(conn, schema, table):
            print(f"  {schema}.{table:<27}{'(absent)':>26}")
            continue
        n = await conn.fetchval(f"SELECT count(*) FROM {schema}.{table}")
        sz = await conn.fetchval(
            "SELECT pg_size_pretty(pg_total_relation_size($1))", f"{schema}.{table}")
        print(f"  {schema + '.' + table:<34}{n:>12,}{sz:>14}")
    # embedding footprint
    if await _table_exists(conn, "public", "info_objects"):
        with_emb = await conn.fetchval(
            "SELECT count(*) FROM public.info_objects WHERE embedding IS NOT NULL")
        print(f"\n  public.info_objects with a non-NULL embedding: {with_emb:,}")


async def plan_and_run(conn, args) -> None:
    apply = args.apply
    header = "APPLYING" if apply else "DRY-RUN (nothing deleted — pass --apply)"
    print(f"\n  === {header} ===\n")
    total = 0

    for schema, table, col in PRUNE_TARGETS:
        if not await _table_exists(conn, schema, table):
            continue
        days = (args.public_articles_days
                if (schema == "public" and table == "articles")
                else args.days)
        cast = await _age_cast(conn, schema, table, col)
        where = f"{cast} < now() - ($1 || ' days')::interval"
        n = await conn.fetchval(
            f"SELECT count(*) FROM {schema}.{table} WHERE {where}", str(days))
        total += n
        print(f"  {schema}.{table}: {n:,} rows older than {days}d"
              + (" → DELETE" if apply else " would be deleted"))
        if apply and n:
            await conn.execute(
                f"DELETE FROM {schema}.{table} WHERE {where}", str(days))

    # Drop old embeddings (keep the row, null the vector).
    if await _table_exists(conn, "public", "info_objects"):
        cast = await _age_cast(conn, "public", "info_objects", "published_at")
        where = f"embedding IS NOT NULL AND {cast} < now() - ($1 || ' days')::interval"
        n = await conn.fetchval(
            f"SELECT count(*) FROM public.info_objects WHERE {where}",
            str(args.embedding_days))
        print(f"  public.info_objects: {n:,} embeddings older than "
              f"{args.embedding_days}d"
              + (" → SET NULL" if apply else " would be nulled"))
        if apply and n:
            await conn.execute(
                f"UPDATE public.info_objects SET embedding=NULL WHERE {where}",
                str(args.embedding_days))

    print(f"\n  {'DELETED' if apply else 'would delete'} {total:,} rows total.")

    if apply:
        # VACUUM FULL returns the freed pages to the OS. It takes an ACCESS
        # EXCLUSIVE lock and cannot run in a transaction — asyncpg is autocommit
        # here, so these run one at a time. Skippable with --no-vacuum on a busy DB.
        if args.no_vacuum:
            print("  skipping VACUUM FULL (--no-vacuum).")
        else:
            for schema, table, _ in PRUNE_TARGETS + [("public", "info_objects", "")]:
                if await _table_exists(conn, schema, table):
                    print(f"  VACUUM FULL {schema}.{table} …", flush=True)
                    await conn.execute(f"VACUUM FULL {schema}.{table}")
        print("\n  Done. Re-run without --apply to confirm the new sizes.")
    else:
        print("  Re-run with --apply to execute, then it VACUUM FULLs to reclaim space.")


async def main() -> None:
    ap = argparse.ArgumentParser(description="Prune the Supabase DB under its cap.")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--public-articles-days", type=int, default=30)
    ap.add_argument("--embedding-days", type=int, default=90)
    ap.add_argument("--apply", action="store_true",
                    help="actually delete + VACUUM FULL (default is dry-run)")
    ap.add_argument("--no-vacuum", action="store_true",
                    help="skip VACUUM FULL (deletes still run under --apply)")
    args = ap.parse_args()

    conn = await asyncpg.connect(_dsn())
    try:
        await measure(conn)
        await plan_and_run(conn, args)
        if args.apply and not args.no_vacuum:
            print("\n  --- sizes after cleanup ---")
            await measure(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
