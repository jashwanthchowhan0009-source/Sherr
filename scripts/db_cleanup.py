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


def _mb(n) -> str:
    return "—" if n is None else f"{n / 1024 / 1024:,.1f} MB"


# Fixed display order for the embedding age buckets below.
_AGE_BUCKETS = ["0-30 d", "31-90 d", "91-365 d", ">365 d", "no/blank date"]


async def _relation_breakdown(conn, schemas) -> None:
    """Every relation > 5 MB in the given schemas, split table / toast / index.

    `pg_total_relation_size` = heap + toast (large-value overflow) + all indexes,
    so the earlier three-table report (which summed pg_total_relation_size) was
    already counting each table's own indexes and toast — the "missing" space is
    the OTHER relations this lists. Splitting the total makes it obvious where a
    relation's bytes actually live (a fat toast = big text/blob columns; a fat
    index total = an ANN/HNSW index, itemised separately below)."""
    print("\n  relations > 5 MB  (total = heap + toast + indexes):\n")
    print(f"  {'relation':<40}{'total':>11}{'heap':>11}{'toast':>11}{'indexes':>11}")
    print("  " + "-" * 84)
    rows = await conn.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS name,
               pg_total_relation_size(c.oid)                       AS total,
               pg_relation_size(c.oid)                             AS heap,
               COALESCE(pg_total_relation_size(c.reltoastrelid),0) AS toast,
               pg_indexes_size(c.oid)                              AS indexes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'm', 'p')
          AND pg_total_relation_size(c.oid) > 5 * 1024 * 1024
        ORDER BY total DESC
        """,
        list(schemas),
    )
    total = 0
    for r in rows:
        total += r["total"]
        print(f"  {r['schema'] + '.' + r['name']:<40}"
              f"{_mb(r['total']):>11}{_mb(r['heap']):>11}"
              f"{_mb(r['toast']):>11}{_mb(r['indexes']):>11}")
    print("  " + "-" * 84)
    print(f"  {'sum of relations > 5 MB':<40}{_mb(total):>11}")


async def _index_breakdown(conn, schemas) -> None:
    """Individual indexes > 5 MB, tagged with their access method — so an HNSW
    vector index on an embedding column is visible on its own line, separately
    from the b-tree indexes, rather than folded into a table's index total."""
    print("\n  indexes > 5 MB  (method exposes hnsw/ivfflat vector indexes):\n")
    print(f"  {'index':<48}{'method':>9}{'size':>11}")
    print("  " + "-" * 68)
    rows = await conn.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS index_name,
               t.relname AS on_table, am.amname AS method,
               pg_relation_size(c.oid) AS size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_am am ON am.oid = c.relam
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind = 'i'
          AND pg_relation_size(c.oid) > 5 * 1024 * 1024
        ORDER BY size DESC
        """,
        list(schemas),
    )
    for r in rows:
        print(f"  {r['schema'] + '.' + r['index_name']:<48}"
              f"{r['method']:>9}{_mb(r['size']):>11}   on {r['on_table']}")


async def _column_footprint(conn, schema, table) -> None:
    """Per-column bytes for one table, so 'this table is big' becomes 'THIS
    column is big'. One sequential pass summing pg_column_size() per column,
    ranked so the dominant column (full body text, raw HTML, a base64 image, an
    embedding) is unambiguous."""
    print(f"\n  per-column footprint of {schema}.{table} "
          f"(sum of pg_column_size, ranked):\n")
    if not await _table_exists(conn, schema, table):
        print("  (absent)")
        return
    cols = await conn.fetch(
        """SELECT column_name, data_type FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = $2
           ORDER BY ordinal_position""",
        schema, table,
    )
    n = await conn.fetchval(f"SELECT count(*) FROM {schema}.{table}")
    if not cols or not n:
        print("  (no columns / empty)")
        return
    select = ", ".join(
        f'sum(pg_column_size({_qi(c["column_name"])}))::bigint AS c{i}'
        for i, c in enumerate(cols))
    row = await conn.fetchrow(f"SELECT {select} FROM {schema}.{table}")
    stats = sorted(
        ((c["column_name"], c["data_type"], row[f"c{i}"] or 0)
         for i, c in enumerate(cols)),
        key=lambda x: x[2], reverse=True)
    print(f"  {n:,} rows\n")
    print(f"  {'column':<26}{'type':<24}{'total':>11}{'avg/row':>12}")
    print("  " + "-" * 73)
    for name, typ, tot in stats:
        print(f"  {name:<26}{typ:<24}{_mb(tot):>11}{tot / n:>10,.0f} B")


async def _embedding_age_report(conn) -> None:
    """public.info_objects embeddings bucketed by row age, with the byte cost of
    each bucket. This is the number the retention-window question turns on: how
    many embeddings (and MB) live in each age band, so shortening the window from
    90 to 30 days has a measured cost rather than an assumed one."""
    print("\n  public.info_objects — embedding cost by row age:\n")
    if not await _table_exists(conn, "public", "info_objects"):
        print("  (absent)")
        return
    rows = await conn.fetch(
        """
        SELECT bucket,
               count(*)                            AS rows,
               count(*) FILTER (WHERE has_emb)     AS with_emb,
               COALESCE(sum(emb_bytes), 0)::bigint AS emb_bytes
        FROM (
          SELECT
            CASE
              WHEN age IS NULL                THEN 'no/blank date'
              WHEN age <= interval '30 days'  THEN '0-30 d'
              WHEN age <= interval '90 days'  THEN '31-90 d'
              WHEN age <= interval '365 days' THEN '91-365 d'
              ELSE '>365 d'
            END                       AS bucket,
            (embedding IS NOT NULL)   AS has_emb,
            pg_column_size(embedding) AS emb_bytes
          FROM (
            SELECT embedding,
                   now() - NULLIF(published_at::text, '')::timestamptz AS age
            FROM public.info_objects
          ) a
        ) b
        GROUP BY bucket
        """)
    by = {r["bucket"]: r for r in rows}
    print(f"  {'age bucket':<16}{'rows':>12}{'with embedding':>16}{'embedding bytes':>18}")
    print("  " + "-" * 62)
    for bucket in _AGE_BUCKETS:
        r = by.get(bucket)
        if not r:
            continue
        print(f"  {bucket:<16}{r['rows']:>12,}{r['with_emb']:>16,}"
              f"{_mb(r['emb_bytes']):>18}")


async def deep_audit(conn) -> None:
    """Read-only. Everything here is SELECT/catalog only — it never deletes and
    is wrapped so a failure in any section prints and moves on rather than
    aborting the run (important: measure/plan already ran by the time this is
    called)."""
    schemas = ["public", APP_SCHEMA]
    print("\n  === DEEP AUDIT (read-only) ===")
    for section in (
        lambda: _relation_breakdown(conn, schemas),
        lambda: _index_breakdown(conn, schemas),
        lambda: _column_footprint(conn, APP_SCHEMA, "articles"),
        lambda: _embedding_age_report(conn),
    ):
        try:
            await section()
        except Exception as ex:                                    # noqa: BLE001
            print(f"  [section failed: {type(ex).__name__}: {ex}]")


async def main() -> None:
    ap = argparse.ArgumentParser(description="Prune the Supabase DB under its cap.")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--public-articles-days", type=int, default=30)
    ap.add_argument("--embedding-days", type=int, default=90)
    ap.add_argument("--apply", action="store_true",
                    help="actually delete + VACUUM FULL (default is dry-run)")
    ap.add_argument("--no-vacuum", action="store_true",
                    help="skip VACUUM FULL (deletes still run under --apply)")
    ap.add_argument("--audit", action="store_true",
                    help="read-only: after measuring, print every relation > 5 MB "
                         "(table/toast/index split), large indexes by method, the "
                         "per-column footprint of the app articles table, and the "
                         "embedding cost by row age")
    args = ap.parse_args()

    conn = await asyncpg.connect(_dsn())
    try:
        await measure(conn)
        await plan_and_run(conn, args)
        if args.apply and not args.no_vacuum:
            print("\n  --- sizes after cleanup ---")
            await measure(conn)
        if args.audit:
            await deep_audit(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
