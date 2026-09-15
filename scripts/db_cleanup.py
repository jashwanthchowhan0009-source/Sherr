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


BATCH = 5000  # rows per statement — small enough to finish under statement_timeout


async def _batched_delete(conn, table, where, arg) -> int:
    """DELETE in ctid-bounded batches. A single bulk DELETE of tens of thousands
    of rows exceeds Supabase's server-side statement_timeout and is cancelled
    (observed: 82k rows → QueryCanceledError). Each batch is its own autocommit
    statement, so progress survives even if a later batch is interrupted."""
    total = 0
    while True:
        status = await conn.execute(
            f"DELETE FROM {table} WHERE ctid = ANY(ARRAY("
            f"  SELECT ctid FROM {table} WHERE {where} LIMIT {BATCH}))",
            arg,
        )
        n = int(status.rsplit(" ", 1)[-1]) if status.startswith("DELETE") else 0
        total += n
        if n:
            print(f"    … deleted {total:,} from {table}", flush=True)
        if n < BATCH:
            return total


async def _batched_null(conn, table, col, where, arg) -> int:
    """SET col=NULL in batches. The `col IS NOT NULL` predicate in `where` makes
    each updated row drop out of the next batch, so the loop always makes
    progress and terminates."""
    total = 0
    while True:
        status = await conn.execute(
            f"UPDATE {table} SET {col}=NULL WHERE ctid = ANY(ARRAY("
            f"  SELECT ctid FROM {table} WHERE {where} LIMIT {BATCH}))",
            arg,
        )
        n = int(status.rsplit(" ", 1)[-1]) if status.startswith("UPDATE") else 0
        total += n
        if n:
            print(f"    … nulled {total:,} {col} in {table}", flush=True)
        if n < BATCH:
            return total


async def plan_and_run(conn, args) -> None:
    apply = args.apply
    header = ("APPLYING (batched to fit statement_timeout)" if apply
              else "DRY-RUN (nothing deleted — pass --apply)")
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
              + (" → DELETE (batched)" if apply else " would be deleted"))
        if apply and n:
            await _batched_delete(conn, f"{schema}.{table}", where, str(days))

    # Drop old embeddings (keep the row, null the vector).
    if await _table_exists(conn, "public", "info_objects"):
        cast = await _age_cast(conn, "public", "info_objects", "published_at")
        where = f"embedding IS NOT NULL AND {cast} < now() - ($1 || ' days')::interval"
        n = await conn.fetchval(
            f"SELECT count(*) FROM public.info_objects WHERE {where}",
            str(args.embedding_days))
        print(f"  public.info_objects: {n:,} embeddings older than "
              f"{args.embedding_days}d"
              + (" → SET NULL (batched)" if apply else " would be nulled"))
        if apply and n:
            await _batched_null(conn, "public.info_objects", "embedding",
                                where, str(args.embedding_days))

    print(f"\n  {'DELETED' if apply else 'would delete'} {total:,} rows total.")

    if apply:
        # VACUUM FULL is what actually returns freed pages to the OS, but it takes
        # an ACCESS EXCLUSIVE lock, cannot run in a transaction, and through the
        # transaction pooler routinely exceeds statement_timeout on a large table
        # (and would lock the live app for the rewrite). Each table is attempted
        # independently and a timeout/refusal is reported per table rather than
        # failing the whole run — so the batched deletes above still stand.
        if args.no_vacuum:
            print("  skipping VACUUM FULL (--no-vacuum). NOTE: the deleted rows are "
                  "now dead tuples — their space is reused by future writes but is "
                  "NOT returned to the OS, so pg_database_size stays flat until a "
                  "VACUUM FULL (or pg_repack) runs.")
        else:
            for schema, table, _ in PRUNE_TARGETS + [("public", "info_objects", "")]:
                if await _table_exists(conn, schema, table):
                    print(f"  VACUUM FULL {schema}.{table} …", flush=True)
                    try:
                        await conn.execute(f"VACUUM FULL {schema}.{table}")
                        print(f"    ✓ reclaimed {schema}.{table}")
                    except Exception as ex:                        # noqa: BLE001
                        print(f"    ✗ {schema}.{table}: {type(ex).__name__}: {ex}")
                        print("      (space NOT returned to OS for this table)")
        print("\n  Done. Re-run without --apply to confirm the new sizes.")
    else:
        print("  Re-run with --apply to execute (batched deletes; VACUUM FULL "
              "reclaims space where the pooler allows it).")


def _qi(ident: str) -> str:
    """Quote an identifier for safe interpolation (double any embedded quote)."""
    return '"' + ident.replace('"', '""') + '"'


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
    column is big', ranked so the dominant column (full body text, raw HTML, a
    base64 image, an embedding) is unambiguous.

    SAMPLED with TABLESAMPLE SYSTEM (reads a few random pages, not the whole
    116 MB table) so it never trips statement_timeout on the toasted text
    columns; the per-column average is extrapolated to the full row count."""
    print(f"\n  per-column footprint of {schema}.{table} "
          f"(sampled avg × row count):\n")
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
        f'avg(pg_column_size({_qi(c["column_name"])}))::float8 AS c{i}'
        for i, c in enumerate(cols))
    row, sampled = None, 0
    for pct in (3, 15, 60):                       # widen until a page is sampled
        r = await conn.fetchrow(
            f"SELECT count(*) AS s, {select} FROM {schema}.{table} "
            f"TABLESAMPLE SYSTEM ({pct})")
        if r and r["s"]:
            row, sampled = r, r["s"]
            break
    if row is None:                               # tiny table — a full scan is cheap
        row = await conn.fetchrow(
            f"SELECT count(*) AS s, {select} FROM {schema}.{table}")
        sampled = row["s"] or 1
    stats = sorted(
        ((c["column_name"], c["data_type"], (row[f"c{i}"] or 0.0))
         for i, c in enumerate(cols)),
        key=lambda x: x[2], reverse=True)
    print(f"  {n:,} rows total; sampled {sampled:,}\n")
    print(f"  {'column':<26}{'type':<22}{'avg/row':>12}{'est. total':>13}")
    print("  " + "-" * 73)
    for name, typ, avg in stats:
        print(f"  {name:<26}{typ:<22}{avg:>10,.0f} B{_mb(avg * n):>13}")


_EMB_BYTES = 384 * 4 + 8  # vector(384) on-disk ≈ 1544 B/row


async def _embedding_age_report(conn) -> None:
    """public.info_objects embeddings bucketed by row age. This is the number the
    retention-window question turns on: how many embeddings live in each age
    band, so shortening 90 → 30 days has a measured cost rather than an assumed
    one. Counts only (the NULL check reads the null bitmap, never detoasts the
    vector), with size estimated as rows × the fixed 384-d vector width, so the
    query stays well under statement_timeout."""
    print(f"\n  public.info_objects — embeddings by row age "
          f"(size ≈ rows × {_EMB_BYTES} B for a 384-d vector):\n")
    if not await _table_exists(conn, "public", "info_objects"):
        print("  (absent)")
        return
    rows = await conn.fetch(
        """
        SELECT
          CASE
            WHEN age IS NULL                THEN 'no/blank date'
            WHEN age <= interval '30 days'  THEN '0-30 d'
            WHEN age <= interval '90 days'  THEN '31-90 d'
            WHEN age <= interval '365 days' THEN '91-365 d'
            ELSE '>365 d'
          END                             AS bucket,
          count(*)                        AS rows,
          count(*) FILTER (WHERE has_emb) AS with_emb
        FROM (
          SELECT (embedding IS NOT NULL) AS has_emb,
                 now() - NULLIF(published_at::text, '')::timestamptz AS age
          FROM public.info_objects
        ) a
        GROUP BY 1
        """)
    by = {r["bucket"]: r for r in rows}
    print(f"  {'age bucket':<16}{'rows':>12}{'with embedding':>16}{'≈ size':>14}")
    print("  " + "-" * 58)
    for bucket in _AGE_BUCKETS:
        r = by.get(bucket)
        if not r:
            continue
        print(f"  {bucket:<16}{r['rows']:>12,}{r['with_emb']:>16,}"
              f"{_mb(r['with_emb'] * _EMB_BYTES):>14}")


def _parse_vec(text: str) -> list[float]:
    return [float(x) for x in text.strip().lstrip("[").rstrip("]").split(",") if x]


def _cos(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


async def embed_probe(conn) -> None:
    """Read-only. Settles whether public.info_objects.embedding holds real MiniLM
    vectors or the md5-hash fallback, by inspecting the STORED vectors directly:

      • fingerprint — a hash vector is SPARSE (nonzero dims ≈ distinct token
        buckets, ≪ 384) and ALL-NON-NEGATIVE (it sums token counts); a MiniLM
        vector is DENSE (all 384 nonzero) and MIXED-SIGN. This alone identifies
        the code path that wrote them.
      • cosine spread over a sample, plus the nearest neighbour of a few seeds
        (headlines), so 'are neighbours actually related' is answerable, not
        assumed."""
    import itertools
    import statistics

    print("\n  === EMBEDDING PROBE (read-only) ===\n")
    if not await _table_exists(conn, "public", "info_objects"):
        print("  info_objects absent")
        return
    rows = await conn.fetch(
        """SELECT id, left(headline, 68) AS h, embedding::text AS vec
           FROM public.info_objects
           WHERE embedding IS NOT NULL
           ORDER BY published_at DESC NULLS LAST
           LIMIT 24""")
    if not rows:
        print("  no rows carry an embedding")
        return

    print("  per-vector fingerprint  (hash = sparse & all ≥0; MiniLM = dense & mixed-sign):\n")
    print(f"  {'nonzero/dim':>13}{'min':>9}{'max':>9}{'neg dims':>10}{'norm':>7}  headline")
    print("  " + "-" * 92)
    vecs: list[list[float]] = []
    for r in rows:
        v = _parse_vec(r["vec"])
        vecs.append(v)
        nz = sum(1 for x in v if x != 0.0)
        neg = sum(1 for x in v if x < 0.0)
        norm = sum(x * x for x in v) ** 0.5
        print(f"  {nz:>7}/{len(v):<5}{min(v):>9.3f}{max(v):>9.3f}{neg:>10}"
              f"{norm:>7.2f}  {r['h']}")

    pair = [_cos(vecs[i], vecs[j])
            for i, j in itertools.combinations(range(len(vecs)), 2)]
    print(f"\n  pairwise cosine over {len(vecs)} sampled vectors: "
          f"min {min(pair):.3f}  mean {statistics.mean(pair):.3f}  "
          f"median {statistics.median(pair):.3f}  max {max(pair):.3f}")

    print("\n  nearest neighbours by pgvector <=> (computed in-DB) for 3 seeds — "
          "are they topically related?")
    for s in rows[:3]:
        nn = await conn.fetch(
            """SELECT left(headline, 68) AS h,
                      1 - (embedding <=> (SELECT embedding FROM public.info_objects WHERE id=$1)) AS sim
               FROM public.info_objects
               WHERE id <> $1 AND embedding IS NOT NULL
               ORDER BY embedding <=> (SELECT embedding FROM public.info_objects WHERE id=$1)
               LIMIT 3""",
            s["id"])
        print(f"\n    seed: {s['h']}")
        for x in nn:
            print(f"       sim {x['sim']:.3f}  {x['h']}")


async def embed_purge(conn) -> None:
    """DESTRUCTIVE, but chosen for what actually reclaims space on THIS DB.

    DROP INDEX frees the HNSW index's pages to the OS immediately — a single
    fast catalog DDL, no scan, no statement_timeout risk. That is the real,
    reclaimable chunk.

    It deliberately does NOT bulk-NULL the 93k embedding values, even though the
    vectors are the md5-hash fallback. On this quota-throttled pooler a mass
    UPDATE (a) hits the same statement_timeout that cancels a 5k-row DELETE, and
    (b) writes 93k dead tuples whose space is not returned until a VACUUM FULL
    the pooler won't run — so it would raise pg_database_size, not lower it. The
    column should be nulled during the Neon migration / a maintenance window on a
    direct (session-mode) connection instead. Pass --embed-null-force to attempt
    it here anyway (tolerant: stops and reports on the first timeout)."""
    force_null = "--embed-null-force" in sys.argv

    print("\n  === EMBEDDING PURGE (DESTRUCTIVE) ===\n")
    if not await _table_exists(conn, "public", "info_objects"):
        print("  info_objects absent — nothing to do")
        return
    db = await conn.fetchval("SELECT current_database()")
    size_before = await conn.fetchval("SELECT pg_database_size($1)", db)
    idx_before = await conn.fetchval(
        "SELECT pg_relation_size(to_regclass('public.idx_info_embedding_hnsw'))")
    with_emb = await conn.fetchval(
        "SELECT count(*) FROM public.info_objects WHERE embedding IS NOT NULL")
    print(f"  {with_emb:,} rows carry an embedding; "
          f"idx_info_embedding_hnsw = {_mb(idx_before or 0)}; "
          f"database = {_mb(size_before)}\n")

    print("  DROP INDEX idx_info_embedding_hnsw …", flush=True)
    await conn.execute("DROP INDEX IF EXISTS public.idx_info_embedding_hnsw")
    print("    ✓ dropped")
    size_after_drop = await conn.fetchval("SELECT pg_database_size($1)", db)
    print(f"  database {_mb(size_before)} → {_mb(size_after_drop)} "
          f"(freed {_mb((size_before or 0) - (size_after_drop or 0))} immediately)")

    if not force_null:
        print("\n  NOT bulk-nulling the embedding values (see the note in the "
              "source): on this throttled pooler it times out and adds dead-tuple "
              "bloat that only VACUUM FULL can reclaim. Null the column during the "
              "migration/maintenance window instead.")
        return

    print("\n  --embed-null-force: nulling embeddings in batches "
          "(will stop cleanly on the first timeout)…")
    total = 0
    try:
        while True:
            status = await conn.execute(
                "UPDATE public.info_objects SET embedding=NULL WHERE ctid = ANY(ARRAY("
                "  SELECT ctid FROM public.info_objects WHERE embedding IS NOT NULL "
                f"  LIMIT {BATCH}))")
            n = int(status.rsplit(" ", 1)[-1]) if status.startswith("UPDATE") else 0
            total += n
            if n:
                print(f"    … nulled {total:,}", flush=True)
            if n < BATCH:
                break
        print(f"  nulled {total:,} embeddings.")
    except Exception as ex:                                        # noqa: BLE001
        print(f"  stopped after {total:,}: {type(ex).__name__}: {ex}")


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
    ap.add_argument("--embed-probe", action="store_true",
                    help="read-only: inspect stored info_objects.embedding vectors "
                         "(fingerprint + cosine + nearest neighbours) to confirm "
                         "whether they are real MiniLM or the md5-hash fallback")
    ap.add_argument("--embed-purge", action="store_true",
                    help="DESTRUCTIVE: DROP the info_objects HNSW index (reclaims it "
                         "immediately). Standalone — does not run the row-pruning "
                         "plan, and does NOT bulk-null embeddings unless "
                         "--embed-null-force is also given.")
    ap.add_argument("--embed-null-force", action="store_true",
                    help="with --embed-purge: also attempt to NULL every embedding "
                         "in batches (tolerant of the pooler timeout). Off by default "
                         "because on a throttled DB it adds unreclaimable bloat.")
    args = ap.parse_args()

    # statement_cache_size=0 is REQUIRED against Supabase's transaction pooler
    # (pgbouncer): cached server-side prepared statements collide across pooled
    # backends and raise DuplicatePreparedStatementError intermittently. This is
    # the same fix schema_audit.py already carries for the same DSN.
    conn = await asyncpg.connect(_dsn(), statement_cache_size=0)
    try:
        if args.embed_probe:
            await embed_probe(conn)
            return
        if args.embed_purge:
            await embed_purge(conn)
            print("\n  --- sizes after purge ---")
            await measure(conn)
            return
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
