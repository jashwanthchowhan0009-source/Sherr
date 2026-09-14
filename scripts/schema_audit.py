#!/usr/bin/env python3
"""scripts/schema_audit.py — READ-ONLY inventory of both database schemas.

WHY THIS EXISTS
===============
root main.py (the site, served by Render) runs its SQL through pgcompat with
search_path=sherrbyte_app. The engine workers (GitHub-Actions crons) write
UNQUALIFIED, i.e. into `public`. Same physical Supabase database, two schemas.
The symptom is a site reading empty tables while the engine fills different
ones — and nothing errors, because both schemas exist.

This script answers, without changing anything, the one question that settles
which copy is live and which is dead: for every table in BOTH `public` and
`sherrbyte_app`, how many rows does it hold and how fresh is its newest row.

It performs ONLY SELECT / COUNT / MAX. It creates nothing, drops nothing, and
writes nothing. Run it, read the markdown it prints, decide from that.

USAGE
=====
    DATABASE_URL='postgresql://…' python scripts/schema_audit.py
    # or:  python scripts/schema_audit.py --schemas public sherrbyte_app

The DSN is read from DATABASE_URL (falling back to SHERR_I_DATABASE_URL) — the
same variable the engine and the deployed app use, so this audits exactly the
database they talk to.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Query params that belong to SQLAlchemy's asyncpg dialect or PgBouncer, not to
# raw asyncpg — passing them in the DSN raises "unexpected connection
# parameter". Stripped exactly as app/db/supabase.py does, so a pasted Supabase
# *pooler* URL connects cleanly here too.
_UNSUPPORTED_QS = {
    "pgbouncer", "prepared_statement_cache_size", "statement_cache_size",
    "prepared_statements", "prepare_threshold",
}

DEFAULT_SCHEMAS = ["public", "sherrbyte_app"]

# The column a table is "current as of", most-specific first. The first of these
# that exists on a table AND is a date/timestamp type is used for MAX(). A table
# with none of them simply reports no timestamp — that is data, not an error.
_TS_PRIORITY = [
    "created_at", "ingested_at", "collected_at", "built_at", "occurred_at",
    "published_at", "computed_at", "checked_at", "fired_at", "saved_at",
    "updated_at", "last_login", "started_at", "ts", "timestamp", "time", "date",
]
_TS_TYPES = {
    "timestamp with time zone", "timestamp without time zone", "date",
}


def _sanitize_dsn(dsn: str) -> str:
    parts = urlsplit(dsn)
    if not parts.query:
        return dsn
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in _UNSUPPORTED_QS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(kept), parts.fragment))


def _qi(ident: str) -> str:
    """Quote an identifier for safe interpolation (double any embedded quote)."""
    return '"' + ident.replace('"', '""') + '"'


async def _tables(conn, schema: str) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'
         ORDER BY table_name
        """,
        schema,
    )
    return [r["table_name"] for r in rows]


async def _timestamp_column(conn, schema: str, table: str) -> str | None:
    rows = await conn.fetch(
        """
        SELECT column_name, data_type
          FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
        """,
        schema, table,
    )
    have = {r["column_name"]: r["data_type"] for r in rows}
    for name in _TS_PRIORITY:
        if name in have and have[name] in _TS_TYPES:
            return name
    return None


async def _inspect(conn, schema: str, table: str) -> dict:
    """Row count + newest-row timestamp for one table. Read-only, and a failure
    on one table (permissions, an odd type) is captured, never fatal."""
    fq = f"{_qi(schema)}.{_qi(table)}"
    out = {"schema": schema, "table": table, "rows": None,
           "ts_col": None, "latest": None, "error": None}
    try:
        out["rows"] = await conn.fetchval(f"SELECT COUNT(*) FROM {fq}")
    except Exception as ex:                                    # noqa: BLE001
        out["error"] = f"count failed: {type(ex).__name__}: {ex}"
        return out
    ts_col = await _timestamp_column(conn, schema, table)
    out["ts_col"] = ts_col
    if ts_col:
        try:
            val = await conn.fetchval(
                f"SELECT MAX({_qi(ts_col)}) FROM {fq}")
            out["latest"] = None if val is None else str(val)
        except Exception as ex:                               # noqa: BLE001
            out["error"] = f"max({ts_col}) failed: {type(ex).__name__}: {ex}"
    return out


def _fmt_rows(n) -> str:
    return "—" if n is None else f"{n:,}"


def _fmt_latest(rec: dict) -> str:
    if rec["error"]:
        return f"⚠️ {rec['error']}"
    if not rec["ts_col"]:
        return "— (no timestamp col)"
    if rec["latest"] is None:
        return f"NULL ({rec['ts_col']})"
    return f"{rec['latest']} ({rec['ts_col']})"


def _render(by_schema: dict[str, list[dict]], schemas: list[str]) -> str:
    lines: list[str] = []
    lines.append("## Schema audit\n")

    # 1) Per-schema inventory: every table, its row count, its newest row.
    lines.append("### Every table, by schema\n")
    lines.append("| Schema | Table | Rows | Latest row |")
    lines.append("|---|---|---:|---|")
    for schema in schemas:
        recs = by_schema.get(schema, [])
        if not recs:
            lines.append(f"| {schema} | _(no base tables)_ | — | — |")
            continue
        for r in recs:
            lines.append(
                f"| {schema} | `{r['table']}` | {_fmt_rows(r['rows'])} "
                f"| {_fmt_latest(r)} |")
    lines.append("")

    # 2) The one that matters: tables present in BOTH schemas, side by side, so
    #    live-vs-dead is a single glance (which copy has rows / recent rows).
    names_by_schema = {s: {r["table"] for r in by_schema.get(s, [])}
                       for s in schemas}
    common = sorted(set.intersection(*[names_by_schema[s] for s in schemas])) \
        if len(schemas) >= 2 else []
    lines.append(f"### Tables present in BOTH schemas ({len(common)})\n")
    if not common:
        lines.append("_None — the two schemas share no table names._\n")
    else:
        header = "| Table |" + "".join(
            f" {s} rows | {s} latest |" for s in schemas)
        sep = "|---|" + "".join("---:|---|" for _ in schemas)
        lines.append(header)
        lines.append(sep)
        idx = {s: {r["table"]: r for r in by_schema.get(s, [])} for s in schemas}
        for t in common:
            cells = [f"`{t}`"]
            for s in schemas:
                r = idx[s][t]
                cells.append(_fmt_rows(r["rows"]))
                cells.append(_fmt_latest(r))
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
        lines.append("> For each shared table, the copy with rows AND a recent "
                     "`latest` is live; the empty/stale copy is dead.\n")
    return "\n".join(lines)


async def _run(dsn: str, schemas: list[str]) -> str:
    import asyncpg

    conn = await asyncpg.connect(_sanitize_dsn(dsn), statement_cache_size=0,
                                 timeout=30.0)
    try:
        resolved = await conn.fetchval("SHOW search_path")
        by_schema: dict[str, list[dict]] = {}
        for schema in schemas:
            tables = await _tables(conn, schema)
            by_schema[schema] = [await _inspect(conn, schema, t) for t in tables]
    finally:
        await conn.close()

    md = _render(by_schema, schemas)
    return f"_Connected search_path: `{resolved}`_\n\n{md}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only audit of DB schemas.")
    ap.add_argument("--schemas", nargs="+", default=DEFAULT_SCHEMAS,
                    help="schemas to inventory (default: public sherrbyte_app)")
    args = ap.parse_args()

    dsn = os.getenv("DATABASE_URL") or os.getenv("SHERR_I_DATABASE_URL")
    if not dsn:
        print("ERROR: set DATABASE_URL (or SHERR_I_DATABASE_URL) to the "
              "Supabase connection string.", file=sys.stderr)
        return 2
    try:
        print(asyncio.run(_run(dsn, args.schemas)))
    except Exception as ex:                                    # noqa: BLE001
        print(f"ERROR: audit failed: {type(ex).__name__}: {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
