#!/usr/bin/env python3
"""scripts/db_cleanup.py — DRY-RUN reclaimable-space report. Deletes NOTHING.

The corpus carries two kinds of dead weight that a free-tier Postgres pays for:

  * BODY-LESS rows — an article/info_object whose body is empty or still the
    drain's placeholder stub. It can never render as a real card.
  * NEVER-SURFACED rows — an article that is not published, or was never
    AI-processed, so no reader path has ever shown it.

This reports how many such rows exist in sherrbyte_app.articles and
public.info_objects and how many bytes deleting them WOULD free (live tuple
bytes via pg_column_size — TOAST/index overhead means the real reclaim is
somewhat larger). It runs SELECT/SUM only and prints a markdown table. It does
NOT delete anything, and prints the exact predicate it counted so a human can
decide and write the DELETE themselves.

    DATABASE_URL='postgresql://…' python scripts/db_cleanup.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_UNSUPPORTED_QS = {
    "pgbouncer", "prepared_statement_cache_size", "statement_cache_size",
    "prepared_statements", "prepare_threshold",
}

# First body-ish column that exists on a table is used for the body-less test.
_BODY_COLS = ["full_body", "body", "content", "clean_text", "text", "summary_60"]
# Placeholder stubs a body may still hold (matched with LIKE, case-sensitive).
_STUBS = ["Sherr AI is preparing%", "Sherr is preparing%", "%is preparing this%"]


def _sanitize_dsn(dsn: str) -> str:
    parts = urlsplit(dsn)
    if not parts.query:
        return dsn
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in _UNSUPPORTED_QS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(kept), parts.fragment))


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


async def _columns(conn, schema: str, table: str) -> set:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=$1 AND table_name=$2", schema, table)
    return {r["column_name"] for r in rows}


def _mb(n) -> str:
    return "—" if n is None else f"{(n / 1_048_576):.1f} MB"


async def _measure(conn, schema: str, table: str) -> dict:
    fq = f"{_qi(schema)}.{_qi(table)}"
    out = {"table": f"{schema}.{table}", "exists": False, "total": None,
           "predicates": {}}
    if await conn.fetchval("SELECT to_regclass($1)", f"{schema}.{table}") is None:
        return out
    out["exists"] = True
    out["total"] = await conn.fetchval(f"SELECT COUNT(*) FROM {fq}")
    cols = await _columns(conn, schema, table)

    predicates: list[tuple[str, str]] = []

    body_col = next((c for c in _BODY_COLS if c in cols), None)
    if body_col:
        stub_or = " OR ".join(f"{_qi(body_col)} LIKE '{s}'" for s in _STUBS)
        predicates.append((
            "body-less",
            f"{_qi(body_col)} IS NULL OR btrim({_qi(body_col)}) = '' OR {stub_or}"))

    surfaced_bits = []
    if "status" in cols:
        surfaced_bits.append("status <> 'published'")
    if "ai_processed" in cols:
        surfaced_bits.append("ai_processed = 0")
    if surfaced_bits:
        predicates.append(("never-surfaced", " OR ".join(surfaced_bits)))

    for label, where in predicates:
        try:
            rows = await conn.fetchval(f"SELECT COUNT(*) FROM {fq} WHERE {where}")
            byts = await conn.fetchval(
                f"SELECT COALESCE(SUM(pg_column_size(t.*)), 0) FROM {fq} t "
                f"WHERE {where}")
            out["predicates"][label] = {"rows": rows, "bytes": byts,
                                        "where": where}
        except Exception as ex:                               # noqa: BLE001
            out["predicates"][label] = {"error": f"{type(ex).__name__}: {ex}",
                                        "where": where}
    return out


def _render(results: list[dict]) -> str:
    lines = ["## DB cleanup — DRY RUN (nothing deleted)\n",
             "| Table | Total rows | Candidate | Rows | Reclaimable |",
             "|---|---:|---|---:|---:|"]
    for r in results:
        if not r["exists"]:
            lines.append(f"| `{r['table']}` | _absent_ | — | — | — |")
            continue
        if not r["predicates"]:
            lines.append(f"| `{r['table']}` | {r['total']:,} | "
                         "_(no body/status columns)_ | — | — |")
            continue
        for label, m in r["predicates"].items():
            if "error" in m:
                lines.append(f"| `{r['table']}` | {r['total']:,} | {label} | "
                             f"⚠️ {m['error']} | — |")
            else:
                lines.append(f"| `{r['table']}` | {r['total']:,} | {label} | "
                             f"{m['rows']:,} | {_mb(m['bytes'])} |")
    lines.append("\n### Predicates counted (write your own DELETE from these)\n")
    for r in results:
        for label, m in r.get("predicates", {}).items():
            lines.append(f"- `{r['table']}` — {label}: `WHERE {m['where']}`")
    lines.append("\n> Dry run only. No rows were deleted. Reclaimable is live "
                 "tuple bytes (pg_column_size); real space freed after VACUUM is "
                 "typically larger once index and TOAST overhead is included.")
    return "\n".join(lines)


async def _run(dsn: str) -> str:
    import asyncpg
    conn = await asyncpg.connect(_sanitize_dsn(dsn), statement_cache_size=0,
                                 timeout=30.0)
    try:
        targets = [("sherrbyte_app", "articles"), ("public", "info_objects"),
                   ("public", "articles")]
        results = [await _measure(conn, s, t) for s, t in targets]
    finally:
        await conn.close()
    return _render(results)


def main() -> int:
    dsn = os.getenv("DATABASE_URL") or os.getenv("SHERR_I_DATABASE_URL")
    if not dsn:
        print("ERROR: set DATABASE_URL (or SHERR_I_DATABASE_URL).", file=sys.stderr)
        return 2
    try:
        print(asyncio.run(_run(dsn)))
    except Exception as ex:                                   # noqa: BLE001
        print(f"ERROR: cleanup dry-run failed: {type(ex).__name__}: {ex}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
