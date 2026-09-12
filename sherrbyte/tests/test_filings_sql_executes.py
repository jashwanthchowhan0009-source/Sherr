"""test_filings_sql_executes.py — filings & watchlist-edge SQL must run on Postgres.

The same hole test_analog_sql_executes closes, for the new code: a raw SQL string
that a unit test never sends to a real server can carry a column/type/operator
bug that is fatal at runtime and invisible in CI. This applies migrations 028 and
029 (idempotent) and then PREPARES every module-level statement the filings and
watchlist-edge paths use — the planner resolves every column and cast without
needing rows.

Requires SHERR_ENGINE_TEST_DSN. Skipped without it, so a contributor without
Postgres is not blocked; CI sets it.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SHERR = os.path.dirname(_HERE)
sys.path.insert(0, _SHERR)                          # sherrbyte/ -> import app.*

DSN = os.getenv("SHERR_ENGINE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="SHERR_ENGINE_TEST_DSN not set")

_MIGRATIONS = ["028_filings.sql", "029_watchlist_edges.sql"]


def _migration_sql(name: str) -> str:
    path = os.path.join(_SHERR, "app", "db", "migrations", name)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


async def _run():
    import asyncpg

    from app.spie.filings.ingest import _INSERT
    from app.spie.proof.data import _WATCHLIST_EDGES_SQL

    conn = await asyncpg.connect(DSN)
    try:
        # Migrations are idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING), so
        # applying them here both proves the DDL parses on the server and makes
        # the test self-sufficient on a fresh database.
        for name in _MIGRATIONS:
            await conn.execute(_migration_sql(name))

        # PREPARE resolves every column, cast and type without mutating anything.
        for sql in (_INSERT, _WATCHLIST_EDGES_SQL):
            await conn.prepare(sql)

        # The endpoint's read queries, too.
        for sql in (
            "SELECT COUNT(*) FROM sherrbyte_app.filings",
            "SELECT source, COUNT(*) AS c, MAX(filing_date) AS latest "
            "  FROM sherrbyte_app.filings GROUP BY source ORDER BY source",
            "SELECT event_class, COUNT(*) AS c FROM sherrbyte_app.filings "
            "GROUP BY event_class ORDER BY c DESC",
        ):
            await conn.prepare(sql)
    finally:
        await conn.close()


def test_filings_and_watchlist_sql_executes():
    asyncio.run(_run())
