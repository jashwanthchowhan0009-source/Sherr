"""Every raw SQL statement in the analog package must EXECUTE on Postgres.

Phase 1 crashed in production with:

    asyncpg.exceptions.UndefinedFunctionError: operator does not exist:
    timestamp with time zone ~ unknown

`_SCAN_SQL` applied the regex operator to published_at. That column is TEXT
under the sqlite-shaped schema and timestamptz once migration 018 has run, and
nothing in the unit tests ever sent the statement to a real server — so a type
mismatch that is fatal at runtime was invisible in CI.

This file closes that hole for the whole package: it extracts every module-level
SQL string from the analog modules and PREPARES each one against a real
PostgreSQL server. Preparing is enough — the planner resolves every operator,
function and column type without needing rows or mutating anything — and it
fails loudly on exactly the class of bug that got through.

Requires SHERR_ENGINE_TEST_DSN. Skipped without it, so a contributor without
Postgres is not blocked; CI sets it.
"""
import ast
import asyncio
import os
import re
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sherrbyte"))

DSN = os.getenv("SHERR_ENGINE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="SHERR_ENGINE_TEST_DSN not set")

# Every module in the package that talks to the database, plus news_match,
# which carries the SAME regex-on-published_at pattern and would have failed the
# same way.
_MODULES = [
    "sherrbyte/app/spie/analog/event_library.py",
    "sherrbyte/app/spie/analog/matcher.py",
    "sherrbyte/app/spie/analog/reaction.py",
    "sherrbyte/app/spie/analog/calibration.py",
    "sherrbyte/app/spie/analog/cards.py",
    "sherrbyte/app/spie/discovery/news_match.py",
]

_SQL_START = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|WITH)\b", re.I)


def _sql_literals(path: str) -> list:
    """(name, sql) for every module-level string constant that looks like SQL."""
    src = open(os.path.join(_ROOT, path)).read()
    out = []
    for node in ast.parse(src).body:
        if not isinstance(node, ast.Assign):
            continue
        val = node.value
        if not (isinstance(val, ast.Constant) and isinstance(val.value, str)):
            continue
        if not _SQL_START.match(val.value):
            continue
        name = node.targets[0].id if isinstance(node.targets[0], ast.Name) \
            else "?"
        out.append((f"{os.path.basename(path)}:{name}", val.value))
    return out


ALL_SQL = [s for m in _MODULES for s in _sql_literals(m)]


# Every table the package's statements name. Built once per module, because a
# prepare() only resolves types if the relations exist — and an earlier version
# of this file passed only when another test happened to have created
# sherrbyte_app.articles first, which is a test that reports the run order
# rather than the code.
_SETUP = """
CREATE SCHEMA IF NOT EXISTS sherrbyte_app;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS sherrbyte_app.articles (
    id BIGSERIAL PRIMARY KEY, headline TEXT, summary_60 TEXT, full_body TEXT,
    source_summary TEXT, source_name TEXT, url TEXT, status TEXT,
    published_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS sherrbyte_app.market_ticks (
    symbol TEXT, market_type TEXT, price DOUBLE PRECISION,
    change_24h DOUBLE PRECISION, ts TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), canonical_name TEXT,
    type TEXT, norm_key TEXT);
CREATE TABLE IF NOT EXISTS cooccurrence (
    entity_a UUID, entity_b UUID, window_start DATE, count INTEGER,
    last_seen TIMESTAMPTZ, npmi DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS watchlist (
    entity_a UUID, entity_b UUID, kind TEXT, score REAL, npmi REAL,
    detail JSONB, seen_at TIMESTAMPTZ);
"""


@pytest.fixture(scope="module", autouse=True)
def schema():
    """Create every relation the statements name, then drop what we made."""
    import asyncpg

    async def go(sql):
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    for path in ("sherrbyte/app/db/migrations/022_event_library.sql",
                 "sherrbyte/app/db/migrations/023_analog_reactions.sql"):
        asyncio.run(go(_SETUP))
        asyncio.run(go(open(os.path.join(_ROOT, path)).read()))
    yield
    # NOTHING IS DROPPED. SHERR_ENGINE_TEST_DSN points at a database shared with
    # every other Postgres-backed test in the suite, and an earlier version of
    # this fixture dropped `entities` and `sherrbyte_app.articles` on the way
    # out — which broke eleven tests in other files that had nothing to do with
    # this one. Every statement above is CREATE IF NOT EXISTS, so leaving the
    # relations in place is both safe and idempotent.


def test_the_extractor_actually_found_the_statements():
    """A test that silently examines nothing passes for the wrong reason."""
    names = {n for n, _ in ALL_SQL}
    assert len(ALL_SQL) >= 6, f"only found {names}"
    assert any("event_library.py:_SCAN_SQL" == n for n in names), names


def _prepare_all(asyncpg, statements):
    async def go():
        conn = await asyncpg.connect(DSN)
        failures = []
        try:
            for name, sql in statements:
                try:
                    # prepare() runs the planner: every operator, function and
                    # column type is resolved, without touching a single row.
                    await conn.prepare(sql)
                except Exception as e:                            # noqa: BLE001
                    failures.append(f"{name}: {type(e).__name__}: {e}")
        finally:
            await conn.close()
        return failures
    return asyncio.run(go())


def test_every_analog_statement_prepares_on_postgres():
    """THE REGRESSION. `published_at ~ ...` against a timestamptz column raises
    here instead of in production."""
    import asyncpg
    failures = _prepare_all(asyncpg, ALL_SQL)
    assert not failures, "SQL that will not execute:\n  " + "\n  ".join(failures)


def test_the_scan_statement_works_against_a_timestamptz_column():
    """The exact production shape: articles.published_at migrated to
    timestamptz by 018. The old statement died on this; the ::text cast makes
    the guard mean the same thing under both column types."""
    import asyncpg
    from app.spie.analog import event_library

    async def go():
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS sherrbyte_app")
            await conn.execute("DROP TABLE IF EXISTS sherrbyte_app.articles")
            await conn.execute(
                "CREATE TABLE sherrbyte_app.articles ("
                " id BIGSERIAL PRIMARY KEY, headline TEXT, summary_60 TEXT,"
                " full_body TEXT, source_summary TEXT, status TEXT,"
                " published_at TIMESTAMPTZ)")
            await conn.execute(
                "INSERT INTO sherrbyte_app.articles"
                " (headline, summary_60, full_body, source_summary, status,"
                "  published_at) VALUES ('h','s','b','p','published', now())")
            rows = await conn.fetch(event_library._SCAN_SQL, 0, 10)
            assert len(rows) == 1
            assert rows[0]["occurred_at"] is not None
        finally:
            await conn.execute("DROP TABLE IF EXISTS sherrbyte_app.articles")
            await conn.close()

    asyncio.run(go())


def test_the_scan_statement_still_works_against_a_text_column():
    """Both schemas, one statement — that is the whole point of the cast."""
    import asyncpg
    from app.spie.analog import event_library

    async def go():
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS sherrbyte_app")
            await conn.execute("DROP TABLE IF EXISTS sherrbyte_app.articles")
            await conn.execute(
                "CREATE TABLE sherrbyte_app.articles ("
                " id BIGSERIAL PRIMARY KEY, headline TEXT, summary_60 TEXT,"
                " full_body TEXT, source_summary TEXT, status TEXT,"
                " published_at TEXT)")
            await conn.execute(
                "INSERT INTO sherrbyte_app.articles"
                " (headline, summary_60, full_body, source_summary, status,"
                "  published_at) VALUES ('h','s','b','p','published',"
                " '2026-08-31T10:00:00+00:00')")
            # And a row with an unparseable stamp: the guard must exclude it
            # rather than let the cast abort the whole scan.
            await conn.execute(
                "INSERT INTO sherrbyte_app.articles"
                " (headline, summary_60, full_body, source_summary, status,"
                "  published_at) VALUES ('h2','s','b','p','published','junk')")
            rows = await conn.fetch(event_library._SCAN_SQL, 0, 10)
            assert len(rows) == 1, "the unparseable row was not filtered out"
        finally:
            await conn.execute("DROP TABLE IF EXISTS sherrbyte_app.articles")
            await conn.close()

    asyncio.run(go())
