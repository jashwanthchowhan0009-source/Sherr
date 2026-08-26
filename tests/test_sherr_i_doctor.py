"""
The Sherr-I doctor and the ticks-backfill endpoint.

/patterns has three sources and two of them get conflated. 'unavailable' means
the query RAISED; an empty insights table reports 'engine' with no patterns. So
"no patterns" and "wrong source" are different faults, and the doctor has to keep
them apart rather than collapsing them into one unhelpful answer.

The Postgres half is skipped unless MARKET_TICKS_TEST_DSN is set — the checks are
information_schema lookups and real counts, which a stub cannot stand in for.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import market_ticks as mt      # noqa: E402
import sherr_i_doctor as doc   # noqa: E402

TEST_DSN = os.getenv("MARKET_TICKS_TEST_DSN", "")
needs_pg = pytest.mark.skipif(not TEST_DSN, reason="MARKET_TICKS_TEST_DSN not set")


# ─── redaction ───────────────────────────────────────────────────────────────
def test_the_password_never_appears_in_the_report():
    """This endpoint is meant to be opened from a phone and pasted into a chat."""
    out = doc.redact("postgresql://postgres.abc:hunter2@aws-0.pooler.supabase.com:6543/postgres")
    assert "hunter2" not in out
    assert "aws-0.pooler.supabase.com" in out and "postgres.abc" in out


def test_an_unparseable_dsn_does_not_take_the_whole_report_down():
    assert doc.redact("not a url at all") == "<unparseable>"
    assert doc.redact("") == ""


# ─── the migration table list ────────────────────────────────────────────────
def test_the_expected_tables_are_read_from_the_migration_files():
    """Hardcoding the list would drift the first time a migration was added."""
    tables = doc._migration_tables()
    for expected in ("insights", "entities", "domain_signals", "cooccurrence",
                     "instrument_keywords", "info_objects",
                     "sherrbyte_app.market_ticks"):
        assert expected in tables, f"{expected} missing from the parsed list"


# ─── a) connection ───────────────────────────────────────────────────────────
def test_no_dsn_is_reported_as_the_seed_case_not_as_a_connection_failure():
    """Empty DSN and unreachable DSN produce different /patterns sources, so the
    doctor must not describe them the same way."""
    async def never(): raise AssertionError("must not try to connect")
    out = asyncio.run(doc.check_connection("", "unset", never))
    assert out["configured"] is False
    assert "seed" in out["verdict"]


def test_a_pool_that_fails_to_build_is_reported_with_its_error():
    async def boom(): raise OSError("connection refused")
    out = asyncio.run(doc.check_connection("postgresql://u:p@h/db", "DATABASE_URL", boom))
    assert out["connected"] is False
    assert "connection refused" in out["error"]
    assert "unavailable" in out["verdict"]


def test_a_pool_factory_returning_none_is_not_mistaken_for_success():
    """get_spie_pool() swallows its own error and returns None."""
    async def nothing(): return None
    out = asyncio.run(doc.check_connection("postgresql://u:p@h/db", "DATABASE_URL", nothing))
    assert out["connected"] is False
    assert "unavailable" in out["verdict"]


# ─── e) scheduling ───────────────────────────────────────────────────────────
def test_the_deployed_scheduler_is_reported_as_not_running_the_detectors():
    """The whole point of (e): app/main.py schedules them, render.yaml does not
    start app/main.py."""
    out = doc.check_schedule(scheduler=None)
    assert out["runs_detectors_here"] is False
    assert "Nothing in production runs" in out["verdict"]
    assert ".github/workflows/cron_ingest.yml" in out["where_detectors_are_scheduled"]


def test_a_scheduler_that_does_run_detectors_is_reported_as_such():
    class _Job:
        def __init__(self, i): self.id, self.trigger, self.next_run_time = i, "cron", None

    class _Sched:
        def get_jobs(self): return [_Job("collect_news"), _Job("detectors")]

    out = doc.check_schedule(_Sched())
    assert out["runs_detectors_here"] is True
    assert "detectors" in out["verdict"]


def test_a_scheduler_that_raises_does_not_take_the_report_down():
    class _Sched:
        def get_jobs(self): raise RuntimeError("not started")

    assert doc.check_schedule(_Sched())["this_process_jobs"][0]["error"]


# ─── the blocker ordering ────────────────────────────────────────────────────
def _report(**over):
    base = {
        "a_connection": {"configured": True, "connected": True, "error": None},
        "b_migrations": {"tables_missing": []},
        "c_inputs": {"domain_signals": {"rows": 100, "by_domain": {
            "news": {"rows": 100, "distinct_days": 30},
            "market": {"rows": 60, "distinct_days": 10}}}},
        "e_schedule": {"runs_detectors_here": True},
    }
    base.update(over)
    return base


def test_a_missing_dsn_outranks_everything_downstream():
    """Fixing anything below a broken link changes nothing, so the first break
    is the only one worth reporting as THE blocker."""
    r = _report(a_connection={"configured": False, "connected": False, "error": None})
    assert doc._blocker(r).startswith("a)")


def test_missing_tables_outrank_empty_inputs():
    r = _report(b_migrations={"tables_missing": ["insights", "entities"]})
    assert doc._blocker(r).startswith("b)")
    assert "insights" in doc._blocker(r)


def test_no_market_signals_names_market_signals_as_the_producer():
    """The most likely real state: news ingests on cron, market never does."""
    r = _report(c_inputs={"domain_signals": {"rows": 100, "by_domain": {
        "news": {"rows": 100, "distinct_days": 30}}}})
    b = doc._blocker(r)
    assert "market_signals" in b


def test_too_little_market_history_is_distinguished_from_none_at_all():
    """One day of signals and zero days of signals need different fixes."""
    r = _report(c_inputs={"domain_signals": {"rows": 100, "by_domain": {
        "market": {"rows": 11, "distinct_days": 1}}}})
    b = doc._blocker(r)
    assert b.startswith("c)") and "needs 6" in b


def test_full_inputs_with_nothing_scheduled_blames_the_scheduler():
    """The case where every count looks healthy and /patterns still serves an
    empty list — because nothing ever writes insights."""
    r = _report(e_schedule={"runs_detectors_here": False})
    assert doc._blocker(r).startswith("e)")


def test_a_healthy_chain_defers_to_the_detector_run():
    assert "No structural blocker" in doc._blocker(_report())


# ─── the engine import ───────────────────────────────────────────────────────
def test_the_detector_package_imports_with_only_the_deployed_requirements():
    """app/spie/discovery must stay free of app.config and app.db: the root
    service ships neither pydantic-settings, pgvector, numpy nor redis, so a
    dependency creeping in there would fail on Render and nowhere else.

    Run in a subprocess with those four packages actively blocked. Asserting on
    sys.modules in-process only proves nothing ELSE has imported them yet, which
    depends on test ordering; blocking them proves the import genuinely does not
    need them.
    """
    import subprocess
    probe = (
        "import sys\n"
        "BLOCKED = {'pydantic_settings', 'pgvector', 'numpy', 'redis', 'arq'}\n"
        "class Blocker:\n"
        "    def find_module(self, name, path=None):\n"
        "        return self if name.split('.')[0] in BLOCKED else None\n"
        "    def load_module(self, name):\n"
        "        raise ImportError(name + ' is not in the root requirements')\n"
        "sys.meta_path.insert(0, Blocker())\n"
        "sys.path.insert(0, %r)\n"
        "from app.spie.discovery import market_reaction, REGISTRY\n"
        "assert 'market_reaction' in REGISTRY\n"
        "print('OK')\n" % str(doc._ENGINE_ROOT)
    )
    r = subprocess.run([sys.executable, "-c", probe],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, f"stdout={r.stdout}\nstderr={r.stderr}"
    assert "OK" in r.stdout


# ─── b) and c) against real Postgres ─────────────────────────────────────────
class _Pool:
    """asyncpg pool surface over a single connection."""
    def __init__(self, conn): self._c = conn

    def acquire(self):
        conn = self._c

        class _Ctx:
            async def __aenter__(self): return conn
            async def __aexit__(self, *a): return False
        return _Ctx()


@needs_pg
def test_a_database_with_none_of_the_engine_tables_reports_them_all_missing():
    """The migrations-never-applied case, which is indistinguishable from a
    wrong-database case without this list."""
    async def go():
        conn = await mt.connect(TEST_DSN)
        try:
            return await doc.check_migrations(conn)
        finally:
            await conn.close()

    out = asyncio.run(go())
    assert "insights" in out["tables_missing"]
    assert "have not been applied" in out["verdict"]


@needs_pg
def test_a_table_the_migrations_do_create_is_seen_as_present():
    """market_ticks is created by 020, so it is the one engine-side table this
    test database genuinely has — it proves presence is detected, not just
    absence."""
    async def go():
        conn = await mt.connect(TEST_DSN)
        try:
            await mt.ensure_schema(conn)
            return await doc.check_migrations(conn)
        finally:
            await conn.close()

    out = asyncio.run(go())
    assert "sherrbyte_app.market_ticks" in out["tables_present"]
    assert "sherrbyte_app" in out["schemas_seen"]


@needs_pg
def test_a_missing_input_table_is_reported_as_an_error_not_a_zero():
    """A count of 0 means 'the table is there and empty'; a missing table is a
    different diagnosis and must not be flattened into the same number."""
    async def go():
        conn = await mt.connect(TEST_DSN)
        try:
            await mt.ensure_schema(conn)
            return await doc.check_inputs(conn)
        finally:
            await conn.close()

    out = asyncio.run(go())
    assert out["domain_signals"]["rows"] is None
    assert "error" in out["domain_signals"]
    assert out["sherrbyte_app.market_ticks"]["rows"] == 0
    assert "empty" in out["_verdict"] or "no domain" in out["_verdict"]


@needs_pg
def test_the_connection_check_reports_the_live_server_when_it_works():
    async def go():
        conn = await mt.connect(TEST_DSN)

        async def pool(): return _Pool(conn)
        try:
            return await doc.check_connection(TEST_DSN, "DATABASE_URL", pool)
        finally:
            await conn.close()

    out = asyncio.run(go())
    assert out["connected"] is True
    assert out["verdict"] == "Pool is up."
    assert out["database"] and out["search_path"]
