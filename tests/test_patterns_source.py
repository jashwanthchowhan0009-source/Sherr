"""
Guards the /patterns provenance contract (Part A).

The SPIE tab was showing sqlite demo rows that looked exactly like real insights.
Every response must now declare where the data came from, and a configured-but-
broken engine must NEVER be papered over with demo rows.
"""

import re
import pathlib

SRC = pathlib.Path(__file__).resolve().parent.parent / "main.py"
CODE = SRC.read_text()


def test_engine_response_is_labelled_engine():
    assert 'data["source"] = "engine"' in CODE


def test_engine_failure_returns_unavailable_not_seed():
    """A configured engine that errors must return empty + 'unavailable', so the
    app can say so instead of showing fake patterns."""
    # the failure path RETURNS immediately — it must not fall through to the seed
    assert 'return {"patterns": [], "total": 0, "source": "unavailable"' in CODE


def test_sqlite_rows_are_labelled_seed():
    assert '"source": "seed"' in CODE


def test_no_sprie_naming_remains():
    """Rename completed: SPRIE -> SPIE everywhere."""
    for f in ["main.py", "index.html"]:
        text = (SRC.parent / f).read_text()
        assert not re.search(r"SPRIE|sprie", text), f"SPRIE naming still in {f}"


# ─── single-service wiring: /patterns reads Supabase directly ────────────────
def test_postgres_is_tried_before_engine_url():
    """One Render service runs both the app and the engine against the same
    Supabase, so /patterns must read insights from the DB directly — ENGINE_URL
    is only for a separate engine deployment."""
    assert CODE.index("if SPIE_DATABASE_URL:") < CODE.index("if ENGINE_URL:")


def test_spie_db_url_falls_back_to_database_url():
    assert 'os.getenv("SPIE_DATABASE_URL") or os.getenv("DATABASE_URL")' in CODE


def test_pooler_safety_on_the_read_pool():
    """Supabase transaction pooler: no server-side prepared-statement cache, and
    pgbouncer-only query params stripped from the DSN."""
    assert "statement_cache_size=0" in CODE
    assert "_sanitize_pg_dsn" in CODE


def test_unreachable_db_reports_unavailable_not_seed():
    assert "Configured Postgres (DATABASE_URL) is not reachable" in CODE


def test_asyncpg_is_declared_for_the_app_service():
    reqs = (SRC.parent / "requirements.txt").read_text()
    assert "asyncpg" in reqs
