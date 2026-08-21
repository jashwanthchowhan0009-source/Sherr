"""
/health is a liveness probe.

Render kills an instance whose health check does not answer in 5 seconds, and the
restart drops it straight back into the same collection cycle — a boot loop
caused entirely by the probe. So the contract is narrow and worth pinning: no
database, no upstream, no I/O, and fast even when everything behind it is slow.
"""

import importlib
import os
import sqlite3
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = str(tmp_path / "h.db")
    for k, v in [("DB_PATH", db), ("ENV", "dev"), ("ADMIN_TOKEN", "t"),
                 ("JWT_SECRET", "s"), ("COLLECT_INTERVAL_MIN", "999")]:
        monkeypatch.setenv(k, v)
    import main
    importlib.reload(main)
    main.init_db()
    conn = main.get_db()
    for i in range(1, 40):
        conn.execute(
            "INSERT INTO articles(url, headline, full_body, source_name, pillar_id, "
            "published_at, ai_processed, status, is_trending) "
            "VALUES(?,?,?,?,?,?,1,'published',?)",
            (f"https://e/{i}", f"Story {i}", "Body.", "Reuters", (i % 9) + 1,
             "2026-08-20", 1 if i % 5 == 0 else 0))
    conn.commit()
    conn.close()
    from fastapi.testclient import TestClient
    return main, TestClient(main.app)


# ─── the probe touches nothing ────────────────────────────────────────────────
def test_health_is_200_and_carries_no_counts(app):
    _, cl = app
    body = cl.get("/health").json()
    assert body["status"] == "ok"
    for k in ("articles", "users", "pillar_counts", "ai_processed", "trending"):
        assert k not in body, f"{k} means a database query on the liveness path"


def test_health_opens_no_database_connection(app, monkeypatch):
    """The strongest form of the contract: make get_db explode and the probe must
    still answer. Thirteen COUNT(*) round trips over a pooler is what blew the
    5s budget, and no amount of tuning them is as reliable as not running them."""
    main, cl = app
    def boom():
        raise AssertionError("/health must not touch the database")
    monkeypatch.setattr(main, "get_db", boom)
    assert cl.get("/health").status_code == 200


def test_health_answers_while_the_database_is_slow(app, monkeypatch):
    """A pooler under load is exactly the condition the probe has to survive."""
    main, cl = app
    real = main.get_db
    def slow():
        time.sleep(6)
        return real()
    monkeypatch.setattr(main, "get_db", slow)
    t = time.time()
    assert cl.get("/health").status_code == 200
    assert time.time() - t < 1.0


def test_health_needs_no_auth(app):
    """Render sends no headers."""
    _, cl = app
    assert cl.get("/health").status_code == 200


# ─── the counts moved, and got cheaper ────────────────────────────────────────
def test_admin_stats_carries_what_health_gave_up(app):
    _, cl = app
    body = cl.get("/admin/stats", headers={"x-admin-token": "t"}).json()
    assert body["articles"] == 39
    assert body["ai_processed"] == 39
    assert body["trending"] == 7
    assert sum(body["pillar_counts"].values()) == 39
    assert len(body["pillar_counts"]) == 9


def test_admin_stats_is_guarded(app):
    _, cl = app
    assert cl.get("/admin/stats").status_code == 403


def test_pillar_counts_cost_one_query_not_nine(app):
    """The old shape ran one COUNT(*) per pillar — nine pooler round trips to
    compute what a single GROUP BY answers in one scan."""
    import inspect

    main, _ = app
    src = inspect.getsource(main.admin_stats)
    assert "GROUP BY pillar_id" in src
    assert "for pid in range(1, 10)" not in src.split("pillar_counts")[0]


def test_stats_runs_off_the_event_loop(app):
    import inspect

    main, _ = app
    assert "run_in_executor" in inspect.getsource(main.admin_stats)


# ─── the collector must not starve the probe ──────────────────────────────────
def test_the_write_batch_runs_in_an_executor():
    """One synchronous round trip per article, on the event loop, for a batch of
    a hundred — that is seconds during which the probe cannot be answered."""
    import inspect

    import main
    src = inspect.getsource(main.collect_news)
    assert "run_in_executor(None, _write_batch)" in src
    assert "run_in_executor(None, link_stories" in src


def test_the_boot_drain_cannot_hold_the_port_closed():
    """Uvicorn does not accept connections until lifespan startup returns, so an
    unbounded drain is the same boot loop by a different route."""
    import inspect

    import main
    src = inspect.getsource(main.lifespan)
    assert "wait_for" in src and "DRAIN_BOOT_BUDGET_S" in src
    assert main.DRAIN_BOOT_BUDGET_S > 0
