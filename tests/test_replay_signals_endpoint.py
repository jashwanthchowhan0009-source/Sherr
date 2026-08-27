"""
/admin/replay-signals.

GitHub Actions cannot reach the database and there is no shell on the
deployment, but Render's DATABASE_URL works — so the endpoint is the only way to
run the replay. Its one real risk is subtle: if it grew its own copy of the
Signal shape, the replayed history would resolve to different entities than the
daily job's, and market_reaction would see two shallow series instead of one
deep one. So most of this pins that it runs THE WORKER, not a copy.
"""

import asyncio
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "sherrbyte"))



# ─── the import boundary ─────────────────────────────────────────────────────
def test_the_worker_imports_with_only_the_deployed_requirements():
    """The endpoint imports app.workers.market_signals from a service that ships
    neither pydantic-settings, pgvector, numpy nor redis. Both app/workers/
    __init__.py and market_signals itself must therefore keep app.config and
    app.db lazy — a package-level import there fails on Render and nowhere else.

    Run in a subprocess with those packages blocked: asserting on sys.modules
    in-process only proves nothing else has imported them yet.
    """
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
        "from app.workers import market_signals as ms\n"
        "assert ms._TICK_SOURCES and callable(ms.backfill_from_ticks)\n"
        "assert 'app.config' not in sys.modules and 'app.db' not in sys.modules\n"
        "print('OK')\n" % os.path.join(ROOT, "sherrbyte")
    )
    r = subprocess.run([sys.executable, "-c", probe],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, f"stdout={r.stdout}\nstderr={r.stderr}"


def test_the_cli_entry_point_still_works_after_the_lazy_imports():
    """bootstrap() and teardown() moved their imports inside the functions;
    `python -m app.workers.market_signals --help` must still resolve."""
    r = subprocess.run([sys.executable, "-m", "app.workers.market_signals", "--help"],
                       capture_output=True, text=True, timeout=120,
                       cwd=os.path.join(ROOT, "sherrbyte"))
    assert r.returncode == 0, r.stderr
    assert "--from-ticks" in r.stdout


# ─── the endpoint runs the worker, not a copy ────────────────────────────────
def test_the_endpoint_calls_the_workers_own_backfill():
    """If this ever stops being true, the two paths can drift apart silently."""
    import ast
    import inspect
    import main

    def code_only(fn):
        """Source with docstrings stripped — the prose legitimately mentions
        ref_id, and matching on it would pass or fail for the wrong reason."""
        tree = ast.parse(inspect.getsource(fn).lstrip())
        for node in ast.walk(tree):
            if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and ast.get_docstring(node)):
                node.body = node.body[1:]
        return ast.unparse(tree)

    src = code_only(main.admin_replay_signals) + code_only(main._run_signal_replay)
    assert "backfill_from_ticks" in src
    # No Signal is constructed here — the worker owns that shape.
    for token in ("Signal(", "SignalEntity", "market:", "domain_signals"):
        assert token not in src, f"the endpoint builds its own {token}"


def test_the_endpoint_reuses_the_pool_the_app_already_holds():
    """A second pool against the same database is avoidable — backfill_from_ticks
    takes any async context-manager factory."""
    import inspect
    import main
    src = inspect.getsource(main._run_signal_replay)
    assert "get_spie_pool" in src
    assert "conn_factory=pool.acquire" in src


def test_the_worker_backfill_accepts_a_caller_supplied_connection_factory():
    import inspect
    from app.workers import market_signals as ms
    params = inspect.signature(ms.backfill_from_ticks).parameters
    assert "conn_factory" in params
    assert params["conn_factory"].default is None    # defaults to the engine pool


# ─── progress ────────────────────────────────────────────────────────────────
def test_progress_is_empty_before_any_run_and_never_raises():
    from app.workers import market_signals as ms
    ms.PROGRESS.clear()
    assert ms.progress() == {}


def test_progress_reports_elapsed_while_running_and_after_finishing():
    import time
    from app.workers import market_signals as ms
    ms.PROGRESS.clear()
    ms.PROGRESS.update({"running": True, "started_at": time.time() - 5,
                        "finished_at": None})
    assert ms.progress()["elapsed_s"] >= 5
    ms.PROGRESS["finished_at"] = ms.PROGRESS["started_at"] + 2
    assert ms.progress()["elapsed_s"] == 2
    ms.PROGRESS.clear()


def test_a_failed_run_still_clears_the_running_flag():
    """PROGRESS is what the poller reads; a run that raises must not leave the
    endpoint reporting 'running' forever."""
    from app.workers import market_signals as ms

    class _Boom:
        async def __aenter__(self): raise RuntimeError("pool gone")
        async def __aexit__(self, *a): return False

    ms.PROGRESS.clear()
    with pytest.raises(RuntimeError):
        asyncio.run(ms.backfill_from_ticks(days=90, conn_factory=lambda: _Boom()))
    assert ms.PROGRESS["running"] is False
    assert ms.PROGRESS["finished_at"] is not None
    ms.PROGRESS.clear()


# ─── the endpoint's own guards ───────────────────────────────────────────────
def _client():
    """A client over the real app, patched on the MODULE rather than through the
    environment. main.py reads ADMIN_TOKEN and DISABLE_PENDING_DRAIN into
    constants at import time, so setting them in os.environ here would either
    arrive too late (main already imported) or leak into every other test module
    that imports main afterwards — test_feed_doctor's drain tests especially."""
    import main
    from fastapi.testclient import TestClient
    main.ADMIN_TOKEN = "test-token"
    main.collect_news = lambda: asyncio.sleep(0)
    main._drain_pending_if_stalled = lambda *a, **k: {"published": 0}
    return main, TestClient(main.app)


def test_the_route_is_registered_on_the_root_app():
    """The one that cost a round trip last time: the endpoint has to be on the
    app render.yaml actually starts."""
    from fastapi.routing import APIRoute
    import main
    paths = {r.path for r in main.app.routes if isinstance(r, APIRoute)}
    assert "/admin/replay-signals" in paths


def test_the_endpoint_refuses_without_the_admin_token():
    main, c = _client()
    with c:
        assert c.get("/admin/replay-signals").status_code == 403
        assert c.get("/admin/replay-signals?token=wrong").status_code == 403


def test_an_unconfigured_database_is_reported_rather_than_started():
    """Local sqlite development has no DSN; the endpoint must say so instead of
    launching a task that cannot do anything."""
    main, c = _client()
    original = main.SHERR_I_DATABASE_URL
    main.SHERR_I_DATABASE_URL = ""
    main._replay_task = None
    try:
        with c:
            out = c.get("/admin/replay-signals?token=test-token").json()
    finally:
        main.SHERR_I_DATABASE_URL = original
        main._replay_task = None
    assert out["status"] == "unconfigured"
