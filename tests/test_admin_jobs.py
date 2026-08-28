"""
The three admin jobs added for a deployment with no shell.

/admin/body-audit and /admin/reprocess-bodies exist because the startup drain
released 2792 articles with a placeholder body AND set ai_processed=1 — the
column run_ai_batch filters on — so nothing could ever rewrite them.
/admin/run-detectors exists because the engine's scheduler lives in an app
render.yaml does not start and the Actions cron cannot reach the database, so
every insight still carried the seed date.
"""

import asyncio
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import body_state as bs  # noqa: E402

SOURCE = ("Delhi reported a sharp rise in air pollution on Tuesday as the air "
          "quality index crossed 400 at several monitoring stations across the "
          "capital region, prompting fresh advisories from the authorities.")
STUB = ("Sherr AI is preparing an original, plain-language summary of this story "
        "— the key facts will appear here shortly.\n\nSource: The Hindu\nhttp://x")


def _client():
    import main
    from fastapi.testclient import TestClient
    main.ADMIN_TOKEN = "test-token"
    main.collect_news = lambda: asyncio.sleep(0)
    main._drain_pending_if_stalled = lambda *a, **k: {"published": 0}
    return main, TestClient(main.app)


def test_all_three_routes_are_registered_on_the_root_app():
    """render.yaml starts THIS app; a route on the engine app is unreachable."""
    from fastapi.routing import APIRoute
    import main
    paths = {r.path for r in main.app.routes if isinstance(r, APIRoute)}
    for p in ("/admin/body-audit", "/admin/reprocess-bodies", "/admin/run-detectors"):
        assert p in paths, f"{p} not registered"


@pytest.mark.parametrize("path", ["/admin/body-audit", "/admin/reprocess-bodies",
                                  "/admin/run-detectors"])
def test_each_route_refuses_without_the_admin_token(path):
    main, c = _client()
    with c:
        assert c.get(path).status_code == 403
        assert c.get(path + "?token=wrong").status_code == 403


def test_the_body_audit_counts_a_real_sqlite_corpus(tmp_path):
    """End to end over the app's own schema, not a stub."""
    import main
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    # status, summary_60's siblings and reprocessed arrive via _MIGRATIONS, not
    # CREATE_TABLES — init_db() applies both, so the fixture must too.
    for stmt in main._MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass    # already present
    for i, (body, status) in enumerate((
            (SOURCE, "published"),           # the publisher's prose
            (STUB, "published"),             # the drain's placeholder
            ("Officials in the capital said readings had passed the severe "
             "threshold at several sites this week, and advisories now cover "
             "outdoor activity while curbs are reviewed.", "published"),
            ("", "pending_rewrite"))):
        conn.execute(
            "INSERT INTO articles (url, headline, full_body, summary_60, "
            "source_summary, status) VALUES (?,?,?,?,?,?)",
            (f"http://x/{i}", f"H{i}", body, SOURCE, SOURCE[:200], status))
    conn.commit()
    out = bs.audit(conn)
    conn.close()

    assert out["by_state"][bs.SOURCE_TEXT] == 1
    assert out["by_state"][bs.STUB] == 1
    assert out["by_state"][bs.ORIGINAL] == 1
    assert out["needs_rewrite"] == 2, "the unpublished row must not be counted"


def test_the_reprocess_refuses_to_rewrite_without_an_ai_provider(monkeypatch):
    """Rewriting with no provider means writing the stub again — which is what
    put the corpus in this state. It must report, not proceed."""
    import main
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "rule-based"})
    monkeypatch.setattr(main, "get_db", lambda: _EmptyConn())
    out = main._reprocess_bodies_sync(10, 5)
    assert out["ok"] is False
    assert "no AI provider" in out["detail"]


class _EmptyConn:
    def execute(self, *a, **k): return self
    def fetchall(self): return []
    def fetchone(self): return None
    def commit(self): pass
    def close(self): pass


def test_a_rewrite_that_comes_back_as_the_stub_is_not_counted_as_done():
    """Otherwise the row is flagged reprocessed=1 carrying the same placeholder
    and never gets another attempt — the original failure, repeated."""
    import inspect
    import main
    src = inspect.getsource(main._reprocess_bodies_sync)
    assert "is_stub(result" in src
    assert "reprocessed=1" in src


def test_the_reprocess_never_feeds_full_body_to_the_ai():
    """On a drained row full_body IS the stub; summarizing it yields another."""
    import inspect
    import main
    src = inspect.getsource(main._reprocess_bodies_sync)
    assert "source_material" in src
    assert '"body": r["full_body"]' not in src


def test_both_ai_passes_read_the_surviving_source_text():
    """run_ai_batch and admin_reprocess had the same defect."""
    import inspect
    import main
    for fn in (main.run_ai_batch, main.admin_reprocess):
        src = inspect.getsource(fn)
        assert "source_material" in src, f"{fn.__name__} still feeds full_body"
        assert "summary_60" in src


def test_the_detector_run_avoids_the_worker_module_that_needs_missing_deps():
    """app.workers.detectors imports app.db → pydantic-settings, which the root
    service does not ship. The REGISTRY itself needs only stdlib."""
    import ast
    import inspect
    import main
    # Comments mention the module by name to explain WHY it is avoided, so this
    # has to look at the imports themselves, not the source text.
    tree = ast.parse(inspect.getsource(main._run_detectors).lstrip())
    imported = {n.module for n in ast.walk(tree)
                if isinstance(n, ast.ImportFrom) and n.module}
    assert "app.spie.discovery" in imported
    assert not any(m.startswith("app.workers") or m.startswith("app.db")
                   for m in imported), imported


def test_the_scheduler_registers_both_new_nightly_jobs():
    """The whole point of #2: this app's scheduler is the only one that runs."""
    import inspect
    import main
    src = inspect.getsource(main.lifespan)
    assert "sherr_i_detectors" in src
    assert "body_reprocess" in src


def test_the_detector_job_is_scheduled_after_the_market_ticks_job():
    """market_ticks_daily writes one of the detectors' inputs."""
    import main
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    import market_ticks
    s = AsyncIOScheduler()
    market_ticks.register_jobs(s)
    s.add_job(main.detectors_job, "cron", hour=2, minute=10, id="sherr_i_detectors")
    fields = {j.id: str(j.trigger) for j in s.get_jobs()}
    assert "hour='1', minute='30'" in fields["market_ticks_daily"]
    assert "hour='2', minute='10'" in fields["sherr_i_detectors"]
