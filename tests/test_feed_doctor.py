"""
Feed doctor and the pending-rewrite drain.

An empty home feed has four unrelated causes that look identical from the app,
and the deployment has no shell to investigate with. These tests pin the two
things that makes recoverable: that the doctor names the RIGHT cause, and that
the drain is not one accidental URL away from a corpus-wide write.
"""

import importlib
import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

SCHEMA = """
CREATE TABLE articles(
  id INTEGER PRIMARY KEY, headline TEXT, full_body TEXT, source_name TEXT,
  url TEXT, status TEXT, ai_processed INT, pillar_id INT, published_at TEXT,
  originality_json TEXT, image_url TEXT DEFAULT '')
"""

COLS = ("id,headline,full_body,source_name,url,status,ai_processed,"
        "pillar_id,published_at")


def _db(tmp_path, rows):
    p = str(tmp_path / "t.db")
    c = sqlite3.connect(p)
    c.execute(SCHEMA)
    c.executemany(f"INSERT INTO articles({COLS}) VALUES(?,?,?,?,?,?,?,?,?)", rows)
    c.commit()
    c.close()
    return p


def _row(i, status="pending_rewrite", ai=1):
    return (i, f"Story {i} on the economy",
            f"The publisher's own body text for story {i}, at some length.",
            "Reuters", f"https://example.com/{i}", status, ai, 0, "2026-08-19")


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Boot main.py against a throwaway corpus. Imported inside the fixture so
    the env gates (ADMIN_TOKEN, DB_PATH) are read at import, as they are in prod."""
    def _make(rows):
        monkeypatch.setenv("DB_PATH", _db(tmp_path, rows))
        monkeypatch.setenv("ENV", "dev")
        monkeypatch.setenv("ADMIN_TOKEN", "test-token")
        monkeypatch.setenv("JWT_SECRET", "test-secret")
        import main
        importlib.reload(main)
        from fastapi.testclient import TestClient
        return TestClient(main.app)
    return _make


AUTH = {"x-admin-token": "test-token"}


# ─── the doctor names the cause, not just the count ───────────────────────────
def test_pending_rewrite_is_diagnosed_and_the_fix_is_named(client):
    c = client([_row(i) for i in range(1, 6)])
    d = c.get("/admin/feed-doctor", headers=AUTH).json()
    assert d["servable"] == 0 and d["pending_rewrite"] == 5
    assert "pending_rewrite" in d["diagnosis"]
    assert "/admin/publish-pending" in d["fix"]


def test_unprocessed_articles_are_not_blamed_on_the_gate(client):
    """ai_processed=0 and status='published' is a stalled AI cycle, which needs
    /admin/reprocess — republishing would do nothing."""
    c = client([_row(i, status="published", ai=0) for i in range(1, 4)])
    d = c.get("/admin/feed-doctor", headers=AUTH).json()
    assert d["not_ai_processed"] == 3
    assert "reprocess" in d["fix"]


def test_blocked_originality_is_not_offered_a_republish(client):
    """These are verbatim copies. The fix is a rewrite, and the doctor must not
    point at the drain — that would republish the exposure it was built to stop."""
    c = client([_row(i, status="blocked_originality") for i in range(1, 4)])
    d = c.get("/admin/feed-doctor", headers=AUTH).json()
    assert "/admin/publish-pending" not in d["fix"]
    assert "rewrite" in d["fix"]


def test_an_empty_table_is_reported_as_never_collected(client):
    d = client([]).get("/admin/feed-doctor", headers=AUTH).json()
    assert d["total"] == 0 and "collector" in d["fix"]


def test_a_healthy_corpus_says_the_corpus_is_not_the_problem(client):
    c = client([_row(i, status="published") for i in range(1, 4)])
    d = c.get("/admin/feed-doctor", headers=AUTH).json()
    assert d["servable"] == 3
    assert "not empty" in d["diagnosis"]


# ─── the drain does not fire by accident ──────────────────────────────────────
def test_publish_pending_defaults_to_a_dry_run(client):
    """A corpus-wide write must never be the result of hitting a bare URL."""
    c = client([_row(i) for i in range(1, 6)])
    out = c.post("/admin/publish-pending", headers=AUTH).json()
    assert out["dry_run"] is True
    assert out["found"] == 5 and out["published"] == 0
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["pending_rewrite"] == 5


def test_publish_pending_applies_only_when_asked(client):
    c = client([_row(i) for i in range(1, 6)])
    out = c.post("/admin/publish-pending?dry_run=false", headers=AUTH).json()
    assert out["published"] == 5
    d = c.get("/admin/feed-doctor", headers=AUTH).json()
    assert d["pending_rewrite"] == 0 and d["servable"] == 5


def test_the_drain_replaces_the_publishers_body(client, tmp_path):
    """Aggregator posture: the headline stays (with credit), the prose does not.
    Serving the publisher's body is the exposure the whole gate exists to stop."""
    c = client([_row(1)])
    c.post("/admin/publish-pending?dry_run=false", headers=AUTH)
    conn = sqlite3.connect(os.environ["DB_PATH"])
    body = conn.execute("SELECT full_body FROM articles WHERE id=1").fetchone()[0]
    assert "publisher's own body text" not in body
    assert "example.com/1" in body          # outbound link to the source
    conn.close()


def test_the_drain_leaves_blocked_articles_alone(client):
    """blocked_originality failed the body gate. The drain is scoped to
    pending_rewrite and must not sweep those up on its way past."""
    c = client([_row(1), _row(2, status="blocked_originality")])
    out = c.post("/admin/publish-pending?dry_run=false", headers=AUTH).json()
    assert out["published"] == 1
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["blocked_originality"] == 1


def test_force_mode_is_not_reachable_over_http(client):
    """--mode force republishes the publisher's prose verbatim. It stays a
    deliberate command-line act; no query parameter may select it."""
    c = client([_row(1)])
    out = c.post("/admin/publish-pending?dry_run=false&mode=force", headers=AUTH).json()
    assert out["mode"] == "aggregator"


# ─── both endpoints are actually guarded ──────────────────────────────────────
@pytest.mark.parametrize("path", ["/admin/feed-doctor", "/admin/publish-pending"])
@pytest.mark.parametrize("hdr", [{}, {"x-admin-token": "wrong"}])
def test_admin_endpoints_reject_a_missing_or_wrong_token(client, path, hdr):
    c = client([_row(1)])
    call = c.get if path.endswith("doctor") else c.post
    assert call(path, headers=hdr).status_code == 403


def test_production_refuses_to_start_without_an_admin_token(monkeypatch, tmp_path):
    """The token used to default to the string 'sherr-admin', which is in the
    repo — that is the same as having no token on every /admin/* endpoint."""
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("JWT_SECRET", "x" * 40)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "x.db"))
    import main
    with pytest.raises(RuntimeError, match="ADMIN_TOKEN"):
        importlib.reload(main)


# ─── the startup drain ────────────────────────────────────────────────────────
def test_startup_drain_releases_a_stalled_backlog(client):
    """The feed serves only 'published'. When the AI pass stalls the backlog grows
    and the feed empties — which is what a reader sees as an app with no articles
    and no images."""
    import main
    c = client([_row(i) for i in range(1, 40)])
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["servable"] == 0
    main._drain_pending_if_stalled()
    d = c.get("/admin/feed-doctor", headers=AUTH).json()
    assert d["servable"] == 39 and d["pending_rewrite"] == 0


def test_startup_drain_leaves_a_normal_lag_alone(client):
    """A handful of pending rows is the rewrite pass being briefly behind. Draining
    those would race it and publish stubs for articles about to get real bodies."""
    import main
    c = client([_row(i) for i in range(1, 5)])
    out = main._drain_pending_if_stalled()
    assert out["skipped"] == "below threshold"
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["pending_rewrite"] == 4


def test_startup_drain_never_raises(client, monkeypatch):
    """It runs inside lifespan; an exception here must not stop the app booting."""
    import main
    client([_row(1)])
    monkeypatch.setattr(main, "DB_PATH", "/nonexistent/nope.db")
    monkeypatch.setattr(main, "get_db", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert "error" in main._drain_pending_if_stalled()


def test_startup_drain_can_be_disabled(client, monkeypatch):
    import main
    client([_row(i) for i in range(1, 40)])
    monkeypatch.setattr(main, "PENDING_DRAIN_THRESHOLD", 0)
    assert main._drain_pending_if_stalled()["skipped"] == "disabled"


def test_startup_drain_does_not_release_blocked_articles(client):
    """blocked_originality failed the body gate — releasing those recreates the
    copyright exposure the gate exists to prevent."""
    import main
    c = client([_row(i) for i in range(1, 30)]
               + [_row(i, status="blocked_originality") for i in range(30, 34)])
    main._drain_pending_if_stalled()
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["blocked_originality"] == 4
