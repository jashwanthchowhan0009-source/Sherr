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
  originality_json TEXT, image_url TEXT DEFAULT '',
  -- the imagery columns main.py adds by migration; the drain carries the
  -- publisher's image across through them
  source_image_url TEXT DEFAULT '', image_source TEXT DEFAULT '',
  image_credit TEXT DEFAULT '')
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


def test_startup_drain_has_no_floor_by_default(client):
    """The floor used to be 25, on the reasoning that a small backlog is just the
    rewrite pass being briefly behind. With that pass pointed at a decommissioned
    model the floor only decided how long an empty feed stayed empty."""
    import main
    c = client([_row(i) for i in range(1, 5)])
    assert main.PENDING_DRAIN_THRESHOLD == 0
    main._drain_pending_if_stalled()
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["pending_rewrite"] == 0


def test_drain_is_uncapped(client):
    """The ask was to release ALL pending articles, not a page of them. run_sqlite
    is called with limit=None; this pins that against a future default."""
    import main
    c = client([_row(i) for i in range(1, 220)])
    out = main._drain_pending_if_stalled()
    assert out["found"] == 219 and out["published"] == 219
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["servable"] == 219


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
    monkeypatch.setattr(main, "DISABLE_PENDING_DRAIN", True)
    assert main._drain_pending_if_stalled()["skipped"] == "disabled"


def test_startup_drain_does_not_release_blocked_articles(client):
    """blocked_originality failed the body gate — releasing those recreates the
    copyright exposure the gate exists to prevent."""
    import main
    c = client([_row(i) for i in range(1, 30)]
               + [_row(i, status="blocked_originality") for i in range(30, 34)])
    main._drain_pending_if_stalled()
    assert c.get("/admin/feed-doctor", headers=AUTH).json()["blocked_originality"] == 4


# ─── startup must actually start ──────────────────────────────────────────────
def test_create_task_rejects_a_future_which_is_what_broke_boot():
    """asyncio.create_task() takes a coroutine; run_in_executor() returns a Future.
    Handing one to the other raises inside lifespan, so the app never starts —
    pinning the API here because the two calls read as interchangeable."""
    import asyncio

    async def go():
        fut = asyncio.get_event_loop().run_in_executor(None, lambda: None)
        with pytest.raises(TypeError):
            asyncio.create_task(fut)
        await fut
    asyncio.run(go())


def test_lifespan_completes_and_drains(tmp_path, monkeypatch):
    """The real lifespan, start to finish. A TypeError anywhere in it means the
    service boot-loops, which no unit test of the drain alone would catch."""
    import asyncio
    import sqlite3

    db = str(tmp_path / "boot.db")
    monkeypatch.setenv("DB_PATH", db)
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ADMIN_TOKEN", "t")
    monkeypatch.setenv("JWT_SECRET", "s")
    import main
    importlib.reload(main)
    main.init_db()

    conn = main.get_db()
    for i in range(1, 40):
        conn.execute(
            "INSERT INTO articles(headline, full_body, source_name, url, status, "
            "ai_processed, pillar_id, published_at) "
            "VALUES(?,?,?,?,'pending_rewrite',1,0,'2026-08-20')",
            (f"Economy story {i} on markets and policy", f"Body {i}.",
             "Reuters", f"https://example.com/{i}"))
    conn.commit()
    conn.close()

    async def _noop(*a, **k):
        return None
    monkeypatch.setattr(main, "collect_news", _noop)

    async def run():
        async with main.lifespan(main.app):
            await asyncio.sleep(2.0)          # let the drain task land
    asyncio.run(run())

    c = sqlite3.connect(db)
    assert c.execute(
        "SELECT COUNT(*) FROM articles WHERE status='pending_rewrite'").fetchone()[0] == 0
    assert c.execute(
        "SELECT COUNT(*) FROM articles WHERE status='published'").fetchone()[0] == 39
    c.close()


def test_groq_model_is_a_currently_served_id():
    """llama-3.3-70b-versatile is decommissioned; a 404 from Groq means the rewrite
    pass silently stops promoting articles and the feed drains to empty."""
    import ai_processor
    assert ai_processor.GROQ_MODEL == "llama-3.1-8b-instant"


# ─── the drain runs inline at boot, and on demand ─────────────────────────────
def test_drain_runs_inline_during_lifespan_not_as_a_task(tmp_path, monkeypatch):
    """As a background task it was one more thing that could be starved or
    swallowed before it ran. The feed has to be servable by the time the first
    request lands, so it runs inline."""
    import asyncio
    import sqlite3

    db = str(tmp_path / "inline.db")
    for k, v in [("DB_PATH", db), ("ENV", "dev"), ("ADMIN_TOKEN", "t"),
                 ("JWT_SECRET", "s"), ("COLLECT_INTERVAL_MIN", "999")]:
        monkeypatch.setenv(k, v)
    import main
    importlib.reload(main)
    main.init_db()
    conn = main.get_db()
    for i in range(1, 300):
        conn.execute(
            "INSERT INTO articles(headline, full_body, source_name, url, status, "
            "ai_processed, pillar_id, published_at) "
            "VALUES(?,?,?,?,'pending_rewrite',1,0,'2026-08-20')",
            (f"Story {i} on markets and the economy", f"Body {i}.",
             "Reuters", f"https://example.com/{i}"))
    conn.commit()
    conn.close()

    async def _noop(*a, **k):
        return None
    monkeypatch.setattr(main, "collect_news", _noop)

    async def run():
        async with main.lifespan(main.app):
            pass                      # no sleep: inline means it is already done
    asyncio.run(run())

    c = sqlite3.connect(db)
    assert c.execute(
        "SELECT COUNT(*) FROM articles WHERE status='pending_rewrite'").fetchone()[0] == 0
    assert c.execute(
        "SELECT COUNT(*) FROM articles WHERE status='published'").fetchone()[0] == 299
    c.close()


def test_flush_pending_needs_no_token(client):
    """Deliberately open so it can be hit from a phone browser while the feed is
    being brought back up."""
    c = client([_row(i) for i in range(1, 30)])
    r = c.get("/admin/flush-pending")
    assert r.status_code == 200
    assert r.json()["published"] == 29


def test_flush_pending_reports_the_resulting_corpus_state(client):
    """One call has to answer both 'did it run' and 'is the feed servable now' —
    the second being the question actually being asked."""
    c = client([_row(i) for i in range(1, 30)])
    now = c.get("/admin/flush-pending").json()["now"]
    assert now["servable"] == 29 and now["pending_rewrite"] == 0
    assert set(now) >= {"servable", "pending_rewrite", "blocked_originality",
                        "not_ai_processed", "total"}


def test_flush_pending_is_a_no_op_once_drained(client):
    """A GET that mutates will be re-fired by crawlers and link previews. The
    second call must do nothing rather than churn the corpus."""
    c = client([_row(i) for i in range(1, 30)])
    c.get("/admin/flush-pending")
    second = c.get("/admin/flush-pending").json()
    assert second["published"] == 0 and second["skipped"] == "nothing pending"
    assert second["now"]["servable"] == 29


def test_flush_pending_will_not_release_blocked_articles(client):
    """The open endpoint must not be a way to publish rows that failed the body
    gate — that is the one exclusion the drain does not negotiate."""
    c = client([_row(i) for i in range(1, 20)]
               + [_row(i, status="blocked_originality") for i in range(20, 25)])
    out = c.get("/admin/flush-pending").json()
    assert out["published"] == 19
    assert out["now"]["blocked_originality"] == 5


# ─── the publisher's image survives the drain ─────────────────────────────────
def _img_row(i, image_url="", source_image_url="https://cdn.example/p.jpg"):
    return (i, f"Story {i} on the economy", f"Body {i}.", "Reuters",
            f"https://example.com/{i}", "pending_rewrite", 1, 0, "2026-08-19",
            image_url, source_image_url, "", "")


IMG_COLS = (COLS + ",image_url,source_image_url,image_source,image_credit")


def _img_db(tmp_path, rows):
    p = str(tmp_path / "img.db")
    c = sqlite3.connect(p)
    c.execute(SCHEMA)
    c.executemany(f"INSERT INTO articles({IMG_COLS}) "
                  f"VALUES({','.join('?' * 13)})", rows)
    c.commit()
    c.close()
    return p


def test_drain_carries_the_publishers_image_into_the_published_row(tmp_path, monkeypatch):
    """image_url is written as '' at ingest and the publisher's image lands in
    source_image_url. Serve-time resolution copies one to the other, but only
    while IMAGE_MODE == 'thumbnail' — so the published row has to carry it
    itself, or it renders as generated art the moment that flips."""
    db = _img_db(tmp_path, [_img_row(1)])
    from publish_pending import run_sqlite
    run_sqlite(db, mode="aggregator", dry_run=False, limit=None)
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    r = c.execute("SELECT * FROM articles WHERE id=1").fetchone()
    assert r["image_url"] == "https://cdn.example/p.jpg"
    assert r["image_source"] == "thumbnail"
    assert "Reuters" in r["image_credit"]
    c.close()


def test_drain_never_blanks_an_image_the_row_already_had(tmp_path):
    """Our own hosted image outranks the publisher's."""
    db = _img_db(tmp_path, [_img_row(1, image_url="https://sherrbyte.test/own.jpg")])
    from publish_pending import run_sqlite
    run_sqlite(db, mode="aggregator", dry_run=False, limit=None)
    c = sqlite3.connect(db)
    got = c.execute("SELECT image_url FROM articles WHERE id=1").fetchone()[0]
    assert got == "https://sherrbyte.test/own.jpg"
    c.close()


def test_a_row_with_no_image_publishes_without_inventing_one(tmp_path):
    db = _img_db(tmp_path, [_img_row(1, source_image_url="")])
    from publish_pending import run_sqlite
    run_sqlite(db, mode="aggregator", dry_run=False, limit=None)
    c = sqlite3.connect(db)
    r = c.execute("SELECT image_url, image_source, status FROM articles "
                  "WHERE id=1").fetchone()
    assert r[0] == "" and r[1] == "" and r[2] == "published"
    c.close()


def test_the_drain_still_runs_on_a_schema_without_the_image_columns(tmp_path):
    """An emergency drain must not die because one schema is older than another.
    Missing imagery columns cost the images, not the drain."""
    p = str(tmp_path / "old.db")
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, headline TEXT, "
              "full_body TEXT, source_name TEXT, url TEXT, status TEXT, "
              "ai_processed INT, pillar_id INT, published_at TEXT, "
              "originality_json TEXT)")
    c.execute("INSERT INTO articles VALUES(1,'Economy story','Body.','Reuters',"
              "'https://e/1','pending_rewrite',1,0,'2026-08-19',NULL)")
    c.commit()
    c.close()
    from publish_pending import run_sqlite
    out = run_sqlite(p, mode="aggregator", dry_run=False, limit=None)
    assert out["published"] == 1


# ─── backend selection ────────────────────────────────────────────────────────
def test_postgres_is_used_when_database_url_is_a_postgres_url(monkeypatch):
    """Render's free tier destroys the sqlite file on every deploy, so the whole
    point is that this flips without touching any call site."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:6543/db?pgbouncer=true")
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ADMIN_TOKEN", "t")
    monkeypatch.setenv("JWT_SECRET", "s")
    import main
    importlib.reload(main)
    assert main.USE_POSTGRES is True


def test_sqlite_remains_the_backend_for_local_development(monkeypatch, tmp_path):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SHERR_I_DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "x.db"))
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ADMIN_TOKEN", "t")
    monkeypatch.setenv("JWT_SECRET", "s")
    import main
    importlib.reload(main)
    assert main.USE_POSTGRES is False
