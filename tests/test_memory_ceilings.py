"""Every pass that scales with the corpus has a ceiling, on a 512MB instance.

WHY THIS FILE EXISTS. sherrbyte-api on Render's free plan is a hard 512MB, and
on 2026-10-16/17 it was killed repeatedly — "Ran out of memory (used over
512MB) while running your code", followed by "Service recovered", followed by
the same thing two hours later.

Nothing had been deployed that allocated more. The CORPUS had grown. Three
reads had no bound other than how much news there happened to be:

  * link_stories clustered every published row inside a 45-day window at the
    end of every collection cycle. Measured on a news-shaped corpus,
    synthesis.cluster_articles alone peaks at ~45MB over 6,000 rows, ~95MB over
    12,000 and ~190MB over 25,000 — the pair counter is superlinear in the row
    count — before the fetched rows themselves and on top of a resident app.
  * _live_thread_ids scanned a pillar's slice of the same window on a READER'S
    request, so the cost multiplied by however many people were scrolling.
  * /admin/rescope selected the whole corpus WITH its bodies in one list.

And two fetches checked their size limit only AFTER buffering the whole
response, which is a check that cannot fire in the case it exists for.

These assert the bounds, not the measurements: a cap that is only a constant
nobody reads is how this comes back.
"""
import asyncio
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import article_reader  # noqa: E402
import main  # noqa: E402


# ── the story-thread pass ────────────────────────────────────────────────────

def _corpus(tmp_path, n: int):
    """n published, AI-processed rows, all today so the window holds them."""
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for stmt in main._MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass
    for i in range(n):
        conn.execute(
            "INSERT INTO articles (url, headline, micro_tags, pillar_id, "
            "ai_processed, status, published_at) "
            "VALUES (?,?,?,?,1,'published',datetime('now'))",
            (f"http://x/{i}", f"Reserve bank holds the repo rate steady {i}",
             '["markets"]', 2))
    conn.commit()
    return conn


def _spy_on_clustering(monkeypatch) -> dict:
    """Record the ids actually handed to the clusterer, and cluster them."""
    seen: dict = {}
    real = main.synthesis.cluster_articles

    def _spy(rows, **kw):
        seen["ids"] = sorted(r["id"] for r in rows)
        return real(rows, **kw)

    monkeypatch.setattr(main.synthesis, "cluster_articles", _spy)
    return seen


def _source(fn) -> str:
    import inspect
    return inspect.getsource(fn)


def test_link_stories_never_clusters_more_than_its_cap(tmp_path, monkeypatch):
    """The window says which rows are ELIGIBLE; the cap says how many are
    resident. Without the second one the pass costs whatever the month ingested."""
    conn = _corpus(tmp_path, 40)
    seen = _spy_on_clustering(monkeypatch)
    monkeypatch.setattr(main, "STORY_LINK_MAX_ROWS", 10)
    main.link_stories(conn)
    conn.close()
    assert len(seen["ids"]) == 10, "the cap did not reach the query"


def test_the_cap_keeps_the_newest_rows(tmp_path, monkeypatch):
    """Dropping the oldest is the whole point: a 40-day-old article is not
    about to join a thread forming today."""
    conn = _corpus(tmp_path, 30)
    seen = _spy_on_clustering(monkeypatch)
    monkeypatch.setattr(main, "STORY_LINK_MAX_ROWS", 5)
    main.link_stories(conn)
    conn.close()
    assert seen["ids"] == [26, 27, 28, 29, 30], seen["ids"]


def test_the_cap_is_read_at_call_time():
    """Bound as a default argument it would freeze at import, and lowering the
    ceiling on a thrashing instance would need a deploy. This repo has been
    burnt by import-time configuration before (see CLAUDE.md on reload)."""
    import inspect
    sig = inspect.signature(main.link_stories)
    assert sig.parameters["max_rows"].default is None


def test_the_live_thread_scan_is_limited():
    """It runs per reader request, so an uncapped scan multiplies by traffic."""
    src = _source(main._live_thread_ids)
    assert "LIMIT ?" in src, "the candidate scan has no LIMIT"
    assert "LIVE_THREAD_MAX_ROWS" in src


def test_rescope_pages_rather_than_materialising_every_body():
    """`SELECT id, headline, summary_60, full_body FROM articles` with no LIMIT
    is the whole corpus with its bodies — one admin tap, one dead instance."""
    src = _source(main.admin_rescope)
    assert "RESCOPE_BATCH" in src
    assert "LIMIT ?" in src


def test_every_corpus_scaled_cap_has_a_sane_default():
    for name, lo, hi in (("STORY_LINK_MAX_ROWS", 100, 20000),
                         ("LIVE_THREAD_MAX_ROWS", 100, 20000),
                         ("RESCOPE_BATCH", 50, 5000),
                         ("SYNTHESIS_POOL", 50, 5000),
                         ("SIG_BACKFILL_BATCH", 50, 5000)):
        v = getattr(main, name)
        assert lo <= v <= hi, f"{name}={v} is outside a 512MB instance's range"


# ── the two fetches that buffered first and checked afterwards ───────────────

class _FakeStream:
    """Minimal stand-in for an httpx streaming response."""

    def __init__(self, chunks, headers=None, status=200):
        self._chunks = chunks
        self.headers = headers or {"content-type": "text/html"}
        self.status_code = status
        self.encoding = "utf-8"
        self.delivered = 0

    async def aiter_bytes(self):
        for c in self._chunks:
            self.delivered += len(c)
            yield c

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeClient:
    def __init__(self, response):
        self._response = response

    def stream(self, *a, **kw):
        return self._response

    async def aclose(self):
        pass


def test_the_reader_abandons_an_oversized_page_mid_download(monkeypatch):
    """The cap is enforced WHILE the body streams. The old code called
    `client.get` and then measured `r.content`, by which point the whole thing
    — 300MB, say, or a video behind a URL that used to be an article — was
    already resident, six at a time under fetch_many's semaphore."""
    chunk = b"x" * 100_000
    over = article_reader.MAX_BYTES // len(chunk) + 5
    resp = _FakeStream([chunk] * over)
    text = asyncio.run(article_reader.fetch_article(
        "https://example.com/a", _FakeClient(resp)))
    assert text == ""
    assert resp.delivered <= article_reader.MAX_BYTES + len(chunk), (
        "the whole body was downloaded before the cap was applied")


def test_the_reader_still_reads_a_normal_page():
    """A cap that also rejects the ordinary case is not a fix."""
    para = ("<p>" + " ".join(["Officials said the measure would take effect "
                              "on Monday."] * 30) + "</p>")
    resp = _FakeStream([("<html><body>" + para * 4 + "</body></html>").encode()])
    text = asyncio.run(article_reader.fetch_article(
        "https://example.com/a", _FakeClient(resp)))
    assert "Officials said" in text


def test_the_reader_skips_a_body_whose_declared_length_is_over_the_cap():
    """Cheapest possible refusal: no body is read at all."""
    resp = _FakeStream([b"x" * 10], headers={
        "content-type": "text/html",
        "content-length": str(article_reader.MAX_BYTES + 1)})
    text = asyncio.run(article_reader.fetch_article(
        "https://example.com/a", _FakeClient(resp)))
    assert text == ""
    assert resp.delivered == 0


def test_a_feed_over_the_cap_is_skipped_not_buffered():
    """RSS_CONCURRENCY feeds are in flight at once, so one enormous response is
    not one enormous response."""
    chunk = b"y" * 100_000
    over = main.RSS_MAX_BYTES // len(chunk) + 5
    resp = _FakeStream([chunk] * over)
    status, text = asyncio.run(
        main._get_capped(_FakeClient(resp), "http://f", "Feed"))
    assert status == 200 and text == ""
    assert resp.delivered <= main.RSS_MAX_BYTES + len(chunk)


def test_a_normal_feed_is_returned_whole():
    body = b"<rss><channel><title>t</title></channel></rss>"
    resp = _FakeStream([body])
    status, text = asyncio.run(
        main._get_capped(_FakeClient(resp), "http://f", "Feed"))
    assert status == 200
    assert text == body.decode()


def test_a_non_200_feed_returns_no_body():
    resp = _FakeStream([b"nope"], status=503)
    status, text = asyncio.run(
        main._get_capped(_FakeClient(resp), "http://f", "Feed"))
    assert status == 503 and text == ""


# ── the endpoint you reach for when the dashboard says 512MB ─────────────────

def _client():
    from fastapi.testclient import TestClient
    main.ADMIN_TOKEN = "test-token"
    main.collect_news = lambda: asyncio.sleep(0)
    return TestClient(main.app)


def test_admin_memory_is_guarded():
    with _client() as c:
        assert c.get("/admin/memory").status_code == 403


def test_admin_memory_reports_the_number_and_the_caps(monkeypatch):
    """It must answer while the instance is thrashing, so it touches NO
    database — the moment you want it is the moment a query would time out."""
    with _client() as c:
        # Patched only around the REQUEST: the app's own lifespan runs init_db,
        # which is not what this is asserting.
        monkeypatch.setattr(main, "get_db", _no_db)
        out = c.get("/admin/memory?token=test-token").json()
        monkeypatch.undo()
    assert out["limit_mb"] == 512
    assert out["caps"]["STORY_LINK_MAX_ROWS"] == main.STORY_LINK_MAX_ROWS
    assert "cache_local_keys" in out["holding"]
    if out["rss_mb"] is not None:                      # no procfs on some CI
        assert out["rss_mb"] > 0
        assert out["used_pct"] is not None


def _no_db(*a, **k):
    raise AssertionError("/admin/memory must not touch the database")
