"""Real URLs, link previews, and the read cache that stands in front of Postgres.

Two problems, one section of main.py:

  * THE APP HAD ONE URL. Every screen lived behind a JS view switch, so a reader
    could not link to a story, a refresh lost their place, and a shared link
    previewed as the generic app card because unfurlers do not run JavaScript.
  * THE FEED WENT TO POSTGRES ONCE PER READER. Supabase's pooler allows
    connections in the low tens; that does not degrade under a crowd, it
    exhausts and every request fails at once, including the admin endpoints you
    would use to find out why.

The catch-all that serves the SPA is the dangerous part of the first fix: it
matches everything, so if it is registered too early — or forgets to refuse the
API prefixes — /feed and /admin/* start returning 560KB of HTML with a 200, and
every client reports a JSON parse error instead of a 404.
"""
import asyncio
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import cache  # noqa: E402
import main  # noqa: E402


@pytest.fixture(autouse=True)
def _empty_cache():
    """RESET, never save-and-restore: a payload left by a previous test would
    be served to this one and the assertion would measure that instead."""
    cache._local.clear()
    yield
    cache._local.clear()


@pytest.fixture
def client():
    return TestClient(main.app)


# ─── the catch-all must not shadow the API ──────────────────────────────────

def test_the_catchall_is_the_last_route_registered():
    """ORDER IS THE MECHANISM. FastAPI matches in registration order, so a
    catch-all declared anywhere above /feed swallows it."""
    paths = [getattr(r, "path", "") for r in main.app.routes]
    assert paths[-1] == "/{full_path:path}", \
        f"something is registered after the catch-all: {paths[-1]}"


def test_api_and_admin_paths_are_refused_by_the_catchall(client):
    """The second belt. Ordering protects today's routes; this protects the one
    somebody adds below the catch-all by accident."""
    for path in ("/api/does-not-exist", "/admin/does-not-exist",
                 "/api/sherr-i/nope"):
        r = client.get(path)
        assert r.status_code == 404, f"{path} returned {r.status_code}"
        assert "text/html" not in r.headers.get("content-type", "")


def test_the_real_api_routes_still_answer(client):
    """The routes a crowd and an operator actually use, proven to survive."""
    assert client.get("/health").status_code == 200
    assert client.get("/patterns").status_code == 200
    assert client.get("/api/sherr-i/analogs").status_code == 200
    # /admin refuses on the TOKEN, which proves it reached the handler rather
    # than being served index.html with a 200.
    assert client.get("/admin/body-audit").status_code in (401, 403)


# ─── the SPA paths ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", ["/", "/explore", "/bytes", "/profile",
                                  "/feed", "/search", "/bookmarks"])
def test_each_app_path_serves_the_app(client, path):
    """Real URLs need the SERVER to answer them; the History API alone only
    works after a tap, and breaks on refresh and on a pasted link.

    The browser Accept header is what four of these paths use to tell a person
    navigating from a program calling the API on the same URL."""
    r = client.get(path, headers={"Accept": "text/html,application/xhtml+xml"})
    assert r.status_code == 200
    assert "<html" in r.text.lower() or "sherr" in r.text.lower()


@pytest.mark.parametrize("path", ["/feed", "/explore", "/search", "/bookmarks"])
def test_the_shared_paths_still_answer_the_app_with_json(client, path):
    """THE HALF THAT WOULD BREAK EVERY CLIENT IN THE WILD. api() in index.html
    sets no Accept header, so fetch() sends `*/*` — which must never match
    text/html, or every call in the app receives 560KB of HTML instead of an
    object and reports a JSON parse error."""
    r = client.get(path, headers={"Accept": "*/*"})
    assert r.status_code == 200
    assert "application/json" in r.headers.get("content-type", "")


def test_an_unknown_path_is_a_404_not_the_app(client):
    """A catch-all that answers everything makes every typo look like a
    working page, and hides real routing mistakes."""
    assert client.get("/not-a-real-screen").status_code == 404


# ─── slugs ──────────────────────────────────────────────────────────────────

def test_the_slug_resolves_by_id_not_by_words():
    """A headline rewritten by a later synthesis pass changes the slug. Every
    link already shared has to keep working, so the id is what resolves."""
    slug = main.article_slug(4321, "Crude climbs as OPEC+ weighs deeper cuts")
    assert slug.endswith("-4321")
    assert main.article_id_from_slug(slug) == 4321
    assert main.article_id_from_slug("completely-different-words-4321") == 4321
    assert main.article_id_from_slug("no-id-here") is None


def test_a_headline_with_no_usable_words_still_slugs():
    assert main.article_id_from_slug(main.article_slug(7, "!!! ???")) == 7


# ─── link previews ──────────────────────────────────────────────────────────

def _seed_article(tmp_path, monkeypatch):
    db = str(tmp_path / "spa.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    conn.execute(
        "INSERT INTO articles (id, url, headline, summary_60, full_body,"
        " image_url, status, ai_processed, pillar_id, published_at)"
        " VALUES (11,'u','Crude climbs as OPEC+ weighs deeper cuts',"
        "'Benchmark crude settled higher on Monday.','body',"
        "'https://img.example/a.jpg','published',1,2,'2026-09-01T10:00:00+00:00')")
    conn.commit()
    conn.close()

    def _get_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(main, "get_db", _get_db)


def test_a_shared_story_carries_its_own_og_tags(tmp_path, monkeypatch, client):
    """UNFURLERS DO NOT RUN JAVASCRIPT. WhatsApp, Slack, iMessage and X read the
    HTML as delivered, so tags the SPA adds after load are invisible to every
    one of them — which is why a shared story has always previewed as the
    generic app card. These have to be in the bytes on the wire."""
    _seed_article(tmp_path, monkeypatch)
    body = client.get("/bytes/crude-climbs-as-opec-weighs-deeper-cuts-11").text
    assert 'property="og:title" content="Crude climbs as OPEC+ weighs deeper cuts' in body
    assert 'Benchmark crude settled higher on Monday.' in body
    assert 'property="og:image" content="https://img.example/a.jpg"' in body
    assert '<link rel="canonical"' in body


def test_the_article_tags_come_before_the_apps_own(tmp_path, monkeypatch, client):
    """A scraper takes the FIRST value it finds for a property. index.html
    already carries a generic og:title, so appending would produce a page whose
    per-article tags are silently ignored."""
    _seed_article(tmp_path, monkeypatch)
    body = client.get("/bytes/x-11").text
    ours = body.index('content="Crude climbs as OPEC+ weighs deeper cuts')
    head = body.index("<head>")
    assert head < ours < body.index("</head>")


def test_an_unknown_story_gets_the_generic_preview_not_another_articles(
        tmp_path, monkeypatch, client):
    _seed_article(tmp_path, monkeypatch)
    body = client.get("/bytes/deleted-story-999999").text
    assert "Crude climbs" not in body
    assert 'property="og:title"' in body


# ─── crawling ───────────────────────────────────────────────────────────────

def test_robots_points_at_the_sitemap_and_keeps_crawlers_out_of_the_api(client):
    body = client.get("/robots.txt").text
    assert "Sitemap: " in body and "/sitemap.xml" in body
    assert "Disallow: /admin" in body and "Disallow: /api/" in body


def test_the_sitemap_lists_the_screens_and_the_articles(tmp_path, monkeypatch,
                                                        client):
    _seed_article(tmp_path, monkeypatch)
    r = client.get("/sitemap.xml")
    assert r.status_code == 200
    assert "xml" in r.headers["content-type"]
    assert f"{main.SITE_URL}/explore" in r.text
    assert "/bytes/crude-climbs-as-opec-weighs-deeper-cuts-11" in r.text


# ─── the read cache ─────────────────────────────────────────────────────────

def test_a_second_reader_does_not_reach_the_database():
    """THE WHOLE POINT. At a 30-second TTL a thousand readers a minute cost two
    queries instead of a thousand."""
    calls = []

    async def go():
        for _ in range(50):
            await cache.get_or_set("k", 30, lambda: calls.append(1) or {"a": 1})

    asyncio.run(go())
    assert len(calls) == 1, f"the producer ran {len(calls)} times, expected 1"


def test_the_cache_survives_redis_being_absent():
    """The local layer is not a nicety. Redis going down is EXACTLY the moment
    a stampede would arrive, and a cache whose only layer is remote fails open
    onto the database it was protecting."""
    assert cache.REDIS_URL == "" or cache.STATS["redis"] != "connected"
    calls = []

    async def go():
        for _ in range(10):
            await cache.get_or_set("nored", 30, lambda: calls.append(1) or [1, 2])

    asyncio.run(go())
    assert len(calls) == 1


def test_an_expired_entry_is_refetched():
    calls = []

    async def go():
        await cache.get_or_set("t", 30, lambda: calls.append(1) or {"v": 1})
        # Expire it by hand rather than sleeping — a real sleep in a unit test
        # is a slow test that still cannot prove anything a clock cannot.
        cache._local["t"] = (0.0, {"v": 1})
        await cache.get_or_set("t", 30, lambda: calls.append(1) or {"v": 2})

    asyncio.run(go())
    assert len(calls) == 2


def test_a_producer_failure_is_not_cached():
    """Storing an error under a 30-second TTL turns one bad query into thirty
    seconds of guaranteed-wrong answers for every reader."""
    def boom():
        raise RuntimeError("db down")

    async def go():
        with pytest.raises(RuntimeError):
            await cache.get_or_set("bad", 30, boom)
        return await cache.get("bad")

    assert asyncio.run(go()) is None


def test_the_local_layer_is_bounded():
    """An unbounded cache on a 512MB free instance is a memory leak with a TTL."""
    async def go():
        for i in range(cache._LOCAL_MAX + 50):
            await cache.set(f"k{i}", {"i": i}, 60)

    asyncio.run(go())
    assert len(cache._local) <= cache._LOCAL_MAX


def test_an_unavailable_answer_is_never_cached(client, monkeypatch):
    """`unavailable` is a transient reachability condition. Caching it would
    keep reporting the outage for a minute after Postgres came back."""
    async def none_pool():
        return None

    monkeypatch.setattr(main, "get_spie_pool", none_pool)
    assert client.get("/api/sherr-i/analogs").json()["source"] == "unavailable"
    assert asyncio.run(cache.get("analogs::0:25:0")) is None


# ─── the connection ceiling ─────────────────────────────────────────────────

def test_the_database_pools_are_capped():
    """An uncapped pool does not degrade under a crowd — it takes every slot,
    and the scheduler, the admin endpoints and the engine's own pool all fail
    to connect at once, which looks like the database is down."""
    import pgcompat
    assert 1 <= pgcompat.POOL_MAX <= 16
    src = open(main.__file__).read()
    assert 'max_size=int(os.getenv("SPIE_POOL_MAX", "4"))' in src
    assert "min_size=1, max_size=POOL_MAX" in open(pgcompat.__file__).read()
