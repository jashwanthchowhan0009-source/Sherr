"""Feed preferences: what the reader picks has to be what the feed ranks.

The screen looked static because, server-side, half of it was. These tests pin
the two defects that made it so, and the catalogue endpoint that replaces the
hardcoded client-side lists.
"""
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import cache  # noqa: E402
import main   # noqa: E402


@pytest.fixture(autouse=True)
def _empty_cache():
    cache._local.clear()
    yield
    cache._local.clear()


@pytest.fixture
def client():
    return TestClient(main.app)


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "prefs.db")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

    def _get_db():
        c = sqlite3.connect(path)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(main, "get_db", _get_db)
    # The save path recomputes the reader's ranked rows in a thread; that is
    # not what these tests are about, so it is stubbed to keep them pure.
    monkeypatch.setattr(main, "compute_feed_for_user", lambda uid: None)
    return _get_db


def _saved(get_db, uid=1):
    conn = get_db()
    try:
        return {r["topic_name"]: r["pillar_id"] for r in conn.execute(
            "SELECT topic_name, pillar_id FROM user_preferences WHERE user_id=?",
            (uid,)).fetchall()}
    finally:
        conn.close()


def test_categories_are_persisted_not_discarded(db, client):
    """UpdateTopicsReq has always carried `categories`; the handler only read
    `topics`, so every pillar the reader switched on was dropped at the door —
    and compute_feed_for_user weights pillar_score at 2x, so the category half
    of the screen ranked nothing at all."""
    r = client.put("/me/topics", json={"categories": ["tech", "sports"],
                                       "topics": ["Cricket"]})
    assert r.status_code == 200
    assert r.json()["categories_saved"] == 2
    saved = _saved(db)
    assert saved["tech"] == 3
    assert saved["sports"] == 9
    assert saved["Cricket"] == 9


def test_an_unknown_topic_is_no_longer_filed_as_society(db, client):
    """`MICRO_TOPICS.get(topic, 1)` filed every topic the map had never heard
    of under pillar 1. The screen offers about twenty of those, so a reader who
    picked only markets and engineering got a feed tilted at politics."""
    client.put("/me/topics", json={
        "categories": [], "topics": ["Zxqv Nonexistent Topic"]})
    assert _saved(db)["Zxqv Nonexistent Topic"] == main.PILLAR_UNSCOPED
    assert main.PILLAR_UNSCOPED not in main.PILLARS   # so it biases no pillar


def test_a_topic_the_map_knows_under_another_spelling_still_resolves(db, client):
    client.put("/me/topics", json={"categories": [], "topics": ["cricket"]})
    assert _saved(db)["cricket"] == 9          # case-insensitive
    assert main.resolve_topic_pillar("Future Tech") == 3   # containment match
    assert main.resolve_topic_pillar("") == main.PILLAR_UNSCOPED


def test_a_bogus_category_slug_is_ignored_rather_than_stored(db, client):
    r = client.put("/me/topics", json={"categories": ["not-a-pillar"], "topics": []})
    assert r.json()["categories_saved"] == 0
    assert _saved(db) == {}


def test_reading_back_splits_categories_from_topics(db, client):
    """The client had to infer the split from /me's one flat list, and inferred
    it wrongly: every topic carries a pillar_id, so each one switched its whole
    category on too. The reader came back to categories they never chose."""
    client.put("/me/topics", json={"categories": ["tech"], "topics": ["AI", "Space"]})
    got = client.get("/me/topics").json()
    assert got["categories"] == ["tech"]
    assert sorted(got["topics"]) == ["AI", "Space"]


def test_saving_drops_the_readers_cached_feed_pages(db, client, monkeypatch):
    """/feed is cached per (reader, page, filter) for FEED_CACHE_SECONDS. Left
    in place, a save is correct and invisible — which is the whole complaint."""
    import asyncio
    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
        cache.set("feed:1:1:20::0", {"articles": ["stale"]}, 60))
    assert cache._local_get("feed:1:1:20::0") is not None
    client.put("/me/topics", json={"categories": ["tech"], "topics": []})
    assert cache._local_get("feed:1:1:20::0") is None


def test_the_topic_catalogue_is_served_with_live_counts(db, client):
    """The list lived in index.html twice, in two arrays that had already
    drifted apart from each other and from MICRO_TOPICS. One served list, with
    counts over the corpus, is what makes the screen dynamic."""
    conn = db()
    for i, tags in enumerate(['["AI","Space"]', '["AI"]', '["AI"]']):
        conn.execute(
            "INSERT INTO articles (id, url, headline, status, ai_processed, "
            "micro_tags, published_at) VALUES (?,?,?,?,?,?,?)",
            (100 + i, f"u{i}", f"Story {i}", "published", 1, tags,
             "2026-09-01T10:00:00+00:00"))
    conn.commit()
    conn.close()
    r = client.get("/topics/catalogue").json()
    by_name = {t["name"]: t for t in r["topics"]}
    assert by_name["AI"]["count"] == 3
    assert by_name["AI"]["pillar"] == "tech"
    assert by_name["Space"]["count"] == 1
    # Sorted most-published first, so the screen leads with what is live.
    assert r["topics"][0]["name"] == "AI"
    # A topic nobody has published is still offered — the corpus moves.
    assert by_name["Wimbledon"]["count"] == 0
    assert len(r["pillars"]) == 9


def test_the_catalogue_survives_a_database_failure(db, client, monkeypatch):
    """An empty preferences screen is indistinguishable from a broken one, and
    this database has been over its quota before."""
    def _boom():
        raise RuntimeError("402 over quota")
    monkeypatch.setattr(main, "get_db", _boom)
    r = client.get("/topics/catalogue")
    assert r.status_code == 200
    assert len(r.json()["topics"]) > 50        # the full map, with zero counts
