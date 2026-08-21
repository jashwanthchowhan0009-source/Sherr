"""
The four findings from the final pass, plus the two the verification turned up.

Each of these was invisible from the outside: a cumulative counter that always
read "success", routes the frontend had always called and the backend never had,
a referrer header that turned every publisher image into a gradient, and a dead
session reported as a missing page.
"""

import importlib
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


@pytest.fixture
def app(tmp_path, monkeypatch):
    for k, v in [("DB_PATH", str(tmp_path / "f.db")), ("ENV", "dev"),
                 ("ADMIN_TOKEN", "t"), ("JWT_SECRET", "s"),
                 ("COLLECT_INTERVAL_MIN", "999"), ("IMAGE_MODE", "thumbnail")]:
        monkeypatch.setenv(k, v)
    import main
    importlib.reload(main)
    main.init_db()
    from fastapi.testclient import TestClient
    return main, TestClient(main.app)


def _article(i):
    return {"url": f"https://pub.example/{i}", "title_hash": f"h{i}",
            "headline": f"Story {i} on the economy", "summary_60": "s",
            "full_body": "Publisher body.", "source_summary": "s",
            "when_info": "", "where_info": "", "what_info": "", "how_info": "",
            "image_url": "", "source_image_url": f"https://cdn.pub/{i}.jpg",
            "source_name": "Reuters", "pillar_id": 2, "micro_tags": "[]",
            "scope": "global", "published_at": "2026-08-21T10:00:00"}


# ─── 4. the insert counter was cumulative ────────────────────────────────────
def test_a_duplicate_insert_is_not_counted_as_new(app):
    """conn.total_changes is CUMULATIVE for the connection, so once one row had
    ever landed it stayed > 0 and every later call reported success — including
    the ones ON CONFLICT skipped. "[CYCLE] inserted=N" counted the whole batch
    every cycle no matter what was written."""
    main, _ = app
    conn = main.get_db()
    batch = [_article(i) for i in range(1, 6)]
    assert sum(1 for a in batch if main._insert_with_dedup(conn, a)) == 5
    assert sum(1 for a in batch if main._insert_with_dedup(conn, a)) == 0
    conn.close()


def test_a_later_batch_counts_only_its_new_rows(app):
    main, _ = app
    conn = main.get_db()
    for a in (_article(i) for i in range(1, 6)):
        main._insert_with_dedup(conn, a)
    mixed = [_article(i) for i in range(4, 9)]          # 4,5 dupes; 6,7,8 new
    assert sum(1 for a in mixed if main._insert_with_dedup(conn, a)) == 3
    conn.close()


def test_the_counter_reads_rowcount_not_total_changes(app):
    import inspect
    main, _ = app
    src = inspect.getsource(main._insert_with_dedup)
    assert "cur.rowcount" in src
    assert "return conn.total_changes" not in src


# ─── 3. the /live routes the frontend has always called ─────────────────────
@pytest.mark.parametrize("path", [
    "/live/weather", "/live/word-of-day", "/live/dictionary/lucid"])
def test_the_live_routes_exist(app, path):
    """They 404'd, so the weather tile, the Word of the Day card and the
    dictionary lookup each sat on their loading state forever."""
    main, _ = app
    assert any(getattr(r, "path", "") == path.split("?")[0]
               or getattr(r, "path", "").startswith("/live/dictionary")
               for r in main.app.routes), f"{path} is not registered"


def test_an_upstream_outage_is_503_not_an_unhandled_error(app):
    """This environment cannot reach the upstreams, which makes it a good test of
    the failure path: a transport error must become a clean status, not a stack
    trace, or the caller cannot tell an outage from a bug."""
    _, cl = app
    for p in ("/live/weather", "/live/word-of-day", "/live/dictionary/lucid"):
        assert cl.get(p).status_code in (200, 503), p


def test_a_non_word_is_rejected_before_the_upstream(app):
    _, cl = app
    assert cl.get("/live/dictionary/123").status_code == 400


def test_the_dictionary_shape_matches_what_the_card_reads(app):
    """The card does meanings[0].part_of_speech; the upstream nests definitions a
    level deeper. Returning the fetcher's flat shape would render blank."""
    main, _ = app
    out = main._dict_payload("lucid", {
        "word": "lucid", "phonetic": "/ˈluːsɪd/",
        "meanings": [{"partOfSpeech": "adjective",
                      "definitions": [{"definition": "clear", "example": "a lucid note"}]}]})
    assert out["meanings"][0]["part_of_speech"] == "adjective"
    assert out["meanings"][0]["definition"] == "clear"


def test_the_phonetic_falls_back_to_the_phonetics_list(app):
    """Half of dictionaryapi's entries leave the top-level phonetic empty."""
    main, _ = app
    out = main._dict_payload("x", {"phonetics": [{}, {"text": "/eks/"}], "meanings": []})
    assert out["phonetic"] == "/eks/"


# ─── 1. imagery: referrer policy, everywhere ────────────────────────────────
def test_every_img_tag_carries_the_referrer_policy():
    """Publisher CDNs refuse hotlinked requests by Referer; the tag then fires
    onerror and falls back to category art. One tag had it and twenty-two did
    not, which is why "the app has no images" survived a fix that worked."""
    html = open(os.path.join(ROOT, "index.html"), encoding="utf-8").read()
    unguarded = re.findall(r"<img\b(?![^>]*referrerpolicy)[^>]*>", html)
    assert unguarded == [], f"{len(unguarded)} <img> without referrerpolicy"


def test_the_document_sets_a_referrer_policy_too():
    """The per-tag attribute is a list that can be added to; the meta covers
    anything a future tag forgets."""
    html = open(os.path.join(ROOT, "index.html"), encoding="utf-8").read()
    assert re.search(r'<meta\s+name="referrer"\s+content="no-referrer"', html)


# ─── 2. IMAGE_MODE agreed with itself ───────────────────────────────────────
def test_image_mode_defaults_match_across_modules():
    """They disagreed — "stock" in image_service, "thumbnail" in main — so which
    imagery policy applied depended on which module happened to read it."""
    import image_service
    import main
    assert main.IMAGE_MODE == image_service.IMAGE_MODE == "thumbnail"


def test_image_mode_is_declared_in_the_blueprint():
    y = open(os.path.join(ROOT, "render.yaml"), encoding="utf-8").read()
    assert "IMAGE_MODE" in y and "thumbnail" in y


# ─── a dead session is 401, not 404 ─────────────────────────────────────────
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer garbage"}])
def test_me_reports_a_dead_session_as_401(app, headers):
    """An invalid token fell through to anonymous uid=1, and with no such row
    /me answered 404 — indistinguishable from a broken route. The client only
    refreshes on 401, so a stale session never recovered."""
    _, cl = app
    assert cl.get("/me", headers=headers).status_code == 401


def test_me_still_works_with_a_real_session(app):
    _, cl = app
    r = cl.post("/signup", json={"email": "a@b.com", "password": "pw123456",
                                 "name": "A", "topics": []})
    tok = r.json().get("access_token") or r.json().get("token")
    assert cl.get("/me", headers={"Authorization": f"Bearer {tok}"}).status_code == 200


def test_public_endpoints_stay_readable_without_a_session(app):
    """require_user is for endpoints describing a person. The feed is not one."""
    _, cl = app
    assert cl.get("/feed?page=1&limit=5").status_code == 200
