"""The TL;DR a reader generates by tapping the button on a myFeed card.

Two properties carry the design and both are pinned here:

* IT SPENDS NO PROVIDER QUOTA. The free tier allows 15 requests a minute and
  the rewrite drain already budgets 12 of them, so a reader-triggered model
  call would compete with the pass that is actually writing the corpus. The
  payload is DERIVED from what the writer already wrote, which is why these
  tests can assert exact output with no provider configured at all.
* AN UNWRITTEN STORY SAYS SO. The placeholder body and the publisher's own
  headline are not a summary; printing either back as one is worse than
  printing nothing, so the endpoint answers ready:false with a reason.
"""
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import body_state  # noqa: E402
import cache       # noqa: E402
import main        # noqa: E402


@pytest.fixture(autouse=True)
def _empty_cache():
    cache._local.clear()
    yield
    cache._local.clear()


@pytest.fixture
def client():
    return TestClient(main.app)


def _seed(tmp_path, monkeypatch, rows):
    db = str(tmp_path / "tldr.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    for r in rows:
        cols = ",".join(r.keys())
        marks = ",".join("?" for _ in r)
        conn.execute(f"INSERT INTO articles ({cols}) VALUES ({marks})",
                     tuple(r.values()))
    conn.commit()
    conn.close()

    def _get_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(main, "get_db", _get_db)


_BASE = dict(url="u", status="published", ai_processed=1, pillar_id=2,
             published_at="2026-09-01T10:00:00+00:00")


def test_a_written_story_returns_bullets_and_key_points(tmp_path, monkeypatch, client):
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=21, headline="Crude climbs as OPEC+ weighs deeper cuts",
        source_headline="Oil advances after OPEC+ signals restraint",
        summary_60="Benchmark crude settled higher after the meeting in Vienna. "
                   "Ministers signalled deeper output restraint into the quarter. "
                   "Refiners had positioned for a smaller reduction.",
        what_info="Crude oil prices rose after the OPEC+ meeting.",
        who_subject="OPEC+", who_affected='["refiners"]',
        when_info="Monday", where_info="Vienna",
        why_info="Concern over a supply glut.")])
    r = client.get("/article/21/tldr").json()
    assert r["ready"] is True
    assert r["signal_id"] == "SB-21"
    assert r["source"] == "derived"
    # One bullet per sentence of the written summary, in order.
    assert r["tldr"][0].startswith("Benchmark crude settled higher")
    assert len(r["tldr"]) == 3
    keys = [p["k"] for p in r["key_points"]]
    assert keys == ["What", "Who", "Where", "When", "Why"]
    assert dict((p["k"], p["v"]) for p in r["key_points"])["Who"] == "OPEC+, refiners"


def test_the_hook_is_preferred_over_the_summary(tmp_path, monkeypatch, client):
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=22, headline="OPEC+ weighs deeper cuts",
        hook="The cut landed smaller than the market had priced for weeks.",
        summary_60="Benchmark crude settled higher after the meeting.")])
    r = client.get("/article/22/tldr").json()
    assert r["tldr"] == ["The cut landed smaller than the market had priced for weeks."]


def test_an_unwritten_story_says_so_instead_of_echoing_the_headline(
        tmp_path, monkeypatch, client):
    """The placeholder body and a what_info seeded with the publisher's own
    headline are both non-information. Neither may be served as a TL;DR."""
    stub = body_state.source_material()  # whatever the current placeholder is
    headline = "Missile strike hits Riyadh airport terminal"
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=23, headline=headline, source_headline=headline,
        summary_60=stub if body_state.is_stub(stub) else
                   "SherrByte has not yet published this story.",
        what_info=headline, where_info="Not specified")])
    r = client.get("/article/23/tldr").json()
    assert r["ready"] is False
    assert r["tldr"] == []
    assert r["key_points"] == []
    assert r["reason"]
    # And nothing in the payload is the headline handed back as a summary.
    assert headline not in str(r["tldr"]) + str(r["key_points"])


def test_a_headline_echo_in_the_summary_is_not_a_bullet(tmp_path, monkeypatch, client):
    headline = "Parliament passes the data protection amendment"
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=24, headline=headline, summary_60=headline + ".")])
    r = client.get("/article/24/tldr").json()
    assert r["tldr"] == []


def test_a_missing_story_404s_and_is_not_cached_as_stale(tmp_path, monkeypatch, client):
    _seed(tmp_path, monkeypatch, [])
    assert client.get("/article/999/tldr").status_code == 404
    assert client.get("/article/999/tldr").status_code == 404


def test_abbreviations_do_not_split_a_sentence_into_two_bullets():
    out = main._sentences("The U.S. said the deal holds. Rs. 400 crore was committed.")
    assert out == ["The U.S. said the deal holds.", "Rs. 400 crore was committed."]
