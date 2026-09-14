"""The myFeed dossier endpoint — node + strings + dots in one payload.

THE DEGRADED PATH IS THE POINT. Most rows carry no strings/dots (the synthesis
pass has not reached them), so the endpoint must still return a complete Node and
mark the other two panes 'pending' rather than erroring or blanking. That is the
normal case at first, not an edge case, and it is what these tests pin.
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
    # A shared local layer would leak one test's dossier into the next; the
    # endpoint is cached by id, so a fresh dict per test keeps them independent.
    cache._local.clear()
    yield
    cache._local.clear()


@pytest.fixture
def client():
    return TestClient(main.app)


def _seed(tmp_path, monkeypatch, rows):
    db = str(tmp_path / "dossier.db")
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


def test_a_bare_row_still_renders_the_node_and_marks_the_rest_pending(
        tmp_path, monkeypatch, client):
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=11, headline="Crude climbs as OPEC+ weighs deeper cuts",
        summary_60="Benchmark crude settled higher.",
        what_info="Crude oil prices rose after the meeting.",
        who_subject="OPEC+", who_affected='["refiners","importers"]',
        when_info="Monday", where_info="Vienna",
        how_info="Members signalled deeper output restraint.",
        why_info="Concern over a supply glut.", hook="The cut was smaller than priced.")])
    d = client.get("/article/11/dossier").json()
    assert d["signal_id"] == "SB-11"
    assert d["hook"] == "The cut was smaller than priced."
    # The Node is fully populated from columns the row already carries.
    assert d["node"]["what"] == "Crude oil prices rose after the meeting."
    assert d["node"]["who_subject"] == "OPEC+"
    assert d["node"]["who_affected"] == ["refiners", "importers"]
    assert d["node"]["mechanism"] == "Members signalled deeper output restraint."
    # No strings/dots were written → both panes are pending, and neither errors.
    assert d["strings"] == [] and d["strings_status"] == "pending"
    assert d["dots"] == {} and d["dots_status"] == "pending"
    # A single un-synthesised row reads as single-source.
    assert d["verification"]["state"] == "single"


def test_a_synthesised_row_returns_strings_and_dots_ready(
        tmp_path, monkeypatch, client):
    strings = ('[{"stage":"Origin","title":"Glut","detail":"Inventories built."},'
               '{"stage":"The Spark","title":"Meeting","detail":"Ministers convened."}]')
    dots = ('{"market_debt":{"summary":"Energy equities rose.","impact":"up",'
            '"instruments":[{"name":"Brent","ticker":"BZ"}]},'
            '"asymmetric_catch":{"summary":"Coverage omitted the storage overhang."}}')
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=12, headline="OPEC+ weighs deeper cuts",
        what_info="Prices rose.", synthesis_sources="[12,13,14]",
        strings=strings, dots=dots)])
    d = client.get("/article/12/dossier").json()
    assert d["strings_status"] == "ready"
    assert len(d["strings"]) == 2
    assert d["strings"][0]["stage"] == "Origin"
    assert d["dots_status"] == "ready"
    assert d["dots"]["market_debt"]["impact"] == "up"
    assert d["dots"]["market_debt"]["instruments"][0]["ticker"] == "BZ"
    # Three merged sources → corroborated verification with the count.
    assert d["verification"]["state"] == "corroborated"
    assert d["verification"]["sources"] == 3


def test_a_missing_article_is_404(tmp_path, monkeypatch, client):
    _seed(tmp_path, monkeypatch, [])
    assert client.get("/article/999/dossier").status_code == 404


def test_dossier_serves_stale_when_the_db_is_down(tmp_path, monkeypatch, client):
    """A blown Supabase quota returns 402 and every query raises. The dossier
    must then serve its last-good payload — this is what stops /myfeed hanging on
    'Loading dossier…' — rather than propagating the error."""
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=21, headline="Gravity storage tested in China",
        what_info="A gravity-based power store was tested.")])
    # First call succeeds and populates the stale copy.
    assert client.get("/article/21/dossier").json()["node"]["what"] \
        == "A gravity-based power store was tested."
    # Expire only the FRESH entry (the stale copy must survive), then take the DB
    # down. Without Redis in tests both live in the local dict, so drop just the
    # fresh key rather than clearing everything.
    cache._local.pop("dossier:21", None)

    def _dead_db():
        raise RuntimeError("FATAL: 402 Payment Required (quota exceeded)")
    monkeypatch.setattr(main, "get_db", _dead_db)

    r = client.get("/article/21/dossier")
    assert r.status_code == 200, "a DB outage must serve stale, not error"
    assert r.json()["node"]["what"] == "A gravity-based power store was tested."


def test_malformed_strings_dots_degrade_to_pending_not_error(
        tmp_path, monkeypatch, client):
    # A row whose JSON columns are junk must not 500 — the reader still gets the
    # Node, and the broken panes read as pending.
    _seed(tmp_path, monkeypatch, [dict(
        _BASE, id=15, headline="Something happened", what_info="A thing.",
        strings="not json at all", dots="{broken")])
    r = client.get("/article/15/dossier")
    assert r.status_code == 200
    d = r.json()
    assert d["node"]["what"] == "A thing."
    assert d["strings_status"] == "pending"
    assert d["dots_status"] == "pending"
