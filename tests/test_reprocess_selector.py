"""The rewrite pass reported "complete, rewritten 0" for weeks while 21,655
articles sat on a placeholder. Nothing raised; nothing was logged.

The cause: SELECT_NEEDING_REWRITE filtered on `reprocessed=0`, on the
assumption that only a successful AI pass ever sets that column. It doesn't —
run_ai_batch sets reprocessed=1 in the same UPDATE that stores the placeholder
body when every provider is down. So the corpus was stub AND reprocessed=1, the
selector matched nothing, and the loop finished instantly with no error.

These tests fail against that selector.
"""
import os
import sqlite3

import pytest

import body_state
from ai_processor import _SAFE_BODY, _SAFE_SUMMARY

REAL = ("Delhi's air quality index crossed 400 on Sunday, a third consecutive "
        "day in the severe band, prompting curbs on construction work.")
MINE = ("Readings across the capital held above the severe threshold through "
        "Sunday, a third straight day in that band. Officials paused building "
        "work and urged residents to stay indoors until winds pick up.")


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("""CREATE TABLE articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, headline TEXT,
        source_headline TEXT, full_body TEXT, summary_60 TEXT,
        source_summary TEXT, status TEXT, ai_processed INTEGER DEFAULT 0,
        reprocessed INTEGER DEFAULT 0, pillar_id INTEGER DEFAULT 3,
        micro_tags TEXT, source_name TEXT, published_at TEXT)""")
    yield c
    c.close()


def add(c, **kw):
    row = {"url": "u", "headline": "h", "source_headline": "sh",
           "full_body": MINE, "summary_60": MINE[:120], "source_summary": REAL,
           "status": "published", "reprocessed": 0,
           "published_at": "2026-08-31T10:00:00+00:00"}
    row.update(kw)
    cols = ", ".join(row)
    c.execute(f"INSERT INTO articles ({cols}) VALUES ({', '.join('?' * len(row))})",
              tuple(row.values()))
    c.commit()
    return c.execute("SELECT last_insert_rowid() AS i").fetchone()["i"]


def selected(c, limit=100):
    return [r["id"] for r in c.execute(body_state.SELECT_NEEDING_REWRITE, (limit,))]


def test_stub_body_already_flagged_reprocessed_is_still_selected(conn):
    """THE REGRESSION. A provider outage leaves exactly this row."""
    rid = add(conn, full_body=_SAFE_BODY, summary_60=_SAFE_SUMMARY,
              reprocessed=1, ai_processed=1)
    assert selected(conn) == [rid]


def test_stub_summary_with_written_body_is_selected(conn):
    """summary_60 is what the Home card shows, so a stub there is work even
    when the body was rewritten."""
    rid = add(conn, full_body=MINE, summary_60=_SAFE_SUMMARY, reprocessed=1)
    assert selected(conn) == [rid]


def test_empty_body_is_selected(conn):
    rid = add(conn, full_body="   ", reprocessed=1)
    assert selected(conn) == [rid]


def test_unprocessed_row_is_still_selected(conn):
    rid = add(conn, reprocessed=0)
    assert selected(conn) == [rid]


def test_finished_row_is_not_selected(conn):
    add(conn, reprocessed=1, ai_processed=1)
    assert selected(conn) == []


def test_draft_rows_are_never_selected(conn):
    add(conn, full_body=_SAFE_BODY, reprocessed=0, status="draft")
    assert selected(conn) == []


def test_selector_and_audit_agree(conn):
    """The disagreement between these two IS the silent no-op: audit() reported
    thousands needing a rewrite while the selector returned none."""
    for _ in range(4):
        add(conn, full_body=_SAFE_BODY, summary_60=_SAFE_SUMMARY, reprocessed=1)
    add(conn, reprocessed=1, ai_processed=1)
    assert body_state.audit(conn)["needs_rewrite"] == len(selected(conn)) == 4


def test_markers_are_matched_anywhere_not_just_as_a_prefix(conn):
    """classify() matches markers as substrings. A prefix-only LIKE would miss
    any marker that isn't the opening words."""
    mid = next(m for m in body_state._STUB_MARKERS
               if not _SAFE_BODY.lower().startswith(m))
    rid = add(conn, full_body=f"Context follows. {mid} Please check back.",
              reprocessed=1)
    assert rid in selected(conn)


def test_hostile_marker_text_cannot_break_the_query(monkeypatch, conn):
    """An apostrophe would end the SQL literal; % and _ are LIKE wildcards.
    Either would turn a harmless rewording into a silent selector failure."""
    monkeypatch.setattr(body_state, "_STUB_MARKERS",
                        ["sherr's 100% draft_note"], raising=False)
    sql = body_state._needing_rewrite_sql()
    rid = add(conn, full_body="Sherr's 100% draft_note is here.", reprocessed=1)
    add(conn, full_body="Sherr is 100 draft note free.", reprocessed=1)
    assert [r["id"] for r in conn.execute(sql, (100,))] == [rid]
