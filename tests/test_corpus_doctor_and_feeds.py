"""Exposure brief Phase A2 + A5: financial feed registry and the doctor endpoints.

- feeds_financial now carries weight / jurisdiction / licence per source, the
  brief's primary/press/macro set is present, blocked feeds never ingest, and
  the (url, name) shape the RSS concat depends on is unchanged.
- /admin/licence-audit and /admin/corpus-doctor exist and report the gates.
"""
import os
import sqlite3

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


import sys
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import feeds_financial as ff


# ── registry ─────────────────────────────────────────────────────────────────

def test_every_financial_feed_has_an_explicit_licence():
    ok = {"commercial_ok", "attribution_required", "blocked"}
    for f in ff._FEEDS:
        assert f.licence in ok, f"{f.name} has no explicit licence"
        assert f.jurisdiction in {"IN", "US", "GLOBAL"}, f.name
        assert f.weight in (ff._PRIMARY, ff._PRESS), f.name


def test_blocked_feeds_never_ingest_and_are_not_whitelisted():
    names_ingested = {n for _u, n in ff.FINANCIAL_FEEDS}
    for f in ff._FEEDS:
        if f.licence == "blocked":
            assert f.name not in names_ingested
            assert f.name not in ff.FINANCIAL_SOURCES


def test_the_brief_primary_and_macro_sources_are_present():
    names = {f.name for f in ff._FEEDS}
    # Primary Indian regulatory/exchange
    for n in ("NSE Announcements", "BSE Announcements", "SEBI Releases",
              "SEBI Orders", "RBI Releases", "MCA Notices", "IBBI Notices"):
        assert n in names, f"missing primary source {n}"
    # Macro / global that transmit into India
    for n in ("Fed / FOMC", "US Treasury", "EIA Energy", "OPEC", "LBMA Metals"):
        assert n in names, f"missing macro source {n}"


def test_primary_outweighs_press():
    assert ff.source_weight("NSE Announcements") == 1.0
    assert ff.source_weight("Mint Markets") == 0.6
    # A corpus-history name still counts as press, not 0.
    assert ff.source_weight("Mint") == 0.6
    assert ff.source_weight("Some Random Blog") == 0.0


def test_rss_tuple_shape_is_preserved():
    # root main.py concatenates FINANCIAL_FEEDS onto RSS_FEEDS with no adapter.
    assert all(isinstance(t, tuple) and len(t) == 2 for t in ff.FINANCIAL_FEEDS)
    assert ff.feed_class("Mint Markets") == "financial"
    assert ff.feed_class("The Guardian") == "general"


def test_licence_audit_shape():
    a = ff.licence_audit()
    assert a["unlicensed"] == 0
    assert a["blocked"] == 0
    assert set(a["by_licence"]) <= {"commercial_ok", "attribution_required",
                                    "blocked"}
    assert a["ingestible"] == len(ff.FINANCIAL_FEEDS)


# ── endpoints present ────────────────────────────────────────────────────────

def test_doctor_endpoints_registered():
    m = _read(MAIN)
    assert '@app.get("/admin/licence-audit")' in m
    assert '@app.get("/admin/corpus-doctor")' in m
    # corpus-doctor reports the A5 gates.
    block = m[m.index('@app.get("/admin/corpus-doctor")'):
              m.index('@app.get("/admin/corpus-doctor")') + 3200]
    for g in ("financial_articles_total", "financial_articles_30d",
              "original_body_rate", "with_symbol_rate", "symbol_universe"):
        assert g in block, f"gate {g} missing from corpus-doctor"


# ── corpus-doctor SQL runs against a sqlite-shaped articles table ─────────────

def test_corpus_doctor_financial_sql_executes_on_sqlite():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE articles (id INTEGER PRIMARY KEY, feed_class TEXT, "
        "status TEXT, published_at TEXT, full_body TEXT, summary_60 TEXT, "
        "source_summary TEXT)")
    con.execute(
        "INSERT INTO articles (feed_class,status,published_at,full_body,"
        "summary_60,source_summary) VALUES "
        "('financial','published',datetime('now'),"
        "'Reliance reported a rise in quarterly refining margins as Brent "
        "crude eased, lifting the standalone oil-to-chemicals segment across "
        "the reporting period.',"
        "'Reliance refining margins rose as crude eased over the quarter.',"
        "'Reliance Q results')")
    # exact queries the endpoint runs
    fin_total = con.execute(
        "SELECT COUNT(*) AS c FROM articles WHERE feed_class='financial'"
    ).fetchone()["c"]
    fin_30d = con.execute(
        "SELECT COUNT(*) AS c FROM articles WHERE feed_class='financial' "
        "AND published_at >= datetime('now','-30 days')").fetchone()["c"]
    rows = con.execute(
        "SELECT full_body, summary_60, source_summary, status FROM articles "
        "WHERE feed_class='financial' AND status='published'").fetchall()
    con.close()
    assert fin_total == 1 and fin_30d == 1 and len(rows) == 1
