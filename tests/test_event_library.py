"""Phase 1 of the historical analog engine — the event library.

The library decides which past articles can serve as analogs at all. Two gates
do the work, and both are here because getting either wrong is silent:

  * a placeholder article is not evidence (the same rule news_match applies),
  * an article that reaches no PRICED instrument cannot support a reaction
    statistic, however well it resolves as news.

Classification is deterministic and rule-based on purpose: event_class is 35% of
the Phase 2 matcher's weight, so it must not depend on a model's mood.
"""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sherrbyte"))

from app.spie.analog import event_library as el  # noqa: E402


# ─── classification ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("headline,expected", [
    ("RBI holds repo rate at 6.5% for the sixth straight meeting",
     "central_bank_policy"),
    ("US widens sanctions on Russian oil exports", "sanctions"),
    ("Missile strike closes shipping lane near the Strait of Hormuz",
     "geopolitical_conflict"),
    ("Port closure halts iron ore shipments for a third day",
     "supply_disruption"),
    ("Tata Motors to acquire stake in battery maker", "m_and_a"),
    ("Infosys posts profit of Rs 6,500 crore for the quarter", "earnings"),
    ("Maruti cuts full-year guidance on weak rural demand", "guidance_change"),
    ("Wipro chief executive steps down after four years", "leadership_change"),
    ("Builder defaults on Rs 900 crore of debt", "default_credit"),
    ("SEBI opens a probe into disclosure lapses", "regulatory_action"),
    ("Gold price climbs as bullion demand firms", "commodity_shock"),
    ("Rupee slips past 84 against the dollar", "currency_move"),
    ("Local club wins the district football final", "other"),
    ("", "other"),
])
def test_classification(headline, expected):
    assert el.classify(headline) == expected


def test_every_rule_class_is_in_the_closed_taxonomy():
    """A class the CHECK constraint rejects would fail every insert at runtime."""
    for klass, _ in el._CLASS_RULES:
        assert klass in el.EVENT_CLASSES


def test_the_migration_constraint_matches_the_python_taxonomy():
    """The SQL CHECK and EVENT_CLASSES must not drift apart."""
    sql = open(os.path.join(
        _ROOT, "sherrbyte/app/db/migrations/022_event_library.sql")).read()
    for klass in el.EVENT_CLASSES:
        assert f"'{klass}'" in sql, f"{klass} missing from the CHECK constraint"


def test_the_more_specific_class_wins():
    """'RBI raises rates' is central bank policy, not a regulatory action, even
    though SEBI-style regulator words also appear in the corpus."""
    assert el.classify(
        "Reserve Bank raises repo rate; regulator flags compliance gaps"
    ) == "central_bank_policy"


# ─── symbol linking ──────────────────────────────────────────────────────────

def test_symbol_index_only_contains_priced_instruments():
    """An instrument in SEED with no price series cannot support a reaction
    statistic, so it must not appear as a linked symbol."""
    idx = el.symbol_index()
    priced = set(el._instruments().values())
    for _, syms in idx.items():
        assert set(syms) <= priced


def test_symbol_index_maps_to_tickers_not_display_names():
    """Phase 3 joins these to market_ticks.symbol."""
    idx = el.symbol_index()
    assert "BZ=F" in idx.get("brent crude", [])


def test_crude_news_links_to_crude():
    syms = el.linked_symbols("OPEC agrees an output cut", "Saudi Arabia leads it")
    assert "BZ=F" in syms or "CL=F" in syms


def test_an_article_about_nothing_priced_links_to_no_symbol():
    """The gate that keeps mid-cap news out of the library."""
    assert el.linked_symbols(
        "District football final draws a record crowd",
        "The match went to penalties on Sunday evening.") == []


def test_short_keys_cannot_enter_the_index():
    """Bare two-letter keys would match half the corpus."""
    assert all(len(k) >= 3 for k in el.symbol_index())


# ─── the funnel's diagnosis ──────────────────────────────────────────────────

def test_an_all_stub_corpus_is_diagnosed_as_such():
    f = {"scanned": 100, "stub_skipped": 100, "no_entities": 0,
         "no_symbols": 0, "written": 0, "errors": 0}
    assert "usable as evidence" in el._diagnose(f)


def test_a_corpus_reaching_no_instrument_is_diagnosed_as_such():
    f = {"scanned": 100, "stub_skipped": 0, "no_entities": 0,
         "no_symbols": 100, "written": 0, "errors": 0}
    assert "priced instrument" in el._diagnose(f)


def test_an_empty_corpus_is_not_reported_as_a_gate_failure():
    f = {"scanned": 0, "stub_skipped": 0, "no_entities": 0,
         "no_symbols": 0, "written": 0, "errors": 0}
    assert "no published articles" in el._diagnose(f)


# ─── against a real Postgres, when one is configured ─────────────────────────

DSN = os.getenv("SHERR_ENGINE_TEST_DSN")
pytestmark_db = pytest.mark.skipif(
    not DSN, reason="SHERR_ENGINE_TEST_DSN not set")


@pytestmark_db
def test_the_migration_applies_and_rejects_unusable_rows():
    """The CHECK constraints are the last line against dead-weight rows.

    asyncio.run rather than a pytest-asyncio marker, matching the convention in
    test_pattern_freshness.py — the marker is a no-op unless the plugin is
    installed, and a test that silently does not run is worse than none.
    """
    import asyncio
    import asyncpg
    asyncio.run(_check_constraints(asyncpg))


async def _check_constraints(asyncpg):
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(open(os.path.join(
            _ROOT, "sherrbyte/app/db/migrations/022_event_library.sql")).read())

        # An unknown class must be refused.
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO hist_events (article_id, occurred_at, entity_ids,"
                " event_class, linked_symbols) VALUES"
                " (1, now(), ARRAY[gen_random_uuid()], 'not_a_class',"
                " ARRAY['BZ=F'])")

        # A row with no symbol can never be an analog.
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO hist_events (article_id, occurred_at, entity_ids,"
                " event_class, linked_symbols) VALUES"
                " (2, now(), ARRAY[gen_random_uuid()], 'earnings',"
                " ARRAY[]::text[])")
    finally:
        await conn.execute("DROP TABLE IF EXISTS hist_events")
        await conn.close()


# ─── build(), end to end against a fake connection ───────────────────────────

class FakeConn:
    """Enough asyncpg surface for build(): one page of rows, then empty."""

    def __init__(self, rows):
        self.pages = [rows, []]
        self.written = []

    async def fetch(self, sql, *args):
        return self.pages.pop(0) if self.pages else []

    async def execute(self, sql, *args):
        self.written.append(args)


# body_state._MIN_ORIGINAL_WORDS is 25: anything shorter classifies as EMPTY,
# which row_is_healthy rejects just as it rejects a stub.
_REAL_BODY = (
    "Producers agreed the reduction after two days of talks, with the change "
    "taking effect from the start of next month. Officials said the decision "
    "reflects softer demand across several importing economies, and that the "
    "group would meet again before the quarter ends to review whether the cut "
    "needs extending any further.")


def _article(aid, headline, summary, body=_REAL_BODY):
    return {"id": aid, "headline": headline, "summary_60": summary,
            "full_body": body, "source_summary": "Publisher's own words here.",
            "occurred_at": "2026-06-01T10:00:00+00:00"}


def _run_build(rows, monkeypatch, entities=("11111111-1111-1111-1111-111111111111",)):
    import asyncio

    async def fake_entities(conn, headline, summary):
        return list(entities)

    monkeypatch.setattr(el, "_entities_for", fake_entities)
    conn = FakeConn(rows)
    return conn, asyncio.run(el.build(conn, batch=50))


def test_build_writes_a_usable_article(monkeypatch):
    conn, f = _run_build([_article(
        1, "OPEC agrees a production cut", "Saudi Arabia leads the decision.")],
        monkeypatch)
    assert f["written"] == 1 and len(conn.written) == 1
    article_id, _, eids, klass, syms = conn.written[0]
    assert article_id == 1 and eids and syms
    assert klass in el.EVENT_CLASSES


def test_build_skips_a_placeholder_article(monkeypatch):
    """A stub is not evidence — it would match on nothing real."""
    from ai_processor import _SAFE_BODY, _SAFE_SUMMARY
    conn, f = _run_build([_article(
        2, "OPEC agrees a production cut", _SAFE_SUMMARY, _SAFE_BODY)],
        monkeypatch)
    assert f["stub_skipped"] == 1 and f["written"] == 0 and conn.written == []


def test_build_skips_an_article_that_reaches_no_instrument(monkeypatch):
    conn, f = _run_build([_article(
        3, "District football final draws a crowd", "It went to penalties.")],
        monkeypatch)
    assert f["no_symbols"] == 1 and f["written"] == 0


def test_build_skips_an_article_resolving_to_no_known_entity(monkeypatch):
    """create=False means an unseen name contributes nothing, and an event with
    no entity cannot be matched by entity_jaccard — 45% of the ranking."""
    conn, f = _run_build([_article(
        4, "OPEC agrees a production cut", "Saudi Arabia leads it.")],
        monkeypatch, entities=())
    assert f["no_entities"] == 1 and f["written"] == 0


def test_build_counts_by_class(monkeypatch):
    _, f = _run_build([
        _article(5, "OPEC agrees a production cut", "Crude supply tightens."),
        _article(6, "RBI holds the repo rate", "The rupee steadied after."),
    ], monkeypatch)
    assert f["written"] == 2
    assert sum(f["by_class"].values()) == 2


def test_build_reports_a_diagnosis_not_just_counts(monkeypatch):
    _, f = _run_build([_article(
        7, "OPEC agrees a production cut", "Crude supply tightens.")],
        monkeypatch)
    assert f["diagnosis"] and "written" in f["diagnosis"]
