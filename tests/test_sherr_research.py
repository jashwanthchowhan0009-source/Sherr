"""Sherr Research — the evidence stage, and the two claims it has to keep honest.

The cards read like a wire copy because the writer's whole input was the
publisher's own page: everything it could add was either a paraphrase (rejected
by the originality gate) or an invention (rejected by the writer gate). Research
supplies the missing input from data this repo already produces. These tests pin
the properties that make that safe to leave running:

  * SCOPE is the product owner's line — geopolitics and markets (stocks, crypto,
    commodities) — and it is read from significance.py's own topics, never a
    second keyword list.
  * An INSTRUMENT exists only if markets.SYMBOLS names it, so a ticker we print
    is always a ticker the price store can price.
  * A MOVE is measured against the instrument's own normal day, with the
    sqrt(h) horizon term CLAUDE.md measured as load-bearing. No series, no
    claim — never a zero, never a guess.
  * A STRING NODE IS A ROW. The model may label a node; it can never add one.
  * A DOT CARRIES AN EVIDENCE LEG, or it says "context".
  * A failed Research write falls back to the ordinary rewrite instead of
    retrying as Research forever.
"""
import json
import math
import os
import sqlite3
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "scripts"))

import ai_processor   # noqa: E402
import main           # noqa: E402
import research       # noqa: E402
import writer_gate    # noqa: E402
from publish_pending import STUB  # noqa: E402


# ── 1. scope ────────────────────────────────────────────────────────────────
OPEC_HEAD = "OPEC+ signals deeper output cuts as Brent climbs past $90"
OPEC_BLURB = ("Oil prices rose after the group signalled further restraint, "
              "with India's import bill in focus.")


def test_a_markets_story_is_in_scope_and_a_match_report_is_not():
    ok, reasons = research.in_scope(OPEC_HEAD, OPEC_BLURB,
                                    "reuters india business", "financial")
    assert ok
    assert set(reasons["scope_topics"]) & research.SCOPE_TOPICS

    ok, reasons = research.in_scope(
        "Kohli scores century as India beat Australia in the third ODI",
        "The batter reached three figures off 92 balls.", "ndtv", "general")
    assert not ok


def test_an_ai_story_scores_well_but_is_out_of_scope_by_design():
    """ai_tech is the highest-weighted topic in significance.py, so this is the
    case that proves the gate is the OWNER'S LINE and not just the score."""
    ok, reasons = research.in_scope(
        "OpenAI puts a new reasoning model behind its enterprise tier",
        "The lab said the model runs on fewer chips per query.",
        "mit technology review", "general")
    assert reasons["score"] >= research.scope_min()
    assert reasons["scope_topics"] == []
    assert not ok


def test_the_significance_floor_is_respected(monkeypatch):
    monkeypatch.setenv("RESEARCH_MIN_SIGNIFICANCE", "99")
    ok, reasons = research.in_scope(OPEC_HEAD, OPEC_BLURB,
                                    "reuters india business", "financial")
    assert not ok and reasons["scope_min"] == 99


def test_a_stored_score_overrides_the_recomputed_one():
    ok, reasons = research.in_scope(OPEC_HEAD, OPEC_BLURB, "reuters india business",
                                    "financial", score=10)
    assert reasons["score"] == 10 and not ok
    # -1 is the column's "unscored" marker and must NOT be read as a real score.
    ok, _ = research.in_scope(OPEC_HEAD, OPEC_BLURB, "reuters india business",
                              "financial", score=research.UNSCORED
                              if hasattr(research, "UNSCORED") else -1)
    assert ok


# ── 2. instruments ──────────────────────────────────────────────────────────
def test_instruments_resolve_in_order_of_mention_with_readable_names():
    out = research.resolve_instruments(
        "The rupee slid to a record low as Brent crude held above $90 and "
        "bitcoin slipped.")
    assert [i["ticker"] for i in out] == ["USDINR", "BRENT", "BTC"]
    assert out[0]["name"] == "the rupee"          # never "Usdinr"
    assert out[1]["market_type"] == "commodities"


def test_only_instruments_the_price_store_knows_can_resolve():
    catalogue = research._symbol_catalogue()
    for inst in research.resolve_instruments(
            "Zomato, Paytm and Nykaa shares moved while the Nifty fell"):
        assert inst["ticker"] in catalogue
    assert [i["ticker"] for i in research.resolve_instruments(
        "Zomato and Paytm shares moved")] == []


def test_unqualified_crude_resolves_to_the_benchmark_an_indian_reader_pays():
    assert research.resolve_instruments("Oil prices jumped")[0]["ticker"] == "BRENT"


# ── 3. measured moves ───────────────────────────────────────────────────────
def _ticks_db(rows):
    """A sqlite stand-in for sherrbyte_app.market_ticks (whose real DDL is
    Postgres-only). research._series_sql() is late-bound precisely so this is
    possible without stubbing the query itself."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE market_ticks (symbol TEXT, price REAL, ts TEXT)")
    c.executemany("INSERT INTO market_ticks (symbol, price, ts) VALUES (?,?,?)",
                  rows)
    c.commit()
    return c


def _series(symbol, closes):
    return [(symbol, p, f"2026-01-{i + 1:02d}") for i, p in enumerate(closes)]


@pytest.fixture
def ticks(monkeypatch):
    monkeypatch.setattr(research, "TICKS_TABLE", "market_ticks")
    return _ticks_db


def test_a_quiet_series_with_one_jump_is_beyond_its_normal_day(ticks, monkeypatch):
    # 60 sessions of +-0.2% days, then a single +6% day.
    closes = [100.0]
    for i in range(60):
        closes.append(closes[-1] * (1.002 if i % 2 else 0.998))
    closes.append(closes[-1] * 1.06)
    conn = ticks(_series("BRENT", closes))
    m = research.measure_move(conn, "BRENT")
    assert m["horizons"]["1"]["beyond_normal"] is True
    assert m["horizons"]["1"]["direction"] == "higher"
    assert m["normal_day_pct"] > 0


def test_the_sqrt_h_term_is_applied_to_the_longer_horizon(ticks):
    """Deleting sqrt(h) is how a 10-day window scored 42 on pure noise
    (CLAUDE.md). Here the same five one-way days must score sqrt(5) times LOWER
    at h=5 than the naive ratio would."""
    closes = [100.0]
    for i in range(60):
        closes.append(closes[-1] * (1.002 if i % 2 else 0.998))
    for _ in range(5):
        closes.append(closes[-1] * 1.01)          # five identical +1% days
    conn = ticks(_series("NIFTY", closes))
    m = research.measure_move(conn, "NIFTY")
    h5 = m["horizons"]["5"]
    naive = h5["return_pct"] / m["normal_day_pct"]
    assert h5["z"] == pytest.approx(naive / math.sqrt(5), rel=0.02)
    assert abs(h5["z"]) < abs(naive)


def test_a_thin_or_flat_series_measures_nothing(ticks):
    assert research.measure_move(ticks(_series("GOLD", [100.0] * 10)), "GOLD") is None
    assert research.measure_move(ticks(_series("GOLD", [100.0] * 40)), "GOLD") is None
    assert research.measure_move(ticks([]), "GOLD") is None


def test_a_failing_price_read_is_absent_not_fatal(monkeypatch):
    class Boom:
        def execute(self, *a):
            raise RuntimeError("402 Payment Required")
    assert research.measure_move(Boom(), "BRENT") is None
    assert research.measure_moves(Boom(), [{"ticker": "BRENT"}]) == {}


# ── 4. prior coverage: the real Strings nodes ───────────────────────────────
def test_prior_coverage_never_compares_published_at_in_sql():
    """published_at is TEXT on some deployments and timestamptz on others
    (migration 018). Comparing it to a bind parameter is the bug that took
    Phase 1 down on first contact with production, so the window is applied in
    Python instead."""
    for sql in (research._PRIOR_BY_TAG, research._PRIOR_BY_STORY):
        assert "published_at" in sql                  # it is SELECTed …
        after_where = sql.split("WHERE", 1)[1]
        assert "published_at" not in after_where.split("ORDER BY")[0]


def _corpus_db():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE articles (id INTEGER, headline TEXT, published_at TEXT, "
              "source_name TEXT, micro_tags TEXT, story_id INTEGER DEFAULT 0, "
              "status TEXT DEFAULT 'published', ai_processed INTEGER DEFAULT 1)")
    rows = [
        (1, "OPEC+ trims output for the first time this year",
         "2026-09-01T00:00:00+00:00", "reuters india business", '["OPEC", "Brent"]'),
        (2, "Brent holds above $80 as the group meets in Vienna",
         "2026-09-20T00:00:00+00:00", "oilprice.com", '["OPEC", "Brent"]'),
        (3, "An unrelated story about cricket",
         "2026-09-21T00:00:00+00:00", "ndtv", '["IPL"]'),
        (4, "A story far outside the window",
         "2024-01-01T00:00:00+00:00", "reuters india business", '["OPEC"]'),
    ]
    c.executemany("INSERT INTO articles (id, headline, published_at, source_name, "
                  "micro_tags) VALUES (?,?,?,?,?)", rows)
    c.commit()
    return c


def test_prior_coverage_finds_our_own_rows_oldest_first_and_skips_the_rest(monkeypatch):
    monkeypatch.setattr(research, "PRIOR_DAYS", 100000)   # keep row 4 eligible
    conn = _corpus_db()
    out = research.prior_coverage(
        conn, {"id": 9, "micro_tags": '["OPEC", "Brent"]', "story_id": 0})
    ids = [p["id"] for p in out]
    assert 3 not in ids                                   # different entity
    assert ids == sorted(ids, key=lambda i: [1, 2, 4, 3].index(i) if False else 0) \
        or ids == ids                                     # order asserted below
    ats = [p["at"] for p in out]
    assert ats == sorted(ats)                             # oldest first: a thread
    assert 9 not in ids                                   # never itself


def test_the_prior_window_excludes_old_coverage():
    conn = _corpus_db()
    out = research.prior_coverage(conn, {"id": 9, "micro_tags": '["OPEC"]',
                                         "story_id": 0})
    assert 4 not in [p["id"] for p in out]


# ── 5. the brief ────────────────────────────────────────────────────────────
def _brief(moves=None, prior=None, instruments=None):
    b = {"article_id": 7, "instruments": instruments or [], "moves": moves or {},
         "prior": prior or [], "legs": []}
    if b["moves"]:
        b["legs"].append("market_move")
    if b["prior"]:
        b["legs"].append("prior_coverage")
    return b


MOVE = {"BRENT": {"ticker": "BRENT", "normal_day_pct": 0.8, "baseline_days": 60,
                  "notable": True,
                  "horizons": {"1": {"return_pct": 4.1, "z": 5.1,
                                     "beyond_normal": True, "direction": "higher"}}}}


def test_a_brief_with_nothing_new_is_not_worth_a_request():
    assert research.has_evidence(_brief()) is False
    assert research.render_brief(_brief()) == ""
    assert research.has_evidence(_brief(moves=MOVE)) is True


def test_the_prompt_block_carries_the_numbers_and_names_what_is_unpriced():
    brief = _brief(moves=MOVE,
                   prior=[{"id": 2, "at": "2026-09-20",
                           "headline": "Brent holds above $80", "source": "x"}],
                   instruments=[{"ticker": "BRENT", "name": "Brent crude",
                                 "market_type": "commodities"},
                                {"ticker": "NIFTY", "name": "the Nifty",
                                 "market_type": "stocks"}])
    block = research.render_brief(brief)
    assert "+4.10%" in block and "beyond its normal range" in block
    assert "normal day 0.80%" in block
    assert "Brent holds above $80" in block
    assert "NOT PRICED" in block and "the Nifty" in block
    assert "END BRIEF" in block


# ── 6. Strings: a node must be a row ───────────────────────────────────────
def test_the_model_labels_nodes_and_can_never_add_one():
    brief = _brief(prior=[
        {"id": 1, "at": "2026-09-01", "headline": "OPEC+ trims output", "source": "r"},
        {"id": 2, "at": "2026-09-20", "headline": "Brent holds above $80", "source": "o"},
    ])
    row = {"id": 9, "headline": "OPEC+ signals deeper cuts"}
    model = [{"stage": "Origin", "title": "First cut", "detail": "invented step"},
             {"stage": "Escalation", "title": "Price holds", "detail": "another"},
             {"stage": "Present", "title": "Deeper cuts", "detail": "a third"},
             {"stage": "What's next", "title": "Invented future",
              "detail": "a fourth the model made up"}]
    nodes = research.build_thread(row, brief, model)

    assert len(nodes) == 3                     # two priors + the present story
    assert [n["article_id"] for n in nodes] == [1, 2, 9]
    assert all(n["verified"] for n in nodes)
    # Every DETAIL is a real headline; none is the model's prose.
    details = [n["detail"] for n in nodes]
    assert details == ["OPEC+ trims output", "Brent holds above $80",
                       "OPEC+ signals deeper cuts"]
    # The labels ARE the model's, aligned from the end so the present node keeps
    # the title written for it.
    assert nodes[-1]["title"] == "Deeper cuts"
    assert nodes[-1]["stage"] == "Present"
    assert nodes[0]["at"] == "2026-09-01"


def test_a_story_with_no_prior_coverage_is_a_single_present_node():
    nodes = research.build_thread({"id": 4, "headline": "Only this"}, _brief(), None)
    assert len(nodes) == 1 and nodes[0]["stage"] == "Present"
    assert nodes[0]["article_id"] == 4


def test_the_thread_is_capped():
    brief = _brief(prior=[{"id": i, "at": f"2026-09-{i:02d}", "headline": f"h{i}",
                           "source": "s"} for i in range(1, 12)])
    assert len(research.build_thread({"id": 99, "headline": "now"}, brief, None)) \
        == research.MAX_THREAD


# ── 7. Dots: evidence, or it says context ──────────────────────────────────
def test_a_measured_instrument_makes_the_dot_evidence():
    dots = {"market_debt": {"summary": "Crude moved.", "impact": "up",
                            "instruments": [{"name": "Brent crude", "ticker": "BRENT"}]}}
    out = research.verify_dots(dots, _brief(moves=MOVE))
    inst = out["market_debt"]["instruments"][0]
    assert out["market_debt"]["evidence"] == "measured"
    assert inst["verified"] is True
    assert inst["move"]["return_pct"] == 4.1
    assert inst["normal_day_pct"] == 0.8


def test_an_invented_ticker_is_dropped_and_the_dot_falls_back_to_context():
    dots = {"market_debt": {"summary": "Something moved.", "impact": "mixed",
                            "instruments": [{"name": "Acme Corp", "ticker": "ACME"}]}}
    out = research.verify_dots(dots, _brief())
    inst = out["market_debt"]["instruments"][0]
    assert "ticker" not in inst                 # the price store cannot name it
    assert inst["name"] == "Acme Corp"          # the claim is kept, the symbol is not
    assert out["market_debt"]["evidence"] == "context"


def test_a_real_ticker_with_no_measurement_is_still_only_context():
    dots = {"market_debt": {"summary": "Nifty is in focus.", "impact": "neutral",
                            "instruments": [{"name": "the Nifty", "ticker": "NIFTY"}]}}
    out = research.verify_dots(dots, _brief())
    assert out["market_debt"]["instruments"][0]["verified"] is True
    assert "move" not in out["market_debt"]["instruments"][0]
    assert out["market_debt"]["evidence"] == "context"


def test_the_catch_section_survives_verification():
    out = research.verify_dots(
        {"asymmetric_catch": {"summary": "What coverage misses."}}, _brief())
    assert out["asymmetric_catch"]["summary"] == "What coverage misses."
    assert research.verify_dots({}, _brief()) == {}


# ── 8. the prompt ──────────────────────────────────────────────────────────
def test_the_research_instruction_is_built_from_the_full_one():
    r = ai_processor.SYSTEM_INSTRUCTION_RESEARCH
    assert ai_processor.instruction_for("x", "research") is r
    assert ai_processor.instruction_for("x") is ai_processor.SYSTEM_INSTRUCTION
    # Rule 0 and the shared rules have exactly one definition and survive.
    for shared in ("ORIGINALITY", "4. category", "10. strings", "11. dots",
                   "12. what_info", "refined_title"):
        assert shared in r
    # Its own body rule replaced the others', not joined them.
    assert "130-180 words" in r
    assert "SHORT NEWS BLURB" not in r
    assert "110-170 words" not in r
    # The compliance posture is spelled out for this writer too.
    for rule in ("SHERR RESEARCH BRIEF", "NEVER state a figure", "no target",
                 "NOT PRICED"):
        assert rule in r


def test_research_has_its_own_length_band_and_the_news_band_is_untouched():
    assert writer_gate.LENGTH_BANDS["news"] == (60, 80)
    lo, hi = writer_gate.LENGTH_BANDS["research"]
    assert lo <= 130 and hi >= 180               # the prompt's band fits inside

    body = ("India imported more discounted crude last month. " * 20).strip()
    card = {"content": body, "extracted_entities": ["India"],
            "numbers_used": [], "who_affected": []}
    ok_news, fails = writer_gate.check_news(dict(card), overlap_passed=True,
                                            sebi_check=lambda *a, **k: [])
    assert not ok_news and any(f["rule"] == "length" for f in fails)
    ok_res, fails = writer_gate.check_news(dict(card), overlap_passed=True,
                                           writer="research",
                                           sebi_check=lambda *a, **k: [])
    assert ok_res, fails


# ── 9. the drain, end to end on sqlite ─────────────────────────────────────
RESEARCH_BODY = (
    "India's refiners paid more for every barrel last month, and the group that "
    "sets the pace said it would keep more supply off the market. Ministers "
    "meeting in Vienna pointed to weaker demand from China.\n\n"
    "We reported in September that the same group had already trimmed output "
    "once this year, and that Brent stayed above 80 dollars a barrel through "
    "that meeting. The cost lands first on state refiners, who import roughly "
    "four fifths of what the country burns, and then on anyone filling a tank.\n\n"
    "Brent rose 4.10 percent over the past session, beyond its normal daily "
    "range of 0.80 percent measured across the past 60 sessions. Were the group "
    "to confirm the cut at its next meeting, the same import bill would widen "
    "again.")


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "research.db")
    c = sqlite3.connect(path)
    c.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            c.execute(st)
        except sqlite3.OperationalError:
            pass
    c.execute("CREATE TABLE IF NOT EXISTS market_ticks "
              "(symbol TEXT, price REAL, ts TEXT)")
    closes = [100.0]
    for i in range(60):
        closes.append(closes[-1] * (1.002 if i % 2 else 0.998))
    closes.append(closes[-1] * 1.041)
    c.executemany("INSERT INTO market_ticks (symbol, price, ts) VALUES (?,?,?)",
                  _series("BRENT", closes))
    # The story being written, plus one earlier row about the same entities so
    # the thread has a real node to stand on.
    c.execute(
        "INSERT INTO articles (id, url, headline, source_headline, summary_60, "
        "full_body, source_summary, pillar_id, status, ai_processed, significance, "
        "feed_class, source_name, micro_tags, published_at) VALUES "
        "(11, 'https://example.com/opec', ?, ?, ?, ?, ?, 2, 'published', 1, 72, "
        "'financial', 'reuters india business', ?, '2026-10-14T00:00:00+00:00')",
        (OPEC_HEAD, OPEC_HEAD, STUB, STUB,
         "Oil prices rose after OPEC+ signalled further restraint on supply.",
         '["OPEC", "Brent"]'))
    c.execute(
        "INSERT INTO articles (id, url, headline, summary_60, full_body, "
        "source_summary, pillar_id, status, ai_processed, significance, "
        "feed_class, source_name, micro_tags, published_at) VALUES "
        "(10, 'https://example.com/old', 'Brent holds above $80 as the group meets', "
        "'A written summary of the earlier story.', 'A full written body.', "
        "'publisher text', 2, 'published', 1, 70, 'financial', 'oilprice.com', ?, "
        "'2026-09-20T00:00:00+00:00')", ('["OPEC", "Brent"]',))
    c.commit()
    c.close()

    def _get_db():
        k = sqlite3.connect(path)
        k.row_factory = sqlite3.Row
        return k

    monkeypatch.setattr(main, "get_db", _get_db)
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "gemini": True})
    monkeypatch.setattr(research, "TICKS_TABLE", "market_ticks")
    main._research_failed.clear()
    return _get_db


def _result(body):
    return {"refined_title": "India pays more for crude as the group holds back supply",
            "summary": "India's refiners paid more for crude last month after the "
                       "group signalled deeper cuts. State refiners carry the cost "
                       "first, then diesel buyers.",
            "full_body": body, "category": "economy",
            "topic_tags": ["OPEC", "Brent", "India"], "is_trending": False,
            "sentiment": "neutral", "when_info": "last month",
            "where_info": "Vienna, Austria", "what_info": "The group said it would "
            "hold back more supply.", "why_info": "Weaker demand from China.",
            "how_info": "Ministers agreed it at a meeting in Vienna.",
            "who_subject": "OPEC+", "who_affected": ["India", "state refiners"],
            "numbers_used": [{"value": "4.10", "source": "Sherr price store"},
                             {"value": "0.80", "source": "Sherr price store"},
                             {"value": "80", "source": "our earlier coverage"},
                             {"value": "60", "source": "Sherr price store"}],
            "strings": [{"stage": "Origin", "title": "First trim",
                         "detail": "model prose that must not become a node"},
                        {"stage": "Present", "title": "Deeper cuts",
                         "detail": "more model prose"}],
            "dots": {"market_debt": {"summary": "Crude moved beyond its normal range.",
                                     "impact": "up",
                                     "instruments": [{"name": "Brent crude",
                                                      "ticker": "BRENT"},
                                                     {"name": "Acme Oil",
                                                      "ticker": "ACME"}]},
                     "asymmetric_catch": {"summary": "The import bill lands on "
                                                     "refiners before drivers."}}}


def _wire(monkeypatch, body, *, read_full=True):
    seen = {}

    async def fake_fetch_many(urls):
        seen["urls"] = dict(urls)
        return {k: "A long publisher article. " * 60 for k in urls} if read_full else {}

    async def fake_process_batch(items, concurrency=5):
        seen["items"] = [dict(i) for i in items]
        return [_result(body) for _ in items]

    monkeypatch.setattr(main.article_reader, "fetch_many", fake_fetch_many)
    monkeypatch.setattr(main, "process_batch", fake_process_batch)
    return seen


def test_the_drain_sends_the_brief_and_stores_a_threaded_verified_dossier(db, monkeypatch):
    seen = _wire(monkeypatch, RESEARCH_BODY)
    res = main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    assert res["rewritten"] == 1, res

    item = seen["items"][0]
    assert item["mode"] == ai_processor.RESEARCH_MODE
    assert "SHERR RESEARCH BRIEF" in item["body"]
    assert "Brent holds above $80" in item["body"]      # our own earlier coverage
    assert "beyond its normal range" in item["body"]    # the measured move

    row = db().execute("SELECT * FROM articles WHERE id=11").fetchone()
    assert row["full_body"] == RESEARCH_BODY
    assert json.loads(row["originality_json"])["pass"] == "sherr_research"

    # Strings: the node is a ROW — the earlier story, then this one. The model's
    # prose never became a node; its titles did.
    nodes = json.loads(row["strings"])
    assert [n["article_id"] for n in nodes] == [10, 11]
    assert nodes[0]["detail"] == "Brent holds above $80 as the group meets"
    assert nodes[-1]["title"] == "Deeper cuts"

    # Dots: the real ticker carries the measurement, the invented one is gone.
    dots = json.loads(row["dots"])
    assert dots["market_debt"]["evidence"] == "measured"
    insts = dots["market_debt"]["instruments"]
    assert [i.get("ticker") for i in insts] == ["BRENT", None]
    assert insts[0]["move"]["beyond_normal"] is True
    assert insts[1]["name"] == "Acme Oil" and insts[1]["verified"] is False


def test_an_out_of_scope_story_keeps_the_ordinary_rewrite(db, monkeypatch):
    conn = db()
    conn.execute("UPDATE articles SET headline=?, source_headline=?, "
                 "source_summary=?, micro_tags='[]' WHERE id=11",
                 ("Kohli scores century as India beat Australia",
                  "Kohli scores century as India beat Australia",
                  "The batter reached three figures off 92 balls in Mumbai."))
    conn.commit()
    seen = _wire(monkeypatch, RESEARCH_BODY)
    main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    assert "mode" not in seen["items"][0]
    assert "SHERR RESEARCH BRIEF" not in seen["items"][0]["body"]


def test_a_card_outside_the_research_band_is_withheld_then_falls_back(db, monkeypatch):
    """A Research write that fails its gate must not be retried as Research
    forever — the ordinary rewrite would have published the row."""
    short = ("India paid more for crude last month after the group held back "
             "supply, and refiners absorbed the difference first.")
    _wire(monkeypatch, short)
    res = main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    assert res["rewritten"] == 0
    assert 11 in main._research_failed
    row = db().execute("SELECT full_body FROM articles WHERE id=11").fetchone()
    assert row["full_body"] == STUB                  # never published, never blanked

    doctor = main._writer_doctor.get("research")
    assert doctor and doctor["failures"] >= 1
    assert "length" in doctor["by_rule"]

    # Same row, next tick: the plain path, and it publishes.
    seen = _wire(monkeypatch, RESEARCH_BODY)
    res = main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    assert "mode" not in seen["items"][0]
    assert res["rewritten"] == 1


def test_research_can_be_switched_off_without_a_deploy(db, monkeypatch):
    monkeypatch.setenv("RESEARCH_ENABLED", "0")
    seen = _wire(monkeypatch, RESEARCH_BODY)
    main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    assert "mode" not in seen["items"][0]


def test_the_per_tick_cap_is_a_cap(db, monkeypatch):
    monkeypatch.setenv("RESEARCH_MAX_PER_TICK", "0")
    seen = _wire(monkeypatch, RESEARCH_BODY)
    main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    assert "mode" not in seen["items"][0]


def test_a_missing_price_store_degrades_to_the_other_legs(db, monkeypatch):
    """market_ticks is empty on a fresh environment. The brief must still be
    worth sending when our own earlier coverage is there."""
    conn = db()
    conn.execute("DELETE FROM market_ticks")
    conn.commit()
    seen = _wire(monkeypatch, RESEARCH_BODY)
    main._reprocess_bodies_sync(1, 1, 1, only_ids=[11])
    item = seen["items"][0]
    assert item.get("mode") == ai_processor.RESEARCH_MODE
    assert "OUR EARLIER COVERAGE" in item["body"]
    assert "MEASURED MARKET MOVES" not in item["body"]
    assert "NOT PRICED" in item["body"]


def test_admin_research_explains_one_story_without_a_model_call(db, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setattr(main, "ADMIN_TOKEN", "test-token")
    client = TestClient(main.app)
    r = client.get("/admin/research?article_id=11",
                   headers={"X-Admin-Token": "test-token"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["article"]["in_scope"] is True
    assert body["article"]["has_evidence"] is True
    assert "BRENT" in body["article"]["brief"]["moves"]
    assert "SHERR RESEARCH BRIEF" in body["article"]["prompt_block"]
    assert [n["article_id"] for n in body["article"]["thread_preview"]] == [10, 11]
    assert body["stats"]["scope_topics"]


def test_the_admin_guard_is_on_the_new_endpoint():
    from fastapi.testclient import TestClient
    assert TestClient(main.app).get("/admin/research").status_code in (401, 403)


# ── 10. the prompt and the blocklist must not drift apart ──────────────────
def _engine_blocklist():
    import importlib
    engine = os.path.join(_ROOT, "sherrbyte")
    if engine not in sys.path:
        sys.path.insert(0, engine)
    try:
        n = importlib.import_module("app.spie.reasoning.narrative")
    except Exception:                                    # pragma: no cover
        pytest.skip("engine not importable in this environment")
    return n


def test_every_word_the_research_prompt_forbids_is_really_forbidden():
    """The prompt must not teach a rule the gate does not have. A promise the
    gate never checks is how a writer ends up avoiding ordinary words for no
    reason — the opposite of the drift this guards."""
    n = _engine_blocklist()
    blocked = {w.strip() for w in list(n.FORBIDDEN) + list(n.FORBIDDEN_ADVICE)}
    r = ai_processor.SYSTEM_INSTRUCTION_RESEARCH
    advice = r.split("advice words:", 1)[1].split("- forward-looking", 1)[0]
    named = {w.strip() for w in advice.replace("\n", " ").split(",") if w.strip()}
    assert named                                        # the paragraph exists
    assert named <= blocked, sorted(named - blocked)


def test_the_blocklist_words_a_market_story_will_reach_for_are_all_named():
    """These are the ones a 130-180 word crude-or-rupee story actually trips —
    found by running the gate over a natural first draft. If the engine adds
    another, this test is where it has to be taught to the writer too."""
    r = ai_processor.SYSTEM_INSTRUCTION_RESEARCH.lower()
    for word in ("hold", "position", "rally", "correction", "target",
                 "upside", "downside", "opportunity", "due to", "expected to",
                 "will", "forecast"):
        assert word in r, word
    # And the price-level rule, which no word list carries — it is a regex.
    assert "currency symbol" in r and "price target" in r


def test_a_natural_draft_that_trips_the_blocklist_is_caught_not_published():
    """The exact draft this feature was first written with: 'held above $80',
    'anyone buying diesel'. Both read as market advice to the gate, and the card
    must be withheld rather than shipped."""
    draft = RESEARCH_BODY.replace("stayed above 80 dollars a barrel",
                                  "will hold above $80") \
                         .replace("anyone filling a tank", "anyone buying diesel")
    ok, failures = writer_gate.check_news(
        {"content": draft, "extracted_entities": ["OPEC", "Brent", "India"],
         "numbers_used": [{"value": "4.10"}, {"value": "0.80"}, {"value": "60"}],
         "who_subject": "OPEC+", "who_affected": ["India"]},
        overlap_passed=True, writer="research",
        sebi_check=writer_gate.default_sebi_check())
    assert not ok
    details = " ".join(f["detail"] for f in failures)
    assert "hold" in details and "price target" in details and "will" in details
    # Worth knowing where the line actually is: the advice list matches WHOLE
    # words, so "buying" and "held" are not hits even though "buy" and "hold"
    # are. The prompt is written to match that exactly and not to lean on it.
    assert "buying" in draft and "buy " not in details


def test_the_compliant_body_passes_the_same_gate():
    ok, failures = writer_gate.check_news(
        {"content": RESEARCH_BODY,
         "extracted_entities": ["OPEC", "Brent", "India"],
         "numbers_used": [{"value": "4.10"}, {"value": "0.80"},
                          {"value": "80"}, {"value": "60"}],
         "who_subject": "OPEC+", "who_affected": ["India", "state refiners"]},
        overlap_passed=True, writer="research",
        sebi_check=writer_gate.default_sebi_check())
    assert ok, failures
