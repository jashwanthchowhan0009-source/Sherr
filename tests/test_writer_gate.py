"""The writing spec's quality gate, its wiring into synthesis, and writer-doctor.

The spec (News · Strings · Dots) is explicit that the hook must come from SELECTION
and SPECIFICITY, never from tone, and enforces that with a fixed checklist run before
any card is published. These pin that checklist rule by rule — each one a pass and a
fail case, because a gate that never rejects and a gate that always rejects are
equally useless — and then prove the three things the wiring must guarantee:

  * a News body that fails the gate is NOT written when enforcement is on — the row
    keeps its safe placeholder for the next tick, never a fragment;
  * the same body IS written in measurement mode (WRITER_GATE_ENABLED=0), so the pass
    rate can be measured on live traffic before enforcement is switched on;
  * every decision, pass or fail, lands on /admin/writer-doctor bucketed by writer and
    rule — the number the spec gates building Strings and Dots on.
"""
import json
import os
import sqlite3
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import synthesis        # noqa: E402
import writer_gate      # noqa: E402


def _sebi():
    """The engine's ONE compliance blocklist, reached the way the app reaches it —
    never a second copy re-listed in the test (CLAUDE.md)."""
    return writer_gate.default_sebi_check()


# ─── banned terms: SEBI + throat-clearing + intensifiers + nominalisation ───────

def test_a_compliance_word_is_a_banned_term():
    """The SEBI list is the engine blocklist, injected — not re-listed here."""
    hits = writer_gate.banned_terms(
        "Analysts say the stock looks bullish after the results.",
        [], sebi_check=_sebi())
    assert any("compliance" in h for h in hits)


def test_throat_clearing_anywhere_is_a_banned_term():
    hits = writer_gate.banned_terms(
        "The bank cut rates. In a significant development, markets steadied.", [])
    assert any("throat-clearing" in h for h in hits)


def test_an_intensifier_without_a_number_is_banned():
    hits = writer_gate.banned_terms("The rupee plunged in afternoon trade.", [])
    assert any("intensifier" in h for h in hits)


def test_an_intensifier_attached_to_a_number_is_allowed():
    """"soared" alone is noise; "soared 40%" is signal. The rule is clause-scoped."""
    hits = writer_gate.banned_terms("The rupee plunged 3% in afternoon trade.", [])
    assert not any("intensifier" in h for h in hits)


def test_the_passive_nominalisation_pattern_is_banned():
    hits = writer_gate.banned_terms(
        "There was a tightening of lending norms by the regulator.", [])
    assert any("nominalisation" in h for h in hits)


def test_a_name_that_collides_with_a_banned_word_is_masked():
    """A firm called "Massive Dynamic" must not trip the intensifier list — the name
    is a quoted fact, masked before the tone scan, exactly as the SEBI check masks it."""
    hits = writer_gate.banned_terms(
        "Massive Dynamic reported a quiet quarter with steady demand.",
        ["Massive Dynamic"], sebi_check=_sebi())
    assert not any("intensifier" in h for h in hits)


def test_clean_prose_has_no_banned_terms():
    assert writer_gate.banned_terms(
        "The Reserve Bank cut its benchmark rate for the third time this year.",
        ["Reserve Bank"], sebi_check=_sebi()) == []


# ─── numbers verified ───────────────────────────────────────────────────────────

def test_a_number_backed_by_numbers_used_passes():
    assert writer_gate.numbers_verified(
        "Prices rose 4% on the session.",
        [{"value": "4%", "source": "Wire"}]) == []


def test_a_number_in_neither_numbers_used_nor_the_source_fails():
    fails = writer_gate.numbers_verified("Prices rose 4% on the session.", [], "")
    assert fails and "4" in fails[0]


def test_a_number_present_in_the_source_is_accepted_even_if_unlisted():
    """A figure that appears in the sources is demonstrably not invented — that keeps
    a model under-populating a brand-new field from stalling every numeric story."""
    assert writer_gate.numbers_verified(
        "Prices rose 4% on the session.", [],
        "Traders noted a 4% gain across the session.") == []


def test_a_comma_grouped_number_matches_its_plain_form():
    assert writer_gate.numbers_verified(
        "Revenue reached 1,200 crore.",
        [{"value": "1200 crore", "source": "Filing"}]) == []


def test_a_body_with_no_figures_passes_trivially():
    assert writer_gate.numbers_verified("Prices rose on the session.", []) == []


# ─── first six words ─────────────────────────────────────────────────────────────

def test_a_throat_clearing_opening_fails_first_six_words():
    assert writer_gate.first_six_words(
        "In a significant development, the market fell.")


def test_an_informative_opening_passes():
    assert writer_gate.first_six_words(
        "Brent fell four percent in two sessions, the steepest drop since March.") == []


def test_empty_text_fails_first_six_words():
    assert writer_gate.first_six_words("")


# ─── length bands ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("writer,n,ok", [
    ("news", 62, True), ("news", 40, False), ("news", 95, False),
    ("strings", 150, True), ("strings", 100, False),
    ("dots", 165, True), ("dots", 210, False),
])
def test_length_bands(writer, n, ok):
    text = " ".join(["word"] * n)
    assert (writer_gate.length_ok(text, writer) == []) is ok


# ─── causal language (Strings / Dots) ───────────────────────────────────────────

def test_an_unattributed_causal_claim_fails():
    assert writer_gate.causal_language("The rate cut led to a selloff.")


def test_an_attributed_causal_claim_is_allowed():
    assert writer_gate.causal_language(
        "The company cited the rate cut, which led to a review.", attributed=True) == []


# ─── open loop ───────────────────────────────────────────────────────────────────

def test_a_card_ending_on_a_question_is_an_open_loop():
    assert writer_gate.open_loop("So what happens next?", "news")


def test_a_card_that_closes_its_loop_passes():
    assert writer_gate.open_loop("The decision closed the matter.", "news") == []


# ─── entity presence ─────────────────────────────────────────────────────────────

def test_no_resolved_entity_fails():
    assert writer_gate.entity_presence([])
    assert writer_gate.entity_presence(["   "])


def test_a_resolved_entity_passes():
    assert writer_gate.entity_presence(["Reserve Bank"]) == []


# ─── the composed News gate ──────────────────────────────────────────────────────

GOOD_NEWS = ("Benchmark crude settled higher after delegates signalled that producers "
             "might tighten supply at their coming session, according to officials "
             "familiar with the talks. The group framed the discussion as preliminary "
             "and stressed that no formal decision had been reached. Traders weighed "
             "the remarks against a backdrop of steady demand, while ministers prepared "
             "to gather and review production policy over the following weeks.")


def test_a_clean_news_card_passes_the_whole_gate():
    ok, failures = writer_gate.check_news(
        {"content": GOOD_NEWS, "extracted_entities": ["OPEC+", "Vienna"],
         "numbers_used": []},
        overlap_passed=True, sebi_check=_sebi())
    assert ok and failures == []


def test_a_card_failing_several_rules_reports_each():
    ok, failures = writer_gate.check_news(
        {"content": "In a significant development, prices soared today. What next?",
         "extracted_entities": [], "numbers_used": []},
        overlap_passed=True, sebi_check=_sebi())
    rules = {f["rule"] for f in failures}
    assert not ok
    assert {"banned_terms", "first_six_words", "length",
            "open_loop", "entity_presence"} <= rules


def test_overlap_passed_false_records_the_ngram_rule():
    ok, failures = writer_gate.check_news(
        {"content": GOOD_NEWS, "extracted_entities": ["OPEC+"], "numbers_used": []},
        overlap_passed=False, sebi_check=_sebi())
    assert not ok
    assert any(f["rule"] == "ngram_overlap" for f in failures)


def test_overlap_is_computed_from_source_text_when_no_verdict_is_given():
    """With neither overlap_passed nor a clean source the ngram rule is skipped; with
    a source the body copies verbatim, it fires."""
    body = " ".join(["word"] * 65)
    source = body + " and some more original tail text about the same subject here now"
    ok, failures = writer_gate.check_news(
        {"content": body, "extracted_entities": ["X"], "numbers_used": []},
        source_text=source, sebi_check=_sebi())
    assert any(f["rule"] == "ngram_overlap" for f in failures)


# ─── wiring into the synthesis write path ────────────────────────────────────────

A = ("Oil prices rose on Monday after OPEC+ delegates said the group was "
     "weighing deeper output cuts at its next meeting in Vienna.")
B = ("Crude prices rose on Monday as OPEC+ delegates said the group was "
     "weighing deeper output cuts when ministers next meet in Vienna.")
# Original, 40 words, no figures: clears originality against A/B, so it reaches the
# gate, then FAILS the News length band. That is the "regenerate later / stay a
# placeholder" case the wiring must handle without ever writing a fragment.
SHORT = ("Officials meeting in the capital signalled that oil producers may revisit "
         "their supply plans, though nothing has been settled. Traders described the "
         "mood as cautious, noting that any change would depend on how demand holds "
         "through the season ahead here.")


@pytest.fixture(autouse=True)
def _reset_state():
    import main
    baseline = {"clusters": 0, "written": 0, "merged": 0, "failed": 0,
                "sources_used": 0, "reasons": {}, "pool": {}, "pairs": {},
                "near_misses": []}
    main._synth_run.clear(); main._synth_run.update(baseline)
    main._writer_doctor.clear()
    yield
    main._synth_run.clear(); main._synth_run.update(baseline)
    main._writer_doctor.clear()


def _row(rid, headline, summary):
    return {"id": rid, "headline": headline, "pillar_id": 2,
            "published_at": "2026-09-01T10:00:00+00:00", "micro_tags": "[]",
            "source_summary": summary, "summary_60": "", "full_body": "",
            "source_name": "Wire", "source_headline": headline, "url": f"u{rid}"}


def _db(tmp_path, rows):
    import main
    conn = sqlite3.connect(str(tmp_path / "s.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    for r in rows:
        conn.execute(
            "INSERT INTO articles (id, url, headline, source_headline, full_body,"
            " summary_60, source_summary, status, pillar_id, published_at,"
            " micro_tags, source_name, ai_processed, reprocessed)"
            " VALUES (?,?,?,?,?,?,?,'published',?,?,?,?,1,0)",
            (r["id"], r["url"], r["headline"], r["source_headline"], "", "",
             r["source_summary"], r["pillar_id"], r["published_at"],
             r["micro_tags"], r["source_name"]))
    conn.commit()
    return conn


def _cluster(tmp_path):
    return _db(tmp_path, [
        _row(1, "Crude climbs as OPEC+ weighs deeper output cuts", A),
        _row(2, "Oil advances after OPEC+ signals further restraint", B)])


def _answer(content):
    return synthesis.parse_synthesis(json.dumps({
        "headline": "Producers weigh deeper output cuts",
        "content": content, "extracted_entities": ["OPEC+", "Vienna"],
        "primary_source_attribution": "Wire"}))


def test_a_gate_failure_withholds_the_card_when_enforced(tmp_path, monkeypatch):
    import main
    monkeypatch.setattr(main, "WRITER_GATE_ENABLED", True)
    conn = _cluster(tmp_path)

    async def fake(prompt, n_sources=0):
        return _answer(SHORT)                       # clears overlap, fails length

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    leftover = main._synthesise_clusters(conn, work, budget=10)

    assert sorted(r["id"] for r in leftover) == [1, 2], \
        "a withheld card must go back to the single-article path"
    assert not any(r["synthesis_sources"] for r in
                   conn.execute("SELECT synthesis_sources FROM articles")), \
        "nothing may be written when the gate fails under enforcement"
    doc = main._writer_doctor["news"]
    assert doc["attempts"] == 1 and doc["failures"] == 1
    assert "length" in doc["by_rule"]


def test_measurement_mode_publishes_but_still_records(tmp_path, monkeypatch):
    """WRITER_GATE_ENABLED=0 measures the pass rate without withholding — so the
    spec's ">=90% before Strings and Dots" question can be answered on live traffic."""
    import main
    monkeypatch.setattr(main, "WRITER_GATE_ENABLED", False)
    conn = _cluster(tmp_path)

    async def fake(prompt, n_sources=0):
        return _answer(SHORT)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    main._synthesise_clusters(conn, work, budget=10)

    written = [r for r in conn.execute("SELECT synthesis_sources FROM articles")
               if r["synthesis_sources"]]
    assert len(written) == 1, "measurement mode must still publish the card"
    doc = main._writer_doctor["news"]
    assert doc["attempts"] == 1 and doc["failures"] == 1 and "length" in doc["by_rule"]


def test_a_clean_card_passes_the_gate_and_is_written(tmp_path, monkeypatch):
    import main
    monkeypatch.setattr(main, "WRITER_GATE_ENABLED", True)
    conn = _cluster(tmp_path)

    async def fake(prompt, n_sources=0):
        return _answer(GOOD_NEWS)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    main._synthesise_clusters(conn, work, budget=10)

    written = [r for r in conn.execute("SELECT full_body, synthesis_sources FROM articles")
               if r["synthesis_sources"]]
    assert len(written) == 1 and written[0]["full_body"] == GOOD_NEWS
    doc = main._writer_doctor["news"]
    assert doc["attempts"] == 1 and doc["passes"] == 1 and doc["failures"] == 0


def test_an_overlapping_body_records_the_ngram_rule_on_the_doctor(tmp_path, monkeypatch):
    import main
    conn = _cluster(tmp_path)

    async def copies(prompt, n_sources=0):
        return _answer(A + " " + A)                 # reproduces its source

    monkeypatch.setattr(main.ai_processor, "synthesize", copies)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    main._synthesise_clusters(conn, work, budget=10)
    assert "ngram_overlap" in main._writer_doctor["news"]["by_rule"]


# ─── the /admin/writer-doctor endpoint ───────────────────────────────────────────
# Called directly rather than through TestClient: the app's lifespan starts the
# APScheduler, and a second TestClient(app) in one process conflicts on its job ids
# (the same global-state flake that hits the other admin-endpoint tests here). The
# endpoint is a plain coroutine, so exercising it directly is both cleaner and
# deterministic.

def test_the_endpoint_is_registered_on_the_root_app():
    """render.yaml starts THIS app; a route on the engine app would be unreachable."""
    import main
    from fastapi.routing import APIRoute
    paths = {r.path for r in main.app.routes if isinstance(r, APIRoute)}
    assert "/admin/writer-doctor" in paths


def test_the_endpoint_refuses_without_the_admin_token():
    import asyncio
    import main
    from fastapi import HTTPException
    main.ADMIN_TOKEN = "tok"
    with pytest.raises(HTTPException):
        asyncio.run(main.admin_writer_doctor())
    with pytest.raises(HTTPException):
        asyncio.run(main.admin_writer_doctor(token="wrong"))


def test_the_endpoint_reports_the_pass_rate_and_bucketed_rules():
    import asyncio
    import main
    main.ADMIN_TOKEN = "tok"
    main._writer_doctor.clear()
    main._record_writer_result("news", True)
    main._record_writer_result("news", False, [{"rule": "length", "detail": "40 words"}])
    d = asyncio.run(main.admin_writer_doctor(x_admin_token="", token="tok"))
    news = d["writers"]["news"]
    assert news["attempts"] == 2 and news["passes"] == 1
    assert news["pass_rate"] == 0.5
    assert news["by_rule"]["length"] == 1
    assert news["recent"][0]["rule"] == "length"
    assert d["phase_gate"]["news_pass_rate"] == 0.5
    assert d["phase_gate"]["met"] is False        # 0.5 < 0.90
    main._writer_doctor.clear()
