"""The Telugu card: a translation in the same request, gated on what can be gated.

The properties under test are the ones the design rests on:

  * it costs NO extra provider request — the field rides the prompt that already
    writes the story;
  * a failing translation costs the English card NOTHING;
  * the Latin half of the compliance blocklist is still the ENGINE's one list,
    and the Telugu half mirrors it word for word (asserted, not assumed);
  * a figure that appears only in the Telugu is treated as invented.

The Telugu strings below are real sentences, not lorem: a script-ratio check is
meaningless against filler.
"""

import json
import os
import pathlib
import sqlite3
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import ai_processor   # noqa: E402
import main           # noqa: E402
import telugu         # noqa: E402

# One story, written twice. The English is what the gate already passed.
EN = {
    "refined_title": "OPEC+ weighs deeper cuts as Brent jumps past its normal day",
    "summary": ("Ministers met in Vienna and examined a further reduction in crude "
                "output. Brent rose 4.1 percent after the meeting, beyond its "
                "normal daily range."),
    "full_body": ("The OPEC+ group met in Vienna and examined a further reduction "
                  "in crude output. Brent rose 4.1 percent after the meeting, which "
                  "is beyond its normal daily range, and officials pointed to an "
                  "effect on the Indian import bill. Earlier this year the same "
                  "group trimmed output for the first time, and that decision was "
                  "followed by a smaller move."),
}

TE_BODY = ("ఒపెక్+ కూటమి వియన్నాలో సమావేశమై ముడి చమురు ఉత్పత్తిని మరింత తగ్గించే "
           "అంశాన్ని పరిశీలించింది. సమావేశం తరువాత బ్రెంట్ ధర 4.1 శాతం పెరిగింది, "
           "ఇది దాని సాధారణ రోజువారీ శ్రేణిని దాటింది. భారత దిగుమతి బిల్లుపై "
           "ప్రభావం ఉంటుందని అధికారులు పేర్కొన్నారు.")
TE = {
    "refined_title": "ఒపెక్+ ఉత్పత్తి కోతలపై చర్చలు జరిగాయి",
    "summary": "ఒపెక్+ కూటమి వియన్నాలో సమావేశమై ఉత్పత్తి కోతలను పరిశీలించింది.",
    "full_body": TE_BODY,
}


# ── 1. is it actually Telugu ─────────────────────────────────────────────────
def test_a_real_translation_passes_every_check():
    ok, failures = telugu.check(TE, EN)
    assert ok, failures


def test_an_english_answer_in_the_telugu_field_is_refused():
    # A model that cannot translate answers in English rather than refusing, and
    # an English string in this column would render as a duplicate card in the
    # "wrong" language with no error anywhere.
    ok, failures = telugu.check(dict(TE, full_body=EN["full_body"]), EN)
    assert not ok
    assert "not_telugu:body" in failures


def test_figures_and_tickers_do_not_count_against_the_script_ratio():
    # A numeric card is the common case for this writer; counting its digits and
    # its Latin tickers as "not Telugu" would fail every market story.
    assert telugu.looks_telugu("బ్రెంట్ ధర 4.1 శాతం పెరిగింది")
    assert telugu.script_ratio("4.1 90 2026") == 0.0      # no letters at all


# ── 2. one blocklist: the engine's for Latin, this module's for Telugu ──────
def test_the_latin_half_is_still_the_engines_own_blocklist():
    sebi = telugu._sebi_check()
    assert sebi is not None, "the engine blocklist must be reachable"
    leaked = dict(TE, full_body=TE_BODY + " Investors should buy at this target.")
    ok, failures = telugu.check(leaked, EN)
    assert not ok
    assert any(f.startswith("compliance:") for f in failures), failures


def test_a_telugu_advice_word_is_caught_and_names_its_english_mirror():
    pushy = dict(TE, full_body=TE_BODY + " ఇప్పుడే కొనండి.")
    ok, failures = telugu.check(pushy, EN)
    assert not ok
    assert "advice_te:buy" in failures


def _engine_blocklist():
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "sherrbyte"))
    from app.spie.reasoning.narrative import FORBIDDEN, FORBIDDEN_ADVICE
    return {w.strip() for w in FORBIDDEN} | {w.strip() for w in FORBIDDEN_ADVICE}


def test_every_telugu_entry_mirrors_a_word_the_engine_really_forbids():
    # This is the drift test. Without it the Telugu list could quietly forbid
    # something the English card is allowed to say, or keep forbidding a word
    # the engine dropped — and nobody would find out from either side.
    engine = _engine_blocklist()
    missing = sorted({mirror for _, mirror in telugu.FORBIDDEN_TE
                      if mirror not in engine})
    assert not missing, f"not in the engine blocklist: {missing}"


def test_ordinary_business_words_are_not_on_the_telugu_list():
    # "అమ్మకం" (sales) and "పెరుగుదల" (growth) are ordinary market vocabulary.
    # Only imperatives and unambiguous advice nouns belong here.
    words = {w for w, _ in telugu.FORBIDDEN_TE}
    assert "అమ్మకం" not in words
    assert "పెరుగుదల" not in words
    ok, failures = telugu.check(
        dict(TE, full_body=TE_BODY + " అమ్మకం పెరుగుదల నమోదైంది."), EN)
    assert ok, failures


# ── 3. figures ───────────────────────────────────────────────────────────────
def test_a_figure_only_the_telugu_states_is_treated_as_invented():
    ok, failures = telugu.check(
        dict(TE, full_body=TE_BODY + " బంగారం ధర 7,500 రూపాయలు."), EN)
    assert not ok
    assert "invented_number:7500" in failures


def test_a_year_or_a_date_is_not_an_invented_figure():
    # The English wrote the month in words; the Telugu date phrase carries digits.
    ok, failures = telugu.check(
        dict(TE, full_body=TE_BODY + " 2026 లో 12వ తేదీన ఇది జరిగింది."), EN)
    assert ok, failures


def test_the_value_decides_not_the_digit_count():
    # "2026" and "7500" are both four digits and only one of them is a date.
    assert telugu._tolerated("2026") is True
    assert telugu._tolerated("7500") is False
    assert telugu._tolerated("4.1") is False          # a measured figure
    assert telugu._tolerated("12") is True


def test_telugu_numerals_are_the_same_number():
    assert telugu.figures("౯౦ శాతం") == {"90"}
    assert telugu.invented_figures("౯౦", "90 percent") == []


def test_dropping_a_figure_is_allowed():
    # Telugu prose is tighter; losing a number is a translation choice, not a
    # hallucination. Only ADDING one is a failure.
    thin = dict(TE, full_body="ఒపెక్+ కూటమి వియన్నాలో సమావేశమై ముడి చమురు "
                              "ఉత్పత్తిని తగ్గించే అంశాన్ని పరిశీలించింది. "
                              "బ్రెంట్ ధర పెరిగింది, ఇది దాని సాధారణ రోజువారీ "
                              "శ్రేణిని దాటింది. దిగుమతి బిల్లుపై ప్రభావం "
                              "ఉంటుందని అధికారులు పేర్కొన్నారు.")
    ok, failures = telugu.check(thin, EN)
    assert ok, failures


# ── 4. proportion and shape ─────────────────────────────────────────────────
def test_a_truncated_translation_is_refused():
    ok, failures = telugu.check(dict(TE, full_body="ఒపెక్+ సమావేశమైంది."), EN)
    assert not ok and "too_short" in failures


def test_a_translation_that_grew_a_paragraph_is_refused():
    ok, failures = telugu.check(dict(TE, full_body=TE_BODY * 3), EN)
    assert not ok and "too_long" in failures


def test_a_missing_half_is_nothing_at_all():
    assert telugu.check({"full_body": TE_BODY}, EN) == (False, ["missing"])
    assert telugu.parse({"full_body": TE_BODY}) == {}
    assert telugu.parse({"refined_title": TE["refined_title"]}) == {}
    assert telugu.parse(None) == {} and telugu.parse("not json") == {}
    assert telugu.parse(json.dumps(TE, ensure_ascii=False))["full_body"] == TE_BODY


def test_each_failure_is_reported_once():
    # The same rule firing on the headline and the body is ONE thing to fix.
    ok, failures = telugu.check(
        {"refined_title": "OPEC meets", "full_body": EN["full_body"]}, EN)
    assert not ok
    assert failures.count("not_telugu:headline") == 1
    assert len(failures) == len(set(failures))


# ── 5. the prompt, and the request it rides on ──────────────────────────────
def test_the_field_is_asked_for_in_every_writing_mode():
    for instr in (ai_processor.instruction_for("short blurb"),
                  ai_processor.instruction_for("word " * 200),
                  ai_processor.instruction_for("b", ai_processor.RESEARCH_MODE)):
        assert "18. telugu" in instr
        # ...and always before the closing line, or the model stops reading.
        assert instr.index("18. telugu") < instr.rindex("Output the JSON object only.")


def test_the_prompt_demands_a_translation_and_not_a_second_article():
    instr = ai_processor.instruction_for("word " * 200)
    rule = instr.split("18. telugu")[1]
    assert "TRANSLATE WHAT YOU WROTE" in instr
    assert "Omit the field entirely" in rule          # silence is a valid answer
    assert "ALREADY BE IN YOUR ENGLISH ANSWER" in rule
    assert "currency symbol" in rule


def test_the_switch_is_read_at_call_time(monkeypatch):
    # CLAUDE.md: configuration read at import is what forces importlib.reload,
    # which this repo has already been burnt by.
    monkeypatch.setenv("TELUGU_ENABLED", "0")
    assert "18. telugu" not in ai_processor.instruction_for("word " * 200)
    monkeypatch.setenv("TELUGU_ENABLED", "1")
    assert "18. telugu" in ai_processor.instruction_for("word " * 200)


def test_the_output_ceiling_moves_with_the_field(monkeypatch):
    # A response cut off mid-JSON loses the ENGLISH card too, so the ceiling is
    # raised by the field rather than hoped to be enough.
    long_body = "word " * 200
    monkeypatch.setenv("TELUGU_ENABLED", "1")
    with_te = ai_processor._out_tokens(long_body, gemini=True)
    monkeypatch.setenv("TELUGU_ENABLED", "0")
    without = ai_processor._out_tokens(long_body, gemini=True)
    assert with_te > without
    assert without == 2048                            # the pre-Telugu value


def test_the_response_schema_carries_the_field():
    te = ai_processor._GEMINI_SCHEMA["properties"]["telugu"]
    assert set(te["properties"]) == {"refined_title", "summary", "full_body"}
    # Optional: a model that omits it must not fail schema validation.
    assert "telugu" not in ai_processor._GEMINI_SCHEMA["required"]


def test_no_second_provider_call_was_added():
    # The whole design rests on this: one request writes the story AND the
    # translation. A translate() coroutine would mean a second call per article
    # against a 15-request-a-minute ceiling.
    src = pathlib.Path("ai_processor.py").read_text(encoding="utf-8")
    assert "async def translate" not in src
    assert "telugu" in src.split("_PROVIDER_CALLS")[0]   # the rule, not a call
    main_src = pathlib.Path("main.py").read_text(encoding="utf-8")
    block = main_src.split("def _write_telugu")[1].split("\n# The Section-1")[0]
    for call in ("process_batch", "process_article", "await "):
        assert call not in block, f"_write_telugu must spend no request: {call}"


# ── 6. the write path ───────────────────────────────────────────────────────
def _conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, headline TEXT, "
              "full_body TEXT, telugu TEXT DEFAULT '')")
    c.execute("INSERT INTO articles (id, headline, full_body, telugu) VALUES "
              "(7, ?, ?, '')", (EN["refined_title"], EN["full_body"]))
    c.commit()
    return c


def _stored(c):
    return c.execute("SELECT telugu FROM articles WHERE id=7").fetchone()["telugu"]


def test_a_passing_translation_is_stored_as_json_in_text(monkeypatch):
    monkeypatch.setenv("TELUGU_ENABLED", "1")
    c = _conn()
    main._write_telugu(c, 7, dict(EN, telugu=TE))
    got = json.loads(_stored(c))
    assert got["full_body"] == TE_BODY
    assert got["refined_title"] == TE["refined_title"]


def test_a_failing_translation_costs_the_english_card_nothing(monkeypatch):
    monkeypatch.setenv("TELUGU_ENABLED", "1")
    c = _conn()
    bad = dict(TE, full_body=TE_BODY + " ఇప్పుడే కొనండి.")
    main._write_telugu(c, 7, dict(EN, telugu=bad))
    assert _stored(c) == ""                            # nothing written
    row = c.execute("SELECT headline, full_body FROM articles WHERE id=7").fetchone()
    assert row["headline"] == EN["refined_title"]      # English untouched
    assert row["full_body"] == EN["full_body"]


def test_a_row_with_no_translation_is_the_normal_case(monkeypatch):
    monkeypatch.setenv("TELUGU_ENABLED", "1")
    c = _conn()
    main._write_telugu(c, 7, dict(EN))                 # model omitted the field
    assert _stored(c) == ""


def test_the_switch_stops_the_write_too(monkeypatch):
    monkeypatch.setenv("TELUGU_ENABLED", "0")
    c = _conn()
    main._write_telugu(c, 7, dict(EN, telugu=TE))
    assert _stored(c) == ""


def test_every_attempt_is_recorded_so_the_pass_rate_is_a_number(monkeypatch):
    monkeypatch.setenv("TELUGU_ENABLED", "1")
    monkeypatch.setattr(telugu, "_STATS",
                        {"offered": 0, "written": 0, "failed": 0, "rules": {}})
    c = _conn()
    main._write_telugu(c, 7, dict(EN, telugu=TE))
    main._write_telugu(c, 7, dict(EN, telugu=dict(TE, full_body="చిన్నది.")))
    st = telugu.stats()
    assert st["offered"] == 2 and st["written"] == 1 and st["failed"] == 1
    assert st["pass_rate"] == 0.5
    assert "too_short" in st["rules"]
    assert "DIAGNOSIS" in st


def test_the_diagnosis_says_nothing_has_been_offered_yet(monkeypatch):
    monkeypatch.setattr(telugu, "_STATS",
                        {"offered": 0, "written": 0, "failed": 0, "rules": {}})
    st = telugu.stats()
    assert st["pass_rate"] is None                     # a 0/0 rate would be a lie
    assert "not run" in st["DIAGNOSIS"] or "omitting" in st["DIAGNOSIS"]


def test_the_write_is_wired_into_both_rewrite_paths():
    src = pathlib.Path("main.py").read_text(encoding="utf-8")
    assert src.count("_write_telugu(conn, row[\"id\"], result)") == 2
    # ...and it runs AFTER the row's own UPDATE in each, never instead of it.
    for chunk in src.split("_write_telugu(conn")[1:]:
        assert "UPDATE articles SET headline" not in chunk.split("\n")[0]


def test_the_column_is_a_migration_with_an_empty_default():
    src = pathlib.Path("main.py").read_text(encoding="utf-8")
    assert "\"ALTER TABLE articles ADD COLUMN telugu TEXT DEFAULT ''\"" in src


# ── 7. the surface ──────────────────────────────────────────────────────────
def test_the_dossier_serves_the_translation_and_an_empty_dict_otherwise():
    assert telugu.for_card(json.dumps(TE, ensure_ascii=False))["full_body"] == TE_BODY
    assert telugu.for_card("") == {}
    assert telugu.for_card("{}") == {}
    src = pathlib.Path("main.py").read_text(encoding="utf-8")
    assert '"telugu": telugu.for_card(d.get("telugu"))' in src


def test_myfeed_shows_the_pill_only_when_there_is_a_translation():
    html = pathlib.Path("index.html").read_text(encoding="utf-8")
    bind = html.split("function mfBindLang")[1].split("\nfunction ")[0]
    # No translation, no pill: an empty language toggle reads as broken.
    assert "if (!body || !head) return;" in bind
    assert "mfd-lang" in bind
    apply_ = html.split("function mfApplyLang")[1].split("\nfunction ")[0]
    assert ".mf-headline" in apply_ and ".mfd-lead" in apply_
    # The reader's choice is a per-viewer convenience, so every localStorage
    # touch is guarded and the default is English.
    pref = html.split("function mfLangPref")[1].split("\nfunction ")[0]
    assert "catch" in pref and "'en'" in pref
    assert "mfBindLang(el, d.telugu)" in html
    # Every desktop rule lives in the one @media block (CLAUDE.md).
    start = html.index("@media (min-width: 1024px)")
    i, depth = html.index("{", start), 0
    while True:
        if html[i] == "{":
            depth += 1
        elif html[i] == "}":
            depth -= 1
            if depth == 0:
                break
        i += 1
    assert ".mfd-lang{" in html and ".mfd-lang{" not in html[start:i]
