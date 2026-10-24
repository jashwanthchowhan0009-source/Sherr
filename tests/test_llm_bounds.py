"""llm_bounds.py — the size ceiling on anything a model wrote.

THE NUMBER THIS FILE EXISTS FOR. Driving `_validate_and_fix` with a long
response wrote **2.1MB into one row**, 300KB in each of seven columns, because
`full_body` and all six WWWH fields had a minimum and no maximum while
`refined_title` and `summary` were capped. `synthesis.parse_synthesis` had the
same split: `numbers_used` items cut at 60/120 chars, `content` uncapped.

It matters on this deployment specifically: the database has already been over
its size quota (egress ~487%, size ~248%), the instance has already been
OOM-killed, and a 300KB body is SERVED — one such row makes every /feed page
containing it 300KB heavier for every reader.

What the tests pin is not "a cap exists" but the two properties that make it
safe: it CLAMPS rather than rejects (rejecting sends the row back to its
placeholder forever, the failure mode CLAUDE.md records for
`no_source_material`), and it cannot touch a legitimate value.
"""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import ai_processor as ai   # noqa: E402
import llm_bounds as L      # noqa: E402
import synthesis            # noqa: E402

HUGE = "word " * 60_000      # 300,000 chars


@pytest.fixture(autouse=True)
def _clean():
    L.reset()
    yield
    L.reset()


# ─── the measured bound ──────────────────────────────────────────────────────
def _hostile_card():
    return {
        "refined_title": "A perfectly reasonable headline here",
        "summary": "a " * 30,
        "full_body": HUGE,
        "category": "tech",
        "topic_tags": ["x" * 9_000, "y"],
        "what_info": HUGE, "why_info": HUGE, "how_info": HUGE,
        "who_subject": HUGE, "when_info": HUGE, "where_info": HUGE,
        "who_affected": ["z" * 9_000],
    }


def test_the_whole_row_is_bounded_end_to_end():
    """The headline result: 2.1MB -> under 16KB, through the real validator."""
    before = L.row_chars(_hostile_card())
    assert before > 2_000_000            # the measured starting point
    after = L.row_chars(ai._validate_and_fix(_hostile_card(), "t", "b"))
    assert after < 16_000
    assert before / after > 100


@pytest.mark.parametrize("field,cap", sorted(L.CARD_CAPS.items()))
def test_every_named_card_field_is_capped(field, cap):
    r = ai._validate_and_fix(_hostile_card(), "t", "b")
    assert len(str(r[field])) <= cap


def test_no_single_list_item_is_unbounded():
    """A word count does not bound a column: one "word" can be 9,000 characters
    long, which is exactly how the list items got through."""
    r = ai._validate_and_fix(_hostile_card(), "t", "b")
    assert all(len(t) <= L.MAX_TAG_CHARS for t in r["topic_tags"])
    assert all(len(a) <= L.MAX_ITEM_CHARS for a in r["who_affected"])


def test_synthesis_content_is_capped():
    """It had a MIN_CONTENT_WORDS floor and no ceiling."""
    out = synthesis.parse_synthesis({
        "headline": "A reasonable synthesised headline",
        "content": HUGE,
        "extracted_entities": ["e" * 9_000],
        "fact_conflicts": ["c" * 9_000],
        "who_affected": ["w" * 9_000],
        "primary_source_attribution": "s" * 9_000,
    })
    assert len(out["content"]) <= L.MAX_BODY_CHARS
    assert len(out["primary_source_attribution"]) <= L.MAX_ITEM_CHARS
    for k in ("extracted_entities", "fact_conflicts", "who_affected"):
        assert all(len(x) <= L.MAX_ITEM_CHARS for x in out[k]), k


def test_the_synth_caps_name_the_parsers_own_return_keys():
    """`extracted_entities`, not `entities`. The clamp runs on the parsed
    contract after the parser has renamed, so a cap under the model's JSON key
    would silently apply to nothing."""
    out = synthesis.parse_synthesis({"headline": "h " * 3,
                                     "content": "word " * 40})
    for key in list(L.SYNTH_CAPS) + list(L.SYNTH_LIST_CAPS):
        assert key in out, key


def test_the_nested_structures_are_left_to_their_own_parsers():
    """`strings`, `dots` and `numbers_used` are lists of dicts whose parsers
    already bound them. A flat text clamp over them would DROP them, which is
    worse than not clamping — so they must be absent from the caps tables."""
    for key in ("strings", "dots", "numbers_used"):
        assert key not in L.SYNTH_CAPS and key not in L.SYNTH_LIST_CAPS
    out = synthesis.parse_synthesis({
        "headline": "h h h", "content": "word " * 40,
        "numbers_used": [{"value": "4%", "source": "RBI"}],
    })
    assert out["numbers_used"] == [{"value": "4%", "source": "RBI"}]


# ─── it cannot touch a legitimate value ──────────────────────────────────────
def test_a_body_at_the_top_of_the_longest_band_is_untouched():
    """LENGTH_BANDS tops out at 200 words (Research). The cap is ~6x that, so a
    clamp firing is evidence the PROMPT is wrong, not that the cap is tight."""
    body = "word " * 200
    assert L.clamp_text(body, L.MAX_BODY_CHARS, field="full_body") == body
    assert L.stats()["total"] == 0


def test_nothing_is_clamped_on_an_ordinary_card():
    r = ai._validate_and_fix({
        "refined_title": "RBI holds the repo rate steady as inflation cools",
        "summary": "The central bank kept its policy rate unchanged on Tuesday, "
                   "citing an easing inflation path and steady credit growth "
                   "across the banking system this quarter.",
        "full_body": "word " * 160,
        "category": "economy",
        "topic_tags": ["Markets", "Policy"],
        "what_info": "The repo rate was left at 6.5 percent.",
        "who_affected": ["borrowers", "banks"],
    }, "t", "b")
    assert L.stats()["total"] == 0
    assert r["full_body"] == "word " * 160


# ─── clamp, never reject ─────────────────────────────────────────────────────
def test_an_overrunning_response_still_publishes():
    """Rejecting would send the row back to its placeholder for the next tick to
    fetch and fail on identically — the forever-placeholder failure mode."""
    r = ai._validate_and_fix(_hostile_card(), "t", "b")
    assert r["full_body"] and r["full_body"] != ai._SAFE_BODY
    assert r["refined_title"]


def test_prose_is_cut_on_a_word_boundary():
    out = L.clamp_text("alpha bravo charlie delta echo", 20)
    assert not out.endswith(" ")
    assert " " in out and out in "alpha bravo charlie delta echo"
    assert out.split()[-1] in ("alpha", "bravo", "charlie")


def test_a_single_enormous_token_is_still_bounded():
    """The one case a word boundary cannot help with, and the one that must
    still be capped."""
    out = L.clamp_text("z" * 50_000, 100)
    assert len(out) == 100


def test_a_boundary_that_would_throw_away_the_budget_is_ignored():
    """A string whose only space is at character 3 must not be cut to 3."""
    out = L.clamp_text("ab " + "z" * 500, 100)
    assert len(out) > 90


# ─── shape, not just size ────────────────────────────────────────────────────
def test_a_nested_value_in_a_flat_list_is_dropped_not_stringified():
    """Storing the repr would put Python syntax on a card. It is not a value
    that was shortened; it is the wrong shape."""
    out = L.clamp_items([{"a": 1}, ["b"], "real", 42, None, True],
                        L.MAX_ITEM_CHARS, max_items=16)
    assert out == ["real", "42"]


def test_a_missing_field_is_never_invented():
    """Only keys PRESENT in the dict and NAMED in the caps table are touched."""
    r = L.clamp_card({"full_body": "x"})
    assert set(r) == {"full_body"}


def test_a_field_with_no_chosen_cap_is_not_clamped():
    """So nobody's new field starts getting truncated without them choosing a
    cap for it."""
    assert "a_new_field" not in L.CARD_CAPS
    r = L.clamp_card({"a_new_field": HUGE})
    assert len(r["a_new_field"]) == len(HUGE)


def test_a_non_dict_passes_through():
    assert L.clamp_card("nope") == "nope"
    assert L.clamp_card(None) is None


def test_none_becomes_an_empty_string_not_the_word_none():
    assert L.clamp_text(None, 10) == ""


# ─── the counters are the point, not the clamp ───────────────────────────────
def test_a_clamp_is_counted_and_the_worst_overrun_recorded():
    """`worst_seen_chars` is what distinguishes a prompt drifting past its band
    (8,100) from a provider ignoring the output ceiling (300,000)."""
    L.clamp_text(HUGE, L.MAX_BODY_CHARS, field="full_body")
    L.clamp_text("x" * 9_000, L.MAX_BODY_CHARS, field="full_body")
    st = L.stats()
    assert st["clamps"]["full_body"] == 2
    assert st["worst_seen_chars"]["full_body"] == 300_000
    assert "300000" in L.diagnose()


def test_an_unclamped_run_says_so_rather_than_reporting_nothing():
    assert "No model output has been clamped" in L.diagnose()
    assert "ceiling, not a formatter" in L.diagnose()


def test_the_counter_dict_cannot_grow_with_traffic():
    """Its keys are field names from the caps tables, so unlike a span name it
    is bounded by construction."""
    for _ in range(500):
        L.clamp_text(HUGE, 10, field="full_body")
    assert len(L.stats()["clamps"]) == 1


def test_a_clamp_without_a_field_name_is_not_counted():
    """clamp_text is also used as a plain helper; only a named field is a
    reportable event."""
    L.clamp_text(HUGE, 10)
    assert L.stats()["total"] == 0


# ─── the admin endpoint ──────────────────────────────────────────────────────
def test_admin_llm_bounds_is_guarded_and_opens_no_connection(monkeypatch):
    from fastapi.testclient import TestClient
    import main

    assert TestClient(main.app).get("/admin/llm-bounds").status_code in (401, 403)

    monkeypatch.setattr(main, "_check_admin", lambda t: None)
    monkeypatch.setattr(main, "get_db",
                        lambda: (_ for _ in ()).throw(AssertionError("no DB")))
    L.clamp_text(HUGE, 10, field="full_body")
    body = TestClient(main.app).get("/admin/llm-bounds").json()
    assert body["stats"]["clamps"]["full_body"] == 1
    assert body["caps"]["body"] == L.MAX_BODY_CHARS
    assert body["DIAGNOSIS"]
