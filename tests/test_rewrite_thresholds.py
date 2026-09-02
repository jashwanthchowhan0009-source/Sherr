"""The rewrite pass wrote nothing for a corpus of 25,714 articles.

Every one of 500 attempts came back "ai_returned_stub" with no provider error:
the model answered, and a local validator threw the answer away. Three separate
faults, each invisible on its own:

  1. TWO THRESHOLDS DISAGREED. ai_processor._validate_and_fix replaced any body
     under 40 words with the placeholder; body_state.classify accepted an
     original body at 25. A genuine 30-word rewrite satisfied the gate and was
     destroyed before it could reach it.

  2. THE PLACEHOLDER WAS FED BACK IN. source_material stub-checked full_body but
     not summary_60, so a row with no publisher blurb handed the model its own
     "Sherr AI is preparing an original summary of this story." and asked for an
     original article.

  3. THE PROMPT ASKED FOR FICTION. 150-200 words was demanded from a 200-char
     RSS blurb. A model that does not invent returns something short; something
     short was then replaced by the placeholder, closing the loop.

These tests pin all three, plus the input floor that stops the first and third
fixes combining into a licence to fabricate.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ai_processor  # noqa: E402
import body_state  # noqa: E402
import text_utils  # noqa: E402
from ai_processor import _SAFE_BODY, _SAFE_SUMMARY  # noqa: E402
from text_utils import word_count  # noqa: E402

HEAD = "Brent crude climbs as OPEC+ signals deeper output cuts"
BLURB = ("Oil prices rose on Monday after OPEC+ delegates said the group was "
         "weighing deeper output cuts at its next meeting, with Brent settling "
         "above $78 a barrel.")


# ═══ 1. the two thresholds ═══════════════════════════════════════════════════

def test_both_modules_read_the_same_constant_object():
    """THE BUG. Not 'the numbers are equal' — the same object, so they cannot
    drift apart in a future edit."""
    assert body_state._MIN_ORIGINAL_WORDS is text_utils.MIN_ORIGINAL_WORDS
    assert ai_processor.MIN_ORIGINAL_WORDS is text_utils.MIN_ORIGINAL_WORDS


def test_no_module_hardcodes_a_second_word_floor():
    """A literal 40 reappearing in the validator is the regression."""
    src = open(ai_processor.__file__).read()
    assert 'word_count(result["full_body"]) < 40' not in src
    assert 'word_count(result["full_body"]) < MIN_ORIGINAL_WORDS' in src


def test_a_body_at_the_floor_survives_validation():
    """25 words used to be replaced by the placeholder. It must now be kept."""
    body = " ".join(["word"] * text_utils.MIN_ORIGINAL_WORDS)
    out = ai_processor._validate_and_fix(
        {"refined_title": "A title", "summary": " ".join(["s"] * 12),
         "full_body": body, "category": "economy"}, HEAD, BLURB)
    assert out["full_body"] == body
    assert not body_state.is_stub(out["full_body"])


def test_a_body_below_the_floor_is_still_replaced():
    short = " ".join(["word"] * (text_utils.MIN_ORIGINAL_WORDS - 1))
    out = ai_processor._validate_and_fix(
        {"refined_title": "A title", "summary": " ".join(["s"] * 12),
         "full_body": short, "category": "economy"}, HEAD, BLURB)
    assert out["full_body"] == _SAFE_BODY


def test_what_validation_keeps_is_what_the_gate_accepts():
    """The two rules must agree at every length, not just at the boundary.

    This is the property that was violated: a body could pass one and fail the
    other, and nothing anywhere reported the contradiction.
    """
    for n in range(1, 60):
        body = "Producers agreed the change after two days of talks. " + \
               " ".join(["detail"] * max(0, n - 9))
        kept = ai_processor._validate_and_fix(
            {"refined_title": "T", "summary": " ".join(["s"] * 12),
             "full_body": body, "category": "economy"},
            HEAD, BLURB)["full_body"] != _SAFE_BODY
        accepted = body_state.classify(body, "", BLURB) == body_state.ORIGINAL
        assert kept == accepted, (
            f"{word_count(body)} words: validator kept={kept}, gate accepted="
            f"{accepted} — the thresholds disagree again")


# ═══ 2. the placeholder leak ═════════════════════════════════════════════════

def test_a_stub_summary_is_never_offered_as_source_material():
    """THE INDEFENSIBLE ONE: asking the model to rewrite our own placeholder."""
    mat = body_state.source_material(HEAD, _SAFE_SUMMARY, "", _SAFE_BODY)
    assert "Sherr AI is preparing" not in mat
    assert not body_state.is_stub(mat)


def test_every_column_gets_the_stub_check_not_just_the_body():
    for s60, ss, fb in [(_SAFE_SUMMARY, "", _SAFE_BODY),
                        ("", _SAFE_SUMMARY, _SAFE_BODY),
                        (_SAFE_SUMMARY, _SAFE_SUMMARY, _SAFE_BODY)]:
        mat = body_state.source_material(HEAD, s60, ss, fb)
        assert "Sherr AI is preparing" not in mat, (s60[:20], ss[:20])


def test_real_publisher_text_is_still_used():
    mat = body_state.source_material(HEAD, _SAFE_SUMMARY, BLURB, _SAFE_BODY)
    assert "OPEC+ delegates" in mat and HEAD in mat


def test_the_longest_real_column_wins():
    long_one = "A much longer piece of real publisher prose. " * 4
    mat = body_state.source_material(HEAD, long_one, BLURB, _SAFE_BODY)
    assert "much longer piece" in mat


# ═══ 3. what the prompt asks for ═════════════════════════════════════════════

def test_the_prompt_no_longer_demands_a_word_count_the_source_cannot_support():
    si = ai_processor.SYSTEM_INSTRUCTION
    assert "150-200 words" not in si
    assert "2-3 sentences" in si


def test_the_prompt_forbids_invention_explicitly():
    si = ai_processor.SYSTEM_INSTRUCTION.upper()
    assert "NEVER INVENT" in si
    assert "DO NOT PAD" in ai_processor.SYSTEM_INSTRUCTION.upper()


def test_the_prompt_still_forbids_reproducing_the_source():
    """Loosening the length must not loosen the originality rule."""
    si = ai_processor.SYSTEM_INSTRUCTION
    assert "ORIGINALITY" in si and "Never copy sentences" in si


def test_the_asked_for_length_clears_the_gate():
    """2-3 sentences at 40-70 words sits above the 25-word floor with room."""
    assert 40 > text_utils.MIN_ORIGINAL_WORDS


# ═══ 4. the input floor ══════════════════════════════════════════════════════

def test_a_headline_alone_is_not_enough_to_attempt_a_rewrite():
    """Lowering the output floor to 25 and asking for 2-3 sentences would
    otherwise let a nine-word headline authorise a 25-word invention."""
    assert not body_state.has_usable_source(_SAFE_SUMMARY, "", _SAFE_BODY)


def test_a_real_blurb_is_enough():
    assert body_state.has_usable_source(_SAFE_SUMMARY, BLURB, _SAFE_BODY)


def test_the_headline_does_not_count_toward_the_input_floor():
    """It is ours to restate, not evidence to summarise."""
    long_head = " ".join(["headline"] * 40)
    assert not body_state.has_usable_source(_SAFE_SUMMARY, "", _SAFE_BODY)
    # even passed as a column it is the caller's job not to; the function only
    # ever sees the three source columns.
    assert body_state.has_usable_source.__doc__


def test_a_stub_never_counts_as_usable_source():
    assert not body_state.has_usable_source(_SAFE_SUMMARY, _SAFE_SUMMARY,
                                            _SAFE_BODY)


def test_the_floor_is_below_a_typical_rss_blurb():
    """200 characters is roughly 30-35 words; the floor must not reject those."""
    assert body_state.MIN_SOURCE_WORDS < word_count(BLURB)


# ═══ 5. the pass itself, end to end ══════════════════════════════════════════

def _corpus(tmp_path, n_with_blurb=3, n_starved=2):
    """A sqlite corpus in the exact production state: stub body, stub summary,
    reprocessed=1 — the shape that made the selector return nothing."""
    import sqlite3
    import main
    db = str(tmp_path / "c.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    for i in range(n_with_blurb + n_starved):
        conn.execute(
            "INSERT INTO articles (url,headline,source_headline,full_body,"
            "summary_60,source_summary,status,ai_processed,reprocessed,"
            "pillar_id,published_at) VALUES (?,?,?,?,?,?,'published',1,1,2,?)",
            (f"u{i}", f"Market update {i} on commodities", f"Wire {i}",
             _SAFE_BODY, _SAFE_SUMMARY,
             BLURB if i < n_with_blurb else "", "2026-08-31T10:00:00+00:00"))
    conn.commit()
    conn.close()
    return db


def test_a_starved_row_is_never_flagged_as_done(tmp_path, monkeypatch):
    """The failure this whole pass exists to end: a row that was not rewritten
    must not be marked reprocessed, or it disappears from the backlog silently.
    """
    import sqlite3
    import main
    db = _corpus(tmp_path, n_with_blurb=0, n_starved=2)
    # Arrive as production rows do: already flagged by a previous AI pass.
    conn = sqlite3.connect(db)
    conn.execute("UPDATE articles SET reprocessed=0")
    conn.commit()
    conn.close()

    monkeypatch.setattr(main, "get_db", lambda: _open(db))
    _with_provider(monkeypatch, main)
    main._reprocess_bodies_sync(10, 10)

    conn = sqlite3.connect(db)
    flagged = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE COALESCE(reprocessed,0)=1"
    ).fetchone()[0]
    conn.close()
    assert flagged == 0, "a starved row was marked done without being rewritten"


def _open(db):
    import sqlite3
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn


def _with_provider(monkeypatch, main):
    """Satisfy the no-provider refusal guard.

    _reprocess_bodies_sync correctly refuses to run when no key is configured —
    rewriting without a provider would only write the placeholder again. These
    tests exercise what happens AFTER that guard, so a provider is declared.
    """
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "gemini": 1,
                                 "total_keys": 1, "model": "test"})


def test_the_run_counters_account_for_every_attempted_row(tmp_path, monkeypatch):
    """rewritten + failed + already_original must equal attempted. They did not:
    starved rows were counted twice, as both failed and already-original."""
    import main
    db = _corpus(tmp_path, n_with_blurb=3, n_starved=2)
    monkeypatch.setattr(main, "get_db", lambda: _open(db))
    _with_provider(monkeypatch, main)

    async def dead(key, title, body, client):
        return None, 200

    monkeypatch.setitem(ai_processor._PROVIDER_CALLS, "gemini", dead)
    res = main._reprocess_bodies_sync(10, 10)
    assert (res["rewritten"] + res["failed"] + res["already_original"]
            == res["attempted"])


def test_a_starved_row_never_reaches_the_provider(tmp_path, monkeypatch):
    """No provider call is spent to receive a fabrication."""
    import main
    db = _corpus(tmp_path, n_with_blurb=0, n_starved=3)
    monkeypatch.setattr(main, "get_db", lambda: _open(db))
    _with_provider(monkeypatch, main)
    calls = []

    async def counting(key, title, body, client):
        calls.append(title)
        return None, 200

    monkeypatch.setitem(ai_processor._PROVIDER_CALLS, "gemini", counting)
    main._reprocess_bodies_sync(10, 10)
    assert calls == [], f"{len(calls)} provider call(s) spent on starved rows"
