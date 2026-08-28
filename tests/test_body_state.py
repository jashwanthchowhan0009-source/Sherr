"""
What is actually in an article body.

Three paths write articles.full_body and two leave something a reader should
never see: the publisher's raw RSS description at ingest, and the startup drain's
placeholder. The drain is what released the corpus — and it sets ai_processed=1,
the exact column run_ai_batch filters on, so every drained row became invisible
to the pass meant to rewrite it and kept the stub a reader opens.

These pin the classifier that finds those rows, and the rule that decides what a
rewrite is written FROM.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import body_state as bs  # noqa: E402

SOURCE = ("Delhi reported a sharp rise in air pollution on Tuesday as the air "
          "quality index crossed 400 at several monitoring stations across the "
          "capital region, prompting fresh advisories from the authorities.")
STUB = ("Sherr AI is preparing an original, plain-language summary of this story "
        "— the key facts, who is involved and why it matters will appear here "
        "shortly. Use the source link to read the full report at the original "
        "publisher.\n\nSource: The Hindu\nhttps://example.com/a")
ORIGINAL = ("Air quality in the capital worsened sharply this week. Officials "
            "logged readings past the severe threshold at multiple sites, and "
            "advisories now cover outdoor activity while construction curbs are "
            "under review across the wider region.")


def test_the_drain_stub_is_recognised_even_with_its_credit_line_appended():
    """The drain appends "Source: X" and a URL, so an equality check misses it."""
    assert bs.classify(STUB, SOURCE) == bs.STUB
    assert bs.is_stub(STUB) is True


def test_the_ai_processors_own_copy_of_the_stub_is_recognised_too():
    """ai_processor and main.py each carry a copy that differ by a sentence;
    matching on the shared opening clause catches both."""
    assert bs.is_stub("Sherr AI is preparing an original summary of this story.")


def test_the_publishers_own_prose_is_flagged_as_source_text():
    """The live copyright exposure — a body that is the publisher's words."""
    assert bs.classify(SOURCE, SOURCE) == bs.SOURCE_TEXT


def test_a_near_copy_of_the_truncated_source_is_still_source_text():
    """summary_60 stores only clean[:400], so a reproduced body is a near-copy of
    a prefix rather than an exact match — which is why this uses the originality
    gate rather than a string compare."""
    assert bs.classify(SOURCE[:200] + " Officials said more data was awaited.",
                       SOURCE) == bs.SOURCE_TEXT


def test_an_ai_written_body_is_original():
    assert bs.classify(ORIGINAL, SOURCE) == bs.ORIGINAL


def test_an_empty_or_too_short_body_is_not_counted_as_original():
    """A one-liner is not a summary; counting it as healthy would hide it."""
    assert bs.classify("", SOURCE) == bs.EMPTY
    assert bs.classify("Officials responded.", SOURCE) == bs.EMPTY


def test_every_rewriteable_state_is_in_the_needs_rewrite_set():
    assert set(bs.NEEDS_REWRITE) == {bs.SOURCE_TEXT, bs.STUB, bs.EMPTY}
    assert bs.ORIGINAL not in bs.NEEDS_REWRITE


# ─── what a rewrite reads from ───────────────────────────────────────────────
def test_the_stub_is_never_used_as_source_material():
    """The bug this exists to prevent: the existing reprocess pass fed full_body
    to the AI, and on a drained row full_body IS the stub — so it summarized its
    own placeholder and produced another one."""
    out = bs.source_material("Delhi air worsens", SOURCE, "", STUB)
    assert not bs.is_stub(out)
    assert SOURCE[:40] in out


def test_the_longest_surviving_publisher_text_is_preferred():
    """summary_60 holds clean[:400], source_summary only clean[:200]."""
    out = bs.source_material("H", SOURCE, SOURCE[:80], "")
    assert SOURCE[:120] in out


def test_a_raw_ingest_body_is_used_when_the_drain_never_reached_the_row():
    long_raw = SOURCE + " " + SOURCE
    out = bs.source_material("H", SOURCE[:100], "", long_raw)
    assert len(out) > len(SOURCE)


def test_the_headline_is_prepended_when_it_adds_something():
    assert bs.source_material("Delhi air worsens", SOURCE).startswith("Delhi air worsens")


def test_the_headline_is_not_repeated_when_the_body_already_carries_it():
    out = bs.source_material("Delhi reported a sharp rise", SOURCE)
    assert out.count("Delhi reported a sharp rise") == 1


def test_no_source_text_at_all_yields_something_empty_rather_than_the_stub():
    """A row with nothing to summarize must not be handed the placeholder as if
    it were material — the caller has to be able to tell there is nothing."""
    assert bs.source_material("", "", "", STUB).strip() == ""


# ─── the corpus audit ────────────────────────────────────────────────────────
class _Row(dict):
    def keys(self):  # sqlite3.Row surface used by classify_row
        return super().keys()


class _Conn:
    def __init__(self, rows): self._rows = rows
    def execute(self, *a, **k): return self
    def fetchall(self): return self._rows


def test_the_audit_counts_every_state_and_names_what_needs_rewriting():
    rows = [
        _Row(id=1, full_body=ORIGINAL, summary_60=SOURCE, source_summary="",
             status="published", reprocessed=1),
        _Row(id=2, full_body=STUB, summary_60=SOURCE, source_summary="",
             status="published", reprocessed=0),
        _Row(id=3, full_body=SOURCE, summary_60=SOURCE, source_summary="",
             status="published", reprocessed=0),
        _Row(id=4, full_body="", summary_60="", source_summary="",
             status="pending_rewrite", reprocessed=0),
    ]
    out = bs.audit(_Conn(rows))
    assert out["total"] == 4
    assert out["by_state"] == {bs.ORIGINAL: 1, bs.STUB: 1, bs.SOURCE_TEXT: 1,
                               bs.EMPTY: 1}
    # The unpublished row is not a reader-facing problem.
    assert out["needs_rewrite"] == 2
    assert out["healthy"] == 1


def test_the_candidate_query_targets_the_drains_own_fingerprint():
    """The drain sets ai_processed=1 but never reprocessed=1, so reprocessed=0 on
    a published row is exactly the set it released."""
    q = " ".join(bs.SELECT_NEEDING_REWRITE.split())
    assert "status='published'" in q
    assert "COALESCE(reprocessed,0)=0" in q
    assert "summary_60" in q and "source_summary" in q, \
        "the rewrite needs the surviving source text, not just full_body"


# ─── the stub the drain actually writes ──────────────────────────────────────
def test_both_placeholder_texts_are_recognised():
    """There are TWO stubs, written by different code paths, and the first
    version of this module knew only ai_processor's. The one the drain actually
    wrote across the corpus — "SherrByte has not yet published…" — classified as
    `original`, so the audit reported a healthy corpus and the rewrite skipped
    exactly the rows it existed to fix."""
    import sys as _s
    _s.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
    from publish_pending import STUB as DRAIN_STUB
    from ai_processor import _SAFE_BODY as AI_STUB

    assert bs.is_stub(DRAIN_STUB)
    assert bs.is_stub(AI_STUB)
    # As stored: the drain appends a credit line and a URL.
    assert bs.classify(DRAIN_STUB + "\n\nSource: The Hindu\nhttp://x", SOURCE) == bs.STUB


def test_the_markers_are_derived_from_the_stubs_themselves():
    """Retyping them here is how the two desynced in the first place."""
    import sys as _s
    _s.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
    from publish_pending import STUB as DRAIN_STUB
    assert any(m in " ".join(DRAIN_STUB.split()).lower() for m in bs._STUB_MARKERS)


# ─── the summary column (BUG 1) ──────────────────────────────────────────────
def _safe_summary():
    from ai_processor import _SAFE_SUMMARY
    return _SAFE_SUMMARY


def test_a_row_with_an_original_body_but_a_stub_summary_is_not_healthy():
    """The bug exactly: classify() judged full_body alone, so a rewritten body
    passed as `original` while summary_60 still held the placeholder — and
    summary_60 is what the Home card renders. The audit reported 17,017 healthy
    articles while every card said "Sherr AI is preparing an original summary"."""
    row = {"full_body": ORIGINAL, "summary_60": _safe_summary(),
           "source_summary": SOURCE}
    assert bs.classify_row(row) == bs.ORIGINAL          # the body really is fine
    assert bs.classify_row_summary(row) == bs.STUB      # the summary is not
    assert bs.row_is_healthy(row) is False              # so the row is not


def test_a_row_is_healthy_only_when_both_columns_are_original():
    row = {"full_body": ORIGINAL, "summary_60": "Readings passed the severe "
           "threshold at several sites; advisories now cover outdoor activity.",
           "source_summary": SOURCE}
    assert bs.row_is_healthy(row) is True


def test_an_empty_summary_is_caught_even_with_a_good_body():
    row = {"full_body": ORIGINAL, "summary_60": "", "source_summary": SOURCE}
    assert bs.classify_row_summary(row) == bs.EMPTY
    assert bs.row_is_healthy(row) is False


def test_a_short_summary_is_not_penalised_for_being_short():
    """A summary is SUPPOSED to be brief — the minimum-word rule that guards a
    body would reject every legitimate one."""
    row = {"full_body": ORIGINAL, "summary_60": "Air quality worsened sharply "
           "across the capital.", "source_summary": SOURCE}
    assert bs.classify_row_summary(row) == bs.ORIGINAL


def test_a_summary_copied_from_the_publisher_is_flagged():
    row = {"full_body": ORIGINAL, "summary_60": SOURCE, "source_summary": SOURCE}
    assert bs.classify_row_summary(row) == bs.SOURCE_TEXT
    assert bs.row_is_healthy(row) is False


def test_the_audit_reports_the_summary_column_separately():
    """Without its own block, a corpus of stub summaries is invisible."""
    rows = [
        _Row(id=1, full_body=ORIGINAL, summary_60=_safe_summary(),
             source_summary=SOURCE, status="published", reprocessed=1),
        _Row(id=2, full_body=ORIGINAL, summary_60="Readings passed the severe "
             "threshold at several sites this week.",
             source_summary=SOURCE, status="published", reprocessed=1),
    ]
    out = bs.audit(_Conn(rows))
    assert out["published_by_state"][bs.ORIGINAL] == 2   # both bodies fine
    assert out["summary_by_state"][bs.STUB] == 1         # one summary is not
    assert out["healthy"] == 1, "healthy must require BOTH columns"
    assert out["needs_rewrite"] == 1


def test_a_rewritten_summary_is_not_used_as_the_publisher_reference():
    """summary_60 starts as publisher text but the rewrite replaces it with
    ours. Using it as the reference would compare a fixed body against our own
    summary of it and could flag the article as a copy of itself."""
    row = {"full_body": ORIGINAL,
           "summary_60": ORIGINAL[:90],        # our summary, echoes our body
           "source_summary": SOURCE[:200]}     # the publisher's, untouched
    assert bs.classify_row(row) == bs.ORIGINAL


def test_the_publishers_text_is_still_caught_via_source_summary():
    row = {"full_body": SOURCE, "summary_60": "Our own short line about it.",
           "source_summary": SOURCE[:200]}
    assert bs.classify_row(row) == bs.SOURCE_TEXT
