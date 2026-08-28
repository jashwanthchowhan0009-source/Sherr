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
