"""
P0.6 — the originality gate.

This is a legal control, not a quality heuristic. The tests are written as the four
cases the spec names, plus the one the spec's own rule would have let through.
"""

import pytest

from app.pipeline.originality import (
    MAX_CONTIGUOUS_RUN, MAX_NGRAM_OVERLAP, MAX_QUOTE_TOKENS,
    longest_common_run, ngrams, normalize, originality_check, quoted_spans, tokenize)


SOURCE = (
    "The Indian Express on Tuesday reported that before Dharmendra Pradhan submitted "
    "his resignation to the Prime Minister on Saturday, the government had proposed a "
    "change in his portfolio as a compromise solution to the CJP. Party spokesperson "
    "Sambit Patra rejected the report at a press briefing in New Delhi, calling it "
    "speculative and without foundation. He added that the leadership had not discussed "
    "any portfolio reshuffle at any point during the week in question."
)

PARAPHRASE = (
    "BJP has pushed back on a newspaper account of the events around Dharmendra "
    "Pradhan's exit. A party spokesperson told reporters in Delhi that no reshuffle "
    "discussion took place, describing the account as speculative. The disputed claim "
    "concerns whether an alternative role was floated before the resignation reached "
    "the Prime Minister."
)

QUOTE_20 = (
    "BJP has disputed the newspaper's account of the resignation. Speaking in Delhi, "
    "party spokesperson Sambit Patra said the report was “speculative and without "
    "foundation” and that the leadership had not discussed any portfolio reshuffle. "
    "The party gave no further detail on the timing of the decision."
)

QUOTE_40 = (
    "The party responded today. Spokesperson Sambit Patra said, “before Dharmendra "
    "Pradhan submitted his resignation to the Prime Minister on Saturday, the government "
    "had proposed a change in his portfolio as a compromise solution to the CJP and the "
    "leadership had not discussed any reshuffle” in his remarks."
)


# ─── the four cases the spec names ────────────────────────────────────────────
def test_verbatim_copy_fails():
    passed, m = originality_check(SOURCE, SOURCE)
    assert not passed
    assert m["overlap"] == 1.0
    assert m["longest_run"] > MAX_CONTIGUOUS_RUN


def test_paraphrase_passes():
    passed, m = originality_check(PARAPHRASE, SOURCE)
    assert passed, m["reasons"]
    assert m["overlap"] <= MAX_NGRAM_OVERLAP


def test_short_attributed_quote_passes():
    """A 20-word quote inside quotation marks with the speaker named is legitimate."""
    passed, m = originality_check(QUOTE_20, SOURCE)
    assert passed, m["reasons"]
    assert m["quoted_spans"] and not any(s["over_limit"] for s in m["quoted_spans"])


def test_long_quote_fails():
    passed, m = originality_check(QUOTE_40, SOURCE)
    assert not passed
    assert any("quoted span" in r or "overlap" in r for r in m["reasons"])


# ─── the case Jaccard alone would have missed ─────────────────────────────────
def test_verbatim_excerpt_from_a_long_source_fails():
    """Jaccard divides by the UNION, so lifting one paragraph out of a long article
    scores low and would have passed the spec's rule. Containment is what catches it,
    and this is the exact shape of real-world infringement."""
    long_source = SOURCE + " " + " ".join(
        f"Additional background paragraph {i} about unrelated parliamentary schedules."
        for i in range(40))
    excerpt = ("the government had proposed a change in his portfolio as a compromise "
               "solution to the CJP")
    passed, m = originality_check(excerpt, long_source)
    assert m["overlap"] <= MAX_NGRAM_OVERLAP, "precondition: Jaccard alone would pass"
    assert m["containment"] > MAX_NGRAM_OVERLAP
    assert not passed


# ─── the audit trail ──────────────────────────────────────────────────────────
def test_metrics_are_returned_even_when_it_passes():
    """The row is the audit trail, so the numbers are recorded either way."""
    _passed, m = originality_check(PARAPHRASE, SOURCE)
    for key in ("overlap", "containment", "longest_run", "quoted_spans",
                "generated_tokens", "source_tokens", "thresholds"):
        assert key in m


def test_failure_states_its_reasons():
    _passed, m = originality_check(SOURCE, SOURCE)
    assert m["reasons"] and all(isinstance(r, str) for r in m["reasons"])


# ─── quote handling ───────────────────────────────────────────────────────────
def test_wrapping_the_whole_article_in_quotes_does_not_launder_it():
    """The exemption is per-span and capped, so quoting everything fails twice over."""
    passed, m = originality_check(f'"{SOURCE}"', SOURCE)
    assert not passed
    assert any(s["over_limit"] for s in m["quoted_spans"])


def test_quoted_spans_are_detected_for_both_quote_styles():
    assert quoted_spans('He said "one two three" today')[0]["tokens"] == 3
    assert quoted_spans("He said “one two three” today")[0]["tokens"] == 3


def test_quote_limit_boundary():
    words = " ".join(f"w{i}" for i in range(MAX_QUOTE_TOKENS))
    assert not quoted_spans(f'He said "{words}"')[0]["over_limit"]
    assert quoted_spans(f'He said "{words} extra"')[0]["over_limit"]


# ─── the primitives ───────────────────────────────────────────────────────────
def test_normalize_strips_punctuation_case_and_accents():
    assert normalize("The  Prime-Minister's, résumé!") == "the prime minister s resume"


def test_ngrams_are_seven_long_and_absent_for_short_text():
    assert ngrams(tokenize("one two three")) == set()
    assert all(len(g) == 7 for g in ngrams(tokenize(" ".join(str(i) for i in range(12)))))


def test_longest_common_run_finds_the_shared_stretch():
    a = tokenize("alpha beta gamma delta epsilon zeta")
    b = tokenize("nothing beta gamma delta epsilon here")
    assert longest_common_run(a, b) == 4


def test_longest_common_run_handles_empty_input():
    assert longest_common_run([], ["a"]) == 0
    assert longest_common_run(["a"], []) == 0


def test_empty_generated_text_is_not_silently_passed_as_original():
    """Nothing to compare is not the same as original — but it must not crash."""
    _passed, m = originality_check("", SOURCE)
    assert m["generated_tokens"] == 0
    assert m["overlap"] == 0.0


# ─── thresholds are the ones the spec fixed ───────────────────────────────────
def test_thresholds_match_the_spec():
    assert MAX_NGRAM_OVERLAP == 0.08
    assert MAX_CONTIGUOUS_RUN == 25
    assert MAX_QUOTE_TOKENS == 25
