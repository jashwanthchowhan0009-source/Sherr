"""
The Section-1 NEWS writer + Section-4 quality gate (SHERR_WRITING_SPEC.md).

These pin the gate rule-by-rule, the failure-handling runner (regenerate once,
then fall back to the safe summary), the shared rate limiter's two acquire modes,
and the writer-doctor aggregation. All pure/stubbed — no network, no DB.
"""

import asyncio
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import writer_gate as g          # noqa: E402
import ai_processor as ai        # noqa: E402
import rate_limit                # noqa: E402


SRC = ("The Reserve Bank of India kept the repo rate at 6.5 percent in 2023 and "
       "again on Tuesday, the monetary policy committee decided in Mumbai. "
       "Reuters reported the decision.")
HEADLINES = ["RBI keeps repo rate unchanged at 6.5%"]


def _good_card():
    return {
        "headline": "Reserve Bank held the repo rate steady on Tuesday morning",
        "body": (
            "The Reserve Bank of India kept its policy repo rate unchanged at 6.5 "
            "percent on Tuesday, the central bank decided in Mumbai. The monetary "
            "policy committee took the decision, citing an easing inflation path and "
            "steady bank credit across the wider economy. It is the fourth consecutive "
            "review to leave the rate at 6.5 percent, a level first reached in 2023."),
        "why_it_matters": (
            "The rate has now held at 6.5 percent for a fourth straight review, the "
            "longest such stretch since the 2023 tightening cycle drew to a close."),
        "numbers_used": [
            {"value": "6.5", "unit": "percent", "source_span": "6.5 percent"},
            {"value": "2023", "unit": "year", "source_span": "in 2023"},
        ],
        "entities": {"subject": "Reserve Bank of India", "affected": []},
        "primary_source_attribution": "Reuters",
    }


def _failed_rules(card, src=SRC, headlines=HEADLINES):
    r = g.run_gate(card, src, headlines)
    return sorted(rid for rid, p, _ in r.results if not p)


# ─── the good card clears every rule ─────────────────────────────────────────

def test_good_card_passes_all_seven_rules():
    r = g.run_gate(_good_card(), SRC, HEADLINES)
    assert r.passed, r.failures
    assert [rid for rid, _, _ in r.results] == g.RULE_IDS


def test_good_card_body_is_within_the_60_80_band():
    body = _good_card()["body"]
    assert 60 <= len(g._spec_words(body)) <= 80


def test_body_band_is_the_existing_news_length_band_not_60_110():
    # The reconciliation decision: the code's band wins, the spec was updated.
    assert (g.BODY_MIN_WORDS, g.BODY_MAX_WORDS) == g.LENGTH_BANDS["news"] == (60, 80)


# ─── G2_NUMBERS ──────────────────────────────────────────────────────────────

def test_g2_fails_on_a_number_missing_from_numbers_used():
    c = _good_card()
    c["numbers_used"] = [c["numbers_used"][0]]        # drop the 2023 entry
    assert "G2_NUMBERS" in _failed_rules(c)


def test_g2_fails_when_a_source_span_is_not_in_the_input():
    c = _good_card()
    c["numbers_used"][0]["source_span"] = "9.9 percent"   # not in SRC
    assert "G2_NUMBERS" in _failed_rules(c)


def test_g2_passes_when_the_card_has_no_numbers():
    c = _good_card()
    c["headline"] = "Reserve Bank held the repo rate steady again on Tuesday"
    c["body"] = ("The Reserve Bank of India kept its policy repo rate unchanged on "
                 "Tuesday, the central bank decided in Mumbai. The monetary policy "
                 "committee took the decision, citing an easing inflation path and "
                 "steady bank credit across the wider economy. It was the latest such "
                 "review to leave the benchmark rate exactly where it already stood.")
    c["why_it_matters"] = ("The benchmark rate is unchanged for another review, the "
                           "longest such stretch since the last tightening cycle drew "
                           "to a close.")
    c["numbers_used"] = []
    assert "G2_NUMBERS" not in _failed_rules(c)


# ─── G3_BANNED_TERMS + attribution exemption ─────────────────────────────────

def test_g3_fails_on_an_unattributed_banned_term():
    c = _good_card()
    c["why_it_matters"] = ("The rate held at 6.5 percent and the economy is likely to "
                           "keep easing over the coming year across every major sector.")
    assert "G3_BANNED_TERMS" in _failed_rules(c)


def test_g3_matches_inflections_whole_word():
    c = _good_card()
    # "forecasts" (inflection of forecast) — banned.
    c["body"] = c["body"].replace("It is the fourth", "The bank forecasts the fourth")
    assert "G3_BANNED_TERMS" in _failed_rules(c)


def test_g3_exempts_a_banned_term_inside_an_attributed_statement():
    c = _good_card()
    # Same forbidden word, but reported as what a named party said.
    c["body"] = (
        "The Reserve Bank of India kept its policy repo rate unchanged at 6.5 percent "
        "on Tuesday, the central bank decided in Mumbai. The governor said inflation is "
        "expected to ease further, citing steady bank credit across the wider economy. "
        "It is the fourth consecutive review to leave the rate at 6.5 percent since 2023.")
    assert "G3_BANNED_TERMS" not in _failed_rules(c)


def test_g3_does_not_trip_on_will_as_a_different_word():
    # "willing" is a different whole word from the modal "will".
    c = _good_card()
    c["body"] = c["body"].replace("citing an easing", "with members willing to cite an easing")
    assert "G3_BANNED_TERMS" not in _failed_rules(c)


# ─── G4_LENGTH ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("field,value,expect", [
    ("headline", "RBI holds rate", True),                       # 3 words < 6
    ("why_it_matters", "Rates held.", True),                    # 2 words < 12
])
def test_g4_fails_out_of_band_fields(field, value, expect):
    c = _good_card()
    c[field] = value
    assert ("G4_LENGTH" in _failed_rules(c)) is expect


def test_g4_fails_a_body_over_80_words():
    c = _good_card()
    c["body"] = c["body"] + " " + " ".join(["padding"] * 30) + "."
    assert "G4_LENGTH" in _failed_rules(c)


# ─── G5_WHY_GROUNDED ─────────────────────────────────────────────────────────

def test_g5_fails_when_why_introduces_a_new_number():
    c = _good_card()
    c["why_it_matters"] = ("The rate held at 6.5 percent, the fourth straight review and "
                           "a milestone unmatched in the prior 40 years of policy.")
    assert "G5_WHY_GROUNDED" in _failed_rules(c)


def test_g5_fails_when_why_names_an_entity_absent_from_body():
    c = _good_card()
    c["entities"] = {"subject": "Reserve Bank of India", "affected": ["Nirmala Sitharaman"]}
    c["why_it_matters"] = ("Nirmala Sitharaman noted the rate held at 6.5 percent for a "
                           "fourth straight review, the longest such stretch since 2023.")
    assert "G5_WHY_GROUNDED" in _failed_rules(c)


# ─── G6_HEADLINE_ECHO ────────────────────────────────────────────────────────

def test_g6_fails_on_a_five_word_run_shared_with_a_source_headline():
    c = _good_card()
    c["headline"] = "RBI keeps repo rate unchanged again"     # 5-run with the source
    assert "G6_HEADLINE_ECHO" in _failed_rules(c)


def test_g6_passes_a_four_word_overlap():
    c = _good_card()
    c["headline"] = "RBI keeps repo rate frozen for a fourth review"  # 4-run only
    assert "G6_HEADLINE_ECHO" not in _failed_rules(c)


# ─── G7_SHAPE ────────────────────────────────────────────────────────────────

def test_g7_fails_on_an_empty_required_field():
    c = _good_card()
    c["primary_source_attribution"] = ""
    assert _failed_rules(c) == ["G7_SHAPE"] or "G7_SHAPE" in _failed_rules(c)


def test_g7_is_the_only_failure_for_a_none_output():
    r = g.run_gate(None, SRC, HEADLINES)
    assert not r.passed
    assert [f["rule"] for f in r.failures] == ["G7_SHAPE"]


def test_g7_allows_empty_numbers_used_and_affected():
    c = _good_card()
    c["headline"] = "Reserve Bank held the benchmark rate steady again on Tuesday"
    c["body"] = ("The Reserve Bank of India kept its benchmark rate unchanged on Tuesday, "
                 "the central bank decided in Mumbai. The monetary policy committee took "
                 "the decision, citing an easing inflation path and steady bank credit "
                 "across the wider economy. It was the latest review to leave the "
                 "benchmark rate exactly where it already sat before the meeting.")
    c["why_it_matters"] = ("The benchmark rate is unchanged for another review, the "
                           "longest such stretch since the last tightening cycle ended.")
    c["numbers_used"] = []
    c["entities"] = {"subject": "Reserve Bank of India", "affected": []}
    assert "G7_SHAPE" not in _failed_rules(c)


# ─── the prompt is the spec's, verbatim, and asks for numbers_used ───────────

def test_news_prompt_is_verbatim_and_requests_numbers_used():
    p = ai.NEWS_PROMPT
    assert "You are the SherrByte NEWS writer." in p
    assert "numbers_used" in p and "source_span" in p
    assert "body: 60-80 words" in p          # code-matched band
    assert p.rstrip().endswith("SOURCE TEXT:")


# ─── failure handling: regenerate once, then _SAFE_SUMMARY ───────────────────

class _Lim:
    def __init__(self, spare=True):
        self.spare = spare
        self.acquired = 0
        self.tried = 0

    async def acquire(self):
        self.acquired += 1

    async def try_acquire(self):
        self.tried += 1
        return self.spare


def _writer_returning(*cards):
    seq = iter(cards)

    async def w(source_text, corrective=None):
        return next(seq), "fake/news-v1"
    return w


def _run(**kw):
    g.reset_attempts()
    return asyncio.run(g.process_source(
        article_id=kw.pop("article_id", 1), source_text=SRC,
        source_headlines=HEADLINES, **kw))


def test_first_pass_success_populates_why_and_records_one_attempt():
    res = _run(writer=_writer_returning(_good_card()), limiter=_Lim())
    assert res.ok and not res.fell_back
    assert res.why_it_matters
    rep = g.writer_doctor_report(24)
    assert rep["attempted"] == 1 and rep["passed"] == 1 and rep["regenerated"] == 0


def test_regeneration_recovers_a_first_pass_failure():
    bad = {"headline": "RBI holds rate", "body": "Too short.", "why_it_matters": "x",
           "numbers_used": [], "entities": {"subject": "RBI", "affected": []},
           "primary_source_attribution": "Reuters"}
    res = _run(writer=_writer_returning(bad, _good_card()), limiter=_Lim(spare=True))
    assert res.ok and not res.fell_back
    rep = g.writer_doctor_report(24)
    assert rep["attempted"] == 2 and rep["regenerated"] == 1 and rep["passed"] == 1


def test_two_failures_fall_back_to_safe_summary():
    bad = {"headline": "RBI holds rate", "body": "Too short.", "why_it_matters": "x",
           "numbers_used": [], "entities": {"subject": "RBI", "affected": []},
           "primary_source_attribution": "Reuters"}
    res = _run(writer=_writer_returning(bad, bad), limiter=_Lim(spare=True),
               safe_summary="SAFE")
    assert not res.ok and res.fell_back
    assert res.safe_summary == "SAFE"
    rep = g.writer_doctor_report(24)
    assert rep["fell_back_to_safe"] == 1 and rep["passed"] == 0


def test_saturated_limiter_skips_regeneration_and_falls_back():
    bad = {"headline": "RBI holds rate", "body": "Too short.", "why_it_matters": "x",
           "numbers_used": [], "entities": {"subject": "RBI", "affected": []},
           "primary_source_attribution": "Reuters"}
    lim = _Lim(spare=False)          # no spare token → no regeneration
    res = _run(writer=_writer_returning(bad), limiter=lim, safe_summary="SAFE")
    assert not res.ok and res.fell_back
    assert lim.tried == 1            # it asked, and was refused
    rep = g.writer_doctor_report(24)
    assert rep["attempted"] == 1 and rep["regenerated"] == 0 and rep["fell_back_to_safe"] == 1


# ─── the shared rate limiter ─────────────────────────────────────────────────

def test_try_acquire_refuses_once_the_bucket_is_empty():
    async def go():
        lim = rate_limit.RateLimiter(2)          # 2 tokens
        assert await lim.try_acquire() is True
        assert await lim.try_acquire() is True
        assert await lim.try_acquire() is False  # spent
    asyncio.run(go())


def test_acquire_refills_over_time_with_a_fake_clock():
    async def go():
        t = {"now": 1000.0}
        lim = rate_limit.RateLimiter(60, per=60.0, clock=lambda: t["now"])  # 1/sec
        assert await lim.try_acquire() is True
        for _ in range(59):
            await lim.try_acquire()
        assert await lim.try_acquire() is False  # bucket drained
        t["now"] += 1.0                          # one second → one token back
        assert await lim.try_acquire() is True
    asyncio.run(go())


# ─── writer-doctor aggregation shape (spec 5) ────────────────────────────────

def test_report_has_all_section_5_fields_and_buckets_by_rule_and_writer():
    g.reset_attempts()
    bad = {"headline": "RBI holds rate", "body": "Too short.", "why_it_matters": "x",
           "numbers_used": [], "entities": {"subject": "RBI", "affected": []},
           "primary_source_attribution": "Reuters"}
    gate = g.run_gate(bad, SRC, HEADLINES)
    g.record_attempt(article_id=7, writer_id="m/news-v1", attempt=1, gate=gate,
                     is_regen=False, fell_back=True)
    rep = g.writer_doctor_report(24)
    for k in ("window_hours", "attempted", "passed", "pass_rate", "regenerated",
              "fell_back_to_safe", "by_rule", "by_writer", "recent_failures"):
        assert k in rep
    assert set(rep["by_rule"]) == set(g.RULE_IDS)
    assert rep["by_rule"]["G4_LENGTH"] >= 1
    assert rep["by_writer"]["m/news-v1"]["attempted"] == 1
    assert rep["recent_failures"][0]["article_id"] == "7"
