"""
The Sherr-I pipeline: anomaly -> graph -> news -> card.

THE ARCHITECTURE RULE IS THE THING UNDER TEST. Deterministic math decides
whether a signal exists; the LLM is called only after one fires, and only to
write prose from a fixed payload. If the math is silent, no LLM call happens and
nothing is rendered.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "sherrbyte"))

from app.spie.discovery import news_match, tick_anomaly          # noqa: E402
from app.spie.graph import edges as E                            # noqa: E402
from app.spie.reasoning import card as C                         # noqa: E402

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def series(values):
    return [(T0 + timedelta(days=i), v) for i, v in enumerate(values)]


def flat(n=60, base=100.0):
    """Alternating hair-width moves: a real MAD, no real signal."""
    return [base + (0.01 if i % 2 else -0.01) for i in range(n)]


# ─── STEP 1: anomaly ─────────────────────────────────────────────────────────
def test_a_flat_series_produces_no_trigger():
    assert tick_anomaly.score_series("X", "stocks", series(flat())) is None


def test_a_single_injected_six_sigma_move_triggers():
    v = flat()
    v[-1] = v[-2] * 1.18
    r = tick_anomaly.score_series("X", "stocks", series(v))
    assert r is not None
    assert abs(r.z) >= tick_anomaly.Z_THRESHOLD
    assert r.direction == 1
    assert r.pct_change == pytest.approx(18.0, abs=0.5)


def test_a_twenty_point_series_returns_none_never_a_score():
    """A z from a short series is arithmetic, not evidence — and downstream a
    score is a reason to call an LLM and render a card."""
    v = flat(20)
    v[-1] = v[-2] * 1.2
    assert tick_anomaly.score_series("X", "stocks", series(v)) is None


def test_the_threshold_and_window_are_module_constants():
    assert tick_anomaly.Z_THRESHOLD == 2.5
    assert tick_anomaly.WINDOW == 30
    assert tick_anomaly.MIN_OBSERVATIONS == 40


def test_the_score_is_of_the_return_not_the_price_level():
    """Two series with identical returns but different price levels must score
    identically; a level-based score would not."""
    v = flat(); v[-1] = v[-2] * 1.15
    cheap = tick_anomaly.score_series("A", "stocks", series(v))
    dear = tick_anomaly.score_series("B", "stocks", series([x * 1000 for x in v]))
    assert cheap and dear
    assert cheap.z == pytest.approx(dear.z, abs=1e-6)


def test_the_scored_day_is_excluded_from_its_own_baseline():
    """A baseline containing the observation pulls toward it and understates
    the deviation."""
    v = flat(); v[-1] = v[-2] * 1.15
    r = tick_anomaly.score_series("X", "stocks", series(v))
    assert r.window_n == tick_anomaly.WINDOW


def test_a_perfectly_flat_window_is_skipped_not_divided_by_zero():
    v = [100.0] * 60
    assert tick_anomaly.score_series("X", "stocks", series(v)) is None


def test_a_downward_move_carries_direction_minus_one():
    v = flat(); v[-1] = v[-2] * 0.85
    r = tick_anomaly.score_series("X", "stocks", series(v))
    assert r.direction == -1 and r.pct_change < 0


def test_the_result_is_typed_and_serialisable():
    v = flat(); v[-1] = v[-2] * 1.15
    r = tick_anomaly.score_series("NIFTY", "stocks", series(v))
    d = r.as_dict()
    assert set(d) == {"symbol", "market_type", "value", "pct_change", "z",
                      "window_n", "direction", "ts"}
    assert isinstance(d["ts"], str)


# ─── STEP 2: graph ───────────────────────────────────────────────────────────
def test_the_seed_graph_covers_the_indian_market_at_the_stated_size():
    assert 60 <= len(E.SEED_EDGES) <= 100
    srcs = {s for s, *_ in E.SEED_EDGES}
    for expected in ("Brent Crude", "USD/INR", "Repo rate", "Gold", "Monsoon"):
        assert expected in srcs, expected


def test_crude_reaches_refiners_airlines_and_the_rupee():
    names = E.downstream_names("Brent Crude", 2)
    for expected in ("Oil marketing companies", "Airlines", "USD/INR"):
        assert expected in names, expected


def test_usdinr_reaches_it_exporters():
    assert "IT exporters" in E.downstream_names("USD/INR", 2)


def test_the_repo_rate_reaches_banks():
    assert "Banks" in E.downstream_names("Repo rate", 2)


def test_direction_is_the_sign_not_the_arrow():
    """Crude -> Airlines is `dampens`: fuel is a cost, so crude up is airlines
    down. The arrow is still source -> target."""
    hit = [e for e in E.SEED_EDGES
           if e[0] == "Brent Crude" and e[1] == "Airlines"][0]
    assert hit[3] == "dampens"


def test_traversal_is_cycle_safe():
    """Several pairs are deliberately mutual (NIFTY 50 <-> Sensex)."""
    got = E.traverse("NIFTY 50", 2)
    assert "NIFTY 50" not in [r["entity"] for r in got]
    assert len(got) == len({r["entity"] for r in got})


def test_depth_is_capped_at_two():
    assert E.traverse("Brent Crude", 9) == E.traverse("Brent Crude", 2)
    assert E.traverse("Brent Crude", 0) == []


def test_every_result_carries_the_path_that_reached_it():
    """The path IS the explanation a card renders."""
    for r in E.traverse("Brent Crude", 2):
        assert r["path"] and len(r["path"]) == r["hops"]
        assert E.describe_path(r).startswith("Brent Crude")


def test_every_seeded_direction_is_a_known_value():
    for _, _, _, direction, _ in E.SEED_EDGES:
        assert direction in ("amplifies", "dampens")


def test_every_seeded_edge_carries_a_note():
    """The note is the one line a card uses to say why the edge exists."""
    for src, tgt, _, _, note in E.SEED_EDGES:
        assert note.strip(), f"{src} -> {tgt}"


# ─── STEP 3: matcher ─────────────────────────────────────────────────────────
def test_a_placeholder_article_is_never_evidence():
    """The check nothing else had. A card citing four articles that all read
    "Sherr AI is preparing an original summary" cites nothing."""
    from ai_processor import _SAFE_SUMMARY
    assert news_match.is_real({"full_body": "x " * 200, "summary_60": _SAFE_SUMMARY,
                               "source_summary": "src"}) is False


def test_a_real_article_passes_the_stub_check():
    assert news_match.is_real({
        # >= 25 words: below that body_state treats a body as EMPTY, because a
        # one-liner is not a summary of anything.
        "full_body": ("Officials logged readings past the severe threshold at "
                      "several monitoring sites this week, and advisories now "
                      "cover outdoor activity while construction curbs are "
                      "under review across the wider capital region."),
        "summary_60": "Readings passed the severe threshold at several sites.",
        "source_summary": "Delhi reported a sharp rise in pollution on Tuesday.",
    }) is True


def test_short_terms_are_dropped_so_a_match_means_something():
    """"AI" or "Oil" alone would match half the corpus."""
    terms = news_match._terms(["AI", "Gold", "Brent Crude"])
    assert "ai" not in terms
    assert "gold" in terms and "brent crude" in terms


def test_overlap_counts_distinct_terms_across_headline_and_summary():
    row = {"headline": "Brent crude climbs as OPEC holds output",
           "summary_60": "Oil marketing companies slip on the news"}
    assert news_match.score_article(row, ["brent crude", "oil marketing companies"]) == 2


def test_the_window_is_twelve_hours_and_the_minimum_is_two_articles():
    assert news_match.WINDOW_HOURS == 12
    assert news_match.MIN_ARTICLES == 2


def test_no_terms_or_no_timestamp_returns_empty_without_querying():
    class _Boom:
        async def fetch(self, *a):
            raise AssertionError("must not query")
    assert asyncio.run(news_match.match(_Boom(), [], T0)) == []
    assert asyncio.run(news_match.match(_Boom(), ["Gold"], None)) == []


# ─── STEP 4: card ────────────────────────────────────────────────────────────
def _anomaly(z=3.0):
    return tick_anomaly.AnomalyResult(
        symbol="CL=F", market_type="commodities", value=86.4, pct_change=2.4,
        z=z, window_n=30, direction=1, ts=T0)


def _articles(n=2):
    return [{"title": f"Headline {i}", "source": f"Source {i}", "url": f"u{i}",
             "published_at": T0} for i in range(n)]


def test_no_llm_call_happens_when_the_math_is_silent():
    """The architecture rule. Silence is a valid output and it must be free."""
    C.reset_counters()
    assert asyncio.run(C.build(None, _articles(5), [], [])) is None
    assert C.LLM_CALLS["attempted"] == 0


def test_no_llm_call_happens_with_too_few_real_articles():
    C.reset_counters()
    assert asyncio.run(C.build(_anomaly(), _articles(1), [], [])) is None
    assert C.LLM_CALLS["attempted"] == 0


def test_the_llm_never_receives_raw_prices_or_the_series():
    payload = C.build_payload(_anomaly(), _articles(3), ["A -> B"])
    flat_text = str(payload).lower()
    assert "series" not in flat_text and "closes" not in flat_text
    assert set(payload) == {"symbol", "market_type", "direction", "pct_change",
                            "z_score", "baseline_days", "observed_on",
                            "headlines", "sources", "graph_paths"}


def test_the_prompt_forbids_prediction_and_causation():
    assert "predict" in C.PROMPT.lower()
    assert "causation" in C.PROMPT.lower()
    assert "already been established" in C.PROMPT.lower()


def test_signal_strength_is_bounded_and_monotonic():
    assert C.signal_strength(0, 0, 0) == 0
    assert C.signal_strength(99, 99, 99) == 100
    assert C.signal_strength(4, 3, 2) > C.signal_strength(2.5, 2, 1)


def test_signal_strength_is_never_called_a_confidence_or_a_percentage():
    """It is a ranking score. Printing it as a probability would invite reading
    "77" as "77% likely to be true", which it is not."""
    e = C.explain_signal_strength()
    assert "NOT a probability" in e["scale"]
    import inspect
    fields = C.DecisionCard.model_fields
    assert "signal_strength" in fields and "confidence" not in fields
    src = inspect.getsource(C.signal_strength)
    assert "confidence" not in src.lower()


def test_the_card_computes_its_own_evidence_and_score_not_the_llm():
    """No response, valid or not, can inflate the score or invent a citation."""
    card = C._coerce({"observation": "o", "what_to_watch": "w",
                      "signal_strength": 100, "evidence": [{"title": "INVENTED"}]},
                     _anomaly(2.5), _articles(2), ["WTI Crude"])
    assert [e.title for e in card.evidence] == ["Headline 0", "Headline 1"]
    assert card.signal_strength == C.signal_strength(2.5, 2, 2)


def test_a_response_missing_prose_is_retried_once_then_gives_up():
    C.reset_counters()
    calls = []

    async def empty(payload):
        calls.append(1)
        C.LLM_CALLS["attempted"] += 1
        return {"observation": "", "what_to_watch": ""}

    orig = C._ask_llm
    C._ask_llm = empty
    try:
        got = asyncio.run(C.build(_anomaly(), _articles(2), [], []))
    finally:
        C._ask_llm = orig
    assert got is None, "an unvalidated card must never be rendered"
    assert len(calls) == 2, "one retry, then stop"
    assert C.LLM_CALLS["failed"] == 1


def test_a_valid_response_produces_a_card_on_the_first_call():
    C.reset_counters()

    async def ok(payload):
        C.LLM_CALLS["attempted"] += 1
        return {"observation": "WTI Crude rose 2.4% alongside OPEC coverage.",
                "what_to_watch": "Whether refiners report margin pressure."}

    orig = C._ask_llm
    C._ask_llm = ok
    try:
        card = asyncio.run(C.build(_anomaly(), _articles(3), ["A -> B"], ["WTI Crude"]))
    finally:
        C._ask_llm = orig
    assert card is not None
    assert C.LLM_CALLS["attempted"] == 1 and C.LLM_CALLS["succeeded"] == 1
    assert len(card.evidence) == 3
    assert 0 <= card.signal_strength <= 100
