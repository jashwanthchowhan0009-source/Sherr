"""
News-keyword ↔ market-instrument links, and the funnel diagnostics.

The bug these guard against: market instruments enter domain_signals as their own
entity ("WTI Crude") while news talks about "Iran" and "OPEC". Nothing joined the
two, so every reasoned card died at "no related news" and the detector reported a
bare 0 that was indistinguishable from a wiring fault.
"""

import pytest

from app.spie.discovery import market_reaction
from app.spie.knowledge import instrument_map
from app.spie.knowledge.entity_resolver import normalize_name
from app.spie.reasoning import engine as reasoning_engine
from app.spie.reasoning.narrative import build_narrative, violates_language_rules, window_phrase
from app.workers.market_signals import CRYPTO, INSTRUMENTS


# ─── the seed covers what the market worker actually tracks ───────────────────
def test_every_tracked_instrument_has_keyword_mappings():
    """A tracked instrument with no mapping can never produce a reasoned card —
    it would be silently invisible rather than reported as unmapped."""
    tracked = {name for _sym, name, _cls in INSTRUMENTS} | set(CRYPTO.values())
    assert tracked - set(instrument_map.SEED) == set()


def test_seed_rows_are_deduplicated_and_normalized():
    rows = instrument_map.seed_rows()
    assert rows
    keys = [(i, n) for i, _k, n in rows]
    assert len(keys) == len(set(keys))                  # no duplicate links
    for _instrument, keyword, norm in rows:
        assert norm == normalize_name(keyword) and norm


def test_mappings_are_scoped_per_instrument():
    """The old entity_ticker_map lookup was unscoped — every mapped entity was
    related to every instrument. Oil keywords must not reach crypto."""
    assert "Iran" in instrument_map.SEED["WTI Crude"]
    assert "Iran" not in instrument_map.SEED["Bitcoin"]
    assert "OPEC" not in instrument_map.SEED["Ethereum"]


def test_seed_synonyms_collapse_to_the_same_key_news_uses():
    """'RBI' in a seed must land on the same normalized key that ingestion
    produces for 'Reserve Bank of India', or the join silently finds nothing."""
    keys = instrument_map._lookup_keys([
        {"keyword": "RBI", "norm": "rbi"},
        {"keyword": "Reliance", "norm": "reliance"},
    ])
    assert "reserve bank of india" in keys
    assert "reliance industries" in keys


def test_lookup_keys_drop_empties_and_dedupe():
    keys = instrument_map._lookup_keys([
        {"keyword": "OPEC", "norm": "opec"},
        {"keyword": "OPEC", "norm": "opec"},
        {"keyword": "", "norm": ""},
    ])
    assert keys == ["opec"]


# ─── window honesty ───────────────────────────────────────────────────────────
@pytest.mark.parametrize("hours,expected", [
    (24, "24h"), (36, "36h"), (48, "2 days"), (72, "3 days"), (168, "7 days"),
])
def test_window_phrase_is_human(hours, expected):
    assert window_phrase(hours) == expected


def _narrative(window_hours):
    return build_narrative({
        "focal": {"type": "market_move", "instrument": "WTI Crude",
                  "asset_class": "commodities", "move_pct": 3.2, "direction": 1},
        "window_hours": window_hours,
        "news_link": [{"article_count": 8, "source_count": 6,
                       "entities": ["Iran", "OPEC"]}],
        "historical": {"similar_count": 0, "followed_direction": 0},
        "evidence": {"articles": 8, "sources": 6}, "confidence": 0.41,
    })


def test_wide_window_is_disclosed_not_hidden():
    """Widening the window finds more overlap but says less about sequence. The
    card has to admit that rather than letting '7 days' read as tight timing."""
    wide = _narrative(168)
    assert "7 days" in wide and "wide window" in wide
    tight = _narrative(24)
    assert "24h" in tight and "wide window" not in tight


def test_wide_window_narrative_still_obeys_the_language_contract():
    assert violates_language_rules(_narrative(168)) == []


# ─── funnel diagnostics: 0 must be explainable ────────────────────────────────
def test_reasoning_diagnosis_names_the_blocking_stage():
    d = reasoning_engine._diagnose
    assert "no market signals" in d(
        {"insights_written": 0, "instruments_with_history": 0, "significant_moves": 0,
         "with_connected_entities": 0, "with_related_news": 0})
    assert "none moved" in d(
        {"insights_written": 0, "instruments_with_history": 11, "significant_moves": 0,
         "with_connected_entities": 0, "with_related_news": 0})
    assert "instrument_keywords" in d(
        {"insights_written": 0, "instruments_with_history": 11, "significant_moves": 3,
         "with_connected_entities": 0, "with_related_news": 0})
    assert "no-overlap" in d(
        {"insights_written": 0, "instruments_with_history": 11, "significant_moves": 3,
         "with_connected_entities": 3, "with_related_news": 0})
    assert "written" in d(
        {"insights_written": 2, "instruments_with_history": 11, "significant_moves": 3,
         "with_connected_entities": 3, "with_related_news": 3})


def test_market_reaction_diagnosis_distinguishes_no_mapping_from_no_overlap():
    d = market_reaction._diagnose
    no_mapping = d({"insights_written": 0, "instruments": 13, "with_enough_history": 13,
                    "significant_moves": 2, "with_related_entities": 0,
                    "with_related_news": 0}, 5, 2)
    no_overlap = d({"insights_written": 0, "instruments": 13, "with_enough_history": 13,
                    "significant_moves": 2, "with_related_entities": 2,
                    "with_related_news": 0}, 5, 2)
    assert "instrument_keywords" in no_mapping
    assert "no-overlap" in no_overlap
    assert no_mapping != no_overlap


def test_market_reaction_diagnosis_reports_thin_history():
    msg = market_reaction._diagnose(
        {"insights_written": 0, "instruments": 13, "with_enough_history": 0,
         "significant_moves": 0, "with_related_entities": 0,
         "with_related_news": 0}, 5, 2)
    assert "days of history" in msg
