"""
Tier 1 observation cards.

These exist so the app always has something honest to show. The risk is that
"always has something" quietly becomes "overstates what it has" — so the tests here
are mostly about what a Tier 1 card is NOT allowed to claim.
"""

import pytest

from app.spie.discovery import observation as obs
from app.spie.knowledge.entity_resolver import is_valid_mention
from app.spie.reasoning.narrative import build_narrative, violates_language_rules


# ─── ranking is honest about which statistic it used ──────────────────────────
def test_ranks_by_z_when_a_baseline_exists():
    assert obs.rank_key(1.2, 2.4) == (2.4, "mad_z")


def test_falls_back_to_raw_move_without_a_baseline():
    """One daily bucket cannot produce a z. Ranking still works, and says so."""
    assert obs.rank_key(-3.4, None) == (3.4, "abs_pct_change")


def test_baseline_note_flags_a_thin_baseline_as_provisional():
    assert obs.baseline_note(1) == "provisional baseline, n=1"
    assert obs.baseline_note(3) == "provisional baseline, n=3"


def test_baseline_note_states_n_even_when_adequate():
    """A statistic is never shown without the n it rests on."""
    assert obs.baseline_note(5) == "baseline n=5"
    assert "n=" in obs.baseline_note(12)


def test_no_baseline_note_when_no_z_was_computed():
    assert obs.baseline_note(None) is None


def test_provisional_threshold_is_below_the_full_one():
    assert obs.MIN_BUCKETS_FOR_Z < obs.PROVISIONAL_BASELINE_POINTS


# ─── junk entities never reach a card ─────────────────────────────────────────
def test_junk_entities_are_stripped_from_cards():
    rows = [{"entity": "Reliance Industries"}, {"entity": "It's"}, {"entity": "One"},
            {"entity": "Nifty"}, {"entity": "Will"}, {"entity": "Don't"}]
    kept = [r["entity"] for r in obs.clean_entities(rows)]
    assert kept == ["Reliance Industries", "Nifty"]


def test_contractions_are_not_entities():
    """The apostrophe-compound rule for 'Jean-Pierre' was matching every English
    contraction, which is how "It's" became a named entity."""
    for junk in ("It's", "Don't", "That's", "We've", "They're", "You're"):
        assert not is_valid_mention(junk), junk


def test_real_apostrophe_names_still_resolve():
    for real in ("O'Brien", "L'Oreal", "Moody's", "McDonald's", "Jean-Pierre",
                 "Spider-Man", "Coca-Cola"):
        assert is_valid_mention(real), real


def test_number_words_are_not_entities():
    for junk in ("One", "Two", "Three", "Half", "Another", "Every"):
        assert not is_valid_mention(junk), junk


def test_clean_entities_drops_blanks():
    assert obs.clean_entities([{"entity": ""}, {"entity": None}, {}]) == []


# ─── the narrative stays short and claims nothing it lacks ────────────────────
def _card(cross=(), connected=()):
    c = {"focal": {"type": "market_move", "instrument": "Sensex",
                   "asset_class": "stocks", "move_pct": 2.11, "direction": 1},
         "window_hours": 48,
         "news_link": [{"cluster_headline": "Markets climb", "article_count": 24,
                        "source_count": 6, "entities": ["Reliance Industries", "Nifty"]}],
         "connected": list(connected), "cross_market": list(cross),
         "historical": {"similar_count": 0, "followed_direction": 0},
         "lag": {"passed": False, "reason": "Tier 1 uses today's data only"},
         "evidence": {"sources": 6, "articles": 24, "clusters": 1},
         "confidence": 0.33}
    c["narrative"] = build_narrative(c, concise=True)
    return c


def test_concise_narrative_is_short():
    """Tier 1 is a glanceable card, not a report."""
    n = _card()["narrative"]
    assert 2 <= n.count(". ") + 1 <= 4


def test_concise_narrative_omits_history_it_does_not_have():
    """The full builder ends with 'No comparable prior coverage on record yet.'
    Tier 1 has no history by construction, so stating its absence is noise."""
    n = _card()["narrative"]
    assert "prior coverage" not in n
    assert "rank correlation" not in n
    assert "Connected to:" not in n


def test_concise_narrative_still_reports_cross_market():
    n = _card(cross=[{"instrument": "Gold", "asset_class": "metals",
                      "move_pct": 1.32, "direction": 1}])["narrative"]
    assert "Gold" in n and "aligns with" in n


def test_entities_are_not_repeated_in_the_narrative():
    """Every news link carries the same connected-entity list; flattening without
    dedup produced 'covered Reliance, Nifty and Reliance'."""
    c = _card()
    c["news_link"] = c["news_link"] * 3
    n = build_narrative(c, concise=True)
    assert n.count("Reliance Industries") == 1


def test_narrative_obeys_the_language_contract():
    assert violates_language_rules(_card()["narrative"],
                                   entity_names=["Sensex", "Nifty"]) == []


def test_full_mode_still_reports_history():
    """Concise must not change Tier 2 behaviour."""
    assert "prior coverage" in build_narrative(_card(), concise=False)


# ─── registered so it actually runs ───────────────────────────────────────────
def test_observation_is_in_the_detector_registry():
    from app.spie.discovery import REGISTRY
    assert "observation" in REGISTRY


def test_observation_runs_before_the_history_dependent_detectors():
    """Ordering matters for the Sherr-I tab: Tier 1 is the one detector guaranteed to
    have something to say on a young corpus."""
    from app.spie.discovery import REGISTRY
    assert list(REGISTRY)[0] == "observation"
