"""
Reasoning Engine (Part 1) — pure logic, DB-free.

Two guarantees matter most and are asserted hard:
  1. The narrative is TEMPLATE-assembled from real fields (never invents a number).
  2. It uses observation language only — no forecasting, no causal claims.
"""

import pytest

from app.spie.reasoning.confidence import WEIGHTS, components, score
from app.spie.reasoning.narrative import (
    build_narrative, confidence_word, move_words, signed_pct,
    violates_language_rules, FORBIDDEN,
)
from app.spie.reasoning.engine import asset_class_of


# ─── asset-class agnosticism ───────────────────────────────────────────────────
def test_asset_class_parsed_uniformly_for_every_class():
    for cls in ["stocks", "crypto", "metals", "commodities", "forex", "rates"]:
        assert asset_class_of(f"yahoo:{cls}") == cls
    assert asset_class_of("") == "market"
    assert asset_class_of(None) == "market"
    assert asset_class_of("coingecko") == "coingecko"


# ─── narrative pieces ─────────────────────────────────────────────────────────
def test_move_words_and_signed_pct():
    assert move_words(1) == "rose" and move_words(-1) == "fell" and move_words(0) == "was flat"
    assert signed_pct(3.2, 1) == "+3.20%"
    assert signed_pct(-1.83, -1) == "-1.83%"


def test_confidence_word_bands():
    assert confidence_word(0.85) == "high"
    assert confidence_word(0.78) == "moderate"   # spec example: "moderate (78%)"
    assert confidence_word(0.6) == "moderate"
    assert confidence_word(0.35) == "limited"
    assert confidence_word(0.1) == "low"


# ─── the full narrative, from a realistic reasoned object ─────────────────────
_R = {
    "focal": {"type": "market_move", "instrument": "WTI Crude",
              "asset_class": "commodities", "move_pct": 3.2, "direction": 1},
    "window_hours": 24,
    "news_link": [{"cluster_headline": "Tanker traffic slows near Strait of Hormuz",
                   "entities": ["Iran", "Strait of Hormuz"],
                   "article_count": 8, "source_count": 6}],
    "connected": [{"entity": "Reliance Industries", "npmi": 0.62},
                  {"entity": "Indian Oil", "npmi": 0.55}],
    "cross_market": [{"instrument": "Gold", "asset_class": "metals",
                      "move_pct": 1.1, "direction": 1, "shared_entities": 2},
                     {"instrument": "USD/INR", "asset_class": "forex",
                      "move_pct": 0.4, "direction": 1, "shared_entities": 1}],
    "historical": {"similar_count": 3, "followed_direction": 2, "note": "2 of 3"},
    "evidence": {"sources": 6, "articles": 8, "clusters": 1},
    "confidence": 0.78,
}


def test_narrative_contains_every_section_from_real_fields():
    n = build_narrative(_R)
    assert "WTI Crude" in n and "commodities" in n and "+3.20%" in n     # the move
    assert "8 articles" in n and "6 sources" in n                        # the news
    assert "Iran" in n and "Strait of Hormuz" in n
    assert "Gold +1.10%" in n and "USD/INR +0.40%" in n                  # cross-market
    assert "3 markets" in n
    assert "Reliance Industries" in n                                    # connected
    assert "2 of the last 3 times" in n                                  # historical
    assert "moderate (78%)" in n                                         # confidence


def test_narrative_uses_observation_language_only():
    assert violates_language_rules(build_narrative(_R)) == []


def test_narrative_never_fabricates_missing_sections():
    """A thin window yields a SHORT honest narrative, not invented filler."""
    thin = {"focal": {"type": "market_move", "instrument": "Gold",
                      "asset_class": "metals", "move_pct": -0.9, "direction": -1},
            "window_hours": 48,
            "news_link": [], "connected": [], "cross_market": [],
            "historical": {"similar_count": 0, "followed_direction": 0},
            "evidence": {"sources": 0, "articles": 0}, "confidence": 0.2}
    n = build_narrative(thin)
    assert "Gold" in n and "-0.90%" in n
    assert "aligns with" not in n          # no cross-market claim without data
    assert "Connected to" not in n         # no connected claim without data
    assert "No comparable prior coverage" in n
    assert violates_language_rules(n) == []


@pytest.mark.parametrize("phrase", ["will rise", "causes", "predict", "expected to"])
def test_language_guard_catches_forecast_phrasing(phrase):
    assert violates_language_rules(f"Crude {phrase} tomorrow") != []


def test_forbidden_list_covers_the_stated_rules():
    for must in ["will ", "causes", "predict", "expect", "forecast"]:
        assert any(must in f for f in FORBIDDEN), must


# ─── confidence formula ───────────────────────────────────────────────────────
def test_weights_sum_to_one():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


def test_components_are_normalised():
    c = components(source_count=99, npmi_values=[2.0], similar_count=5,
                   followed_count=99, co_moving=99)
    assert all(0.0 <= v <= 1.0 for v in c.values())


def test_thin_history_is_penalised_not_rewarded():
    """One prior instance must not read as 100% consistency."""
    one = components(source_count=6, npmi_values=[0.6], similar_count=1,
                     followed_count=1, co_moving=1)
    many = components(source_count=6, npmi_values=[0.6], similar_count=4,
                      followed_count=4, co_moving=1)
    assert one["historical_consistency"] == 0.30
    assert many["historical_consistency"] == 1.0


def test_more_evidence_raises_confidence():
    weak = score(source_count=1, npmi_values=[0.1], similar_count=0,
                 followed_count=0, co_moving=0)
    strong = score(source_count=6, npmi_values=[0.8], similar_count=4,
                   followed_count=4, co_moving=3)
    assert 0.0 <= weak < strong <= 1.0


def test_cross_market_breadth_contributes():
    base = dict(source_count=4, npmi_values=[0.5], similar_count=3, followed_count=2)
    assert score(**base, co_moving=3) > score(**base, co_moving=0)
