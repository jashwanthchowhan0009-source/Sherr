"""
Interpretation layer + the investment-advice guard.

Two separate risks. First, the card said what happened and never what it meant.
Second — and this is the one with legal weight — an interpretation is one careless
template away from reading as investment advice, which in India is a regulated
activity under the SEBI Investment Advisers Regulations. The guard is enforced at
runtime, not just asserted here: a violation drops the insight.
"""

import pytest

from app.spie.reasoning import interpretation as I
from app.spie.reasoning.narrative import (
    DISCLAIMER, FORBIDDEN_ADVICE, PRICE_TARGET_RE, violates_language_rules)


def _r(focal_class, direction, cross, confidence=0.6, lag=None, hist=None,
       instrument="Sensex"):
    return {"focal": {"instrument": instrument, "asset_class": focal_class,
                      "direction": direction, "move_pct": 2.0},
            "cross_market": cross, "confidence": confidence,
            "lag": lag or {"passed": False},
            "historical": hist or {"similar_count": 0, "followed_direction": 0}}


def _cm(cls, direction):
    return {"instrument": cls.title(), "asset_class": cls,
            "direction": direction, "move_pct": 1.0}


# ─── every template is legally safe ───────────────────────────────────────────
def test_no_template_contains_forbidden_language():
    """The templates are the only prose in the system. If one of them is unsafe,
    every card carrying that pattern is unsafe."""
    fields = dict(instrument="Sensex", classes="equities and metals", n_markets=3,
                  n_others=2, market_word="markets", n_up=2, n_down=1,
                  lag_days="about 2 days", rho=0.66, similar=3, followed=2)
    for key, (title, body) in I.TEMPLATES.items():
        text = f"{title}. {body.format(**fields)}"
        assert violates_language_rules(text, entity_names=["Sensex"]) == [], key


def test_weak_prefixed_templates_are_also_safe():
    fields = dict(instrument="Sensex", classes="equities", n_markets=2, n_others=1,
                  market_word="market", n_up=1, n_down=1, lag_days="the same day",
                  rho=0.5, similar=2, followed=1)
    for key, (_title, body) in I.TEMPLATES.items():
        b = body.format(**fields)
        text = I.WEAK_PREFIX + b[0].lower() + b[1:]
        assert violates_language_rules(text, entity_names=["Sensex"]) == [], key


# ─── the guard actually bites ─────────────────────────────────────────────────
@pytest.mark.parametrize("phrase", [
    "Investors should buy Sensex here.",
    "A good opportunity to sell.",
    "We recommend a target of 82,000.",
    "This is bullish for equities.",
    "Expect a rally in gold.",
    "Take a position in crude.",
    "Set a stop loss below this level.",
    "Allocate more to metals.",
    "Undervalued at these levels.",
])
def test_advice_language_is_blocked(phrase):
    assert violates_language_rules(phrase) != []


@pytest.mark.parametrize("phrase", ["target of ₹82,000", "$95 price", "Rs 1,200",
                                    "INR 500 level", "usd 42.50"])
def test_price_levels_are_blocked(phrase):
    assert PRICE_TARGET_RE.search(phrase) is not None


def test_advice_terms_match_on_word_boundaries():
    """'buyer' and 'sells' in a quoted headline must not trip the guard, or real
    coverage becomes unreportable."""
    assert violates_language_rules("A buyer emerged and the seller withdrew.") == []
    assert violates_language_rules("Buy now.") != []


def test_entity_names_are_masked_before_scanning():
    """Real names collide with the blocklist. Dropping a valid insight because a
    company is called Target is a silent failure."""
    text = "Connected to: Will Smith and Target Corporation."
    assert violates_language_rules(text) != []            # unmasked, trips
    assert violates_language_rules(
        text, entity_names=["Will Smith", "Target Corporation"]) == []


def test_flat_currency_pair_does_not_read_as_a_price_target():
    """'USD/INR 0.00%' matches the price regex until the instrument name is masked."""
    assert violates_language_rules("USD/INR 0.00% was flat.",
                                   entity_names=["USD/INR"]) == []


def test_disclaimer_text_is_fixed():
    assert DISCLAIMER == ("SherrByte provides market intelligence, "
                          "not investment advice.")


def test_forbidden_advice_covers_the_named_terms():
    for w in ("buy", "sell", "recommend", "target", "invest"):
        assert w in FORBIDDEN_ADVICE


# ─── classification ───────────────────────────────────────────────────────────
@pytest.mark.parametrize("expected,r", [
    ("broad_defensive",    _r("metals", 1, [_cm("stocks", -1)], instrument="Gold")),
    ("broad_risk_seeking", _r("stocks", 1, [_cm("crypto", 1)])),
    ("defensive_bid",      _r("metals", 1, [_cm("forex", 1)], instrument="Gold")),
    ("energy_led",         _r("commodities", 1, [_cm("metals", 1)])),
    ("currency_rates",     _r("forex", 1, [_cm("rates", 1)])),
    ("divergent",          _r("stocks", 1, [_cm("forex", 1), _cm("commodities", -1)])),
    ("broad_aligned",      _r("stocks", 1, [_cm("metals", 1)])),
    ("news_led",           _r("stocks", 1, [], lag={"passed": True, "lag": 2, "rho": 0.6})),
    ("recurring",          _r("stocks", 1, [],
                              hist={"similar_count": 3, "followed_direction": 2})),
    ("isolated",           _r("stocks", 1, [])),
])
def test_classify_maps_shape_to_pattern(expected, r):
    assert I.classify(r) == expected


def test_two_aligned_markets_are_a_pattern_not_a_fallthrough():
    """Requiring three co-movers left the commonest real case reading as 'not yet a
    pattern' when two markets had plainly moved together."""
    assert I.classify(_r("stocks", 1, [_cm("metals", 1)])) == "broad_aligned"


# ─── weak evidence names the shape but does not assert it ─────────────────────
def test_weak_confidence_keeps_the_shape_and_flags_it():
    r = _r("stocks", 1, [_cm("metals", 1)], confidence=0.2)
    out = I.interpret(r)
    assert out["pattern"] == "broad_aligned"      # shape is a fact from the data
    assert out["established"] is False
    assert out["text"].startswith(I.WEAK_PREFIX)


def test_strong_confidence_states_the_shape_plainly():
    out = I.interpret(_r("stocks", 1, [_cm("metals", 1)], confidence=0.7))
    assert out["established"] is True
    assert not out["text"].startswith(I.WEAK_PREFIX)


def test_interpretation_never_names_a_direction_to_come():
    for r in (_r("metals", 1, [_cm("stocks", -1)], instrument="Gold"),
              _r("stocks", 1, [_cm("crypto", 1)]),
              _r("commodities", 1, [_cm("metals", 1)])):
        text = I.interpret(r)["text"].lower()
        for w in ("will", "next week", "tomorrow", "going to"):
            assert w not in text
