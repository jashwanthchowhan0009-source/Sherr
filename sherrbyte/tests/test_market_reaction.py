"""
Part C: the news↔market detector's pure logic + the language rule.

The language rule is a product/legal constraint, not a style preference: Sherr-I
reports an OBSERVED SEQUENCE. It must never claim causation ("caused", "because
of") or make a forecast ("will", "expect", "predict").
"""

import inspect

from app.spie.discovery import REGISTRY, market_reaction
from app.spie.discovery.anomaly_math import ewma, mad, mad_zscore
from app.spie.discovery.market_reaction import move_phrase, relation_phrase


# ─── registration ─────────────────────────────────────────────────────────────
def test_detector_is_registered():
    assert "market_reaction" in REGISTRY


# ─── language rule (hard requirement) ─────────────────────────────────────────
_BANNED = ["caused", "causes", "causing", "because of", "will move", "will rise",
           "will fall", "predict", "forecast", "expected to", "should move"]


def test_move_phrase_is_descriptive_only():
    for direction, pct in [(1, 2.4), (-1, 1.8), (0, 0.0)]:
        text = move_phrase(direction, pct).lower()
        assert not any(b in text for b in _BANNED), text
    assert "rose" in move_phrase(1, 2.4)
    assert "fell" in move_phrase(-1, 1.8)


def test_relation_phrase_is_sequence_not_causation():
    for mode in ["news_then_move", "move_then_news"]:
        text = relation_phrase(mode).lower()
        assert not any(b in text for b in _BANNED), text
    assert relation_phrase("news_then_move") == "preceded the move"
    assert relation_phrase("move_then_news") == "followed the move"


_NEGATIONS = ["not a causal claim", "not a forecast", "not causation",
              "not a prediction"]


def _emitted_text(func):
    """Source of `func` minus its docstring, comments and explicit DISCLAIMERS —
    i.e. only the claims that can reach explain_json.

    Docstrings quote the banned words to state the rule, and the disclaimer itself
    legitimately contains "forecast" ("...not a forecast"); scanning either would
    be a false positive."""
    src = inspect.getsource(func)
    if func.__doc__:
        src = src.replace(func.__doc__, "")
    text = "\n".join(l.split("#")[0] for l in src.splitlines()).lower()
    for neg in _NEGATIONS:
        text = text.replace(neg, "")
    return text


def test_emitted_text_has_no_causal_or_predictive_language():
    for func in (market_reaction.run, move_phrase, relation_phrase):
        text = _emitted_text(func)
        for phrase in _BANNED:
            assert phrase not in text, f"{func.__name__} emits '{phrase}'"


def test_explain_disclaims_causation_and_forecast():
    src = inspect.getsource(market_reaction.run)
    assert "not a causal claim" in src
    assert "not a forecast" in src


# ─── significance is per-instrument, not a fixed % ───────────────────────────
def test_same_move_is_significant_for_a_calm_instrument_only():
    """1.5% is routine for crude, extraordinary for a currency pair — the MAD
    z-score must reflect that, which a fixed threshold could not."""
    calm = [0.05, -0.08, 0.06, -0.04, 0.07, -0.05, 0.03]      # e.g. USD/INR
    volatile = [1.9, -2.2, 1.7, -1.6, 2.4, -2.0, 1.8]          # e.g. crude

    def z_for(history, move):
        return mad_zscore(abs(move), ewma([abs(h) for h in history], 0.3),
                          mad([abs(h) for h in history]))

    assert z_for(calm, 1.5) > z_for(volatile, 1.5)


def test_flat_move_is_never_significant():
    history = [0.5, -0.4, 0.6, -0.5, 0.45]
    assert mad_zscore(0.0, ewma([abs(h) for h in history], 0.3),
                      mad([abs(h) for h in history])) <= 0


# ─── run() contract ───────────────────────────────────────────────────────────
def test_run_defaults_match_the_spec():
    sig = inspect.signature(market_reaction.run)
    assert sig.parameters["lookback_hours"].default == 48     # preceding 24-48h
    assert sig.parameters["forward_hours"].default == 48      # reverse direction
    assert sig.parameters["min_clusters"].default >= 2        # not a single story


def test_both_directions_are_implemented():
    src = inspect.getsource(market_reaction.run)
    assert "news_then_move" in src and "move_then_news" in src
