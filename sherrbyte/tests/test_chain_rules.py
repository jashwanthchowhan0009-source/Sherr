"""
Unit tests for the pure core of decision/rules (SPIE Task 5) — DB-free.

Covers direction wildcard semantics, single-condition matching, full-chain matching
with canonical entity overlap, and the log-odds (sigmoid) confidence aggregation.
"""

from app.spie.decision.rules import (
    direction_matches, signal_matches_condition, chain_matches,
    sigmoid, chain_confidence,
)


# ─── direction wildcard ───────────────────────────────────────────────────────
def test_direction_wildcard_and_strict():
    assert direction_matches(0, 1) is True          # 0 = wildcard
    assert direction_matches(0, -1) is True
    assert direction_matches(0, 1, strict=True) is False   # strict neutral
    assert direction_matches(1, 1) is True
    assert direction_matches(1, -1) is False
    assert direction_matches(1, 0) is False          # neutral signal ≠ required +1


def test_signal_matches_condition_domain_and_direction():
    cond = {"domain": "weather", "direction": 1}
    assert signal_matches_condition(cond, {"domain": "weather", "direction": 1})
    assert not signal_matches_condition(cond, {"domain": "news", "direction": 1})
    assert not signal_matches_condition(cond, {"domain": "weather", "direction": -1})


# ─── full chain (entity overlap on the anchor) ───────────────────────────────
_CONDS = [
    {"domain": "weather", "direction": 1},
    {"domain": "news", "direction": -1},
    {"domain": "commodities", "direction": 1},
]


def _sig(domain, direction, ents, cred=0.9):
    return {"domain": domain, "direction": direction, "entity_ids": set(ents), "credibility": cred}


def test_chain_fires_when_all_conditions_share_anchor():
    signals = [
        _sig("weather", 1, ["A", "X"]),
        _sig("news", -1, ["A", "Y"]),
        _sig("commodities", 1, ["A", "Z"]),
    ]
    matched, evidence = chain_matches(_CONDS, signals, "A")
    assert matched and len(evidence) == 3


def test_chain_fails_if_a_condition_missing():
    signals = [
        _sig("weather", 1, ["A"]),
        _sig("news", -1, ["A"]),
        # no commodities signal for A
    ]
    matched, evidence = chain_matches(_CONDS, signals, "A")
    assert not matched and evidence == []


def test_chain_fails_without_entity_overlap():
    # Each condition matches, but on DIFFERENT entities → no shared anchor.
    signals = [
        _sig("weather", 1, ["A"]),
        _sig("news", -1, ["B"]),
        _sig("commodities", 1, ["C"]),
    ]
    assert chain_matches(_CONDS, signals, "A")[0] is False


# ─── log-odds confidence ──────────────────────────────────────────────────────
def test_sigmoid_bounds():
    assert sigmoid(0) == 0.5
    assert sigmoid(-100) == 0.0 and sigmoid(100) == 1.0


def test_confidence_weights_and_bias_from_rule():
    ev = [_sig("weather", 1, ["A"]), _sig("news", -1, ["A"]), _sig("commodities", 1, ["A"])]
    # default weights (1.0 each), no bias → sigmoid(0.9+0.9+0.9)=sigmoid(2.7)
    base = chain_confidence({}, ev)
    assert base > 0.9
    # a negative prior bias lowers confidence (weights-as-data)
    biased = chain_confidence({"_bias": -1.5}, ev)
    assert biased < base
    # a heavier weight on one condition raises it
    heavier = chain_confidence({"2": 2.0}, ev)
    assert heavier > base


# ─── JSONB decoding (regression: 'str' object has no attribute 'get') ─────────
import json as _json

from app.spie.decision.rules import _as_json


def test_as_json_handles_asyncpg_string_jsonb():
    """asyncpg returns JSONB as str unless a codec is registered — the live run
    crashed on `c.get(...)` because conditions_json was a string."""
    conds = [{"domain": "weather", "direction": 1}, {"domain": "news", "direction": -1}]
    assert _as_json(conds, []) == conds                       # already parsed
    assert _as_json(_json.dumps(conds), []) == conds          # the crash case
    assert _as_json(_json.dumps(_json.dumps(conds)), []) == conds   # double-encoded
    assert _as_json(_json.dumps(conds).encode(), []) == conds       # bytes


def test_as_json_falls_back_on_junk():
    assert _as_json(None, []) == []
    assert _as_json("not json", []) == []
    assert _as_json(42, {}) == {}


def test_conditions_parse_then_extract_domains():
    conds = _as_json(_json.dumps([{"domain": "forex", "direction": 1},
                                  {"domain": "metals", "direction": 1}]), [])
    domains = sorted({c["domain"] for c in conds if isinstance(c, dict) and c.get("domain")})
    assert domains == ["forex", "metals"]
