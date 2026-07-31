"""
Unit tests for the pure npmi_score (Sherr-I Graph Engine, Task 3).

NPMI = PMI / -log p(a,b), computed from cluster-deduped counts. Verifies the
canonical anchors (independent ≈ 0, perfect = 1, negative < 0) and — the whole
point — that a hub entity's co-occurrence is discounted relative to a specific one.
"""

from app.spie.graph.cooccurrence import npmi_score


def test_independent_pair_is_zero():
    # p(a,b) == p(a)·p(b): N=100, ca=cb=10, expected cab = 1.
    assert abs(npmi_score(1, 10, 10, 100)) < 1e-9


def test_perfect_cooccurrence_is_one():
    # a and b appear only together (cab == ca == cb).
    assert npmi_score(5, 5, 5, 100) == 1.0


def test_all_stories_is_one():
    assert npmi_score(100, 100, 100, 100) == 1.0


def test_below_chance_is_negative():
    # ca=cb=50 → expected 25 together; only 10 → less than chance.
    assert npmi_score(10, 50, 50, 100) < 0


def test_hub_is_discounted_vs_specific():
    # Same pair count (5) and same rare partner (ca=5), but partner b is a hub
    # (cb=90) vs specific (cb=5). The hub association must score much lower.
    hub = npmi_score(5, 5, 90, 100)
    specific = npmi_score(5, 5, 5, 100)
    assert hub < specific
    assert hub < 0.2 and specific > 0.9


def test_undefined_returns_none():
    assert npmi_score(0, 10, 10, 100) is None      # never co-occur
    assert npmi_score(5, 0, 10, 100) is None        # degenerate
    assert npmi_score(5, 10, 10, 0) is None          # no stories


def test_range_is_bounded():
    for args in [(1, 10, 10, 100), (5, 5, 5, 100), (10, 50, 50, 100), (5, 5, 90, 100)]:
        v = npmi_score(*args)
        assert -1.0 <= v <= 1.0
