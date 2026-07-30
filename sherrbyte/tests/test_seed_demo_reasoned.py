"""
Demo-mode reasoned insights.

The risk this guards against is not a crash — it is a demo card that reads as live
intelligence. Every insight the seeder writes must carry its disclosure, must come
from the real template rather than hand-written prose, and must never claim evidence
it does not have.
"""

import asyncio

import pytest

from app.spie.reasoning.narrative import build_narrative, violates_language_rules
from app.workers import seed_demo_reasoned as seeder


# ─── stub DB ──────────────────────────────────────────────────────────────────
class FakeConn:
    """Minimal asyncpg-shaped stub. `populated=False` models an empty corpus."""

    def __init__(self, populated=True, npmi=True):
        self.populated, self.npmi = populated, npmi

    async def fetchrow(self, q, *a):
        if not self.populated:
            return None
        return {"eid": "e1", "v": 2.87, "source_id": "yahoo:commodities",
                "at": None, "day": None}

    async def fetch(self, q, *a):
        if not self.populated:
            return []
        if "canonical_name AS name" in q:
            return [{"name": "Iran", "c": 7, "sources": 5},
                    {"name": "OPEC", "c": 4, "sources": 3}]
        if "io.headline" in q:
            return [{"headline": "Tanker traffic slows near the Strait of Hormuz"}]
        if "MAX(c.npmi)" in q:
            return [{"a": "Iran", "b": "OPEC", "npmi": 0.71}] if self.npmi else []
        if "canonical_name" in q:
            return [{"id": "e1", "canonical_name": "WTI Crude"}]
        return [{"eid": "e2", "v": 1.4, "source_id": "yahoo:metals"}]


@pytest.fixture(autouse=True)
def _names(monkeypatch):
    async def fake(conn, ids):
        return [{"e1": "WTI Crude", "e2": "Gold"}.get(str(i), str(i)) for i in ids]
    monkeypatch.setattr(seeder, "names_for", fake)


def _tier_b(populated=True, npmi=True):
    return asyncio.run(seeder.tier_b(FakeConn(populated, npmi)))["reasoned"]


# ─── disclosure is mandatory ──────────────────────────────────────────────────
def test_every_demo_insight_is_tagged():
    r = _tier_b()
    assert r["demo"] is True
    assert r["demo_basis"] == "constructed_example"
    assert r["demo_note"]


def test_tier_a_is_tagged_too():
    """A widened window is not what the live engine does, so a tier-A card is
    still disclosed — 'real data' is not the same as 'a live run would show this'."""
    tagged = seeder._tag({}, basis="real_data_widened_window", note="n", fields=[])
    assert tagged["demo"] is True
    assert tagged["demo_basis"] == "real_data_widened_window"


def test_representative_fields_are_named_not_hidden():
    """On an empty corpus every field is representative, and the card must say which."""
    r = _tier_b(populated=False)
    for f in ("instrument", "move_pct", "news_entities", "headlines", "cross_market"):
        assert f in r["demo_fields"]
    assert "Representative fields" in r["demo_note"]


def test_full_real_data_claims_no_representative_fields():
    r = _tier_b(populated=True)
    assert r["demo_fields"] == []
    assert "Every field came from real data" in r["demo_note"]


# ─── the narrative comes from the real template ───────────────────────────────
def test_narrative_is_template_generated_not_authored():
    """Re-running the shipped template over the same dict must reproduce the text
    exactly — proof the string was not hand-written into the seeder."""
    r = _tier_b()
    assert r["narrative"] == build_narrative(r)


def test_narrative_obeys_the_language_contract():
    for populated in (True, False):
        assert violates_language_rules(_tier_b(populated)["narrative"]) == []


def test_wide_demo_window_is_disclosed_in_the_prose():
    r = _tier_b()
    assert "wide window" in r["narrative"]


# ─── no evidence is invented ──────────────────────────────────────────────────
def test_absent_lag_evidence_is_reported_as_absent():
    r = _tier_b()
    assert r["lag"]["passed"] is False and r["lag"]["reason"]
    assert "rank correlation" not in r["narrative"]


def test_absent_history_is_reported_as_absent():
    r = _tier_b()
    assert r["historical"]["similar_count"] == 0
    assert "No comparable prior coverage" in r["narrative"]


def test_npmi_is_used_when_measured_and_left_empty_when_not():
    """Real association strength raises confidence; its absence must not be filled
    with a guess."""
    with_npmi = _tier_b(npmi=True)
    without = _tier_b(npmi=False)
    assert any(c["npmi"] == 0.71 for c in with_npmi["connected"])
    assert all(c["npmi"] is None for c in without["connected"])
    assert with_npmi["confidence"] > without["confidence"]


def test_confidence_stays_low_without_lag_or_history():
    """Two factors cannot buy a confident-looking card — that is the M5 design."""
    assert _tier_b()["confidence"] < 0.5


def test_confidence_breakdown_is_present_and_complete():
    r = _tier_b()
    factors = {f["factor"] for f in r["confidence_breakdown"]}
    assert factors == {"source_diversity", "npmi_strength", "lag_evidence",
                       "historical_consistency", "cross_market"}


def test_methods_list_excludes_methods_that_did_not_run():
    """M2 (lag) and M3 (historical echo) did not produce evidence, so claiming them
    would overstate what the card rests on."""
    r = _tier_b()
    assert "M2" not in r["methods"] and "M3" not in r["methods"]
    assert "M5" in r["methods"]
