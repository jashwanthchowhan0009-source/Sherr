"""
Rule-based fallback when both AI providers are down.

The corpus reached 1600+ parked rows and served an empty feed because the rewrite
pass was pointed at a decommissioned model: with no provider there is no rewritten
headline, and with no headline every article parked. These tests pin the way out
of that, and — more importantly — the one line that must NOT move with it: an
overlapping body is still blocked, provider outage or not.
"""

import asyncio
import importlib
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import ai_processor as ai  # noqa: E402


@pytest.fixture
def both_down(monkeypatch):
    """Neither provider answers — the exact 4xx/5xx/no-key case."""
    async def dead(*a, **k):
        return None
    monkeypatch.setattr(ai, "_call_gemini", dead)
    monkeypatch.setattr(ai, "_call_groq", dead)


TITLE = "RBI holds the repo rate steady as inflation cools"
BODY = ("The central bank said on Tuesday it would keep the policy repo rate "
        "unchanged at 6.5 percent, citing an easing inflation trajectory and "
        "steady growth in bank credit across the economy.")


# ─── no article is left parked ────────────────────────────────────────────────
def test_both_providers_down_still_yields_a_publishable_article(both_down):
    r = asyncio.run(ai.process_article(TITLE, BODY))
    assert r["publish_as_aggregator"] is True
    assert r["refined_title"] == TITLE          # kept, under credit
    assert r["ai_fallback"] is True


def test_fallback_classifies_by_keyword_rather_than_defaulting(both_down):
    """The point of a rule-based fallback is the rules. Falling through to the
    caller's default category would file the whole outage under one pillar."""
    r = asyncio.run(ai.process_article(TITLE, BODY, fallback_category="arts"))
    assert r["category"] == "economy"
    assert r["classifier"]["matched"]


def test_fallback_still_reports_its_evidence(both_down):
    r = asyncio.run(ai.process_article(TITLE, BODY))
    assert "matched" in r["classifier"]


def test_an_unusable_provider_response_is_treated_as_a_failure(monkeypatch):
    """A 200 carrying junk is a provider failure like any other — publish, not park."""
    async def junk(*a, **k):
        return "not a dict"
    monkeypatch.setattr(ai, "_call_gemini", junk)
    r = asyncio.run(ai.process_article(TITLE, BODY))
    assert r.get("publish_as_aggregator") is True


# ─── what the fallback must NEVER do ──────────────────────────────────────────
def test_the_fallback_never_serves_the_publishers_body(both_down):
    """An outage is not a licence to reproduce someone's article. The headline is
    kept under credit; the prose is ours."""
    r = asyncio.run(ai.process_article(TITLE, BODY))
    assert "6.5 percent" not in r["full_body"]
    assert "central bank said on Tuesday" not in r["full_body"]
    assert r["full_body"] == ai._SAFE_BODY


def test_a_healthy_provider_is_not_marked_for_the_fallback(monkeypatch):
    async def ok(*a, **k):
        return {"refined_title": "Our own rewritten headline here",
                "summary": " ".join(["word"] * 30),
                "full_body": " ".join(["original"] * 60),
                "category": "economy"}
    monkeypatch.setattr(ai, "_call_gemini", ok)
    r = asyncio.run(ai.process_article(TITLE, BODY))
    assert "publish_as_aggregator" not in r
    assert r.get("ai_fallback") is not True


# ─── the gate honours the signal, but not unconditionally ─────────────────────
@pytest.fixture
def gate(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "g.db"))
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("ADMIN_TOKEN", "t")
    monkeypatch.setenv("JWT_SECRET", "s")
    import main
    importlib.reload(main)
    return main


def test_gate_publishes_a_fallback_row_instead_of_parking_it(gate):
    status, audit = gate._gate_article(
        TITLE, ai._SAFE_BODY, TITLE, BODY,
        ai_result={"publish_as_aggregator": True, "classifier": {"matched": ["rbi"]}})
    assert status == "published"
    assert audit["posture"] == "aggregator" and audit["ai_fallback"] is True


def test_without_the_signal_a_copied_headline_still_parks(gate):
    status, _ = gate._gate_article(TITLE, ai._SAFE_BODY, TITLE, BODY)
    assert status == "pending_rewrite"


def test_an_overlapping_body_is_blocked_even_during_an_outage(gate):
    """The headline rule bends for an outage. The body rule does not — a body that
    overlaps the source is a reproduction whatever the provider situation is."""
    status, _ = gate._gate_article(
        TITLE, BODY, TITLE, BODY,
        ai_result={"publish_as_aggregator": True})
    assert status == "blocked_originality"


def test_the_aggregator_body_carries_credit_and_a_link(gate):
    row = {"source_name": "Reuters", "url": "https://example.com/a1"}

    class R(dict):
        def keys(self):
            return super().keys()
        def __getitem__(self, k):
            return super().__getitem__(k)
    result = {"full_body": ai._SAFE_BODY}
    gate._apply_aggregator_posture(result, R(row))
    assert "Source: Reuters" in result["full_body"]
    assert "https://example.com/a1" in result["full_body"]


def test_the_aggregator_body_survives_a_row_missing_those_columns(gate):
    """Older rows and other call sites may not carry source_name/url. Attribution
    degrades to a generic credit rather than raising mid-batch."""
    result = {"full_body": ai._SAFE_BODY}
    gate._apply_aggregator_posture(result, {})
    assert "the original publisher" in result["full_body"]
