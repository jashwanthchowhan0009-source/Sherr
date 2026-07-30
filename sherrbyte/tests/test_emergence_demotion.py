"""
Emergence demotion (Part 3).

Emergence is cheap to trigger: any pair that co-occurs now and didn't before fires
it. On a young corpus that surfaces FIFA↔World Cup — statistically new, and useless.
These tests pin the three filters and the two language rules that stop it.
"""

import re

import pytest

from app.spie.discovery import emergence


# ─── thresholds are the ones the spec fixed ───────────────────────────────────
def test_thresholds_require_association_and_corroboration():
    assert emergence.MIN_NPMI == 0.5
    assert emergence.MIN_SOURCES == 4


def test_run_defaults_use_those_thresholds():
    """A caller that passes nothing must get the strict behaviour, not the old one."""
    import inspect
    sig = inspect.signature(emergence.run)
    assert sig.parameters["min_npmi"].default == emergence.MIN_NPMI
    assert sig.parameters["min_sources"].default == emergence.MIN_SOURCES


# ─── the absence claim is bounded by real corpus depth ────────────────────────
def test_thin_corpus_does_not_claim_90_days():
    """With three weeks of data, 'absent for 90 days' is a claim we cannot make."""
    clause = emergence.history_clause(90, corpus_days=21)
    assert clause == "newly appearing in current coverage"
    assert "90" not in clause and "day" not in clause.replace("newly appearing", "")


def test_absence_claim_is_capped_at_actual_history():
    """Even with enough history to make the claim, it cannot exceed what we hold."""
    assert "45 days" in emergence.history_clause(90, corpus_days=45)
    assert "90 days" in emergence.history_clause(90, corpus_days=200)


@pytest.mark.parametrize("corpus_days", [0, 1, 15, 29])
def test_every_thin_corpus_length_avoids_a_duration_claim(corpus_days):
    assert emergence.history_clause(90, corpus_days) == "newly appearing in current coverage"


def test_history_claim_boundary_is_inclusive():
    n = emergence.MIN_HISTORY_DAYS_FOR_ABSENCE_CLAIM
    assert "days" in emergence.history_clause(90, n)
    assert emergence.history_clause(90, n - 1) == "newly appearing in current coverage"


# ─── language: our own corpus count is never the claim ────────────────────────
def _why(names, source_count, npmi, clause):
    """Mirror of the narrative the detector builds, so the wording is testable
    without a database."""
    return (f"{names[0]} and {names[1]} are appearing together across "
            f"{source_count} independent sources, {clause}. "
            f"Association strength (NPMI) {npmi:.2f} — above chance.")


def test_narrative_never_states_a_corpus_occurrence_count():
    """'co-occurred 12 times in the app' describes what we ingested, not the world."""
    why = _why(["FIFA", "Argentina"], 5, 0.71,
               emergence.history_clause(90, corpus_days=21))
    assert "times" not in why
    assert not re.search(r"co-occurred \d+", why)
    assert "in the app" not in why


def test_narrative_leads_with_independent_sources_and_npmi():
    why = _why(["Telegram", "Russia"], 6, 0.64,
               emergence.history_clause(90, corpus_days=120))
    assert "6 independent sources" in why
    assert "(NPMI) 0.64" in why


def test_narrative_carries_no_forecast_language():
    from app.spie.reasoning.narrative import violates_language_rules
    why = _why(["Telegram", "Russia"], 6, 0.64,
               emergence.history_clause(90, corpus_days=120))
    assert violates_language_rules(why) == []


# ─── blocklist plumbing ───────────────────────────────────────────────────────
def test_blocklist_lookup_is_direction_independent():
    """Pairs are stored lexically ordered, and the detector sorts before lookup, so
    (FIFA, World Cup) and (World Cup, FIFA) are the same row."""
    blocked = {("fifa", "world cup")}
    assert tuple(sorted(["world cup", "fifa"])) in blocked
    assert tuple(sorted(["fifa", "world cup"])) in blocked


def test_migration_seeds_the_named_obvious_pairs():
    """The pairs called out as embarrassing must actually be in the migration."""
    from pathlib import Path
    sql = Path(__file__).resolve().parents[1].joinpath(
        "app/db/migrations/018_expected_pairs.sql").read_text()
    for a, b in [("fifa", "world cup"), ("argentina", "fifa"), ("covid", "fauci"),
                 ("apple", "iphone"), ("india", "rbi")]:
        assert f"'{a}'" in sql and f"'{b}'" in sql


def test_migration_pairs_are_lexically_ordered():
    """The table CHECKs norm_a <= norm_b; a mis-ordered seed would fail on boot."""
    from pathlib import Path
    sql = Path(__file__).resolve().parents[1].joinpath(
        "app/db/migrations/018_expected_pairs.sql").read_text()
    rows = re.findall(r"\('([^']+)',\s*'([^']+)',\s*'[^']*'\)", sql)
    assert rows
    for a, b in rows:
        assert a <= b, f"({a}, {b}) violates the CHECK constraint"
