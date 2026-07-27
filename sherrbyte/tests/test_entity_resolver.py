"""
Unit tests for the pure (DB-free) core of pipeline/entity_resolver.

These cover the deterministic normalization + seeded-synonym resolution that all
downstream co-occurrence/correlation counts depend on. The async DB paths
(resolve/backfill against Postgres) are exercised by integration tests, not here.
"""

from app.spie.knowledge.entity_resolver import (
    normalize_name,
    coarse_type,
    resolve_key,
    SEED_LOOKUP,
)


# ─── normalize_name ───────────────────────────────────────────────────────────
def test_normalize_strips_corporate_suffix():
    assert normalize_name("Apple Inc.") == "apple"
    assert normalize_name("Tata Motors Ltd") == normalize_name("Tata Motors") == "tata motors"
    assert normalize_name("Reliance Industries Limited") == "reliance industries"


def test_normalize_drops_leading_the():
    assert normalize_name("The Reserve Bank of India") == "reserve bank of india"


def test_normalize_ampersand_and_punctuation():
    assert normalize_name("Procter & Gamble") == "procter and gamble"
    assert normalize_name("A.T. & T.") == "a t and t"


def test_normalize_whitespace_and_case():
    assert normalize_name("  TATA   motors ") == "tata motors"


def test_normalize_is_idempotent():
    for s in ["Tata Motors Ltd", "The Reserve Bank of India", "Procter & Gamble", "  X  "]:
        once = normalize_name(s)
        assert normalize_name(once) == once


def test_normalize_never_reduces_to_empty():
    # A bare suffix token must not vanish entirely.
    assert normalize_name("Ltd") == "ltd"
    assert normalize_name("") == ""
    assert normalize_name("   ") == ""


# ─── coarse_type ──────────────────────────────────────────────────────────────
def test_coarse_type_buckets():
    assert coarse_type("ORG") == "ORG"
    assert coarse_type("person") == "PERSON"
    assert coarse_type("GPE") == "GPE"
    assert coarse_type("LOC") == "GPE"
    assert coarse_type("EVENT") == "MISC"
    assert coarse_type("") == "MISC"
    assert coarse_type(None) == "MISC"


# ─── resolve_key (seed synonyms) ──────────────────────────────────────────────
def test_seed_collapses_short_forms():
    # The whole point: TaMo / Tata Motors Ltd / Tata Motors → one key.
    k1, t1, d1 = resolve_key("TaMo", "ORG")
    k2, _, _ = resolve_key("Tata Motors Ltd", "ORG")
    k3, _, _ = resolve_key("Tata Motors", "ORG")
    assert k1 == k2 == k3 == "tata motors"
    assert d1 == "Tata Motors"          # canonical display comes from the seed
    assert t1 == "ORG"


def test_seed_abbreviations():
    assert resolve_key("RBI")[0] == "reserve bank of india"
    assert resolve_key("INFY", "ORG")[0] == "infosys"
    assert resolve_key("Nifty50")[0] == resolve_key("Nifty", "MISC")[0] == "nifty 50"


def test_seed_overrides_type_and_display():
    # "PM Modi" arrives as MISC but the seed knows it's a PERSON named Narendra Modi.
    key, ctype, display = resolve_key("PM Modi", "MISC")
    assert key == "narendra modi"
    assert ctype == "PERSON"
    assert display == "Narendra Modi"


def test_non_seed_preserves_display_and_type():
    key, ctype, display = resolve_key("Wipro Ltd", "ORG")
    assert key == "wipro"
    assert ctype == "ORG"
    assert display == "Wipro Ltd"       # display keeps the mention's own casing


def test_empty_mention_yields_no_key():
    assert resolve_key("   ", "ORG")[0] == ""


def test_seed_lookup_is_populated():
    # Guards against a seed table that silently failed to build.
    assert "rbi" in SEED_LOOKUP and SEED_LOOKUP["rbi"][0] == "reserve bank of india"
    assert "tamo" in SEED_LOOKUP


# ─── is_valid_mention (junk-entity filter, BUG-1 fix) ─────────────────────────
from app.spie.knowledge.entity_resolver import is_valid_mention


def test_filter_rejects_function_words_and_tags():
    for junk in ["The", "This", "But", "After", "And", "Now", "That",
                 "Comments", "Read", "EXCLUSIVE", "WATCH", "LIVE", "BREAKING"]:
        assert not is_valid_mention(junk, "MISC"), junk


def test_filter_rejects_dates_weekdays_months_and_short():
    assert not is_valid_mention("July 2026", "DATE")      # temporal NER type
    assert not is_valid_mention("Monday", "MISC")          # weekday
    assert not is_valid_mention("July", "MISC")            # month
    assert not is_valid_mention("2026", "CARDINAL")        # number type
    assert not is_valid_mention("AI", "MISC")              # <= 2 chars


def test_filter_keeps_real_entities():
    for good, t in [("Reserve Bank of India", "ORG"), ("Narendra Modi", "PERSON"),
                    ("Mumbai", "GPE"), ("Tata Motors", "ORG"), ("Nifty 50", "MISC"),
                    ("RBI", "ORG"), ("ISRO", "ORG")]:
        assert is_valid_mention(good, t), good


def test_filter_on_realistic_headline_mentions():
    # Naive extraction of "Exclusive: Modi Meets Adani In Mumbai On Monday, Nifty Jumps"
    raw = ["Exclusive", "Modi", "Adani", "In", "Mumbai", "On", "Monday", "Nifty"]
    kept = [m for m in raw if is_valid_mention(m, "MISC")]
    assert kept == ["Modi", "Adani", "Mumbai", "Nifty"]
