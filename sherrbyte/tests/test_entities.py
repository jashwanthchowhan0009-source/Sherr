"""
P1.1 — corpus-aware entity admission.

The blocklist approach failed one word at a time: "It's" got in, then "One", then
"Two", each fixed by appending another literal. These tests assert the *measured*
rules instead — a term is rejected because of what the corpus says about it, not
because someone remembered to list it.
"""

import pytest

from app.spie.graph import entities as E


TOTAL_DOCS = 1000
DF = {
    "reliance industries": {"documents": 40, "domains": 9},
    "dharmendra pradhan": {"documents": 22, "domains": 6},
    "india": {"documents": 300, "domains": 40},     # 30% — over the ceiling
    "it": {"documents": 420, "domains": 45},
    "one": {"documents": 380, "domains": 44},
    "the": {"documents": 900, "domains": 50},
    # Key is the POST-normalisation surface: normalize_name peels the "Ltd"
    # suffix, so "Single Source Ltd" looks up as "single source".
    "single source": {"documents": 9, "domains": 1},
    "rare thing": {"documents": 2, "domains": 3},
}


def admit(name, ner_type="MISC"):
    return E.is_admissible(name, ner_type, DF.get(E.canonical_surface(name)), TOTAL_DOCS)


# ─── the two the spec names ───────────────────────────────────────────────────
def test_junk_words_are_rejected():
    for junk in ("It's", "One", "The"):
        ok, _why = admit(junk)
        assert not ok, junk


def test_real_entities_are_admitted():
    assert admit("Reliance Industries", "ORG")[0]
    assert admit("Dharmendra Pradhan", "PERSON")[0]


# ─── 1. NER type whitelist ────────────────────────────────────────────────────
@pytest.mark.parametrize("t", ["DATE", "CARDINAL", "PERCENT", "TIME", "MONEY",
                               "ORDINAL", "QUANTITY"])
def test_non_entity_ner_types_are_dropped(t):
    assert not E.type_allowed(t)
    assert not admit("Tuesday", t)[0]


@pytest.mark.parametrize("t", ["ORG", "PERSON", "GPE", "PRODUCT", "EVENT", "LAW", "NORP"])
def test_entity_ner_types_are_allowed(t):
    assert E.type_allowed(t)


# ─── 2. surface normalisation ─────────────────────────────────────────────────
def test_possessives_collapse_onto_the_base_entity():
    """'India's' and 'India' must share one document-frequency count, not split it."""
    assert E.canonical_surface("India's") == E.canonical_surface("India") == "india"


def test_leading_article_is_stripped():
    assert E.canonical_surface("The Reserve Bank") == "reserve bank"


def test_contractions_reduce_to_a_stem_that_cannot_survive():
    """'It's' → 'it', which is two characters and fails on length alone. The point is
    that no list had to name it."""
    assert E.canonical_surface("It's") == "it"
    assert not admit("It's")[0]


def test_normalisation_of_a_real_name_is_stable():
    assert E.canonical_surface("Reliance Industries") == "reliance industries"


# ─── 3. document-frequency ceiling ────────────────────────────────────────────
def test_high_document_frequency_is_rejected_even_for_a_real_place():
    """India is a genuine GPE. In 30% of documents it still carries no information
    about any single one of them."""
    ok, why = admit("India", "GPE")
    assert not ok and "document frequency" in why


def test_ceiling_is_the_documented_value():
    assert E.MAX_DOCUMENT_FREQUENCY == 0.15


def test_a_term_just_under_the_ceiling_survives():
    stats = {"documents": int(TOTAL_DOCS * 0.14), "domains": 5}
    assert E.is_admissible("Borderline Corp", "ORG", stats, TOTAL_DOCS)[0]


# ─── 4. support floor ─────────────────────────────────────────────────────────
def test_single_publisher_is_not_enough():
    ok, why = admit("Single Source Ltd", "ORG")
    assert not ok and "publisher domain" in why


def test_too_few_documents_is_not_enough():
    ok, why = admit("Rare Thing", "ORG")
    assert not ok and "documents" in why


def test_unseen_terms_are_not_admitted_by_default():
    """An unknown term fails rather than passing on the assumption it is fine."""
    ok, why = E.is_admissible("Never Seen Corp", "ORG", None, TOTAL_DOCS)
    assert not ok and "no corpus support" in why


def test_support_floor_values_match_the_spec():
    assert E.MIN_DOCUMENTS == 3
    assert E.MIN_PUBLISHER_DOMAINS == 2


# ─── rejections explain themselves ────────────────────────────────────────────
def test_every_rejection_states_a_reason():
    for name, t in [("It's", "MISC"), ("India", "GPE"), ("Tuesday", "DATE"),
                    ("Rare Thing", "ORG")]:
        ok, why = admit(name, t)
        assert not ok and why and why != "ok"
