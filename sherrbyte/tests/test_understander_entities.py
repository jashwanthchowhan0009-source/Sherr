"""
Regression for hyphenated-entity truncation in the regex NER fallback.

The live run produced "Spider" and "Man" as separate entities: the fallback regex
matched only letters and spaces, so a hyphen ended the match. Names with internal
hyphens/apostrophes must stay intact.
"""

import re

# The pattern used by pipeline.understander.extract_entities' regex fallback.
_WORD = r"[A-Z][a-zA-Z]*(?:[-'’][A-Za-z]+)*"
_PATTERN = rf"\b({_WORD}(?:\s+{_WORD}){{0,3}})\b"


def _extract(text):
    return re.findall(_PATTERN, text)


def test_hyphenated_names_are_not_split():
    found = _extract("Tom Holland returns in Spider-Man, says Coca-Cola exec Jean-Pierre Dubois")
    joined = " | ".join(found)
    assert "Spider-Man" in joined
    assert "Coca-Cola" in joined
    assert "Jean-Pierre" in joined
    # the old bug: bare "Spider" / "Coca" as standalone matches
    assert "Spider" not in found
    assert "Coca" not in found


def test_plain_names_still_match():
    found = _extract("Narendra Modi met Rohit Sharma in Mumbai")
    joined = " | ".join(found)
    assert "Narendra Modi" in joined and "Rohit Sharma" in joined and "Mumbai" in joined
