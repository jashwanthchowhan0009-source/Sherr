"""
adapters/credibility.py — source-credibility score (0..1), stdlib-only.

The engine assumes a source-credibility score to surface in explainability; the
current codebase had none, so this provides a lightweight curated one. Matched by
case-insensitive substring on the source name; unknown sources get a neutral 0.5.
Extend the tiers as the source list grows (or later back it with a table).
"""

from __future__ import annotations

# Higher = more trusted. Wire services / newspapers of record on top.
_TIERS: list[tuple[float, tuple[str, ...]]] = [
    (0.95, ("reuters", "associated press", "ap news", "pti", "press trust",
            "bloomberg", "afp")),
    (0.90, ("the hindu", "indian express", "bbc", "guardian", "livemint",
            "mint", "business standard", "economic times", "financial times")),
    (0.80, ("ndtv", "hindustan times", "the wire", "scroll", "cnbc", "moneycontrol")),
    (0.70, ("times of india", "toi", "india today", "news18", "firstpost", "zee")),
    # Market/data feeds are mechanical, not editorial → treated as high-reliability.
    (0.90, ("yahoo", "coingecko", "nse", "bse", "open-meteo", "openweather")),
]

_DEFAULT = 0.5


def score(source_name: str) -> float:
    if not source_name:
        return _DEFAULT
    s = source_name.strip().lower()
    for value, needles in _TIERS:
        if any(n in s for n in needles):
            return value
    return _DEFAULT
