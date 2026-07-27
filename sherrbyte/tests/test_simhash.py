"""
Unit tests for the pure core of knowledge/simhash (SPIE Task 2).

DB-free. Verifies shingling, 64-bit SimHash stability, the Hamming-distance
operating point (near-duplicate wire copies ≤ 3, distinct stories ≫ 3), and the
signed/unsigned BIGINT round-trip used for Postgres storage.
"""

from app.spie.knowledge.simhash import (
    shingles, simhash64, hamming, to_signed, to_unsigned, HAMMING_THRESHOLD,
)


# ─── shingles ─────────────────────────────────────────────────────────────────
def test_shingles_basic():
    assert shingles("The RBI cut rates today", k=3) == [
        "the rbi cut", "rbi cut rates", "cut rates today",
    ]


def test_shingles_short_and_empty():
    assert shingles("hi", k=3) == ["hi"]
    assert shingles("", k=3) == []


# ─── simhash stability ────────────────────────────────────────────────────────
def test_simhash_deterministic_and_identical():
    t = "Reserve Bank of India holds the repo rate steady at 6.5 percent"
    assert simhash64(t) == simhash64(t)             # deterministic
    assert hamming(simhash64(t), simhash64(t)) == 0  # identical text → distance 0


def test_simhash_empty_is_zero():
    assert simhash64("") == 0


# ─── the operating point: near-dup ≤ 3, distinct ≫ 3 ─────────────────────────
# The Hamming ≤ 3 point catches *verbatim* wire republication — the real problem
# (the same PTI/ANI body carried by many outlets, each adding a byline/agency tag),
# NOT paraphrases. A genuinely rewritten story is a different story.
_WIRE = ("Reserve Bank of India kept the repo rate unchanged at 6.5 percent on "
         "Friday citing sticky food inflation and resilient economic growth the "
         "central bank monetary policy committee said in its statement the decision "
         "was widely expected by economists and markets showed little reaction as "
         "the rupee held steady against the dollar in afternoon trade")

# Same wire body as republished by another outlet — identical text + an agency tag.
_WIRE_REPUB = _WIRE + " pti news agency"

_DIFFERENT = ("India cricket team clinched a last over win against Australia in the "
              "T20 series decider at the Wankhede stadium on Friday night as fans "
              "celebrated across the country through the evening")


def test_near_duplicate_within_threshold():
    d = hamming(simhash64(_WIRE), simhash64(_WIRE_REPUB))
    assert d <= HAMMING_THRESHOLD, f"verbatim republish distance {d} should be ≤ {HAMMING_THRESHOLD}"


def test_distinct_story_far_apart():
    d = hamming(simhash64(_WIRE), simhash64(_DIFFERENT))
    assert d > HAMMING_THRESHOLD, f"distinct-story distance {d} should be ≫ {HAMMING_THRESHOLD}"


# ─── signed/unsigned round-trip for BIGINT storage ────────────────────────────
def test_signed_roundtrip():
    for u in [0, 1, (1 << 62), (1 << 63), (1 << 63) + 5, (1 << 64) - 1]:
        s = to_signed(u)
        assert -(1 << 63) <= s < (1 << 63)          # fits a signed BIGINT
        assert to_unsigned(s) == u                   # lossless round-trip


def test_hamming_uses_signed_values_correctly():
    # A fingerprint stored as a negative BIGINT must still Hamming-compare right.
    u1 = simhash64(_WIRE)
    u2 = simhash64(_WIRE_REPUB)
    assert hamming(to_unsigned(to_signed(u1)), to_unsigned(to_signed(u2))) == hamming(u1, u2)
