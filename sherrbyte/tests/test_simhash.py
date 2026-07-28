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


def test_identical_body_same_cluster_guarantee():
    # "Insert the same body twice via the backfill path" → identical SimHash →
    # Hamming 0 ≤ threshold, so assign_cluster() must merge them into one cluster.
    body = ("Reserve Bank of India kept the repo rate unchanged at 6.5 percent on "
            "Friday, the central bank's monetary policy committee said.")
    a, b = simhash64(body), simhash64(body)
    assert a == b
    assert hamming(a, b) == 0 <= HAMMING_THRESHOLD


# ─── banded index (batch backfill path) ───────────────────────────────────────
import random

from app.spie.knowledge.simhash import SimHashIndex


def _full_scan(stored, sh):
    best, bh = None, HAMMING_THRESHOLD + 1
    for fp, cid in stored:
        h = hamming(sh, fp)
        if h <= HAMMING_THRESHOLD and h < bh:
            bh, best = h, cid
    return best


def test_banded_index_matches_full_scan():
    """Pigeonhole guarantee: with 4×16-bit bands, any pair within Hamming 3 shares
    at least one exact band — so the index must return exactly what a full scan does."""
    random.seed(42)
    idx, stored = SimHashIndex(), []
    for cid in range(1, 301):
        fp = random.getrandbits(64)
        stored.append((fp, cid))
        idx.add(fp, cid)

    # exact duplicates
    for fp, _ in stored[:40]:
        assert idx.find(fp) == _full_scan(stored, fp)

    # near-duplicates (1-3 flipped bits) and clearly-different (8 flipped bits)
    for fp, _ in stored[:40]:
        for nbits in (1, 2, 3, 8):
            v = fp
            for b in random.sample(range(64), nbits):
                v ^= (1 << b)
            assert idx.find(v) == _full_scan(stored, v), f"{nbits}-bit flip mismatch"

    # random probes
    for _ in range(200):
        p = random.getrandbits(64)
        assert idx.find(p) == _full_scan(stored, p)


def test_index_add_grows_and_finds_new_entries():
    idx = SimHashIndex()
    assert idx.find(12345) is None          # empty index
    idx.add(12345, 7)
    assert idx.find(12345) == 7             # exact hit
    assert idx.size == 1
