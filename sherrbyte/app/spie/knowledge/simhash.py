"""
knowledge/simhash.py — near-duplicate detection via 64-bit SimHash (SPIE Task 2).

Manku, Jain & Das Sarma (WWW 2007) operating point: 64-bit fingerprint over text
shingles; two docs with Hamming distance <= 3 are treated as the same story.

Pure core (stdlib only, unit-testable):
    shingles()   — word k-shingles of cleaned text
    simhash64()  — 64-bit SimHash (unsigned int 0..2^64-1)
    hamming()    — Hamming distance between two 64-bit fingerprints
    to_signed()/to_unsigned() — map 64-bit ↔ Postgres signed BIGINT

Async:
    assign_cluster() — fingerprint a doc, join the nearest recent cluster
                       (Hamming <= 3) or start a new one; persists to article_fingerprints.
"""

from __future__ import annotations

import hashlib
import logging
import re

log = logging.getLogger("sherbyte.simhash")

_MASK64 = (1 << 64) - 1
HAMMING_THRESHOLD = 3


def shingles(text: str, k: int = 3) -> list[str]:
    """Overlapping word k-shingles of cleaned text (lowercased, alnum tokens)."""
    tokens = re.findall(r"[a-z0-9]+", (text or "").lower())
    if len(tokens) < k:
        return [" ".join(tokens)] if tokens else []
    return [" ".join(tokens[i:i + k]) for i in range(len(tokens) - k + 1)]


def _feature_hash(feature: str) -> int:
    """Stable 64-bit hash of one feature (blake2b — deterministic across runs)."""
    return int.from_bytes(hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest(), "big")


def simhash64(text: str, k: int = 3) -> int:
    """64-bit SimHash of `text` as an unsigned int (0 for empty input)."""
    feats = shingles(text, k)
    if not feats:
        return 0
    bit_sums = [0] * 64
    for f in feats:
        h = _feature_hash(f)
        for i in range(64):
            bit_sums[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(64):
        if bit_sums[i] > 0:
            out |= (1 << i)
    return out


def hamming(a: int, b: int) -> int:
    """Number of differing bits between two 64-bit fingerprints (XOR + popcount)."""
    return bin((a ^ b) & _MASK64).count("1")


def to_signed(u: int) -> int:
    """Unsigned 64-bit → signed for Postgres BIGINT storage."""
    u &= _MASK64
    return u - (1 << 64) if u >= (1 << 63) else u


def to_unsigned(s: int) -> int:
    """Signed BIGINT (as read back) → unsigned 64-bit for Hamming."""
    return s + (1 << 64) if s < 0 else s


async def assign_cluster(conn, doc_id, text: str, *, window_days: int = 3,
                         scan_limit: int = 4000) -> int:
    """Fingerprint `text`, join the nearest recent story cluster (Hamming <= 3) or
    open a new one; upsert into article_fingerprints. Returns the cluster_id.

    The candidate scan is bounded to recent fingerprints (a new wire story matches
    its siblings from the same day/week); this is the simple V1.1 operating point —
    the Manku permutation index is a later scale optimization, not needed now.
    """
    sh = simhash64(text)
    rows = await conn.fetch(
        "SELECT simhash, cluster_id FROM article_fingerprints "
        "WHERE created_at >= now() - ($1 || ' days')::interval "
        "ORDER BY created_at DESC LIMIT $2",
        str(int(window_days)), int(scan_limit),
    )
    best_cid, best_h = None, HAMMING_THRESHOLD + 1
    for r in rows:
        h = hamming(sh, to_unsigned(r["simhash"]))
        if h <= HAMMING_THRESHOLD and h < best_h:
            best_h, best_cid = h, r["cluster_id"]

    if best_cid is None:
        best_cid = await conn.fetchval("SELECT nextval('article_cluster_seq')")

    await conn.execute(
        "INSERT INTO article_fingerprints (article_id, simhash, cluster_id) "
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (article_id) DO UPDATE SET simhash = EXCLUDED.simhash, "
        "cluster_id = EXCLUDED.cluster_id",
        doc_id, to_signed(sh), int(best_cid),
    )
    return int(best_cid)
