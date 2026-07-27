"""
pipeline/cooccurrence.py — materialized entity co-occurrence (Intelligence Engine V1, Step 3).

Pure core (no DB, unit-testable):
    pairs_from_entities()  → canonical unordered (a, b) pairs for one signal
    bucket_of()            → the daily window (UTC date) a signal falls in

Async DB:
    update_for_signal()    → incremental upsert of one signal's pairs (called at ingest)
    backfill()             → rebuild trailing-N-days counts from domain_signals

Counts are additive via ON CONFLICT. A full backfill (no limit) first clears the
window so it is idempotent; a limited backfill is an additive inspection batch.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone
from itertools import combinations
from typing import Optional

log = logging.getLogger("sherbyte.cooc")


# ─── NPMI (pure) ──────────────────────────────────────────────────────────────
def npmi_score(cab: int, ca: int, cb: int, n: int) -> Optional[float]:
    """Normalized pointwise mutual information of a co-occurring pair, in [-1, 1].

        pmi  = log( p(a,b) / (p(a)·p(b)) ) = log( cab·N / (ca·cb) )
        npmi = pmi / -log p(a,b)

    cab = stories where a & b co-occur, ca/cb = stories each appears in, N = total
    stories (all cluster-deduped). +1 = always together, 0 = independent (chance),
    −1 = never together. None when undefined (no co-occurrence)."""
    if cab <= 0 or ca <= 0 or cb <= 0 or n <= 0:
        return None
    p_ab = cab / n
    if p_ab >= 1.0:
        return 1.0                      # co-occur in every story → maximal association
    pmi = math.log(cab * n / (ca * cb))
    npmi = pmi / (-math.log(p_ab))
    return max(-1.0, min(1.0, npmi))


# ─── Pure core ────────────────────────────────────────────────────────────────
def pairs_from_entities(entity_ids) -> list[tuple[str, str]]:
    """Every unordered pair of distinct entities in one signal, each ordered
    canonically (a < b by string) so (X,Y) and (Y,X) collapse. Empty for < 2."""
    uniq = []
    seen = set()
    for e in entity_ids or []:
        s = str(e)
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    out = []
    for a, b in combinations(uniq, 2):
        out.append((a, b) if a < b else (b, a))
    return out


def bucket_of(ts) -> date:
    """The daily window (UTC date) a signal timestamp belongs to."""
    if isinstance(ts, datetime):
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc)
        return ts.date()
    if isinstance(ts, date):
        return ts
    # ISO string fallback
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc).date()


# ─── Async DB ─────────────────────────────────────────────────────────────────
_UPSERT = """
    INSERT INTO cooccurrence (entity_a, entity_b, window_start, count, last_seen)
    VALUES ($1, $2, $3, 1, $4)
    ON CONFLICT (entity_a, entity_b, window_start)
    DO UPDATE SET count     = cooccurrence.count + 1,
                  last_seen = GREATEST(cooccurrence.last_seen, EXCLUDED.last_seen)
"""


_EVENT_INSERT = """
    INSERT INTO cooccurrence_events (entity_a, entity_b, window_start, cluster_id)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT DO NOTHING
    RETURNING 1
"""


async def update_for_signal(conn, entity_ids, ts, cluster_id=None) -> int:
    """Fold one signal's entity pairs into the daily buckets. Returns pairs counted.

    When a story `cluster_id` is given (news path), a pair-day only counts each
    cluster ONCE — so the same wire story republished across many outlets does not
    inflate the count. Without a cluster (other domains), each signal counts.
    """
    pairs = pairs_from_entities(entity_ids)
    if not pairs:
        return 0
    window = bucket_of(ts)
    at = ts if isinstance(ts, datetime) else datetime.now(timezone.utc)

    if cluster_id is None:
        await conn.executemany(_UPSERT, [(a, b, window, at) for a, b in pairs])
        return len(pairs)

    # Cluster-deduped: only bump the pair-day count the first time this story
    # cluster contributes it (the events ledger enforces uniqueness).
    counted = 0
    for a, b in pairs:
        is_new = await conn.fetchval(_EVENT_INSERT, a, b, window, int(cluster_id))
        if is_new:
            await conn.execute(_UPSERT, a, b, window, at)
            counted += 1
    return counted


async def backfill(conn, days: int = 90, limit: Optional[int] = None) -> dict:
    """Rebuild co-occurrence for the trailing `days` window from domain_signals.

    Full run (limit is None): clears the window first, then folds in every signal —
    idempotent. Limited run (limit=N): additive inspection batch over the N most
    recent signals, for verifying a small real-Postgres run before the full pass.
    """
    reset = limit is None
    if reset:
        await conn.execute(
            "DELETE FROM cooccurrence WHERE window_start >= (now() - ($1 || ' days')::interval)::date",
            str(days),
        )

    q = (
        "SELECT entity_ids, ts FROM domain_signals "
        "WHERE ts >= now() - ($1 || ' days')::interval "
        "AND COALESCE(array_length(entity_ids, 1), 0) >= 2 "
        "ORDER BY ts DESC"
    )
    args = [str(days)]
    if limit is not None:
        q += " LIMIT $2"
        args.append(int(limit))

    rows = await conn.fetch(q, *args)
    signals = pairs = 0
    for r in rows:
        n = await update_for_signal(conn, r["entity_ids"], r["ts"])
        if n:
            signals += 1
            pairs += n

    total_rows = await conn.fetchval("SELECT COUNT(*) FROM cooccurrence")
    result = {
        "window_days": days, "limit": limit, "reset_window": reset,
        "signals_processed": signals, "pairs_written": pairs,
        "cooccurrence_rows": int(total_rows),
    }
    log.info("cooccurrence backfill: %s", result)
    return result


# Count each story once: cluster_id when present, else the signal's own (negated)
# id so a signal with no cluster is its own unique story.
_STORY_KEY = "COALESCE(cluster_id, -id)"


async def compute_npmi(conn, days: int = 90, min_count: int = 3) -> int:
    """Materialize the npmi column over the trailing window from cluster-deduped
    counts. Pairs with fewer than `min_count` co-occurrences are left NULL
    (rare-pair PMI is unstable). Returns how many pairs were scored."""
    n = await conn.fetchval(
        f"SELECT COUNT(DISTINCT {_STORY_KEY}) FROM domain_signals "
        "WHERE ts >= now() - ($1 || ' days')::interval",
        str(int(days)),
    )
    if not n or n < 2:
        return 0

    d = str(int(days))
    # Reset the window first so rare / disappeared pairs end up NULL.
    await conn.execute(
        "UPDATE cooccurrence SET npmi = NULL "
        "WHERE window_start >= (now() - ($1 || ' days')::interval)::date", d,
    )

    pairs = await conn.fetch(
        "SELECT entity_a, entity_b, SUM(count) AS cab FROM cooccurrence "
        "WHERE window_start >= (now() - ($1 || ' days')::interval)::date "
        "GROUP BY entity_a, entity_b HAVING SUM(count) >= $2",
        d, min_count,
    )

    ecache: dict = {}

    async def _ecount(eid) -> int:
        if eid not in ecache:
            ecache[eid] = await conn.fetchval(
                f"SELECT COUNT(DISTINCT {_STORY_KEY}) FROM domain_signals "
                "WHERE $1 = ANY(entity_ids) AND ts >= now() - ($2 || ' days')::interval",
                eid, d,
            ) or 0
        return ecache[eid]

    scored = 0
    for p in pairs:
        a, b, cab = p["entity_a"], p["entity_b"], int(p["cab"])
        val = npmi_score(cab, await _ecount(a), await _ecount(b), int(n))
        if val is None:
            continue
        await conn.execute(
            "UPDATE cooccurrence SET npmi = $1 WHERE entity_a = $2 AND entity_b = $3 "
            "AND window_start >= (now() - ($4 || ' days')::interval)::date",
            val, a, b, d,
        )
        scored += 1

    log.info("npmi: scored %d pairs over %d stories (window %dd)", scored, int(n), days)
    return scored
