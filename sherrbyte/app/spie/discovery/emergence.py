"""
detectors/emergence.py — newly-emerging entity pairs (Intelligence Engine V1, Step 4).

Fires on entity pairs that co-occur >= 3 times in the current 7-day window AND had
ZERO occurrences in the trailing 90 days before it — i.e. a connection that just
appeared. Reads only the materialized cooccurrence table (never brute-forces pairs).
"""

from __future__ import annotations

import logging

from app.spie.discovery.base import write_insight, names_for, domains_for, source_stats

log = logging.getLogger("sherbyte.detectors.emergence")


async def run(conn, *, current_days: int = 7, history_days: int = 90,
              min_count: int = 3) -> int:
    """Detect and persist emergence insights. Returns how many were written."""
    candidates = await conn.fetch(
        """
        SELECT entity_a, entity_b, SUM(count) AS c
        FROM cooccurrence
        WHERE window_start >= (now() - ($1 || ' days')::interval)::date
        GROUP BY entity_a, entity_b
        HAVING SUM(count) >= $2
        """,
        str(current_days), min_count,
    )

    written = 0
    for r in candidates:
        a, b = r["entity_a"], r["entity_b"]

        # Must be genuinely new: zero co-occurrence in [90d ago, 7d ago).
        prior = await conn.fetchval(
            """
            SELECT COALESCE(SUM(count), 0) FROM cooccurrence
            WHERE entity_a = $1 AND entity_b = $2
              AND window_start >= (now() - ($3 || ' days')::interval)::date
              AND window_start <  (now() - ($4 || ' days')::interval)::date
            """,
            a, b, str(history_days), str(current_days),
        )
        if prior and int(prior) > 0:
            continue

        count = int(r["c"])
        names = await names_for(conn, [a, b])
        stats = await source_stats(conn, [a, b], current_days)
        domains = await domains_for(conn, [a, b], current_days)

        why = (f"{names[0]} and {names[1]} co-occurred {count} times in the last "
               f"{current_days} days, with no appearances together in the preceding "
               f"{history_days} days — a newly emerging connection.")
        # Confidence blends how often it co-occurred with how many distinct sources
        # corroborate it (a single-source burst is weaker than a multi-source one).
        src = min(stats["source_count"], 3) / 3.0
        confidence = round(min(1.0, (count / 10.0)) * (0.5 + 0.5 * src), 3)
        explain = {"why": why, **stats, "confidence": confidence}

        await write_insight(
            conn, type="emergence", entity_ids=[a, b], domains=domains,
            score=float(count), explain=explain, signature=f"emergence:{a}:{b}",
        )
        written += 1

    log.info("emergence: %d insights", written)
    return written
