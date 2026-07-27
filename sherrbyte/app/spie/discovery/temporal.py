"""
detectors/temporal.py — leading-indicator (lag) correlation (Intelligence Engine V1, Step 4).

Candidate pairs come ONLY from the cooccurrence table (count >= 3) — never all
pairs, which would be a spurious-correlation factory. For each candidate we build
a daily series per entity (value = Σ magnitude·direction), test lags [0,1,2,3,7],
and fire only when all guards pass. Output is strictly "historically observed" /
"detected correlation" — never causation. pandas-free, nightly.
"""

from __future__ import annotations

import logging

from app.spie.discovery.base import write_insight, names_for, domains_for, source_stats
from app.spie.discovery.correlation_math import best_lag_correlation, two_period_consistent

log = logging.getLogger("sherbyte.detectors.temporal")

LAGS = (0, 1, 2, 3, 7)


async def _daily_series(conn, entity_id, days: int) -> dict:
    """Per-entity daily series: value = Σ(magnitude · direction) that UTC day."""
    rows = await conn.fetch(
        """
        SELECT (ts AT TIME ZONE 'UTC')::date AS d, SUM(magnitude * direction) AS v
        FROM domain_signals
        WHERE $1 = ANY(entity_ids) AND ts >= now() - ($2 || ' days')::interval
        GROUP BY (ts AT TIME ZONE 'UTC')::date
        """,
        entity_id, str(days),
    )
    return {r["d"]: float(r["v"] or 0.0) for r in rows}


async def run(conn, *, days: int = 90, min_cooc: int = 3, min_r: float = 0.5,
              min_overlap: int = 8) -> int:
    """Detect and persist temporal_correlation insights. Returns how many written."""
    candidates = await conn.fetch(
        """
        SELECT entity_a, entity_b, SUM(count) AS c
        FROM cooccurrence
        WHERE window_start >= (now() - ($1 || ' days')::interval)::date
        GROUP BY entity_a, entity_b
        HAVING SUM(count) >= $2
        """,
        str(days), min_cooc,
    )

    written = 0
    for r in candidates:
        a, b = r["entity_a"], r["entity_b"]
        sa = await _daily_series(conn, a, days)
        sb = await _daily_series(conn, b, days)
        if len(sa) < min_overlap or len(sb) < min_overlap:
            continue

        lag, rr, n = best_lag_correlation(sa, sb, LAGS)

        # Three guards, all required: strong effect, enough overlap, ≥2 periods.
        if abs(rr) < min_r or n < min_overlap:
            continue
        if not two_period_consistent(sa, sb, lag):
            continue

        # Lags are non-negative, so A leads B by `lag` days.
        leader, follower = a, b
        names = await names_for(conn, [leader, follower])
        stats = await source_stats(conn, [a, b], days)
        domains = await domains_for(conn, [a, b], days)

        why = (f"Historically observed: {names[0]} movements have been followed by "
               f"{names[1]} movements about {lag} day(s) later "
               f"(correlation {rr:.2f} over {n} overlapping days, in ≥2 separate "
               f"periods). This is a detected correlation, not causation, and is not "
               f"a prediction.")
        explain = {
            "why": why,
            "leader_entity": str(leader), "follower_entity": str(follower),
            "lag_days": lag, "r": round(rr, 3),
            "windows_tested": list(LAGS), "observations": n,
            **stats, "confidence": round(abs(rr), 3),
        }

        await write_insight(
            conn, type="temporal_correlation", entity_ids=[leader, follower],
            domains=domains, score=abs(rr), explain=explain,
            signature=f"temporal:{leader}:{follower}:{lag}",
        )
        written += 1

    log.info("temporal_correlation: %d insights", written)
    return written
