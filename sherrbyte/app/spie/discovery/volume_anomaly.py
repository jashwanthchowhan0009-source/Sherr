"""
discovery/volume_anomaly.py — per-entity volume spike detector (Sherr-I Task 4).

Signal: an entity's daily story volume jumps far above its own recent baseline.
Method: daily *cluster* counts (SimHash-deduped, so wire republication doesn't
inflate the spike) → EWMA baseline (α=0.3) → MAD-based robust z-score. Fires when
z ≥ 3.5 AND the day has ≥ 5 distinct stories. Detection only — describes what was
observed, never predicts.
"""

from __future__ import annotations

import logging

from app.spie.discovery.base import write_insight, names_for, domains_for, source_stats
from app.spie.discovery.anomaly_math import ewma, mad, mad_zscore

log = logging.getLogger("sherbyte.detectors.volume_anomaly")

# One story = its SimHash cluster, else the signal's own (negated) id.
_STORY_KEY = "COALESCE(cluster_id, -id)"


async def run(conn, *, window_days: int = 45, min_clusters: int = 5,
              z_threshold: float = 3.5, min_history: int = 7, alpha: float = 0.3) -> int:
    """Detect and persist volume_anomaly insights for the latest data day."""
    latest = await conn.fetchval(
        "SELECT max((ts AT TIME ZONE 'UTC')::date) FROM domain_signals"
    )
    if latest is None:
        return 0

    # Entities with >= min_clusters distinct stories on the latest day.
    candidates = await conn.fetch(
        f"""
        SELECT eid, COUNT(DISTINCT {_STORY_KEY}) AS c
        FROM domain_signals, unnest(entity_ids) AS eid
        WHERE (ts AT TIME ZONE 'UTC')::date = $1
        GROUP BY eid
        HAVING COUNT(DISTINCT {_STORY_KEY}) >= $2
        """,
        latest, min_clusters,
    )

    written = 0
    for cand in candidates:
        eid = cand["eid"]
        today = int(cand["c"])

        rows = await conn.fetch(
            f"""
            SELECT (ts AT TIME ZONE 'UTC')::date AS d, COUNT(DISTINCT {_STORY_KEY}) AS c
            FROM domain_signals
            WHERE $1 = ANY(entity_ids)
              AND (ts AT TIME ZONE 'UTC')::date > ($2::date - $3::int)
              AND (ts AT TIME ZONE 'UTC')::date <= $2::date
            GROUP BY (ts AT TIME ZONE 'UTC')::date
            ORDER BY d
            """,
            eid, latest, int(window_days),
        )
        series = {r["d"]: int(r["c"]) for r in rows}
        active_days = [d for d in series if d != latest]
        if len(active_days) < min_history:
            continue

        # Dense history from the entity's first active day to yesterday (0-filled),
        # so quiet days count toward the baseline but pre-existence days don't.
        start = min(series)
        history = []
        d = start
        while d < latest:
            history.append(series.get(d, 0))
            d = d.fromordinal(d.toordinal() + 1)
        if len(history) < min_history:
            continue

        baseline = ewma(history, alpha)
        scale = mad(history)
        z = mad_zscore(today, baseline, scale)
        if z < z_threshold:
            continue

        names = await names_for(conn, [eid])
        stats = await source_stats(conn, [eid], window_days)
        domains = await domains_for(conn, [eid], window_days)
        why = (f"{names[0]} appeared in {today} distinct stories on {latest} — a "
               f"volume spike far above its recent baseline of ~{baseline:.1f}/day "
               f"(robust MAD z-score {z:.1f}). Observed volume anomaly, not a prediction.")
        explain = {
            "why": why, "method": "ewma+mad_zscore",
            "clusters_today": today, "baseline": round(baseline, 2),
            "mad": round(scale, 2), "z": round(z, 2),
            "window_days": window_days, **stats,
            "confidence": round(min(1.0, z / 10.0), 3),
        }
        await write_insight(
            conn, type="volume_anomaly", entity_ids=[eid], domains=domains,
            score=float(round(z, 2)), explain=explain,
            signature=f"volume_anomaly:{eid}:{latest}",
        )
        written += 1

    log.info("volume_anomaly: %d insights", written)
    return written
