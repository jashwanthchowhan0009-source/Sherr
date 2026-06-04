"""
workers/signal_worker.py — Aggregates feedback signals (Learn stage glue).

Cron: twice hourly. Three jobs:
  1. Decay stale preference weights gently toward the neutral 1.0 baseline so
     old interests fade if not reinforced.
  2. Recompute is_trending from recent engagement + importance.
  3. Adapt each active user's blend weights (α content / β collab / γ freshness)
     based on how much collaborative signal they've generated.

Standalone:  python -m app.workers.signal_worker
"""

from __future__ import annotations

import asyncio
import logging

from app.config import settings
from app.db import db

log = logging.getLogger("sherbyte.worker.signal")

DECAY = 0.98  # multiplicative pull toward 1.0 per run


async def _decay_preferences() -> int:
    result = await db.execute(
        """
        UPDATE user_preferences
        SET weight = 1.0 + (weight - 1.0) * $1
        WHERE updated_at < now() - interval '2 days'
        """,
        DECAY,
    )
    return int(result.split()[-1]) if result and result.split()[-1].isdigit() else 0


async def _refresh_trending() -> int:
    await db.execute("UPDATE info_objects SET is_trending = FALSE WHERE is_trending = TRUE")
    result = await db.execute(
        """
        UPDATE info_objects SET is_trending = TRUE
        WHERE id IN (
            SELECT io.id FROM info_objects io
            WHERE io.published_at > now() - interval '24 hours'
              AND (io.importance >= 0.6 OR io.engagement >= 10)
            ORDER BY (io.importance + LEAST(io.engagement::float / 50, 1.0)) DESC
            LIMIT 40
        )
        """
    )
    return int(result.split()[-1]) if result and result.split()[-1].isdigit() else 0


async def _adapt_weights() -> int:
    """Lean on collaborative filtering for users with rich histories."""
    rows = await db.fetch(
        """
        SELECT user_id, COUNT(*) AS n
        FROM signals
        WHERE created_at > now() - interval '30 days'
        GROUP BY user_id
        """
    )
    updated = 0
    for r in rows:
        n = r["n"]
        if n >= 50:
            alpha, beta, gamma = 0.4, 0.45, 0.15
        elif n >= 15:
            alpha, beta, gamma = 0.5, 0.3, 0.2
        else:
            alpha, beta, gamma = settings.rec_alpha, settings.rec_beta, settings.rec_gamma
        await db.execute(
            """
            INSERT INTO user_weights (user_id, alpha, beta, gamma, updated_at)
            VALUES ($1,$2,$3,$4, now())
            ON CONFLICT (user_id) DO UPDATE
              SET alpha=excluded.alpha, beta=excluded.beta,
                  gamma=excluded.gamma, updated_at=now()
            """,
            r["user_id"], alpha, beta, gamma,
        )
        updated += 1
    return updated


async def run(ctx=None) -> dict:
    decayed = await _decay_preferences()
    trending = await _refresh_trending()
    weights = await _adapt_weights()
    return {"prefs_decayed": decayed, "trending_set": trending, "weights_adapted": weights}


async def _main() -> None:
    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        log.info("signal aggregation: %s", await run())
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
