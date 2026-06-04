"""
recommender/als.py — ALS collaborative filtering.

Trains an implicit-feedback ALS model over the signals matrix (users × info
objects, weighted by interaction strength) and persists the learned latent
factors into als_user_factors / als_item_factors. The hybrid scorer then reads
those factors to compute the collaborative term as a dot product.

Uses the `implicit` library when available; otherwise it no-ops the training
(the hybrid scorer simply falls back to content+freshness only). Either way the
service keeps running.
"""

from __future__ import annotations

import logging

import numpy as np

from app.config import settings
from app.db.supabase import db

log = logging.getLogger("sherbyte.als")

# Implicit-feedback confidence weights per signal kind.
SIGNAL_WEIGHTS = {
    "save": 5.0, "like": 4.0, "share": 4.0, "quiz": 3.0,
    "read": 2.0, "dwell": 1.0, "dislike": -3.0, "hide": -4.0,
}


async def _load_signal_matrix():
    """Build a sparse interaction matrix from the signals table."""
    rows = await db.fetch(
        """
        SELECT user_id, info_object_id, kind, value
        FROM signals
        WHERE info_object_id IS NOT NULL
          AND created_at > now() - interval '60 days'
        """
    )
    if not rows:
        return None

    users = sorted({str(r["user_id"]) for r in rows})
    items = sorted({str(r["info_object_id"]) for r in rows})
    u_idx = {u: i for i, u in enumerate(users)}
    i_idx = {it: i for i, it in enumerate(items)}

    mat = np.zeros((len(users), len(items)), dtype=np.float32)
    for r in rows:
        w = SIGNAL_WEIGHTS.get(r["kind"], 1.0) * float(r["value"] or 1.0)
        mat[u_idx[str(r["user_id"])], i_idx[str(r["info_object_id"])]] += w
    mat = np.clip(mat, 0.0, None)  # implicit ALS expects non-negative confidence
    return users, items, mat


async def train() -> dict:
    """Retrain ALS and persist factors. Returns stats."""
    loaded = await _load_signal_matrix()
    if loaded is None:
        log.info("[ALS] no signals yet — skipping train")
        return {"trained": False, "reason": "no_signals"}
    users, items, mat = loaded

    try:
        import implicit
        from scipy.sparse import csr_matrix

        model = implicit.als.AlternatingLeastSquares(
            factors=settings.als_factors,
            iterations=settings.als_iterations,
            regularization=settings.als_regularization,
        )
        # implicit expects item-user during fit (CSR).
        model.fit(csr_matrix(mat))
        user_factors = model.user_factors
        item_factors = model.item_factors
    except Exception as e:
        log.warning("[ALS] implicit unavailable, using SVD fallback: %s", e)
        # Truncated-SVD fallback gives comparable latent factors.
        k = min(settings.als_factors, min(mat.shape) - 1) if min(mat.shape) > 1 else 1
        u, s, vt = np.linalg.svd(mat, full_matrices=False)
        user_factors = u[:, :k] * np.sqrt(s[:k])
        item_factors = (vt[:k, :].T) * np.sqrt(s[:k])

    async with db.acquire() as conn:
        for uid, vec in zip(users, user_factors):
            await conn.execute(
                """
                INSERT INTO als_user_factors (user_id, factors, updated_at)
                VALUES ($1,$2, now())
                ON CONFLICT (user_id) DO UPDATE
                  SET factors=excluded.factors, updated_at=now()
                """,
                uid, [float(x) for x in vec],
            )
        for iid, vec in zip(items, item_factors):
            await conn.execute(
                """
                INSERT INTO als_item_factors (info_object_id, factors, updated_at)
                VALUES ($1,$2, now())
                ON CONFLICT (info_object_id) DO UPDATE
                  SET factors=excluded.factors, updated_at=now()
                """,
                iid, [float(x) for x in vec],
            )

    stats = {"trained": True, "users": len(users), "items": len(items)}
    log.info("[ALS] retrained: %s", stats)
    return stats


async def collab_scores(user_id: str, item_ids: list[str]) -> dict[str, float]:
    """Dot-product the user's factors with each item's factors → raw scores."""
    if not item_ids:
        return {}
    urow = await db.fetchrow("SELECT factors FROM als_user_factors WHERE user_id=$1", user_id)
    if not urow:
        return {iid: 0.0 for iid in item_ids}
    uvec = np.asarray(urow["factors"], dtype=np.float32)
    rows = await db.fetch(
        "SELECT info_object_id, factors FROM als_item_factors WHERE info_object_id = ANY($1::uuid[])",
        item_ids,
    )
    out = {iid: 0.0 for iid in item_ids}
    for r in rows:
        ivec = np.asarray(r["factors"], dtype=np.float32)
        n = min(len(uvec), len(ivec))
        if n:
            out[str(r["info_object_id"])] = float(np.dot(uvec[:n], ivec[:n]))
    return out
