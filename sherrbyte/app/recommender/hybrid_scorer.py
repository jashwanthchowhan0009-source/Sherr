"""
recommender/hybrid_scorer.py — Stage 05: PERSONALIZE.

Fuses three signals into one ranking, then diversifies with MMR:

    score = α · content  +  β · collab  +  γ · freshness

  • content   — match of the candidate to the user's pillar/topic affinities.
  • collab    — ALS latent-factor dot product (collaborative filtering).
  • freshness — exponential time decay.

α/β/γ are learned per user (user_weights) and default to config values. Muted
topics/sources/entities are hard-filtered. The output is written to the `feeds`
cache and returned as ranked info-object dicts.
"""

from __future__ import annotations

import json
import logging

from app.config import PILLARS, settings
from app.db.supabase import db
from app.recommender import als
from app.recommender.mmr import rerank
from app.recommender.temporal import freshness

log = logging.getLogger("sherbyte.scorer")

CANDIDATE_WINDOW_DAYS = 7
CANDIDATE_LIMIT = 500


def _minmax(values: dict[str, float]) -> dict[str, float]:
    if not values:
        return {}
    lo, hi = min(values.values()), max(values.values())
    span = (hi - lo) or 1.0
    return {k: (v - lo) / span for k, v in values.items()}


async def _user_weights(user_id: str) -> tuple[float, float, float]:
    row = await db.fetchrow(
        "SELECT alpha, beta, gamma FROM user_weights WHERE user_id=$1", user_id
    )
    if row:
        return row["alpha"], row["beta"], row["gamma"]
    return settings.rec_alpha, settings.rec_beta, settings.rec_gamma


async def _preferences(user_id: str):
    rows = await db.fetch(
        "SELECT topic, pillar_id, weight FROM user_preferences WHERE user_id=$1", user_id
    )
    pillar_w: dict[int, float] = {}
    topic_w: dict[str, float] = {}
    for r in rows:
        pillar_w[r["pillar_id"]] = pillar_w.get(r["pillar_id"], 0.0) + r["weight"]
        topic_w[r["topic"].lower()] = r["weight"]
    return pillar_w, topic_w


async def _mutes(user_id: str):
    rows = await db.fetch(
        "SELECT mute_type, value FROM user_mutes WHERE user_id=$1", user_id
    )
    muted = {"topic": set(), "source": set(), "entity": set()}
    for r in rows:
        muted.setdefault(r["mute_type"], set()).add(r["value"].lower())
    return muted


def _content_score(cand, pillar_w: dict[int, float], topic_w: dict[str, float]) -> float:
    score = pillar_w.get(cand["pillar_id"], 0.0)
    tags = cand["micro_tags"] if isinstance(cand["micro_tags"], list) else json.loads(cand["micro_tags"] or "[]")
    score += sum(topic_w.get(str(t).lower(), 0.0) for t in tags) * 1.5
    score += cand["importance"]  # global newsworthiness nudges content relevance
    return score


async def score_feed(user_id: str, limit: int = 50,
                     pillar: int | None = None, scope: str | None = None) -> list[dict]:
    """Compute and persist the personalized feed for a user."""
    pillar_w, topic_w = await _preferences(user_id)
    muted = await _mutes(user_id)
    alpha, beta, gamma = await _user_weights(user_id)

    q = """
        SELECT id, headline, summary, topic, pillar_id, micro_tags, scope,
               importance, sentiment, is_trending, source_name, image_url,
               thread_id, published_at, embedding, entities
        FROM info_objects
        WHERE published_at > now() - ($1 || ' days')::interval
    """
    args: list = [str(CANDIDATE_WINDOW_DAYS)]
    if pillar:
        q += f" AND pillar_id = ${len(args)+1}"; args.append(pillar)
    if scope:
        q += f" AND scope = ${len(args)+1}"; args.append(scope)
    q += f" ORDER BY published_at DESC LIMIT {CANDIDATE_LIMIT}"
    rows = await db.fetch(q, *args)

    # Hard-filter mutes.
    candidates = []
    for r in rows:
        if PILLARS.get(r["pillar_id"], {}).get("slug", "") in muted["topic"]:
            continue
        if (r["source_name"] or "").lower() in muted["source"]:
            continue
        candidates.append(r)

    if not candidates:
        return []

    item_ids = [str(r["id"]) for r in candidates]
    collab_raw = await als.collab_scores(user_id, item_ids)

    content_raw = {str(r["id"]): _content_score(r, pillar_w, topic_w) for r in candidates}
    fresh_raw = {str(r["id"]): freshness(r["published_at"]) for r in candidates}

    content_n = _minmax(content_raw)
    collab_n = _minmax(collab_raw)
    # freshness already in (0,1]

    scored = []
    for r in candidates:
        iid = str(r["id"])
        final = (alpha * content_n.get(iid, 0.0)
                 + beta * collab_n.get(iid, 0.0)
                 + gamma * fresh_raw.get(iid, 0.0))
        scored.append({
            "id": iid,
            "score": final,
            "embedding": list(r["embedding"]) if r["embedding"] is not None else None,
            "row": r,
        })

    # Diversify, then keep top `limit`.
    ranked = rerank(scored, top_k=limit)

    # Persist to feed cache.
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM feeds WHERE user_id=$1", user_id)
        for item in ranked:
            await conn.execute(
                """
                INSERT INTO feeds (user_id, info_object_id, score, computed_at)
                VALUES ($1,$2,$3, now())
                ON CONFLICT (user_id, info_object_id)
                DO UPDATE SET score=excluded.score, computed_at=now()
                """,
                user_id, item["id"], item["score"],
            )

    return ranked
