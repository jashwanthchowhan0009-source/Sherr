"""
api/feed.py — /feed/personalized, /feed/trending, /feed/explore (Deliver stage).

Personalized feed runs the hybrid scorer (cached in `feeds`) and supports
infinite scroll via page/limit. Trending and explore are lighter global queries.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.config import PILLARS, PILLAR_ALIASES, SLUG_TO_PILLAR
from app.db.redis import cache_get, cache_set
from app.db.supabase import db
from app.recommender.hybrid_scorer import score_feed
from app.security import optional_user

log = logging.getLogger("sherbyte.feed")
router = APIRouter(prefix="/feed", tags=["feed"])


def row_to_feed_dict(r) -> dict:
    p = PILLARS.get(r["pillar_id"], PILLARS[1])
    tags = r["micro_tags"]
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []
    return {
        "id": str(r["id"]),
        "headline": r["headline"],
        "summary": r["summary"],
        "topic": r["topic"],
        "pillar_id": r["pillar_id"],
        "pillar_name": p["name"],
        "pillar_color": p["color"],
        "pillar_emoji": p["emoji"],
        "category": p["slug"],
        "micro_tags": tags or [],
        "scope": r["scope"],
        "importance": round(r["importance"], 3),
        "sentiment": r["sentiment"],
        "is_trending": r["is_trending"],
        "image_url": r["image_url"],
        "source_name": r["source_name"],
        "thread_id": str(r["thread_id"]) if r["thread_id"] else None,
        "published_at": r["published_at"].isoformat() if r["published_at"] else None,
    }


_SELECT = """
    SELECT id, headline, summary, topic, pillar_id, micro_tags, scope, importance,
           sentiment, is_trending, source_name, image_url, thread_id, published_at
    FROM info_objects
"""


@router.get("/personalized")
async def personalized(
    page: int = Query(1, ge=1),
    limit: int = Query(20, le=50),
    pillar: int = Query(0),
    scope: str = Query(""),
    user_id: Optional[str] = Depends(optional_user),
):
    """Hybrid-scored, MMR-diversified feed with infinite-scroll paging.

    Anonymous users (no token) get the global trending-by-importance feed.
    """
    offset = (page - 1) * limit
    pillar_arg = pillar or None
    scope_arg = scope or None

    if not user_id:
        q = _SELECT + " WHERE 1=1"
        args: list = []
        if pillar_arg:
            q += f" AND pillar_id=${len(args)+1}"; args.append(pillar_arg)
        # Region: national includes local (all India); local = local only;
        # global = everything (no filter). Fixed literals — safe (no interpolation).
        if scope_arg == "national":
            q += " AND scope IN ('national','local')"
        elif scope_arg == "local":
            q += " AND scope = 'local'"
        # Fresh-on-top: importance decayed by a 6h half-life so newly-ingested
        # stories rise to the top on each refresh (recency tiebreak).
        q += (" ORDER BY importance * power(0.5, GREATEST(EXTRACT(EPOCH FROM (now()-published_at)),0)/21600.0) DESC,"
              f" published_at DESC LIMIT ${len(args)+1} OFFSET ${len(args)+2}")
        args += [limit + 1, offset]
        rows = await db.fetch(q, *args)
        has_more = len(rows) > limit
        return {
            "articles": [row_to_feed_dict(r) for r in rows[:limit]],
            "page": page, "has_more": has_more, "personalized": False,
        }

    # Recompute the cached feed only on the first page; later pages read the cache.
    has_prefs = await db.fetchval(
        "SELECT 1 FROM user_preferences WHERE user_id=$1 LIMIT 1", user_id
    )
    if has_prefs and page == 1:
        await score_feed(user_id, limit=200, pillar=pillar_arg, scope=scope_arg)

    rows = await db.fetch(
        """
        SELECT io.id, io.headline, io.summary, io.topic, io.pillar_id, io.micro_tags,
               io.scope, io.importance, io.sentiment, io.is_trending, io.source_name,
               io.image_url, io.thread_id, io.published_at
        FROM info_objects io JOIN feeds f ON f.info_object_id = io.id
        WHERE f.user_id=$1 ORDER BY f.score DESC LIMIT $2 OFFSET $3
        """,
        user_id, limit + 1, offset,
    ) if has_prefs else []

    if len(rows) < 5:
        # Cold start — fall back to global importance feed.
        rows = await db.fetch(
            _SELECT + (" ORDER BY importance * power(0.5, GREATEST(EXTRACT(EPOCH FROM (now()-published_at)),0)/21600.0) DESC,"
                       " published_at DESC LIMIT $1 OFFSET $2"),
            limit + 1, offset,
        )

    has_more = len(rows) > limit
    return {
        "articles": [row_to_feed_dict(r) for r in rows[:limit]],
        "page": page, "has_more": has_more, "personalized": bool(has_prefs),
    }


@router.get("/trending")
async def trending(limit: int = Query(15, le=40)):
    cache_key = f"feed:trending:{limit}"
    cached = await cache_get(cache_key)
    if cached:
        return {"articles": cached, "cached": True}
    rows = await db.fetch(
        _SELECT + " WHERE is_trending=TRUE ORDER BY importance DESC, published_at DESC LIMIT $1",
        limit,
    )
    articles = [row_to_feed_dict(r) for r in rows]
    await cache_set(cache_key, articles, ttl=120)
    return {"articles": articles, "cached": False}


@router.get("/explore")
async def explore(
    category: str = Query(""),
    pillar: int = Query(0),
    scope: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(30, le=100),
):
    offset = (page - 1) * limit
    if category and not pillar:
        pillar = SLUG_TO_PILLAR.get(category.lower()) or PILLAR_ALIASES.get(category.lower(), 0)

    q = _SELECT + " WHERE 1=1"
    args: list = []
    if pillar:
        q += f" AND pillar_id=${len(args)+1}"; args.append(pillar)
    if scope == "national":
        q += " AND scope IN ('national','local')"
    elif scope == "local":
        q += " AND scope = 'local'"
    q += f" ORDER BY published_at DESC LIMIT ${len(args)+1} OFFSET ${len(args)+2}"
    args += [limit + 1, offset]

    rows = await db.fetch(q, *args)
    has_more = len(rows) > limit
    return {"articles": [row_to_feed_dict(r) for r in rows[:limit]], "has_more": has_more}
