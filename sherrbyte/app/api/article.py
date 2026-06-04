"""
api/article.py — /article/{id}, /article/{id}/full, search, bookmarks.

/article/{id}       → the info object card (feed shape + WWWW + entities).
/article/{id}/full  → adds the full body and the story-thread timeline.
Reads record a lightweight engagement bump + a 'read' signal for the Learn loop.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.feed import row_to_feed_dict
from app.db.supabase import db
from app.security import optional_user, require_user

log = logging.getLogger("sherbyte.article")
router = APIRouter(tags=["article"])


def _decode_entities(value) -> list:
    if isinstance(value, list):
        return value
    try:
        return json.loads(value or "[]")
    except Exception:
        return []


async def _record_read(user_id: Optional[str], info_id: str) -> None:
    await db.execute("UPDATE info_objects SET engagement = engagement + 1 WHERE id=$1", info_id)
    if user_id:
        await db.execute(
            "INSERT INTO signals (user_id, info_object_id, kind, value) VALUES ($1,$2,'read',1.0)",
            user_id, info_id,
        )


@router.get("/article/{info_id}")
async def get_article(info_id: str, user_id: Optional[str] = Depends(optional_user)):
    row = await db.fetchrow(
        """
        SELECT id, headline, summary, topic, pillar_id, micro_tags, scope, importance,
               sentiment, is_trending, source_name, image_url, thread_id, published_at,
               who, what, where_info, when_info, why_info, entities
        FROM info_objects WHERE id=$1
        """,
        info_id,
    )
    if not row:
        raise HTTPException(404, "Article not found")

    data = row_to_feed_dict(row)
    data["wwww"] = {
        "who": row["who"], "what": row["what"], "where": row["where_info"],
        "when": row["when_info"], "why": row["why_info"],
    }
    data["entities"] = _decode_entities(row["entities"])
    await _record_read(user_id, info_id)
    return data


@router.get("/article/{info_id}/full")
async def get_article_full(info_id: str, user_id: Optional[str] = Depends(optional_user)):
    row = await db.fetchrow(
        """
        SELECT id, headline, summary, body, topic, pillar_id, micro_tags, scope,
               importance, sentiment, is_trending, source_name, image_url, thread_id,
               published_at, who, what, where_info, when_info, why_info, entities
        FROM info_objects WHERE id=$1
        """,
        info_id,
    )
    if not row:
        raise HTTPException(404, "Article not found")

    data = row_to_feed_dict(row)
    data["body"] = row["body"]
    data["wwww"] = {
        "who": row["who"], "what": row["what"], "where": row["where_info"],
        "when": row["when_info"], "why": row["why_info"],
    }
    data["entities"] = _decode_entities(row["entities"])

    timeline = []
    if row["thread_id"]:
        sib = await db.fetch(
            """
            SELECT io.id, io.headline, io.published_at, io.source_name, sn.position
            FROM story_nodes sn JOIN info_objects io ON io.id = sn.info_object_id
            WHERE sn.thread_id=$1 ORDER BY sn.position ASC
            """,
            row["thread_id"],
        )
        timeline = [
            {"id": str(s["id"]), "headline": s["headline"],
             "source_name": s["source_name"], "position": s["position"],
             "published_at": s["published_at"].isoformat() if s["published_at"] else None}
            for s in sib
        ]
    data["timeline"] = timeline
    await _record_read(user_id, info_id)
    return data


@router.get("/search")
async def search(q: str = Query("", min_length=1)):
    rows = await db.fetch(
        """
        SELECT id, headline, summary, topic, pillar_id, micro_tags, scope, importance,
               sentiment, is_trending, source_name, image_url, thread_id, published_at
        FROM info_objects
        WHERE headline ILIKE $1 OR summary ILIKE $1 OR topic ILIKE $1
        ORDER BY published_at DESC LIMIT 25
        """,
        f"%{q}%",
    )
    return {"articles": [row_to_feed_dict(r) for r in rows]}


# ─── Bookmarks ────────────────────────────────────────────────────────────────
@router.get("/bookmarks")
async def list_bookmarks(user_id: str = Depends(require_user)):
    rows = await db.fetch(
        """
        SELECT io.id, io.headline, io.summary, io.topic, io.pillar_id, io.micro_tags,
               io.scope, io.importance, io.sentiment, io.is_trending, io.source_name,
               io.image_url, io.thread_id, io.published_at
        FROM bookmarks b JOIN info_objects io ON io.id = b.info_object_id
        WHERE b.user_id=$1 ORDER BY b.saved_at DESC
        """,
        user_id,
    )
    return {"articles": [row_to_feed_dict(r) for r in rows]}


@router.post("/bookmarks/{info_id}")
async def toggle_bookmark(info_id: str, user_id: str = Depends(require_user)):
    existing = await db.fetchval(
        "SELECT 1 FROM bookmarks WHERE user_id=$1 AND info_object_id=$2", user_id, info_id
    )
    if existing:
        await db.execute(
            "DELETE FROM bookmarks WHERE user_id=$1 AND info_object_id=$2", user_id, info_id
        )
        return {"saved": False}
    await db.execute(
        """
        INSERT INTO bookmarks (user_id, info_object_id) VALUES ($1,$2)
        ON CONFLICT DO NOTHING
        """,
        user_id, info_id,
    )
    await db.execute(
        "INSERT INTO signals (user_id, info_object_id, kind, value) VALUES ($1,$2,'save',1.0)",
        user_id, info_id,
    )
    return {"saved": True}
