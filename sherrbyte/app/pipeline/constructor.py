"""
pipeline/constructor.py — Stage 03: CONSTRUCT.

Assembles the canonical InfoObject from a raw article + its Understanding:

    InfoObject { headline · wwww · entities · topic · importance · embedding[384] }

Responsibilities:
  • Entity normalization (dedupe + light canonicalization).
  • Importance scoring (0..1 newsworthiness) — drives trending & ranking.
  • Persisting the info_object row (embedding is filled later by the embedder).
"""

from __future__ import annotations

import json
import logging

from app.config import PILLARS
from app.models.article import ArticleIn
from app.models.info_object import Entity, InfoObjectIn
from app.pipeline.understander import Understanding
from app.text_utils import word_count

log = logging.getLogger("sherbyte.constructor")

# Light canonical map — extend as needed; keeps "PM Modi" and "Modi" together.
_CANONICAL = {
    "pm modi": "Narendra Modi", "modi": "Narendra Modi",
    "sc": "Supreme Court", "supreme court of india": "Supreme Court",
    "rbi": "Reserve Bank of India", "isro": "ISRO", "nasa": "NASA",
    "us fed": "Federal Reserve", "fed": "Federal Reserve",
}


def normalize_entities(entities: list[Entity]) -> list[Entity]:
    out, seen = [], set()
    for e in entities:
        canon = _CANONICAL.get(e.name.lower(), e.canonical or e.name).strip()
        key = canon.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(Entity(name=e.name, type=e.type, canonical=canon))
    return out


def score_importance(u: Understanding, art: ArticleIn) -> float:
    """Heuristic newsworthiness in [0,1].

    Combines trending signal, entity density, scope, body richness, and a small
    recency-agnostic baseline. Freshness decay is applied later, at rank time.
    """
    score = 0.15  # baseline so nothing is exactly zero
    if u.is_trending:
        score += 0.35
    score += min(0.20, 0.03 * len(u.entities))           # entity density
    score += {"global": 0.12, "national": 0.10, "local": 0.06}.get(u.scope, 0.05)
    wc = word_count(u.body)
    score += min(0.12, wc / 1500)                          # substance
    if any(u.wwww.model_dump().values()):                  # has structured WWWW
        score += 0.06
    return round(min(1.0, score), 4)


async def construct(article_id: str, art: ArticleIn, u: Understanding) -> InfoObjectIn:
    """Build the InfoObjectIn (not yet embedded)."""
    entities = normalize_entities(u.entities)
    importance = score_importance(u, art)
    return InfoObjectIn(
        article_id=article_id,
        headline=u.headline,
        summary=u.summary,
        body=u.body,
        wwww=u.wwww,
        entities=entities,
        topic=u.topic or PILLARS[u.pillar_id]["name"],
        pillar_id=u.pillar_id,
        micro_tags=u.micro_tags,
        scope=u.scope,
        importance=importance,
        sentiment=u.sentiment,
        is_trending=u.is_trending,
        source_name=art.source_name,
        image_url=art.image_url,
        video_url=getattr(art, "video_url", ""),
        published_at=art.published_at,
    )


async def persist_info_object(conn, obj: InfoObjectIn) -> str:
    """Insert the info_object row and return its id (embedding/thread filled later)."""
    row = await conn.fetchrow(
        """
        INSERT INTO info_objects
            (article_id, headline, summary, body, who, what, where_info, when_info,
             why_info, entities, topic, pillar_id, micro_tags, scope, importance,
             sentiment, is_trending, source_name, image_url, published_at, video_url)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
        RETURNING id
        """,
        obj.article_id, obj.headline, obj.summary, obj.body,
        obj.wwww.who, obj.wwww.what, obj.wwww.where, obj.wwww.when, obj.wwww.why,
        json.dumps([e.model_dump() for e in obj.entities]),
        obj.topic, obj.pillar_id, json.dumps(obj.micro_tags), obj.scope,
        obj.importance, obj.sentiment, obj.is_trending,
        obj.source_name, obj.image_url, obj.published_at, getattr(obj, "video_url", "") or "",
    )
    # Mark the source article processed.
    if obj.article_id:
        await conn.execute("UPDATE articles SET processed=TRUE WHERE id=$1", obj.article_id)
    return str(row["id"])
