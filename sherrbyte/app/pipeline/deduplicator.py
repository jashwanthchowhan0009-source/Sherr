"""
pipeline/deduplicator.py — Stage 01b: 3-LAYER DEDUP.

The same story arrives from many sources within a single cycle and across
cycles. We catch duplicates in three escalating layers, cheapest first:

  Layer 1 — URL hash      : exact same link (Redis set + DB unique constraint).
  Layer 2 — Title hash    : same normalized headline from different sources.
  Layer 3 — Body SimHash  : near-identical body within a Hamming radius,
                            compared against recently collected articles.

dedupe_batch() runs intra-batch first (so three copies in one cycle collapse
to one) then checks each survivor against Redis / Postgres.
"""

from __future__ import annotations

import logging

from app.config import settings
from app.db.redis import seen_before
from app.db.supabase import db
from app.models.article import ArticleIn
from app.text_utils import simhash_similar

log = logging.getLogger("sherbyte.dedup")


async def _is_known_url(url_hash: str) -> bool:
    # Layer 1: Redis membership (fast) — falls back to DB if Redis is cold.
    try:
        if await seen_before(url_hash, bucket="dedup:urls"):
            return True
    except Exception as e:
        log.debug("Redis dedup unavailable, using DB only: %s", e)
    exists = await db.fetchval("SELECT 1 FROM articles WHERE url_hash=$1 LIMIT 1", url_hash)
    return bool(exists)


async def _is_known_title(title_hash: str) -> bool:
    # Layer 2: same normalized headline already stored.
    exists = await db.fetchval(
        "SELECT 1 FROM articles WHERE title_hash=$1 LIMIT 1", title_hash
    )
    return bool(exists)


async def _is_near_duplicate(art: ArticleIn) -> bool:
    # Layer 3: SimHash radius check against recent bodies in the same window.
    if not art.simhash:
        return False
    rows = await db.fetch(
        """
        SELECT simhash FROM articles
        WHERE simhash IS NOT NULL
          AND collected_at > now() - ($1 || ' hours')::interval
        ORDER BY collected_at DESC
        LIMIT 2000
        """,
        str(settings.story_link_window_hours),
    )
    for r in rows:
        if simhash_similar(art.simhash, r["simhash"], max_distance=3):
            return True
    return False


async def dedupe_batch(articles: list[ArticleIn]) -> list[ArticleIn]:
    """Return only the genuinely new, non-duplicate articles from a batch."""
    # Intra-batch collapse on url + title fingerprints.
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    intra: list[ArticleIn] = []
    for a in articles:
        if a.url_hash in seen_urls or a.title_hash in seen_titles:
            continue
        seen_urls.add(a.url_hash)
        seen_titles.add(a.title_hash)
        intra.append(a)

    unique: list[ArticleIn] = []
    for a in intra:
        if await _is_known_url(a.url_hash):
            continue
        if await _is_known_title(a.title_hash):
            continue
        if await _is_near_duplicate(a):
            continue
        unique.append(a)

    dropped = len(articles) - len(unique)
    log.info("[DEDUP] %d unique of %d raw (dropped %d duplicates)",
             len(unique), len(articles), dropped)
    return unique


async def persist_articles(articles: list[ArticleIn]) -> list[str]:
    """Insert deduped articles, returning the ids of those actually written."""
    ids: list[str] = []
    async with db.acquire() as conn:
        for a in articles:
            row = await conn.fetchrow(
                """
                INSERT INTO articles
                    (url, url_hash, title, title_hash, simhash, body,
                     image_url, source_name, scope, published_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (url) DO NOTHING
                RETURNING id
                """,
                a.url, a.url_hash, a.title, a.title_hash, a.simhash, a.body,
                a.image_url, a.source_name, a.scope, a.published_at,
            )
            if row:
                ids.append(str(row["id"]))
    log.info("[DEDUP] persisted %d new articles", len(ids))
    return ids
