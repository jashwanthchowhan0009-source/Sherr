"""
pipeline package — the core ingestion engine.

run_cycle() drives the full Collect → Dedup → Understand → Construct → Embed →
Connect path for one batch. It's called by the ingest worker (cron every ~25 min)
and can be invoked directly for a manual refresh.
"""

from __future__ import annotations

import asyncio
import logging

from app.db.supabase import db
from app.models.article import ArticleIn
from app.pipeline.collector import collect_all
from app.pipeline.connector import connect
from app.pipeline.constructor import construct, persist_info_object
from app.pipeline.deduplicator import dedupe_batch, persist_articles
from app.pipeline.embedder import embed_info_object
from app.pipeline.understander import understand

log = logging.getLogger("sherbyte.pipeline")


async def _process_one(article_id: str, art: ArticleIn) -> str | None:
    """Understand → Construct → Embed → Connect for a single article."""
    try:
        understanding = await understand(art)
        obj = await construct(article_id, art, understanding)
        async with db.acquire() as conn:
            info_id = await persist_info_object(conn, obj)
        await embed_info_object(info_id, obj.headline, obj.summary, obj.topic)
        await connect(info_id)
        return info_id
    except Exception as e:
        log.warning("pipeline failed for article %s: %s", article_id, e)
        return None


async def run_cycle(understand_concurrency: int = 5) -> dict:
    """Run one full ingestion cycle. Returns a small stats dict."""
    log.info("[PIPELINE] cycle start")

    raw = await collect_all()
    unique = await dedupe_batch(raw)
    article_ids = await persist_articles(unique)

    # Re-pair persisted ids with their ArticleIn (order preserved by persist).
    by_url = {a.url_hash: a for a in unique}
    pairs: list[tuple[str, ArticleIn]] = []
    rows = await db.fetch(
        "SELECT id, url_hash FROM articles WHERE id = ANY($1::uuid[])", article_ids
    ) if article_ids else []
    for r in rows:
        art = by_url.get(r["url_hash"])
        if art:
            pairs.append((str(r["id"]), art))

    sem = asyncio.Semaphore(understand_concurrency)

    async def _guarded(aid, art):
        async with sem:
            return await _process_one(aid, art)

    results = await asyncio.gather(*[_guarded(aid, art) for aid, art in pairs])
    constructed = sum(1 for r in results if r)

    stats = {
        "collected": len(raw),
        "unique": len(unique),
        "persisted": len(article_ids),
        "constructed": constructed,
    }
    log.info("[PIPELINE] cycle done: %s", stats)
    return stats


__all__ = ["run_cycle"]
