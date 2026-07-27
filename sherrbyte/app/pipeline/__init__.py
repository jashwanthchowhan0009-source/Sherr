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

        # Feed the pattern engine: emit a news Signal for this article. Entity
        # resolution + incremental co-occurrence happen inside persist_signals.
        # Best-effort — a signal hiccup must never fail ingestion.
        try:
            from app.spie.knowledge.adapters.news import from_info_object
            from app.spie.knowledge.signals import persist_signals
            sigs = from_info_object({
                "id": info_id,
                "entities": obj.entities,
                "sentiment": obj.sentiment,
                "importance": obj.importance,
                "source_name": obj.source_name,
                "published_at": obj.published_at,
                "wwww": obj.wwww,
            })
            async with db.acquire() as conn:
                await persist_signals(conn, sigs)
        except Exception as e:
            log.warning("signal emit failed for %s: %s", info_id, e)

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
    try:
        await _notify_new_articles([r for r in results if r])
    except Exception as e:                       # never let push break ingestion
        log.warning("[PIPELINE] push notify skipped: %s", e)
    return stats


async def _notify_new_articles(new_ids: list[str]) -> None:
    """Push to subscribed users when a high-importance story lands in one of
    their preferred pillars. No-op unless web-push is configured."""
    from app.api.push import push_enabled, send_push_to_user
    if not new_ids or not push_enabled():
        return
    rows = await db.fetch(
        "SELECT id, headline, pillar_id, importance FROM info_objects "
        "WHERE id = ANY($1::uuid[]) AND importance >= 0.55 ORDER BY importance DESC",
        new_ids,
    )
    if not rows:
        return
    best_by_pillar: dict = {}
    for r in rows:
        best_by_pillar.setdefault(r["pillar_id"], r)
    subs = await db.fetch(
        "SELECT DISTINCT user_id FROM push_subscriptions "
        "WHERE last_push_at IS NULL OR last_push_at < now() - interval '3 hours'"
    )
    for su in subs:
        uid = su["user_id"]
        prefs = await db.fetch(
            "SELECT pillar_id FROM user_preferences WHERE user_id=$1 ORDER BY weight DESC LIMIT 3", uid
        )
        pset = {p["pillar_id"] for p in prefs}
        cand = None
        for pid, r in best_by_pillar.items():
            if (not pset or pid in pset) and (cand is None or r["importance"] > cand["importance"]):
                cand = r
        if cand:
            n = await send_push_to_user(uid, {"title": "SherrByte", "body": cand["headline"], "url": "/"})
            if n:
                await db.execute("UPDATE push_subscriptions SET last_push_at=now() WHERE user_id=$1", uid)


__all__ = ["run_cycle"]
