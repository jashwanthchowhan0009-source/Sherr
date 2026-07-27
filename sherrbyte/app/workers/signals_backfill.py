"""
workers/signals_backfill.py — seed domain_signals from existing info_objects.

A fresh engine deploy has an empty domain_signals table (signals only accumulate
as new articles are ingested). This backfills the news domain from the info_objects
already in the DB, so entity resolution + co-occurrence populate immediately and the
detectors have real data to run on. Entity resolution and incremental co-occurrence
happen inside persist_signals.

Standalone:
    python -m app.workers.signals_backfill --limit 100    # small verify batch first
    python -m app.workers.signals_backfill                # full backfill
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from app.db import db

log = logging.getLogger("sherbyte.worker.signals_backfill")


async def run(limit: int | None = None) -> dict:
    from app.spie.knowledge.adapters.news import from_info_object
    from app.spie.knowledge.signals import persist_signals
    from app.spie.knowledge.simhash import assign_cluster

    q = ("SELECT id, article_id, headline, body, summary, entities, sentiment, "
         "importance, source_name, where_info, published_at FROM info_objects "
         "WHERE entities IS NOT NULL ORDER BY published_at DESC")
    if limit is not None:
        q += f" LIMIT {int(limit)}"

    processed = 0
    async with db.acquire() as conn:
        rows = await conn.fetch(q)
        for r in rows:
            ents = r["entities"]
            if isinstance(ents, str):
                try:
                    ents = json.loads(ents or "[]")
                except Exception:
                    ents = []
            # SimHash story cluster over cleaned text (dedup wire republication).
            doc_id = r["article_id"] or r["id"]
            text = f"{r['headline']} {r['body'] or r['summary'] or ''}"
            cluster_id = None
            try:
                cluster_id = await assign_cluster(conn, doc_id, text)
            except Exception as e:
                log.warning("simhash cluster assign failed for %s: %s", r["id"], e)
            sigs = from_info_object({
                "id": str(r["id"]),
                "entities": ents or [],
                "sentiment": r["sentiment"],
                "importance": r["importance"] or 0.0,
                "source_name": r["source_name"],
                "published_at": r["published_at"],
                "wwww": {"where": r["where_info"]},
            })
            for s in sigs:
                s.cluster_id = cluster_id
            await persist_signals(conn, sigs)
            processed += 1

    # Summary — what got built, and the strongest entities / pairs so far.
    signals = await db.fetchval("SELECT COUNT(*) FROM domain_signals")
    entities = await db.fetchval("SELECT COUNT(*) FROM entities")
    cooc = await db.fetchval("SELECT COUNT(*) FROM cooccurrence")
    fingerprints = await db.fetchval("SELECT COUNT(*) FROM article_fingerprints")
    clusters = await db.fetchval("SELECT COUNT(DISTINCT cluster_id) FROM article_fingerprints")
    top_entities = await db.fetch(
        "SELECT canonical_name, mention_count FROM entities "
        "ORDER BY mention_count DESC LIMIT 10"
    )
    top_pairs = await db.fetch(
        """
        SELECT ea.canonical_name AS a, eb.canonical_name AS b, SUM(c.count) AS n
        FROM cooccurrence c
        JOIN entities ea ON ea.id = c.entity_a
        JOIN entities eb ON eb.id = c.entity_b
        GROUP BY ea.canonical_name, eb.canonical_name
        ORDER BY n DESC LIMIT 10
        """
    )
    return {
        "info_objects_processed": processed,
        "domain_signals": int(signals or 0),
        "entities": int(entities or 0),
        "cooccurrence_rows": int(cooc or 0),
        "fingerprints": int(fingerprints or 0),
        "story_clusters": int(clusters or 0),
        "dedup_ratio": round(1 - (int(clusters or 0) / int(fingerprints)), 3) if fingerprints else 0,
        "top_entities": [dict(r) for r in top_entities],
        "top_pairs": [dict(r) for r in top_pairs],
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Backfill domain_signals from info_objects.")
    parser.add_argument("--limit", type=int, default=None,
                        help="process only the N most-recent info_objects")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(limit=args.limit)
        log.info("signals backfill: %s", result)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
