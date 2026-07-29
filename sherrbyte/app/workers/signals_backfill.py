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


# Tables derived entirely from articles/info_objects — safe to wipe and rebuild so
# a re-run is deterministic (no accumulation from a previous partial/duplicate run).
# Tables this worker rebuilds from articles/info_objects. `insights` is NOT here:
# it is DETECTOR output and the user-visible product. A crashed signals backfill
# must never destroy it (the detectors rewrite their own rows via signature).
_DERIVED_TABLES = [
    "cooccurrence_events", "cooccurrence", "domain_signals",
    "article_fingerprints", "entity_aliases", "entities",
]


async def reset_derived(conn) -> None:
    """Clear the signal-derived tables + restart the cluster sequence.

    DESTRUCTIVE and never automatic — only runs behind an explicit --reset (or the
    standalone `python -m app.workers.reset_spie`). A backfill that crashes halfway
    on a flaky pooler previously wiped the graph (and insights) on every attempt.
    """
    log.warning("reset_derived: TRUNCATE %s", ", ".join(_DERIVED_TABLES))
    await conn.execute("TRUNCATE " + ", ".join(_DERIVED_TABLES) + " RESTART IDENTITY CASCADE")
    await conn.execute("ALTER SEQUENCE IF EXISTS article_cluster_seq RESTART WITH 1")


async def run(limit: int | None = None, reset: bool = False, batch_size: int = 25) -> dict:
    from app.spie.knowledge.adapters.news import from_info_object
    from app.spie.knowledge.signals import persist_signals
    from app.spie.knowledge.entity_resolver import (
        seed_aliases, is_valid_mention, filter_stats, reset_filter_stats,
        RESOLVER_BUILD,
    )
    from app.spie.knowledge.simhash import assign_cluster, SimHashIndex

    # Prove which code is running (if this marker is missing from the logs/summary,
    # the deployed image is stale and no amount of re-running will change the data).
    log.info("signals_backfill using %s", RESOLVER_BUILD)
    reset_filter_stats()
    raw_body_hits = 0      # rows whose text came from articles.body (the raw source)
    raw_body_missing = 0   # rows that fell back to info_objects.body/summary

    # Fingerprint on the RAW article body (articles.body) — the AI-touched
    # info_objects.body diverges per outlet and would hide wire duplicates.
    # RESUMABLE: skip info_objects that already produced a signal. Without a reset
    # this makes a re-run continue where a crashed one stopped instead of
    # duplicating signals — the behaviour needed on a flaky free-tier pooler.
    q = ("SELECT io.id, io.article_id, io.headline, "
         "COALESCE(a.body, io.body, io.summary, '') AS raw_body, "
         "COALESCE(length(a.body), 0) AS a_body_len, "
         "io.entities, io.sentiment, io.importance, io.source_name, "
         "io.where_info, io.published_at "
         "FROM info_objects io LEFT JOIN articles a ON a.id = io.article_id "
         "WHERE io.entities IS NOT NULL "
         "AND NOT EXISTS (SELECT 1 FROM domain_signals ds "
         "                WHERE ds.ref_id = io.id::text AND ds.domain = 'news') "
         "ORDER BY io.published_at DESC")
    if limit is not None:
        q += f" LIMIT {int(limit)}"

    # Setup (reset + seed + index load + row fetch) on its own short-lived
    # connection — never held across the processing loop.
    async with db.acquire() as conn:
        if reset:
            await reset_derived(conn)
            await seed_aliases(conn)          # re-seed curated aliases after wipe
        # Batch dedup index: one load, banded in-memory lookups. Without this a
        # full backfill re-queries thousands of fingerprints per article AND the
        # scan cap starts silently missing merges past ~4000 fingerprints.
        index = await SimHashIndex.load(conn)
        rows = await conn.fetch(q)

    processed = 0
    failed = 0
    # Process in batches, each on a FRESH connection. Holding one connection for
    # the whole corpus is what produced "connection has been released back to the
    # pool" — long-lived connections hit the pool/command timeout (and pgbouncer
    # transaction mode recycles the backend underneath). The SimHash index is
    # in-memory, so it carries across batches untouched.
    for start in range(0, len(rows), batch_size):
        chunk = rows[start:start + batch_size]
        try:
            async with db.acquire() as conn:
                for r in chunk:
                    ents = r["entities"]
                    if isinstance(ents, str):
                        try:
                            ents = json.loads(ents or "[]")
                        except Exception:
                            ents = []
                    # Stored info_objects.entities predate the junk filter, so filter
                    # them HERE too — not only inside resolve() — so junk never
                    # reaches the adapter/signal at all (defense in depth).
                    ents = [e for e in (ents or [])
                            if is_valid_mention(
                                (e.get("name") or e.get("canonical") or "") if isinstance(e, dict)
                                else getattr(e, "name", ""),
                                (e.get("type", "MISC") if isinstance(e, dict)
                                 else getattr(e, "type", "MISC")))]

                    # SimHash story cluster over the RAW body (dedup republication).
                    doc_id = r["article_id"] or r["id"]
                    if int(r["a_body_len"] or 0) > 0:
                        raw_body_hits += 1
                    else:
                        raw_body_missing += 1
                    text = f"{r['headline']} {r['raw_body']}"
                    cluster_id = None
                    try:
                        cluster_id = await assign_cluster(conn, doc_id, text, index=index)
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
                    # Skip inline co-occurrence: it costs round-trips per entity
                    # PAIR (~45 pairs/article) over the WAN. cooccurrence_backfill
                    # rebuilds the whole window in one pass right after this run.
                    await persist_signals(conn, sigs, conn_factory=db.acquire,
                                          update_cooccurrence=False)
                    processed += 1
        except Exception as e:
            # A dropped/recycled connection kills only its batch; the run continues
            # on a fresh one instead of aborting the whole backfill.
            failed += len(chunk)
            log.warning("batch at offset %d failed (%s) — continuing", start, e)
        if start and start % (batch_size * 10) == 0:
            log.info("signals_backfill: %d/%d processed", processed, len(rows))

    stats = filter_stats()

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
    # Coverage: is anything still un-backfilled? Counted with the same predicate the
    # worker selects on, so it stays correct across resumed (non-reset) runs.
    eligible = await db.fetchval(
        "SELECT COUNT(*) FROM info_objects WHERE entities IS NOT NULL"
    )
    remaining = await db.fetchval(
        "SELECT COUNT(*) FROM info_objects io WHERE io.entities IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM domain_signals ds "
        "                WHERE ds.ref_id = io.id::text AND ds.domain = 'news')"
    )
    return {
        # Build + filter proof. If resolver_build is missing/old, or
        # entities_filtered_out is 0 on real news data, the deployed code is stale.
        "resolver_build": RESOLVER_BUILD,
        "info_objects_eligible": int(eligible or 0),
        "info_objects_remaining": int(remaining or 0),
        # True once every eligible info_object has a signal — survives resumed runs
        # (re-run until this is true; each run picks up where the last stopped).
        "coverage_complete": (int(remaining or 0) == 0 and failed == 0),
        "rows_failed": failed,
        "reset_ran": reset,
        "batch_size": batch_size,
        "mentions_checked": stats["checked"],
        "entities_filtered_out": stats["filtered_out"],
        "raw_body_from_articles": raw_body_hits,
        "raw_body_fallback": raw_body_missing,
        "info_objects_processed": processed,
        "domain_signals": int(signals or 0),
        "entities": int(entities or 0),
        "cooccurrence_rows": int(cooc or 0),
        "fingerprints": int(fingerprints or 0),
        "story_clusters": int(clusters or 0),
        # articles per unique story: 1.0 = no duplicates, > 1.0 = wire dupes collapsed.
        "dedup_ratio": round(int(fingerprints) / int(clusters), 3) if clusters else 0.0,
        "top_entities": [dict(r) for r in top_entities],
        "top_pairs": [dict(r) for r in top_pairs],
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Backfill domain_signals from info_objects.")
    parser.add_argument("--limit", type=int, default=None,
                        help="process only the N most-recent info_objects")
    parser.add_argument("--reset", action="store_true",
                        help="DESTRUCTIVE: TRUNCATE the signal-derived tables first "
                             "(entities, aliases, fingerprints, domain_signals, cooccurrence). "
                             "Off by default so a crashed run never wipes the graph. "
                             "Never touches insights.")
    parser.add_argument("--no-reset", action="store_true",
                        help="(deprecated, now the default) kept so existing scripts keep working")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="rows per DB connection (default 25); lower if the pooler drops connections")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(limit=args.limit,
                           reset=(args.reset and not args.no_reset),
                           batch_size=max(1, args.batch_size))
        log.info("signals backfill: %s", result)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
