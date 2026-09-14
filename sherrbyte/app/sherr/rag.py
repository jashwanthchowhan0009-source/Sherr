"""
sherr/rag.py — RAG-consensus retrieval.

Before Sherr writes, we ground it: pull the most semantically similar info
objects (excluding the subject itself) via pgvector cosine search and feed their
summaries in as "source consensus". This keeps generations factual and lets the
DEEP/EXPLAINER modes synthesize across multiple reports of the same story.

Prefers same-thread members (Connect stage) and falls back to global vector
nearest-neighbours.
"""

from __future__ import annotations

import logging

from app.db.supabase import db

log = logging.getLogger("sherbyte.rag")


async def retrieve_consensus(info_object_id: str, k: int = 5) -> list[str]:
    """Return up to k corroborating source summaries for grounding."""
    # The subject's embedding is NOT pulled into Python — only a presence flag.
    # A 1536-float vector is ~6 KB, and fetching it here just to send it back as
    # a bind parameter for the neighbour search was pure Supabase egress. The
    # neighbour query below references it by id through an in-DB subquery instead.
    subject = await db.fetchrow(
        "SELECT thread_id, source_name, summary, (embedding IS NOT NULL) AS has_embedding "
        "FROM info_objects WHERE id=$1",
        info_object_id,
    )
    if not subject:
        return []

    consensus: list[str] = []
    seen: set[str] = set()

    # 1) Same-thread siblings (strongest signal — Connect already linked them).
    if subject["thread_id"]:
        sib = await db.fetch(
            """
            SELECT io.source_name, io.summary
            FROM story_nodes sn JOIN info_objects io ON io.id = sn.info_object_id
            WHERE sn.thread_id=$1 AND io.id <> $2 AND io.summary <> ''
            ORDER BY io.published_at DESC LIMIT $3
            """,
            subject["thread_id"], info_object_id, k,
        )
        for r in sib:
            line = f"{r['source_name']}: {r['summary']}"
            if line not in seen:
                seen.add(line)
                consensus.append(line)

    # 2) Top up with global vector neighbours. The subject vector is referenced
    #    by id in-DB, so the pgvector search runs entirely server-side and no
    #    embedding column ever crosses the wire.
    if len(consensus) < k and subject["has_embedding"]:
        nn = await db.fetch(
            """
            SELECT source_name, summary,
                   1 - (embedding <=> (SELECT embedding FROM info_objects WHERE id=$1)) AS sim
            FROM info_objects
            WHERE id <> $1 AND embedding IS NOT NULL AND summary <> ''
            ORDER BY embedding <=> (SELECT embedding FROM info_objects WHERE id=$1)
            LIMIT $2
            """,
            info_object_id, k * 2,
        )
        for r in nn:
            if r["sim"] < 0.5:
                continue
            line = f"{r['source_name']}: {r['summary']}"
            if line not in seen:
                seen.add(line)
                consensus.append(line)
            if len(consensus) >= k:
                break

    log.debug("RAG retrieved %d consensus sources for %s", len(consensus), info_object_id)
    return consensus[:k]
