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
    subject = await db.fetchrow(
        "SELECT embedding, thread_id, source_name, summary FROM info_objects WHERE id=$1",
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

    # 2) Top up with global vector neighbours.
    if len(consensus) < k and subject["embedding"] is not None:
        nn = await db.fetch(
            """
            SELECT source_name, summary, 1 - (embedding <=> $1) AS sim
            FROM info_objects
            WHERE id <> $2 AND embedding IS NOT NULL AND summary <> ''
            ORDER BY embedding <=> $1
            LIMIT $3
            """,
            subject["embedding"], info_object_id, k * 2,
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
