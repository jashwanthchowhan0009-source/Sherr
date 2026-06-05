"""
pipeline/connector.py — Stage 04: CONNECT.

Threads info objects into evolving stories using pgvector cosine search over
thread centroids (HNSW-indexed):

  • Find the most similar recent thread to the new object's embedding.
  • If similarity ≥ STORY_LINK_THRESHOLD, attach it as a new node on that thread's
    timeline, linked to the latest existing node (backstory edge), and roll the
    centroid forward.
  • Otherwise, open a fresh thread seeded by this object.

The result is the story_threads / story_nodes graph that powers /sherr/thread
and timeline views.
"""

from __future__ import annotations

import asyncio
import logging

from app.config import settings
from app.db.supabase import db

log = logging.getLogger("sherbyte.connector")


async def _nearest_thread(conn, embedding: list[float]) -> tuple[str | None, float]:
    """Return (thread_id, cosine_similarity) for the closest recent thread."""
    row = await conn.fetchrow(
        """
        SELECT id, 1 - (centroid <=> $1) AS similarity
        FROM story_threads
        WHERE centroid IS NOT NULL
          AND updated_at > now() - ($2 || ' hours')::interval
        ORDER BY centroid <=> $1
        LIMIT 1
        """,
        embedding, str(settings.story_link_window_hours),
    )
    if not row:
        return None, 0.0
    return str(row["id"]), float(row["similarity"])


async def _attach_node(conn, thread_id: str, info_object_id: str, similarity: float) -> str:
    parent = await conn.fetchrow(
        "SELECT id, position FROM story_nodes WHERE thread_id=$1 ORDER BY position DESC LIMIT 1",
        thread_id,
    )
    parent_id = parent["id"] if parent else None
    position = (parent["position"] + 1) if parent else 0
    node = await conn.fetchrow(
        """
        INSERT INTO story_nodes
            (thread_id, info_object_id, parent_node_id, position, similarity)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (thread_id, info_object_id) DO NOTHING
        RETURNING id
        """,
        thread_id, info_object_id, parent_id, position, similarity,
    )
    # Roll the centroid forward as the running mean, bump counters.
    await conn.execute(
        """
        UPDATE story_threads
        SET centroid = (
                SELECT AVG(io.embedding)::vector
                FROM story_nodes sn JOIN info_objects io ON io.id = sn.info_object_id
                WHERE sn.thread_id = $1 AND io.embedding IS NOT NULL
            ),
            node_count = node_count + 1,
            updated_at = now()
        WHERE id = $1
        """,
        thread_id,
    )
    await conn.execute("UPDATE info_objects SET thread_id=$1 WHERE id=$2",
                       thread_id, info_object_id)
    return str(node["id"]) if node else ""


async def _open_thread(conn, info_object_id: str, embedding: list[float],
                       title: str, topic: str, pillar_id: int) -> str:
    thread = await conn.fetchrow(
        """
        INSERT INTO story_threads (title, topic, pillar_id, centroid, node_count)
        VALUES ($1,$2,$3,$4,1)
        RETURNING id
        """,
        title, topic, pillar_id, embedding,
    )
    thread_id = str(thread["id"])
    await conn.execute(
        """
        INSERT INTO story_nodes (thread_id, info_object_id, position, similarity)
        VALUES ($1,$2,0,1.0)
        ON CONFLICT (thread_id, info_object_id) DO NOTHING
        """,
        thread_id, info_object_id,
    )
    await conn.execute("UPDATE info_objects SET thread_id=$1 WHERE id=$2",
                       thread_id, info_object_id)
    return thread_id


async def connect(info_object_id: str) -> str:
    """Thread one (already-embedded) info object. Returns its thread_id."""
    obj = await db.fetchrow(
        "SELECT headline, topic, pillar_id, embedding FROM info_objects WHERE id=$1",
        info_object_id,
    )
    if not obj or obj["embedding"] is None:
        log.debug("connect skipped (no embedding) for %s", info_object_id)
        return ""

    embedding = obj["embedding"]
    async with db.acquire() as conn:
        thread_id, sim = await _nearest_thread(conn, embedding)
        if thread_id and sim >= settings.story_link_threshold:
            await _attach_node(conn, thread_id, info_object_id, sim)
            log.debug("linked %s → thread %s (sim=%.3f)", info_object_id, thread_id, sim)
            return thread_id
        new_id = await _open_thread(
            conn, info_object_id, embedding,
            title=obj["headline"], topic=obj["topic"], pillar_id=obj["pillar_id"],
        )
        log.debug("opened thread %s for %s", new_id, info_object_id)
        return new_id


async def connect_pending(limit: int = 256, concurrency: int = 5) -> int:
    """Thread any embedded objects not yet on a thread (used by workers)."""
    rows = await db.fetch(
        """
        SELECT id FROM info_objects
        WHERE thread_id IS NULL AND embedding IS NOT NULL
        ORDER BY published_at ASC LIMIT $1
        """,
        limit,
    )
    if not rows:
        return 0
    # Thread concurrently (bounded) rather than one-at-a-time: each connect()
    # makes several DB round-trips, so a sequential loop dominated cycle time.
    sem = asyncio.Semaphore(concurrency)

    async def _guarded(info_id: str) -> None:
        async with sem:
            await connect(info_id)

    await asyncio.gather(*[_guarded(str(r["id"])) for r in rows])
    log.info("[CONNECT] threaded %d info objects", len(rows))
    return len(rows)
