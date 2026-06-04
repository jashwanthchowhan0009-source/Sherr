"""
sherr/core.py — Sherr orchestrator (Stage 06: NARRATE).

Ties the writer engine together:
    info_object_id + mode
        → load the info object
        → retrieve RAG consensus
        → generate the narrative (writer)
        → apply SGI watermark
        → cache and return.

This is the single entry point the API layer calls.
"""

from __future__ import annotations

import logging

from app.config import settings
from app.db.redis import cache_get, cache_set
from app.db.supabase import db
from app.sherr import rag, watermark, writer
from app.sherr.prompts import normalize_mode
from app.sherr.router import provider_status

log = logging.getLogger("sherbyte.sherr")

_CACHE_TTL = 3600  # generations are deterministic enough to cache for an hour


async def _load_info_object(info_object_id: str):
    return await db.fetchrow(
        """
        SELECT id, headline, summary, body, who, what, where_info, when_info,
               why_info, thread_id, source_name
        FROM info_objects WHERE id=$1
        """,
        info_object_id,
    )


async def generate(info_object_id: str, mode: str = "BRIEF",
                   use_cache: bool = True) -> dict:
    """Generate a watermarked Sherr narrative for an info object."""
    mode = normalize_mode(mode)
    cache_key = f"sherr:{info_object_id}:{mode}"

    if use_cache:
        cached = await cache_get(cache_key)
        if cached:
            return cached

    obj = await _load_info_object(info_object_id)
    if not obj:
        raise ValueError(f"info object {info_object_id} not found")

    consensus = await rag.retrieve_consensus(info_object_id, k=5)
    wwww = {
        "who": obj["who"], "what": obj["what"], "where": obj["where_info"],
        "when": obj["when_info"], "why": obj["why_info"],
    }

    text = await writer.write(
        headline=obj["headline"], summary=obj["summary"], body=obj["body"],
        wwww=wwww, consensus=consensus, mode=mode,
    )

    envelope = watermark.apply(
        text, mode=mode,
        model=provider_status()["model"],
        info_object_id=info_object_id,
    )
    envelope["mode"] = mode
    envelope["sources_used"] = len(consensus)

    if use_cache:
        await cache_set(cache_key, envelope, ttl=_CACHE_TTL)
    return envelope


async def brief(info_object_id: str) -> dict:
    return await generate(info_object_id, mode="BRIEF")


async def explain(info_object_id: str) -> dict:
    return await generate(info_object_id, mode="EXPLAINER")


async def thread_narrative(info_object_id: str) -> dict:
    return await generate(info_object_id, mode="THREAD")
