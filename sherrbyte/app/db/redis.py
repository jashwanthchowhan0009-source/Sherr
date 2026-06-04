"""
db/redis.py — Redis client.

Three roles in SherByte:
  1. Hot cache for feed pages, trending lists, and rendered Sherr briefs.
  2. Fast dedup membership set (layer 1 of the 3-layer deduplicator).
  3. Backing store for the ARQ task queue (see app/workers).

The client is a lazy singleton so importing this module never opens a socket.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import redis.asyncio as aioredis

from app.config import settings

log = logging.getLogger("sherbyte.redis")

_client: Optional[aioredis.Redis] = None


def get_redis() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# ─── Typed JSON cache helpers ─────────────────────────────────────────────────
async def cache_get(key: str) -> Optional[Any]:
    raw = await get_redis().get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


async def cache_set(key: str, value: Any, ttl: int = 60) -> None:
    payload = value if isinstance(value, str) else json.dumps(value, default=str)
    await get_redis().set(key, payload, ex=ttl)


async def cache_delete(*keys: str) -> None:
    if keys:
        await get_redis().delete(*keys)


# ─── Dedup membership set (layer 1) ───────────────────────────────────────────
_DEDUP_SET = "dedup:urls"
_DEDUP_HASHES = "dedup:hashes"


async def seen_before(member: str, bucket: str = _DEDUP_SET) -> bool:
    """Return True if member already in the set, else add it and return False."""
    added = await get_redis().sadd(bucket, member)
    return added == 0


async def mark_seen(member: str, bucket: str = _DEDUP_SET) -> None:
    await get_redis().sadd(bucket, member)
