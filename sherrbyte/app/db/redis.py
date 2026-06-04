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


# Redis is OPTIONAL. If REDIS_URL is unreachable, every helper here degrades to
# a safe no-op so the app runs on Postgres alone (caching off, dedup falls back
# to the DB layers). This keeps free-tier / single-service deploys trivial.
_REDIS_OK = True


def _redis_down(exc: Exception) -> None:
    global _REDIS_OK
    if _REDIS_OK:
        log.warning("Redis unavailable, degrading to no-op cache/dedup: %s", exc)
        _REDIS_OK = False


# ─── Typed JSON cache helpers ─────────────────────────────────────────────────
async def cache_get(key: str) -> Optional[Any]:
    try:
        raw = await get_redis().get(key)
    except Exception as e:
        _redis_down(e)
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


async def cache_set(key: str, value: Any, ttl: int = 60) -> None:
    payload = value if isinstance(value, str) else json.dumps(value, default=str)
    try:
        await get_redis().set(key, payload, ex=ttl)
    except Exception as e:
        _redis_down(e)


async def cache_delete(*keys: str) -> None:
    if not keys:
        return
    try:
        await get_redis().delete(*keys)
    except Exception as e:
        _redis_down(e)


# ─── Dedup membership set (layer 1) ───────────────────────────────────────────
_DEDUP_SET = "dedup:urls"


async def seen_before(member: str, bucket: str = _DEDUP_SET) -> bool:
    """True if member already in the set, else add it. Returns False if Redis is
    down — the deduplicator then relies on its DB-backed layers 1-3."""
    try:
        added = await get_redis().sadd(bucket, member)
        return added == 0
    except Exception as e:
        _redis_down(e)
        return False


async def mark_seen(member: str, bucket: str = _DEDUP_SET) -> None:
    try:
        await get_redis().sadd(bucket, member)
    except Exception as e:
        _redis_down(e)
