"""cache.py — the read cache that stands between 1000+ readers and one Postgres.

THE CONSTRAINT IS CONNECTIONS, NOT CPU. Supabase's transaction pooler and
Render's free tier both cap concurrent connections in the low tens. A feed that
goes to the database once per reader does not fail gradually under load — it
exhausts the pool and every request starts timing out at once, including the
admin endpoints you would use to find out why.

So the three endpoints a crowd actually hits — the feed, /patterns and the
analog cards — answer from here, and only a cache MISS reaches Postgres. At a
30-second TTL, a thousand readers a minute cost two queries.

TWO LAYERS, AND THE SECOND IS NOT OPTIONAL:

  1. Redis (Upstash / Render Key-Value), shared by every process. Survives a
     restart, and is what makes the cache useful across more than one worker.
  2. A per-process dict with the same TTL. This is the layer that actually saves
     the database when Redis is missing or has just gone down, which is exactly
     the moment a stampede would otherwise arrive. A cache whose only layer is
     remote fails open onto the thing it was protecting.

Redis is OPTIONAL and every helper degrades to the local layer rather than
raising: a cache is never allowed to be the reason a request fails.

The value is always JSON. Nothing here stores a Python object, so a cached
payload cannot pin a database row, a connection, or a model in memory.
"""

from __future__ import annotations

import json
import logging
import os
import time

log = logging.getLogger("sherbyte.cache")

REDIS_URL = (os.getenv("UPSTASH_REDIS_URL")
             or os.getenv("REDIS_URL")
             or "").strip()
# One prefix per deploy generation. Bumping it invalidates everything at once,
# which is the only safe way to ship a payload shape change against a warm cache.
PREFIX = os.getenv("CACHE_PREFIX", "sherr:v1")
# Ceiling on Redis sockets from this process. Without it a burst opens one
# connection per concurrent request and the cache becomes its own bottleneck.
MAX_CONNECTIONS = int(os.getenv("CACHE_MAX_CONNECTIONS", "16"))
ENABLED = os.getenv("CACHE_ENABLED", "1") not in ("0", "false", "no")

_client = None
_client_tried = False
_redis_ok = True

# key -> (expires_at, payload). Bounded, because an unbounded local cache on a
# 512MB free instance is a memory leak with a TTL.
_local: dict = {}
_LOCAL_MAX = int(os.getenv("CACHE_LOCAL_MAX", "512"))

STATS = {"hit_local": 0, "hit_redis": 0, "miss": 0, "set": 0,
         "errors": 0, "redis": "unconfigured"}


def _k(key: str) -> str:
    return f"{PREFIX}:{key}"


async def _redis():
    """The shared client, or None. Built once; never rebuilt on failure."""
    global _client, _client_tried, _redis_ok
    if not ENABLED or not REDIS_URL:
        STATS["redis"] = "unconfigured" if not REDIS_URL else "disabled"
        return None
    if _client_tried:
        return _client if _redis_ok else None
    _client_tried = True
    try:
        import redis.asyncio as aioredis                          # noqa: PLC0415
        _client = aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True,
            max_connections=MAX_CONNECTIONS,
            socket_connect_timeout=2, socket_timeout=2,
            health_check_interval=30, retry_on_timeout=True,
        )
        STATS["redis"] = "connected"
    except Exception as e:                                        # noqa: BLE001
        log.warning("cache: Redis unavailable, local layer only: %s", e)
        STATS["redis"] = f"unavailable: {type(e).__name__}"
        _client, _redis_ok = None, False
    return _client


def _local_get(key: str):
    hit = _local.get(key)
    if not hit:
        return None
    expires, payload = hit
    if expires < time.time():
        _local.pop(key, None)
        return None
    return payload


def _local_set(key: str, payload, ttl: int) -> None:
    if len(_local) >= _LOCAL_MAX:
        # Evict the soonest to expire — cheap, and correct for a TTL cache.
        try:
            oldest = min(_local, key=lambda k: _local[k][0])
            _local.pop(oldest, None)
        except ValueError:                                        # pragma: no cover
            _local.clear()
    _local[key] = (time.time() + ttl, payload)


async def get(key: str):
    """The cached payload, or None. Never raises."""
    if not ENABLED:
        return None
    payload = _local_get(key)
    if payload is not None:
        STATS["hit_local"] += 1
        return payload
    client = await _redis()
    if client is None:
        STATS["miss"] += 1
        return None
    try:
        raw = await client.get(_k(key))
    except Exception as e:                                        # noqa: BLE001
        global _redis_ok
        if _redis_ok:
            log.warning("cache: Redis read failed, degrading to local: %s", e)
        _redis_ok = False
        STATS["errors"] += 1
        STATS["redis"] = f"degraded: {type(e).__name__}"
        STATS["miss"] += 1
        return None
    if raw is None:
        STATS["miss"] += 1
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        STATS["miss"] += 1
        return None
    STATS["hit_redis"] += 1
    return payload


async def set(key: str, payload, ttl: int) -> None:                # noqa: A001
    """Store a JSON-serialisable payload under both layers. Never raises."""
    if not ENABLED or payload is None:
        return
    _local_set(key, payload, ttl)
    STATS["set"] += 1
    client = await _redis()
    if client is None:
        return
    try:
        await client.set(_k(key), json.dumps(payload, default=str), ex=ttl)
    except Exception as e:                                        # noqa: BLE001
        global _redis_ok
        if _redis_ok:
            log.warning("cache: Redis write failed: %s", e)
        _redis_ok = False
        STATS["errors"] += 1


async def get_or_set(key: str, ttl: int, producer):
    """The cached payload, or `producer()`'s answer, stored and returned.

    A PRODUCER FAILURE IS NOT CACHED. Storing an error under a 30-second TTL
    turns one bad query into thirty seconds of guaranteed-wrong answers for
    every reader; the exception propagates to the endpoint, which already knows
    how to say "unavailable" honestly.
    """
    hit = await get(key)
    if hit is not None:
        return hit
    payload = producer()
    if hasattr(payload, "__await__"):
        payload = await payload
    await set(key, payload, ttl)
    return payload


async def close() -> None:
    global _client, _client_tried
    if _client is not None:
        try:
            await _client.aclose()
        except Exception:                                         # noqa: BLE001
            pass
    _client, _client_tried = None, False


def stats() -> dict:
    total = STATS["hit_local"] + STATS["hit_redis"] + STATS["miss"]
    out = dict(STATS)
    out["hit_rate"] = (round((STATS["hit_local"] + STATS["hit_redis"]) / total, 3)
                       if total else None)
    out["local_keys"] = len(_local)
    out["enabled"] = ENABLED
    out["max_connections"] = MAX_CONNECTIONS
    return out
