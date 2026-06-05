"""
db/supabase.py — Postgres (Supabase) access layer.

Supabase is managed Postgres, so for the hot path (vector search, joins, writes)
we talk to it directly through an asyncpg connection pool — that's what gives us
first-class pgvector support. The supabase-py client is exposed separately for
the things it does best: auth helpers and Storage.

Usage:
    from app.db.supabase import db
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT ...")

    # convenience wrappers
    rows = await db.fetch("SELECT ...", arg1)
    row  = await db.fetchrow("SELECT ... WHERE id=$1", some_id)
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

import asyncpg

from app.config import settings

log = logging.getLogger("sherbyte.db")


class Database:
    """Thin async wrapper around an asyncpg pool with pgvector registered."""

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is not None:
            return
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is not configured")

        self._pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
            init=self._init_connection,
            # 60s ceilings so the Supabase pooler doesn't time out during the
            # heavy initial sync: `timeout` caps connection acquisition + the
            # handshake, `command_timeout` caps each query.
            timeout=60.0,
            command_timeout=60.0,
            # Supabase's connection pooler (pgbouncer, transaction mode on
            # port 6543 — the IPv4 endpoint Render needs) does not keep the
            # same backend across statements, so server-side prepared-statement
            # caching breaks ("prepared statement does not exist"). Disabling
            # the cache makes asyncpg prepare+execute+discard within one
            # protocol round-trip, which is pooler-safe. Harmless on a direct
            # connection — it only forgoes a minor caching optimisation.
            statement_cache_size=0,
        )
        log.info("Postgres pool ready (min=%d max=%d)",
                 settings.db_pool_min, settings.db_pool_max)

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection) -> None:
        """Register the pgvector codec so vectors round-trip as Python lists."""
        try:
            from pgvector.asyncpg import register_vector
            await register_vector(conn)
        except Exception as e:  # pgvector extension not installed yet, or lib missing
            log.warning("pgvector codec not registered: %s", e)

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            log.info("Postgres pool closed")

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool not initialised — call connect() first")
        return self._pool

    @asynccontextmanager
    async def acquire(self):
        async with self.pool.acquire() as conn:
            yield conn

    # ─── Convenience query helpers ────────────────────────────────────────────
    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args) -> str:
        async with self.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args_iter) -> None:
        async with self.acquire() as conn:
            await conn.executemany(query, args_iter)

    async def healthcheck(self) -> bool:
        try:
            return (await self.fetchval("SELECT 1")) == 1
        except Exception as e:
            log.error("DB healthcheck failed: %s", e)
            return False


db = Database()


# ─── supabase-py client (auth / storage) ──────────────────────────────────────
_supabase_client = None


def get_supabase():
    """Lazy singleton for the supabase-py client. Returns None if unconfigured."""
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    if not (settings.supabase_url and settings.supabase_key):
        return None
    try:
        from supabase import create_client
        _supabase_client = create_client(settings.supabase_url, settings.supabase_key)
        return _supabase_client
    except Exception as e:
        log.warning("supabase-py client unavailable: %s", e)
        return None
