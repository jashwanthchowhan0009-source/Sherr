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

import inspect
import itertools
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

from app.config import settings

log = logging.getLogger("sherbyte.db")


# Query params that belong to SQLAlchemy's asyncpg dialect or PgBouncer, NOT to
# raw asyncpg — passing them in the DSN raises "unexpected connection parameter".
# We strip them (the equivalents are set as real create_pool kwargs below).
_UNSUPPORTED_QS = {
    "pgbouncer", "prepared_statement_cache_size", "statement_cache_size",
    "prepared_statements", "prepare_threshold",
}


def _sanitize_dsn(dsn: str) -> str:
    """Drop query params raw asyncpg can't parse (pgbouncer / SQLAlchemy-only ones)
    so a pasted Supabase *pooler* URL connects cleanly. sslmode and the rest are
    left intact (asyncpg understands them)."""
    parts = urlsplit(dsn)
    if not parts.query:
        return dsn
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in _UNSUPPORTED_QS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))


# Unique prepared-statement names so two pooled backends never collide on a
# generated name under pgbouncer transaction mode.
_stmt_counter = itertools.count()


def _unique_stmt_name(*_args, **_kwargs) -> str:
    return f"__sherr_stmt_{os.getpid()}_{next(_stmt_counter)}__"


# Only pass prepared_statement_name_func if this asyncpg version accepts it.
try:
    _SUPPORTS_STMT_NAME_FUNC = "prepared_statement_name_func" in inspect.signature(
        asyncpg.connect
    ).parameters
except (ValueError, TypeError):
    _SUPPORTS_STMT_NAME_FUNC = False


class Database:
    """Thin async wrapper around an asyncpg pool with pgvector registered."""

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is not None:
            return
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is not configured")

        dsn = _sanitize_dsn(settings.database_url)

        # asyncpg + Supabase's transaction pooler (pgbouncer, port 6543) needs two
        # things together to be safe:
        #   1. statement_cache_size=0 — pgbouncer transaction mode doesn't keep the
        #      same backend across statements, so cached server-side prepared
        #      statements vanish ("prepared statement does not exist"). Disabling the
        #      cache makes asyncpg prepare+execute+discard in one round-trip.
        #   2. unique prepared-statement names — because two pooled backends can
        #      otherwise pick the same generated name ("prepared statement __asyncpg…
        #      already exists"). Guarded: only passed if this asyncpg supports it.
        kwargs = dict(
            dsn=dsn,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
            init=self._init_connection,
            timeout=60.0,          # cap connection acquisition + handshake
            command_timeout=60.0,  # cap each query
            statement_cache_size=0,
        )
        if _SUPPORTS_STMT_NAME_FUNC:
            kwargs["prepared_statement_name_func"] = _unique_stmt_name

        try:
            self._pool = await asyncpg.create_pool(**kwargs)
        except TypeError:
            # Older asyncpg without prepared_statement_name_func — retry without it.
            kwargs.pop("prepared_statement_name_func", None)
            self._pool = await asyncpg.create_pool(**kwargs)

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
