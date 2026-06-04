"""
db package — Postgres pool, Redis client, and the migration runner.
"""

from __future__ import annotations

import logging
from pathlib import Path

from app.db.supabase import db, get_supabase
from app.db.redis import get_redis, close_redis

log = logging.getLogger("sherbyte.db")

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def run_migrations() -> None:
    """Apply every *.sql file in migrations/ in lexical order.

    Each file is written to be idempotent (IF NOT EXISTS / guarded DO blocks),
    so re-running on every boot is safe and keeps dev/prod schemas in sync.
    """
    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        log.warning("No migration files found in %s", _MIGRATIONS_DIR)
        return

    async with db.acquire() as conn:
        for f in files:
            sql = f.read_text(encoding="utf-8")
            try:
                await conn.execute(sql)
                log.info("migration applied: %s", f.name)
            except Exception as e:
                log.error("migration FAILED (%s): %s", f.name, e)
                raise


__all__ = ["db", "get_supabase", "get_redis", "close_redis", "run_migrations"]
