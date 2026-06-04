"""
workers package — ARQ background workers.

Each worker is runnable two ways:
  • As an ARQ task (see WorkerSettings) driven by a Redis queue.
  • As a standalone script for GitHub-Actions cron, e.g.
        python -m app.workers.ingest_worker

`bootstrap()` opens the DB pool so standalone runs have everything they need.
"""

from __future__ import annotations

import logging

from app.config import settings
from app.db import db, run_migrations

log = logging.getLogger("sherbyte.worker")


async def bootstrap() -> None:
    await db.connect()
    await run_migrations()


async def teardown() -> None:
    from app.db import close_redis
    await close_redis()
    await db.disconnect()


# ─── ARQ worker settings ──────────────────────────────────────────────────────
async def _startup(ctx) -> None:
    await bootstrap()


async def _shutdown(ctx) -> None:
    await teardown()


def _arq_settings():
    """Build ARQ WorkerSettings lazily (arq import is optional for cron-only use)."""
    from arq import cron
    from arq.connections import RedisSettings

    from app.workers.als_worker import run as als_run
    from app.workers.embed_worker import run as embed_run
    from app.workers.ingest_worker import run as ingest_run
    from app.workers.signal_worker import run as signal_run

    class WorkerSettings:
        redis_settings = RedisSettings.from_dsn(settings.redis_url)
        on_startup = _startup
        on_shutdown = _shutdown
        functions = [ingest_run, embed_run, als_run, signal_run]
        cron_jobs = [
            cron(ingest_run, minute={0, 15, 30, 45}),       # every 15 min
            cron(embed_run, minute=set(range(0, 60, 5))),   # every 5 min
            cron(signal_run, minute={10, 40}),              # twice hourly
            cron(als_run, hour={3}, minute={0}),            # nightly retrain
        ]

    return WorkerSettings


WorkerSettings = None
try:  # only materialize if arq is installed
    WorkerSettings = _arq_settings()
except Exception:  # pragma: no cover
    pass
