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

log = logging.getLogger("sherbyte.worker")

# app.config and app.db are imported inside the functions that use them, not at
# package level. Importing this package is what a caller does to reach ONE
# worker, and a package-level `from app.config import settings` makes that pull
# pydantic-settings — which the ROOT service does not ship. Keeping it lazy is
# what lets main.py's /admin/replay-signals import app.workers.market_signals
# and call its backfill directly, instead of the endpoint carrying its own copy
# of the Signal shape. Both names are only ever used inside functions anyway.


async def bootstrap() -> None:
    from app.db import db, run_migrations
    await db.connect()
    await run_migrations()


async def teardown() -> None:
    from app.db import close_redis, db
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

    from app.config import settings

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
