"""
main.py — FastAPI entry point.

Wires the lifespan (DB pool + migrations + Redis + optional in-process scheduler),
mounts every router, and exposes the static taxonomy + health endpoints.

Run:  uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from functools import lru_cache

from app.sherr import admin
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import PILLARS, settings
from app.db import close_redis, db, run_migrations
from app.sherr.router import provider_status

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sherbyte")

_scheduler = None


async def _ingest_job():
    """Background ingest cycle (imported lazily to keep startup light)."""
    from app.pipeline import run_cycle
    try:
        await run_cycle(understand_concurrency=settings.understand_concurrency)
    except Exception as e:
        log.error("ingest cycle failed: %s", e, exc_info=True)


async def _detectors_job():
    """Nightly pattern-detector pass (emergence, temporal correlation)."""
    from app.workers.detectors import run as run_detectors
    try:
        log.info("detectors: %s", await run_detectors())
    except Exception as e:
        log.error("detectors job failed: %s", e, exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await run_migrations()

    global _scheduler
    if settings.run_scheduler:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        _scheduler = AsyncIOScheduler()
        _scheduler.add_job(_ingest_job, "interval",
                           minutes=settings.collect_interval_min, id="ingest")
        # Detectors are heavy + materialized, so run once nightly (02:00 UTC).
        _scheduler.add_job(_detectors_job, "cron", hour=2, id="detectors")
        _scheduler.start()
        asyncio.create_task(_ingest_job())  # kick one cycle on boot
        log.info("Scheduler started: ingest every %d min, detectors nightly @02:00",
                 settings.collect_interval_min)

    log.info("AI providers: %s", provider_status())
    log.info("%s v%s ready (env=%s)", settings.app_name, settings.app_version, settings.env)
    yield

    if _scheduler:
        _scheduler.shutdown(wait=False)
    await close_redis()
    await db.disconnect()


app = FastAPI(title=settings.app_name, version=settings.app_version, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────
from app.api import (  # noqa: E402  (import after app to avoid cycles)
    activity, article, auth, compat, feed, live, markets, signal, sherr, push,
    notifications, patterns,
)

app.include_router(patterns.router)
app.include_router(push.router)
app.include_router(notifications.router)
app.include_router(auth.router)
app.include_router(feed.router)
app.include_router(article.router)
app.include_router(signal.router)
app.include_router(sherr.router)
app.include_router(markets.router)
app.include_router(live.router)
app.include_router(activity.router)
# MVP-compatibility routes (flat /login, /feed, /interact, …) for the v5 frontend.
app.include_router(compat.router)
app.include_router(admin.router, prefix="/admin", tags=["Admin Maintenance"])

# ─── Static taxonomy + health ─────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _pillars_payload() -> dict:
    return {"pillars": [{**v, "id": k} for k, v in PILLARS.items()]}


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"service": settings.app_name, "version": settings.app_version, "status": "ok"}


@app.api_route("/ping", methods=["GET", "HEAD"])
async def ping():
    """Ultra-light keep-alive endpoint. Accepts GET and HEAD, no DB, always 200 —
    so uptime pingers (e.g. UptimeRobot) keep the free instance awake and report
    it as 'Up' regardless of the HTTP method they use."""
    return {"ok": True}


@app.get("/pillars")
async def get_pillars():
    return _pillars_payload()


@app.get("/topics")
async def get_topics():
    return _pillars_payload()


@app.get("/health")
async def health():
    db_ok = await db.healthcheck()
    counts = {}
    if db_ok:
        try:
            counts = {
                "articles": await db.fetchval("SELECT COUNT(*) FROM articles"),
                "info_objects": await db.fetchval("SELECT COUNT(*) FROM info_objects"),
                "threads": await db.fetchval("SELECT COUNT(*) FROM story_threads"),
                "users": await db.fetchval("SELECT COUNT(*) FROM users"),
            }
        except Exception:
            pass
    return {
        "status": "ok" if db_ok else "degraded",
        "version": settings.app_version,
        "db": db_ok,
        "ai": provider_status(),
        "counts": counts,
    }


if __name__ == "__main__":
    import os
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
