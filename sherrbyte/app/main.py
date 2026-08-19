"""
main.py — FastAPI entry point.

Wires the lifespan (DB pool + migrations + Redis + optional in-process scheduler),
mounts every router, and exposes the static taxonomy + health endpoints.

Run:  uvicorn app.main:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import os

import asyncio
import logging
from contextlib import asynccontextmanager
from functools import lru_cache

from app.sherr import admin
from fastapi import FastAPI, HTTPException
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


async def _market_signals_job():
    """Write each instrument's daily move into domain_signals. Must run BEFORE the
    detectors so news↔market joins have market data to work with."""
    from app.workers.market_signals import run as run_market
    try:
        log.info("market signals: %s", await run_market())
    except Exception as e:
        log.error("market signals job failed: %s", e, exc_info=True)


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
        # Market moves: a few times a day so intraday moves are captured near the
        # close of the major sessions (and always before the nightly detectors).
        _scheduler.add_job(_market_signals_job, "cron", hour="1,7,13,19", id="market_signals")
        # Detectors are heavy + materialized, so run once nightly (02:00 UTC).
        _scheduler.add_job(_detectors_job, "cron", hour=2, id="detectors")
        # Explore page feeds — six independent jobs on their own intervals.
        try:
            from app.pipeline import explore_feeds
            explore_feeds.register_jobs(_scheduler)
        except Exception as e:
            log.error("explore jobs not registered: %s", e)
        _scheduler.start()
        asyncio.create_task(_ingest_job())  # kick one cycle on boot
        # Warm every Explore key immediately so the first request is a cache hit
        # rather than waiting up to 12h for the slowest job to fire.
        try:
            from app.pipeline import explore_feeds as _ef
            asyncio.create_task(_ef.refresh_all())
        except Exception as e:
            log.error("explore warm-up skipped: %s", e)
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
    # Same fix as the sqlite app: "*" with credentials is rejected by browsers and
    # advertises the API to every origin. Pinned via CORS_ORIGINS.
    allow_origins=[o.strip() for o in (os.getenv(
        "CORS_ORIGINS",
        "https://sherrbyte.vercel.app,https://sherrbyte.com,"
        "https://www.sherrbyte.com,http://localhost:3000,http://localhost:5173"
    ) or "").split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────
from app.api import (  # noqa: E402  (import after app to avoid cycles)
    activity, article, auth, compat, feed, live, markets, signal, sherr, push,
    notifications, patterns,
)


# ─── Explore page snapshot ────────────────────────────────────────────────────
@app.get("/explore/snapshot")
async def explore_snapshot():
    """Every Explore section from cache in one call. Pure Redis read — no upstream
    API is touched on this path, which is what keeps it fast regardless of whether
    Yahoo or GNews is having a bad day."""
    from app.pipeline import explore_feeds
    return await explore_feeds.snapshot()


@app.post("/explore/refresh")
async def explore_refresh(name: str = ""):
    """Force a refresh — all sections, or one by name. Operational escape hatch for
    when a section is stale and the next scheduled run is hours away."""
    from app.pipeline import explore_feeds
    if name:
        if name not in explore_feeds.FETCHERS:
            raise HTTPException(404, f"unknown section: {name}")
        return await explore_feeds.refresh(name)
    return await explore_feeds.refresh_all()


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
