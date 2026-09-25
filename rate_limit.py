"""
rate_limit.py — one async token-bucket limiter, shared across the ingest pipeline.

SHERR_WRITING_SPEC.md §4.4: the NEWS writer's regenerate-once doubles worst-case
LLM calls, so every writer call (first pass AND regeneration) must pass through a
single limiter sized to the Gemini free tier — 15 requests a minute. The bucket
is the ceiling the whole pipeline shares, not a per-caller guess, so three
callers each believing they are inside the limit cannot together breach it (the
same failure CLAUDE.md records for BODY_DRAIN_RPM).

Two acquire modes, and the difference is the whole point of §4.4:

  • ``await acquire()`` — a first-pass generation. It waits for a token, so a
    first pass is never dropped, only slowed.
  • ``try_acquire()`` — a regeneration. It takes a token only if one is free
    RIGHT NOW and never waits, so a backlog of retries cannot starve new
    ingestion. When it returns False the caller skips regeneration and goes
    straight to the safe fallback (a safe fallback beats a stalled queue).

Pure stdlib, no external dependency. A monotonic clock drives the refill so a
wall-clock adjustment cannot hand out a burst of tokens.
"""

from __future__ import annotations

import asyncio
import os
import time


class RateLimiter:
    """A refilling token bucket. ``rate`` tokens are added over ``per`` seconds,
    capped at ``rate`` (so an idle limiter allows one full minute's burst and no
    more). Thread/task-safe for asyncio via a single lock.
    """

    def __init__(self, rate: int, per: float = 60.0, *,
                 clock=time.monotonic) -> None:
        self.rate = max(1, int(rate))
        self.per = float(per)
        self._clock = clock
        self._tokens = float(self.rate)
        self._updated = clock()
        self._lock = asyncio.Lock()

    def _refill_locked(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        if elapsed <= 0:
            return
        self._tokens = min(float(self.rate),
                           self._tokens + elapsed * (self.rate / self.per))
        self._updated = now

    async def acquire(self) -> None:
        """Block until a token is available, then spend it. For a first-pass call —
        never dropped, only delayed."""
        while True:
            async with self._lock:
                self._refill_locked()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Seconds until the next whole token, computed inside the lock.
                deficit = 1.0 - self._tokens
                wait = deficit * (self.per / self.rate)
            await asyncio.sleep(max(wait, 0.01))

    async def try_acquire(self) -> bool:
        """Spend a token only if one is free now; never wait. For a regeneration —
        a saturated limiter means skip the retry (spec §4.4)."""
        async with self._lock:
            self._refill_locked()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def tokens_available(self) -> float:
        """Best-effort current token count, for diagnostics. Not a reservation."""
        self._refill_locked()
        return self._tokens


# The one limiter the ingest pipeline shares. Sized to the Gemini free tier by
# default; INGEST_RPM overrides it without a deploy, matching how every other
# rate constant in this repo is an env var.
INGEST_LIMITER = RateLimiter(int(os.getenv("INGEST_RPM", "15") or "15"))
