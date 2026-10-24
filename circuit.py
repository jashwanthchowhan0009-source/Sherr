"""
circuit.py — circuit breakers for the model providers.

WHAT WAS MISSING. `_call_provider` tries every key in a pool, then the cascade
tries every provider, on EVERY article. That is correct when a call fails for a
reason the next key or the next provider might not share. It is waste when the
provider is simply down: each article pays the full connect timeout once per
key, the drain's 60-second tick is spent waiting rather than writing, and the
only record is a line in `PROVIDER_ERRORS` saying the same thing for the
hundredth time.

A breaker remembers. After `threshold` consecutive failures of the kind that
mean "this provider is not answering", the provider is skipped for a cooldown
and the cascade moves on immediately. One probe is let through when the cooldown
expires; it closes the breaker on success and re-opens it — with a doubled
cooldown — on failure.

WHICH FAILURES TRIP IT, AND THIS IS THE WHOLE DESIGN.

  trips:  status 0  — no response at all: DNS, connect, read timeout
          5xx       — the provider's own fault, and not key-specific

  never:  401/403   — the KEY is the problem, and `key_pool` already rotates on
                      it. Opening a breaker here would take a provider out over
                      one bad key in a pool of good ones.
          429       — "slow down", not "down". This is the one that matters: the
                      free tier bounces bursts routinely, `body_drain_job`
                      already backs off on exactly this signal (read from
                      `ai_processor.PROVIDER_ERRORS`), and a breaker would take
                      a HEALTHY provider out of service for a cooldown — the
                      opposite of the correct response, and it would stall the
                      one pass writing the corpus. CLAUDE.md's rule for the
                      drain is that a rate limit and a failure are different
                      answers; this is the same rule one layer down.
          400/404   — a configuration error, not a transient one. A 404 is how a
                      RETIRED MODEL ID presents (CLAUDE.md: the call 404s, the
                      cascade drops through, and the only symptom is a backlog
                      that stops clearing). Cooling down and retrying is the
                      wrong shape for something only a config change fixes, and
                      `GROQ_RETIRED` in tests/test_feed_doctor.py is its guard.
                      It still lands in PROVIDER_ERRORS.

PURE. No network, no database, no sleeping. The clock is injected, so every
state transition and every cooldown is tested without waiting for one.

PER-PROCESS, and that is a stated bound, the same one `you_contents`' budget
ledger and `_token_version`'s TTL cache carry: with N instances each has its own
view. That is the right scope for this — a breaker is about what THIS process
has observed, and a shared one would let an instance that has not made a call
refuse to make one.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Callable, Iterable

log = logging.getLogger("sherbyte.circuit")

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"

# The statuses that mean "this provider is not answering". 0 is the sentinel
# ai_processor._record_error already uses for a request that never got a
# response, so the two modules agree without a second vocabulary.
NO_RESPONSE = 0


def trips_breaker(status: int) -> bool:
    """Whether this status is evidence the PROVIDER is down.

    Everything the module header lists as 'never' returns False here, so the
    policy lives in one function and a caller cannot get it half right.
    """
    if status == NO_RESPONSE:
        return True
    return 500 <= int(status) <= 599


# ─── tuning, read at call time ───────────────────────────────────────────────
# Never at import: module-level configuration is what forces importlib.reload,
# which this suite has already been burnt by (CLAUDE.md, "Tests: two traps").
def _int_env(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.getenv(name, str(default)))))
    except (TypeError, ValueError):
        return default


def enabled() -> bool:
    return os.getenv("CIRCUIT_ENABLED", "1") not in ("0", "false", "False")


def threshold() -> int:
    """Consecutive trip-worthy failures before the breaker opens.

    Not 1. A single timeout is ordinary on a free tier and on Render's cold
    starts, and opening on it would take a working provider out over one slow
    request.
    """
    return _int_env("CIRCUIT_THRESHOLD", 4, 1, 50)


def cooldown_s() -> float:
    return float(_int_env("CIRCUIT_COOLDOWN_S", 60, 1, 3600))


def max_cooldown_s() -> float:
    """The ceiling on the doubling. A provider that has been down for an hour
    should not be probed every minute, but it must still be probed — a breaker
    that never closes is an outage nobody notices."""
    return float(_int_env("CIRCUIT_MAX_COOLDOWN_S", 900, 1, 86400))


class Breaker:
    """One provider's breaker. Not thread-safe by design: the drain is a single
    asyncio task and `await` points are the only interleaving, so the
    read-modify-write below cannot be torn."""

    def __init__(self, name: str, *, clock: Callable[[], float] | None = None):
        self.name = name
        self._clock = clock or time.monotonic
        self.reset()

    # ─── state ──────────────────────────────────────────────────────────────
    def reset(self) -> None:
        self._failures = 0
        self._opened_at: float | None = None
        self._cooldown = cooldown_s()
        self._probing = False
        self._probe_at: float | None = None
        # Counters, for /admin. Cumulative over the process's life.
        self.trips = 0
        self.short_circuited = 0
        self.successes = 0
        self.failures_seen = 0
        self.last_reason = ""
        self.last_status = None

    def state(self) -> str:
        if self._opened_at is None:
            return CLOSED
        if self._probing or self._clock() - self._opened_at >= self._cooldown:
            return HALF_OPEN
        return OPEN

    def _probe_is_stale(self) -> bool:
        """A probe whose outcome was never recorded must not wedge the breaker.

        `allow()` marks a probe in flight so the rest of the batch is not sent
        to a provider that is down. If the caller then raises before recording
        an outcome, that flag would never clear and the provider would be
        skipped FOREVER — a breaker that cannot close is a worse outage than the
        one it was protecting against. A probe is therefore abandoned after one
        cooldown and the next caller gets a fresh one.
        """
        if not self._probing or self._probe_at is None:
            return False
        return self._clock() - self._probe_at >= self._cooldown

    def retry_in(self) -> float:
        """Seconds until the next probe is allowed. 0 when a call may go now."""
        if self._opened_at is None:
            return 0.0
        return max(0.0, self._cooldown - (self._clock() - self._opened_at))

    # ─── the decision ───────────────────────────────────────────────────────
    def allow(self) -> bool:
        """May a call be made now?

        Returns True in CLOSED, and exactly once per cooldown in HALF_OPEN — the
        probe. A second caller arriving while a probe is in flight is refused,
        so a provider that is down does not receive the whole batch as probes.
        """
        if not enabled():
            return True
        st = self.state()
        if st == CLOSED:
            return True
        if st == HALF_OPEN and (not self._probing or self._probe_is_stale()):
            if self._probing:
                log.warning("[CIRCUIT] %s: previous probe recorded no outcome, "
                            "abandoning it", self.name)
            self._probing = True
            self._probe_at = self._clock()
            log.info("[CIRCUIT] %s half-open: letting one probe through "
                     "(down %.0fs, last %s)",
                     self.name, self._clock() - (self._opened_at or 0),
                     self.last_reason or self.last_status)
            return True
        self.short_circuited += 1
        return False

    # ─── outcomes ───────────────────────────────────────────────────────────
    def record_success(self) -> None:
        """A provider that answered is a provider that is up. The failure count
        is cleared, not decremented: four failures and then a success is not
        three failures, it is a provider that works."""
        self.successes += 1
        was = self.state()
        self._failures = 0
        self._probing = False
        self._probe_at = None
        if self._opened_at is not None:
            log.info("[CIRCUIT] %s closed after a successful probe", self.name)
        self._opened_at = None
        self._cooldown = cooldown_s()
        if was != CLOSED:
            self.last_reason = ""

    def record_failure(self, status: int, detail: str = "") -> bool:
        """Record an outcome. Returns True if the breaker is now open.

        A status that does not trip the breaker is still counted in
        `failures_seen` — the number is wanted in the diagnosis — but it does
        NOT advance the consecutive count, so a run of 429s can never open it.
        """
        self.failures_seen += 1
        if not trips_breaker(status):
            # The provider ANSWERED. A 429, a 401 or a 404 is a usable HTTP
            # response, which is proof the host is reachable and serving — so it
            # closes the breaker rather than leaving it open on evidence that
            # contradicts it. Whose problem the status is belongs to the layer
            # that owns it: key_pool rotates on 401/403/429 and the drain backs
            # off on 429.
            if self._opened_at is not None or self._probing:
                log.info("[CIRCUIT] %s answered (HTTP %s) — closing, the host "
                         "is reachable", self.name, status)
            self._failures = 0
            self._probing = False
            self._probe_at = None
            self._opened_at = None
            self._cooldown = cooldown_s()
            return False

        self.last_status = status
        self.last_reason = (detail or ("no response" if status == NO_RESPONSE
                                       else f"HTTP {status}"))[:200]

        if self._probing:
            # The probe failed: straight back to open, with a longer cooldown.
            self._probing = False
            self._probe_at = None
            self._cooldown = min(self._cooldown * 2, max_cooldown_s())
            self._opened_at = self._clock()
            self.trips += 1
            log.warning("[CIRCUIT] %s probe failed (%s) — open again for %.0fs",
                        self.name, self.last_reason, self._cooldown)
            return True

        self._failures += 1
        if self._opened_at is None and self._failures >= threshold():
            self._opened_at = self._clock()
            self._cooldown = cooldown_s()
            self.trips += 1
            log.warning("[CIRCUIT] %s OPEN after %d consecutive failures (%s) "
                        "— skipping it for %.0fs",
                        self.name, self._failures, self.last_reason,
                        self._cooldown)
            return True
        return self._opened_at is not None

    # ─── reporting ──────────────────────────────────────────────────────────
    def snapshot(self) -> dict:
        return {
            "state": self.state(),
            "consecutive_failures": self._failures,
            "threshold": threshold(),
            "retry_in_s": round(self.retry_in(), 1),
            "cooldown_s": round(self._cooldown, 1),
            "trips": self.trips,
            "short_circuited": self.short_circuited,
            "successes": self.successes,
            "failures_seen": self.failures_seen,
            # A reason, never a key and never a URL with one in it.
            "last_reason": self.last_reason,
        }


# ─── registry ────────────────────────────────────────────────────────────────
_BREAKERS: dict[str, Breaker] = {}


def get(name: str) -> Breaker:
    b = _BREAKERS.get(name)
    if b is None:
        b = _BREAKERS[name] = Breaker(name)
    return b


def reset_all() -> None:
    """Drop every breaker. Called from the test conftest: a breaker is
    module-level state that would otherwise carry one test's failures into the
    next, which is the same trap cache.py's local layer needed isolating for."""
    _BREAKERS.clear()


def snapshot(names: Iterable[str] | None = None) -> dict:
    keys = list(names) if names is not None else sorted(_BREAKERS)
    return {n: _BREAKERS[n].snapshot() for n in keys if n in _BREAKERS}


def open_breakers() -> list[str]:
    return [n for n, b in sorted(_BREAKERS.items()) if b.state() == OPEN]


def diagnose() -> str:
    """One line for /admin/pipeline."""
    if not enabled():
        return "CIRCUIT_ENABLED=0 — breakers are measured but never short-circuit."
    if not _BREAKERS:
        return "No provider call has been made in this process yet."
    opn = open_breakers()
    half = [n for n, b in sorted(_BREAKERS.items()) if b.state() == HALF_OPEN]
    if opn:
        worst = _BREAKERS[opn[0]]
        return (f"{', '.join(opn)} OPEN — {worst.last_reason or 'unreachable'}; "
                f"next probe in {worst.retry_in():.0f}s. The cascade is skipping "
                f"it, so writes are running on the remaining provider(s).")
    if half:
        return f"{', '.join(half)} half-open — one probe is allowed through."
    return ("All breakers closed. Note that 429s never trip one: a rate limit "
            "is handled by the drain's backoff, not by taking a healthy "
            "provider out of service.")
