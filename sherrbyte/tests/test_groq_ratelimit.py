"""Groq rate-limit guardrails: 429 backoff-with-jitter and TPM throttling.

These drive the coroutines with ``asyncio.run`` (the repo's test suite is sync
and does not depend on pytest-asyncio).
"""

import asyncio

import httpx

from app.config import settings
from app.sherr import router


# ─── backoff delay helper ────────────────────────────────────────────────────
def test_retry_after_honors_server_header():
    r = httpx.Response(429, headers={"Retry-After": "7"})
    d = router._retry_after_seconds(r, attempt=0)
    assert 7.0 <= d <= 7.6                      # header value + small jitter


def test_retry_after_full_jitter_within_cap():
    r = httpx.Response(429)                      # no header -> exp backoff w/ jitter
    for attempt in range(5):
        cap = min(settings.groq_backoff_base_sec * (2 ** attempt),
                  settings.groq_backoff_max_sec)
        for _ in range(25):
            d = router._retry_after_seconds(r, attempt)
            assert 0.0 <= d <= cap               # full jitter: random in [0, cap]


# ─── _groq retry behaviour ───────────────────────────────────────────────────
def test_groq_backs_off_then_succeeds(monkeypatch):
    monkeypatch.setattr(router.settings, "groq_api_key", "x")
    monkeypatch.setattr(router.settings, "groq_max_retries", 3)
    sleeps: list[float] = []

    async def fake_sleep(s):                     # don't actually wait in tests
        sleeps.append(s)
    monkeypatch.setattr(router.asyncio, "sleep", fake_sleep)

    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] < 3:                       # two 429s, then succeed
            return httpx.Response(429, headers={"Retry-After": "2"}, text="slow down")
        return httpx.Response(200, json={"choices": [{"message": {"content": " hi "}}]})

    async def go():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            return await router._groq("sys", "usr", False, 0.2, 128, client)
        finally:
            await client.aclose()

    out = asyncio.run(go())
    assert out == "hi"
    assert calls["n"] == 3                        # retried rather than failing instantly
    assert len(sleeps) == 2                       # one backoff per 429
    assert all(s >= 2.0 for s in sleeps)          # honored Retry-After


def test_groq_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(router.settings, "groq_api_key", "x")
    monkeypatch.setattr(router.settings, "groq_max_retries", 2)
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)
    monkeypatch.setattr(router.asyncio, "sleep", fake_sleep)

    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(429, text="rate limited")   # always throttled

    async def go():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            return await router._groq("sys", "usr", False, 0.2, 128, client)
        finally:
            await client.aclose()

    out = asyncio.run(go())
    assert out is None
    assert calls["n"] == 3                        # initial try + 2 retries
    assert len(sleeps) == 2                       # bounded number of backoffs


# ─── TPM throttle ────────────────────────────────────────────────────────────
def test_limiter_throttles_when_over_tpm(monkeypatch):
    lim = router._GroqLimiter(tpm=100, max_concurrency=2)

    clock = {"t": 1000.0}
    monkeypatch.setattr(router.time, "monotonic", lambda: clock["t"])

    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)
        clock["t"] += 60.0                        # advance past the rolling window
    monkeypatch.setattr(router.asyncio, "sleep", fake_sleep)

    async def go():
        await lim.reserve(80)                      # fits: used=80
        assert sleeps == []                        # first reservation never waits
        await lim.reserve(80)                      # 80+80 > 100 -> must wait once

    asyncio.run(go())
    assert len(sleeps) == 1                        # throttled instead of firing


def test_limiter_clamps_oversized_request():
    lim = router._GroqLimiter(tpm=100, max_concurrency=1)
    # A request bigger than the whole budget must not deadlock.
    asyncio.run(lim.reserve(10_000))
