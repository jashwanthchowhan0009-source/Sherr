"""CoinGecko's public tier, and the two ways a --days 400 run lost every coin.

A `--days 400` backfill failed all 11 crypto symbols: 5 with HTTP 401 and 6 with
HTTP 429. The 401s read as an authentication problem and sent us looking for a
missing API key. They were not auth failures — the public tier refuses any window
wider than 365 days and answers 401 rather than 400 or 416. The 6 that got 429
never reached range validation: they were rate limited first, at the old 1.5s
gap (40 calls a minute, well over the tier's ~5-15).

So: clamp the window, pace the calls, and treat a 429 as backpressure to wait
out rather than a symbol to drop.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import market_ticks  # noqa: E402


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self):
        return self._payload


def _series_payload(n_days=3):
    """n daily points, oldest first, in CoinGecko's [ms, price] shape."""
    day_ms = 86_400_000
    return {"prices": [[1_760_000_000_000 + i * day_ms, 100.0 + i]
                       for i in range(n_days)]}


class FakeClient:
    """Records every request so the test can assert on what was actually sent."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {})})
        return self.responses.pop(0) if self.responses else FakeResponse(500)


@pytest.fixture(autouse=True)
def no_real_sleeping(monkeypatch):
    """Backoff waits are asserted on, not served."""
    slept = []

    async def fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(market_ticks.asyncio, "sleep", fake_sleep)
    return slept


# ─── the clamp ───────────────────────────────────────────────────────────────

def test_a_400_day_request_is_clamped_to_the_tier_ceiling():
    """THE 401 BUG. 400 days went to the provider unclamped."""
    client = FakeClient([FakeResponse(200, _series_payload())])
    asyncio.run(market_ticks.coingecko_daily(client, "bitcoin", 400))
    assert client.calls[0]["params"]["days"] == market_ticks.COINGECKO_MAX_DAYS


def test_a_request_inside_the_ceiling_is_sent_unchanged():
    client = FakeClient([FakeResponse(200, _series_payload())])
    asyncio.run(market_ticks.coingecko_daily(client, "bitcoin", 90))
    assert client.calls[0]["params"]["days"] == 90


def test_the_clamp_is_logged_per_symbol(caplog):
    """A short crypto series must never look like missing data."""
    client = FakeClient([FakeResponse(200, _series_payload())])
    with caplog.at_level("INFO"):
        asyncio.run(market_ticks.coingecko_daily(client, "solana", 400))
    msg = " ".join(r.getMessage() for r in caplog.records)
    assert "solana" in msg and "clamped" in msg and "400" in msg


def test_a_401_that_survives_the_clamp_says_it_is_not_an_auth_failure():
    client = FakeClient([FakeResponse(401)])
    with pytest.raises(RuntimeError) as e:
        asyncio.run(market_ticks.coingecko_daily(client, "cardano", 30))
    assert "not auth" in str(e.value)


# ─── the 429s ────────────────────────────────────────────────────────────────

def test_a_429_is_retried_rather_than_dropping_the_symbol(no_real_sleeping):
    client = FakeClient([FakeResponse(429),
                         FakeResponse(429),
                         FakeResponse(200, _series_payload())])
    out = asyncio.run(market_ticks.coingecko_daily(client, "ethereum", 30))
    assert out and len(client.calls) == 3
    assert len(no_real_sleeping) == 2


def test_backoff_grows_between_retries(no_real_sleeping):
    client = FakeClient([FakeResponse(429), FakeResponse(429),
                         FakeResponse(200, _series_payload())])
    asyncio.run(market_ticks.coingecko_daily(client, "ripple", 30))
    assert no_real_sleeping[1] > no_real_sleeping[0]


def test_retry_after_header_wins_when_it_is_longer(no_real_sleeping):
    """The provider's own number is authoritative over our guess."""
    client = FakeClient([FakeResponse(429, headers={"retry-after": "900"}),
                         FakeResponse(200, _series_payload())])
    asyncio.run(market_ticks.coingecko_daily(client, "litecoin", 30))
    assert no_real_sleeping[0] == 900.0


def test_a_junk_retry_after_does_not_crash_the_backoff(no_real_sleeping):
    client = FakeClient([FakeResponse(429, headers={"retry-after": "soon"}),
                         FakeResponse(200, _series_payload())])
    asyncio.run(market_ticks.coingecko_daily(client, "dogecoin", 30))
    assert no_real_sleeping[0] > 0


def test_persistent_429_fails_with_a_message_naming_the_real_cause(no_real_sleeping):
    client = FakeClient([FakeResponse(429)] * (market_ticks.COINGECKO_RETRIES + 1))
    with pytest.raises(RuntimeError) as e:
        asyncio.run(market_ticks.coingecko_daily(client, "matic-network", 30))
    assert "rate limited" in str(e.value)
    assert "not a data problem" in str(e.value)


# ─── pacing ──────────────────────────────────────────────────────────────────

def test_the_gap_between_calls_is_slower_than_the_tier_limit():
    """1.5s was 40 calls a minute against a ~5-15 ceiling."""
    assert market_ticks.COINGECKO_GAP_S >= 4.0
    assert market_ticks.COINGECKO_CONCURRENCY == 1


def test_no_api_key_is_ever_sent_to_coingecko():
    """CoinGecko is on the non-commercial licensing audit list: the fix is
    pacing, never a key or a paid tier."""
    client = FakeClient([FakeResponse(200, _series_payload())])
    asyncio.run(market_ticks.coingecko_daily(client, "bitcoin", 30))
    sent = client.calls[0]["params"]
    assert not any("key" in k.lower() for k in sent), sent
    src = open(market_ticks.__file__).read()
    assert "x_cg_demo_api_key" not in src and "x_cg_pro_api_key" not in src
