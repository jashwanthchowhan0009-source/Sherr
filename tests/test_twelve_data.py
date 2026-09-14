"""Twelve Data provider + the /quote endpoint it backs.

The quota is the constraint (800 req/day free), so these pin the two things that
protect it: the endpoint skips cleanly when no key is configured, and a successful
answer is cached so a burst of Dots tickers does not become a burst of upstream
calls.
"""
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cache          # noqa: E402
import main           # noqa: E402
import twelve_data    # noqa: E402


@pytest.fixture
def client():
    return TestClient(main.app)


# ─── the pure parser / key handling ─────────────────────────────────────────

def test_a_comma_list_becomes_several_keys(monkeypatch):
    monkeypatch.setenv("TWELVE_DATA_API_KEYS", "k1, k2 ,k3")
    assert twelve_data.keys() == ["k1", "k2", "k3"]
    assert twelve_data.enabled() is True


def test_no_key_is_a_clean_skip(monkeypatch):
    monkeypatch.delenv("TWELVE_DATA_API_KEYS", raising=False)
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    assert twelve_data.keys() == []
    assert twelve_data.enabled() is False


def test_parse_multi_and_single_and_skips_errors():
    multi = {"BTC/USD": {"symbol": "BTC/USD", "close": "64000.5",
                         "percent_change": "1.23", "name": "Bitcoin"},
             "OOPS": {"status": "error", "code": 400}}
    got = twelve_data._parse(multi, ["BTC/USD", "OOPS"])
    assert got == {"BTC/USD": {"price": 64000.5, "change_pct": 1.23, "name": "Bitcoin"}}
    single = {"symbol": "USD/INR", "close": "83.2", "percent_change": "-0.1"}
    assert twelve_data._parse(single, ["USD/INR"])["USD/INR"]["change_pct"] == -0.1


def test_body_level_429_is_detected():
    assert twelve_data._rate_limited({"code": 429, "message": "limit"}) is True
    assert twelve_data._rate_limited({"symbol": "BTC/USD"}) is False


# ─── the endpoint ────────────────────────────────────────────────────────────

def test_quote_skips_cleanly_when_unconfigured(client, monkeypatch):
    monkeypatch.delenv("TWELVE_DATA_API_KEYS", raising=False)
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    r = client.get("/quote?symbols=BTC/USD,ETH/USD")
    assert r.status_code == 200
    assert r.json() == {"quotes": {}, "source": "unconfigured"}


def test_quote_caches_so_a_burst_is_one_upstream_call(client, monkeypatch):
    cache._local.clear()
    monkeypatch.setenv("TWELVE_DATA_API_KEYS", "k1")
    calls = {"n": 0}

    async def fake_get_quotes(symbols):
        calls["n"] += 1
        return {s.upper(): {"price": 1.0, "change_pct": 0.5, "name": s}
                for s in symbols}

    monkeypatch.setattr(twelve_data, "get_quotes", fake_get_quotes)
    a = client.get("/quote?symbols=BTC/USD,ETH/USD").json()
    b = client.get("/quote?symbols=eth/usd,btc/usd").json()   # same set, different order/case
    assert a["source"] == "twelvedata"
    assert "BTC/USD" in a["quotes"]
    assert a == b
    assert calls["n"] == 1, "the second request must be served from cache"
    cache._local.clear()
