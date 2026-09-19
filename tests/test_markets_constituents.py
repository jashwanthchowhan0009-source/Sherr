"""Markets API output reaches the page:
- /markets/constituents (the "Companies in NIFTY" list) — the frontend called it
  all along; it never existed, so the list sat on "Loading companies…";
- /markets/history now returns a Yahoo-backed `series` [{t,p}] for the detail
  chart (which read h.series while the backend only returned `points`, so every
  chart said "No chart data").
"""
import asyncio
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import markets


# ── maps / resolution (pure) ─────────────────────────────────────────────────

def test_every_index_set_index_has_constituents():
    # Must match the frontend INDEX_SET.
    for idx in ("NIFTY", "SENSEX", "NASDAQ", "DOW", "SP500", "FTSE", "NIKKEI"):
        assert idx in markets._CONSTITUENTS, f"no constituents for {idx}"
        assert len(markets._CONSTITUENTS[idx]) >= 8


def test_range_map_covers_frontend_ranges():
    for r in ("1D", "1W", "1M", "6M", "1Y", "5Y", "MAX"):
        assert r in markets._RANGE_MAP


def test_resolve_yahoo():
    assert markets._resolve_yahoo("stocks", "NIFTY") == "^NSEI"
    assert markets._resolve_yahoo("crypto", "BTC") == "BTC-USD"
    assert markets._resolve_yahoo("stocks", "RELIANCE.NS") == "RELIANCE.NS"
    assert markets._resolve_yahoo("metals", "GOLD") == "GC=F"


# ── _yahoo_chart parses a Yahoo chart payload into {t(ms), p} ─────────────────

class _FakeResp:
    status_code = 200

    def json(self):
        return {"chart": {"result": [{
            "timestamp": [1000, 2000, 3000],
            "indicators": {"quote": [{"close": [10.0, None, 12.5]}]},
        }]}}


class _FakeClient:
    async def get(self, *a, **k):
        return _FakeResp()


def test_yahoo_chart_parsing():
    series = asyncio.run(markets._yahoo_chart(_FakeClient(), "^NSEI", "1M"))
    # None close is dropped; t is ms (seconds * 1000).
    assert series == [{"t": 1000000, "p": 10.0}, {"t": 3000000, "p": 12.5}]


# ── /markets/constituents shape (Yahoo monkeypatched) ────────────────────────

def test_constituents_endpoint_shape():
    async def _fake_yahoo(client, syms):
        return {s: {"price": 100.0 + i, "change": 1.0,
                    "change_pct": float(i), "currency": "INR"}
                for i, s in enumerate(syms)}

    async def _none(*a, **k):
        return None

    orig = (markets._yahoo, markets._cache_get, markets._cache_set)
    markets._yahoo, markets._cache_get, markets._cache_set = _fake_yahoo, _none, _none
    try:
        out = asyncio.run(markets.markets_constituents("NIFTY"))
    finally:
        markets._yahoo, markets._cache_get, markets._cache_set = orig

    assert out["index"] == "NIFTY"
    assert out["count"] == len(markets._CONSTITUENTS["NIFTY"])
    c0 = out["constituents"][0]
    for k in ("symbol", "name", "price", "change_pct", "currency"):
        assert k in c0
    # sorted movers-first (by |change_pct| desc)
    pcts = [abs(c["change_pct"]) for c in out["constituents"]]
    assert pcts == sorted(pcts, reverse=True)


def test_constituents_unknown_index_is_graceful():
    out = asyncio.run(markets.markets_constituents("MADEUP"))
    assert out["constituents"] == [] and "detail" in out
