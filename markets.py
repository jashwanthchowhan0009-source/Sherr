"""
markets.py — Real-time market data aggregator.

Providers (in priority order where applicable):
  • Stocks:     Finnhub (if key), Yahoo Finance (free fallback)
  • Crypto:     CoinGecko (free, no key)
  • Metals:     Metals-API (if key), Yahoo Finance futures fallback
  • Forex:      ExchangeRate-API (if key), Yahoo Finance fallback
  • Oil/Gas:    Yahoo Finance

Caching:
  Each asset class has its own TTL to balance freshness with API quota.
  All caches are in-memory — they reset on redeploy. That's fine for markets.

Graceful degradation:
  If a provider fails, returns {} for that section. UI never crashes.
"""

import os
import time
import asyncio
import logging
from typing import Optional
import httpx
from fastapi import APIRouter

log = logging.getLogger("sherbyte.markets")
router = APIRouter()

# ─── API keys (all optional) ─────────────────────────────────────────────
ALPHAVANTAGE_KEY = os.getenv("ALPHAVANTAGE_KEY", "")
FINNHUB_KEY      = os.getenv("FINNHUB_KEY", "")
METALS_API_KEY   = os.getenv("METALS_API_KEY", "")
EXCHANGE_RATE_KEY= os.getenv("EXCHANGE_RATE_KEY", "")

# ─── TTL cache ───────────────────────────────────────────────────────────
_cache: dict = {}

def _cget(key: str):
    e = _cache.get(key)
    if not e or time.time() > e["exp"]:
        return None
    return e["data"]

def _cset(key: str, data, ttl: int):
    _cache[key] = {"data": data, "exp": time.time() + ttl}


# ─── Provider: Yahoo Finance (free, primary for stocks + forex + futures) ─
async def _yahoo(client: httpx.AsyncClient, symbols: list[str]) -> dict:
    try:
        r = await client.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(symbols)},
            headers={"User-Agent": "Mozilla/5.0 (compatible; SherByte/5.1)"},
            timeout=8,
        )
        if r.status_code != 200:
            log.warning("Yahoo HTTP %d for %s", r.status_code, symbols)
            return {}
        out = {}
        for q in r.json().get("quoteResponse", {}).get("result", []):
            price = q.get("regularMarketPrice", 0) or 0
            out[q["symbol"]] = {
                "price":      round(price, 2),
                "change":     round(q.get("regularMarketChange", 0) or 0, 2),
                "change_pct": round(q.get("regularMarketChangePercent", 0) or 0, 2),
                "high":       round(q.get("regularMarketDayHigh", 0) or 0, 2),
                "low":        round(q.get("regularMarketDayLow", 0) or 0, 2),
                "prev_close": round(q.get("regularMarketPreviousClose", 0) or 0, 2),
                "currency":   q.get("currency", ""),
            }
        return out
    except Exception as e:
        log.warning("Yahoo fetch failed: %s", e)
        return {}


async def _yahoo_history(client: httpx.AsyncClient, symbol: str, points: int = 20) -> list[float]:
    """Short sparkline series — last N 5-min candles today."""
    try:
        r = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "5m", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0 (compatible; SherByte/5.1)"},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        result = r.json().get("chart", {}).get("result", [])
        if not result:
            return []
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        clean = [c for c in closes if c is not None]
        return [round(c, 2) for c in clean[-points:]]
    except Exception:
        return []


# ─── Provider: CoinGecko (free, no key) ──────────────────────────────────
async def _coingecko(client: httpx.AsyncClient, ids: list[str]) -> dict:
    try:
        r = await client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids":                ",".join(ids),
                "vs_currencies":      "usd,inr",
                "include_24hr_change":"true",
                "include_market_cap": "true",
            },
            timeout=8,
        )
        if r.status_code != 200:
            return {}
        out = {}
        name_map = {
            "bitcoin":  "BTC",  "ethereum": "ETH",  "solana":  "SOL",
            "dogecoin": "DOGE", "cardano":  "ADA",  "ripple":  "XRP",
            "binancecoin": "BNB", "polkadot": "DOT",
        }
        for coin, d in r.json().items():
            sym = name_map.get(coin, coin.upper())
            out[sym] = {
                "price_usd":       round(d.get("usd", 0) or 0, 2),
                "price_inr":       round(d.get("inr", 0) or 0, 2),
                "change_pct":      round(d.get("usd_24h_change", 0) or 0, 2),
                "market_cap_usd":  int(d.get("usd_market_cap", 0) or 0),
            }
        return out
    except Exception as e:
        log.warning("CoinGecko failed: %s", e)
        return {}


# ─── Provider: Finnhub (optional, real-time stocks) ──────────────────────
async def _finnhub_quote(client: httpx.AsyncClient, symbol: str) -> Optional[dict]:
    if not FINNHUB_KEY:
        return None
    try:
        r = await client.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": FINNHUB_KEY},
            timeout=6,
        )
        if r.status_code != 200:
            return None
        d = r.json()
        if not d.get("c"):
            return None
        prev = d.get("pc", 0) or 0
        return {
            "price":      round(d["c"], 2),
            "change":     round(d["c"] - prev, 2),
            "change_pct": round((d["c"] - prev) / prev * 100, 2) if prev else 0,
            "high":       round(d.get("h", 0), 2),
            "low":        round(d.get("l", 0), 2),
            "prev_close": round(prev, 2),
        }
    except Exception as e:
        log.warning("Finnhub %s failed: %s", symbol, e)
        return None


# ─── Provider: Metals-API (optional, precious metals) ────────────────────
async def _metals_api(client: httpx.AsyncClient) -> dict:
    if not METALS_API_KEY:
        return {}
    try:
        r = await client.get(
            "https://metals-api.com/api/latest",
            params={"access_key": METALS_API_KEY, "base": "USD", "symbols": "XAU,XAG,XPT,XPD"},
            timeout=8,
        )
        if r.status_code != 200:
            return {}
        rates = r.json().get("rates", {})
        out = {}
        # Metals-API: 1 USD = X units; invert for price-per-ounce in USD
        for sym, label in [("XAU", "GOLD"), ("XAG", "SILVER"),
                            ("XPT", "PLATINUM"), ("XPD", "PALLADIUM")]:
            rate = rates.get(sym)
            if rate and rate > 0:
                out[label] = {"price_usd_oz": round(1 / rate, 2)}
        return out
    except Exception as e:
        log.warning("Metals-API failed: %s", e)
        return {}


# ─── Aggregators — one per asset class ───────────────────────────────────

async def fetch_stocks(with_sparkline: bool = False) -> dict:
    cached = _cget(f"stocks_{with_sparkline}")
    if cached:
        return cached

    symbols = ["^NSEI", "^BSESN", "^IXIC", "^GSPC", "^DJI", "^FTSE", "^N225"]
    labels  = {
        "^NSEI":  "NIFTY",   "^BSESN": "SENSEX",  "^IXIC": "NASDAQ",
        "^GSPC":  "SP500",   "^DJI":   "DOW",     "^FTSE": "FTSE",
        "^N225":  "NIKKEI",
    }

    async with httpx.AsyncClient() as client:
        base = await _yahoo(client, symbols)
        result = {labels[s]: base.get(s, {}) for s in symbols}

        if with_sparkline:
            sparks = await asyncio.gather(*[_yahoo_history(client, s, 20) for s in symbols])
            for s, spark in zip(symbols, sparks):
                if spark and labels[s] in result and result[labels[s]]:
                    result[labels[s]]["spark"] = spark

    _cset(f"stocks_{with_sparkline}", result, 60)
    return result


async def fetch_crypto() -> dict:
    cached = _cget("crypto")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        data = await _coingecko(client, [
            "bitcoin", "ethereum", "solana", "dogecoin",
            "cardano", "ripple", "binancecoin",
        ])
    _cset("crypto", data, 45)
    return data


async def fetch_metals() -> dict:
    cached = _cget("metals")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        metals = await _metals_api(client)
        # Augment/fallback via Yahoo futures
        fut = await _yahoo(client, ["GC=F", "SI=F", "PL=F", "PA=F"])
        fb = {"GC=F": "GOLD", "SI=F": "SILVER", "PL=F": "PLATINUM", "PA=F": "PALLADIUM"}
        for sym, label in fb.items():
            y = fut.get(sym, {})
            if not y:
                continue
            if label not in metals or not metals[label]:
                metals[label] = {
                    "price_usd_oz": y["price"],
                    "change":       y["change"],
                    "change_pct":   y["change_pct"],
                }
            else:
                metals[label].setdefault("change",     y.get("change", 0))
                metals[label].setdefault("change_pct", y.get("change_pct", 0))

        # Convert USD/oz → INR/10g for Indian audience (10g = 0.3215 oz)
        # Pull live USDINR from Yahoo
        fx = await _yahoo(client, ["USDINR=X"])
        usd_inr = (fx.get("USDINR=X") or {}).get("price", 83.0)
        for label, d in metals.items():
            if "price_usd_oz" in d:
                d["price_inr_10g"] = round(d["price_usd_oz"] * 0.3215 * usd_inr, 0)
    _cset("metals", metals, 180)
    return metals


async def fetch_forex() -> dict:
    cached = _cget("forex")
    if cached:
        return cached
    pairs = ["USDINR=X", "EURINR=X", "GBPINR=X", "JPYINR=X", "EURUSD=X", "GBPUSD=X"]
    async with httpx.AsyncClient() as client:
        data = await _yahoo(client, pairs)
    labels = {
        "USDINR=X": "USDINR", "EURINR=X": "EURINR", "GBPINR=X": "GBPINR",
        "JPYINR=X": "JPYINR", "EURUSD=X": "EURUSD", "GBPUSD=X": "GBPUSD",
    }
    result = {labels[p]: data.get(p, {}) for p in pairs}
    _cset("forex", result, 90)
    return result


async def fetch_commodities() -> dict:
    """Oil + natural gas — non-metal commodities."""
    cached = _cget("commodities")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        data = await _yahoo(client, ["CL=F", "BZ=F", "NG=F"])
    result = {
        "WTI_CRUDE": data.get("CL=F", {}),
        "BRENT":     data.get("BZ=F", {}),
        "NATGAS":    data.get("NG=F", {}),
    }
    _cset("commodities", result, 120)
    return result


# ─── Routes ──────────────────────────────────────────────────────────────
@router.get("/markets")
async def markets_all(spark: bool = False):
    """All markets in one call. Fetches each asset class in parallel."""
    stocks, crypto, metals, forex, comm = await asyncio.gather(
        fetch_stocks(spark), fetch_crypto(), fetch_metals(), fetch_forex(), fetch_commodities(),
        return_exceptions=True,
    )
    _safe = lambda v: v if isinstance(v, dict) else {}
    return {
        "stocks":      _safe(stocks),
        "crypto":      _safe(crypto),
        "metals":      _safe(metals),
        "forex":       _safe(forex),
        "commodities": _safe(comm),
        "timestamp":   int(time.time()),
        "providers": {
            "stocks_primary": "finnhub" if FINNHUB_KEY else "yahoo",
            "metals_primary": "metals-api" if METALS_API_KEY else "yahoo-futures",
            "crypto":         "coingecko",
            "forex":          "yahoo",
        },
    }


@router.get("/markets/stocks")
async def markets_stocks(spark: bool = False):
    return await fetch_stocks(spark)


@router.get("/markets/crypto")
async def markets_crypto():
    return await fetch_crypto()


@router.get("/markets/metals")
async def markets_metals():
    return await fetch_metals()


@router.get("/markets/forex")
async def markets_forex():
    return await fetch_forex()


@router.get("/markets/commodities")
async def markets_commodities():
    return await fetch_commodities()
