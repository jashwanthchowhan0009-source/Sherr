"""
api/markets.py — Real-time market data aggregator (ported from the MVP).

Providers:
  • Stocks/forex/futures: Yahoo Finance (free) + optional Finnhub for stocks.
  • Crypto:               CoinGecko (free, no key).
  • Metals:               Metals-API (optional) → Yahoo futures fallback.

In-memory TTL caches per asset class; graceful degradation (returns {} on
provider failure so the UI never crashes).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import os

import httpx
from fastapi import APIRouter

log = logging.getLogger("sherbyte.markets")
router = APIRouter(prefix="/markets", tags=["markets"])

# Optional provider keys (markets degrade gracefully to free Yahoo/CoinGecko).
FINNHUB_KEY = os.getenv("FINNHUB_KEY", "")
METALS_API_KEY = os.getenv("METALS_API_KEY", "")

# ─── TTL cache ────────────────────────────────────────────────────────────────
_cache: dict = {}


def _cget(key: str):
    e = _cache.get(key)
    if not e or time.time() > e["exp"]:
        return None
    return e["data"]


def _cset(key: str, data, ttl: int):
    _cache[key] = {"data": data, "exp": time.time() + ttl}


# ─── Providers ────────────────────────────────────────────────────────────────
async def _yahoo(client: httpx.AsyncClient, symbols: list[str]) -> dict:
    """Quote via the v8 chart endpoint's `meta` block.

    Yahoo's old /v7/finance/quote now requires a crumb+cookie session and
    returns 401/429 for anonymous servers (which silently zeroed out stocks,
    forex and metals). The /v8/finance/chart endpoint still serves anonymously
    and its `meta` carries the live price + previous close, so we derive quotes
    from there. Symbols are fetched concurrently.
    """
    pairs = await asyncio.gather(*[_yahoo_quote_one(client, s) for s in symbols])
    return {sym: data for sym, data in pairs if data}


async def _yahoo_quote_one(client: httpx.AsyncClient, symbol: str) -> tuple[str, dict]:
    try:
        r = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "5m", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0 (compatible; SherByte/6.0)"},
            timeout=8,
        )
        if r.status_code != 200:
            return symbol, {}
        result = r.json().get("chart", {}).get("result", [])
        if not result:
            return symbol, {}
        meta = result[0].get("meta", {})
        price = meta.get("regularMarketPrice", 0) or 0
        prev = meta.get("chartPreviousClose") or meta.get("previousClose") or 0
        change = (price - prev) if (price and prev) else 0
        change_pct = (change / prev * 100) if prev else 0
        return symbol, {
            "price": round(price, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "high": round(meta.get("regularMarketDayHigh", 0) or 0, 2),
            "low": round(meta.get("regularMarketDayLow", 0) or 0, 2),
            "prev_close": round(prev, 2),
            "currency": meta.get("currency", ""),
        }
    except Exception as e:
        log.warning("Yahoo chart %s failed: %s", symbol, e)
        return symbol, {}


async def _yahoo_history(client: httpx.AsyncClient, symbol: str, points: int = 20) -> list[float]:
    try:
        r = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "5m", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0 (compatible; SherByte/6.0)"},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        result = r.json().get("chart", {}).get("result", [])
        if not result:
            return []
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        return [round(c, 2) for c in closes if c is not None][-points:]
    except Exception:
        return []


async def _coingecko(client: httpx.AsyncClient, ids: list[str]) -> dict:
    try:
        r = await client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ",".join(ids), "vs_currencies": "usd,inr",
                    "include_24hr_change": "true", "include_market_cap": "true"},
            timeout=8,
        )
        if r.status_code != 200:
            return {}
        name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
                    "dogecoin": "DOGE", "cardano": "ADA", "ripple": "XRP",
                    "binancecoin": "BNB", "polkadot": "DOT"}
        out = {}
        for coin, d in r.json().items():
            out[name_map.get(coin, coin.upper())] = {
                "price_usd": round(d.get("usd", 0) or 0, 2),
                "price_inr": round(d.get("inr", 0) or 0, 2),
                "change_pct": round(d.get("usd_24h_change", 0) or 0, 2),
                "market_cap_usd": int(d.get("usd_market_cap", 0) or 0),
            }
        return out
    except Exception as e:
        log.warning("CoinGecko failed: %s", e)
        return {}


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
        for sym, label in [("XAU", "GOLD"), ("XAG", "SILVER"),
                           ("XPT", "PLATINUM"), ("XPD", "PALLADIUM")]:
            rate = rates.get(sym)
            if rate and rate > 0:
                out[label] = {"price_usd_oz": round(1 / rate, 2)}
        return out
    except Exception as e:
        log.warning("Metals-API failed: %s", e)
        return {}


# ─── Aggregators ──────────────────────────────────────────────────────────────
async def fetch_stocks(with_sparkline: bool = False) -> dict:
    cached = _cget(f"stocks_{with_sparkline}")
    if cached:
        return cached
    symbols = ["^NSEI", "^BSESN", "^IXIC", "^GSPC", "^DJI", "^FTSE", "^N225"]
    labels = {"^NSEI": "NIFTY", "^BSESN": "SENSEX", "^IXIC": "NASDAQ",
              "^GSPC": "SP500", "^DJI": "DOW", "^FTSE": "FTSE", "^N225": "NIKKEI"}
    async with httpx.AsyncClient() as client:
        base = await _yahoo(client, symbols)
        result = {labels[s]: base.get(s, {}) for s in symbols}
        if with_sparkline:
            sparks = await asyncio.gather(*[_yahoo_history(client, s, 20) for s in symbols])
            for s, spark in zip(symbols, sparks):
                if spark and result.get(labels[s]):
                    result[labels[s]]["spark"] = spark
    _cset(f"stocks_{with_sparkline}", result, 60)
    return result


async def fetch_crypto() -> dict:
    cached = _cget("crypto")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        data = await _coingecko(client, ["bitcoin", "ethereum", "solana", "dogecoin",
                                         "cardano", "ripple", "binancecoin"])
    _cset("crypto", data, 45)
    return data


async def fetch_metals() -> dict:
    cached = _cget("metals")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        metals = await _metals_api(client)
        fut = await _yahoo(client, ["GC=F", "SI=F", "PL=F", "PA=F"])
        fb = {"GC=F": "GOLD", "SI=F": "SILVER", "PL=F": "PLATINUM", "PA=F": "PALLADIUM"}
        for sym, label in fb.items():
            y = fut.get(sym, {})
            if not y:
                continue
            if label not in metals or not metals[label]:
                metals[label] = {"price_usd_oz": y["price"], "change": y["change"],
                                 "change_pct": y["change_pct"]}
            else:
                metals[label].setdefault("change", y.get("change", 0))
                metals[label].setdefault("change_pct", y.get("change_pct", 0))
        fx = await _yahoo(client, ["USDINR=X"])
        usd_inr = (fx.get("USDINR=X") or {}).get("price", 83.0)
        for d in metals.values():
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
    labels = {"USDINR=X": "USDINR", "EURINR=X": "EURINR", "GBPINR=X": "GBPINR",
              "JPYINR=X": "JPYINR", "EURUSD=X": "EURUSD", "GBPUSD=X": "GBPUSD"}
    result = {labels[p]: data.get(p, {}) for p in pairs}
    _cset("forex", result, 90)
    return result


async def fetch_commodities() -> dict:
    cached = _cget("commodities")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        data = await _yahoo(client, ["CL=F", "BZ=F", "NG=F"])
    result = {"WTI_CRUDE": data.get("CL=F", {}), "BRENT": data.get("BZ=F", {}),
              "NATGAS": data.get("NG=F", {})}
    _cset("commodities", result, 120)
    return result


# ─── Routes ───────────────────────────────────────────────────────────────────
@router.get("")
@router.get("/")
async def markets_all(spark: bool = False):
    stocks, crypto, metals, forex, comm = await asyncio.gather(
        fetch_stocks(spark), fetch_crypto(), fetch_metals(), fetch_forex(), fetch_commodities(),
        return_exceptions=True,
    )
    _safe = lambda v: v if isinstance(v, dict) else {}
    return {
        "stocks": _safe(stocks), "crypto": _safe(crypto), "metals": _safe(metals),
        "forex": _safe(forex), "commodities": _safe(comm),
        "timestamp": int(time.time()),
        "providers": {
            "stocks_primary": "finnhub" if FINNHUB_KEY else "yahoo",
            "metals_primary": "metals-api" if METALS_API_KEY else "yahoo-futures",
            "crypto": "coingecko", "forex": "yahoo",
        },
    }


@router.get("/stocks")
async def markets_stocks(spark: bool = False):
    return await fetch_stocks(spark)


@router.get("/crypto")
async def markets_crypto():
    return await fetch_crypto()


@router.get("/metals")
async def markets_metals():
    return await fetch_metals()


@router.get("/forex")
async def markets_forex():
    return await fetch_forex()


@router.get("/commodities")
async def markets_commodities():
    return await fetch_commodities()


# ─── Historical series (for per-item detail graphs) ────────────────────────────
_COIN_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "DOGE": "dogecoin",
             "ADA": "cardano", "XRP": "ripple", "BNB": "binancecoin", "DOT": "polkadot"}
_STOCK_SYM = {"NIFTY": "^NSEI", "SENSEX": "^BSESN", "NASDAQ": "^IXIC", "SP500": "^GSPC",
              "DOW": "^DJI", "FTSE": "^FTSE", "NIKKEI": "^N225"}
_FOREX_SYM = {"USDINR": "USDINR=X", "EURINR": "EURINR=X", "GBPINR": "GBPINR=X",
              "JPYINR": "JPYINR=X", "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X"}
_METAL_SYM = {"GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F"}


async def _yahoo_series(client: httpx.AsyncClient, symbol: str, rng: str) -> list[dict]:
    try:
        r = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "1d", "range": rng},
            headers={"User-Agent": "Mozilla/5.0 (compatible; SherByte/6.0)"},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        res = r.json().get("chart", {}).get("result", [])
        if not res:
            return []
        ts = res[0].get("timestamp", []) or []
        closes = res[0].get("indicators", {}).get("quote", [{}])[0].get("close", []) or []
        return [{"t": int(t) * 1000, "p": round(c, 2)}
                for t, c in zip(ts, closes) if c is not None]
    except Exception as e:
        log.warning("Yahoo series %s failed: %s", symbol, e)
        return []


@router.get("/history")
async def markets_history(category: str, symbol: str, days: int = 30):
    """Historical price series for one item: CoinGecko for crypto, Yahoo for
    stocks/forex/metals. Returns [{t: epoch_ms, p: price}, ...]."""
    cat, sym = category.lower(), symbol.upper()
    key = f"hist_{cat}_{sym}_{days}"
    cached = _cget(key)
    if cached:
        return cached
    series: list[dict] = []
    async with httpx.AsyncClient() as client:
        if cat == "crypto":
            cid = _COIN_IDS.get(sym, sym.lower())
            try:
                r = await client.get(
                    f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart",
                    params={"vs_currency": "usd", "days": days}, timeout=8,
                )
                if r.status_code == 200:
                    series = [{"t": int(p[0]), "p": round(p[1], 4)}
                              for p in r.json().get("prices", [])]
            except Exception as e:
                log.warning("CoinGecko history failed: %s", e)
        else:
            symap = {"stocks": _STOCK_SYM, "forex": _FOREX_SYM, "metals": _METAL_SYM}.get(cat, {})
            ysym = symap.get(sym, sym)
            rng = "1mo" if days <= 31 else ("3mo" if days <= 93 else "1y")
            series = await _yahoo_series(client, ysym, rng)
    out = {"category": cat, "symbol": sym, "series": series}
    _cset(key, out, 600)
    return out
