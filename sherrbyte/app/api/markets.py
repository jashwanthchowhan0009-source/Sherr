"""
api/markets.py — Real-time market data aggregator (ported from the MVP).

Providers:
  • Stocks/forex/futures: Yahoo Finance (free) + optional Finnhub for stocks.
  • Crypto:               CoinGecko (free, no key).
  • Metals:               gold-api.com spot (free, no key) → metals.dev (keyed)
                          → metals-api (keyed) → Yahoo futures fallback.

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

from app.db.supabase import db

log = logging.getLogger("sherbyte.markets")
router = APIRouter(prefix="/markets", tags=["markets"])

# Provider keys — read from env/secrets ONLY, never hardcoded.
FINNHUB_KEY    = os.getenv("FINNHUB_KEY", "")
METALS_API_KEY = os.getenv("METALS_API_KEY", "")      # legacy metals-api.com
COINGECKO_KEY  = os.getenv("COINGECKO_KEY", "")       # CoinGecko demo key (x-cg-demo-api-key)
METALS_DEV_KEY = os.getenv("METALS_DEV_KEY", "")      # metals.dev

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
            headers=({"x-cg-demo-api-key": COINGECKO_KEY} if COINGECKO_KEY else {}),
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


async def _gold_api(client: httpx.AsyncClient) -> dict:
    """gold-api.com — free, keyless live spot price (USD / troy oz).

    This is the authoritative *spot* source and is preferred over Yahoo's GC=F
    *futures* contract, whose front-month quote can run well above spot and made
    the gold rate look wrong (e.g. an inflated ₹/10g headline). Each metal is
    fetched concurrently; any miss falls through to the keyed providers / Yahoo.
    """
    out: dict = {}

    async def one(sym: str, label: str) -> None:
        try:
            r = await client.get(
                f"https://api.gold-api.com/price/{sym}",
                headers={"User-Agent": "Mozilla/5.0 (compatible; SherByte/6.0)"},
                timeout=8,
            )
            if r.status_code != 200:
                return
            price = r.json().get("price")
            if price and float(price) > 0:
                out[label] = {"price_usd_oz": round(float(price), 2)}
        except Exception as e:
            log.debug("gold-api %s failed: %s", sym, e)

    await asyncio.gather(*[
        one("XAU", "GOLD"), one("XAG", "SILVER"),
        one("XPT", "PLATINUM"), one("XPD", "PALLADIUM"),
    ])
    return out


async def _metals_dev(client: httpx.AsyncClient) -> dict:
    """metals.dev latest spot (USD / troy oz). Keyed; empty if no key."""
    if not METALS_DEV_KEY:
        return {}
    try:
        r = await client.get(
            "https://api.metals.dev/v1/latest",
            params={"api_key": METALS_DEV_KEY, "currency": "USD", "unit": "toz"},
            timeout=8,
        )
        if r.status_code != 200:
            return {}
        m = r.json().get("metals", {})
        out = {}
        for k, label in [("gold", "GOLD"), ("silver", "SILVER"),
                         ("platinum", "PLATINUM"), ("palladium", "PALLADIUM")]:
            v = m.get(k)
            if v:
                out[label] = {"price_usd_oz": round(float(v), 2)}
        return out
    except Exception as e:
        log.warning("metals.dev failed: %s", e)
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


async def _coincap(client: httpx.AsyncClient, ids: list[str]) -> dict:
    """CoinCap — free & keyless. Fallback for CoinGecko's rate-limited demo tier."""
    try:
        r = await client.get(
            "https://api.coincap.io/v2/assets",
            params={"ids": ",".join(ids)}, timeout=8,
        )
        if r.status_code != 200:
            return {}
        name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
                    "dogecoin": "DOGE", "cardano": "ADA", "ripple": "XRP",
                    "binancecoin": "BNB", "polkadot": "DOT"}
        out = {}
        for d in r.json().get("data", []):
            cid = d.get("id", "")
            price = float(d.get("priceUsd") or 0)
            out[name_map.get(cid, (d.get("symbol") or cid).upper())] = {
                "price_usd": round(price, 2),
                "price_inr": round(price * 84.0, 2),   # approx; CoinCap is USD-only
                "change_pct": round(float(d.get("changePercent24Hr") or 0), 2),
                "market_cap_usd": int(float(d.get("marketCapUsd") or 0)),
            }
        return out
    except Exception as e:
        log.warning("CoinCap failed: %s", e)
        return {}


async def fetch_crypto() -> dict:
    cached = _cget("crypto")
    if cached:
        return cached
    ids = ["bitcoin", "ethereum", "solana", "dogecoin", "cardano", "ripple", "binancecoin", "polkadot"]
    async with httpx.AsyncClient() as client:
        data = await _coingecko(client, ids)
        # CoinGecko's keyless tier is heavily rate-limited and frequently returns
        # nothing → the tiles rendered $0. Fill any missing coin from CoinCap.
        if not data or any(k not in data for k in ("BTC", "ETH", "SOL")):
            for k, v in (await _coincap(client, ids)).items():
                data.setdefault(k, v)
    if data:
        _cset("crypto", data, 45)
    return data


async def _snapshot_metals(metals: dict) -> None:
    """Persist one row per metal per day so we can build our own history graph
    (metals.dev historical data is paywalled on the free tier)."""
    try:
        for label, d in metals.items():
            oz = d.get("price_usd_oz")
            if oz:
                await db.execute(
                    """INSERT INTO metals_history (metal, day, price_usd_oz)
                       VALUES ($1, CURRENT_DATE, $2)
                       ON CONFLICT (metal, day) DO UPDATE SET price_usd_oz = excluded.price_usd_oz""",
                    label, float(oz),
                )
    except Exception as e:
        log.debug("metals snapshot skipped: %s", e)


async def fetch_metals() -> dict:
    cached = _cget("metals")
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        # Authoritative live spot first (gold-api.com, keyless), then keyed
        # providers; fill in any metal a higher-priority source missed.
        metals: dict = {}
        for src in (_gold_api, _metals_dev, _metals_api):
            try:
                got = await src(client)
            except Exception:
                got = {}
            for label, d in got.items():
                if d.get("price_usd_oz") and label not in metals:
                    metals[label] = dict(d)
        # Yahoo futures: last-resort price for any metal still missing, and the
        # source of day-change figures the spot providers don't supply.
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
        # Indian retail = international spot + ~6% import duty + 3% GST + premium.
        # Pure spot conversion under-reads vs the ₹/10g price Indians actually see,
        # so apply a retail factor to land in the real retail range (24K).
        INDIA_RETAIL_FACTOR = 1.17
        for d in metals.values():
            if "price_usd_oz" in d:
                # 10 g = 0.32154 troy oz; ₹/10g = USD/oz × oz_per_10g × USDINR × retail.
                d["price_inr_10g"] = round(d["price_usd_oz"] * 0.3215 * usd_inr * INDIA_RETAIL_FACTOR, 0)
    await _snapshot_metals(metals)   # build daily history for the detail graphs
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
    syms = ["CL=F", "BZ=F", "NG=F", "HG=F", "ZW=F", "ZC=F", "SB=F"]
    async with httpx.AsyncClient() as client:
        data = await _yahoo(client, syms)
    result = {"WTI_CRUDE": data.get("CL=F", {}), "BRENT": data.get("BZ=F", {}),
              "NATGAS": data.get("NG=F", {}), "COPPER": data.get("HG=F", {}),
              "WHEAT": data.get("ZW=F", {}), "CORN": data.get("ZC=F", {}),
              "SUGAR": data.get("SB=F", {})}
    _cset("commodities", result, 120)
    return result


async def fetch_sectors() -> dict:
    """Indian sectoral indices — Bank Nifty, Nifty IT/Auto/Pharma/FMCG/Metal."""
    cached = _cget("sectors")
    if cached:
        return cached
    symbols = ["^NSEBANK", "^CNXIT", "^CNXAUTO", "^CNXPHARMA", "^CNXFMCG", "^CNXMETAL"]
    labels = {"^NSEBANK": "BANKNIFTY", "^CNXIT": "NIFTY_IT", "^CNXAUTO": "NIFTY_AUTO",
              "^CNXPHARMA": "NIFTY_PHARMA", "^CNXFMCG": "NIFTY_FMCG", "^CNXMETAL": "NIFTY_METAL"}
    async with httpx.AsyncClient() as client:
        base = await _yahoo(client, symbols)
    result = {labels[s]: base.get(s, {}) for s in symbols}
    _cset("sectors", result, 90)
    return result


async def fetch_rates() -> dict:
    """US Treasury bond yields (CBOE indices) — 13-week, 5Y, 10Y, 30Y. The
    Yahoo 'price' for these is the yield in percent."""
    cached = _cget("rates")
    if cached:
        return cached
    symbols = ["^IRX", "^FVX", "^TNX", "^TYX"]
    labels = {"^IRX": "US13W", "^FVX": "US5Y", "^TNX": "US10Y", "^TYX": "US30Y"}
    async with httpx.AsyncClient() as client:
        base = await _yahoo(client, symbols)
    result = {labels[s]: base.get(s, {}) for s in symbols}
    _cset("rates", result, 120)
    return result


# ─── Routes ───────────────────────────────────────────────────────────────────
@router.get("")
@router.get("/")
async def markets_all(spark: bool = False):
    stocks, crypto, metals, forex, comm, sectors, rates = await asyncio.gather(
        fetch_stocks(spark), fetch_crypto(), fetch_metals(), fetch_forex(),
        fetch_commodities(), fetch_sectors(), fetch_rates(),
        return_exceptions=True,
    )
    _safe = lambda v: v if isinstance(v, dict) else {}
    return {
        "stocks": _safe(stocks), "crypto": _safe(crypto), "metals": _safe(metals),
        "forex": _safe(forex), "commodities": _safe(comm),
        "sectors": _safe(sectors), "rates": _safe(rates),
        "timestamp": int(time.time()),
        "providers": {
            "stocks_primary": "finnhub" if FINNHUB_KEY else "yahoo",
            "metals_primary": "gold-api",
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


@router.get("/sectors")
async def markets_sectors():
    return await fetch_sectors()


@router.get("/rates")
async def markets_rates():
    return await fetch_rates()


# ─── Historical series (for per-item detail graphs) ────────────────────────────
_COIN_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "DOGE": "dogecoin",
             "ADA": "cardano", "XRP": "ripple", "BNB": "binancecoin", "DOT": "polkadot"}
_STOCK_SYM = {"NIFTY": "^NSEI", "SENSEX": "^BSESN", "NASDAQ": "^IXIC", "SP500": "^GSPC",
              "DOW": "^DJI", "FTSE": "^FTSE", "NIKKEI": "^N225"}
_FOREX_SYM = {"USDINR": "USDINR=X", "EURINR": "EURINR=X", "GBPINR": "GBPINR=X",
              "JPYINR": "JPYINR=X", "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X"}
_METAL_SYM = {"GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F"}
_COMMO_SYM = {"WTI_CRUDE": "CL=F", "BRENT": "BZ=F", "NATGAS": "NG=F", "COPPER": "HG=F",
              "WHEAT": "ZW=F", "CORN": "ZC=F", "SUGAR": "SB=F"}
_SECTOR_SYM = {"BANKNIFTY": "^NSEBANK", "NIFTY_IT": "^CNXIT", "NIFTY_AUTO": "^CNXAUTO",
               "NIFTY_PHARMA": "^CNXPHARMA", "NIFTY_FMCG": "^CNXFMCG", "NIFTY_METAL": "^CNXMETAL"}
_RATES_SYM = {"US13W": "^IRX", "US5Y": "^FVX", "US10Y": "^TNX", "US30Y": "^TYX"}

# Constituent companies shown when an index is opened (symbol, display name).
_INDEX_CONSTITUENTS = {
    "NASDAQ": [("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "Nvidia"), ("TSLA", "Tesla"),
               ("AMZN", "Amazon"), ("GOOGL", "Alphabet"), ("META", "Meta"), ("NFLX", "Netflix"), ("AMD", "AMD")],
    "SP500":  [("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "Nvidia"), ("AMZN", "Amazon"),
               ("BRK-B", "Berkshire"), ("JPM", "JPMorgan"), ("V", "Visa"), ("UNH", "UnitedHealth")],
    "DOW":    [("AAPL", "Apple"), ("MSFT", "Microsoft"), ("JPM", "JPMorgan"), ("V", "Visa"),
               ("HD", "Home Depot"), ("KO", "Coca-Cola"), ("MCD", "McDonald's"), ("DIS", "Disney")],
    "NIFTY":  [("RELIANCE.NS", "Reliance"), ("TCS.NS", "TCS"), ("HDFCBANK.NS", "HDFC Bank"),
               ("INFY.NS", "Infosys"), ("ICICIBANK.NS", "ICICI Bank"), ("TITAN.NS", "Titan"),
               ("TATAMOTORS.NS", "Tata Motors"), ("BHARTIARTL.NS", "Airtel"), ("SBIN.NS", "SBI")],
    "SENSEX": [("RELIANCE.NS", "Reliance"), ("TCS.NS", "TCS"), ("HDFCBANK.NS", "HDFC Bank"),
               ("INFY.NS", "Infosys"), ("ICICIBANK.NS", "ICICI Bank"), ("ITC.NS", "ITC"),
               ("LT.NS", "L&T"), ("SBIN.NS", "SBI")],
}


async def _yahoo_series(client: httpx.AsyncClient, symbol: str, rng: str, interval: str = "1d") -> list[dict]:
    try:
        r = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": interval, "range": rng},
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


# Range → (Yahoo range, Yahoo interval) and (CoinGecko days) for the chart tabs.
_RANGE_YF = {
    "1D": ("1d", "5m"), "1W": ("5d", "30m"), "1M": ("1mo", "1d"),
    "6M": ("6mo", "1d"), "1Y": ("1y", "1d"), "5Y": ("5y", "1wk"), "MAX": ("max", "1mo"),
}
_RANGE_CG = {"1D": "1", "1W": "7", "1M": "30", "6M": "180", "1Y": "365", "5Y": "1825", "MAX": "max"}


@router.get("/history")
async def markets_history(category: str, symbol: str, range: str = "1M", days: int = 0):
    """Historical price series for one item across a timeframe (1D/1W/1M/6M/1Y/5Y/MAX).
    CoinGecko for crypto, Yahoo for stocks/forex/metals. Returns
    [{t: epoch_ms, p: price}, ...]. Metals are USD/oz (frontend → ₹/10g)."""
    cat, sym = category.lower(), symbol.upper()
    rng = (range or "1M").upper()
    if rng not in _RANGE_YF:
        rng = "1M"
    key = f"hist_{cat}_{sym}_{rng}"
    cached = _cget(key)
    if cached:
        return cached
    series: list[dict] = []
    yf_range, yf_int = _RANGE_YF[rng]
    async with httpx.AsyncClient() as client:
        if cat == "crypto":
            cid = _COIN_IDS.get(sym, sym.lower())
            try:
                r = await client.get(
                    f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart",
                    params={"vs_currency": "usd", "days": _RANGE_CG[rng]}, timeout=8,
                )
                if r.status_code == 200:
                    series = [{"t": int(p[0]), "p": round(p[1], 4)}
                              for p in r.json().get("prices", [])]
            except Exception as e:
                log.warning("CoinGecko history failed: %s", e)
        elif cat == "metals":
            series = await _yahoo_series(client, _METAL_SYM.get(sym, sym), yf_range, yf_int)
        else:
            symap = {"stocks": _STOCK_SYM, "forex": _FOREX_SYM,
                     "commodities": _COMMO_SYM, "sectors": _SECTOR_SYM,
                     "rates": _RATES_SYM}.get(cat, {})
            ysym = symap.get(sym, sym)
            series = await _yahoo_series(client, ysym, yf_range, yf_int)
    out = {"category": cat, "symbol": sym, "range": rng, "series": series}
    _cset(key, out, 600)
    return out


@router.get("/constituents")
async def markets_constituents(index: str):
    """Live quotes for the companies that make up an index (Apple/Tesla for
    NASDAQ, Reliance/TCS for NIFTY, …) — shown when the index is opened."""
    idx = index.upper()
    cons = _INDEX_CONSTITUENTS.get(idx, [])
    if not cons:
        return {"index": idx, "constituents": []}
    key = f"cons_{idx}"
    cached = _cget(key)
    if cached:
        return cached
    async with httpx.AsyncClient() as client:
        quotes = await _yahoo(client, [s for s, _ in cons])
    items = []
    for sym, name in cons:
        q = quotes.get(sym, {})
        items.append({
            "symbol": sym, "name": name,
            "price": q.get("price", 0), "change_pct": q.get("change_pct", 0),
            "currency": q.get("currency", ""),
        })
    out = {"index": idx, "constituents": items}
    _cset(key, out, 60)
    return out
