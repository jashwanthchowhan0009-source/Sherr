"""
pipeline/explore_feeds.py — Explore page data, fetched on a schedule and cached.

Six independent fetchers write JSON into Upstash Redis; /explore/snapshot merges the
cached keys in one pass. The read path never calls an upstream API, which is what
keeps it inside the 100ms target — an upstream that is slow or down changes the age
of the data, not the latency of the page.

DELIBERATELY NO APP IMPORTS. Only os / httpx / stdlib, so the module can be loaded by
file path from the sqlite app too (same pattern as originality.py) without dragging in
asyncpg and the Supabase client. Keys come from the environment; none are hardcoded.

FAILURE MODEL. A fetcher that raises logs and returns the stale cached value. Stale
data with a visible `as_of` is useful; an empty tile is not, and a raised exception
inside an APScheduler job would silently kill that job for the process lifetime.
Every entry carries `as_of` and `stale` so the UI can say how old it is.

UPSTASH over the REST API rather than the redis client: it is plain HTTPS, so httpx
(already a dependency) is enough and there is no connection pool to manage in a
worker that mostly sleeps.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time

import httpx

log = logging.getLogger("sherbyte.explore")

UPSTASH_URL = (os.getenv("UPSTASH_REDIS_REST_URL") or "").rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN") or ""
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY") or ""

WEATHER_LAT = float(os.getenv("EXPLORE_LAT", "17.38"))
WEATHER_LON = float(os.getenv("EXPLORE_LON", "78.48"))

_PREFIX = "explore:"
# (key, interval_seconds). TTL is 2x the interval, so a value only expires after two
# consecutive failed refreshes rather than the moment one is late.
SCHEDULE = {
    "markets":     2 * 60,
    "forex":       5 * 60,
    "weather":    15 * 60,
    "space":      12 * 3600,
    "word_of_day": 12 * 3600,
    "news_top":   12 * 3600,
}

# Process-local mirror of every value ever written. Upstash being unreachable must not
# empty the page — this is what /explore/snapshot falls back to.
_LOCAL: dict = {}


# ─── cache ────────────────────────────────────────────────────────────────────
async def _upstash(client: httpx.AsyncClient, *path: str, body=None):
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    try:
        url = UPSTASH_URL + "/" + "/".join(path)
        headers = {"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        if body is None:
            r = await client.get(url, headers=headers, timeout=8)
        else:
            r = await client.post(url, headers=headers, content=body, timeout=8)
        if r.status_code != 200:
            log.warning("upstash %s for %s", r.status_code, path[0])
            return None
        return r.json().get("result")
    except Exception as e:
        log.warning("upstash %s failed: %s", path[0], e)
        return None


async def cache_set(client, name: str, value: dict, ttl: int) -> None:
    payload = {"data": value, "as_of": time.time()}
    _LOCAL[name] = payload
    # SET via POST so the value is not URL-encoded into the path.
    await _upstash(client, "set", _PREFIX + name, f"?EX={ttl}",
                   body=json.dumps(payload))


async def cache_get(client, name: str):
    raw = await _upstash(client, "get", _PREFIX + name)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return _LOCAL.get(name)


# ─── fetchers ─────────────────────────────────────────────────────────────────
_YF = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
# Symbol -> display key. Using the v8 chart endpoint directly rather than yfinance:
# yfinance pulls in pandas + numpy for what is one JSON call, and it wraps the same
# endpoint this codebase already uses successfully in workers/market_signals.py.
MARKET_SYMBOLS = {
    "^NSEI": "NIFTY50", "^BSESN": "SENSEX",
    "GC=F": "GOLD", "SI=F": "SILVER", "CL=F": "CRUDEOIL",
}


async def _quote(client: httpx.AsyncClient, sym: str) -> dict | None:
    r = await client.get(_YF.format(sym=sym), params={"range": "5d", "interval": "1d"},
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    if r.status_code != 200:
        return None
    meta = (r.json().get("chart", {}).get("result") or [{}])[0].get("meta", {})
    price = meta.get("regularMarketPrice")
    prev = meta.get("chartPreviousClose") or meta.get("previousClose")
    if not price:
        return None
    chg = (price - prev) if prev else 0.0
    return {"price": round(float(price), 2),
            "change": round(float(chg), 2),
            "change_pct": round(float(chg / prev * 100), 2) if prev else 0.0,
            "currency": meta.get("currency", "")}


async def markets(client: httpx.AsyncClient) -> dict:
    out: dict = {}

    async def one(sym, key):
        try:
            q = await _quote(client, sym)
            if q:
                out[key] = q
        except Exception as e:
            log.warning("market %s failed: %s", sym, e)

    await asyncio.gather(*(one(s, k) for s, k in MARKET_SYMBOLS.items()))

    # Bitcoin from CoinGecko's public endpoint (no key).
    try:
        r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                             params={"ids": "bitcoin", "vs_currencies": "usd",
                                     "include_24hr_change": "true"}, timeout=10)
        if r.status_code == 200:
            b = (r.json() or {}).get("bitcoin") or {}
            if b.get("usd"):
                out["BTC"] = {"price": round(float(b["usd"]), 2),
                              "change_pct": round(float(b.get("usd_24h_change") or 0), 2),
                              "currency": "USD"}
    except Exception as e:
        log.warning("coingecko failed: %s", e)
    return out


async def forex(client: httpx.AsyncClient) -> dict:
    r = await client.get("https://api.frankfurter.dev/v1/latest",
                         params={"base": "USD", "symbols": "INR,EUR,GBP"}, timeout=10)
    r.raise_for_status()
    rates = (r.json() or {}).get("rates") or {}
    usd_inr = rates.get("INR")
    out = {"USD/INR": usd_inr}
    # Frankfurter quotes against the base, so EUR/INR is INR-per-USD divided by
    # EUR-per-USD. Deriving it here keeps the page to one upstream call.
    if usd_inr and rates.get("EUR"):
        out["EUR/INR"] = round(usd_inr / rates["EUR"], 4)
    if usd_inr and rates.get("GBP"):
        out["GBP/INR"] = round(usd_inr / rates["GBP"], 4)
    return {k: v for k, v in out.items() if v}


async def weather(client: httpx.AsyncClient) -> dict:
    r = await client.get("https://api.open-meteo.com/v1/forecast", params={
        "latitude": WEATHER_LAT, "longitude": WEATHER_LON,
        "current": "temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m",
        "daily": "temperature_2m_max,temperature_2m_min", "timezone": "auto",
        "forecast_days": 3}, timeout=10)
    r.raise_for_status()
    d = r.json() or {}
    cur = d.get("current") or {}
    return {"lat": WEATHER_LAT, "lon": WEATHER_LON,
            "temp_c": cur.get("temperature_2m"),
            "humidity": cur.get("relative_humidity_2m"),
            "wind_kph": cur.get("wind_speed_10m"),
            "code": cur.get("weather_code"),
            "daily": d.get("daily") or {}}


async def space(client: httpx.AsyncClient) -> dict:
    r = await client.get("https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
                         params={"limit": 3}, timeout=15)
    r.raise_for_status()
    return {"launches": [{
        "name": l.get("name"),
        "net": l.get("net"),
        "provider": ((l.get("launch_service_provider") or {}).get("name")),
        "pad": ((l.get("pad") or {}).get("name")),
        "status": ((l.get("status") or {}).get("abbrev")),
    } for l in (r.json() or {}).get("results", [])]}


# Curated so the word is always one a reader benefits from seeing, and so the
# dictionary lookup is a known-good request rather than a random-word gamble.
WORD_LIST = [
    "ephemeral", "ubiquitous", "candour", "pragmatic", "nuance", "paradigm",
    "resilience", "salient", "tacit", "veracity", "arbitrage", "liquidity",
    "volatility", "hedge", "yield", "inflation", "recession", "surplus",
    "austerity", "subsidy", "tariff", "quorum", "mandate", "ordinance",
    "moratorium", "amortise", "collateral", "dividend", "equity", "leverage",
    "sovereign", "fiscal", "monetary", "remittance", "deficit", "bourse",
    "cadence", "conflate", "corollary", "empirical", "heuristic", "iterate",
    "juxtapose", "latent", "mitigate", "obfuscate", "precedent", "quantify",
    "rescind", "scrutiny", "threshold", "underwrite", "vindicate", "zeitgeist",
]


async def word_of_day(client: httpx.AsyncClient) -> dict:
    # Seeded on the UTC date, so every device sees the same word on the same day and
    # a restart does not reshuffle it.
    day = int(time.time() // 86400)
    word = WORD_LIST[random.Random(day).randrange(len(WORD_LIST))]
    r = await client.get(
        f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}", timeout=10)
    r.raise_for_status()
    entry = (r.json() or [{}])[0]
    meaning = (entry.get("meanings") or [{}])[0]
    definition = ((meaning.get("definitions") or [{}])[0])
    return {"word": word,
            "phonetic": entry.get("phonetic") or "",
            "part_of_speech": meaning.get("partOfSpeech") or "",
            "definition": definition.get("definition") or "",
            "example": definition.get("example") or ""}


async def news_top(client: httpx.AsyncClient) -> dict:
    if not GNEWS_API_KEY:
        raise RuntimeError("GNEWS_API_KEY not set")
    r = await client.get("https://gnews.io/api/v4/top-headlines", params={
        "country": "in", "lang": "en", "max": 10, "apikey": GNEWS_API_KEY}, timeout=15)
    r.raise_for_status()
    return {"articles": [{
        "title": a.get("title"),
        "url": a.get("url"),
        "source": ((a.get("source") or {}).get("name")),
        "published_at": a.get("publishedAt"),
    } for a in (r.json() or {}).get("articles", [])]}


FETCHERS = {"markets": markets, "forex": forex, "weather": weather,
            "space": space, "word_of_day": word_of_day, "news_top": news_top}


# ─── orchestration ────────────────────────────────────────────────────────────
async def refresh(name: str, client: httpx.AsyncClient | None = None) -> dict:
    """Run one fetcher and cache it. Never raises — a scheduler job that throws is
    silently dropped by APScheduler for the rest of the process lifetime."""
    own = client is None
    client = client or httpx.AsyncClient(follow_redirects=True)
    try:
        data = await FETCHERS[name](client)
        if data:
            await cache_set(client, name, data, SCHEDULE[name] * 2)
            return {"ok": True, "name": name}
        raise RuntimeError("fetcher returned nothing")
    except Exception as e:
        log.warning("explore fetch %s failed: %s", name, e)
        return {"ok": False, "name": name, "error": str(e)}
    finally:
        if own:
            await client.aclose()


async def refresh_all() -> dict:
    """Warm every key. Used on startup so the first request is never a cold read."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(*(refresh(n, client) for n in FETCHERS))
    ok = [r["name"] for r in results if r["ok"]]
    failed = {r["name"]: r["error"] for r in results if not r["ok"]}
    log.info("explore warm: %d/%d ok%s", len(ok), len(FETCHERS),
             f", failed: {failed}" if failed else "")
    return {"ok": ok, "failed": failed}


async def snapshot() -> dict:
    """Every cached section in one object. Pure cache read on the happy path.

    A section that has never been fetched comes back as null WITH a reason, rather
    than being omitted — the client can then render an honest empty state instead of
    guessing whether the key is missing or the value is genuinely absent.
    """
    now = time.time()
    out: dict = {}
    async with httpx.AsyncClient() as client:
        entries = await asyncio.gather(
            *(cache_get(client, n) for n in FETCHERS), return_exceptions=True)
    for name, entry in zip(FETCHERS, entries):
        if isinstance(entry, Exception) or not entry:
            out[name] = None
            continue
        age = now - float(entry.get("as_of") or 0)
        out[name] = {"data": entry.get("data"),
                     "as_of": entry.get("as_of"),
                     "age_seconds": round(age, 1),
                     # Past its refresh interval: still shown, but flagged so the UI
                     # can mark it rather than passing stale numbers off as live.
                     "stale": age > SCHEDULE[name]}
    out["_missing"] = [n for n in FETCHERS if out.get(n) is None]
    return out


def register_jobs(scheduler) -> None:
    """Attach every fetcher to an existing APScheduler instance."""
    for name, seconds in SCHEDULE.items():
        scheduler.add_job(refresh, "interval", seconds=seconds,
                          args=[name], id=f"explore_{name}",
                          replace_existing=True, max_instances=1)
    log.info("explore jobs registered: %s", ", ".join(SCHEDULE))
