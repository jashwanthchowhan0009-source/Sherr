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
import re
import time
from datetime import datetime, timedelta, timezone

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
    "world_bank": 24 * 3600,     # annual series; daily is already generous
    "nasa":       12 * 3600,     # APOD changes once a day
    "flights":    12 * 3600,     # anonymous OpenSky is heavily rate limited
    "macro_rates": 6 * 3600,
    "govt_press":  1 * 3600,
    # Part B. Intervals follow how fast each source actually changes: crypto is
    # continuous, mandi is published once a day, GitHub trending barely moves
    # within a day.
    "crypto":          3 * 60,
    "forex_pairs":     10 * 60,
    "mandi":           12 * 3600,
    "launches":        6 * 3600,
    "ai_papers":       6 * 3600,
    "github_repos":    12 * 3600,
    "tech_news":       30 * 60,
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



# ─── second tranche: zero-key public datasets ─────────────────────────────────
FRED_API_KEY = os.getenv("FRED_API_KEY") or ""
NASA_API_KEY = os.getenv("NASA_API_KEY") or "DEMO_KEY"   # DEMO_KEY works, rate-limited
DATA_GOV_IN_KEY = os.getenv("DATA_GOV_IN_KEY") or ""


async def world_bank(client: httpx.AsyncClient) -> dict:
    """India macro indicators. One call per series; the API is per-indicator."""
    series = {"NY.GDP.MKTP.KD.ZG": "gdp_growth_pct",
              "FP.CPI.TOTL.ZG": "inflation_pct",
              "SL.UEM.TOTL.ZS": "unemployment_pct"}
    out: dict = {}

    async def one(code, key):
        try:
            r = await client.get(
                f"https://api.worldbank.org/v2/country/IND/indicator/{code}",
                params={"format": "json", "per_page": 5, "mrnev": 1}, timeout=12)
            if r.status_code != 200:
                return
            payload = r.json()
            rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
            if rows and rows[0].get("value") is not None:
                out[key] = {"value": round(float(rows[0]["value"]), 2),
                            "year": rows[0].get("date"),
                            "indicator": (rows[0].get("indicator") or {}).get("value")}
        except Exception as e:
            log.warning("world bank %s failed: %s", code, e)

    await asyncio.gather(*(one(c, k) for c, k in series.items()))
    return out


async def nasa(client: httpx.AsyncClient) -> dict:
    """Astronomy Picture of the Day. Image URL is NASA-hosted and free to use."""
    r = await client.get("https://api.nasa.gov/planetary/apod",
                         params={"api_key": NASA_API_KEY}, timeout=12)
    r.raise_for_status()
    d = r.json() or {}
    return {"title": d.get("title"), "date": d.get("date"),
            "explanation": (d.get("explanation") or "")[:600],
            "media_type": d.get("media_type"),
            "url": d.get("url"), "hdurl": d.get("hdurl"),
            "copyright": d.get("copyright") or "NASA"}


async def flights(client: httpx.AsyncClient) -> dict:
    """Live aircraft over India via OpenSky. Anonymous access is heavily rate
    limited, so this is a 12h job and a small bounding box, not a live tracker."""
    r = await client.get("https://opensky-network.org/api/states/all",
                         params={"lamin": 6.5, "lomin": 68.0,
                                 "lamax": 35.5, "lomax": 97.5}, timeout=20)
    r.raise_for_status()
    states = (r.json() or {}).get("states") or []
    return {"count": len(states), "sample": [{
        "callsign": (s[1] or "").strip(), "origin_country": s[2],
        "lon": s[5], "lat": s[6], "altitude_m": s[7],
        "velocity_ms": s[9], "on_ground": s[8],
    } for s in states[:15] if s and s[5] and s[6]]}


async def macro_rates(client: httpx.AsyncClient) -> dict:
    """US macro from FRED. Skipped cleanly when no key is configured."""
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY not set")
    series = {"DFF": "fed_funds_rate", "DGS10": "us_10y_yield",
              "CPIAUCSL": "us_cpi_index", "UNRATE": "us_unemployment"}
    out: dict = {}

    async def one(sid, key):
        try:
            r = await client.get("https://api.stlouisfed.org/fred/series/observations",
                                 params={"series_id": sid, "api_key": FRED_API_KEY,
                                         "file_type": "json", "sort_order": "desc",
                                         "limit": 1}, timeout=12)
            if r.status_code != 200:
                return
            obs = (r.json() or {}).get("observations") or []
            if obs and obs[0].get("value") not in (None, "", "."):
                out[key] = {"value": float(obs[0]["value"]), "date": obs[0].get("date")}
        except Exception as e:
            log.warning("fred %s failed: %s", sid, e)

    await asyncio.gather(*(one(s, k) for s, k in series.items()))
    return out


_RSS_TITLE = re.compile(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", re.S)
_RSS_LINK = re.compile(r"<link>(.*?)</link>", re.S)
_RSS_DATE = re.compile(r"<pubDate>(.*?)</pubDate>", re.S)


async def govt_press(client: httpx.AsyncClient) -> dict:
    """PIB press releases. RSS, so parsed with regex rather than adding a parser
    dependency — the feed is a fixed, well-formed shape."""
    r = await client.get("https://www.pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
                         timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    items = r.text.split("<item>")[1:11]
    out = []
    for it in items:
        t = _RSS_TITLE.search(it)
        l = _RSS_LINK.search(it)
        d = _RSS_DATE.search(it)
        if t:
            out.append({"title": t.group(1).strip(),
                        "url": (l.group(1).strip() if l else ""),
                        "published_at": (d.group(1).strip() if d else "")})
    if not out:
        raise RuntimeError("no items parsed from PIB feed")
    return {"releases": out}


# ─── Part B additions ─────────────────────────────────────────────────────────
# Same contract as everything above: one async function, a short timeout, a plain
# dict out, and an exception on failure so refresh() records it rather than
# caching an empty shape that looks like real data.

CRYPTO_IDS = "bitcoin,ethereum,solana,ripple,binancecoin"
FX_PAIRS = [("USD", "INR"), ("EUR", "USD"), ("GBP", "INR"),
            ("EUR", "INR"), ("JPY", "INR"), ("AED", "INR"), ("USD", "CNY")]
MANDI_COMMODITIES = ("Onion", "Tomato", "Potato")
MANDI_RESOURCE = "9ef84268-d588-465a-a308-a864a43d0070"


async def crypto(client: httpx.AsyncClient) -> dict:
    """Prices, BTC dominance and the Fear & Greed index.

    Three independent upstreams gathered concurrently: one being down costs that
    field, not the section.
    """
    async def prices():
        r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                             params={"ids": CRYPTO_IDS, "vs_currencies": "usd",
                                     "include_24hr_change": "true"}, timeout=10)
        r.raise_for_status()
        return {k.upper(): {"price": v.get("usd"),
                            "change_pct": round(v.get("usd_24h_change") or 0, 2)}
                for k, v in (r.json() or {}).items()}

    async def dominance():
        r = await client.get("https://api.coingecko.com/api/v3/global", timeout=10)
        r.raise_for_status()
        d = ((r.json() or {}).get("data") or {}).get("market_cap_percentage") or {}
        return round(float(d.get("btc") or 0), 1)

    async def fng():
        r = await client.get("https://api.alternative.me/fng/", timeout=10)
        r.raise_for_status()
        row = ((r.json() or {}).get("data") or [{}])[0]
        return {"value": int(row.get("value") or 0),
                "label": row.get("value_classification") or ""}

    got = await asyncio.gather(prices(), dominance(), fng(), return_exceptions=True)
    coins, dom, greed = [None if isinstance(g, Exception) else g for g in got]
    if not coins:
        raise RuntimeError("coingecko returned no prices")
    return {"coins": coins, "btc_dominance": dom, "fear_greed": greed}


async def forex_pairs(client: httpx.AsyncClient) -> dict:
    """The seven pairs the Forex page lists, plus the dollar index.

    Frankfurter quotes one base per call, so the bases are grouped and fetched
    concurrently rather than one request per pair.
    """
    bases = {}
    for a, b in FX_PAIRS:
        bases.setdefault(a, []).append(b)

    async def one(base, syms):
        r = await client.get(f"https://api.frankfurter.app/latest",
                             params={"from": base, "to": ",".join(syms)}, timeout=10)
        r.raise_for_status()
        return base, (r.json() or {}).get("rates") or {}

    got = await asyncio.gather(*(one(b, s) for b, s in bases.items()),
                               return_exceptions=True)
    out = {}
    for g in got:
        if isinstance(g, Exception):
            continue
        base, rates = g
        for sym, rate in rates.items():
            out[f"{base}/{sym}"] = round(float(rate), 4)
    if not out:
        raise RuntimeError("frankfurter returned no rates")
    return {"pairs": out}


async def mandi(client: httpx.AsyncClient) -> dict:
    """Agmarknet mandi prices for the staples people actually track.

    Needs DATA_GOV_IN_KEY. Raising when it is absent is deliberate: the section
    then reports "not fetched" rather than showing a number nobody can source.
    """
    key = (os.getenv("DATA_GOV_IN_KEY") or "").strip()
    if not key:
        raise RuntimeError("DATA_GOV_IN_KEY not set")
    state = (os.getenv("EXPLORE_STATE") or "Telangana").strip()

    async def one(commodity):
        r = await client.get(f"https://api.data.gov.in/resource/{MANDI_RESOURCE}",
                             params={"api-key": key, "format": "json", "limit": 20,
                                     "filters[commodity]": commodity,
                                     "filters[state]": state}, timeout=10)
        r.raise_for_status()
        recs = (r.json() or {}).get("records") or []
        if not recs:
            return None
        # The cheapest modal price across the state's mandis is the number a
        # reader can act on; the spread is what tells them how firm it is.
        rows = [x for x in recs if x.get("modal_price")]
        if not rows:
            return None
        best = min(rows, key=lambda x: float(x["modal_price"]))
        return {"commodity": commodity,
                "market": best.get("market") or "",
                "modal_price": float(best["modal_price"]),
                "min_price": float(best.get("min_price") or 0),
                "max_price": float(best.get("max_price") or 0),
                "arrival_date": best.get("arrival_date") or ""}

    got = await asyncio.gather(*(one(c) for c in MANDI_COMMODITIES),
                               return_exceptions=True)
    items = [g for g in got if g and not isinstance(g, Exception)]
    if not items:
        raise RuntimeError("agmarknet returned no records")
    return {"state": state, "items": items}


async def launches(client: httpx.AsyncClient) -> dict:
    """Launch Library 2.3.0 — rocket, provider, mission and net for the countdown."""
    r = await client.get("https://ll.thespacedevs.com/2.3.0/launches/upcoming/",
                         params={"limit": 3, "mode": "list"}, timeout=12)
    r.raise_for_status()
    out = []
    for x in (r.json() or {}).get("results") or []:
        out.append({
            "name": x.get("name") or "",
            "rocket": ((x.get("rocket") or {}).get("configuration") or {}).get("name") or "",
            "provider": (x.get("launch_service_provider") or {}).get("name") or "",
            "mission": (x.get("mission") or {}).get("name") or "",
            "pad": ((x.get("pad") or {}).get("name")) or "",
            "net": x.get("net") or "",
            "status": ((x.get("status") or {}).get("abbrev")) or "",
        })
    if not out:
        raise RuntimeError("launch library returned no launches")
    return {"launches": out}


async def ai_papers(client: httpx.AsyncClient) -> dict:
    """Hugging Face daily papers — the AI & Tech card's left half."""
    r = await client.get("https://huggingface.co/api/daily_papers",
                         params={"limit": 8}, timeout=10)
    r.raise_for_status()
    out = []
    for x in (r.json() or [])[:8]:
        paper = x.get("paper") or {}
        out.append({"title": (paper.get("title") or x.get("title") or "").strip(),
                    "upvotes": paper.get("upvotes") or 0,
                    "id": paper.get("id") or "",
                    "url": "https://huggingface.co/papers/" + (paper.get("id") or "")})
    if not out:
        raise RuntimeError("huggingface returned no papers")
    return {"papers": out}


async def github_repos(client: httpx.AsyncClient) -> dict:
    """Repos with real momentum: over a thousand stars AND pushed this month.

    Stars alone surfaces the same permanent top-100 every day; the recency filter
    is what makes it a trending list rather than a hall of fame.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    r = await client.get("https://api.github.com/search/repositories",
                         params={"q": f"stars:>1000 pushed:>{since}",
                                 "sort": "stars", "order": "desc", "per_page": 8},
                         headers={"Accept": "application/vnd.github+json"}, timeout=12)
    r.raise_for_status()
    out = []
    for x in (r.json() or {}).get("items") or []:
        out.append({"name": x.get("full_name") or "",
                    "desc": (x.get("description") or "")[:120],
                    "stars": x.get("stargazers_count") or 0,
                    "lang": x.get("language") or "",
                    "url": x.get("html_url") or ""})
    if not out:
        raise RuntimeError("github returned no repos")
    return {"repos": out}


async def tech_news(client: httpx.AsyncClient) -> dict:
    """Hacker News top stories. The index call returns 500 ids; only the first
    eight are hydrated, because the other 492 are a rate limit waiting to
    happen."""
    r = await client.get("https://hacker-news.firebaseio.com/v0/topstories.json",
                         timeout=10)
    r.raise_for_status()
    ids = (r.json() or [])[:8]

    async def item(i):
        rr = await client.get(f"https://hacker-news.firebaseio.com/v0/item/{i}.json",
                              timeout=8)
        rr.raise_for_status()
        d = rr.json() or {}
        return {"title": d.get("title") or "", "score": d.get("score") or 0,
                "url": d.get("url") or f"https://news.ycombinator.com/item?id={i}"}

    got = await asyncio.gather(*(item(i) for i in ids), return_exceptions=True)
    out = [g for g in got if isinstance(g, dict)]
    if not out:
        raise RuntimeError("hacker news returned no stories")
    return {"stories": out}


FETCHERS = {"markets": markets, "forex": forex, "weather": weather,
            "space": space, "word_of_day": word_of_day, "news_top": news_top,
            "world_bank": world_bank, "nasa": nasa, "flights": flights,
            "macro_rates": macro_rates, "govt_press": govt_press,
            "crypto": crypto, "forex_pairs": forex_pairs, "mandi": mandi,
            "launches": launches, "ai_papers": ai_papers,
            "github_repos": github_repos, "tech_news": tech_news}


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
