"""
api/live.py — keyless live-data endpoints (weather + dictionary + word of day).

All providers are free and need no API key:
  • Weather:    Open-Meteo  (https://open-meteo.com)
  • Dictionary: dictionaryapi.dev (English)
  • Word of day: rotates daily from a curated list, defined via dictionaryapi.dev.

Each response degrades gracefully ({} / minimal) on provider failure, and is
cached in-memory with a sensible TTL to stay inside free rate limits.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import httpx
from fastapi import APIRouter, Query

log = logging.getLogger("sherbyte.live")
router = APIRouter(prefix="/live", tags=["live"])

# ─── tiny TTL cache ────────────────────────────────────────────────────────────
_cache: dict = {}


def _cget(key: str):
    e = _cache.get(key)
    return e["data"] if e and time.time() < e["exp"] else None


def _cset(key: str, data, ttl: int):
    _cache[key] = {"data": data, "exp": time.time() + ttl}


# ─── Weather (Open-Meteo) ──────────────────────────────────────────────────────
_WMO = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime fog", 51: "Light drizzle", 53: "Drizzle", 55: "Dense drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain", 71: "Light snow", 73: "Snow",
    75: "Heavy snow", 80: "Rain showers", 81: "Rain showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm + hail", 99: "Thunderstorm + hail",
}
_WMO_EMOJI = {0: "☀️", 1: "🌤️", 2: "⛅", 3: "☁️", 45: "🌫️", 48: "🌫️",
              51: "🌦️", 53: "🌦️", 55: "🌧️", 61: "🌧️", 63: "🌧️", 65: "🌧️",
              71: "🌨️", 73: "🌨️", 75: "❄️", 80: "🌦️", 81: "🌧️", 82: "⛈️",
              95: "⛈️", 96: "⛈️", 99: "⛈️"}


@router.get("/weather")
async def weather(lat: float = Query(9.9312), lon: float = Query(76.2673),
                  city: str = Query("Kochi")):
    """Current conditions for a coordinate (defaults to Kochi, Kerala)."""
    key = f"wx_{round(lat,2)}_{round(lon,2)}"
    cached = _cget(key)
    if cached:
        return cached
    out = {"city": city, "temp_c": None, "feels_c": None, "condition": "",
           "emoji": "🌡️", "humidity": None, "wind_kph": None}
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={"latitude": lat, "longitude": lon,
                        "current": "temperature_2m,apparent_temperature,weather_code,"
                                   "relative_humidity_2m,wind_speed_10m"},
            )
            if r.status_code == 200:
                c = r.json().get("current", {})
                code = int(c.get("weather_code", 0) or 0)
                out.update({
                    "temp_c": round(c.get("temperature_2m", 0) or 0),
                    "feels_c": round(c.get("apparent_temperature", 0) or 0),
                    "condition": _WMO.get(code, "—"),
                    "emoji": _WMO_EMOJI.get(code, "🌡️"),
                    "humidity": c.get("relative_humidity_2m"),
                    "wind_kph": round((c.get("wind_speed_10m", 0) or 0)),
                })
            # Resolve a real city name from the coordinates (keyless reverse geocode).
            # Without this the response just echoed the default "Kochi" for every user.
            try:
                g = await client.get(
                    "https://api.bigdatacloud.net/data/reverse-geocode-client",
                    params={"latitude": lat, "longitude": lon, "localityLanguage": "en"},
                )
                if g.status_code == 200:
                    gj = g.json()
                    name = gj.get("city") or gj.get("locality") or gj.get("principalSubdivision")
                    region = gj.get("principalSubdivision")
                    if name:
                        out["city"] = f"{name}, {region}" if region and region != name else name
            except Exception:
                pass
    except Exception as e:
        log.warning("weather failed: %s", e)
    _cset(key, out, 900)   # 15 min
    return out


# ─── Dictionary (dictionaryapi.dev) ────────────────────────────────────────────
async def _define(word: str) -> dict:
    word = (word or "").strip().lower()
    if not word:
        return {}
    key = f"dict_{word}"
    cached = _cget(key)
    if cached is not None:
        return cached
    out: dict = {"word": word, "phonetic": "", "meanings": []}
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}")
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    entry = data[0]
                    out["phonetic"] = entry.get("phonetic") or next(
                        (p.get("text", "") for p in entry.get("phonetics", []) if p.get("text")), "")
                    for m in entry.get("meanings", [])[:3]:
                        defs = m.get("definitions", [])
                        if defs:
                            out["meanings"].append({
                                "part_of_speech": m.get("partOfSpeech", ""),
                                "definition": defs[0].get("definition", ""),
                                "example": defs[0].get("example", ""),
                            })
    except Exception as e:
        log.warning("dictionary failed (%s): %s", word, e)
    _cset(key, out, 86400)   # 1 day
    return out


@router.get("/dictionary/{word}")
async def dictionary(word: str):
    return await _define(word)


# ─── Word of the Day (rotates daily) ───────────────────────────────────────────
_WOTD = [
    "serendipity", "ephemeral", "petrichor", "luminous", "halcyon", "quintessential",
    "ineffable", "mellifluous", "solitude", "wanderlust", "ethereal", "epiphany",
    "resilience", "eloquent", "nostalgia", "sonder", "aurora", "zenith", "vivid",
    "tenacity", "euphoria", "lucid", "candor", "myriad", "fathom", "gossamer",
    "incandescent", "labyrinth", "panacea", "reverie", "sublime", "verdant",
]


@router.get("/word-of-day")
async def word_of_day():
    idx = date.today().toordinal() % len(_WOTD)
    word = _WOTD[idx]
    d = await _define(word)
    d["date"] = date.today().isoformat()
    return d
