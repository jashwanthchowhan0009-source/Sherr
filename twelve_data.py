"""twelve_data.py — Twelve Data quotes, with key rotation.

Powers the live tickers in the myFeed Dots tab and a crypto fallback for the
Explore ticker tape. Twelve Data's free tier is 8 requests/minute and 800/day,
so callers cache aggressively (the /quote endpoint holds each answer for
`TWELVE_DATA_CACHE_SECONDS`, default 30 min) to stay well inside it.

`TWELVE_DATA_API_KEYS` may hold ONE key or several comma-separated ones; each is
tried in turn on a rate-limit (HTTP 429 or a body-level `code:429`), the way
key_pool rotates the LLM keys — so several free keys multiply the daily budget.
Returns {} when no key is set: a clean skip, never an error.
"""
from __future__ import annotations

import logging
import os

import httpx

log = logging.getLogger("sherbyte.twelvedata")

_BASE = "https://api.twelvedata.com/quote"
# 429 (HTTP or body-level) means this key is spent; try the next one.
ROTATE_CODES = frozenset({429})


def keys() -> list[str]:
    """The configured keys, in order. Accepts the plural list var and the
    singular name, comma-separated in either."""
    raw = (os.getenv("TWELVE_DATA_API_KEYS")
           or os.getenv("TWELVE_DATA_API_KEY") or "")
    return [k.strip() for k in raw.split(",") if k.strip()]


def enabled() -> bool:
    return bool(keys())


def _dedup_upper(symbols) -> list[str]:
    seen: set = set()
    out: list = []
    for s in symbols or []:
        s = str(s or "").strip().upper()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _parse(data, syms: list[str]) -> dict:
    """Twelve Data returns a flat object for ONE symbol (carrying a `symbol`
    key), or `{SYM: {...}}` for several. A per-symbol error object is skipped, so
    one bad symbol never voids the batch."""
    out: dict = {}

    def one(sym: str, d) -> None:
        if not isinstance(d, dict) or d.get("status") == "error":
            return
        price = d.get("close", d.get("price"))
        if price in (None, ""):
            return
        try:
            price = round(float(price), 4)
        except (TypeError, ValueError):
            return
        chg = d.get("percent_change")
        try:
            chg = round(float(chg), 2) if chg not in (None, "") else 0.0
        except (TypeError, ValueError):
            chg = 0.0
        out[sym] = {"price": price, "change_pct": chg,
                    "name": d.get("name") or sym}

    if len(syms) == 1 and isinstance(data, dict) and "symbol" in data:
        one(syms[0], data)
    elif isinstance(data, dict):
        for sym in syms:
            if sym in data:
                one(sym, data[sym])
    return out


def _rate_limited(data) -> bool:
    return isinstance(data, dict) and str(data.get("code")) in {str(c) for c in ROTATE_CODES}


async def get_quotes(symbols) -> dict:
    """{SYMBOL: {price, change_pct, name}} for the symbols Twelve Data covers.

    Empty dict when no keys are configured, every key is rate-limited, or the
    upstream is down — the caller renders those tickers without a live move
    rather than erroring.
    """
    ks = keys()
    syms = _dedup_upper(symbols)
    if not ks or not syms:
        return {}
    param = ",".join(syms)
    async with httpx.AsyncClient() as client:
        for key in ks:
            try:
                r = await client.get(
                    _BASE, params={"symbol": param, "apikey": key}, timeout=12)
            except Exception as e:                                # noqa: BLE001
                log.warning("twelvedata request failed: %s", e)
                continue
            if r.status_code in ROTATE_CODES:
                continue                                          # spent key → next
            if r.status_code != 200:
                log.warning("twelvedata HTTP %d: %s", r.status_code, r.text[:200])
                return {}
            try:
                data = r.json()
            except ValueError:
                return {}
            if _rate_limited(data):
                continue                                          # body-level 429 → next key
            return _parse(data, syms)
    log.info("twelvedata: all %d key(s) rate-limited", len(ks))
    return {}
