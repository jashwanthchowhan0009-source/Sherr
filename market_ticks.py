"""
market_ticks.py — the historical daily price store for Sherr-I.

WHY THIS EXISTS. Sherr-I links market anomalies to news events, and "anomaly" is
a claim about history: a 1% day is ordinary for crude and extraordinary for
USD/INR. markets.py holds only the LATEST quote, in memory, reset on every
redeploy — so nothing in the deployed app can currently say what normal looks
like for a symbol. This module keeps one row per symbol per day, in Postgres, and
that series is the baseline every later detector measures against.

  • schema      sherrbyte_app.market_ticks (db/migrations/020_market_ticks.sql)
  • symbols     markets.SYMBOLS — literally what /markets quotes, not a copy
  • sources     Yahoo v8 chart (everything) + CoinGecko market_chart (coins)
  • backfill    scripts/backfill_ticks.py — 90 days, re-runnable
  • daily       append_daily(), wired to the scheduler in main.py

POSTGRES ONLY. The DDL is real Postgres (SERIAL, NUMERIC, TIMESTAMPTZ, an
expression unique index); there is no sqlite path. Every entry point returns a
"skipped: no DATABASE_URL" result instead of raising, so local sqlite development
and the deployed app run the same code and the job is simply inert without a DSN.

IDEMPOTENT BY CONSTRUCTION. Every write is ON CONFLICT on (symbol, UTC day) — the
backfill can be re-run any number of times, and the daily job re-appending a day
it already wrote updates that row rather than adding a second one.

ONE ROW PER TRADING DAY, NOT PER UTC INSTANT. A bar's stored `ts` is its trading
date at 00:00:00+00, derived from the exchange's own gmtoffset. Bucketing raw
epochs by UTC date would file the ASX's 23:00 UTC open under the previous day
while filing the S&P's under the correct one; normalising to the exchange's local
date makes "one row per symbol per day" mean the same thing on every exchange,
and lines Yahoo's bars up with CoinGecko's UTC-midnight daily points.

No pandas, no numpy — httpx and plain SQL.
"""

from __future__ import annotations

import asyncio
import logging
import os
import pathlib
from datetime import datetime, timedelta, timezone

import httpx

import markets

log = logging.getLogger("sherbyte.market_ticks")

# ─── configuration ───────────────────────────────────────────────────────────
DSN = (os.getenv("DATABASE_URL") or os.getenv("SHERR_I_DATABASE_URL") or "").strip()

BACKFILL_DAYS = int(os.getenv("MARKET_TICKS_BACKFILL_DAYS", "90"))
# The daily job re-checks a short trailing window rather than yesterday alone.
# Strictly-yesterday leaves a permanent hole in the series after any missed run,
# and the free tier this deploys to restarts and sleeps; the upsert makes
# re-writing a day already stored free.
DAILY_LOOKBACK_DAYS = int(os.getenv("MARKET_TICKS_DAILY_LOOKBACK", "5"))

# Yahoo tolerates parallel symbol requests; CoinGecko's free tier does not (it
# answers 429 well below ten a second), so the two have separate budgets.
YAHOO_CONCURRENCY = int(os.getenv("MARKET_TICKS_YAHOO_CONCURRENCY", "4"))
COINGECKO_CONCURRENCY = int(os.getenv("MARKET_TICKS_COINGECKO_CONCURRENCY", "1"))
COINGECKO_GAP_S = float(os.getenv("MARKET_TICKS_COINGECKO_GAP_S", "1.5"))

# Yahoo load-balances across these two and will often answer on one while
# rate-limiting the other — the same failover markets.py already relies on.
_YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
_UA = {"User-Agent": "Mozilla/5.0"}

# change_24h is NUMERIC(8,4): ±9999.9999 percent. A value past that is a bad
# upstream print, not a real move — stored as NULL rather than clamped, because a
# fabricated ceiling would read as a genuine 9999% day to every later detector.
_PCT_LIMIT = 9999.9999

_DDL_PATH = (pathlib.Path(__file__).resolve().parent
             / "sherrbyte" / "app" / "db" / "migrations" / "020_market_ticks.sql")

TABLE = "sherrbyte_app.market_ticks"

_UPSERT = f"""
INSERT INTO {TABLE} (symbol, market_type, price, change_24h, ts)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (symbol, ((ts AT TIME ZONE 'UTC')::date)) DO UPDATE
   SET price       = EXCLUDED.price,
       change_24h  = EXCLUDED.change_24h,
       market_type = EXCLUDED.market_type,
       ts          = EXCLUDED.ts
"""


# ─── the symbol catalogue ────────────────────────────────────────────────────
def catalogue(only: str | None = None) -> list[tuple[str, str]]:
    """[(symbol, market_type)] for everything markets.py quotes.

    De-duplicated by symbol: HG=F is listed under both metals and commodities,
    and the unique index is (symbol, day), so it gets ONE series. First claim in
    markets.SYMBOLS wins — deterministic, rather than whichever fetch finished
    last deciding the row's market_type.
    """
    seen: dict[str, str] = {}
    for market_type, syms in markets.SYMBOLS.items():
        if only and market_type != only:
            continue
        for sym in syms:
            seen.setdefault(sym, market_type)
    return sorted(seen.items())


def market_types() -> list[str]:
    return list(markets.SYMBOLS)


# ─── connection ──────────────────────────────────────────────────────────────
def _sanitize_dsn(dsn: str) -> str:
    """Strip query params raw asyncpg rejects, so a pasted Supabase *pooler* URL
    connects cleanly. Same list main.py._sanitize_pg_dsn uses."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
    drop = {"pgbouncer", "prepared_statement_cache_size", "statement_cache_size",
            "prepared_statements", "prepare_threshold", "options"}
    p = urlsplit(dsn)
    if not p.query:
        return dsn
    kept = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in drop]
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(kept), p.fragment))


def configured(dsn: str | None = None) -> bool:
    d = (dsn if dsn is not None else DSN) or ""
    return d.lower().startswith(("postgres://", "postgresql://"))


async def connect(dsn: str | None = None):
    """One asyncpg connection. Callers close it; there is no pool because every
    entry point here is a short batch job, not a request path.

    statement_cache_size=0 for Supabase's transaction pooler — the same setting
    main.py's SPIE pool uses, for the same reason.
    """
    import asyncpg
    return await asyncpg.connect(
        dsn=_sanitize_dsn(dsn or DSN), timeout=30.0, command_timeout=60.0,
        statement_cache_size=0)


async def ensure_schema(conn) -> None:
    """Apply 020_market_ticks.sql. Idempotent, and the migration file is the one
    source of truth — this does not carry its own copy of the DDL to drift from."""
    await conn.execute(_DDL_PATH.read_text(encoding="utf-8"))


# ─── fetchers ────────────────────────────────────────────────────────────────
def _trading_day(epoch: float, gmtoffset: int) -> datetime:
    """A bar's epoch -> its trading date at 00:00:00+00. See the module docstring
    for why the exchange's own offset decides the day rather than raw UTC."""
    local = datetime.fromtimestamp(epoch + (gmtoffset or 0), tz=timezone.utc)
    return datetime(local.year, local.month, local.day, tzinfo=timezone.utc)


def _pct(price: float, prev: float | None) -> float | None:
    """Percent change against the previous close. None when there is no previous
    bar (the first day of any series) or the value cannot be represented."""
    if not prev:
        return None
    pct = (price - prev) / prev * 100.0
    if abs(pct) > _PCT_LIMIT:
        return None
    return round(pct, 4)


def _series(closes: list[tuple[datetime, float]]) -> list[tuple[datetime, float, float | None]]:
    """(ts, price) pairs -> (ts, price, change_24h_pct), chronological."""
    out = []
    prev = None
    for ts, price in closes:
        out.append((ts, round(float(price), 4), _pct(price, prev)))
        prev = price
    return out


async def yahoo_daily(client: httpx.AsyncClient, symbol: str, days: int) -> list:
    """Daily closes for one Yahoo symbol.

    Asks for ?range={days}d&interval=1d first, as specified. Yahoo's documented
    range values are an enum (1d/5d/1mo/3mo/…) and it rejects arbitrary day
    counts on some symbols with "Invalid input - range", so a rejection falls
    back to explicit period1/period2 epochs, which are unambiguous and give
    exactly the window asked for either way.
    """
    now = int(datetime.now(tz=timezone.utc).timestamp())
    attempts = [
        {"range": f"{days}d", "interval": "1d"},
        {"period1": now - days * 86400, "period2": now, "interval": "1d"},
    ]
    last = ""
    for params in attempts:
        for host in _YAHOO_HOSTS:
            try:
                r = await client.get(f"https://{host}/v8/finance/chart/{symbol}",
                                     params=params, headers=_UA, timeout=20)
            except Exception as e:
                last = f"{type(e).__name__}: {e}"
                continue
            if r.status_code != 200:
                last = f"HTTP {r.status_code}"
                continue
            try:
                result = (r.json().get("chart") or {}).get("result") or []
            except Exception as e:
                last = f"bad JSON: {e}"
                continue
            if not result:
                last = "empty result"
                continue
            block = result[0]
            meta = block.get("meta") or {}
            stamps = block.get("timestamp") or []
            quote = ((block.get("indicators") or {}).get("quote") or [{}])[0]
            closes = quote.get("close") or []
            gmt = meta.get("gmtoffset") or 0
            pairs: list[tuple[datetime, float]] = []
            for epoch, close in zip(stamps, closes):
                # Yahoo pads the array with nulls for halted / not-yet-closed
                # bars. Interpolating them would invent a close; they are dropped.
                if close is None or epoch is None:
                    continue
                pairs.append((_trading_day(epoch, gmt), float(close)))
            if pairs:
                # A duplicate trading day (the in-progress bar alongside the
                # settled one) keeps the later value.
                merged: dict[datetime, float] = {}
                for ts, price in pairs:
                    merged[ts] = price
                return _series(sorted(merged.items()))
            last = "no usable closes"
    raise RuntimeError(last or "yahoo returned nothing")


async def coingecko_daily(client: httpx.AsyncClient, coin_id: str, days: int) -> list:
    """Daily closes for one CoinGecko coin.

    `interval` is deliberately not sent: it is a paid-plan parameter now, and the
    public endpoint already returns daily granularity for any days > 90... and
    hourly at exactly 90. Whatever granularity comes back is collapsed to one
    point per UTC day below, so both cases store the same thing.
    """
    r = await client.get(
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
        params={"vs_currency": "usd", "days": days}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    prices = (r.json() or {}).get("prices") or []
    if not prices:
        raise RuntimeError("no prices")
    # Last point of each UTC day = that day's close.
    by_day: dict[datetime, float] = {}
    for ms, price in prices:
        if price is None:
            continue
        d = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        by_day[datetime(d.year, d.month, d.day, tzinfo=timezone.utc)] = float(price)
    if not by_day:
        raise RuntimeError("no usable prices")
    return _series(sorted(by_day.items()))


async def fetch_symbol(client: httpx.AsyncClient, symbol: str, market_type: str,
                       days: int) -> list:
    if market_type == "crypto":
        return await coingecko_daily(client, symbol, days)
    return await yahoo_daily(client, symbol, days)


# ─── write ───────────────────────────────────────────────────────────────────
async def write_ticks(conn, symbol: str, market_type: str, series: list,
                      since: datetime | None = None) -> int:
    """Upsert one symbol's series. Returns rows written."""
    rows = [(symbol, market_type, price, change, ts)
            for ts, price, change in series
            if since is None or ts >= since]
    if not rows:
        return 0
    await conn.executemany(_UPSERT, rows)
    return len(rows)


# ─── orchestration ───────────────────────────────────────────────────────────
async def _run(days: int, *, only: str | None, dry_run: bool, since: datetime | None,
               dsn: str | None, conn=None) -> dict:
    """Shared body of backfill() and append_daily(). Never raises for one symbol —
    a dead upstream costs that symbol, not the run."""
    targets = catalogue(only)
    if only and not targets:
        return {"ok": False, "error": f"unknown market_type: {only}",
                "known": market_types()}
    if not dry_run and not configured(dsn):
        return {"ok": False, "skipped": "no DATABASE_URL — market_ticks is Postgres-only",
                "symbols": len(targets)}

    own_conn = conn is None
    if not dry_run and own_conn:
        conn = await connect(dsn)
    try:
        if not dry_run:
            await ensure_schema(conn)

        y_gate = asyncio.Semaphore(YAHOO_CONCURRENCY)
        c_gate = asyncio.Semaphore(COINGECKO_CONCURRENCY)
        written: dict[str, int] = {}
        failed: dict[str, str] = {}
        lock = asyncio.Lock()

        async def one(client, symbol, market_type):
            gate = c_gate if market_type == "crypto" else y_gate
            async with gate:
                try:
                    series = await fetch_symbol(client, symbol, market_type, days)
                except Exception as e:
                    failed[symbol] = str(e)
                    log.warning("market_ticks %s (%s) failed: %s",
                                symbol, market_type, e)
                    return
                finally:
                    if market_type == "crypto":
                        await asyncio.sleep(COINGECKO_GAP_S)
                if dry_run:
                    written[symbol] = len([s for s in series
                                           if since is None or s[0] >= since])
                    return
                # One writer at a time: a single connection cannot interleave
                # statements, and these batches are small.
                async with lock:
                    written[symbol] = await write_ticks(
                        conn, symbol, market_type, series, since)

        async with httpx.AsyncClient(follow_redirects=True) as client:
            await asyncio.gather(*(one(client, s, m) for s, m in targets))

        return {"ok": True, "dry_run": dry_run, "days": days,
                "symbols": len(targets), "symbols_ok": len(written),
                "rows_written": sum(written.values()),
                "failed": failed}
    finally:
        if not dry_run and own_conn and conn is not None:
            await conn.close()


async def backfill(days: int = BACKFILL_DAYS, *, only: str | None = None,
                   dry_run: bool = False, dsn: str | None = None, conn=None) -> dict:
    """Pull `days` of daily closes for every catalogued symbol. Safe to re-run."""
    return await _run(days, only=only, dry_run=dry_run, since=None, dsn=dsn, conn=conn)


async def append_daily(*, dsn: str | None = None, conn=None) -> dict:
    """The scheduled job: append yesterday's close for every symbol.

    Fetches a short window and upserts anything from the last
    DAILY_LOOKBACK_DAYS, so a run that was missed (restart, sleeping free tier,
    an exchange holiday) is repaired by the next one instead of leaving a
    permanent hole in the series.
    """
    days = max(DAILY_LOOKBACK_DAYS, 2)
    since = (datetime.now(tz=timezone.utc) - timedelta(days=days)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return await _run(days, only=None, dry_run=False, since=since, dsn=dsn, conn=conn)


async def daily_job() -> None:
    """APScheduler entry point. Logs its result and never raises — a job that
    throws is dropped by APScheduler for the rest of the process lifetime."""
    try:
        log.info("market_ticks daily: %s", await append_daily())
    except Exception as e:
        log.error("market_ticks daily job failed: %s", e, exc_info=True)


def register_jobs(scheduler, hour: int = 1, minute: int = 30) -> None:
    """One daily cron job. 01:30 UTC is after the US close (21:00 UTC) so
    yesterday's bar has settled everywhere, and before the engine's 02:00
    detector pass, which is what reads this table."""
    scheduler.add_job(daily_job, "cron", hour=hour, minute=minute,
                      id="market_ticks_daily", replace_existing=True,
                      max_instances=1)
    log.info("market_ticks daily job registered @ %02d:%02d UTC", hour, minute)


# ─── report ──────────────────────────────────────────────────────────────────
_REPORT_SQL = f"""
SELECT market_type,
       COUNT(*)                                  AS rows,
       COUNT(DISTINCT symbol)                    AS symbols,
       MIN((ts AT TIME ZONE 'UTC')::date)        AS earliest,
       MAX((ts AT TIME ZONE 'UTC')::date)        AS latest
  FROM {TABLE}
 GROUP BY market_type
 ORDER BY market_type
"""

_TOTALS_SQL = f"""
SELECT COUNT(*)                           AS rows,
       COUNT(DISTINCT symbol)             AS symbols,
       MIN((ts AT TIME ZONE 'UTC')::date) AS earliest,
       MAX((ts AT TIME ZONE 'UTC')::date) AS latest
  FROM {TABLE}
"""


async def report(conn=None, *, dsn: str | None = None) -> dict:
    """Total rows, distinct symbols, and the date span per market_type."""
    if conn is None and not configured(dsn):
        return {"ok": False, "skipped": "no DATABASE_URL"}
    own = conn is None
    conn = conn or await connect(dsn)
    try:
        await ensure_schema(conn)
        per = [dict(r) for r in await conn.fetch(_REPORT_SQL)]
        totals = dict(await conn.fetchrow(_TOTALS_SQL))
        for row in per + [totals]:
            for k in ("earliest", "latest"):
                row[k] = row[k].isoformat() if row[k] else None
        return {"ok": True, "totals": totals, "by_market_type": per}
    finally:
        if own:
            await conn.close()
