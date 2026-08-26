"""
The historical price store.

Sherr-I calls a market move unusual by comparing it against that symbol's own
history, so the value of this table is entirely in the series being clean: one
row per symbol per trading day, no duplicates, no invented closes, and the same
day meaning the same thing on the ASX as on the S&P. These tests pin that.

The Postgres half is skipped unless MARKET_TICKS_TEST_DSN names a database —
they need real Postgres, not a stub, because the whole guarantee lives in an
expression unique index.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import market_ticks as mt  # noqa: E402

TEST_DSN = os.getenv("MARKET_TICKS_TEST_DSN", "")
needs_pg = pytest.mark.skipif(not TEST_DSN, reason="MARKET_TICKS_TEST_DSN not set")

DAY = 86400
# 2026-08-24 00:00 UTC — a Monday, so the bars below are consecutive weekdays.
BASE = int(datetime(2026, 8, 24, tzinfo=timezone.utc).timestamp())


class _Resp:
    def __init__(self, code, payload=None):
        self.status_code = code
        self._p = payload if payload is not None else {}

    def json(self):
        return self._p


class _Client:
    """Answers with a queued response per request, recording the params it saw."""

    def __init__(self, *responses):
        self._queue = list(responses)
        self.calls = []

    async def get(self, url, **kw):
        self.calls.append((url, kw.get("params") or {}))
        return self._queue.pop(0) if self._queue else _Resp(404)


def _chart(stamps, closes, gmtoffset=0):
    return {"chart": {"result": [{
        "meta": {"gmtoffset": gmtoffset, "currency": "USD"},
        "timestamp": list(stamps),
        "indicators": {"quote": [{"close": list(closes)}]},
    }]}}


# ─── bucketing: what counts as "a day" ───────────────────────────────────────
def test_a_bar_is_filed_under_the_exchanges_trading_date_not_the_utc_instant():
    """The ASX opens at 23:00 UTC the day BEFORE its own trading date. Bucketing
    raw epochs by UTC date would file its close under the previous day while
    filing New York's under the right one, and the two series would then be
    silently misaligned by a day — which is exactly the kind of skew a lag
    correlation would read as signal."""
    # 2026-08-24 10:00 AEST == 2026-08-23 23:00 UTC, offset +39600s.
    epoch = int(datetime(2026, 8, 23, 23, 0, tzinfo=timezone.utc).timestamp())
    assert mt._trading_day(epoch, 39600).date().isoformat() == "2026-08-24"
    # New York, same trading date, no ambiguity.
    ny = int(datetime(2026, 8, 24, 13, 30, tzinfo=timezone.utc).timestamp())
    assert mt._trading_day(ny, -14400).date().isoformat() == "2026-08-24"


def test_a_stored_timestamp_is_always_midnight_utc():
    """The unique index buckets by UTC date, so normalising every bar to 00:00:00
    keeps 'the row for this day' addressable rather than depending on what time
    the bar happened to open."""
    ts = mt._trading_day(BASE + 12345, 19800)
    assert (ts.hour, ts.minute, ts.second, ts.microsecond) == (0, 0, 0, 0)
    assert ts.tzinfo is timezone.utc


# ─── change_24h ──────────────────────────────────────────────────────────────
def test_the_first_bar_of_a_series_has_no_change_rather_than_a_zero():
    """There is no previous close to compare it against. Zero would claim the
    symbol was flat that day, which is a different statement from 'unknown'."""
    series = mt._series([(mt._trading_day(BASE, 0), 100.0),
                         (mt._trading_day(BASE + DAY, 0), 110.0)])
    assert series[0][2] is None
    assert series[1][2] == pytest.approx(10.0)


def test_a_percent_too_large_for_the_column_is_stored_as_unknown_not_clamped():
    """change_24h is NUMERIC(8,4). A clamp would hand every later detector a
    genuine-looking 9999% day; NULL says the upstream print was unusable."""
    assert mt._pct(1e9, 0.0001) is None
    assert mt._pct(110.0, 100.0) == pytest.approx(10.0)
    assert mt._pct(100.0, None) is None


# ─── Yahoo parsing ───────────────────────────────────────────────────────────
def test_null_closes_are_dropped_rather_than_interpolated():
    """Yahoo pads its arrays with nulls for halted and not-yet-closed bars.
    Filling them in would write a close that never printed."""
    client = _Client(_Resp(200, _chart(
        [BASE, BASE + DAY, BASE + 2 * DAY], [100.0, None, 120.0])))
    series = asyncio.run(mt.yahoo_daily(client, "^NSEI", 90))
    assert [round(p, 2) for _, p, _ in series] == [100.0, 120.0]


def test_the_range_form_is_tried_first_and_period_epochs_are_the_fallback():
    """?range=90d is what the spec asks for, but Yahoo's documented range values
    are an enum and it rejects arbitrary day counts on some symbols. Falling
    through to explicit epochs means one rejected symbol is not a missing series."""
    client = _Client(_Resp(400), _Resp(400),           # range=90d, both hosts
                     _Resp(200, _chart([BASE], [100.0])))
    series = asyncio.run(mt.yahoo_daily(client, "^NSEI", 90))
    assert len(series) == 1
    assert client.calls[0][1]["range"] == "90d"
    assert "period1" in client.calls[2][1] and "period2" in client.calls[2][1]


def test_both_yahoo_hosts_are_tried_before_a_symbol_is_given_up_on():
    """Yahoo rate-limits one host while answering on the other, and a Render
    instance is exactly the datacenter range it limits."""
    client = _Client(_Resp(429), _Resp(200, _chart([BASE], [100.0])))
    assert len(asyncio.run(mt.yahoo_daily(client, "^NSEI", 90))) == 1
    assert [c[0].split("//")[1].split("/")[0] for c in client.calls] == [
        "query1.finance.yahoo.com", "query2.finance.yahoo.com"]


def test_a_symbol_with_no_usable_bars_raises_so_the_run_reports_it_as_failed():
    """Silently returning an empty series would count the symbol as backfilled."""
    client = _Client(_Resp(200, _chart([BASE], [None])), _Resp(200, _chart([], [])),
                     _Resp(500), _Resp(500))
    with pytest.raises(RuntimeError):
        asyncio.run(mt.yahoo_daily(client, "^NSEI", 90))


# ─── CoinGecko parsing ───────────────────────────────────────────────────────
def test_intraday_coingecko_points_collapse_to_one_close_per_day():
    """market_chart returns hourly granularity at days=90 on the free tier. The
    last point of each UTC day is that day's close; keeping them all would break
    one-row-per-day before the database ever saw it."""
    ms = int(datetime(2026, 8, 24, tzinfo=timezone.utc).timestamp() * 1000)
    client = _Client(_Resp(200, {"prices": [
        [ms, 100.0], [ms + 3600_000, 105.0], [ms + 7200_000, 110.0],
        [ms + DAY * 1000, 120.0]]}))
    series = asyncio.run(mt.coingecko_daily(client, "bitcoin", 90))
    assert [p for _, p, _ in series] == [110.0, 120.0]      # last point per day
    assert series[1][2] == pytest.approx(9.0909, abs=1e-3)


def test_the_paid_interval_parameter_is_not_sent():
    """`interval=daily` is an Enterprise-plan parameter; sending it makes the
    public endpoint reject the call."""
    ms = int(datetime(2026, 8, 24, tzinfo=timezone.utc).timestamp() * 1000)
    client = _Client(_Resp(200, {"prices": [[ms, 100.0]]}))
    asyncio.run(mt.coingecko_daily(client, "bitcoin", 90))
    assert "interval" not in client.calls[0][1]


# ─── the catalogue ───────────────────────────────────────────────────────────
def test_every_symbol_markets_quotes_is_in_the_store_catalogue():
    """The store exists to give /markets' symbols a history. If the two lists can
    drift, a symbol gets added to the app and silently never accumulates one."""
    catalogued = {s for s, _ in mt.catalogue()}
    for market_type, syms in __import__("markets").SYMBOLS.items():
        assert set(syms) <= catalogued, f"{market_type} not fully covered"


def test_a_symbol_listed_under_two_market_types_gets_one_deterministic_series():
    """HG=F is both a metal and a commodity in markets.py. The unique index is on
    (symbol, day), so it gets one row per day either way — this pins WHICH
    market_type that row carries, instead of whichever fetch finished last."""
    pairs = mt.catalogue()
    assert len({s for s, _ in pairs}) == len(pairs)
    assert dict(pairs)["HG=F"] == "metals"


# ─── Postgres round trip ─────────────────────────────────────────────────────
async def _fresh_conn():
    conn = await mt.connect(TEST_DSN)
    await mt.ensure_schema(conn)
    await conn.execute(f"DELETE FROM {mt.TABLE} WHERE symbol LIKE 'TEST.%'")
    return conn


def _fixture(n=3, start=100.0):
    day0 = datetime(2026, 8, 24, tzinfo=timezone.utc)
    return mt._series([(day0 + timedelta(days=i), start + i) for i in range(n)])


@needs_pg
def test_the_migration_is_safe_to_apply_repeatedly():
    """run_migrations() re-applies every file on every boot."""
    async def go():
        conn = await mt.connect(TEST_DSN)
        for _ in range(3):
            await mt.ensure_schema(conn)
        await conn.close()
    asyncio.run(go())


@needs_pg
def test_rerunning_the_backfill_refreshes_a_day_instead_of_duplicating_it():
    """The whole idempotency claim. A second run over the same window must leave
    the row count untouched and carry the newer close."""
    async def go():
        conn = await _fresh_conn()
        try:
            await mt.write_ticks(conn, "TEST.A", "stocks", _fixture(3, 100.0))
            first = await conn.fetchval(
                f"SELECT COUNT(*) FROM {mt.TABLE} WHERE symbol='TEST.A'")
            await mt.write_ticks(conn, "TEST.A", "stocks", _fixture(3, 200.0))
            second = await conn.fetchval(
                f"SELECT COUNT(*) FROM {mt.TABLE} WHERE symbol='TEST.A'")
            price = await conn.fetchval(
                f"SELECT price FROM {mt.TABLE} "
                f"WHERE symbol='TEST.A' ORDER BY ts LIMIT 1")
            return first, second, float(price)
        finally:
            await conn.close()

    first, second, price = asyncio.run(go())
    assert first == 3
    assert second == 3, "a re-run duplicated the days it already had"
    assert price == 200.0, "a re-run did not pick up the revised close"


@needs_pg
def test_two_writes_on_the_same_day_at_different_times_stay_one_row():
    """The daily job writes near midnight UTC and the backfill writes the bar's
    own 00:00 stamp. Both are the same trading day and must not both persist."""
    async def go():
        conn = await _fresh_conn()
        try:
            day = datetime(2026, 8, 24, tzinfo=timezone.utc)
            await mt.write_ticks(conn, "TEST.B", "forex", [(day, 83.0, None)])
            await mt.write_ticks(conn, "TEST.B", "forex",
                                 [(day.replace(hour=23, minute=59), 84.0, 1.2)])
            rows = await conn.fetch(
                f"SELECT price, change_24h FROM {mt.TABLE} WHERE symbol='TEST.B'")
            return rows
        finally:
            await conn.close()

    rows = asyncio.run(go())
    assert len(rows) == 1
    assert float(rows[0]["price"]) == 84.0
    assert float(rows[0]["change_24h"]) == pytest.approx(1.2)


@needs_pg
def test_the_daily_append_only_writes_the_trailing_window():
    """append_daily() fetches a short window and must not rewrite the whole
    90-day history every night."""
    async def go():
        conn = await _fresh_conn()
        try:
            old = datetime(2026, 1, 1, tzinfo=timezone.utc)
            recent = datetime.now(tz=timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0)
            series = [(old, 10.0, None), (recent, 20.0, 1.0)]
            since = recent - timedelta(days=2)
            written = await mt.write_ticks(conn, "TEST.C", "rates", series, since)
            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM {mt.TABLE} WHERE symbol='TEST.C'")
            return written, total
        finally:
            await conn.close()

    written, total = asyncio.run(go())
    assert written == 1, "the append wrote outside its trailing window"
    assert total == 1


@needs_pg
def test_the_report_counts_rows_symbols_and_the_span_per_market_type():
    async def go():
        conn = await _fresh_conn()
        try:
            await mt.write_ticks(conn, "TEST.A", "stocks", _fixture(3))
            await mt.write_ticks(conn, "TEST.D", "stocks", _fixture(2))
            await mt.write_ticks(conn, "TEST.E", "crypto", _fixture(4))
            return await mt.report(conn)
        finally:
            await conn.execute(f"DELETE FROM {mt.TABLE} WHERE symbol LIKE 'TEST.%'")
            await conn.close()

    rep = asyncio.run(go())
    by_type = {r["market_type"]: r for r in rep["by_market_type"]}
    assert by_type["stocks"]["rows"] == 5
    assert by_type["stocks"]["symbols"] == 2
    assert by_type["crypto"]["earliest"] == "2026-08-24"
    assert by_type["crypto"]["latest"] == "2026-08-27"


# ─── degradation ─────────────────────────────────────────────────────────────
def test_without_a_postgres_dsn_the_job_reports_skipped_rather_than_raising():
    """main.py registers this job unconditionally; local sqlite development has
    no DSN. A raising job is dropped by APScheduler for the process lifetime."""
    result = asyncio.run(mt.backfill(5, dsn=""))
    assert result["ok"] is False
    assert "skipped" in result


def test_a_dead_upstream_costs_that_symbol_and_not_the_run():
    """58 symbols across two providers; one being rate limited must still leave
    the other 57 written."""
    async def go():
        async def boom(client, symbol, market_type, days):
            if symbol == "^NSEI":
                raise RuntimeError("HTTP 429")
            day = datetime(2026, 8, 24, tzinfo=timezone.utc)
            return [(day, 100.0, None)]

        original = mt.fetch_symbol
        mt.fetch_symbol = boom
        try:
            return await mt.backfill(5, only="stocks", dry_run=True)
        finally:
            mt.fetch_symbol = original

    result = asyncio.run(go())
    assert result["ok"] is True
    assert result["failed"] == {"^NSEI": "HTTP 429"}
    assert result["symbols_ok"] == result["symbols"] - 1
