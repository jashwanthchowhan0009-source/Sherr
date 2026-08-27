"""
Replaying market_ticks into domain_signals.

market_signals writes ONE day per run, and market_reaction needs six before it
will test an instrument at all — so a fresh database is a week away from its
first insight. --from-ticks replays the stored daily closes to close that gap.

The whole value depends on the replayed signal being INDISTINGUISHABLE from the
one the daily run writes. A different entity name, source_id or ref_id and the
history splits in two: the detector sees two shallow series instead of one deep
one, and the delete-then-insert stops being idempotent across the two paths.
That is what most of these tests are about.

Skipped unless the engine's own dependencies are installed — app.workers pulls
app.config (pydantic-settings) and app.db, which the ROOT service does not ship.
"""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "sherrbyte"))

ms = pytest.importorskip("app.workers.market_signals",
                         reason="engine deps (pydantic-settings, pgvector) not installed")


def test_every_engine_instrument_can_be_replayed_from_the_price_store():
    """The map is keyed by the symbol market_ticks STORES — Yahoo symbols for
    instruments, CoinGecko ids for coins. A key that does not match the store is
    an instrument that silently never gets history."""
    import market_ticks as mt
    stored = {s for s, _ in mt.catalogue()}
    for sym, _, _ in ms.INSTRUMENTS:
        assert sym in ms._TICK_SOURCES
        assert sym in stored, f"{sym} is not a symbol market_ticks collects"
    for cid in ms.CRYPTO:
        assert cid in ms._TICK_SOURCES
        assert cid in stored, f"{cid} is not a symbol market_ticks collects"


def test_only_instruments_the_engine_has_names_for_are_replayed():
    """market_ticks carries 57 symbols; the engine has entity names for 13.
    Inventing names for the rest would create entities the daily path never
    produces, and the two would resolve differently."""
    import market_ticks as mt
    assert len(ms._TICK_SOURCES) == len(ms.INSTRUMENTS) + len(ms.CRYPTO)
    assert len(ms._TICK_SOURCES) < len(mt.catalogue())
    assert "XOM" not in ms._TICK_SOURCES and "^FTSE" not in ms._TICK_SOURCES


def test_the_replayed_identity_matches_what_the_daily_run_emits():
    """collect() sets symbol=cid.upper() and name=CRYPTO[cid] for coins, and the
    ref_id is built from that symbol. The replay has to agree exactly, or the two
    paths write two different rows for the same instrument-day."""
    for sym, name, cls in ms.INSTRUMENTS:
        assert ms._TICK_SOURCES[sym] == (sym, name, cls)
    for cid, name in ms.CRYPTO.items():
        assert ms._TICK_SOURCES[cid] == (cid.upper(), name, "crypto")


def test_the_query_skips_rows_with_no_known_change():
    """change_24h IS NULL is the first bar of a series or an unrepresentable
    print. Replaying it as 0.0 would tell the detector the day was flat, which is
    a different claim from 'unknown' — and a flat day feeds the baseline."""
    assert "change_24h IS NULL" not in ms._TICKS_SQL
    assert "change_24h IS NOT NULL" in ms._TICKS_SQL


def test_the_replay_reads_the_price_store_by_its_qualified_name():
    """The engine's pool connects with no search_path of its own, so an
    unqualified market_ticks would not resolve."""
    assert "sherrbyte_app.market_ticks" in ms._TICKS_SQL


def test_the_replay_deletes_before_inserting_like_the_daily_run_does():
    """persist_signal is a plain INSERT with no upsert, so idempotency comes
    entirely from clearing the ref_ids first. Both paths must do it."""
    import inspect
    src = inspect.getsource(ms.backfill_from_ticks)
    assert "DELETE FROM domain_signals" in src
    assert "ref_id = ANY($1::text[])" in src
    assert src.count("persist_signals") >= 1


def test_the_six_day_gate_is_reported_not_left_to_the_caller_to_work_out():
    """The entire point of the flag is clearing market_reaction's min_history+1,
    so the result says whether it did."""
    import inspect
    src = inspect.getsource(ms.backfill_from_ticks)
    assert "enough_for_market_reaction" in src
    assert "deepest_instrument_days" in src


def test_a_missing_price_store_is_reported_with_the_fix_not_as_a_crash():
    """The common first-run state: the flag is used before the ticks backfill.

    Injected through conn_factory rather than by patching the engine's pool —
    the same seam /admin/replay-signals uses to pass the pool main.py holds."""
    import asyncio

    class _Conn:
        async def fetch(self, *a):
            raise RuntimeError('relation "sherrbyte_app.market_ticks" does not exist')

    class _Acquire:
        async def __aenter__(self): return _Conn()
        async def __aexit__(self, *a): return False

    out = asyncio.run(ms.backfill_from_ticks(days=90, conn_factory=lambda: _Acquire()))
    assert out["written"] == 0
    assert "backfill_ticks" in out["detail"]
