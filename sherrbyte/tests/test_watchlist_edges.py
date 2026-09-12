"""test_watchlist_edges.py — the watchlist-edge loader's fallback contract.

migration 029 generalises ril_edges into hand_edges gated by edge_watchlist. The
daily job (backfill.evaluate_history) now loads watchlist edges and falls back to
ril_edges when 029 has not been applied. That fallback is what keeps a pre-029
database firing the RIL proof unchanged, so it is worth pinning without a DB:
load_watchlist_edges must return [] (never raise) when the tables are absent.
"""

from __future__ import annotations

import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))          # sherrbyte/ -> import app.*

from app.spie.proof import data as D                 # noqa: E402


class _RaisingConn:
    async def fetch(self, *a, **k):
        raise RuntimeError('relation "sherrbyte_app.hand_edges" does not exist')


class _RowsConn:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, *a, **k):
        return self._rows


def test_missing_tables_returns_empty_not_raises():
    got = asyncio.run(D.load_watchlist_edges(_RaisingConn()))
    assert got == []


def test_returns_dicts_when_present():
    rows = [{"edge_key": "o2c_brent", "watch_entity": "RIL", "head": "RIL O2C",
             "tail": "Brent crude", "mechanism": "...",
             "signal_keys": ["ril", "brent", "refining_margin"]}]
    got = asyncio.run(D.load_watchlist_edges(_RowsConn(rows)))
    assert got and got[0]["edge_key"] == "o2c_brent"
    assert got[0]["signal_keys"] == ["ril", "brent", "refining_margin"]
