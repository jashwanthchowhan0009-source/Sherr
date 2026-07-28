"""
Tests for connection resilience in spie/knowledge/signals (DB-free).

Regression: a full backfill held ONE connection across the whole corpus and hit
"connection has been released back to the pool" mid-run. persist_signals must
recognise a dead connection and continue on a fresh one instead of losing the batch.
"""

import asyncio
import contextlib

import pytest

from app.spie.knowledge.signals import _is_connection_error, persist_signals


def test_detects_connection_errors():
    for msg in ["connection has been released back to the pool",
                "connection is closed",
                "server closed the connection unexpectedly",
                "connection reset by peer"]:
        assert _is_connection_error(Exception(msg)), msg


def test_ignores_ordinary_errors():
    for msg in ["duplicate key value violates unique constraint",
                "invalid input syntax for type uuid"]:
        assert not _is_connection_error(Exception(msg)), msg


class _Sig:
    """Minimal stand-in for models.signal.Signal."""
    def __init__(self):
        self.entity_ids = ["a", "b"]
        self.domain = "news"
        self.ts = None
        self.location = None
        self.magnitude = 1.0
        self.direction = 0
        self.sentiment = None
        self.embedding = None
        self.source_id = "s"
        self.credibility = 0.9
        self.confidence = 0.5
        self.novelty = 0.0
        self.ref_id = None
        self.cluster_id = 1


def test_retries_remaining_signals_on_a_fresh_connection():
    """Run via asyncio.run so the suite needs no pytest-asyncio mode config."""
    class Dead:
        calls = 0
        async def fetchval(self, *a, **k):
            Dead.calls += 1
            raise Exception("connection has been released back to the pool")

    class Good:
        written = 0
        async def fetchval(self, *a, **k):
            Good.written += 1
            return Good.written

    @contextlib.asynccontextmanager
    async def factory():
        yield Good()

    n = asyncio.run(persist_signals(Dead(), [_Sig(), _Sig(), _Sig()], conn_factory=factory))
    assert n == 3            # nothing lost — all retried on the fresh connection
    assert Dead.calls == 1   # failed over on the first failure, didn't keep hammering


def test_reraises_when_no_factory_given():
    class Dead:
        async def fetchval(self, *a, **k):
            raise Exception("connection is closed")

    with pytest.raises(Exception):
        asyncio.run(persist_signals(Dead(), [_Sig()]))


# ─── update_cooccurrence flag (WAN round-trip bottleneck) ────────────────────
def test_update_cooccurrence_flag_controls_inline_writes(monkeypatch):
    """Inline co-occurrence costs round-trips per entity PAIR (~45/article). The
    bulk backfill turns it off and lets cooccurrence_backfill rebuild in one pass."""
    from app.spie.knowledge import signals as signals_mod

    calls = {"n": 0}

    async def _fake_update(*a, **k):
        calls["n"] += 1
        return 0

    async def _fake_resolve(conn, ents):
        return ["a", "b", "c"]          # 3 entities → 3 pairs

    monkeypatch.setattr(signals_mod.cooccurrence, "update_for_signal", _fake_update)
    monkeypatch.setattr(signals_mod, "resolve_many", _fake_resolve)

    class Conn:
        async def fetchval(self, *a, **k):
            return 1

    def _sig():
        s = _Sig()
        s.entity_ids = None             # force resolution path
        s.entities = []
        return s

    calls["n"] = 0
    n = asyncio.run(signals_mod.persist_signals(
        Conn(), [_sig(), _sig(), _sig()], update_cooccurrence=False))
    assert n == 3 and calls["n"] == 0   # nothing written inline

    calls["n"] = 0
    n = asyncio.run(signals_mod.persist_signals(Conn(), [_sig(), _sig(), _sig()]))
    assert n == 3 and calls["n"] == 3   # default stays on for live ingest
