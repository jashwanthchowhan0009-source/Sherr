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
