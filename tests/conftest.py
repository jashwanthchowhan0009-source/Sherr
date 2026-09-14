"""Shared test fixtures.

The read cache (cache.py) is a per-PROCESS dict that outlives an individual
test. Now that /explore, /explore/pillars, /explore/snapshot and /patterns cache
their payloads, one test's response could otherwise be served to the next test —
which uses a different temporary database — and the second test would assert
against the first's data. Clearing the local cache around every test keeps them
isolated. (Redis is unconfigured under tests, so the local layer is the whole
cache here.)
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cache  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_read_cache():
    cache._local.clear()
    yield
    cache._local.clear()
