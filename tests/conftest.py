"""Shared test fixtures.

CACHE ISOLATION. cache.py's local layer is a module-level dict that lives for the
whole process, so a payload one test caches (a /feed, /explore, /patterns or
dossier response) would otherwise be served to the next test — which seeds a
different database and expects its own rows. Several endpoints are cached now
(and /explore serves stale on error), so this clears the local layer around every
test to keep them independent. It is a no-op for tests that never touch the cache.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(autouse=True)
def _isolate_cache():
    try:
        import cache
    except Exception:                                             # pragma: no cover
        yield
        return
    cache._local.clear()
    yield
    cache._local.clear()
