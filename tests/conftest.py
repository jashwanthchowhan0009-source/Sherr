"""Shared test fixtures.

MODULE-LEVEL STATE ISOLATION. Three modules keep per-process state that outlives
a test, and each would otherwise carry one test's observations into the next:

  • cache.py's local layer — a module-level dict, so a payload one test caches
    (a /feed, /explore, /patterns or dossier response) would be served to the
    next test, which seeds a different database and expects its own rows.
    Several endpoints are cached now, and /explore serves stale on error.
  • circuit.py's breakers — a test that drives four consecutive failures opens
    one, and the next test's provider call would then be short-circuited before
    it was made.
  • telemetry.py's counters — spans accumulate, so an assertion on a count would
    measure every earlier test too.

Each is cleared BOTH before and after every test: before, so a test is not
affected by one that failed partway through and never reached its teardown.

Every reset is independent and failure-tolerant. A test module that imports
none of these must not be made to, and one module being unimportable must not
stop the others from being reset — which is why this is a loop over the three
rather than one try block around all of them.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# (module name, callable attribute) — the reset for each piece of per-process
# state. A module is imported here only if it is already importable.
_RESETS = (
    ("cache", lambda m: m._local.clear()),
    ("circuit", lambda m: m.reset_all()),
    ("telemetry", lambda m: m.reset()),
)


def _reset_process_state() -> None:
    for name, reset in _RESETS:
        try:
            reset(__import__(name))
        except Exception:                                         # pragma: no cover
            pass


@pytest.fixture(autouse=True)
def _isolate_process_state():
    _reset_process_state()
    yield
    _reset_process_state()
