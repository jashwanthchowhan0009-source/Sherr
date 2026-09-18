"""The analog engine (SHAE) is actually RUN, not just built.

matcher/reaction/cards/event_library and the /api/sherr-i/analogs endpoint all
exist and are tested — but for a long stretch nothing ever called Phase 1
(event_library.build -> hist_events) or Phase 3 (reaction.compute ->
analog_reactions), so both tables stayed empty and the analog surface was
permanently dark. Both detector runners now build the library and the reactions,
so this asserts the wiring is present in each and can never silently regress.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
DETECTORS = os.path.join(_ROOT, "sherrbyte", "app", "workers", "detectors.py")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def test_in_process_runner_builds_library_and_reactions():
    m = _read(MAIN)
    start = m.index("async def _run_detectors(")
    block = m[start:m.index("\n@app.", start)]
    assert "from app.spie.analog import event_library, reaction" in block
    assert "await event_library.build(conn)" in block
    assert "await reaction.compute(conn)" in block
    # Guarded so `only=` still selects a single job.
    assert 'only == "analog"' in block


def test_cron_runner_builds_library_and_reactions():
    d = _read(DETECTORS)
    assert "from app.spie.analog import event_library, reaction" in d
    assert "await event_library.build(conn)" in d
    assert "await reaction.compute(conn)" in d
    assert '_ANALOG = "analog"' in d
    # analog is a selectable --only target, and its funnel is surfaced.
    assert "_ANALOG" in d and "_LAST_ANALOG" in d


def test_only_analog_is_a_fast_path():
    # A targeted analog populate must not pay for the NPMI refresh or the
    # discovery detectors — it exists so the surface can be lit and verified.
    m = _read(MAIN)
    start = m.index("async def _run_detectors(")
    block = m[start:m.index("\n@app.", start)]
    assert 'if only != "analog":' in block
    # And the admin endpoint documents it.
    assert "only=analog" in m


def test_analog_modules_import():
    import importlib
    import sys
    sb = os.path.join(_ROOT, "sherrbyte")
    if sb not in sys.path:
        sys.path.insert(0, sb)
    el = importlib.import_module("app.spie.analog.event_library")
    rx = importlib.import_module("app.spie.analog.reaction")
    assert hasattr(el, "build")
    assert hasattr(rx, "compute")
