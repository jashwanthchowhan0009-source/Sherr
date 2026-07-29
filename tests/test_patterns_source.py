"""
Guards the /patterns provenance contract (Part A).

The SPIE tab was showing sqlite demo rows that looked exactly like real insights.
Every response must now declare where the data came from, and a configured-but-
broken engine must NEVER be papered over with demo rows.
"""

import re
import pathlib

SRC = pathlib.Path(__file__).resolve().parent.parent / "main.py"
CODE = SRC.read_text()


def test_engine_response_is_labelled_engine():
    assert 'data["source"] = "engine"' in CODE


def test_engine_failure_returns_unavailable_not_seed():
    """A configured engine that errors must return empty + 'unavailable', so the
    app can say so instead of showing fake patterns."""
    # the failure path RETURNS immediately — it must not fall through to the seed
    assert 'return {"patterns": [], "total": 0, "source": "unavailable"' in CODE


def test_sqlite_rows_are_labelled_seed():
    assert '"source": "seed"' in CODE


def test_no_sprie_naming_remains():
    """Rename completed: SPRIE -> SPIE everywhere."""
    for f in ["main.py", "index.html"]:
        text = (SRC.parent / f).read_text()
        assert not re.search(r"SPRIE|sprie", text), f"SPRIE naming still in {f}"
