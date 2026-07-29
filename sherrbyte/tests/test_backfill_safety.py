"""
Guards against the data-loss failure mode seen on the free-tier pooler:
signals_backfill used to TRUNCATE derived tables (including `insights`) on EVERY
run, so a crash mid-run destroyed the insights the app was showing.

These are static guarantees — no DB required.
"""

import inspect

from app.workers import signals_backfill as sb


def test_insights_is_never_in_the_backfill_reset_set():
    """insights is detector output / the user-visible product — a signals backfill
    must never truncate it."""
    assert "insights" not in sb._DERIVED_TABLES


def test_reset_is_opt_in():
    """Default must be no-reset so a crashed run can't wipe the graph."""
    sig = inspect.signature(sb.run)
    assert sig.parameters["reset"].default is False


def test_backfill_query_is_resumable():
    """Without a reset, a re-run must skip already-backfilled info_objects rather
    than duplicating their signals."""
    src = inspect.getsource(sb.run)
    assert "NOT EXISTS" in src and "domain_signals" in src


def test_standalone_reset_command_is_dry_run_by_default():
    from app.workers import reset_spie
    sig = inspect.signature(reset_spie.run)
    assert sig.parameters["confirm"].default is False
    assert sig.parameters["with_insights"].default is False
