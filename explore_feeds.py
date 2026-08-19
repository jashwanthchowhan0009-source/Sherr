"""
explore_feeds.py — root-level shim for the Explore data pipeline.

The implementation lives at sherrbyte/app/pipeline/explore_feeds.py, where the spec
put it. This app cannot `from app.pipeline import explore_feeds` because importing
that package runs its __init__, which pulls in asyncpg and the Supabase client —
dependencies the sqlite app has no reason to acquire in order to call six public
HTTP APIs.

Loaded by file path instead, bypassing the package __init__. One implementation, one
schedule, no vendored copy to drift.
"""

from __future__ import annotations

import importlib.util
import pathlib

_SRC = (pathlib.Path(__file__).resolve().parent
        / "sherrbyte" / "app" / "pipeline" / "explore_feeds.py")

_spec = importlib.util.spec_from_file_location("_sherr_explore_feeds", _SRC)
if _spec is None or _spec.loader is None:            # pragma: no cover
    raise ImportError(f"explore feeds module not found at {_SRC}")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

snapshot = _mod.snapshot
refresh = _mod.refresh
refresh_all = _mod.refresh_all
register_jobs = _mod.register_jobs
FETCHERS = _mod.FETCHERS
SCHEDULE = _mod.SCHEDULE

__all__ = ["snapshot", "refresh", "refresh_all", "register_jobs",
           "FETCHERS", "SCHEDULE"]
