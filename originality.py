"""
originality.py — root-level shim for the originality gate (P0.4).

The canonical implementation lives at sherrbyte/app/pipeline/originality.py, where
the spec put it. main.py cannot `from app.pipeline.originality import ...` because
importing that package runs app/pipeline/__init__.py, which pulls in asyncpg and the
Supabase client — dependencies the sqlite app has no business acquiring just to
compare two strings.

So the module is loaded by file path, bypassing package __init__. One implementation,
one set of thresholds, no vendored copy to drift out of sync.
"""

from __future__ import annotations

import importlib.util
import pathlib

_SRC = (pathlib.Path(__file__).resolve().parent
        / "sherrbyte" / "app" / "pipeline" / "originality.py")

_spec = importlib.util.spec_from_file_location("_sherr_originality", _SRC)
if _spec is None or _spec.loader is None:            # pragma: no cover
    raise ImportError(f"originality gate not found at {_SRC}")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

originality_check = _mod.originality_check
quoted_spans = _mod.quoted_spans
tokenize = _mod.tokenize
normalize = _mod.normalize
MAX_NGRAM_OVERLAP = _mod.MAX_NGRAM_OVERLAP
MAX_CONTIGUOUS_RUN = _mod.MAX_CONTIGUOUS_RUN
MAX_QUOTE_TOKENS = _mod.MAX_QUOTE_TOKENS

__all__ = ["originality_check", "quoted_spans", "tokenize", "normalize",
           "MAX_NGRAM_OVERLAP", "MAX_CONTIGUOUS_RUN", "MAX_QUOTE_TOKENS"]
