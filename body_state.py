"""
body_state.py — what is actually in an article's body, and what to rewrite it from.

THE PROBLEM THIS NAMES. Three different paths write `articles.full_body`, and two
of them leave something a reader should never see:

    ingest        full_body = the publisher's raw RSS description (main.py:989).
                  Status is pending_rewrite, so it is not served — until it is.
    AI pass       full_body = an original, AI-written body. The good path.
    startup drain full_body = a stub ("Sherr AI is preparing an original...")
                  plus a credit line and a link, status='published',
                  ai_processed=1.

The drain is what released the corpus, and it sets ai_processed=1 — which is the
exact column run_ai_batch filters on (`WHERE ai_processed=0`). So every drained
row became permanently invisible to the pass that was supposed to rewrite it, and
the stub it was given is what a reader opens.

WHAT SURVIVES FOR A REWRITE. The drain overwrites full_body, so the publisher's
text is gone from there — but it never touches summary_60 (clean[:400]) or
source_summary (clean[:200]), both written at ingest. That truncated source text
plus the headline is the material an original summary can be written from, and it
is why the reprocess pass must NOT feed it full_body: on a drained row, full_body
is the stub, and summarizing the stub produces another stub.

`reprocessed` is already the right marker — the AI pass sets it, the drain does
not — so `status='published' AND reprocessed=0` is precisely the drained set.
The body states below are the check on top of that, so a row is judged by what it
CONTAINS rather than only by a flag.

No app imports: loaded by main.py and by scripts, same posture as originality.py.
"""

from __future__ import annotations

import re

from originality import originality_check

# ─── states ──────────────────────────────────────────────────────────────────
EMPTY = "empty"              # nothing at all
STUB = "stub"               # our placeholder — honest, but not a summary
SOURCE_TEXT = "source_text"  # the publisher's own prose (a copyright exposure)
ORIGINAL = "original"       # AI-written, ours
STATES = (ORIGINAL, STUB, SOURCE_TEXT, EMPTY)

# Rewriteable states, in the order a report should read them.
NEEDS_REWRITE = (SOURCE_TEXT, STUB, EMPTY)

# THE PLACEHOLDER TEXTS, TAKEN FROM THEIR SOURCE OF TRUTH.
#
# There are two, written by different code paths, and hardcoding one of them
# here is what made the first version of this module useless in production: it
# knew ai_processor's "Sherr AI is preparing…" and had never heard of the
# drain's "SherrByte has not yet published…" — which is the one the drain
# actually wrote across the corpus. Every one of those rows classified as
# `original`, the audit reported a healthy corpus, and the rewrite pass skipped
# exactly the rows it existed to fix.
#
# So they are imported, not retyped. A third stub added anywhere else still
# needs adding here, but an EDIT to either of these can no longer silently
# desync the classifier from the thing it classifies.
def _stub_sources() -> list:
    out = []
    try:
        from ai_processor import _SAFE_BODY, _SAFE_SUMMARY
        out += [_SAFE_BODY, _SAFE_SUMMARY]
    except Exception:                                       # pragma: no cover
        pass
    try:
        import os as _os
        import sys as _sys
        _scripts = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "scripts")
        if _scripts not in _sys.path:
            _sys.path.insert(0, _scripts)
        from publish_pending import STUB as _DRAIN_STUB
        out.append(_DRAIN_STUB)
    except Exception:                                       # pragma: no cover
        pass
    return [t for t in out if t]


def _markers_from(texts: list) -> tuple:
    """The first clause of each stub, lowercased. Matched as a substring because
    the drain appends a credit line and a URL, so nothing is ever an exact
    match, and because these sentences get reworded more often than this file
    gets revisited."""
    out = []
    for t in texts:
        head = " ".join(t.split())[:60].lower().strip()
        if head:
            out.append(head)
    return tuple(out)


# Literal fallbacks: if an import above ever fails, the classifier must still
# recognise both known stubs rather than quietly passing them as original.
_STUB_MARKERS = _markers_from(_stub_sources()) + (
    "sherr ai is preparing",
    "sherrbyte has not yet published",
    "will appear here shortly",
    "an original sherrbyte summary will replace this note",
)

# The aggregator credit the drain appends. Stripped before measuring, so a stub
# is not mistaken for a body with real content in it.
_CREDIT_RE = re.compile(r"\n+source:\s*.*$", re.I | re.S)

# Below this many words, a body is not a summary of anything regardless of what
# it overlaps with.
_MIN_ORIGINAL_WORDS = 25


def _strip_credit(body: str) -> str:
    return _CREDIT_RE.sub("", body or "").strip()


def is_stub(body: str) -> bool:
    low = (body or "").lower()
    return any(m in low for m in _STUB_MARKERS)


def source_material(headline: str = "", summary_60: str = "",
                    source_summary: str = "", full_body: str = "") -> str:
    """The best surviving publisher text to write an original summary FROM.

    Longest first: summary_60 holds clean[:400] and source_summary clean[:200],
    both untouched by the drain. full_body is only used when it still holds the
    raw ingest text (a row the drain never reached) — never when it is the stub,
    which is what made the existing reprocess pass summarize its own placeholder.
    """
    candidates = []
    if full_body and not is_stub(full_body):
        candidates.append(full_body)
    candidates += [summary_60 or "", source_summary or ""]
    best = max((c.strip() for c in candidates), key=len, default="")
    head = (headline or "").strip()
    if head and head.lower() not in best.lower():
        best = f"{head}\n\n{best}".strip()
    return best


def classify_summary(summary_60: str, source_summary: str = "") -> str:
    """The state of the SUMMARY column, judged on its own.

    summary_60 is what the Home card renders — it is the text a reader meets
    first, and for most articles the only text they ever read. It was never
    checked: classify() judged full_body alone, so a row whose body had been
    rewritten passed as `original` while summary_60 still held
    ai_processor._SAFE_SUMMARY. That is how the audit could report 17,017
    healthy articles while the app showed "Sherr AI is preparing an original
    summary of this story."

    Note the asymmetry with a body: a summary is SUPPOSED to be short, so the
    minimum-word rule that guards full_body would reject every legitimate one.
    Emptiness and stub text are the only failures here.
    """
    text = _strip_credit(summary_60)
    if not text:
        return EMPTY
    if is_stub(summary_60):
        return STUB
    source = (source_summary or "").strip()
    if source:
        ok, _ = originality_check(text, source)
        if not ok:
            return SOURCE_TEXT
    return ORIGINAL


def classify(full_body: str, summary_60: str = "", source_summary: str = "") -> str:
    """Which of STATES this row's body is in.

    SOURCE_TEXT is decided by the same originality gate the pipeline publishes
    behind, not by a string compare: the stored source text is truncated to 400
    characters, so a body that reproduces it is a near-copy of a prefix rather
    than an exact match.
    """
    body = _strip_credit(full_body)
    if not body:
        return EMPTY
    if is_stub(full_body):
        return STUB

    # source_summary FIRST. It always holds the publisher's text (clean[:200] at
    # ingest) and nothing rewrites it. summary_60 started out as publisher text
    # too — which is why it was the reference here — but the rewrite pass now
    # replaces it with OUR summary, so once a row is fixed, using it would
    # compare our body against our own summary and could flag a perfectly
    # consistent article as a reproduction of itself.
    source = (source_summary or "").strip() or (summary_60 or "").strip()
    if source:
        ok, _ = originality_check(body, source)
        if not ok:
            return SOURCE_TEXT

    if len(body.split()) < _MIN_ORIGINAL_WORDS:
        # Too short to be a summary and not overlapping anything we kept — most
        # likely a truncated ingest body whose source text was never stored.
        return EMPTY
    return ORIGINAL


def _get(row, key):
    try:
        return (row[key] if key in row.keys() else "") or ""
    except AttributeError:                # dict / Record
        return (row.get(key) if hasattr(row, "get") else "") or ""
    except (KeyError, IndexError):
        return ""


def classify_row(row) -> str:
    """The row's BODY state (sqlite3.Row / dict / asyncpg Record)."""
    return classify(_get(row, "full_body"), _get(row, "summary_60"),
                    _get(row, "source_summary"))


def classify_row_summary(row) -> str:
    """The row's SUMMARY state."""
    return classify_summary(_get(row, "summary_60"), _get(row, "source_summary"))


def row_is_healthy(row) -> bool:
    """Both columns original. A row is only healthy when nothing a reader can
    reach is a placeholder — and the summary is the part they reach first."""
    return (classify_row(row) == ORIGINAL
            and classify_row_summary(row) == ORIGINAL)


# ─── corpus-level audit ──────────────────────────────────────────────────────
_AUDIT_SQL = ("SELECT id, full_body, summary_60, source_summary, status, "
              "COALESCE(reprocessed,0) AS reprocessed FROM articles")


def audit(conn, *, published_only: bool = False) -> dict:
    """Count every article by body state. Classification needs the originality
    gate, so it is done in Python rather than SQL — 2.8k rows is one pass."""
    q = _AUDIT_SQL
    if published_only:
        q += " WHERE status='published'"
    rows = conn.execute(q).fetchall()

    by_state: dict = {s: 0 for s in STATES}
    published: dict = {s: 0 for s in STATES}
    summary_published: dict = {s: 0 for s in STATES}
    unreprocessed = 0
    healthy = 0
    for r in rows:
        st = classify_row(r)
        sm = classify_row_summary(r)
        by_state[st] += 1
        try:
            is_pub = (r["status"] or "published") == "published"
        except Exception:
            is_pub = True
        if is_pub:
            published[st] += 1
            summary_published[sm] += 1
            if st == ORIGINAL and sm == ORIGINAL:
                healthy += 1
            if (st in NEEDS_REWRITE or sm in NEEDS_REWRITE) and not r["reprocessed"]:
                unreprocessed += 1

    # A row needs work if EITHER column does. Counting only bodies is what let
    # the audit report a healthy corpus while every Home card showed the stub
    # summary — summary_60 is the text a reader meets first.
    needs = sum(1 for r in rows
                if (classify_row(r) in NEEDS_REWRITE
                    or classify_row_summary(r) in NEEDS_REWRITE)
                and (_get(r, "status") or "published") == "published")

    return {
        "total": len(rows),
        "by_state": by_state,
        "published_by_state": published,
        # The column the Home card actually renders.
        "summary_by_state": summary_published,
        "needs_rewrite": needs,
        "needs_rewrite_unflagged": unreprocessed,
        # Both columns original — not just the body.
        "healthy": healthy,
    }


SELECT_NEEDING_REWRITE = (
    # Candidate set, narrowed in Python by classify(): reprocessed=0 is the
    # drain's own fingerprint (it never sets the column the AI pass does), and
    # the body check is what stops an already-rewritten row being redone.
    "SELECT id, headline, source_headline, full_body, summary_60, source_summary, "
    "pillar_id, micro_tags, source_name, url FROM articles "
    "WHERE status='published' AND COALESCE(reprocessed,0)=0 "
    "ORDER BY published_at DESC LIMIT ?"
)
