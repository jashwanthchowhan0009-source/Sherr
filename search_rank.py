"""
search_rank.py — hybrid retrieval for /search.

WHAT WAS THERE BEFORE. `/search` was one statement:

    WHERE headline LIKE '%q%' OR summary_60 LIKE '%q%' ORDER BY published_at DESC

A substring match with no index, no relevance ordering and no bound on the
corpus it scans. Three consequences, all of them visible to a reader: "oil
price" matched nothing because the two words are never adjacent, a result that
mentioned the query once in passing outranked the story about it as long as it
was newer, and the scan cost grew with the corpus on a request a reader makes.

WHAT THIS IS. Two kinds of ranking, fused with Reciprocal Rank Fusion:

  relevance — did this row match, and how well
              Postgres: full-text (`websearch_to_tsquery` + `ts_rank_cd`) and
              trigram word-similarity, two legs.
              Otherwise: the LIKE match, one leg.
  priority  — among the rows that matched, which is worth reading first
              recency and `significance`, the score this repo already computes.

RRF is used instead of a weighted sum of the raw scores because the scores are
not comparable: `ts_rank_cd` is unbounded and corpus-dependent, trigram
similarity is 0-1, `significance` is 0-100 and recency is a date. Normalising
them against each other needs a calibration nobody here has measured, and a
min-max over the page (the usual shortcut) makes a row's score depend on which
other rows happened to be on the page. A rank is the one thing every leg
produces on the same scale.

    score(row) = Σ  w_leg / (RRF_K + rank_leg(row))

THREE BOUNDED QUERIES, AND THAT IS THE DESIGN. CLAUDE.md's rule is that a query
whose row count is set by how much news there happens to be needs a LIMIT, and
this one runs on a reader's request:

  1. each relevance leg, `LIMIT leg_limit` (200)   — index-backed on Postgres
  2. the union of their ids, one `WHERE id IN (...)` — at most legs x leg_limit
  3. nothing. The priority rankings are derived in Python from the rows step 2
     already fetched, so recency and significance cost no database work at all.

THE CAPABILITY IS THE INDEX, NOT THE SERVER VERSION. A leg is only offered if
its index was created at boot (`ensure_indexes`). Without the index the same
SQL is a sequential scan with a `to_tsvector` call per row, which is strictly
worse than the LIKE it replaced — so a missing index removes the leg rather
than silently degrading every reader's search. `pg_trgm` in particular is an
extension that a managed Postgres may refuse to create.

PURE, except `ensure_indexes`. Everything else is string building and
arithmetic, so the fusion and the degradation are testable without a database.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

log = logging.getLogger("sherbyte.search")

# ─── capabilities ────────────────────────────────────────────────────────────
CAP_FTS = "fts"
CAP_TRGM = "trgm"

# Set by ensure_indexes() at boot. Read through capabilities() so no caller
# binds the value at import time — module-level configuration is what forces
# importlib.reload, which this repo's test suite has already been burnt by.
_CAPS: frozenset[str] = frozenset()


def capabilities() -> frozenset[str]:
    """Which relevance legs this deployment can actually serve from an index."""
    return _CAPS


def set_capabilities(caps: Iterable[str]) -> frozenset[str]:
    """Override the detected set. For tests and for ensure_indexes."""
    global _CAPS
    _CAPS = frozenset(c for c in caps if c in (CAP_FTS, CAP_TRGM))
    return _CAPS


# ─── tuning ──────────────────────────────────────────────────────────────────
# Read at call time, never at import, so an instance can be retuned from the
# dashboard without a deploy — the same reason STORY_LINK_MAX_ROWS is.
def enabled() -> bool:
    return os.getenv("SEARCH_HYBRID_ENABLED", "1") not in ("0", "false", "False")


def leg_limit() -> int:
    """Rows each relevance leg may return. The bound on the whole request."""
    try:
        return max(10, min(1000, int(os.getenv("SEARCH_LEG_LIMIT", "200"))))
    except ValueError:
        return 200


def rrf_k() -> int:
    """RRF's rank-smoothing constant. 60 is the value the original paper used
    and the one every later comparison is reported against; it is here as a
    knob only so a bad default can be moved without a deploy."""
    try:
        return max(1, int(os.getenv("SEARCH_RRF_K", "60")))
    except ValueError:
        return 60


def page_size() -> int:
    try:
        return max(1, min(100, int(os.getenv("SEARCH_PAGE_SIZE", "25"))))
    except ValueError:
        return 25


# Relevance legs carry full weight; priority legs are a tie-breaker among rows
# that already matched, not a reason to surface a row that did not. A priority
# leg at 1.0 would let a merely-recent weak match outrank the story the reader
# asked for, which is the behaviour this module exists to remove.
WEIGHTS: dict[str, float] = {
    CAP_FTS: 1.0,
    CAP_TRGM: 0.7,      # typo and inflection tolerance, not primary relevance
    "like": 1.0,        # the fallback leg stands in for fts
    "recency": 0.5,
    "significance": 0.5,
}

MAX_QUERY_CHARS = 120

# The trigram leg's floor is the server's `pg_trgm.word_similarity_threshold`
# (0.6 by default), not a constant here. It is deliberately NOT `SET` per
# connection: pgcompat talks through Supabase's pooler, which hands the same
# client connection to different server backends, so a session-level SET is a
# setting that may or may not be in force on the backend that runs the next
# query. A GUC we cannot pin is better left at its documented default than
# relied on.


# ─── query sanitisation ──────────────────────────────────────────────────────
_CTRL = re.compile(r"[\x00-\x1f\x7f]")
_WORD = re.compile(r"[^\W_]+", re.UNICODE)


def clean_query(q: str) -> str:
    """Normalise reader input to something every leg can be handed safely.

    `websearch_to_tsquery` is the one tsquery constructor documented never to
    raise on malformed input, which is why it is used rather than
    `to_tsquery` — so this does not have to parse operators. What it does have
    to do is bound the length: a reader can paste a page into the box, and a
    10KB bind parameter is a 10KB bind parameter on every leg.
    """
    q = _CTRL.sub(" ", q or "")
    q = re.sub(r"\s+", " ", q).strip()
    return q[:MAX_QUERY_CHARS]


def query_words(q: str) -> list[str]:
    """The query's significant words, for a leg that needs them individually."""
    return [w for w in _WORD.findall(clean_query(q).lower()) if len(w) > 1]


def is_usable(q: str) -> bool:
    """A query of one character matches a third of the corpus on every leg."""
    return len(clean_query(q)) >= 2


# ─── metadata pre-filtering, at the database layer ───────────────────────────
@dataclass(frozen=True)
class Filters:
    """What the reader narrowed to, applied inside every leg's WHERE clause.

    PRE-filtering, not post: each leg returns its top `leg_limit` rows from the
    already-filtered set, so narrowing to one pillar returns that pillar's best
    200 matches rather than whatever survives filtering the corpus-wide 200.
    Post-filtering is how a narrowed search comes back empty while matching rows
    exist.
    """
    pillar: int | None = None
    scope: str = ""
    feed_class: str = ""
    min_significance: int | None = None
    written_only: bool = False
    since: str = ""          # ISO-8601 UTC, canonical form (see timestamps.py)
    extra_sql: str = ""
    extra_params: tuple = ()


def prefilter(f: Filters) -> tuple[str, list]:
    """The shared WHERE tail. `?` placeholders; pgcompat translates them."""
    sql = " AND ai_processed=1 AND status='published'"
    params: list = []

    if f.pillar is not None:
        sql += " AND pillar_id=?"
        params.append(int(f.pillar))

    s = (f.scope or "").lower()
    if s == "national":
        sql += " AND scope IN ('national','local')"
    elif s == "local":
        sql += " AND scope=?"
        params.append("local")

    if f.feed_class:
        sql += " AND feed_class=?"
        params.append(f.feed_class)

    if f.min_significance is not None:
        # -1 is "unscored", which is not the same claim as "scored below the
        # bar" — a backfill that has not reached a row must not hide it.
        sql += " AND (significance >= ? OR significance < 0)"
        params.append(int(f.min_significance))

    if f.written_only:
        sql += " AND COALESCE(source_headline,'') <> '' AND headline <> source_headline"

    if f.since:
        # A plain comparison, deliberately, and it is valid on all three shapes
        # this column has: canonical ISO TEXT (byte order == time order, which
        # normalise_published_at guarantees), a real timestamptz (the literal is
        # coerced), and sqlite TEXT. What is NOT valid is a regex — CLAUDE.md
        # records that `published_at ~ '...'` is what took Phase 1 down against
        # a converted column.
        sql += " AND published_at >= ?"
        params.append(f.since)

    if f.extra_sql:
        sql += f.extra_sql
        params.extend(f.extra_params)

    return sql, params


# ─── relevance legs ──────────────────────────────────────────────────────────
# Each returns (sql, params) selecting `id` in descending relevance, bounded.
# The ORDER BY expression is repeated in the SELECT list of none of them: only
# the ranking is used downstream, so the score itself never crosses the wire.

# Defined once and shared by the index, the match and the ranking, so the three
# cannot drift apart.
_FTS_VECTOR = ("to_tsvector('english', COALESCE(headline,'') || ' ' "
               "|| COALESCE(summary_60,''))")
_FTS_MATCH = _FTS_VECTOR + " @@ websearch_to_tsquery('english', ?)"


def leg_fts_sql(q: str, f: Filters, limit: int) -> tuple[str, list]:
    """Postgres full-text. The tsvector expression is written EXACTLY as the
    index in ensure_indexes() builds it — a different COALESCE, a different
    column order or a different configuration name and the planner cannot use
    the index, which turns this leg into the sequential scan it replaced."""
    pre, pre_params = prefilter(f)
    sql = (
        "SELECT id FROM articles"
        " WHERE " + _FTS_MATCH +
        pre +
        " ORDER BY ts_rank_cd(" + _FTS_VECTOR + ", websearch_to_tsquery('english', ?)) DESC,"
        " published_at DESC"
        " LIMIT ?"
    )
    return sql, [q] + pre_params + [q, limit]


def leg_trgm_sql(q: str, f: Filters, limit: int) -> tuple[str, list]:
    """Trigram word-similarity of the query against the headline.

    `?  <% headline` asks "do the query's words appear in the headline", which
    is the question a typo or an inflection breaks for full-text and trigram
    answers anyway. `<%` is index-backed by gin_trgm_ops; plain `similarity()`
    in a WHERE clause is not.
    """
    pre, pre_params = prefilter(f)
    sql = (
        "SELECT id FROM articles"
        " WHERE ? <% COALESCE(headline,'')"
        + pre +
        " ORDER BY word_similarity(?, COALESCE(headline,'')) DESC, published_at DESC"
        " LIMIT ?"
    )
    return sql, [q] + pre_params + [q, limit]


def leg_like_sql(q: str, f: Filters, limit: int) -> tuple[str, list]:
    """The fallback leg: sqlite, and Postgres with neither index.

    It is still a substring match, but it is now bounded and it is one leg of a
    fusion rather than the whole ranking — and it matches each WORD of the query
    separately, so "oil price" finds a headline that contains both and not only
    one that contains the phrase. The words are ANDed; ORing them would make a
    two-word query broader than a one-word one.
    """
    pre, pre_params = prefilter(f)
    words = query_words(q) or [clean_query(q)]
    clauses, params = [], []
    for w in words[:6]:                      # a bound: one LIKE pair per word
        clauses.append("(LOWER(headline) LIKE ? OR LOWER(summary_60) LIKE ?)")
        params.extend([f"%{w.lower()}%"] * 2)
    sql = (
        "SELECT id FROM articles WHERE " + " AND ".join(clauses)
        + pre +
        " ORDER BY published_at DESC LIMIT ?"
    )
    return sql, params + pre_params + [limit]


def relevance_legs(q: str, f: Filters, limit: int,
                   caps: Iterable[str] | None = None) -> list[tuple[str, str, list]]:
    """(leg name, sql, params) for every relevance leg this deployment can run.

    The LIKE leg is NOT added alongside the indexed ones. It is a strictly
    coarser view of the same question, so including it would give a substring
    match a second vote and pull the fusion back towards the behaviour being
    replaced.
    """
    caps = capabilities() if caps is None else frozenset(caps)
    out: list[tuple[str, str, list]] = []
    if CAP_FTS in caps:
        sql, p = leg_fts_sql(q, f, limit)
        out.append((CAP_FTS, sql, p))
    if CAP_TRGM in caps:
        sql, p = leg_trgm_sql(q, f, limit)
        out.append((CAP_TRGM, sql, p))
    if not out:
        sql, p = leg_like_sql(q, f, limit)
        out.append(("like", sql, p))
    return out


# ─── priority legs, derived in Python ────────────────────────────────────────
def priority_rankings(rows: Sequence[Mapping]) -> dict[str, list]:
    """Rank the matched rows by recency and by significance.

    No database work: these are orderings of rows the caller has already
    fetched. They are restricted to the matched set by construction, which is
    the property that matters — a recency ranking over the whole corpus would
    rank today's unrelated story above last week's exact match.
    """
    def _id(r):
        return r["id"]

    by_recent = sorted(rows, key=lambda r: (str(r["published_at"] or ""), _id(r)),
                       reverse=True)
    # An unscored row (-1) sorts last on this leg rather than below every scored
    # row on every leg: it still carries its relevance and recency ranks.
    by_sig = sorted(rows, key=lambda r: (_sig(r), str(r["published_at"] or "")),
                    reverse=True)
    return {"recency": [_id(r) for r in by_recent],
            "significance": [_id(r) for r in by_sig]}


def _sig(row: Mapping) -> int:
    try:
        v = int(row["significance"])
    except (KeyError, TypeError, ValueError):
        return -1
    return v


# ─── Reciprocal Rank Fusion ──────────────────────────────────────────────────
@dataclass
class FusedHit:
    id: int
    score: float
    ranks: dict[str, int] = field(default_factory=dict)   # leg -> 1-based rank


def rrf_fuse(rankings: Mapping[str, Sequence], *,
             weights: Mapping[str, float] | None = None,
             k: int | None = None) -> list[FusedHit]:
    """Fuse ranked id lists into one ordering.

        score(id) = Σ  w_leg / (k + rank_leg(id))

    A row missing from a leg contributes nothing from it rather than a penalty:
    the legs ask different questions and absence from one is not evidence
    against the row. k dampens the top of each list, so one leg's first place
    cannot by itself decide the fusion.

    Ties break on id descending. Ids are monotonic here, so that is "the newer
    story first" — and, more importantly, it is deterministic: a fusion that
    returned a different order for the same inputs would make every cached page
    and every test flaky.
    """
    w = dict(WEIGHTS if weights is None else weights)
    kk = rrf_k() if k is None else k

    scores: dict[object, float] = {}
    ranks: dict[object, dict[str, int]] = {}
    for leg, ids in rankings.items():
        weight = w.get(leg, 1.0)
        if not weight:
            continue
        for i, _id in enumerate(ids):
            rank = i + 1
            prev = ranks.setdefault(_id, {})
            # A leg that lists the same id twice is a bug upstream; keep the
            # better rank rather than crediting the row twice for it.
            if leg in prev:
                continue
            prev[leg] = rank
            scores[_id] = scores.get(_id, 0.0) + weight / (kk + rank)

    ordered = sorted(scores.items(),
                     key=lambda kv: (kv[1], _sortable_id(kv[0])), reverse=True)
    return [FusedHit(id=i, score=s, ranks=ranks[i]) for i, s in ordered]


def _sortable_id(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


# ─── index creation / capability detection ───────────────────────────────────
# Each statement runs on its OWN connection. A failed statement aborts the whole
# transaction on Postgres, so probing them on one shared connection would make
# the first failure look like every capability was missing.
_INDEX_SQL: dict[str, tuple[str, ...]] = {
    CAP_FTS: (
        "CREATE INDEX IF NOT EXISTS idx_articles_fts ON articles USING GIN ("
        + _FTS_VECTOR + ")",
    ),
    CAP_TRGM: (
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        "CREATE INDEX IF NOT EXISTS idx_articles_headline_trgm ON articles "
        "USING GIN (COALESCE(headline,'') gin_trgm_ops)",
    ),
}


def ensure_indexes(conn_factory, *, is_postgres: bool) -> frozenset[str]:
    """Create the search indexes and return the capabilities that now exist.

    Best-effort and idempotent, called from init_db. A capability is granted
    only if every statement it needs succeeded, because a leg without its index
    is slower than no leg at all. Nothing here can fail the boot: a managed
    Postgres that refuses `CREATE EXTENSION` leaves `/search` on the LIKE leg,
    which is what it had before this module existed.

    On sqlite there is no FTS leg to build. Adding one would mean an fts5 shadow
    table kept in sync by triggers, for a backend that only ever holds a test
    fixture.
    """
    if not is_postgres:
        return set_capabilities(())

    caps: list[str] = []
    for cap, statements in _INDEX_SQL.items():
        ok = True
        for stmt in statements:
            conn = None
            try:
                conn = conn_factory()
                conn.execute(stmt)
                try:
                    conn.commit()
                except Exception:
                    pass
            except Exception as e:
                ok = False
                log.warning("[SEARCH] %s unavailable: %s -> %s",
                            cap, stmt.split("(")[0].strip(), e)
                break
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
        if ok:
            caps.append(cap)

    out = set_capabilities(caps)
    log.info("[SEARCH] capabilities: %s", ",".join(sorted(out)) or "none (LIKE only)")
    return out


# ─── diagnosis ───────────────────────────────────────────────────────────────
def diagnose(caps: Iterable[str] | None = None, *, is_postgres: bool = True) -> str:
    """One line for /admin/search. 'No clusters' has four causes with four
    different fixes, and so does 'search feels wrong'."""
    caps = capabilities() if caps is None else frozenset(caps)
    if not enabled():
        return ("SEARCH_HYBRID_ENABLED=0 — serving the single-leg LIKE match. "
                "Set it to 1 to use the fusion.")
    if not is_postgres:
        return ("sqlite backend: LIKE leg only, fused with recency and "
                "significance. Expected locally and in tests.")
    if CAP_FTS in caps and CAP_TRGM in caps:
        return "OK — full-text and trigram legs both indexed."
    if CAP_FTS in caps:
        return ("full-text indexed, trigram missing: CREATE EXTENSION pg_trgm "
                "was refused, so typo tolerance is off. Enable the pg_trgm "
                "extension on the database.")
    if CAP_TRGM in caps:
        return ("trigram indexed, full-text missing: the GIN tsvector index "
                "could not be created, so ranking is trigram-only. Check the "
                "boot log for idx_articles_fts.")
    return ("Postgres with NO search index — running the LIKE fallback. Both "
            "index statements failed at boot; see the [SEARCH] log lines.")
