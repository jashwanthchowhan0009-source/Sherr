"""
detectors/emergence.py — newly-emerging entity pairs (Intelligence Engine V1, Step 4).

Fires on entity pairs that co-occur in the current window AND had ZERO occurrences
in the trailing history before it — i.e. a connection that just appeared. Reads only
the materialized cooccurrence table (never brute-forces pairs).

THREE FILTERS keep this from producing the obvious. Emergence is cheap to trigger and
was surfacing pairs like FIFA↔World Cup, which are statistically new only because the
corpus is young — and which make the engine look naive:

  1. expected_pairs — a blocklist of definitionally-linked pairs (migration 018).
  2. NPMI >= MIN_NPMI — the pair must be associated beyond chance, not merely
     co-present. A NULL npmi (too few observations to compute) does not pass.
  3. source_count >= MIN_SOURCES — independent corroboration, so one outlet's
     recurring phrasing cannot mint a "connection".

LANGUAGE: the number of times a pair co-occurs INSIDE OUR OWN CORPUS is an artefact of
what we happened to ingest, not a fact about the world, so it is never the claim. And
the absence claim is bounded by the corpus's real depth — with three weeks of data we
say "newly appearing in current coverage", never "absent for 90 days".
"""

from __future__ import annotations

import json
import logging

from app.spie.discovery.base import write_insight, names_for, domains_for, source_stats

log = logging.getLogger("sherbyte.detectors.emergence")

# Association beyond chance, and independent corroboration. Both required.
MIN_NPMI = 0.5
MIN_SOURCES = 4
# Only the strongest N are surfaced per run. Everything else that passed the filters
# goes to `watchlist` — kept and queryable, just not in the feed. 74 "new connections"
# in one run is a dump, not intelligence: the reader cannot tell which three matter.
MAX_WRITTEN = 12
# A pair already surfaced within this many days is suppressed unless its association
# strength moved materially, so the same connection does not reappear daily.
NOVELTY_WINDOW_DAYS = 5
NOVELTY_NPMI_DELTA = 0.30
# Below this much corpus history, "absent for N days" is not a claim we can make.
MIN_HISTORY_DAYS_FOR_ABSENCE_CLAIM = 30

LAST_RUN: dict = {}


async def _expected_pairs(conn) -> set:
    """The blocklist, as a set of lexically-ordered normalized-key tuples."""
    try:
        rows = await conn.fetch("SELECT norm_a, norm_b FROM expected_pairs")
    except Exception as e:                      # table not migrated yet
        log.warning("expected_pairs unavailable: %s", e)
        return set()
    return {(r["norm_a"], r["norm_b"]) for r in rows}


async def _corpus_history_days(conn) -> int:
    """How many days of news the corpus actually holds. Bounds what we may claim."""
    val = await conn.fetchval(
        "SELECT EXTRACT(DAY FROM now() - MIN(ts))::int "
        "FROM domain_signals WHERE domain = 'news'")
    return int(val or 0)


def history_clause(history_days: int, corpus_days: int) -> str:
    """The strongest absence statement the data actually supports."""
    if corpus_days >= MIN_HISTORY_DAYS_FOR_ABSENCE_CLAIM:
        return (f"with no appearances together in the preceding "
                f"{min(history_days, corpus_days)} days")
    return "newly appearing in current coverage"


def composite_score(npmi: float, source_count: int, novelty: float) -> float:
    """Rank passing candidates: association strength x corroboration x novelty.

    Multiplicative, not additive, so a candidate has to be decent on ALL THREE. An
    additive score lets one strong term carry a pair that is weak everywhere else,
    which is how a run ends up with 74 of them.
    """
    strength = max(0.0, min(1.0, float(npmi or 0.0)))
    breadth = min(int(source_count or 0), 6) / 6.0
    return round(strength * breadth * max(0.0, min(1.0, novelty)), 5)


async def _recent_npmi(conn, a, b, days: int = NOVELTY_WINDOW_DAYS):
    """NPMI recorded the last time this pair was surfaced, if it was."""
    try:
        row = await conn.fetchrow(
            "SELECT (explain_json->>'npmi')::float AS npmi FROM insights "
            "WHERE signature = $1 AND updated_at >= now() - ($2 || ' days')::interval",
            f"emergence:{a}:{b}", str(int(days)))
        return float(row["npmi"]) if row and row["npmi"] is not None else None
    except Exception:
        return None


def novelty_factor(previous_npmi: float | None) -> float:
    """1.0 if the pair is new, 0.0 if it was just surfaced and has not moved.

    Without this the same connection reappears every day it stays above threshold,
    which reads as the engine having nothing new to say.
    """
    return 1.0 if previous_npmi is None else 0.0


async def run(conn, *, current_days: int = 7, history_days: int = 90,
              min_count: int = 3, min_npmi: float = MIN_NPMI,
              min_sources: int = MIN_SOURCES) -> int:
    """Detect and persist emergence insights. Returns how many were written."""
    global LAST_RUN
    blocked = await _expected_pairs(conn)
    corpus_days = await _corpus_history_days(conn)

    # Rank by NPMI (association strength beyond chance) over raw count, so hub
    # entities that co-occur with everything by volume don't dominate.
    candidates = await conn.fetch(
        """
        SELECT entity_a, entity_b, SUM(count) AS c, MAX(npmi) AS npmi
        FROM cooccurrence
        WHERE window_start >= (now() - ($1 || ' days')::interval)::date
        GROUP BY entity_a, entity_b
        HAVING SUM(count) >= $2
        ORDER BY MAX(npmi) DESC NULLS LAST, SUM(count) DESC
        """,
        str(current_days), min_count,
    )

    stats = {"candidates": len(candidates), "rejected_prior_history": 0,
             "rejected_weak_npmi": 0, "rejected_expected_pair": 0,
             "rejected_thin_sources": 0, "written": 0,
             "corpus_history_days": corpus_days, "blocklist_size": len(blocked)}

    passing: list[dict] = []
    written = 0
    for r in candidates:
        a, b = r["entity_a"], r["entity_b"]

        # Must be genuinely new: zero co-occurrence in [history_days ago, current).
        prior = await conn.fetchval(
            """
            SELECT COALESCE(SUM(count), 0) FROM cooccurrence
            WHERE entity_a = $1 AND entity_b = $2
              AND window_start >= (now() - ($3 || ' days')::interval)::date
              AND window_start <  (now() - ($4 || ' days')::interval)::date
            """,
            a, b, str(history_days), str(current_days),
        )
        if prior and int(prior) > 0:
            stats["rejected_prior_history"] += 1
            continue

        # FILTER 2 — association beyond chance. A NULL npmi means there were too few
        # observations to compute one, which is not evidence of association.
        npmi = float(r["npmi"]) if r["npmi"] is not None else None
        if npmi is None or npmi < min_npmi:
            stats["rejected_weak_npmi"] += 1
            continue

        # FILTER 1 — definitionally-linked pairs, matched on normalized keys so the
        # blocklist survives casing and alias differences.
        keys = await conn.fetch(
            "SELECT norm_key FROM entities WHERE id = ANY($1::uuid[])", [a, b])
        norms = sorted(k["norm_key"] for k in keys)
        if len(norms) == 2 and tuple(norms) in blocked:
            stats["rejected_expected_pair"] += 1
            continue

        src_stats = await source_stats(conn, [a, b], current_days)

        # FILTER 3 — independent corroboration.
        source_count = int(src_stats.get("source_count") or 0)
        if source_count < min_sources:
            stats["rejected_thin_sources"] += 1
            continue

        names = await names_for(conn, [a, b])
        domains = await domains_for(conn, [a, b], current_days)
        clause = history_clause(history_days, corpus_days)

        # The count of co-occurrences in our corpus reflects what we ingested, not
        # how connected the pair is — so independent sources and association
        # strength carry the claim instead.
        why = (f"{names[0]} and {names[1]} are appearing together across "
               f"{source_count} independent sources, {clause}. "
               f"Association strength (NPMI) {npmi:.2f} — above chance.")

        # Confidence blends association strength with how many distinct sources
        # corroborate it (a single-source burst is weaker than a multi-source one).
        src = min(source_count, 6) / 6.0
        confidence = round(min(1.0, npmi) * (0.4 + 0.6 * src), 3)
        explain = {"why": why, "method": "emergence", "npmi": round(npmi, 3),
                   "cooccurrence_count": int(r["c"]), "history_claim": clause,
                   "corpus_history_days": corpus_days,
                   **src_stats, "confidence": confidence}

        # Rank first, write later: everything that reaches here has PASSED, so the
        # cap is an editorial decision about how much to surface, not a filter.
        prev = await _recent_npmi(conn, a, b)
        novelty = novelty_factor(prev)
        if prev is not None and abs(npmi - prev) / max(prev, 1e-6) > NOVELTY_NPMI_DELTA:
            novelty = 1.0          # association strength moved materially — say so
        passing.append({
            "a": a, "b": b, "npmi": npmi, "domains": domains, "explain": explain,
            "score": composite_score(npmi, source_count, novelty),
            "novelty": novelty,
        })
        written += 1

    # Top MAX_WRITTEN to the feed; the remainder is kept on the watchlist.
    passing.sort(key=lambda p: p["score"], reverse=True)
    surfaced, parked = passing[:MAX_WRITTEN], passing[MAX_WRITTEN:]

    for p in surfaced:
        p["explain"]["composite_score"] = p["score"]
        await write_insight(
            conn, type="emergence", entity_ids=[p["a"], p["b"]], domains=p["domains"],
            score=round(p["npmi"], 3), explain=p["explain"],
            signature=f"emergence:{p['a']}:{p['b']}")
        written += 1

    if parked:
        try:
            await conn.executemany(
                """
                INSERT INTO watchlist (entity_a, entity_b, kind, score, npmi, detail)
                VALUES ($1, $2, 'emergence', $3, $4, $5::jsonb)
                ON CONFLICT (entity_a, entity_b, kind) DO UPDATE
                    SET score = EXCLUDED.score, npmi = EXCLUDED.npmi,
                        detail = EXCLUDED.detail, seen_at = now()
                """,
                [(p["a"], p["b"], p["score"], p["npmi"],
                  json.dumps({"why": p["explain"].get("why"),
                              "source_count": p["explain"].get("source_count")}))
                 for p in parked])
        except Exception as e:
            log.warning("watchlist write failed: %s", e)

    stats["passing"] = len(passing)
    stats["parked_to_watchlist"] = len(parked)
    stats["suppressed_not_novel"] = sum(1 for p in passing if p["novelty"] == 0.0)
    stats["written"] = written
    LAST_RUN = stats
    log.info("emergence funnel: %s", stats)
    return written
