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

import logging

from app.spie.discovery.base import write_insight, names_for, domains_for, source_stats

log = logging.getLogger("sherbyte.detectors.emergence")

# Association beyond chance, and independent corroboration. Both required.
MIN_NPMI = 0.5
MIN_SOURCES = 4
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

        await write_insight(
            conn, type="emergence", entity_ids=[a, b], domains=domains,
            # Rank by association strength, not by raw corpus count.
            score=round(npmi, 3), explain=explain, signature=f"emergence:{a}:{b}",
        )
        written += 1

    stats["written"] = written
    LAST_RUN = stats
    log.info("emergence funnel: %s", stats)
    return written
