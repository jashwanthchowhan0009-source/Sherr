"""analog/matcher.py — the k most comparable past events.

Given a live event (its entities, class and linked symbols), rank the event
library and return the analogs worth measuring. Pure ranking: it decides
similarity, never significance, and never touches a price.

THE WEIGHTS, AND WHY THERE IS NO VECTOR TERM
============================================
    0.45 * entity_jaccard     do the same real-world things appear
    0.35 * class_match        is it the same KIND of event
    0.20 * npmi_strength      is the association stronger than chance

An earlier draft put 60% on pgvector cosine. That was dropped: the embeddings in
this database come from pipeline/embedder.py's md5 hash fallback, not MiniLM
(sentence-transformers is in requirements-ml.txt, which the detector cron does
not install). Cosine over a hash is lexical collision wearing semantic clothes —
it would have produced confident, meaningless analogs. See CLAUDE.md.

Recency is NOT a term here. It is a term in Phase 3's signal_strength, and
having it in both places would count the same fact twice.

NO SIMHASH / NEAR-DUPLICATE PASS
================================
Near-duplicates in this corpus are republished versions of one story, and the
48h same-symbol cluster collapse below already removes them: two rows for the
same story land in the same window on the same symbol, and only the stronger
survives. A simhash column would be a second mechanism for a problem the first
one already solves.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("sherbyte.analog.matcher")

W_ENTITY = float(os.getenv("SHERR_I_ANALOG_W_ENTITY", "0.45"))
W_CLASS = float(os.getenv("SHERR_I_ANALOG_W_CLASS", "0.35"))
W_NPMI = float(os.getenv("SHERR_I_ANALOG_W_NPMI", "0.20"))

# The engine's own bar for "associated beyond chance", taken from the emergence
# detector rather than redefined here, so one number moves both.
from app.spie.discovery.emergence import MIN_NPMI          # noqa: E402

# Two events on the same symbol inside this window are one event that got
# reported twice. Only the better-scoring row survives.
CLUSTER_HOURS = float(os.getenv("SHERR_I_ANALOG_CLUSTER_HOURS", "48"))

TOP_K = int(os.getenv("SHERR_I_ANALOG_TOP_K", "25"))


def entity_jaccard(a, b) -> float:
    """|A ∩ B| / |A ∪ B| over entity ids. 0.0 when either side is empty."""
    sa, sb = {str(x) for x in (a or [])}, {str(x) for x in (b or [])}
    if not sa or not sb:
        return 0.0
    union = sa | sb
    return len(sa & sb) / len(union) if union else 0.0


def npmi_strength(npmi) -> float:
    """NPMI mapped to 0-1 against the engine's threshold, not against -1..1.

    MIN_NPMI is the floor for "beyond chance", so a pair sitting exactly on it
    contributes nothing and only association ABOVE the bar earns weight. Scoring
    from -1 would hand real weight to pairs the engine considers unrelated.
    """
    if npmi is None:
        return 0.0
    span = 1.0 - MIN_NPMI
    if span <= 0:
        return 1.0 if npmi >= MIN_NPMI else 0.0
    return max(0.0, min(1.0, (float(npmi) - MIN_NPMI) / span))


def score(*, entity_ids, event_class, cand_entity_ids, cand_class, npmi) -> float:
    """The ranking score for one candidate. Deterministic, 0-1."""
    return (W_ENTITY * entity_jaccard(entity_ids, cand_entity_ids)
            + W_CLASS * (1.0 if event_class and event_class == cand_class else 0.0)
            + W_NPMI * npmi_strength(npmi))


# Candidates: overlaps my symbols OR shares my class. Both sides are GIN/btree
# indexed by 022. `article_id <> $3` keeps the live event out of its own analogs.
_CANDIDATES_SQL = """
SELECT event_id, article_id, occurred_at, entity_ids, event_class, linked_symbols
  FROM hist_events
 WHERE (linked_symbols && $1::text[] OR event_class = $2)
   AND article_id <> $3
   AND occurred_at < $4
 ORDER BY occurred_at DESC
 LIMIT $5
"""

# Highest NPMI between any live entity and any candidate entity. cooccurrence
# stores each pair once under CHECK (entity_a < entity_b), so both orderings are
# checked rather than assuming the caller's ids happen to sort correctly.
_NPMI_SQL = """
SELECT MAX(npmi) AS npmi
  FROM cooccurrence
 WHERE npmi IS NOT NULL
   AND ((entity_a = ANY($1::uuid[]) AND entity_b = ANY($2::uuid[]))
     OR (entity_b = ANY($1::uuid[]) AND entity_a = ANY($2::uuid[])))
"""


async def find(conn, *, entity_ids, event_class, linked_symbols, occurred_at,
               article_id=-1, k: int = None, scan_limit: int = 2000,
               min_npmi: float = None) -> dict:
    """The top-k analogs for one live event, plus the funnel that produced them.

    Returns {"analogs": [...], "funnel": {...}}. The funnel is not decoration:
    when this comes back empty, it is the only thing that says whether the
    library is thin, the NPMI gate is biting, or the clustering collapsed
    everything into one row.
    """
    k = TOP_K if k is None else int(k)
    floor = MIN_NPMI if min_npmi is None else float(min_npmi)
    funnel = {"candidates": 0, "dropped_npmi": 0, "dropped_cluster": 0,
              "returned": 0, "min_npmi": floor}

    rows = await conn.fetch(_CANDIDATES_SQL, list(linked_symbols or []),
                            event_class, article_id, occurred_at, scan_limit)
    funnel["candidates"] = len(rows)
    if not rows:
        funnel["diagnosis"] = ("no past event shares a symbol or a class with "
                               "this one — the library has no comparable event")
        return {"analogs": [], "funnel": funnel}

    live_ids = [str(x) for x in (entity_ids or [])]
    scored = []
    for r in rows:
        cand_ids = [str(x) for x in (r["entity_ids"] or [])]
        npmi = None
        if live_ids and cand_ids:
            try:
                npmi = await conn.fetchval(_NPMI_SQL, live_ids, cand_ids)
            except Exception as e:                                 # noqa: BLE001
                log.debug("npmi lookup failed: %s", e)

        # THE NPMI FLOOR IS A HARD GATE, and it is the most likely reason this
        # returns nothing on a young corpus: compute_npmi leaves a pair NULL
        # below min_count=3 co-occurrences, and a NULL cannot clear the bar.
        # That is the intended behaviour — an analog we cannot show is
        # associated beyond chance is not evidence — but the funnel counts it
        # so a silent engine can be told apart from a strict one.
        if npmi is None or float(npmi) < floor:
            funnel["dropped_npmi"] += 1
            continue

        scored.append({
            "event_id": str(r["event_id"]),
            "article_id": r["article_id"],
            "occurred_at": r["occurred_at"],
            "event_class": r["event_class"],
            "linked_symbols": list(r["linked_symbols"] or []),
            "entity_ids": cand_ids,
            "npmi": float(npmi),
            "similarity": round(score(
                entity_ids=live_ids, event_class=event_class,
                cand_entity_ids=cand_ids, cand_class=r["event_class"],
                npmi=npmi), 6),
        })

    scored.sort(key=lambda d: (-d["similarity"], d["occurred_at"]))
    kept = _collapse_clusters(scored)
    funnel["dropped_cluster"] = len(scored) - len(kept)

    now = datetime.now(timezone.utc)
    for d in kept:
        occurred = d["occurred_at"]
        d["age_days"] = round((now - occurred).total_seconds() / 86400.0, 1) \
            if isinstance(occurred, datetime) else None
        d["occurred_at"] = str(occurred)

    out = kept[:k]
    funnel["returned"] = len(out)
    funnel["diagnosis"] = _diagnose(funnel)
    return {"analogs": out, "funnel": funnel}


def _collapse_clusters(scored: list, hours: float = None) -> list:
    """One event per symbol per window — the republished-story collapse.

    Input must already be sorted best-first: the first row seen for a
    (symbol, window) claims it, so the survivor is the highest-scoring version
    of the story rather than whichever happened to be fetched first.

    A row carrying several symbols is dropped only if EVERY one of its symbols
    is already claimed. Dropping it for one overlap would lose the evidence it
    still carries for the others.
    """
    hours = CLUSTER_HOURS if hours is None else float(hours)
    window = max(hours, 0.0001) * 3600.0
    claimed: dict = {}
    kept = []
    for d in scored:
        occurred = d["occurred_at"]
        stamp = occurred.timestamp() if isinstance(occurred, datetime) else 0.0
        free = False
        for sym in d["linked_symbols"] or ["*"]:
            last = claimed.get(sym)
            if last is None or abs(stamp - last) >= window:
                free = True
        if not free:
            continue
        for sym in d["linked_symbols"] or ["*"]:
            claimed[sym] = stamp
        kept.append(d)
    return kept


def _diagnose(f: dict) -> str:
    if not f["candidates"]:
        return "no candidate events"
    if not f["returned"]:
        if f["dropped_npmi"] >= f["candidates"]:
            return (f"all {f['candidates']} candidate(s) fell below the NPMI "
                    f"floor of {f['min_npmi']} — the co-occurrence graph does "
                    f"not yet show these entities associated beyond chance")
        return "every candidate was collapsed as a republished duplicate"
    return (f"{f['returned']} analog(s) from {f['candidates']} candidate(s); "
            f"{f['dropped_npmi']} below NPMI, {f['dropped_cluster']} collapsed")
