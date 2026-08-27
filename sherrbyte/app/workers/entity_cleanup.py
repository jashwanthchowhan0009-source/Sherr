"""
workers/entity_cleanup.py — apply the current resolver to entities already stored.

The resolver decides what is an entity at EXTRACTION time, so tightening it only
affects new mentions. Everything already in the graph keeps whatever the old
rules produced, and /patterns keeps serving it: "India's" as a separate node from
"India", "Fifa's" from "FIFA", plus "Moreover", "Test" and "Kevin M" as named
entities. Two of the top ten patterns were an entity paired with what is really
itself — the same name split across two ids.

This re-runs the current normalize_name / is_valid_mention over every stored
entity and repairs the graph:

    merge   entities that now share (norm_key, type) — the possessive and case
            variants collapse onto the most-referenced survivor
    drop    entities the filter now rejects, and every reference to them
    rebuild co-occurrence from the corrected signals, then NPMI
    prune   insights that have collapsed to fewer than two distinct entities —
            the self-pairs, which are only visible after the merge

Idempotent: a second run finds nothing to merge and nothing to drop.

    python -m app.workers.entity_cleanup --dry-run     # report, change nothing
    python -m app.workers.entity_cleanup
    python -m app.workers.entity_cleanup --days 180    # co-occurrence window
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time

from app.spie.knowledge.entity_resolver import (
    RESOLVER_BUILD, SEED_LOOKUP, is_valid_mention, normalize_name, resolve_key,
    seed_aliases)

log = logging.getLogger("sherbyte.worker.entity_cleanup")

# Live state, so a caller with no shell can poll. Same shape as the other jobs.
PROGRESS: dict = {}


def progress() -> dict:
    p = dict(PROGRESS)
    if p.get("started_at"):
        p["elapsed_s"] = round((p.get("finished_at") or time.time()) - p["started_at"], 1)
    return p


def plan(rows) -> dict:
    """Pure: decide what merges into what, and what is dropped.

    `rows` are (id, canonical_name, type, norm_key, mention_count). Returns
    {"merge": {old_id: new_id}, "junk": [id], "renorm": {id: norm_key},
     "examples": [...]}.

    The survivor of a merge group is the most-referenced entity, tie-broken by
    the shortest display name — "India" over "India's", "FIFA" over "Fifa's" —
    so the canonical form a reader sees is the plain one. A seeded entity always
    wins outright regardless of counts; it is the curated identity.

    TYPE IS PART OF IDENTITY, EXCEPT FOR MISC. entities is keyed (norm_key, type)
    on purpose: "Jordan" the country and "Jordan" the person are different things
    and must not merge. But MISC is not a claim about identity, it is the
    fallback the resolver uses when the NER model offered nothing usable — and
    the same organisation arriving as ORG one day and MISC the next is a large
    share of the duplication here. So MISC folds into a real type when the
    normalized name has exactly one; two real types means genuine ambiguity and
    everything stays separate.

    Case alone was never the problem — normalize_name has always lowercased, so
    "FIFA" and "fifa" already shared a key. What looked like case variants were
    the possessive forms ("Fifa's") and this MISC/ORG split.
    """
    by_norm: dict[str, list] = {}
    junk: list = []
    renorm: dict = {}

    for r in rows:
        eid, name, etype, old_norm, count = (
            r["id"], r["canonical_name"] or "", r["type"] or "MISC",
            r["norm_key"] or "", int(r["mention_count"] or 0))
        if not is_valid_mention(name, etype):
            junk.append({"id": eid, "name": name, "type": etype})
            continue
        # resolve_key, not normalize_name: it applies the curated seed synonyms
        # too, so "Modi's" -> "modi" -> the Narendra Modi identity. Grouping on
        # the bare normalized form would merge the possessives and still leave
        # every seeded short form sitting beside its own canonical entity.
        new_norm, _seed_type, _display = resolve_key(name, etype)
        new_norm = new_norm or old_norm
        # norm_key stores the resolver's own key for this row, which is what the
        # next mention will look up.
        if new_norm != old_norm:
            renorm[eid] = new_norm
        # Seeded means "this row IS the curated canonical form", not "this row
        # resolves through a seed" — every alias does that. Without the
        # distinction the sort below hands the group to whichever alias is most
        # mentioned, so "Modi" (900 mentions) absorbs "Narendra Modi" and the
        # curated identity is lost in the merge that was meant to protect it.
        by_norm.setdefault(new_norm, []).append(
            {"id": eid, "name": name, "type": etype, "count": count,
             "seed": normalize_name(name) == new_norm and new_norm in SEED_LOOKUP})

    merge: dict = {}
    examples: list = []
    for norm, members in by_norm.items():
        real_types = {m["type"] for m in members if m["type"] != "MISC"}
        # One real type -> MISC joins it. Otherwise every type keeps its own group.
        if len(real_types) == 1:
            groups = [members]
        else:
            buckets: dict[str, list] = {}
            for m in members:
                buckets.setdefault(m["type"], []).append(m)
            groups = list(buckets.values())

        for group in groups:
            if len(group) < 2:
                continue
            # Seeded first, then most-referenced, then the shortest display form.
            group.sort(key=lambda m: (not m["seed"], -m["count"], len(m["name"]),
                                      m["name"]))
            survivor = group[0]
            for loser in group[1:]:
                merge[loser["id"]] = survivor["id"]
            if len(examples) < 25:
                examples.append({"canonical": survivor["name"],
                                 "type": survivor["type"],
                                 "absorbed": [f"{m['name']} ({m['type']})"
                                              for m in group[1:]]})

    return {"merge": merge, "junk": junk, "renorm": renorm, "examples": examples}


# ─── SQL ─────────────────────────────────────────────────────────────────────
# The remap is applied set-based through two temp tables rather than row by row:
# entity_ids is an array column, so a per-entity UPDATE would rewrite the same
# domain_signals row once for every merged entity it mentions.
_TEMP = """
CREATE TEMP TABLE _merge (old_id UUID PRIMARY KEY, new_id UUID NOT NULL) ON COMMIT DROP;
CREATE TEMP TABLE _junk (id UUID PRIMARY KEY) ON COMMIT DROP;
"""

# COALESCE(m.new_id, x) maps a merged id onto its survivor and leaves everything
# else alone; the NOT EXISTS drops junk ids entirely. DISTINCT is what collapses
# a row that mentioned both "India" and "India's" into a single id rather than
# leaving the same entity twice in one array.
_REMAP_ARRAY = """
UPDATE {table} t
   SET entity_ids = COALESCE((
         SELECT ARRAY_AGG(DISTINCT COALESCE(m.new_id, x))
           FROM unnest(t.entity_ids) AS x
           LEFT JOIN _merge m ON m.old_id = x
          WHERE NOT EXISTS (SELECT 1 FROM _junk j WHERE j.id = COALESCE(m.new_id, x))
       ), '{{}}'::uuid[])
 WHERE EXISTS (
         SELECT 1 FROM unnest(t.entity_ids) AS x
          WHERE x IN (SELECT old_id FROM _merge) OR x IN (SELECT id FROM _junk))
"""

async def _remap_pairs(conn, table: str) -> dict:
    """Repoint a two-column pair table, then drop what the merge made degenerate.

    Delete-and-reinsert rather than UPDATE: the merge can collide two rows onto
    one primary key, and it can make entity_a == entity_b, which the table's own
    CHECK forbids. Both are ordinary outcomes here, not errors.
    """
    moved = await conn.fetch(
        f"""
        DELETE FROM {table} t
         WHERE t.entity_a IN (SELECT old_id FROM _merge)
            OR t.entity_b IN (SELECT old_id FROM _merge)
            OR t.entity_a IN (SELECT id FROM _junk)
            OR t.entity_b IN (SELECT id FROM _junk)
        RETURNING t.*
        """)
    return {"removed": len(moved)}


async def apply(conn, decided: dict, *, days: int = 90) -> dict:
    """Apply a plan(). Runs inside one transaction — a partially remapped graph
    is worse than an un-cleaned one."""
    merge, junk = decided["merge"], [j["id"] for j in decided["junk"]]
    stats: dict = {"merged": len(merge), "dropped": len(junk)}

    async with conn.transaction():
        await conn.execute(_TEMP)
        if merge:
            await conn.executemany(
                "INSERT INTO _merge (old_id, new_id) VALUES ($1, $2)",
                list(merge.items()))
        if junk:
            await conn.executemany("INSERT INTO _junk (id) VALUES ($1)",
                                   [(j,) for j in junk])

        PROGRESS["phase"] = "repointing signals"
        for table in ("domain_signals", "insights"):
            res = await conn.execute(_REMAP_ARRAY.format(table=table))
            stats[f"{table}_rows_repointed"] = int(res.split()[-1])

        PROGRESS["phase"] = "repointing aliases"
        # An alias may already exist on the survivor; the PK is (norm_alias,
        # entity_id), so move what can move and delete the rest.
        await conn.execute("""
            UPDATE entity_aliases a SET entity_id = m.new_id
              FROM _merge m
             WHERE a.entity_id = m.old_id
               AND NOT EXISTS (SELECT 1 FROM entity_aliases b
                                WHERE b.norm_alias = a.norm_alias AND b.entity_id = m.new_id)
        """)
        await conn.execute(
            "DELETE FROM entity_aliases WHERE entity_id IN (SELECT old_id FROM _merge)")

        PROGRESS["phase"] = "clearing derived pair tables"
        # cooccurrence and its dedup ledger are DERIVED from domain_signals and
        # are rebuilt below, so stale ids are cleared rather than remapped.
        for table in ("cooccurrence", "cooccurrence_events", "watchlist"):
            stats[table] = await _remap_pairs(conn, table)

        PROGRESS["phase"] = "deleting merged and junk entities"
        # entity_aliases cascades. Deleted before the norm_key correction below,
        # not after: a loser's corrected key is by definition its survivor's, and
        # entities is UNIQUE (norm_key, type) — so renaming a row that is about
        # to disappear collides with the row it is being merged into.
        res = await conn.execute(
            "DELETE FROM entities WHERE id IN (SELECT old_id FROM _merge) "
            "OR id IN (SELECT id FROM _junk)")
        stats["entities_deleted"] = int(res.split()[-1])

        PROGRESS["phase"] = "correcting norm_keys"
        # Survivors only — the losers are gone. Their stored key still reflects
        # the old rules ("india s"), so without this the next mention of "India"
        # would fail to find the surviving row and create a third one.
        gone = set(merge) | set(junk)
        survivors = [(eid, norm) for eid, norm in decided["renorm"].items()
                     if eid not in gone]
        if survivors:
            await conn.executemany(
                "UPDATE entities SET norm_key = $2, updated_at = now() WHERE id = $1",
                survivors)
        stats["norm_keys_corrected"] = len(survivors)

        PROGRESS["phase"] = "pruning collapsed insights"
        # Only visible after the merge: an insight whose two entities were the
        # same name under two ids now has one distinct entity, and "X connects to
        # X" is exactly what a reader called noise.
        res = await conn.execute(
            "DELETE FROM insights WHERE COALESCE(array_length(entity_ids, 1), 0) < 2")
        stats["insights_pruned"] = int(res.split()[-1])

    return stats


async def rebuild_graph(conn, days: int = 90) -> dict:
    """Rebuild co-occurrence from the corrected signals, then NPMI.

    The pair counts are materialized from domain_signals, so rebuilding is both
    simpler and safer than trying to add merged counts together — and it is the
    only way the NPMI weights end up consistent with the new entity set.

    cooccurrence_events was cleared above deliberately: it is the dedup ledger,
    and update_for_signal only increments a pair-day the FIRST time a cluster
    contributes it. Leaving it populated would make this rebuild write zero.
    """
    from app.spie.graph import cooccurrence
    PROGRESS["phase"] = "rebuilding co-occurrence"
    out = await cooccurrence.backfill(conn, days=days)
    PROGRESS["phase"] = "recomputing npmi"
    out["npmi_pairs"] = await cooccurrence.compute_npmi(conn, days=days)
    return out


async def run(conn, *, dry_run: bool = False, days: int = 90) -> dict:
    """Plan, apply, rebuild. Returns a report."""
    PROGRESS.clear()
    PROGRESS.update({"running": True, "started_at": time.time(), "finished_at": None,
                     "phase": "reading entities", "dry_run": dry_run})
    try:
        if not dry_run:
            await seed_aliases(conn)
        rows = await conn.fetch(
            "SELECT id, canonical_name, type, norm_key, mention_count FROM entities")
        PROGRESS.update({"entities": len(rows), "phase": "planning"})

        decided = plan(rows)
        report = {
            "resolver_build": RESOLVER_BUILD,
            "entities_before": len(rows),
            "to_merge": len(decided["merge"]),
            "to_drop": len(decided["junk"]),
            "to_renorm": len(decided["renorm"]),
            "merge_examples": decided["examples"],
            "drop_examples": [j["name"] for j in decided["junk"][:40]],
        }
        PROGRESS.update({"to_merge": report["to_merge"], "to_drop": report["to_drop"]})

        if dry_run:
            report["dry_run"] = True
            return report

        report["applied"] = await apply(conn, decided, days=days)
        report["graph"] = await rebuild_graph(conn, days=days)
        report["entities_after"] = int(
            await conn.fetchval("SELECT COUNT(*) FROM entities"))
        report["insights_after"] = int(
            await conn.fetchval("SELECT COUNT(*) FROM insights"))
        PROGRESS["phase"] = "done"
        return report
    finally:
        PROGRESS["running"] = False
        PROGRESS["finished_at"] = time.time()


async def _main() -> None:
    ap = argparse.ArgumentParser(
        description="Re-apply the current resolver to stored entities.")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would merge and drop, change nothing")
    ap.add_argument("--days", type=int, default=90,
                    help="co-occurrence window to rebuild (default 90)")
    args = ap.parse_args()

    from app.db import db
    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        async with db.acquire() as conn:
            result = await run(conn, dry_run=args.dry_run, days=args.days)
        log.info("entity cleanup: %s", {k: v for k, v in result.items()
                                        if not k.endswith("examples")})
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
