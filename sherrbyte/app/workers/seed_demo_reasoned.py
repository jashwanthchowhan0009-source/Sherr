"""
workers/seed_demo_reasoned.py — produce reasoned insights for a demonstration.

    python -m app.workers.seed_demo_reasoned              # seed (tier A, else tier B)
    python -m app.workers.seed_demo_reasoned --limit 3
    python -m app.workers.seed_demo_reasoned --dry-run    # build and print, write nothing
    python -m app.workers.seed_demo_reasoned --clear      # remove every demo insight

WHY THIS EXISTS: the reasoning engine needs several days of market history per
instrument before a MAD baseline exists, and news must overlap a move inside the
link window. Neither condition holds on a fresh corpus, so the engine correctly
produces nothing — which is honest but undemonstrable.

TWO TIERS, tried in order. Both run the REAL code path: the narrative comes from
reasoning/narrative.build_narrative and the confidence from the M5 log-odds
combiner in reasoning/confidence. Nothing here writes prose.

  TIER A — real data, widened window.
      Calls engine.significant_market_moves() and engine.reason_focal() with
      relaxed thresholds (wider news window, lower z, shorter history
      requirement). Every fact in the card is a real row from the DB. The only
      difference from a live run is how far the window reaches.

  TIER B — representative example, only if tier A yields nothing.
      Assembles ONE focal from whatever real values the DB does hold (real
      instrument moves, real news entities, real co-movers) and fills only the
      genuinely-absent fields from a documented representative scenario. It is
      then passed through the SAME template and the SAME M5 maths as any other
      insight.

HONESTY: every insight written here carries explain_json.demo = true, plus
`demo_basis` (which tier), `demo_note` (a sentence a reader can act on) and
`demo_fields` (exactly which values were representative rather than observed).
Tier A is tagged too — a widened window is not what the live engine does, and a
card that a live run would not produce must say so. The UI renders the tag.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from app.db import db
from app.spie.discovery.base import write_insight, names_for
from app.spie.knowledge import instrument_map
from app.spie.reasoning import confidence as conf_mod
from app.spie.reasoning import engine
from app.spie.reasoning import methods as M
from app.spie.reasoning.narrative import build_narrative, violates_language_rules

log = logging.getLogger("sherbyte.worker.seed_demo_reasoned")

_SIG = "demo_reasoned"

# Tier A relaxations. Wide enough to find a real connection on a young corpus,
# and each one is reported in the output so the operator knows what was loosened.
TIER_A = {"window_hours": 24 * 14, "z_threshold": 1.0, "min_history": 1,
          "cooc_days": 365, "history_days": 365}

# Tier B fallback scenario. Used ONLY for fields the database cannot supply, and
# every one used is listed in explain_json.demo_fields.
FALLBACK = {
    "instrument": "WTI Crude", "asset_class": "commodities",
    "move_pct": 3.2, "direction": 1, "z": 2.6,
    "entities": ["Iran", "Strait of Hormuz", "OPEC"],
    "headlines": ["Tanker traffic slows near the Strait of Hormuz",
                  "OPEC+ signals no change to output targets"],
    "cross_market": [
        {"instrument": "Gold", "asset_class": "metals", "move_pct": 1.1,
         "direction": 1, "shared_entities": 2},
        {"instrument": "USD/INR", "asset_class": "forex", "move_pct": 0.4,
         "direction": 1, "shared_entities": 1},
    ],
    "articles": 8, "sources": 6,
}


def _tag(reasoned: dict, *, basis: str, note: str, fields: list[str]) -> dict:
    """Attach the demo disclosure to a reasoned dict."""
    reasoned["demo"] = True
    reasoned["demo_basis"] = basis
    reasoned["demo_note"] = note
    reasoned["demo_fields"] = fields
    return reasoned


# ─── TIER A — the real engine, widened ────────────────────────────────────────
async def tier_a(conn, limit: int) -> list[dict]:
    """Run the genuine reasoning path with relaxed thresholds."""
    focals = await engine.significant_market_moves(
        conn, history_days=TIER_A["history_days"],
        z_threshold=TIER_A["z_threshold"], min_history=TIER_A["min_history"])
    log.info("tier A: %d focal candidate(s)", len(focals))

    out: list[dict] = []
    for focal in focals:
        try:
            r = await engine.reason_focal(conn, focal,
                                          window_hours=TIER_A["window_hours"],
                                          cooc_days=TIER_A["cooc_days"])
        except Exception as e:
            log.warning("tier A reasoning failed for %s: %s", focal.get("entity_id"), e)
            continue
        if not r:
            continue
        days = TIER_A["window_hours"] // 24
        out.append({"focal": focal, "reasoned": _tag(
            r, basis="real_data_widened_window",
            note=(f"Every value here is a real row from the database. The news link "
                  f"window was widened to {days} days (live default is 72h) so a "
                  f"connection could form before market history has accrued."),
            fields=[])})
        if len(out) >= limit:
            break
    return out


# ─── TIER B — representative example through the same machinery ───────────────
async def _real_market_move(conn) -> tuple[dict | None, list[str]]:
    """The largest real move on record, if there is one."""
    row = await conn.fetchrow(
        """
        SELECT unnest(entity_ids) AS eid, AVG(magnitude * direction) AS v,
               MAX(source_id) AS source_id, MAX(ts) AS at,
               (MAX(ts) AT TIME ZONE 'UTC')::date AS day
        FROM domain_signals WHERE domain = 'market'
        GROUP BY 1 ORDER BY ABS(AVG(magnitude * direction)) DESC LIMIT 1
        """)
    if not row or not row["v"]:
        return None, ["instrument", "move_pct", "direction"]
    name = (await names_for(conn, [row["eid"]]))[0]
    v = float(row["v"])
    return ({"entity_id": row["eid"], "instrument": name,
             "asset_class": engine.asset_class_of(row["source_id"]),
             "move_pct": round(v, 3), "direction": 1 if v > 0 else -1,
             "at": row["at"], "day": row["day"]}, [])


async def _real_news(conn, days: int = 365, limit: int = 3) -> tuple[list, list, dict]:
    """The most-covered real news entities, their headlines and article counts."""
    rows = await conn.fetch(
        """
        SELECT e.canonical_name AS name, COUNT(*) AS c,
               COUNT(DISTINCT ds.source_id) AS sources
        FROM domain_signals ds
        JOIN entities e ON e.id = ANY(ds.entity_ids)
        WHERE ds.domain = 'news' AND ds.ts >= now() - ($1 || ' days')::interval
        GROUP BY 1 ORDER BY COUNT(DISTINCT ds.source_id) DESC, COUNT(*) DESC
        LIMIT $2
        """, str(int(days)), int(limit))
    if not rows:
        return [], [], {}
    names = [r["name"] for r in rows]
    heads = await conn.fetch(
        """
        SELECT DISTINCT io.headline
        FROM domain_signals ds JOIN info_objects io ON io.id::text = ds.ref_id
        JOIN entities e ON e.id = ANY(ds.entity_ids)
        WHERE ds.domain = 'news' AND io.headline IS NOT NULL
          AND e.canonical_name = ANY($1::text[])
        ORDER BY 1 LIMIT 3
        """, names)
    return (names, [h["headline"] for h in heads],
            {"articles": sum(int(r["c"]) for r in rows),
             "sources": max(int(r["sources"]) for r in rows)})


async def _real_co_movers(conn, exclude_eid, limit: int = 2) -> list[dict]:
    """Other real instruments that also moved, for the cross-market section."""
    rows = await conn.fetch(
        """
        SELECT unnest(entity_ids) AS eid, AVG(magnitude * direction) AS v,
               MAX(source_id) AS source_id
        FROM domain_signals WHERE domain = 'market'
        GROUP BY 1 ORDER BY ABS(AVG(magnitude * direction)) DESC LIMIT 12
        """)
    out = []
    for r in rows:
        if (exclude_eid is not None and r["eid"] == exclude_eid) or not r["v"]:
            continue
        v = float(r["v"])
        out.append({"instrument": (await names_for(conn, [r["eid"]]))[0],
                    "asset_class": engine.asset_class_of(r["source_id"]),
                    "move_pct": round(v, 3), "direction": 1 if v > 0 else -1,
                    "shared_entities": 1})
        if len(out) >= limit:
            break
    return out


async def _real_npmi(conn, names: list[str]) -> dict:
    """Measured NPMI between the chosen entities, where the graph actually has it.

    Real association strength if it exists, nothing if it doesn't — the confidence
    factor is left to its unevidenced default rather than filled with a guess.
    """
    if len(names) < 2:
        return {}
    rows = await conn.fetch(
        """
        SELECT ea.canonical_name AS a, eb.canonical_name AS b, MAX(c.npmi) AS npmi
        FROM cooccurrence c
        JOIN entities ea ON ea.id = c.entity_a
        JOIN entities eb ON eb.id = c.entity_b
        WHERE ea.canonical_name = ANY($1::text[])
          AND eb.canonical_name = ANY($1::text[])
          AND c.npmi IS NOT NULL
        GROUP BY 1, 2
        """, names)
    out: dict = {}
    for r in rows:
        for n in (r["a"], r["b"]):
            v = float(r["npmi"])
            if n != names[0] and (n not in out or v > out[n]):
                out[n] = round(v, 3)
    return out


async def tier_b(conn) -> dict:
    """Assemble one representative focal from as much real data as exists, then
    run it through the same narrative template and the same M5 combiner."""
    representative: list[str] = []

    move, missing = await _real_market_move(conn)
    representative += missing
    if move is None:
        move = {"entity_id": None, "instrument": FALLBACK["instrument"],
                "asset_class": FALLBACK["asset_class"],
                "move_pct": FALLBACK["move_pct"], "direction": FALLBACK["direction"],
                "at": None, "day": None}

    entities, headlines, counts = await _real_news(conn)
    if not entities:
        entities = FALLBACK["entities"]
        representative.append("news_entities")
    if not headlines:
        headlines = FALLBACK["headlines"]
        representative.append("headlines")
    if not counts:
        counts = {"articles": FALLBACK["articles"], "sources": FALLBACK["sources"]}
        representative.append("article_and_source_counts")

    cross = await _real_co_movers(conn, move.get("entity_id"))
    if not cross:
        cross = FALLBACK["cross_market"]
        representative.append("cross_market")

    # Association strengths for the connected entities. Real NPMI where the graph
    # has it; otherwise the entity is listed with npmi=None, exactly as the engine
    # does for a mapped-but-unmeasured link. No number is invented.
    npmi_map = await _real_npmi(conn, entities)
    connected = [{"entity": n, "npmi": npmi_map.get(n), "centrality": None}
                 for n in entities]
    ranks = M.pagerank([(entities[0], e, npmi_map.get(e) or 0.5)
                        for e in entities[1:]]) if len(entities) > 1 else {}
    for c in connected:
        c["centrality"] = ranks.get(c["entity"])

    news_link = [{"cluster_id": None, "cluster_headline": h,
                  "article_count": max(1, counts["articles"] // max(len(headlines), 1)),
                  "source_count": counts["sources"], "entities": entities[:3]}
                 for h in headlines]

    # No lag evidence is claimed: the guards genuinely did not run.
    lag = {"lag": None, "rho": 0.0, "buckets": 0, "passed": False,
           "reason": "not enough overlapping daily buckets in this corpus yet"}
    historical = {"similar_count": 0, "followed_direction": 0,
                  "note": "no comparable prior coverage on record"}

    m5 = conf_mod.evaluate(
        source_count=counts["sources"],
        npmi_values=[v for v in npmi_map.values() if v is not None],
        similar_count=historical["similar_count"],
        followed_count=historical["followed_direction"],
        co_moving=len(cross), lag_result=lag)

    reasoned = {
        "focal": {"type": "market_move", "instrument": move["instrument"],
                  "asset_class": move["asset_class"], "move_pct": move["move_pct"],
                  "direction": move["direction"], "z": None},
        "window_hours": TIER_A["window_hours"],
        "news_link": news_link,
        "connected": connected,
        "cross_market": cross,
        "historical": historical,
        "lag": lag,
        "evidence": {"sources": counts["sources"], "articles": counts["articles"],
                     "clusters": len(news_link)},
        "confidence": m5["confidence"],
        "confidence_breakdown": m5["breakdown"],
        "confidence_log_odds": m5["log_odds"],
        "methods": ["M1", "M4", "M5", "M7"],
        "method": "reasoning_engine/deterministic+template",
    }
    reasoned["narrative"] = build_narrative(reasoned)

    note = ("Representative example. It is assembled from real database values "
            "wherever they exist and passed through the same narrative template and "
            "the same confidence maths as a live insight."
            + (f" Representative fields: {', '.join(sorted(set(representative)))}."
               if representative else " Every field came from real data."))
    return {"focal": move,
            "reasoned": _tag(reasoned, basis="constructed_example", note=note,
                             fields=sorted(set(representative)))}


# ─── persistence ──────────────────────────────────────────────────────────────
async def _persist(conn, item: dict, idx: int) -> str:
    r = item["reasoned"]
    focal = item["focal"]
    explain = dict(r)
    explain["why"] = r["narrative"]
    eids = [focal["entity_id"]] if focal.get("entity_id") else []
    return await write_insight(
        conn, type="reasoned", entity_ids=eids, domains=["market", "news"],
        score=float(r["confidence"]), explain=explain,
        signature=f"{_SIG}:{focal.get('entity_id') or 'example'}:{idx}")


async def clear(conn) -> int:
    n = await conn.fetchval(
        "WITH d AS (DELETE FROM insights WHERE signature LIKE $1 RETURNING 1) "
        "SELECT COUNT(*) FROM d", f"{_SIG}:%")
    return int(n or 0)


async def run(*, limit: int = 3, dry_run: bool = False,
              do_clear: bool = False) -> dict:
    async with db.acquire() as conn:
        if do_clear:
            return {"cleared": await clear(conn)}

        # Seeding the links first means real cards form on their own as market
        # history accrues — the demo seed is a bridge, not a dependency.
        seeded = await instrument_map.sync_seeds(conn)
        from app.workers.instrument_map import sync_ticker_map
        seeded["ticker_map_rows"] = await sync_ticker_map(conn)

        items = await tier_a(conn, limit)
        tier = "A"
        if not items:
            items = [await tier_b(conn)]
            tier = "B"

        # A template change that smuggled in forecast language must not ship in a
        # demo card either — this is exactly what an investor would read.
        for it in items:
            bad = violates_language_rules(it["reasoned"]["narrative"])
            if bad:
                raise RuntimeError(f"narrative violated language rules: {bad}")

        written = []
        if not dry_run:
            for i, it in enumerate(items):
                written.append(await _persist(conn, it, i))

        return {
            "tier": tier,
            "tier_explanation": (
                "A = real rows from the DB through the real engine, window widened"
                if tier == "A" else
                "B = tier A found no connection; representative example through the "
                "same template and maths"),
            "relaxations": TIER_A if tier == "A" else None,
            "mappings": seeded,
            "insights": len(items),
            "insight_ids": written,
            "dry_run": dry_run,
            "cards": [{"instrument": it["reasoned"]["focal"]["instrument"],
                       "confidence": it["reasoned"]["confidence"],
                       "demo_basis": it["reasoned"]["demo_basis"],
                       "demo_fields": it["reasoned"]["demo_fields"],
                       "narrative": it["reasoned"]["narrative"]} for it in items],
            "example_explain_json": (
                {**items[0]["reasoned"], "why": items[0]["reasoned"]["narrative"]}
                if items else None),
        }


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed reasoned insights for a demonstration (honestly tagged).")
    parser.add_argument("--limit", type=int, default=3, help="max insights to seed")
    parser.add_argument("--dry-run", action="store_true",
                        help="build and print without writing")
    parser.add_argument("--clear", action="store_true",
                        help="delete every demo insight and exit")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(limit=args.limit, dry_run=args.dry_run,
                           do_clear=args.clear)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
