"""
discovery/observation.py — TIER 1 observation cards.

The reasoning engine (Tier 2) needs several days of per-instrument history before a
baseline exists, so on a young corpus it correctly produces nothing. That is honest
and useless: the app shows an empty tab. Tier 1 fills it with cards that need only
TODAY's data and say so plainly.

WHAT IT NEEDS: one day of market signals and news in the last 24-48h. No baseline, no
lead-lag, no historical echo — none of which exist yet, and none of which are faked.

RANKING: |MAD z-score| when the instrument has >= 2 daily buckets, else |% change|.
Which was used is recorded in `ranked_by`, and a z computed from a short baseline
carries `baseline_points` so the card can flag it as provisional. A statistic is never
reported without the n it rests on.

CROSS-MARKET GUARD: another instrument counts as a co-mover only if it moved the same
direction today AND shares news entities with the focal. Same-day movement alone is
coincidence — on any given day roughly half the instruments move the same way, and
with 13 instruments ranked, some alignment is expected by chance. The shared-entity
requirement is what makes it evidence rather than arithmetic.

LANGUAGE: observation and interpretation only. Every card runs the same runtime guard
as Tier 2 (forecast phrasing AND investment-advice phrasing), and a violation drops
the card rather than shipping it.
"""

from __future__ import annotations

import logging

from app.spie.discovery.anomaly_math import ewma, mad, mad_zscore
from app.spie.discovery.base import write_insight, names_for
from app.spie.knowledge import instrument_map
from app.spie.knowledge.entity_resolver import is_valid_mention
from app.spie.reasoning import confidence as conf_mod
from app.spie.reasoning import interpretation as interp
from app.spie.reasoning.narrative import (
    DISCLAIMER, build_narrative, violates_language_rules)

log = logging.getLogger("sherbyte.detectors.observation")

# QUALIFIED with the ds alias: every query using this joins info_objects, which
# also has an "id" column, and an unqualified -id is ambiguous in Postgres.
_STORY_KEY = "COALESCE(ds.cluster_id, -ds.id)"

# A z-score below this many baseline points is reported as provisional; below
# MIN_BUCKETS_FOR_Z it is not computed at all and ranking falls back to raw % change.
MIN_BUCKETS_FOR_Z = 2
PROVISIONAL_BASELINE_POINTS = 4

LAST_RUN: dict = {}


def rank_key(move_pct: float, z: float | None) -> tuple[float, str]:
    """(magnitude, how it was ranked). z when available, else raw move."""
    if z is not None:
        return abs(z), "mad_z"
    return abs(float(move_pct or 0.0)), "abs_pct_change"


def baseline_note(points: int | None) -> str | None:
    """Say what the z rests on, or say nothing rather than implying a settled one."""
    if points is None:
        return None
    if points < PROVISIONAL_BASELINE_POINTS:
        return f"provisional baseline, n={points}"
    return f"baseline n={points}"


def clean_entities(rows: list[dict]) -> list[dict]:
    """Drop junk mentions that should never have entered the graph.

    is_valid_mention is the same filter ingestion uses; applying it again here keeps
    rows that predate a filter fix ("It's", "One") from surfacing on a card. Cheap
    belt-and-braces — the alternative is a re-backfill before every demo.
    """
    out = []
    for r in rows:
        name = (r.get("entity") or "").strip()
        if not name or not is_valid_mention(name, r.get("type") or "MISC"):
            continue
        out.append(r)
    return out


# ─── data access ──────────────────────────────────────────────────────────────
async def _movers(conn, *, history_days: int = 60) -> list[dict]:
    """Today's instrument moves, ranked. No minimum history."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT unnest(entity_ids) AS eid FROM domain_signals
        WHERE domain = 'market' AND ts >= now() - ($1 || ' days')::interval
        """, str(int(history_days)))

    movers: list[dict] = []
    for r in rows:
        series = await conn.fetch(
            """
            SELECT (ts AT TIME ZONE 'UTC')::date AS d, AVG(magnitude * direction) AS v,
                   MAX(ts) AS last_ts, MAX(source_id) AS source_id
            FROM domain_signals
            WHERE domain = 'market' AND $1 = ANY(entity_ids)
              AND ts >= now() - ($2 || ' days')::interval
            GROUP BY 1 ORDER BY d
            """, r["eid"], str(int(history_days)))
        if not series:
            continue
        latest = series[-1]
        move = float(latest["v"] or 0.0)
        if move == 0:
            continue

        history = [abs(float(s["v"] or 0.0)) for s in series[:-1]]
        z = points = None
        if len(history) >= MIN_BUCKETS_FOR_Z - 1 and history:
            z = round(mad_zscore(abs(move), ewma(history, 0.3), mad(history)), 2)
            points = len(history)

        magnitude, ranked_by = rank_key(move, z)
        movers.append({
            "type": "market_move", "entity_id": r["eid"],
            "asset_class": _asset_class(latest["source_id"]),
            "move_pct": round(move, 3), "direction": 1 if move > 0 else -1,
            "z": z, "baseline_points": points, "ranked_by": ranked_by,
            "magnitude": magnitude, "at": latest["last_ts"], "day": latest["d"],
        })
    movers.sort(key=lambda m: m["magnitude"], reverse=True)
    return movers


def _asset_class(source_id: str | None) -> str:
    if not source_id:
        return "market"
    return source_id.split(":", 1)[1] if ":" in source_id else source_id


async def _related(conn, eid, instrument: str, days: int = 90,
                   limit: int = 8) -> list[dict]:
    """Entities linked to the instrument — co-occurrence partners plus seeded map."""
    rows = await conn.fetch(
        """
        SELECT CASE WHEN entity_a = $1 THEN entity_b ELSE entity_a END AS eid,
               MAX(npmi) AS npmi
        FROM cooccurrence
        WHERE (entity_a = $1 OR entity_b = $1)
          AND window_start >= (now() - ($2 || ' days')::interval)::date
        GROUP BY 1 ORDER BY MAX(npmi) DESC NULLS LAST LIMIT $3
        """, eid, str(int(days)), int(limit))

    out, seen = [], {str(eid)}
    if rows:
        for r, n in zip(rows, await names_for(conn, [x["eid"] for x in rows])):
            seen.add(str(r["eid"]))
            out.append({"entity_id": r["eid"], "entity": n, "link": "cooccurrence",
                        "npmi": round(float(r["npmi"]), 3) if r["npmi"] is not None else None})
    mapped = [m for m in await instrument_map.related_entity_ids(conn, instrument)
              if str(m) not in seen][:limit]
    if mapped:
        for m, n in zip(mapped, await names_for(conn, mapped)):
            out.append({"entity_id": m, "entity": n, "link": "map", "npmi": None})
    return clean_entities(out)


async def _news(conn, entity_ids: list, at, hours: int) -> list[dict]:
    if not entity_ids:
        return []
    rows = await conn.fetch(
        f"""
        SELECT {_STORY_KEY} AS cluster_id, MIN(io.headline) AS headline,
               COUNT(*) AS article_count, COUNT(DISTINCT ds.source_id) AS source_count
        FROM domain_signals ds
        LEFT JOIN info_objects io ON io.id::text = ds.ref_id
        WHERE ds.domain = 'news' AND ds.entity_ids && $1::uuid[]
          AND ds.ts >= $2::timestamptz - ($3 || ' hours')::interval
          AND ds.ts <= $2::timestamptz
        GROUP BY 1 ORDER BY COUNT(DISTINCT ds.source_id) DESC, COUNT(*) DESC LIMIT 4
        """, entity_ids, at, str(int(hours)))
    return [{"cluster_id": str(r["cluster_id"]), "cluster_headline": r["headline"],
             "article_count": int(r["article_count"] or 0),
             "source_count": int(r["source_count"] or 0)} for r in rows]


async def _co_movers(conn, focal: dict, entity_ids: list, movers: list[dict],
                     limit: int = 3) -> list[dict]:
    """Same-direction movers today that ALSO share news entities with the focal.

    The shared-entity requirement is the coincidence guard: on a given day about half
    the instruments move the same way, so direction alone is arithmetic, not evidence.
    """
    out = []
    for m in movers:
        if m["entity_id"] == focal["entity_id"] or m["direction"] != focal["direction"]:
            continue
        shared = await conn.fetchval(
            """
            SELECT COUNT(*) FROM cooccurrence
            WHERE ((entity_a = $1 AND entity_b = ANY($2::uuid[]))
                OR (entity_b = $1 AND entity_a = ANY($2::uuid[])))
            """, m["entity_id"], entity_ids) if entity_ids else 0
        if not shared:
            continue
        out.append({"instrument": (await names_for(conn, [m["entity_id"]]))[0],
                    "asset_class": m["asset_class"], "move_pct": m["move_pct"],
                    "direction": m["direction"], "shared_entities": int(shared)})
        if len(out) >= limit:
            break
    return out


# ─── the detector ─────────────────────────────────────────────────────────────
async def run(conn, *, top_n: int = 8, news_hours: int = 48) -> int:
    """Build Tier 1 observation cards for today's top movers."""
    global LAST_RUN
    try:
        await instrument_map.sync_seeds(conn)
    except Exception as e:
        log.warning("instrument_map seed sync failed: %s", e)

    movers = await _movers(conn)
    stats = {"movers": len(movers), "with_related_entities": 0,
             "with_news": 0, "written": 0, "dropped_language": 0,
             "news_hours": news_hours}

    written = 0
    for focal in movers[:top_n]:
        instrument = (await names_for(conn, [focal["entity_id"]]))[0]
        connected = await _related(conn, focal["entity_id"], instrument)
        if connected:
            stats["with_related_entities"] += 1
        link_ids = [focal["entity_id"]] + [c["entity_id"] for c in connected]
        news = await _news(conn, link_ids, focal["at"], news_hours)
        if not news:
            continue
        stats["with_news"] += 1

        cross = await _co_movers(conn, focal, link_ids, movers)
        articles = sum(n["article_count"] for n in news)
        sources = max((n["source_count"] for n in news), default=0)
        top_names = [c["entity"] for c in connected[:3]]
        for n in news:
            n["entities"] = top_names

        # No lag and no history in Tier 1 — both are stated as absent, and both
        # therefore contribute their unevidenced weight to the confidence, not a
        # borrowed one.
        lag = {"passed": False, "reason": "Tier 1 uses today's data only; "
                                          "lead-lag needs 8 daily buckets"}
        historical = {"similar_count": 0, "followed_direction": 0,
                      "note": "Tier 1 does not use historical echo"}
        m5 = conf_mod.evaluate(
            source_count=sources,
            npmi_values=[c["npmi"] for c in connected if c["npmi"] is not None],
            similar_count=0, followed_count=0, co_moving=len(cross), lag_result=lag)

        card = {
            "tier": "observation",
            "tier_label": "Observation — based on today's activity",
            "demo": False,
            "focal": {"type": "market_move", "instrument": instrument,
                      "asset_class": focal["asset_class"],
                      "move_pct": focal["move_pct"], "direction": focal["direction"],
                      "z": focal["z"], "baseline_points": focal["baseline_points"],
                      "ranked_by": focal["ranked_by"],
                      "baseline_note": baseline_note(focal["baseline_points"])},
            "window_hours": news_hours,
            "news_link": news,
            "connected": connected[:6],
            "cross_market": cross,
            "historical": historical,
            "lag": lag,
            "evidence": {"sources": sources, "articles": articles,
                         "clusters": len(news)},
            "confidence": m5["confidence"],
            "confidence_breakdown": m5["breakdown"],
            "confidence_log_odds": m5["log_odds"],
            "methods": ["M1", "M5", "M6", "M7"],
            "method": "observation/deterministic+template",
            "disclaimer": DISCLAIMER,
        }
        card["narrative"] = build_narrative(card, concise=True)
        interp.attach(card)

        names = [instrument] + [c["entity"] for c in connected] + \
                [c["instrument"] for c in cross]
        bad = violates_language_rules(
            f"{card['narrative']} {card['interpretation']['text']}", entity_names=names)
        if bad:
            log.error("observation card violated language rules %s — dropping", bad)
            stats["dropped_language"] += 1
            continue

        explain = dict(card)
        explain["why"] = card["narrative"]
        await write_insight(
            conn, type="observation", entity_ids=[focal["entity_id"]],
            domains=["market", "news"], score=float(focal["magnitude"]),
            explain=explain,
            signature=f"observation:{focal['entity_id']}:{focal['day']}")
        written += 1

    stats["written"] = written
    LAST_RUN = stats
    log.info("observation funnel: %s", stats)
    return written
