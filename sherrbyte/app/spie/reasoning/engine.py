"""
reasoning/engine.py — the SPIE Reasoning Engine.

Turns a focal signal into REASONED intelligence with evidence, using only data the
app already has (domain_signals, cooccurrence+npmi, info_objects embeddings, SimHash
clusters, instrument_keywords). Deterministic throughout; the narrative is assembled by
template (reasoning/narrative.py), never by an LLM.

ASSET-CLASS-AGNOSTIC BY CONSTRUCTION: every market instrument is a domain="market"
signal whose asset class rides in source_id ("yahoo:metals"). A gold move, a NIFTY
move, a USD/INR move and a BTC move all take the same code path — there is no
per-asset branch anywhere in this module.

    reason_focal(conn, focal)            → reasoned_insight dict
    significant_market_moves(conn, ...)  → focal candidates (any asset class)
    run(conn)                            → reason over recent focals, persist insights
"""

from __future__ import annotations

import logging

from app.spie.discovery.base import write_insight, names_for
from app.spie.discovery.anomaly_math import ewma, mad, mad_zscore
from app.spie.knowledge import instrument_map
from app.spie.reasoning import confidence as conf_mod
from app.spie.reasoning import methods as M
from app.spie.reasoning.narrative import build_narrative, violates_language_rules

log = logging.getLogger("sherbyte.reasoning")

# One story = its SimHash cluster, else the signal's own (negated) id.
_STORY_KEY = "COALESCE(cluster_id, -id)"


def asset_class_of(source_id: str | None) -> str:
    """'yahoo:metals' → 'metals'. Unknown/absent → 'market'."""
    if not source_id:
        return "market"
    return source_id.split(":", 1)[1] if ":" in source_id else source_id


# ─── focal selection (uniform across all asset classes) ───────────────────────
async def significant_market_moves(conn, *, history_days: int = 60,
                                   z_threshold: float = 2.0,
                                   min_history: int = 4) -> list[dict]:
    """Market moves that are unusual FOR THAT INSTRUMENT (its own MAD z-score), so a
    1% day can be significant for a currency and routine for crude."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT unnest(entity_ids) AS eid
        FROM domain_signals
        WHERE domain = 'market' AND ts >= now() - ($1 || ' days')::interval
        """,
        str(int(history_days)))

    focals: list[dict] = []
    for r in rows:
        eid = r["eid"]
        series = await conn.fetch(
            """
            SELECT (ts AT TIME ZONE 'UTC')::date AS d,
                   AVG(magnitude * direction) AS v,
                   MAX(ts) AS last_ts, MAX(source_id) AS source_id
            FROM domain_signals
            WHERE domain = 'market' AND $1 = ANY(entity_ids)
              AND ts >= now() - ($2 || ' days')::interval
            GROUP BY 1 ORDER BY d
            """,
            eid, str(int(history_days)))
        if len(series) < min_history + 1:
            continue
        history = [abs(float(s["v"] or 0.0)) for s in series[:-1]]
        latest = series[-1]
        move = float(latest["v"] or 0.0)
        if move == 0:
            continue
        z = mad_zscore(abs(move), ewma(history, 0.3), mad(history))
        if z < z_threshold:
            continue
        focals.append({
            "type": "market_move", "entity_id": eid,
            "asset_class": asset_class_of(latest["source_id"]),
            "move_pct": round(move, 3),
            "direction": 1 if move > 0 else -1,
            "z": round(z, 2), "at": latest["last_ts"], "day": latest["d"],
        })
    return focals


# ─── the reasoning steps ──────────────────────────────────────────────────────
async def _related_entities(conn, eid, days: int, limit: int = 8, *,
                            instrument: str | None = None) -> list[dict]:
    """Step 3 — entities connected to the focal, from two sources.

    1. LEARNED: co-occurrence partners ranked by NPMI (association beyond chance).
    2. SEEDED: instrument_keywords links (see knowledge/instrument_map.py).

    The seeded half exists because a market instrument enters domain_signals as its
    own entity ("WTI Crude") while the news says "Iran" and "OPEC" — the two never
    co-occur in an article, so the learned graph alone has no edge between them and
    every reasoned card died at "no related news". Seeded links are marked
    `link="map"` and carry npmi=None: they say these entities are RELEVANT to the
    instrument, and make no claim about measured association strength. Only the
    learned edges feed the npmi confidence factor, so a seed can never manufacture
    evidence — it only opens the door for real news to be found.
    """
    rows = await conn.fetch(
        """
        SELECT CASE WHEN entity_a = $1 THEN entity_b ELSE entity_a END AS eid,
               MAX(npmi) AS npmi, SUM(count) AS c
        FROM cooccurrence
        WHERE (entity_a = $1 OR entity_b = $1)
          AND window_start >= (now() - ($2 || ' days')::interval)::date
        GROUP BY 1
        ORDER BY MAX(npmi) DESC NULLS LAST, SUM(count) DESC
        LIMIT $3
        """,
        eid, str(int(days)), int(limit))

    out: list[dict] = []
    seen = {str(eid)}
    if rows:
        names = await names_for(conn, [r["eid"] for r in rows])
        for r, n in zip(rows, names):
            seen.add(str(r["eid"]))
            out.append({"entity_id": r["eid"], "entity": n, "link": "cooccurrence",
                        "npmi": round(float(r["npmi"]), 3) if r["npmi"] is not None else None})

    if instrument:
        mapped = [m for m in await instrument_map.related_entity_ids(conn, instrument)
                  if str(m) not in seen]
        if mapped:
            mapped = mapped[:limit]
            for m, n in zip(mapped, await names_for(conn, mapped)):
                out.append({"entity_id": m, "entity": n, "link": "map", "npmi": None})
    return out


async def _news_link(conn, entity_ids: list, at, window_hours: int) -> list[dict]:
    """Step 2 — news clusters in the window whose entities relate to the focal."""
    if not entity_ids:
        return []
    rows = await conn.fetch(
        f"""
        SELECT {_STORY_KEY} AS cluster_id,
               MIN(io.headline) AS headline,
               COUNT(*) AS article_count,
               COUNT(DISTINCT ds.source_id) AS source_count
        FROM domain_signals ds
        LEFT JOIN info_objects io ON io.id::text = ds.ref_id
        WHERE ds.domain = 'news'
          AND ds.entity_ids && $1::uuid[]
          AND ds.ts >= $2::timestamptz - ($3 || ' hours')::interval
          AND ds.ts <= $2::timestamptz
        GROUP BY 1
        ORDER BY COUNT(DISTINCT ds.source_id) DESC, COUNT(*) DESC
        LIMIT 5
        """,
        entity_ids, at, str(int(window_hours)))
    return [{"cluster_id": str(r["cluster_id"]), "cluster_headline": r["headline"],
             "article_count": int(r["article_count"] or 0),
             "source_count": int(r["source_count"] or 0)} for r in rows]


async def _daily_news_series(conn, entity_ids: list, days: int) -> dict:
    """M2 input — daily count of distinct news STORIES touching these entities."""
    if not entity_ids:
        return {}
    rows = await conn.fetch(
        f"""
        SELECT (ts AT TIME ZONE 'UTC')::date AS d,
               COUNT(DISTINCT {_STORY_KEY}) AS c
        FROM domain_signals
        WHERE domain='news' AND entity_ids && $1::uuid[]
          AND ts >= now() - ($2 || ' days')::interval
        GROUP BY 1
        """,
        entity_ids, str(int(days)))
    return {r["d"]: float(r["c"] or 0) for r in rows}


async def _daily_market_series(conn, eid, days: int) -> dict:
    """M2 input — the instrument's daily absolute move."""
    rows = await conn.fetch(
        """
        SELECT (ts AT TIME ZONE 'UTC')::date AS d, AVG(ABS(magnitude)) AS v
        FROM domain_signals
        WHERE domain='market' AND $1 = ANY(entity_ids)
          AND ts >= now() - ($2 || ' days')::interval
        GROUP BY 1
        """,
        eid, str(int(days)))
    return {r["d"]: float(r["v"] or 0.0) for r in rows}


async def _centrality(conn, focal_eid, connected: list, days: int) -> dict:
    """M4 — PageRank over the focal's connected sub-graph, weighted by NPMI, so
    'which of these entities is central' is answered structurally, not by count."""
    ids = [focal_eid] + [c["entity_id"] for c in connected]
    if len(ids) < 2:
        return {}
    rows = await conn.fetch(
        """
        SELECT entity_a, entity_b, MAX(npmi) AS npmi
        FROM cooccurrence
        WHERE entity_a = ANY($1::uuid[]) AND entity_b = ANY($1::uuid[])
          AND window_start >= (now() - ($2 || ' days')::interval)::date
        GROUP BY entity_a, entity_b
        """,
        ids, str(int(days)))
    edges = [(str(r["entity_a"]), str(r["entity_b"]),
              float(r["npmi"]) if r["npmi"] is not None else 0.1) for r in rows]
    if not edges:
        return {}
    return M.pagerank(edges)


async def _cross_market(conn, focal_eid, entity_ids: list, at,
                        window_hours: int, m6_threshold: float = 1.5) -> list[dict]:
    """Step 4 — OTHER instruments (any asset class) that also moved in the window and
    share news entities with the focal. This is the multi-asset payoff."""
    rows = await conn.fetch(
        """
        SELECT unnest(entity_ids) AS eid, AVG(magnitude * direction) AS v,
               MAX(source_id) AS source_id
        FROM domain_signals
        WHERE domain = 'market'
          AND ts >= $1::timestamptz - ($2 || ' hours')::interval
          AND ts <= $1::timestamptz + ($2 || ' hours')::interval
        GROUP BY 1
        """,
        at, str(int(window_hours)))

    out: list[dict] = []
    for r in rows:
        if r["eid"] == focal_eid or not r["v"]:
            continue
        # M7 requires a SHARED DRIVER, not mere simultaneity: the co-mover must
        # itself be connected (M1) to the focal's news entities.
        shared = await conn.fetchval(
            """
            SELECT COUNT(*) FROM cooccurrence
            WHERE ((entity_a = $1 AND entity_b = ANY($2::uuid[]))
                OR (entity_b = $1 AND entity_a = ANY($2::uuid[])))
            """,
            r["eid"], entity_ids) if entity_ids else 0
        if not shared:
            continue
        # ...and its own move must be significant by M6 against its own baseline,
        # so routine noise never counts as convergence.
        hist_rows = await conn.fetch(
            """
            SELECT (ts AT TIME ZONE 'UTC')::date AS d, AVG(magnitude * direction) AS v
            FROM domain_signals
            WHERE domain='market' AND $1 = ANY(entity_ids)
              AND ts >= now() - interval '60 days'
            GROUP BY 1 ORDER BY d
            """,
            r["eid"])
        hist = [abs(float(h["v"] or 0.0)) for h in hist_rows[:-1]]
        v = float(r["v"])
        if len(hist) >= 4:
            z = mad_zscore(abs(v), ewma(hist, 0.3), mad(hist))
            if z < m6_threshold:
                continue
        name = (await names_for(conn, [r["eid"]]))[0]
        out.append({"instrument": name, "asset_class": asset_class_of(r["source_id"]),
                    "move_pct": round(v, 3), "direction": 1 if v > 0 else -1,
                    "shared_entities": int(shared)})
    out.sort(key=lambda x: abs(x["move_pct"]), reverse=True)
    return out[:4]


async def _historical_echo(conn, cluster_ids: list, focal_eid, direction: int,
                           days: int = 365) -> dict:
    """Step 5 — via pgvector, past news clusters semantically similar to this one,
    and whether a same-direction move in the SAME instrument followed. Reported
    honestly, including 'limited history'."""
    if not cluster_ids:
        return {"similar_count": 0, "followed_direction": 0,
                "note": "no comparable prior coverage on record"}
    # Centroid of the current clusters' article embeddings (info_objects embeddings
    # are populated by the embed worker; domain_signals.embedding may be NULL).
    centroid = await conn.fetchval(
        f"""
        SELECT AVG(io.embedding)::vector FROM domain_signals ds
        JOIN info_objects io ON io.id::text = ds.ref_id
        WHERE ds.domain='news' AND io.embedding IS NOT NULL
          AND {_STORY_KEY} = ANY($1::bigint[])
        """,
        [int(c) for c in cluster_ids if str(c).lstrip("-").isdigit()])
    if centroid is None:
        return {"similar_count": 0, "followed_direction": 0,
                "note": "embeddings unavailable for this coverage"}

    similar = await conn.fetch(
        """
        SELECT ds.ts, 1 - (io.embedding <=> $1) AS sim
        FROM domain_signals ds
        JOIN info_objects io ON io.id::text = ds.ref_id
        WHERE ds.domain='news' AND io.embedding IS NOT NULL
          AND ds.ts < now() - interval '3 days'
          AND ds.ts >= now() - ($2 || ' days')::interval
        ORDER BY io.embedding <=> $1
        LIMIT 20
        """,
        centroid, str(int(days)))
    similar = [s for s in similar if (s["sim"] or 0) >= 0.75]
    if not similar:
        return {"similar_count": 0, "followed_direction": 0,
                "note": "no comparable prior coverage on record"}

    followed = 0
    for s in similar:
        moved = await conn.fetchval(
            """
            SELECT AVG(magnitude * direction) FROM domain_signals
            WHERE domain='market' AND $1 = ANY(entity_ids)
              AND ts > $2 AND ts <= $2 + interval '48 hours'
            """,
            focal_eid, s["ts"])
        if moved and ((moved > 0) == (direction > 0)):
            followed += 1

    note = ("limited history" if len(similar) < 2
            else f"{followed} of {len(similar)} prior similar clusters")
    return {"similar_count": len(similar), "followed_direction": followed, "note": note}


# ─── the public entry point ───────────────────────────────────────────────────
async def reason_focal(conn, focal: dict, *, window_hours: int = 48,
                       cooc_days: int = 90) -> dict | None:
    """Build a reasoned_insight for one focal signal. None when there is nothing to
    reason about (no related news in the window) — an honest empty, not a fake card."""
    eid = focal["entity_id"]
    instrument = (await names_for(conn, [eid]))[0]

    connected = await _related_entities(conn, eid, cooc_days, instrument=instrument)
    link_ids = [eid] + [c["entity_id"] for c in connected]
    # Funnel bookkeeping for run() — distinguishes "this instrument has no links at
    # all" from "it has links but no news landed in the window".
    focal["_connected"] = len(connected)
    focal["_mapped"] = sum(1 for c in connected if c["link"] == "map")
    news_link = await _news_link(conn, link_ids, focal["at"], window_hours)
    focal["_news_clusters"] = len(news_link)
    if not news_link:
        return None

    news_entity_ids = link_ids
    cross_market = await _cross_market(conn, eid, news_entity_ids, focal["at"], window_hours)
    historical = await _historical_echo(
        conn, [l["cluster_id"] for l in news_link], eid, focal["direction"])

    # M2 — did news actually PRECEDE the move, and at what lag? Only over the
    # M1-linked entities, with the reference's guards (>=8 buckets, >=2 periods,
    # |rho| >= 0.5). A failure is reported, not hidden.
    lag_result = M.best_lag(
        await _daily_news_series(conn, news_entity_ids, cooc_days),
        await _daily_market_series(conn, eid, cooc_days))

    # M4 — structural centrality within the connected sub-graph.
    ranks = await _centrality(conn, eid, connected, cooc_days)

    articles = sum(l["article_count"] for l in news_link)
    sources = max((l["source_count"] for l in news_link), default=0)

    # Attach the connected-entity names to each link for the narrative.
    top_names = [c["entity"] for c in connected[:3]]
    for l in news_link:
        l["entities"] = top_names

    npmi_values = [c["npmi"] for c in connected if c["npmi"] is not None]
    m5 = conf_mod.evaluate(
        source_count=sources, npmi_values=npmi_values,
        similar_count=historical["similar_count"],
        followed_count=historical["followed_direction"],
        co_moving=len(cross_market), lag_result=lag_result)

    reasoned = {
        "focal": {"type": "market_move", "instrument": instrument,
                  "asset_class": focal["asset_class"], "move_pct": focal["move_pct"],
                  "direction": focal["direction"], "z": focal.get("z")},
        "window_hours": window_hours,
        "news_link": news_link,
        "connected": [{"entity": c["entity"], "npmi": c["npmi"],
                       "centrality": ranks.get(str(c["entity_id"]))}
                      for c in connected[:6]],
        "cross_market": cross_market,
        "historical": historical,
        "lag": lag_result,
        "evidence": {"sources": sources, "articles": articles,
                     "clusters": len(news_link)},
        "confidence": m5["confidence"],
        "confidence_breakdown": m5["breakdown"],
        "confidence_log_odds": m5["log_odds"],
        "methods": ["M1", "M2", "M3", "M4", "M5", "M6", "M7"],
        "method": "reasoning_engine/deterministic+template",
    }
    reasoned["narrative"] = build_narrative(reasoned)

    # Runtime guard: a template change that smuggled in forecast language must not
    # reach the app.
    bad = violates_language_rules(reasoned["narrative"])
    if bad:
        log.error("narrative violated language rules %s — dropping insight", bad)
        return None
    return reasoned


# Funnel from the last run(), so a result of 0 can be explained rather than guessed
# at. Read by workers/detectors.py and surfaced in its summary.
LAST_RUN: dict = {}


async def run(conn, *, window_hours: int | None = None, z_threshold: float = 2.0) -> int:
    """Reason over recent significant moves in ANY asset class; persist as insights
    of type 'reasoned'. Returns how many were written.

    Zero is a legitimate outcome — it means no market move was unusual, or no news
    overlapped it. LAST_RUN records which, so "no mappings" and "no overlap in this
    window" are never confused for each other.
    """
    global LAST_RUN
    if window_hours is None:
        from app.config import settings
        window_hours = int(getattr(settings, "spie_news_window_hours", 72) or 72)
    # Seeded instrument↔keyword links are what let news reach an instrument at all;
    # sync is idempotent, so keeping it here means a fresh deploy is never one
    # forgotten manual step away from an empty SPIE tab.
    try:
        await instrument_map.sync_seeds(conn)
    except Exception as e:
        log.warning("instrument_map seed sync failed: %s", e)

    focals = await significant_market_moves(conn, z_threshold=z_threshold)
    stats = {"window_hours": window_hours,
             "instruments_with_history": 0, "significant_moves": len(focals),
             "with_connected_entities": 0, "with_seeded_links": 0,
             "with_related_news": 0, "insights_written": 0, "errors": 0}
    stats["instruments_with_history"] = int(await conn.fetchval(
        """
        SELECT COUNT(DISTINCT e) FROM (
            SELECT unnest(entity_ids) AS e FROM domain_signals
            WHERE domain='market' AND ts >= now() - interval '60 days') s
        """) or 0)

    written = 0
    for focal in focals:
        try:
            r = await reason_focal(conn, focal, window_hours=window_hours)
        except Exception as e:
            log.warning("reasoning failed for %s: %s", focal.get("entity_id"), e)
            stats["errors"] += 1
            continue
        finally:
            if focal.get("_connected"):
                stats["with_connected_entities"] += 1
            if focal.get("_mapped"):
                stats["with_seeded_links"] += 1
            if focal.get("_news_clusters"):
                stats["with_related_news"] += 1
        if not r:
            continue
        explain = dict(r)
        explain["why"] = r["narrative"]          # the app reads explain_json.why
        await write_insight(
            conn, type="reasoned", entity_ids=[focal["entity_id"]],
            domains=["market", "news"], score=float(r["confidence"]),
            explain=explain,
            signature=f"reasoned:{focal['entity_id']}:{focal['day']}")
        written += 1

    stats["insights_written"] = written
    stats["diagnosis"] = _diagnose(stats)
    LAST_RUN = stats
    log.info("reasoning funnel: %s", stats)
    return written


def _diagnose(s: dict) -> str:
    """Name the exact stage the funnel stopped at, so 0 is never ambiguous."""
    if s["insights_written"]:
        return f"{s['insights_written']} reasoned insight(s) written"
    if not s["instruments_with_history"]:
        return ("no market signals at all — run `python -m app.workers.market_signals`")
    if not s["significant_moves"]:
        return (f"{s['instruments_with_history']} instruments tracked, none moved "
                f"unusually enough to reason about in this window (needs >=5 days of "
                f"history per instrument to compute a baseline)")
    if not s["with_connected_entities"]:
        return ("moves found but no instrument has any linked entities — "
                "instrument_keywords is empty or nothing in it resolves to a known "
                "entity; run `python -m app.workers.instrument_map --report`")
    if not s["with_related_news"]:
        return (f"{s['significant_moves']} significant move(s) with linked entities, "
                f"but no news covering those entities landed in the window — genuine "
                f"no-overlap, not a wiring fault")
    return "news found but every candidate failed a downstream guard"
