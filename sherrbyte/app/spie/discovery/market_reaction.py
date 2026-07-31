"""
discovery/market_reaction.py — news ↔ market linkage (Sherr-I Part C).

Joins the two signal streams Sherr-I now has: domain="market" moves (Part B) and
domain="news" story clusters. Two directions, both reported the same way:

  A. news → move : a significant market move, with related news in the preceding
                   window (news PRECEDED the move)
  B. move → news : a significant move followed by a news spike about related
                   entities in the following window (news FOLLOWED the move)

"Significant" is the instrument's own MAD z-score, not a fixed percentage — a 1%
day is ordinary for crude and extraordinary for USD/INR.

"Related" news = clusters whose entities co-occur with the instrument's entity in
the materialized cooccurrence graph, plus this instrument's seeded keyword links
from instrument_keywords (knowledge/instrument_map.py).

LANGUAGE RULE (hard): output says the news *preceded* or *coincided with* the move.
Never "caused", never "will move". This is detection of an observed sequence, not
causation and not prediction.
"""

from __future__ import annotations

import logging

from app.spie.discovery.base import write_insight, names_for
from app.spie.discovery.anomaly_math import ewma, mad, mad_zscore
from app.spie.knowledge import instrument_map

log = logging.getLogger("sherbyte.detectors.market_reaction")

# Funnel from the last run(), surfaced by workers/detectors.py.
LAST_RUN: dict = {}

# One story = its SimHash cluster, else the signal's own (negated) id.
# QUALIFIED with the ds alias: every query using this joins info_objects, which
# also has an "id" column, and an unqualified -id is ambiguous in Postgres.
_STORY_KEY = "COALESCE(ds.cluster_id, -ds.id)"


def move_phrase(direction: int, pct: float) -> str:
    """Neutral description of a move. No causal or predictive verbs."""
    if direction > 0:
        return f"rose {abs(pct):.2f}%"
    if direction < 0:
        return f"fell {abs(pct):.2f}%"
    return "was flat"


def relation_phrase(mode: str) -> str:
    """Sequence wording only — 'preceded' / 'followed', never 'caused'."""
    return ("preceded the move" if mode == "news_then_move"
            else "followed the move")


async def _related_entity_ids(conn, instrument_eid, days: int, limit: int = 40, *,
                              instrument: str | None = None) -> list:
    """Entities related to the instrument: co-occurrence partners (ranked by NPMI,
    the association-beyond-chance measure) plus this instrument's seeded
    instrument_keywords links.

    The seeded half was previously read from entity_ticker_map with no scoping at
    all — every mapped entity was appended to every instrument's related list, so
    the moment that table had rows, news about anything would have counted as
    related to everything. Links are now per-instrument (see
    knowledge/instrument_map.py), which is also the only shape that can express
    "Iran relates to crude but not to Bitcoin".
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
        instrument_eid, str(int(days)), int(limit),
    )
    related = [r["eid"] for r in rows]

    if instrument:
        seen = {str(instrument_eid)} | {str(r) for r in related}
        for m in await instrument_map.related_entity_ids(conn, instrument):
            if str(m) not in seen:
                seen.add(str(m))
                related.append(m)
    return related


async def _news_window(conn, entity_ids: list, start_sql: str, end_sql: str, args: list):
    """News clusters mentioning any related entity inside a time window."""
    if not entity_ids:
        return []
    return await conn.fetch(
        f"""
        SELECT DISTINCT {_STORY_KEY} AS cluster_id, ds.ref_id, ds.source_id,
               io.headline
        FROM domain_signals ds
        LEFT JOIN info_objects io ON io.id::text = ds.ref_id
        WHERE ds.domain = 'news'
          AND ds.entity_ids && $1::uuid[]
          AND ds.ts >= {start_sql} AND ds.ts < {end_sql}
        LIMIT 50
        """,
        entity_ids, *args,
    )


async def run(conn, *, history_days: int = 60, lookback_hours: int = 48,
              forward_hours: int = 48, z_threshold: float = 2.5,
              min_history: int = 5, min_clusters: int = 2) -> int:
    """Detect and persist market_reaction insights. Returns how many were written.

    Returns 0 until there is both market history (>= min_history days per
    instrument) and related news in the window — that is a data state, not a fault.
    """
    global LAST_RUN
    # Idempotent; keeps a fresh deploy from silently having no instrument links.
    try:
        await instrument_map.sync_seeds(conn)
    except Exception as e:
        log.warning("instrument_map seed sync failed: %s", e)

    instruments = await conn.fetch(
        """
        SELECT DISTINCT unnest(entity_ids) AS eid
        FROM domain_signals
        WHERE domain = 'market' AND ts >= now() - ($1 || ' days')::interval
        """,
        str(int(history_days)),
    )

    stats = {"instruments": len(instruments), "with_enough_history": 0,
             "significant_moves": 0, "with_related_entities": 0,
             "with_related_news": 0, "insights_written": 0}
    written = 0
    for inst in instruments:
        eid = inst["eid"]

        # Daily series of this instrument's signed move, newest last.
        rows = await conn.fetch(
            """
            SELECT (ts AT TIME ZONE 'UTC')::date AS d,
                   AVG(magnitude * direction) AS v, MAX(ts) AS last_ts
            FROM domain_signals
            WHERE domain = 'market' AND $1 = ANY(entity_ids)
              AND ts >= now() - ($2 || ' days')::interval
            GROUP BY 1 ORDER BY d
            """,
            eid, str(int(history_days)),
        )
        if len(rows) < min_history + 1:
            continue
        stats["with_enough_history"] += 1

        history = [float(r["v"] or 0.0) for r in rows[:-1]]
        latest = rows[-1]
        move = float(latest["v"] or 0.0)

        # Significance is judged against the instrument's OWN volatility: a 1% day
        # is routine for crude and extraordinary for a currency pair.
        baseline = ewma([abs(h) for h in history], 0.3)
        scale = mad([abs(h) for h in history])
        z = mad_zscore(abs(move), baseline, scale)
        if z < z_threshold or move == 0:
            continue

        direction = 1 if move > 0 else -1
        day = latest["d"]
        stats["significant_moves"] += 1
        # Name first: the seeded keyword links are keyed on the instrument's
        # display name, so it is needed to build the related set.
        instrument = (await names_for(conn, [eid]))[0]
        related = await _related_entity_ids(conn, eid, history_days,
                                            instrument=instrument)
        if not related:
            continue
        stats["with_related_entities"] += 1

        # A) news BEFORE the move, and B) news AFTER it.
        before = await _news_window(
            conn, related,
            "$2::timestamptz - ($3 || ' hours')::interval", "$2::timestamptz",
            [latest["last_ts"], str(int(lookback_hours))])
        after = await _news_window(
            conn, related,
            "$2::timestamptz", "$2::timestamptz + ($3 || ' hours')::interval",
            [latest["last_ts"], str(int(forward_hours))])

        mode, hits = ("news_then_move", before) if len(before) >= len(after) else \
                     ("move_then_news", after)
        clusters = {h["cluster_id"] for h in hits if h["cluster_id"] is not None}
        if hits:
            stats["with_related_news"] += 1
        if len(clusters) < min_clusters:
            continue

        headlines = [h["headline"] for h in hits if h["headline"]][:5]
        sources = sorted({h["source_id"] for h in hits if h["source_id"]})
        window_h = lookback_hours if mode == "news_then_move" else forward_hours

        why = (f"{instrument} {move_phrase(direction, move)} on {day} — an unusually "
               f"large move for this instrument (robust z-score {z:.1f} against its own "
               f"{history_days}-day history). {len(clusters)} related news "
               f"{'story' if len(clusters) == 1 else 'stories'} across "
               f"{len(sources)} source{'' if len(sources) == 1 else 's'} "
               f"{relation_phrase(mode)} within {window_h}h. "
               f"Observed sequence only — this is not a causal claim and not a forecast.")

        explain = {
            "why": why,
            "method": "market_move_zscore + related_news_window",
            "instrument": instrument,
            "move_pct": round(move, 3),
            "direction": direction,
            "z": round(z, 2),
            "baseline_abs_move": round(baseline, 3),
            "sequence": mode,                        # news_then_move | move_then_news
            "window_hours": window_h,
            "related_news_headlines": headlines,
            "related_news_cluster_ids": sorted(str(c) for c in clusters),
            "article_count": len(hits),
            "source_count": len(sources),
            "top_sources": sources[:5],
            # Confidence blends how unusual the move was with how much independent
            # news corroborates it — never a probability of a future move.
            "confidence": round(min(1.0, (z / 6.0) * 0.6
                                    + min(len(sources), 5) / 5.0 * 0.4), 3),
        }

        await write_insight(
            conn, type="market_reaction", entity_ids=[eid], domains=["market", "news"],
            score=round(float(z), 3), explain=explain,
            signature=f"market_reaction:{eid}:{day}:{mode}",
        )
        written += 1

    stats["insights_written"] = written
    stats["diagnosis"] = _diagnose(stats, min_history, min_clusters)
    LAST_RUN = stats
    log.info("market_reaction funnel: %s", stats)
    return written


def _diagnose(s: dict, min_history: int, min_clusters: int) -> str:
    """Name the stage the funnel stopped at. A detector returning 0 with no
    explanation is indistinguishable from a broken one."""
    if s["insights_written"]:
        return f"{s['insights_written']} insight(s) written"
    if not s["instruments"]:
        return "no market signals — run `python -m app.workers.market_signals`"
    if not s["with_enough_history"]:
        return (f"{s['instruments']} instruments present but none has {min_history + 1} "
                f"days of history yet; the MAD baseline cannot be computed until then")
    if not s["significant_moves"]:
        return (f"{s['with_enough_history']} instruments have history, none moved "
                f"unusually vs its own baseline in this window")
    if not s["with_related_entities"]:
        return ("significant moves found but no instrument has linked entities — "
                "instrument_keywords empty or unresolved; run "
                "`python -m app.workers.instrument_map --report`")
    if not s["with_related_news"]:
        return (f"{s['with_related_entities']} move(s) have linked entities but no "
                f"news about those entities landed in the window — genuine no-overlap")
    return (f"news found, but fewer than {min_clusters} distinct story clusters — "
            f"below the corroboration floor, so nothing was written")
