"""
discovery/pipeline.py — anomaly -> graph -> news -> card, in that order.

The whole architecture rule in one function: run() calls the LLM only for
symbols that already cleared a statistical test AND found real corroborating
articles. Every other symbol costs zero LLM calls, and the reason it produced
nothing is recorded in the funnel so an empty page is explainable.
"""

from __future__ import annotations

import logging

from app.spie.discovery import news_match, tick_anomaly
from app.spie.graph import edges as graph_edges
from app.spie.reasoning import card as card_mod

log = logging.getLogger("sherbyte.pipeline")

LAST_RUN: dict = {}


async def run(conn, *, days: int = 200, limit: int = 12,
              min_articles: int = None) -> dict:
    """Returns {"cards": [...], "funnel": {...}}. Empty cards is normal."""
    min_articles = news_match.MIN_ARTICLES if min_articles is None else min_articles
    card_mod.reset_counters()

    try:
        await graph_edges.sync_seeds(conn)
    except Exception as e:
        log.warning("entity_edges seed sync failed: %s", e)
    index = await graph_edges.load(conn)

    anomalies = await tick_anomaly.scan(conn, days=days)
    funnel = {
        "symbols_scanned": 0, "anomalies": len(anomalies),
        "with_graph_paths": 0, "with_enough_articles": 0,
        "cards": 0, "skipped_no_articles": 0, "skipped_llm_failed": 0,
    }
    cov = await tick_anomaly.coverage(conn, days=days)
    funnel["symbols_scanned"] = cov["symbols_total"]
    funnel["symbols_scoreable"] = cov["symbols_scoreable"]

    cards = []
    for a in anomalies[:limit]:
        # The instrument's display name is what the graph and the corpus use;
        # the raw ticker ("CL=F") appears in neither.
        name = INSTRUMENT_NAMES.get(a.symbol, a.symbol)
        reach = graph_edges.traverse(name, 2, index)
        if reach:
            funnel["with_graph_paths"] += 1
        entities = [name] + [r["entity"] for r in reach]
        paths = [graph_edges.describe_path(r) for r in reach]

        articles = await news_match.match(conn, entities[:12], a.ts)
        if len(articles) < min_articles:
            funnel["skipped_no_articles"] += 1
            continue
        funnel["with_enough_articles"] += 1

        card = await card_mod.build(a, articles, paths, entities[:8],
                                    min_articles=min_articles, display_name=name)
        if card is None:
            funnel["skipped_llm_failed"] += 1
            continue
        cards.append({"anomaly": a.as_dict(), "card": card.model_dump(mode="json")})
        funnel["cards"] += 1

    funnel["llm_calls"] = dict(card_mod.LLM_CALLS)
    LAST_RUN.clear()
    LAST_RUN.update(funnel)
    log.info("sherr-i pipeline: %s", funnel)
    return {"cards": cards, "funnel": funnel}


# Ticker -> the display name the graph and the article corpus speak. Mirrors
# workers/market_signals.INSTRUMENTS, which is where these names come from.
INSTRUMENT_NAMES = {
    "^NSEI": "NIFTY 50", "^BSESN": "Sensex", "^IXIC": "Nasdaq",
    "^NSEBANK": "Bank Nifty", "^INDIAVIX": "India VIX",
    "GC=F": "Gold", "SI=F": "Silver", "HG=F": "Copper",
    "CL=F": "WTI Crude", "BZ=F": "Brent Crude", "NG=F": "Natural Gas",
    "ZW=F": "Wheat", "USDINR=X": "USD/INR", "EURUSD=X": "EUR/USD",
    "^TNX": "US 10Y Yield", "bitcoin": "Bitcoin", "ethereum": "Ethereum",
}
