"""
knowledge/instrument_map.py — news keywords ↔ market instruments.

The reasoning engine could not link market moves to news because the only path
from an instrument to news entities was the co-occurrence graph, and market
instruments barely co-occur with anything: they enter domain_signals as their own
entity ("WTI Crude") and news articles talk about "Iran", "OPEC" and "Reliance
Industries" instead. Nothing joined the two, so every reasoned card died at the
"no related news" step.

This module supplies the missing edge. `SEED` maps each instrument to the
news-side surface forms that are relevant to it; `sync_seeds()` writes them into
`instrument_keywords` idempotently; `related_entity_ids()` resolves them to
canonical entity ids at read time.

TWO HONESTY PROPERTIES, both deliberate:

  • Resolution NEVER creates entities (`create=False`). A keyword that has never
    appeared in the corpus resolves to nothing and contributes nothing. We do not
    mint a "Hormuz" node just because a seed mentions it — that would put an
    entity in the graph with zero evidence behind it.
  • Links are SCOPED PER INSTRUMENT. "Iran" reaches WTI Crude and Brent, not
    Bitcoin. (The previous entity_ticker_map lookup in market_reaction pulled
    every mapped entity for every instrument, which would have related everything
    to everything the moment the table had rows in it.)

Generic lowercase nouns ("oil", "crude", "energy") are included where the seed
list calls for them, but they will usually resolve to nothing: the entity
resolver's junk filter drops bare common nouns, so only the proper nouns in each
row realistically become edges. `coverage()` reports exactly which ones matched
rather than leaving that to guesswork.
"""

from __future__ import annotations

import logging

from app.spie.knowledge.entity_resolver import normalize_name, resolve_key

log = logging.getLogger("sherbyte.instrument_map")

# instrument display name (as emitted by workers/market_signals.INSTRUMENTS)
# → news-side keywords. Proper nouns first: those are what actually survive
# entity extraction and therefore what actually produces links.
SEED: dict[str, list[str]] = {
    # ─── energy ───────────────────────────────────────────────────────────────
    "WTI Crude": [
        "OPEC", "Iran", "Strait of Hormuz", "Saudi Arabia", "Russia", "Venezuela",
        "Reliance Industries", "Indian Oil", "ONGC", "Bharat Petroleum",
        "International Energy Agency", "oil", "crude", "petroleum", "energy",
    ],
    "Brent Crude": [
        "OPEC", "Iran", "Strait of Hormuz", "Saudi Arabia", "Russia", "North Sea",
        "Reliance Industries", "Indian Oil", "ONGC",
        "oil", "crude", "petroleum", "energy",
    ],
    "Natural Gas": [
        "Gazprom", "Qatar", "Russia", "GAIL", "Petronet LNG",
        "natural gas", "LNG", "pipeline", "energy",
    ],
    # ─── precious metals ──────────────────────────────────────────────────────
    "Gold": [
        "Federal Reserve", "Reserve Bank of India", "World Gold Council",
        "MCX", "Akshaya Tritiya", "Diwali",
        "gold", "bullion", "safe haven", "precious metals", "inflation",
    ],
    "Silver": [
        "Federal Reserve", "MCX", "World Silver Survey",
        "silver", "bullion", "precious metals", "industrial metals",
    ],
    # ─── currencies ───────────────────────────────────────────────────────────
    "USD/INR": [
        "Reserve Bank of India", "Federal Reserve", "Shaktikanta Das",
        "Ministry of Finance", "Nirmala Sitharaman",
        "rupee", "dollar", "forex", "currency", "remittances",
        "foreign exchange reserves", "current account deficit",
    ],
    "EUR/USD": [
        "European Central Bank", "Federal Reserve", "Christine Lagarde",
        "European Union", "Eurozone", "Germany",
        "euro", "dollar", "forex", "currency",
    ],
    # ─── equity indices ───────────────────────────────────────────────────────
    "NIFTY 50": [
        "Nifty", "National Stock Exchange", "Securities and Exchange Board of India",
        "Reserve Bank of India", "Reliance Industries", "HDFC Bank", "Infosys",
        "Tata Consultancy Services", "State Bank of India", "Tata Motors",
        "Adani Group", "Nirmala Sitharaman",
        "FII", "DII", "Indian markets", "stocks", "Dalal Street",
    ],
    "Sensex": [
        "Sensex", "Bombay Stock Exchange", "Securities and Exchange Board of India",
        "Reserve Bank of India", "Reliance Industries", "HDFC Bank", "Infosys",
        "Tata Consultancy Services", "State Bank of India",
        "FII", "DII", "Indian markets", "stocks", "Dalal Street",
    ],
    "Nasdaq": [
        "Nvidia", "Apple", "Microsoft", "Alphabet", "Amazon", "Meta", "Tesla",
        "OpenAI", "Federal Reserve", "Wall Street",
        "tech stocks", "semiconductors", "artificial intelligence",
    ],
    # ─── crypto ───────────────────────────────────────────────────────────────
    "Bitcoin": [
        "Bitcoin", "Coinbase", "Binance", "MicroStrategy",
        "Securities and Exchange Commission", "Securities and Exchange Board of India",
        "crypto", "cryptocurrency", "blockchain", "halving", "ETF",
    ],
    "Ethereum": [
        "Ethereum", "Vitalik Buterin", "Coinbase", "Binance",
        "crypto", "cryptocurrency", "blockchain", "smart contracts", "DeFi",
    ],
    # ─── rates ────────────────────────────────────────────────────────────────
    "US 10Y Yield": [
        "Federal Reserve", "Jerome Powell", "US Treasury", "Janet Yellen",
        "Reserve Bank of India",
        "bond", "yield", "treasury", "rates", "inflation", "interest rate",
    ],
}


def seed_rows() -> list[tuple[str, str, str]]:
    """Flatten SEED into (instrument, keyword, norm_keyword), de-duplicated."""
    seen: set[tuple[str, str]] = set()
    rows: list[tuple[str, str, str]] = []
    for instrument, keywords in SEED.items():
        for kw in keywords:
            norm = normalize_name(kw)
            if not norm or (instrument, norm) in seen:
                continue
            seen.add((instrument, norm))
            rows.append((instrument, kw, norm))
    return rows


async def sync_seeds(conn) -> dict:
    """Write the seed mappings idempotently. Only source='seed' rows are touched,
    so anything added by hand with source='manual' survives untouched."""
    rows = seed_rows()
    await conn.executemany(
        """
        INSERT INTO instrument_keywords (instrument, keyword, norm_keyword, source)
        VALUES ($1, $2, $3, 'seed')
        ON CONFLICT (instrument, keyword) DO UPDATE
            SET norm_keyword = EXCLUDED.norm_keyword
            WHERE instrument_keywords.source = 'seed'
        """,
        rows)
    total = await conn.fetchval("SELECT COUNT(*) FROM instrument_keywords")
    return {"seeded": len(rows), "instruments": len(SEED), "table_total": int(total or 0)}


async def keywords_for(conn, instrument: str) -> list[dict]:
    """Every keyword mapped to this instrument, from the table (not from SEED —
    the table is the source of truth so hand-added rows count)."""
    rows = await conn.fetch(
        """
        SELECT keyword, COALESCE(norm_keyword, lower(btrim(keyword))) AS norm,
               weight, source
        FROM instrument_keywords WHERE instrument = $1
        """,
        instrument)
    return [{"keyword": r["keyword"], "norm": r["norm"],
             "weight": float(r["weight"] or 1.0), "source": r["source"]} for r in rows]


def _lookup_keys(keywords: list[dict]) -> list[str]:
    """Normalized lookup keys for a keyword list, seed synonyms applied.

    resolve_key() is the PURE half of the resolver, so "RBI" collapses to
    "reserve bank of india" exactly as it does during ingestion — the same key on
    both sides is what makes the join work at all.
    """
    keys: list[str] = []
    for kw in keywords:
        norm_key, _ctype, _display = resolve_key(kw["keyword"], "MISC")
        keys.append(norm_key or kw["norm"])
    return [k for k in dict.fromkeys(keys) if k]


async def related_entity_ids(conn, instrument: str) -> list:
    """Canonical entity ids for this instrument's mapped keywords.

    A pure LOOKUP, never resolve(): resolve() bumps mention_count and writes an
    alias row on every hit, and this runs on every detector pass for every
    instrument — it would inflate the popularity counter that ranking reads and
    fill entity_aliases with rows no article ever produced. Matching only, so a
    keyword absent from the corpus contributes nothing rather than a fabricated
    node.

    Type-agnostic on purpose: "Iran" is a GPE, "OPEC" an ORG, "Jerome Powell" a
    PERSON. The seed lists keywords, not NER labels, so matching pins the
    normalized name and lets the type fall where it may.
    """
    keys = _lookup_keys(await keywords_for(conn, instrument))
    if not keys:
        return []
    rows = await conn.fetch(
        """
        SELECT id FROM entities WHERE norm_key = ANY($1::text[])
        UNION
        SELECT entity_id AS id FROM entity_aliases WHERE norm_alias = ANY($1::text[])
        """,
        keys)
    return [r["id"] for r in rows]


async def coverage(conn) -> list[dict]:
    """Per-instrument diagnostic: how many mapped keywords resolve to real
    entities, and how many news signals those entities actually appear in.

    This is what distinguishes "no mappings" from "mappings exist but the news in
    this window doesn't overlap" — the two failure modes look identical from a
    detector that just returns 0.
    """
    instruments = [r["instrument"] for r in await conn.fetch(
        "SELECT DISTINCT instrument FROM instrument_keywords ORDER BY 1")]

    report: list[dict] = []
    for name in instruments:
        kws = await keywords_for(conn, name)
        eids = await related_entity_ids(conn, name)
        matched = await conn.fetch(
            "SELECT canonical_name FROM entities WHERE id = ANY($1::uuid[]) "
            "ORDER BY mention_count DESC", eids) if eids else []
        news = await conn.fetchval(
            """
            SELECT COUNT(*) FROM domain_signals
            WHERE domain = 'news' AND entity_ids && $1::uuid[]
            """, eids) if eids else 0
        report.append({
            "instrument": name,
            "keywords": len(kws),
            "resolved_entities": len(eids),
            "matched": [m["canonical_name"] for m in matched][:10],
            "news_signals_reachable": int(news or 0),
        })
    return report
