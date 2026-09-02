"""analog/event_library.py — which past articles can serve as analogs.

Reads sherrbyte_app.articles (the corpus a reader can open), keeps the rows that
resolve to a known entity AND reach a market instrument, classifies each into a
closed taxonomy, and writes public.hist_events.

WHAT IS DELIBERATELY NOT HERE
=============================
No embeddings — the matcher carries no vector term (see CLAUDE.md and the
comment block in 022_event_library.sql). No sentiment model. No LLM: an article
that needs a language model to be classified is classified `other`, and `other`
never contributes class_match to a ranking. Classification is a keyword table
you can read, argue with, and correct.

THE HONEST LIMIT, STATED UP FRONT
=================================
The symbol universe here is the ~13 instruments that appear in BOTH
instrument_map.SEED (which supplies entity -> instrument edges) and
market_signals.INSTRUMENTS/CRYPTO (which supplies instrument -> ticker). It is
NOT the 57 symbols in market_ticks. An article about a mid-cap stock resolves to
entities fine but reaches no instrument, so it gets no row. That is correct
behaviour — an analog with no measurable market side is not an analog — but it
means the library is narrower than the tick coverage suggests.

Crypto carries a second limit: CoinGecko's keyless tier caps history at 365
days, so crypto analogs can never reach further back than that however deep the
news corpus grows.
"""

from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("sherbyte.analog.event_library")

# Rows scanned per backfill batch. The whole corpus is ~22k articles; this keeps
# a single statement's result set bounded on a free-tier connection.
BATCH = int(os.getenv("SHERR_I_EVENT_LIBRARY_BATCH", "500"))

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))


def _body_state():
    """body_state lives at the repo root beside main.py, not in this package."""
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    import body_state
    return body_state


# ─── the closed taxonomy ─────────────────────────────────────────────────────
# Extending this list means editing 022_event_library.sql's CHECK constraint in
# a new migration. That friction is the point: class_match is 35% of the
# matcher's weight, and a class that means different things in different rows
# makes that weight noise.
EVENT_CLASSES = (
    "earnings", "guidance_change", "regulatory_action", "leadership_change",
    "m_and_a", "supply_disruption", "geopolitical_conflict",
    "central_bank_policy", "commodity_shock", "currency_move",
    "sanctions", "default_credit", "other",
)

# Ordered most-specific first: the first class whose evidence appears wins, so
# "RBI raises repo rate" is central_bank_policy rather than regulatory_action.
# Phrases are matched as substrings against a lowercased headline + summary.
_CLASS_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("central_bank_policy", (
        "repo rate", "interest rate", "rate cut", "rate hike", "monetary policy",
        "central bank", "reserve bank", "federal reserve", "fed chair", "fomc",
        "basis points", "bps", "quantitative", "liquidity measures", "mpc")),
    ("sanctions", (
        "sanction", "embargo", "export ban", "import ban", "trade restriction",
        "blacklist", "entity list", "tariff")),
    ("geopolitical_conflict", (
        "war", "invasion", "missile", "airstrike", "ceasefire", "military",
        "troops", "border clash", "conflict", "attack on", "strait of hormuz",
        "hostilities", "peace talks")),
    ("supply_disruption", (
        "supply chain", "shortage", "disruption", "output cut", "production halt",
        "shutdown", "strike", "blockade", "port closure", "outage", "recall",
        "opec+ cut", "supply cut")),
    ("m_and_a", (
        "acquire", "acquisition", "merger", "merges", "takeover", "buyout",
        "stake sale", "divest", "spin-off", "spinoff")),
    ("earnings", (
        "quarterly results", "q1 results", "q2 results", "q3 results",
        "q4 results", "earnings", "net profit", "revenue rose", "revenue fell",
        "profit rose", "profit fell", "beats estimates", "misses estimates",
        "posts profit", "posts loss")),
    ("guidance_change", (
        "guidance", "outlook cut", "outlook raised", "forecast cut",
        "lowers estimate", "raises estimate", "downgrade", "upgrade",
        "target revised")),
    ("leadership_change", (
        "steps down", "resigns", "appointed", "new ceo", "chief executive",
        "names ceo", "board appoints", "successor", "sacked", "ousted")),
    ("default_credit", (
        "default", "bankruptcy", "insolvency", "credit rating", "downgraded to",
        "bond yield", "debt restructuring", "npa", "bad loans", "moratorium")),
    ("regulatory_action", (
        "regulator", "sebi", "cci", "antitrust", "probe", "investigation",
        "penalty", "fine of", "lawsuit", "court order", "ban on", "compliance",
        "approval granted", "licence", "license revoked")),
    ("commodity_shock", (
        "crude", "oil price", "gold price", "silver price", "natural gas",
        "commodity", "barrel", "bullion", "metal prices", "opec")),
    ("currency_move", (
        "rupee", "dollar index", "currency", "forex", "exchange rate",
        "depreciat", "appreciat", "devalu")),
)


def classify(headline: str, summary: str = "") -> str:
    """The article's event class, or 'other'.

    Deterministic and rule-based on purpose: the LLM never decides what kind of
    event something is, because that decision feeds a ranking weight.
    """
    hay = f"{headline or ''} {summary or ''}".lower()
    if not hay.strip():
        return "other"
    for klass, phrases in _CLASS_RULES:
        if any(p in hay for p in phrases):
            return klass
    return "other"


# ─── entity name -> instrument ticker ────────────────────────────────────────

def _instruments():
    """(display name -> ticker) for every instrument that has BOTH an entity
    mapping and a price series. Imported lazily: this module is loaded by the
    detector cron, which does not always have the workers package on the path.
    """
    from app.workers.market_signals import INSTRUMENTS, CRYPTO   # noqa: PLC0415
    out = {name: sym for sym, name, _ in INSTRUMENTS}
    out.update({name: coin_id for coin_id, name in CRYPTO.items()})
    return out


def symbol_index() -> dict:
    """lowercased entity/keyword -> sorted tickers it points at.

    Built by inverting instrument_map.SEED (instrument -> keywords) and then
    resolving each instrument's display name to its ticker. An instrument in
    SEED with no price series is skipped entirely rather than contributing a
    symbol Phase 3 could not measure.
    """
    from app.spie.knowledge.instrument_map import SEED             # noqa: PLC0415
    tickers = _instruments()
    index: dict[str, set] = {}
    for instrument, keywords in SEED.items():
        ticker = tickers.get(instrument)
        if not ticker:
            # In SEED but not priced — cannot support a reaction statistic.
            continue
        # The instrument's own display name counts as a keyword: an article
        # about "Brent Crude" is about Brent Crude.
        for kw in list(keywords) + [instrument]:
            kw = (kw or "").strip().lower()
            if len(kw) >= 3:
                index.setdefault(kw, set()).add(ticker)
    return {k: sorted(v) for k, v in index.items()}


def linked_symbols(headline: str, summary: str, index: dict = None) -> list:
    """Tickers this article's text reaches, via the seeded keyword edges.

    Substring matching over the same text the classifier sees. Short keys are
    already excluded by symbol_index(), which is what stops "oil" style common
    nouns pulling in every instrument.
    """
    index = symbol_index() if index is None else index
    hay = f"{headline or ''} {summary or ''}".lower()
    hits: set = set()
    for kw, syms in index.items():
        if kw in hay:
            hits.update(syms)
    return sorted(hits)


# ─── the corpus read ─────────────────────────────────────────────────────────
# Mirrors news_match._SQL's handling of published_at, and for the same reason:
# the column is TEXT (sqlite-shaped schema through pgcompat), so it is cast, and
# the regex guard stops one unparseable row aborting the whole scan.
_SCAN_SQL = """
SELECT id, headline, summary_60, full_body, source_summary,
       published_at::timestamptz AS occurred_at
  FROM sherrbyte_app.articles
 WHERE status = 'published'
   AND published_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
   AND id > $1
 ORDER BY id
 LIMIT $2
"""

_UPSERT = """
INSERT INTO hist_events (article_id, occurred_at, entity_ids, event_class,
                         linked_symbols)
VALUES ($1, $2, $3::uuid[], $4, $5::text[])
ON CONFLICT (article_id) DO UPDATE
   SET occurred_at    = EXCLUDED.occurred_at,
       entity_ids     = EXCLUDED.entity_ids,
       event_class    = EXCLUDED.event_class,
       linked_symbols = EXCLUDED.linked_symbols,
       updated_at     = now()
"""


async def _entities_for(conn, headline: str, summary: str) -> list:
    """Entity ids this article resolves to, creating nothing.

    create=False is the honesty property inherited from instrument_map: an
    article naming something the graph has never seen contributes no entity
    rather than minting an evidence-free node.
    """
    from app.spie.knowledge import entity_resolver                 # noqa: PLC0415
    seen, out = set(), []
    for raw in _candidate_mentions(headline, summary):
        key = raw.lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            eid = await entity_resolver.resolve(conn, raw, create=False)
        except Exception as e:                                     # noqa: BLE001
            log.debug("resolve(%r) failed: %s", raw, e)
            continue
        if eid and eid not in out:
            out.append(eid)
    return out


def _candidate_mentions(headline: str, summary: str) -> list:
    """Capitalised runs from the headline and summary — the cheap proper-noun
    approximation the resolver's own junk filter then judges. No NER model:
    spacy is on the do-not-import list, and the resolver is the authority on
    what counts as a real mention anyway.
    """
    import re                                                      # noqa: PLC0415
    text = f"{headline or ''}. {summary or ''}"
    runs = re.findall(r"\b[A-Z][A-Za-z&.'-]*(?:\s+[A-Z][A-Za-z&.'-]*)*", text)
    return [r.strip() for r in runs if len(r.strip()) >= 3]


async def build(conn, *, limit: int = None, batch: int = None) -> dict:
    """Populate hist_events from the corpus. Idempotent — safe to re-run.

    Returns a funnel, not just a count: when the library comes out thin, the
    funnel says which gate did it rather than leaving it to be guessed.
    """
    batch = BATCH if batch is None else int(batch)
    bs = _body_state()
    index = symbol_index()

    funnel = {"scanned": 0, "stub_skipped": 0, "no_entities": 0,
              "no_symbols": 0, "written": 0, "errors": 0}
    by_class: dict[str, int] = {}
    last_id, wrote = 0, 0

    while True:
        take = batch if limit is None else min(batch, limit - wrote)
        if take <= 0:
            break
        rows = await conn.fetch(_SCAN_SQL, last_id, take)
        if not rows:
            break
        last_id = rows[-1]["id"]

        for r in rows:
            funnel["scanned"] += 1
            headline = r["headline"] or ""
            summary = r["summary_60"] or ""

            # A placeholder is not evidence — the same rule news_match applies.
            # An analog whose text is a stub would match on nothing real.
            if not bs.row_is_healthy({
                    "full_body": r["full_body"] or "",
                    "summary_60": summary,
                    "source_summary": r["source_summary"] or ""}):
                funnel["stub_skipped"] += 1
                continue

            syms = linked_symbols(headline, summary, index)
            if not syms:
                funnel["no_symbols"] += 1
                continue

            try:
                eids = await _entities_for(conn, headline, summary)
            except Exception as e:                                 # noqa: BLE001
                log.warning("entity resolution failed for article %s: %s",
                            r["id"], e)
                funnel["errors"] += 1
                continue
            if not eids:
                funnel["no_entities"] += 1
                continue

            klass = classify(headline, summary)
            try:
                await conn.execute(_UPSERT, r["id"], r["occurred_at"],
                                   eids, klass, syms)
            except Exception as e:                                 # noqa: BLE001
                log.warning("hist_events upsert failed for article %s: %s",
                            r["id"], e)
                funnel["errors"] += 1
                continue
            funnel["written"] += 1
            by_class[klass] = by_class.get(klass, 0) + 1
            wrote += 1

        if len(rows) < take:
            break

    funnel["by_class"] = dict(sorted(by_class.items(),
                                     key=lambda kv: -kv[1]))
    funnel["diagnosis"] = _diagnose(funnel)
    log.info("[ANALOG] event library: %s", funnel)
    return funnel


def _diagnose(f: dict) -> str:
    if not f["scanned"]:
        return "no published articles with a parseable published_at"
    if not f["written"]:
        if f["stub_skipped"] >= f["scanned"] * 0.9:
            # row_is_healthy rejects three different states — placeholder, too
            # short to be a summary, and the publisher's own text — so do not
            # name only the first one and send someone down the wrong path.
            return ("almost nothing scanned is usable as evidence: the bodies "
                    "are placeholders, too short, or still the publisher's own "
                    "text. Run /admin/reprocess-bodies, then rebuild")
        if f["no_symbols"] >= f["scanned"] * 0.9:
            return ("nothing in the corpus reaches a priced instrument; the "
                    "symbol universe is only the ~13 instruments in both "
                    "instrument_map.SEED and market_signals")
        if f["no_entities"]:
            return ("articles reach instruments but resolve to no known entity "
                    "— the entity graph may be empty for this window")
        return "no usable events found"
    return f"{f['written']} event(s) written across {len(f['by_class'])} class(es)"


async def report(conn) -> dict:
    """Event counts per class, plus coverage — the Phase 1 done-when check."""
    rows = await conn.fetch(
        "SELECT event_class, COUNT(*) AS c FROM hist_events "
        "GROUP BY event_class ORDER BY c DESC")
    total = sum(r["c"] for r in rows)
    span = await conn.fetchrow(
        "SELECT MIN(occurred_at) AS first, MAX(occurred_at) AS last "
        "FROM hist_events")
    syms = await conn.fetch(
        "SELECT s AS symbol, COUNT(*) AS c "
        "FROM hist_events, unnest(linked_symbols) AS s "
        "GROUP BY s ORDER BY c DESC")
    return {
        "total_events": total,
        "by_class": {r["event_class"]: r["c"] for r in rows},
        "by_symbol": {r["symbol"]: r["c"] for r in syms},
        "first_event": str(span["first"]) if span and span["first"] else None,
        "last_event": str(span["last"]) if span and span["last"] else None,
        # Phase 3 suppresses below 5 analogs, so a symbol under that today
        # cannot yet produce a card however good the match is.
        "symbols_at_or_above_5": sorted(
            r["symbol"] for r in syms if r["c"] >= 5),
    }
