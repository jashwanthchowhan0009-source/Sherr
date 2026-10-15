"""research.py — Sherr Research: the evidence a market analyst would gather
BEFORE anything is written.

WHY THIS EXISTS
───────────────
The writer's entire input used to be one publisher page (`article_reader`) or
~700 characters per source in a cluster (`synthesis.build_prompt`). An analyst's
value is in what the publisher's page does NOT contain: what happened before,
the number next to the number, which instrument actually moved. None of that
reached the model, so the only two outcomes available to it were a paraphrase
(rejected by `originality_check`) or an invention (rejected by `writer_gate`).
That is why the cards read like a wire copy: no prompt can fix a missing input.

This module is the missing input. It is the RESEARCH half, and it spends ZERO
provider requests — every leg is a join over data this repo already produces and
then threw away:

  1. SCOPE          `significance.score_article`'s own topic reasons decide
                    whether a story is geopolitics / macro / markets /
                    energy-commodities. There is no second keyword vocabulary
                    here on purpose: two lists drift, and the one in
                    significance.py is the calibrated one.
  2. INSTRUMENTS    Resolved ONLY through `markets.SYMBOLS`, so a ticker we
                    print is always a ticker `market_ticks` can price. A ticker
                    the model typed is not evidence; this is.
  3. MOVES          `sherrbyte_app.market_ticks` — the daily close series the
                    app already stores. The move is reported against the
                    symbol's own normal day (MAD), never as a bare percentage:
                    1% is ordinary for crude and extraordinary for USD/INR.
  4. PRIOR COVERAGE Our own corpus. These rows become the Strings nodes, which
                    is the whole difference between a timeline and a thread.

WHAT IT GUARANTEES
──────────────────
* NO NETWORK, NO MODEL. The only I/O is SQL on the connection the caller owns.
* NEVER RAISES OUTWARD. Every leg is independently wrapped; a leg that fails is
  simply absent from the brief, and a brief with no legs is a valid answer that
  degrades to today's behaviour. A research stage that can take the rewrite
  down with it would be worse than no research stage.
* SEBI POSTURE UNCHANGED. Measured past moves, past tense, no probability and
  no direction claim. The prose is still written by the model from a fixed
  payload, exactly as the analog cards already require, and
  `narrative.violates_language_rules` still screens every generated string.

THE SQRT(h) TERM IS NOT OPTIONAL — see CLAUDE.md. An h-day return accumulates h
days of variance; dividing it by a one-day volatility manufactures confidence
for free (measured: 42 at h=10 against 3 at h=1 on pure noise). The same
formula is used here as in the analog engine so two surfaces cannot disagree
about what "beyond its normal range" means.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
from datetime import datetime, timedelta, timezone

import markets
import significance
import synthesis
from synthesis import parse_ts

log = logging.getLogger("sherbyte.research")


# ─── 1. scope ────────────────────────────────────────────────────────────────
# The product owner's call (2026-10-15): Research runs on geopolitics and
# markets — stocks, crypto, commodities — not on the whole corpus. macro and
# energy_commodities are inside that line (a rate decision and a crude move ARE
# the markets story); ai_tech, space_science, climate_policy and everything
# else keep today's single-call write.
SCOPE_TOPICS = frozenset({"geopolitics", "macro", "markets", "energy_commodities"})


def scope_min() -> int:
    """Significance floor for Research. Read at call time, never cached — the
    repo has already been burned by import-time config (see CLAUDE.md)."""
    return int(os.getenv("RESEARCH_MIN_SIGNIFICANCE", "55"))


def max_per_tick() -> int:
    """How many Research briefs one drain tick may write.

    Research costs the SAME one provider request a plain rewrite costs — the
    brief itself is free — so this is not a quota bound. It is a blast-radius
    bound: a new prompt should not take over the whole tick on the day it ships.
    """
    return int(os.getenv("RESEARCH_MAX_PER_TICK", "4"))


def enabled() -> bool:
    return (os.getenv("RESEARCH_ENABLED", "1") or "1").strip().lower() \
        not in ("0", "false", "no")


def in_scope(headline: str, summary: str = "", source_name: str = "",
             feed_class: str = "general", score: int = None) -> tuple:
    """(bool, reasons) — is this a Research story?

    `score` lets a caller pass the stored `articles.significance` instead of
    re-deriving it; the topics still come from the scorer because the column
    does not keep them.
    """
    try:
        computed, reasons = significance.score_article(
            headline or "", summary or "", source_name or "",
            feed_class or "general")
    except Exception as e:                                        # noqa: BLE001
        log.warning("[RESEARCH] scope scoring failed: %s", e)
        return False, {}
    effective = computed if (score is None or score < 0) else int(score)
    topics = set(reasons.get("topics") or [])
    hit = sorted(topics & SCOPE_TOPICS)
    reasons = dict(reasons)
    reasons["scope_topics"] = hit
    reasons["score"] = effective
    reasons["scope_min"] = scope_min()
    return bool(hit) and effective >= scope_min(), reasons


# ─── 2. instruments ──────────────────────────────────────────────────────────
# Built FROM markets.SYMBOLS so the resolver can only ever name an instrument the
# price store actually carries. The aliases are how a human writes the same
# instrument in a headline ("the rupee", "Brent", "bullion"); the label itself
# ("USDINR", "BRENT") is always accepted too.
_ALIASES: dict[str, tuple[str, ...]] = {
    "NIFTY":    ("nifty", "nifty 50", "nifty50"),
    "SENSEX":   ("sensex", "bse sensex"),
    "BANKNIFTY": ("bank nifty", "banknifty"),
    "NASDAQ":   ("nasdaq",),
    "SP500":    ("s&p 500", "s&p500", "sp 500"),
    "DOW":      ("dow jones", "the dow"),
    "FTSE":     ("ftse",),
    "NIKKEI":   ("nikkei",),
    "DAX":      ("dax",),
    "HANGSENG": ("hang seng", "hangseng"),
    "SHANGHAI": ("shanghai composite",),
    "KOSPI":    ("kospi",),
    "VIX":      ("vix", "volatility index"),
    "INDIAVIX": ("india vix",),
    "GOLD":     ("gold", "bullion"),
    "SILVER":   ("silver",),
    "COPPER":   ("copper",),
    "PLATINUM": ("platinum",),
    "PALLADIUM": ("palladium",),
    "USDINR":   ("rupee", "usd/inr", "usdinr", "usd-inr", "indian rupee"),
    "EURINR":   ("euro-rupee", "eur/inr"),
    "EURUSD":   ("euro", "eur/usd"),
    "GBPUSD":   ("pound", "sterling", "gbp/usd"),
    "DXY":      ("dollar index", "dxy"),
    "WTI_CRUDE": ("wti", "west texas", "us crude"),
    "BRENT":    ("brent", "brent crude"),
    "NATGAS":   ("natural gas", "nat gas", "lng prices"),
    "WHEAT":    ("wheat",),
    "COCOA":    ("cocoa",),
    "US10Y":    ("10-year treasury", "10-year yield", "us 10-year"),
    "US30Y":    ("30-year treasury",),
    "BTC":      ("bitcoin", "btc"),
    "ETH":      ("ethereum", "ether", "eth"),
    "SOL":      ("solana",),
    "XRP":      ("ripple", "xrp"),
    "DOGE":     ("dogecoin",),
    "ADA":      ("cardano",),
    "BNB":      ("binance coin", "bnb"),
    "AVAX":     ("avalanche",),
    "LINK":     ("chainlink",),
    "LTC":      ("litecoin",),
    "EXXONMOBIL": ("exxon", "exxonmobil"),
    "CHEVRON":  ("chevron",),
    "SHELL":    ("shell",),
    "BP":       ("\\bbp\\b",),
    "TOTALENERGIES": ("totalenergies", "total energies"),
    "CONOCOPHILLIPS": ("conocophillips",),
    "SCHLUMBERGER": ("schlumberger",),
}

# How the instrument is NAMED to a reader. Without this a card says "Usdinr".
DISPLAY_NAMES = {
    "USDINR": "the rupee", "EURINR": "the euro-rupee rate",
    "EURUSD": "the euro", "GBPUSD": "the pound", "DXY": "the dollar index",
    "WTI_CRUDE": "WTI crude", "BRENT": "Brent crude", "NATGAS": "natural gas",
    "SP500": "the S&P 500", "DOW": "the Dow", "NIFTY": "the Nifty",
    "SENSEX": "the Sensex", "BANKNIFTY": "the Bank Nifty", "VIX": "the VIX",
    "INDIAVIX": "the India VIX", "US10Y": "the US 10-year yield",
    "US30Y": "the US 30-year yield", "US5Y": "the US 5-year yield",
    "US13W": "the US 13-week bill", "HANGSENG": "the Hang Seng",
    "SHANGHAI": "the Shanghai Composite", "BTC": "Bitcoin",
    "ETH": "Ethereum", "SOL": "Solana", "XRP": "XRP", "DOGE": "Dogecoin",
    "ADA": "Cardano", "BNB": "BNB", "AVAX": "Avalanche", "LINK": "Chainlink",
    "LTC": "Litecoin", "POL": "Polygon", "EXXONMOBIL": "ExxonMobil",
    "TOTALENERGIES": "TotalEnergies", "CONOCOPHILLIPS": "ConocoPhillips",
    "SCHLUMBERGER": "Schlumberger", "BP": "BP", "GOLD": "gold",
    "SILVER": "silver", "COPPER": "copper", "WHEAT": "wheat", "COCOA": "cocoa",
    "NASDAQ": "the Nasdaq", "FTSE": "the FTSE 100", "NIKKEI": "the Nikkei",
    "DAX": "the DAX", "KOSPI": "the Kospi",
}


def display_name(ticker: str) -> str:
    t = (ticker or "").upper()
    return DISPLAY_NAMES.get(t) or t.replace("_", " ").title()


# "crude" / "oil prices" name the complex, not one contract. Brent is the
# international benchmark an Indian reader's import bill is priced off, so an
# unqualified mention resolves there rather than to WTI.
_GENERIC = {
    "BRENT": ("crude", "crude oil", "oil prices", "oil price"),
}


def _symbol_catalogue() -> dict:
    """label -> (yahoo/coingecko id, market_type), from markets.SYMBOLS only."""
    out: dict[str, tuple[str, str]] = {}
    for market_type, mapping in markets.SYMBOLS.items():
        for ident, label in mapping.items():
            out.setdefault(label, (ident, market_type))
    return out


def _compile_patterns():
    cat = _symbol_catalogue()
    pats = []
    for label, (_, market_type) in cat.items():
        words = list(_ALIASES.get(label, ())) + list(_GENERIC.get(label, ()))
        words.append(label.replace("_", " ").lower())
        parts = []
        for w in words:
            parts.append(w if w.startswith("\\b") else r"\b" + re.escape(w) + r"\b")
        pats.append((label, market_type, re.compile("|".join(parts), re.IGNORECASE)))
    return pats


_PATTERNS = _compile_patterns()
MAX_INSTRUMENTS = 4


def resolve_instruments(text: str, limit: int = MAX_INSTRUMENTS) -> list:
    """Every instrument NAMED in the text that the price store can price.

    Order is the order of first mention, so the lead instrument of a headline
    stays the lead instrument of the brief.
    """
    t = text or ""
    if not t.strip():
        return []
    found = []
    for label, market_type, rx in _PATTERNS:
        m = rx.search(t)
        if m:
            found.append((m.start(), label, market_type))
    found.sort()
    out, seen = [], set()
    for _, label, market_type in found:
        if label in seen:
            continue
        seen.add(label)
        out.append({"ticker": label, "name": display_name(label),
                    "market_type": market_type})
        if len(out) >= limit:
            break
    return out


# ─── 3. measured moves ───────────────────────────────────────────────────────
TICKS_TABLE = "sherrbyte_app.market_ticks"
# One quarter of closes: enough for a stable MAD, short enough that the baseline
# is the symbol's RECENT normal rather than last year's regime.
BASELINE_DAYS = int(os.getenv("RESEARCH_BASELINE_DAYS", "90"))
HORIZONS = (1, 5)
# Same bar the analog engine publishes against: below this a move is inside the
# noise the symbol produces on an ordinary week, and saying otherwise would be
# the manufactured confidence CLAUDE.md measured.
Z_NOTABLE = float(os.getenv("RESEARCH_Z_NOTABLE", "2.0"))

def _series_sql() -> str:
    """Built at call time, not at import. The table is schema-qualified for
    Postgres, and the local sqlite backend has no schemas — so the name has to
    be substitutable, which also means a test can point this at a plain table
    instead of stubbing the query."""
    return (f"SELECT price, ts FROM {TICKS_TABLE} "
            "WHERE symbol=? ORDER BY ts DESC LIMIT ?")


def _mad(values: list) -> float:
    """Median absolute deviation from the median. Robust where a standard
    deviation is not: one gap day would otherwise set the whole baseline."""
    if not values:
        return 0.0
    s = sorted(values)
    mid = s[len(s) // 2] if len(s) % 2 else (s[len(s) // 2 - 1] + s[len(s) // 2]) / 2
    devs = sorted(abs(v - mid) for v in values)
    n = len(devs)
    return devs[n // 2] if n % 2 else (devs[n // 2 - 1] + devs[n // 2]) / 2


def measure_move(conn, ticker: str) -> dict | None:
    """The symbol's latest move against its own normal day.

    Returns None — not a zero, not a guess — whenever the series cannot support
    the claim: no rows, too few rows, a flat baseline. A missing measurement is
    an honest answer; a fabricated one is the expensive kind of wrong.
    """
    try:
        rows = conn.execute(_series_sql(), (ticker, BASELINE_DAYS + 1)).fetchall()
    except Exception as e:                                        # noqa: BLE001
        log.info("[RESEARCH] tick read failed for %s: %s", ticker, e)
        return None
    closes = []
    for r in rows:                      # newest first out of SQL
        try:
            closes.append(float(r["price"]))
        except Exception:                                         # noqa: BLE001
            continue
    if len(closes) < 20:
        return None
    closes.reverse()                    # oldest -> newest
    daily = [(closes[i] / closes[i - 1] - 1.0) * 100.0
             for i in range(1, len(closes)) if closes[i - 1]]
    if len(daily) < 15:
        return None
    normal = _mad(daily)
    if normal <= 0:
        return None
    out = {"ticker": ticker, "normal_day_pct": round(normal, 3),
           "baseline_days": len(daily), "horizons": {}}
    for h in HORIZONS:
        if len(closes) <= h or not closes[-1 - h]:
            continue
        ret = (closes[-1] / closes[-1 - h] - 1.0) * 100.0
        # sqrt(h): an h-day return carries h days of variance. Dropping this
        # term is how a 10-day window scored 42 on pure noise.
        z = ret / (normal * math.sqrt(h))
        out["horizons"][str(h)] = {
            "return_pct": round(ret, 2), "z": round(z, 2),
            "beyond_normal": abs(z) >= Z_NOTABLE,
            "direction": "higher" if ret > 0 else ("lower" if ret < 0 else "flat"),
        }
    if not out["horizons"]:
        return None
    out["notable"] = any(h["beyond_normal"] for h in out["horizons"].values())
    return out


def measure_moves(conn, instruments: list) -> dict:
    """ticker -> measurement, for the instruments that HAVE one."""
    out = {}
    for inst in instruments or []:
        m = measure_move(conn, inst.get("ticker") or "")
        if m:
            out[m["ticker"]] = m
    return out


# ─── 3b. dot-to-dot: what followed the LAST times we covered this instrument ──
# This is the cross-story leg, and it is the only thing on a card that answers
# "has this happened before". CLAUDE.md names the compliant way to say what a
# probability would have said: FREQUENCY OF PAST OCCURRENCE — "in 11 comparable
# past events, crude moved beyond its normal daily range within 3 sessions in 8
# of them". Nothing here is a forecast and nothing here is a probability: it is a
# count over stories we actually published, each one openable.
#
# Two floors keep it honest, both borrowed from the analog cards rather than
# invented here:
#   * HISTORY_MIN_EVENTS mirrors AnalogCard's sample floor — below it there is no
#     frequency SENTENCE at all, only the related stories as links. Four events
#     is an anecdote; saying "3 of 4" would dress an anecdote as a base rate.
#   * "beyond normal" is the same measured bar as everywhere else (Z_NOTABLE
#     against the instrument's own MAD), so the count cannot be inflated by
#     calling an ordinary day a move.
HISTORY_DAYS = int(os.getenv("RESEARCH_HISTORY_DAYS", "400"))
HISTORY_MAX_EVENTS = int(os.getenv("RESEARCH_HISTORY_MAX", "12"))
HISTORY_MIN_EVENTS = int(os.getenv("RESEARCH_HISTORY_MIN", "5"))
_HISTORY_SCAN = 40            # rows per pattern before date/dedupe filtering

# LOWER() on both sides, because the tag is whatever case the model wrote. A
# `micro_tags LIKE '%"brent"%'` matches nothing against the stored `"Brent"` on
# Postgres, and that silent miss would read as "this instrument has no history".
_HISTORY_BY_TAG = (
    "SELECT id, headline, published_at FROM articles "
    "WHERE status<>'merged' AND ai_processed=1 "
    "AND LOWER(micro_tags) LIKE ? AND id<>? "
    "ORDER BY id DESC LIMIT ?")


def _history_patterns(ticker: str) -> list:
    """The micro_tags spellings that mean this instrument, lowercased.

    Built from the same `_ALIASES` table the resolver uses, so the two cannot
    disagree about what "Brent" means. A regex-only alias is skipped — a tag is a
    literal string, and `\bbp\b` would match none of them.
    """
    out, seen = [], set()
    for word in [ticker, display_name(ticker)] + list(_ALIASES.get(ticker, ())):
        w = str(word or "").strip().lower()
        if not w or "\\" in w:
            continue
        if w.startswith("the "):          # "the rupee" is tagged "Rupee"
            w = w[4:]
        w = w.replace("_", " ")
        if len(w) < 2 or w in seen:
            continue
        seen.add(w)
        out.append(w)
    return out[:3]


def _close_series(conn, ticker: str) -> list:
    """(date, close) oldest first, over the history window. [] on any failure."""
    try:
        rows = conn.execute(
            f"SELECT price, ts FROM {TICKS_TABLE} WHERE symbol=? "
            "ORDER BY ts DESC LIMIT ?", (ticker, HISTORY_DAYS)).fetchall()
    except Exception as e:                                        # noqa: BLE001
        log.info("[RESEARCH] history series failed for %s: %s", ticker, e)
        return []
    out = []
    for r in rows:
        ts = parse_ts(r["ts"])
        try:
            price = float(r["price"])
        except Exception:                                         # noqa: BLE001
            continue
        if ts is not None:
            out.append((ts.date(), price))
    out.sort()
    return out


def _move_after(series: list, on_date, normal: float) -> dict | None:
    """The next session's move after `on_date`, measured against normal.

    The session AFTER the story, not the one containing it: a story published at
    20:00 IST lands after the Indian close and before the US one, so "the day it
    was published" is a different day depending on the exchange. The next bar is
    unambiguous on every exchange, and it is also the honest question — what
    followed the story.
    """
    if not series or normal <= 0:
        return None
    for i in range(1, len(series)):
        if series[i][0] > on_date and series[i - 1][1]:
            ret = (series[i][1] / series[i - 1][1] - 1.0) * 100.0
            z = ret / normal
            return {"at": series[i][0].isoformat(), "return_pct": round(ret, 2),
                    "z": round(z, 2), "beyond_normal": abs(z) >= Z_NOTABLE,
                    "direction": "higher" if ret > 0 else
                                 ("lower" if ret < 0 else "flat")}
    return None


def instrument_history(conn, ticker: str, article_id: int = 0) -> dict | None:
    """What followed the last times OUR corpus covered this instrument.

    One price query and up to three bounded row scans per instrument. Returns
    None when there is nothing to say — no series, no earlier stories, or a flat
    baseline. `events` is always returned oldest first so the dot reads as a
    history; `n_beyond` is the count the frequency sentence is built from.
    """
    series = _close_series(conn, ticker)
    if len(series) < 20:
        return None
    daily = [(series[i][1] / series[i - 1][1] - 1.0) * 100.0
             for i in range(1, len(series)) if series[i - 1][1]]
    normal = _mad(daily)
    if normal <= 0:
        return None

    cutoff = (datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)).date()
    seen, cands = {article_id}, []
    for pattern in _history_patterns(ticker):
        try:
            rows = conn.execute(_HISTORY_BY_TAG,
                                (f'%"{pattern}"%', article_id or 0,
                                 _HISTORY_SCAN)).fetchall()
        except Exception as e:                                    # noqa: BLE001
            log.info("[RESEARCH] history scan failed for %s: %s", ticker, e)
            continue
        for r in rows:
            if r["id"] in seen:
                continue
            ts = parse_ts(r["published_at"])
            if ts is None or ts.date() < cutoff:
                continue
            seen.add(r["id"])
            cands.append((ts.date(), r["id"], r["headline"] or ""))

    cands.sort(reverse=True)                       # newest first while trimming
    events = []
    for on_date, rid, headline in cands[:HISTORY_MAX_EVENTS]:
        move = _move_after(series, on_date, normal)
        if not move:
            continue
        events.append({"article_id": rid, "headline": headline,
                       "at": on_date.isoformat(), "move": move})
    if not events:
        return None
    events.sort(key=lambda e: e["at"])
    beyond = [e for e in events if e["move"]["beyond_normal"]]
    return {"ticker": ticker, "events": events, "n": len(events),
            "n_beyond": len(beyond), "normal_day_pct": round(normal, 3),
            # The SENTENCE needs the sample floor; the LINKS never do.
            "frequency_publishable": len(events) >= HISTORY_MIN_EVENTS}


def instrument_histories(conn, instruments: list, article_id: int = 0) -> dict:
    out = {}
    for inst in instruments or []:
        h = instrument_history(conn, inst.get("ticker") or "", article_id)
        if h:
            out[h["ticker"]] = h
    return out


# ─── 4. prior coverage: the real Strings nodes ────────────────────────────────
PRIOR_DAYS = int(os.getenv("RESEARCH_PRIOR_DAYS", "90"))
PRIOR_MAX = int(os.getenv("RESEARCH_PRIOR_MAX", "4"))
_PRIOR_SCAN = 12          # rows fetched per tag before date/dedupe filtering

_PRIOR_BY_TAG = (
    "SELECT id, headline, published_at, source_name, micro_tags "
    "FROM articles WHERE status='published' AND ai_processed=1 "
    "AND micro_tags LIKE ? AND id<>? ORDER BY id DESC LIMIT ?")
_PRIOR_BY_STORY = (
    "SELECT id, headline, published_at, source_name, micro_tags "
    "FROM articles WHERE story_id=? AND id<>? AND status<>'merged' "
    "ORDER BY id DESC LIMIT ?")


def _tags_of(row) -> list:
    raw = synthesis._get(row, "micro_tags", "") or ""
    try:
        tags = json.loads(raw) if isinstance(raw, str) else list(raw or [])
    except Exception:                                             # noqa: BLE001
        return []
    return [str(t).strip() for t in tags if str(t).strip()][:6]


def prior_coverage(conn, row, limit: int = PRIOR_MAX) -> list:
    """Earlier rows in OUR corpus about the same entities, oldest first.

    Deliberately NOT a date predicate in SQL. `articles.published_at` is TEXT on
    some deployments and timestamptz on others (migration 018), and comparing it
    to a bind parameter is exactly the class of bug that took Phase 1 down on
    first contact with production. The scan is bounded by id instead and the
    window is applied in Python through `synthesis.parse_ts`, which already
    understands both shapes.
    """
    aid = synthesis._get(row, "id", 0) or 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=PRIOR_DAYS)
    seen, cands = {aid}, []

    def take(sql, args):
        try:
            for r in conn.execute(sql, args).fetchall():
                rid = r["id"]
                if rid in seen:
                    continue
                ts = synthesis.parse_ts(r["published_at"])
                if ts is None or ts < cutoff:
                    continue
                seen.add(rid)
                cands.append((ts, r))
        except Exception as e:                                    # noqa: BLE001
            log.info("[RESEARCH] prior-coverage read failed: %s", e)

    sid = synthesis._get(row, "story_id", 0) or 0
    if sid:
        take(_PRIOR_BY_STORY, (sid, aid, _PRIOR_SCAN))
    for tag in _tags_of(row)[:3]:
        take(_PRIOR_BY_TAG, (f'%"{tag}"%', aid, _PRIOR_SCAN))

    # Newest of the priors first while trimming (the most recent context is the
    # most useful), then flipped to oldest-first because that is the order a
    # thread reads in.
    cands.sort(key=lambda p: p[0], reverse=True)
    picked = cands[:limit]
    picked.sort(key=lambda p: p[0])
    return [{"id": r["id"], "headline": r["headline"] or "",
             "at": ts.date().isoformat(), "source": r["source_name"] or ""}
            for ts, r in picked]


# ─── 5. the brief ────────────────────────────────────────────────────────────

def build_brief(conn, row, source_text: str = "") -> dict:
    """The evidence pack for one story. Pure reads; never raises."""
    headline = synthesis._get(row, "headline", "") or ""
    blurb = synthesis._get(row, "summary_60", "") or ""
    text = f"{headline}\n{blurb}\n{(source_text or '')[:4000]}"
    brief = {"article_id": synthesis._get(row, "id", 0),
             "instruments": [], "moves": {}, "histories": {}, "prior": [],
             "legs": []}
    try:
        brief["instruments"] = resolve_instruments(text)
    except Exception as e:                                        # noqa: BLE001
        log.info("[RESEARCH] instrument resolve failed: %s", e)
    if brief["instruments"]:
        brief["moves"] = measure_moves(conn, brief["instruments"])
        brief["histories"] = instrument_histories(
            conn, brief["instruments"], synthesis._get(row, "id", 0) or 0)
    brief["prior"] = prior_coverage(conn, row)
    if brief["moves"]:
        brief["legs"].append("market_move")
    if brief.get("histories"):
        brief["legs"].append("instrument_history")
    if brief["prior"]:
        brief["legs"].append("prior_coverage")
    if brief["instruments"]:
        brief["legs"].append("instruments")
    return brief


def render_brief(brief: dict) -> str:
    """The brief as the block appended to the prompt.

    Plain lines, not JSON: the model writes better prose from a sentence than
    from a nested object, and every number here is already verified, so the
    model's job is to USE them, never to produce them.
    """
    if not brief:
        return ""
    out = ["", "=== SHERR RESEARCH BRIEF (verified; use these, invent nothing) ==="]
    prior = brief.get("prior") or []
    if prior:
        out.append("")
        out.append("OUR EARLIER COVERAGE of the same entities, oldest first. "
                   "These are real published stories — use them for context, "
                   "and do not restate them as new events:")
        for p in prior:
            out.append(f"  - {p['at']}: {p['headline']}")
    moves = brief.get("moves") or {}
    if moves:
        out.append("")
        out.append("MEASURED MARKET MOVES from our own daily price store. "
                   "'normal day' is the instrument's median absolute daily move "
                   "over the baseline, so a move 'beyond normal' is measured, "
                   "not an opinion. Past tense only, no direction claim about "
                   "the future, no price target:")
        for ticker, m in moves.items():
            bits = []
            for h, d in sorted(m["horizons"].items(), key=lambda kv: int(kv[0])):
                label = "1 session" if h == "1" else f"{h} sessions"
                tag = "beyond its normal range" if d["beyond_normal"] \
                    else "inside its normal range"
                bits.append(f"{label}: {d['return_pct']:+.2f}% ({tag})")
            out.append(f"  - {display_name(ticker)} ({ticker}): " + "; ".join(bits)
                       + f" | normal day {m['normal_day_pct']:.2f}%"
                         f" over {m['baseline_days']} sessions")
    hist = brief.get("histories") or {}
    if hist:
        out.append("")
        out.append("WHAT FOLLOWED THE LAST TIMES WE COVERED THIS INSTRUMENT, "
                   "counted over our own published stories. This is a COUNT OF "
                   "PAST OCCURRENCES, never a probability and never a forecast: "
                   "write it as a count in the past tense, exactly in the shape "
                   "given, or leave it out:")
        for ticker, h in hist.items():
            name = display_name(ticker)
            if h.get("frequency_publishable"):
                out.append(f"  - in {h['n']} earlier stories we published that "
                           f"named {name}, it moved beyond its normal daily "
                           f"range in the next session in {h['n_beyond']} of them")
            else:
                out.append(f"  - {name}: only {h['n']} earlier stories — TOO FEW "
                           f"to state a count. Do not give any number for this; "
                           f"mention the earlier coverage only if it adds a fact.")
            for e in h["events"][-3:]:
                out.append(f"      {e['at']}: {e['headline']} "
                           f"(next session {e['move']['return_pct']:+.2f}%)")

    insts = brief.get("instruments") or []
    unpriced = [i["ticker"] for i in insts if i["ticker"] not in moves]
    if unpriced:
        out.append("")
        out.append("NAMED but NOT PRICED here (do not state any move for these): "
                   + ", ".join(display_name(t) for t in unpriced))
    if len(out) <= 2:
        return ""
    out.append("=== END BRIEF ===")
    return "\n".join(out)


def has_evidence(brief: dict) -> bool:
    """Is there anything here the publisher's page did not already carry?

    A brief with no legs means Research would send the same input the plain
    rewrite sends, under a longer prompt — strictly worse. The caller falls back
    rather than spending the request.
    """
    return bool((brief or {}).get("legs"))


# ─── 6. Strings: a node must be a ROW ────────────────────────────────────────
# Today's `strings` is prose the model wrote about one source: 2-4 steps it
# inferred, none of which a reader can open. A thread's node is an article in
# this corpus — a date, a headline, an id — so every step is verifiable and
# tappable. The model may LABEL a node; it can never add one.
MAX_THREAD = 6


_PRESENT_RE = re.compile(r"present|now|today|latest", re.IGNORECASE)
_FUTURE_RE = re.compile(r"next|future|outlook|ahead|watch", re.IGNORECASE)


def _labels_up_to_present(labels: list) -> list:
    """Drop the model's steps that sit AFTER the present event.

    The prompt allows a "What's next" step, and the thread's last node is always
    the story being written — so aligning the two lists from the end would put a
    speculative future label on the present node. That is the one place a label
    could misrepresent a node, and it is cut here rather than being filtered in
    the renderer, where only one of the two surfaces would get it.
    """
    if not labels:
        return []
    for i in range(len(labels) - 1, -1, -1):
        if _PRESENT_RE.search(str(labels[i].get("stage") or "")):
            return labels[:i + 1]
    while labels and _FUTURE_RE.search(str(labels[-1].get("stage") or "")):
        labels = labels[:-1]
    return labels


def build_thread(row, brief: dict, model_strings=None) -> list:
    """Strings for one story: our earlier rows, then this story as 'Present'.

    `model_strings` is whatever the model returned. Its *stage* and *title* are
    borrowed for the matching node when present, because naming a step is a
    writing job; its details are never allowed to become nodes of their own.
    """
    prior = (brief or {}).get("prior") or []
    nodes = []
    for i, p in enumerate(prior):
        nodes.append({
            "stage": "Background" if i < len(prior) - 1 else "Escalation",
            "title": "", "detail": p["headline"], "article_id": p["id"],
            "at": p["at"], "verified": True,
        })
    nodes.append({
        "stage": "Present", "title": "",
        "detail": synthesis._get(row, "headline", "") or "",
        "article_id": synthesis._get(row, "id", 0), "at": "", "verified": True,
    })
    labels = [s for s in (model_strings or []) if isinstance(s, dict)]
    labels = _labels_up_to_present(labels)
    if labels:
        # Aligned from the END, because the model's LAST remaining step is the
        # present event — that is what `_labels_up_to_present` guarantees — so
        # the present node keeps the label written for it however many steps
        # the model returned.
        for node, lab in zip(reversed(nodes), reversed(labels)):
            title = str(lab.get("title") or "").strip()
            if title:
                node["title"] = title[:60]
            stage = str(lab.get("stage") or "").strip()
            if stage and node["stage"] != "Present":
                node["stage"] = stage[:20]
    return nodes[-MAX_THREAD:]


# ─── 7. Dots: a dot must carry an evidence leg ───────────────────────────────
# `synthesis._parse_dots` accepts whatever ticker the model typed, and nothing
# ever checked it. So the one place the app could print a hard number printed an
# adjective. A dot now declares which of two things it is:
#
#   evidence  — a named instrument that resolves in markets.SYMBOLS AND has a
#               measured move in market_ticks.
#   context   — everything else. Published, clearly labelled, never dressed as
#               a measurement. Exactly the separation AnalogCard already makes.

def verify_dots(dots: dict, brief: dict) -> dict:
    """Attach the evidence leg; drop instruments the price store cannot name."""
    if not isinstance(dots, dict) or not dots:
        return dots if isinstance(dots, dict) else {}
    moves = (brief or {}).get("moves") or {}
    hists = (brief or {}).get("histories") or {}
    catalogue = _symbol_catalogue()
    out = {}
    for section, dot in dots.items():
        if not isinstance(dot, dict):
            continue
        d = dict(dot)
        kept = []
        for inst in (d.get("instruments") or []):
            if not isinstance(inst, dict):
                continue
            ticker = str(inst.get("ticker") or "").strip().upper()
            # A model-typed ticker is accepted only if OUR price store knows it;
            # otherwise the name is kept and the unverifiable ticker is dropped,
            # which is softer than dropping a true instrument over its symbol.
            if ticker and ticker in catalogue:
                entry = {"name": str(inst.get("name") or ticker)[:60],
                         "ticker": ticker, "verified": True}
                m = moves.get(ticker)
                if m:
                    entry["move"] = m["horizons"].get("1") or \
                        next(iter(m["horizons"].values()))
                    entry["normal_day_pct"] = m["normal_day_pct"]
                h = hists.get(ticker)
                if h:
                    # DOT TO DOT: the earlier stories themselves, so a reader can
                    # open them. The links ride along whatever the sample size;
                    # only the COUNT waits for the floor.
                    entry["history"] = {
                        "n": h["n"], "n_beyond": h["n_beyond"],
                        "publishable": bool(h.get("frequency_publishable")),
                        "related": [{"article_id": e["article_id"],
                                     "headline": e["headline"], "at": e["at"],
                                     "return_pct": e["move"]["return_pct"],
                                     "beyond_normal": e["move"]["beyond_normal"]}
                                    for e in h["events"][-4:]],
                    }
                kept.append(entry)
            else:
                name = str(inst.get("name") or "").strip()
                if name:
                    kept.append({"name": name[:60], "verified": False})
        if kept:
            d["instruments"] = kept
        else:
            d.pop("instruments", None)
        # A publishable frequency is as much a measurement as today's move —
        # it is a count over past sessions, measured against the same bar. A
        # history below the sample floor is NOT: its links still show, but the
        # dot stays context, because four events is an anecdote.
        measured = any(i.get("move")
                       or (i.get("history") or {}).get("publishable")
                       for i in kept)
        d["evidence"] = "measured" if measured else "context"
        out[section] = d
    return out


# ─── 8. diagnostics ──────────────────────────────────────────────────────────
# Same posture as the synthesis diagnosis: "Research wrote nothing" has several
# causes with different fixes, and a counter per cause is what separates them.
_stats: dict = {"considered": 0, "in_scope": 0, "briefed": 0, "no_evidence": 0,
                "written": 0, "failed": 0, "legs": {}, "last_at": None,
                "last_brief": None, "skipped_budget": 0}


def note(key: str, n: int = 1) -> None:
    _stats[key] = int(_stats.get(key, 0)) + n


def note_legs(legs) -> None:
    for leg in legs or []:
        _stats["legs"][leg] = _stats["legs"].get(leg, 0) + 1


def note_brief(brief: dict) -> None:
    _stats["last_at"] = datetime.now(timezone.utc).isoformat()
    _stats["last_brief"] = {
        "article_id": (brief or {}).get("article_id"),
        "legs": (brief or {}).get("legs"),
        "instruments": [i["ticker"] for i in (brief or {}).get("instruments") or []],
        "priced": sorted(((brief or {}).get("moves") or {}).keys()),
        "prior": len((brief or {}).get("prior") or []),
    }


def stats() -> dict:
    out = dict(_stats)
    out.update({"enabled": enabled(), "scope_topics": sorted(SCOPE_TOPICS),
                "min_significance": scope_min(), "max_per_tick": max_per_tick(),
                "z_notable": Z_NOTABLE, "baseline_days": BASELINE_DAYS,
                "history_days": HISTORY_DAYS,
                "history_min_events": HISTORY_MIN_EVENTS})
    considered, written = out["considered"], out["written"]
    out["write_rate"] = round(written / considered, 3) if considered else None
    if not out["in_scope"] and considered:
        out["DIAGNOSIS"] = ("Nothing in scope. The drain's candidates are not "
                            "geopolitics/markets stories, or they score below "
                            "RESEARCH_MIN_SIGNIFICANCE.")
    elif out["no_evidence"] and not written:
        out["DIAGNOSIS"] = ("In scope but every brief was empty — no instrument "
                            "resolved and no earlier coverage matched. Check "
                            "market_ticks has rows and micro_tags are populated.")
    elif not considered:
        out["DIAGNOSIS"] = "Research has not run yet this process."
    else:
        out["DIAGNOSIS"] = "ok"
    return out
