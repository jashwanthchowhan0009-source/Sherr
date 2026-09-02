"""analog/cards.py — the two things the analog engine can put on screen.

TWO CARD TYPES, AND THE SECOND EXISTS BECAUSE THE FIRST IS OFTEN SILENT
======================================================================
AnalogCard        aggregate evidence: "in N comparable past events, X moved
                  beyond its normal range within h sessions in M of them".
                  Suppressed below MIN_ANALOGS, and suppressed again if it
                  cannot clear the measured noise floor.

ObservationCard   ONE event, one instrument, one measurement: "after this
                  article, X moved N sigma within 3 sessions". No aggregate, no
                  n_analogs floor, no suppression. This is what renders when the
                  analog engine has nothing to say, so the product is never
                  blank — and it is honest, because it claims nothing beyond
                  the single measurement it reports.

An observation is NOT a weaker analog. It is a different claim: an analog says
"this has happened before and here is how often"; an observation says only
"here is what happened after this one article". Never let the second be read as
the first — the wording below is chosen to keep them apart.

EVERY STRING HERE IS BUILT BY TEMPLATE, NOT BY A MODEL. The LLM is not called
from this module at all. The math decides what is true; these functions decide
how to say it; and everything they produce passes the runtime forbidden-word
blocker before it can reach a reader.

SEBI POSTURE, RESTATED WHERE IT IS ENFORCED
===========================================
Past tense and conditional only. signal_strength is a 0-100 RANKING integer,
never a confidence and never rendered as a percentage. Frequency of past
occurrence is how this engine says what a probability would have said.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, asdict

from app.spie.analog import calibration, reaction

log = logging.getLogger("sherbyte.analog.cards")

# Horizon used for the single-event card. Three sessions is long enough for a
# reaction to show and short enough that the article is still the plausible
# occasion for it.
OBSERVATION_HORIZON = int(os.getenv("SHERR_I_OBSERVATION_HORIZON", "3"))

# An observation is only worth showing when the move was actually notable
# against that instrument's own normal range. Below this it is a quiet day and
# saying anything about it would manufacture significance.
OBSERVATION_MIN_Z = float(os.getenv("SHERR_I_OBSERVATION_MIN_Z", "1.5"))


# ─── the compliance blocker, applied here rather than trusted ───────────────

def check_language(text: str, entity_names=None) -> tuple:
    """(ok, offending_phrases) for any string bound for a reader.

    Delegates to reasoning/narrative.violates_language_rules — the blocker the
    rest of the engine already uses. ONE blocklist, not a second copy: a second
    copy would drift, and drift here is a compliance failure rather than a
    style one.

    entity_names are masked before scanning, because real names collide with
    the list (Target, Rally, Will) and dropping a valid card because a company
    is named Target is a silent failure.
    """
    try:
        from app.spie.reasoning import narrative                  # noqa: PLC0415
    except Exception as e:                                        # noqa: BLE001
        # FAIL CLOSED. If the blocker cannot be loaded we do not get to decide
        # the text was probably fine.
        log.error("language blocker unavailable: %s", e)
        return False, ["blocker-unavailable"]
    hits = narrative.violates_language_rules(text or "", entity_names or [])
    return (not hits), hits


# ─── the cards ──────────────────────────────────────────────────────────────

@dataclass
class AnalogCard:
    """Aggregate evidence over comparable past events."""
    kind: str = "analog"
    symbol: str = ""
    display_name: str = ""
    event_class: str = ""
    horizon_days: int = 0

    n_analogs: int = 0
    n_exceeded: int = 0
    sign_agreement: float = 0.0
    median_abs_z: float = 0.0
    dispersion: float = 0.0

    signal_strength: int = 0
    # Shipped on EVERY card. A score without the bar it has to clear is not
    # auditable, and the reader cannot tell 11 from 60 without it.
    noise_floor: int = 0
    clears_noise: bool = False

    headline: str = ""
    detail: str = ""
    analog_refs: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class ObservationCard:
    """One article, one instrument, one measured move. Never suppressed."""
    kind: str = "observation"
    symbol: str = ""
    display_name: str = ""
    event_class: str = ""
    horizon_days: int = 0

    z: float = 0.0
    move_pct: float = 0.0
    article_id: int = 0
    occurred_at: str = ""

    headline: str = ""
    detail: str = ""

    def as_dict(self) -> dict:
        return asdict(self)


# ─── wording ────────────────────────────────────────────────────────────────
# Past tense throughout. No word about what comes next appears in any template,
# because there is no sentence about the future this engine is entitled to
# write.

def _sessions(h: int) -> str:
    return "one session" if h == 1 else f"{h} sessions"


def analog_text(row: dict, display: str) -> tuple:
    """(headline, detail) for an aggregate card, from stored statistics only."""
    n, exc = row["n_analogs"], row["n_exceeded"]
    h = row["horizon_days"]
    klass = (row["event_class"] or "other").replace("_", " ")

    headline = (f"{display}: in {n} comparable past {klass} events, "
                f"it moved beyond its normal daily range within {_sessions(h)} "
                f"in {exc} of them")

    agreement = row["sign_agreement"]
    direction = ("in the same direction in most of them"
                 if agreement >= 0.7 else
                 "with no consistent direction across them")
    detail = (f"Median move {row['median_abs_z']:.1f}x that instrument's normal "
              f"daily range, {direction}. Signal strength {row['signal_strength']} "
              f"against a noise floor of {row['noise_floor']} at this horizon — "
              + ("above what unrelated data reaches."
                 if row["signal_strength"] > row["noise_floor"]
                 else "at or below what unrelated data reaches, so this is "
                      "reported as context rather than as evidence."))
    return headline, detail


def observation_text(symbol_display: str, z: float, move_pct: float,
                     h: int) -> tuple:
    """(headline, detail) for a single measured move.

    Deliberately narrower language than the analog card: it describes one
    event and makes no claim about frequency, because there is no sample here
    to have a frequency.
    """
    direction = "rose" if move_pct > 0 else "fell"
    headline = (f"{symbol_display} {direction} {abs(move_pct):.1f}% within "
                f"{_sessions(h)} of this article")
    detail = (f"That is {abs(z):.1f}x the instrument's normal daily range at "
              f"the time, measured against the 60 sessions before the article. "
              f"This is a single observation, not a pattern: no comparable past "
              f"events were counted for it.")
    return headline, detail


# ─── building ───────────────────────────────────────────────────────────────

def _display(symbol: str) -> str:
    """Ticker -> the name a reader has seen. 'CL=F rose 8%' means nothing."""
    try:
        from app.workers.market_signals import INSTRUMENTS, CRYPTO  # noqa: PLC0415
        names = {sym: name for sym, name, _ in INSTRUMENTS}
        names.update({cid: name for cid, name in CRYPTO.items()})
        return names.get(symbol, symbol)
    except Exception:                                             # noqa: BLE001
        return symbol


def _vet(card) -> bool:
    """Every reader-facing string on the card passes the blocker, or it is
    dropped. A card that cannot be phrased compliantly is not shown."""
    # The instrument's display name is masked: it is a quoted fact, not
    # something the template asserts, and real names collide with the blocklist
    # ("Target", "Rally"). Every word the template itself contributes is still
    # checked.
    names = [getattr(card, "display_name", "") or ""]
    for fieldname in ("headline", "detail"):
        text = getattr(card, fieldname, "") or ""
        ok, bad = check_language(text, names)
        if not ok:
            log.error("[ANALOG] card dropped — %s contains blocked word(s) %s: %r",
                      fieldname, bad, text[:120])
            return False
    return True


_ANALOG_SQL = """
SELECT symbol, event_class, horizon_days, n_analogs, n_exceeded,
       sign_agreement, median_abs_z, dispersion, recency_weight,
       signal_strength, noise_floor, breakdown
  FROM analog_reactions
 WHERE ($1::text[] IS NULL OR symbol = ANY($1::text[]))
   AND ($2::int IS NULL OR horizon_days = $2::int)
   AND signal_strength >= $3::int
 ORDER BY signal_strength DESC, n_analogs DESC
 LIMIT $4::int
"""


async def analog_cards(conn, *, symbols=None, horizon=None,
                       min_strength: int = 0, limit: int = 25,
                       only_above_noise: bool = False) -> list:
    """Aggregate cards from the stored reaction statistics.

    `only_above_noise` is the honest default for a reader-facing surface, and
    off by default here so an operator can still inspect what did not clear.
    """
    rows = await conn.fetch(_ANALOG_SQL, list(symbols) if symbols else None,
                            horizon, int(min_strength), int(limit))
    out = []
    for r in rows:
        d = dict(r)
        clears = d["signal_strength"] > d["noise_floor"]
        if only_above_noise and not clears:
            continue
        display = _display(d["symbol"])
        headline, detail = analog_text(d, display)
        refs = d.get("breakdown") or []
        if isinstance(refs, str):
            import json                                            # noqa: PLC0415
            try:
                refs = json.loads(refs)
            except Exception:                                      # noqa: BLE001
                refs = []
        card = AnalogCard(
            symbol=d["symbol"], display_name=display,
            event_class=d["event_class"], horizon_days=d["horizon_days"],
            n_analogs=d["n_analogs"], n_exceeded=d["n_exceeded"],
            sign_agreement=round(d["sign_agreement"], 3),
            median_abs_z=round(d["median_abs_z"], 2),
            dispersion=round(d["dispersion"], 2),
            signal_strength=d["signal_strength"],
            noise_floor=d["noise_floor"], clears_noise=clears,
            headline=headline, detail=detail,
            analog_refs=[{"event_id": b.get("event_id"),
                          "occurred_at": b.get("occurred_at"),
                          "z": b.get("z")} for b in refs[:10]])
        if _vet(card):
            out.append(card)
    return out


_RECENT_EVENTS_SQL = """
SELECT event_id, article_id, occurred_at, event_class, linked_symbols
  FROM hist_events
 WHERE ($1::text[] IS NULL OR linked_symbols && $1::text[])
 ORDER BY occurred_at DESC
 LIMIT $2::int
"""

_SERIES_SQL = """
SELECT ts, price FROM sherrbyte_app.market_ticks
 WHERE symbol = $1 ORDER BY ts
"""


async def observation_cards(conn, *, symbols=None, limit: int = 25,
                            horizon: int = None, min_z: float = None) -> list:
    """Single-event cards. NEVER suppressed by a sample-size floor.

    This is the surface that keeps the product from going blank: it needs one
    event and one price series, not five comparable events. It still refuses to
    report a quiet day as anything — min_z is a significance bar, not a sample
    bar, and dropping it would mean narrating noise.
    """
    h = OBSERVATION_HORIZON if horizon is None else int(horizon)
    floor = OBSERVATION_MIN_Z if min_z is None else float(min_z)

    events = await conn.fetch(_RECENT_EVENTS_SQL,
                              list(symbols) if symbols else None,
                              int(limit) * 8)
    series_cache: dict = {}
    out = []
    for e in events:
        for sym in (e["linked_symbols"] or []):
            if symbols and sym not in set(symbols):
                continue
            if sym not in series_cache:
                rows = await conn.fetch(_SERIES_SQL, sym)
                series_cache[sym] = [(r["ts"], float(r["price"])) for r in rows
                                     if r["price"] is not None]
            cell = reaction.measure(series_cache[sym], e["occurred_at"], h)
            if not cell.get("ok") or abs(cell["z"]) < floor:
                continue
            display = _display(sym)
            move_pct = (pow(2.718281828459045, cell["r"]) - 1.0) * 100.0
            headline, detail = observation_text(display, cell["z"], move_pct, h)
            card = ObservationCard(
                symbol=sym, display_name=display,
                event_class=e["event_class"], horizon_days=h,
                z=round(cell["z"], 2), move_pct=round(move_pct, 2),
                article_id=e["article_id"], occurred_at=str(e["occurred_at"]),
                headline=headline, detail=detail)
            if _vet(card):
                out.append(card)
            if len(out) >= limit:
                return out
    return out


async def watchlist_symbols(conn, min_score: float = 0.0) -> list:
    """Instruments reachable from the pairs on the engine's watchlist.

    NOTE ON WHAT 019_watchlist.sql ACTUALLY IS. It is not a per-user watchlist.
    Its columns are (entity_a, entity_b, kind, score, npmi) — the emergence
    detector's PARKED ENTITY PAIRS: connections it saw but judged not yet novel
    or strong enough to publish. There is no user_id in it, so there is no
    per-user join to make.

    What it does support, and what this returns, is "instruments connected to
    something the engine has already flagged as interesting". That is a real
    filter and a useful one; it is just not personalisation. Per-user
    personalisation needs a user->symbol table that does not exist yet, and
    inventing one here would be a schema decision smuggled in as a card feature.

    The endpoint still takes an explicit `symbols` list, which is the seam a
    real user watchlist plugs into the day one exists.
    """
    try:
        rows = await conn.fetch(
            "SELECT DISTINCT e.canonical_name FROM watchlist w "
            "JOIN entities e ON e.id IN (w.entity_a, w.entity_b) "
            "WHERE COALESCE(w.score, 0) >= $1", float(min_score))
    except Exception as e:                                        # noqa: BLE001
        log.warning("watchlist lookup failed: %s", e)
        return []
    if not rows:
        return []

    names = {(r["canonical_name"] or "").lower() for r in rows}
    try:
        from app.spie.analog.event_library import symbol_index    # noqa: PLC0415
        index = symbol_index()
    except Exception as e:                                        # noqa: BLE001
        log.warning("symbol index unavailable: %s", e)
        return []
    out: set = set()
    for kw, syms in index.items():
        if kw in names:
            out.update(syms)
    return sorted(out)


async def build(conn, *, symbols=None, horizon=None, limit: int = 25,
                use_watchlist: bool = False) -> dict:
    """Everything the analogs endpoint serves, with a funnel.

    Analog cards first — they are the stronger claim. Observation cards fill in
    when there are none, so the surface is never empty while the library is
    still accumulating.
    """
    # An empty watchlist means "no preference expressed", not "interested in
    # nothing" — so it widens to everything rather than returning a blank page.
    if use_watchlist:
        watch = await watchlist_symbols(conn)
        if watch:
            symbols = sorted(set(watch) & set(symbols)) if symbols else watch

    analogs = await analog_cards(conn, symbols=symbols, horizon=horizon,
                                 limit=limit)
    above = [c for c in analogs if c.clears_noise]

    observations = []
    if not above:
        observations = await observation_cards(conn, symbols=symbols,
                                               limit=limit, horizon=horizon)

    return {
        "analogs": [c.as_dict() for c in analogs],
        "observations": [c.as_dict() for c in observations],
        "counts": {"analogs": len(analogs), "analogs_above_noise": len(above),
                   "observations": len(observations)},
        "noise_floor_by_horizon": dict(calibration.NOISE_FLOOR),
        "watchlist_filtered": bool(use_watchlist and symbols),
    }
