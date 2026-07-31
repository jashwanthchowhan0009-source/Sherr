"""
reasoning/interpretation.py — the "what this means" layer.

The reasoned card reported evidence correctly and answered nothing. A reader saw
"Sensex rose 2.46%, 99 articles, connected to Trump, confidence 22%" and had no way
to know what kind of thing they were looking at. This module supplies the missing
sentence — WHAT KIND of pattern it is, and WHAT MARKET CONDITION it reflects.

DETERMINISTIC AND TEMPLATE-FILLED. classify() reads only fields the engine already
computed (asset classes, directions, co-mover count, lag result, history) and returns
a pattern key; each key maps to a fixed template. No LLM, so no sentence can appear
that was not written here and reviewed.

INTERPRETATION IS NOT PREDICTION AND NOT ADVICE. Every template describes a market
CONDITION in the past tense or as an observed association. None names a direction to
come, an action to take, or a level to watch. The output passes through the same
runtime guard as the narrative (narrative.violates_language_rules), which now also
blocks advice vocabulary and price levels, and a violation drops the insight.

Legal framing: describing an observed market condition is commentary. Telling a reader
what to do with their money is investment advice and regulated (SEBI Investment
Advisers Regulations in India). Everything here stays firmly on the first side, and
every card carries narrative.DISCLAIMER.
"""

from __future__ import annotations

# Asset-class semantics. These are properties of the instruments themselves, not
# judgements: metals and government rates are the classes markets have historically
# moved toward in defensive periods; equities and crypto are the risk-linked ones.
DEFENSIVE_CLASSES = {"metals", "rates"}
RISK_CLASSES = {"stocks", "crypto"}
ENERGY_CLASSES = {"commodities"}
CURRENCY_CLASSES = {"forex"}

# Confidence below this is reported as "not yet an established pattern" regardless of
# which shape matched — a thin card must not read as a firm characterisation.
WEAK_CONFIDENCE = 0.30


def _classes(items: list[dict]) -> set:
    return {i.get("asset_class") for i in items if i.get("asset_class")}


def _pretty_classes(classes: set) -> str:
    """'metals, stocks' — plain words, in a stable order."""
    label = {"metals": "metals", "rates": "government rates", "stocks": "equities",
             "crypto": "crypto", "commodities": "commodities", "forex": "currencies"}
    names = sorted(label.get(c, c) for c in classes if c)
    if not names:
        return "several markets"
    if len(names) == 1:
        return names[0]
    return ", ".join(names[:-1]) + " and " + names[-1]


def classify(r: dict) -> str:
    """Return the interpretation key for a reasoned insight's computed shape.

    Order matters: the more specific structural readings are tested before the
    general ones, so a broad defensive move is not reported as merely 'several
    markets moved'.
    """
    focal = r.get("focal") or {}
    cross = r.get("cross_market") or []
    lag = r.get("lag") or {}
    hist = r.get("historical") or {}

    f_class = focal.get("asset_class")
    f_dir = int(focal.get("direction") or 0)

    up = [c for c in cross if int(c.get("direction") or 0) > 0]
    down = [c for c in cross if int(c.get("direction") or 0) < 0]
    all_classes = _classes(cross) | ({f_class} if f_class else set())

    defensive_up = [c for c in up if c.get("asset_class") in DEFENSIVE_CLASSES]
    defensive_up += ([focal] if f_class in DEFENSIVE_CLASSES and f_dir > 0 else [])
    risk_down = [c for c in down if c.get("asset_class") in RISK_CLASSES]
    risk_down += ([focal] if f_class in RISK_CLASSES and f_dir < 0 else [])
    risk_up = [c for c in up if c.get("asset_class") in RISK_CLASSES]
    risk_up += ([focal] if f_class in RISK_CLASSES and f_dir > 0 else [])

    # 1-2. Defensive/risk rotation across classes — the strongest readings.
    if defensive_up and risk_down:
        return "broad_defensive"
    if len(risk_up) >= 2 and not defensive_up:
        return "broad_risk_seeking"

    # 3. Defensive asset moving up alongside anything else.
    if f_class in DEFENSIVE_CLASSES and f_dir > 0 and cross:
        return "defensive_bid"

    # 4. Energy leading, spilling into other classes — the supply-linked shape.
    if f_class in ENERGY_CLASSES and len(all_classes) >= 2:
        return "energy_led"

    # 5. Currency and rates together.
    if f_class in CURRENCY_CLASSES and ("rates" in _classes(cross)):
        return "currency_rates"

    # 6. Co-movers split in both directions — a rotation, not a broad move.
    if up and down:
        return "divergent"

    # 7. Everything aligned. One co-mover is enough: two markets moving the same way
    # at the same time is already a shape worth naming, and requiring three left the
    # commonest real case falling through to the bland "monitoring" default.
    if len(cross) >= 1 and (not down or not up):
        return "broad_aligned"

    # 8. News measurably preceded the instrument (M2 passed its guards).
    if lag.get("passed"):
        return "news_led"

    # 9. This shape has recurred and mostly resolved the same way.
    if int(hist.get("similar_count") or 0) >= 2 and \
            int(hist.get("followed_direction") or 0) * 2 >= int(hist["similar_count"]):
        return "recurring"

    # 10. Moved on its own.
    if not cross:
        return "isolated"

    return "monitoring"


# ─── the templates ────────────────────────────────────────────────────────────
# Each is (title, body). Bodies take the fields built in interpret(). Every one
# describes a CONDITION; none names a direction to come or an action to take.
TEMPLATES: dict[str, tuple[str, str]] = {
    "broad_defensive": (
        "A broad risk-driven pattern",
        "This is a market-wide pattern rather than a story about one instrument: "
        "{n_markets} markets across {classes} moved together, with defensive assets "
        "rising while equity-linked ones fell. That combination is historically "
        "associated with periods when attention shifts toward assets treated as more "
        "stable. It describes what markets did, not why."),
    "broad_risk_seeking": (
        "A broad risk-seeking pattern",
        "Equity-linked markets moved up together across {classes}, without a matching "
        "move in defensive assets. This is the shape markets tend to show when "
        "appetite for risk is broad rather than concentrated in one name."),
    "defensive_bid": (
        "Demand for defensive assets",
        "{instrument} moved alongside {n_others} other {market_word}. A move in "
        "{classes} of this kind is historically associated with periods of heightened "
        "caution across markets. Worth noting: this is one window, not a trend."),
    "energy_led": (
        "An energy-linked repricing",
        "The move began in energy and appears alongside {classes}. A pattern where "
        "commodity moves show up across several asset classes usually reflects a "
        "shared input cost rather than company-specific news — energy feeds into "
        "transport, manufacturing and currencies at once."),
    "currency_rates": (
        "A currency and rates pattern",
        "{instrument} moved together with government rates. Currencies and rates "
        "moving as a pair tends to reflect a shift in the relative return on holding "
        "one currency over another, rather than news about any single company."),
    "divergent": (
        "A rotation, not a broad move",
        "Markets moved in opposite directions in this window — {n_up} up and "
        "{n_down} down across {classes}. This suggests money moving between asset "
        "classes rather than a single force acting on all of them. Divergence is a "
        "weaker signal than alignment and is worth reading as such."),
    "broad_aligned": (
        "A broad, aligned move",
        "{n_markets} markets across {classes} moved the same way at the same time. "
        "Alignment on this scale usually indicates a shared driver rather than "
        "{n_markets} separate stories. What that driver is, this pattern does not say."),
    "news_led": (
        "Coverage moved ahead of the market",
        "Across the available history, coverage of these entities has been observed "
        "moving {lag_days} ahead of {instrument}, with a rank correlation of {rho}. "
        "This is a timing relationship in past data — an association, not a mechanism, "
        "and not a claim about what follows."),
    "recurring": (
        "A pattern that has repeated",
        "Coverage of this shape has appeared {similar} times before, and {followed} "
        "of those were followed by a move in the same direction in {instrument}. "
        "Small samples like this describe a tendency in the record, nothing more."),
    "isolated": (
        "Isolated to one instrument",
        "{instrument} moved without matching moves elsewhere. An isolated move tends "
        "to reflect something specific to that instrument or its coverage rather than "
        "a market-wide condition — which makes it narrower, not more significant."),
    "monitoring": (
        "Not yet an established pattern",
        "The engine observed a move in {instrument} and related coverage, but the "
        "supporting evidence is thin — too few corroborating markets or too little "
        "history to characterise the shape. It is shown for transparency, and the "
        "reading may change as more data arrives."),
}


# Prepended when confidence is thin. The pattern's SHAPE — which markets moved, in
# which directions — is a fact read straight off the data and does not depend on
# confidence. What confidence measures is how well-evidenced the link to news is. So a
# weak card still names the shape (that is the reader's "so what") and is explicit that
# it is not yet established, rather than being replaced by a bland placeholder.
# Ends as its own sentence: lower-casing the body to splice it in mangled proper
# nouns ("gold moved without...") since several templates open with the instrument.
WEAK_PREFIX = "On thin evidence so far, this is not yet an established pattern. "


def interpret(r: dict) -> dict:
    """Build the interpretation block: {pattern, title, text, established}."""
    key = classify(r)
    weak = float(r.get("confidence") or 0.0) < WEAK_CONFIDENCE

    focal = r.get("focal") or {}
    cross = r.get("cross_market") or []
    lag = r.get("lag") or {}
    hist = r.get("historical") or {}

    up = [c for c in cross if int(c.get("direction") or 0) > 0]
    down = [c for c in cross if int(c.get("direction") or 0) < 0]
    all_classes = _classes(cross) | ({focal.get("asset_class")}
                                     if focal.get("asset_class") else set())
    n_markets = len(cross) + 1
    lag_days = int(lag.get("lag") or 0)

    fields = {
        "instrument": focal.get("instrument") or "this instrument",
        "classes": _pretty_classes(all_classes),
        "n_markets": n_markets,
        "n_others": len(cross),
        "market_word": "market" if len(cross) == 1 else "markets",
        "n_up": len(up),
        "n_down": len(down),
        "lag_days": ("the same day" if lag_days == 0
                     else f"about {lag_days} day{'' if lag_days == 1 else 's'}"),
        "rho": lag.get("rho"),
        "similar": int(hist.get("similar_count") or 0),
        "followed": int(hist.get("followed_direction") or 0),
    }

    title, body = TEMPLATES[key]
    text = body.format(**fields)
    if weak and key != "monitoring":
        text = WEAK_PREFIX + text
    return {"pattern": key, "title": title, "text": text,
            "established": not weak}


def attach(r: dict) -> dict:
    """Attach the interpretation to a reasoned dict, in place."""
    r["interpretation"] = interpret(r)
    return r
