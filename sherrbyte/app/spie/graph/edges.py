"""
graph/edges.py — the hand-authored directed causal graph.

WHY NOT LEARNED. graph/cooccurrence.py already learns associations from the
corpus and weights them by NPMI, and it is the right tool for "these two
entities keep appearing together". It cannot supply DIRECTION: nothing in a
co-occurrence count says crude drives airline costs rather than the reverse, and
90 days of daily closes cannot establish it either. Direction asserted from
domain knowledge is honest; direction inferred from this much data would be a
spurious correlation wearing a mechanism's clothes.

`direction` is the SIGN, not the arrow. The arrow is always source -> target.
  amplifies — source up tends to push target up
  dampens   — source up tends to push target down (crude -> airlines: fuel cost)

traverse() is depth-capped at 2 and cycle-safe. Two hops is as far as a
one-line explanation stays true; past that the chain is a story, not a finding.
"""

from __future__ import annotations

import logging

log = logging.getLogger("sherbyte.graph.edges")

MAX_DEPTH = 2

# ─── the seed graph ──────────────────────────────────────────────────────────
# (source, target, relation, direction, note)
# Names are display forms and match the instrument names market_signals writes,
# so an anomaly on "WTI Crude" finds its edges without a translation layer.
SEED_EDGES: list = [
    # ── Crude oil ────────────────────────────────────────────────────────────
    ("WTI Crude", "Brent Crude", "same_commodity", "amplifies", "Both track global crude; they move together"),
    ("Brent Crude", "WTI Crude", "same_commodity", "amplifies", "Both track global crude; they move together"),
    ("WTI Crude", "Oil marketing companies", "input_cost", "dampens", "Refiners buy crude; a higher barrel squeezes marketing margins"),
    ("Brent Crude", "Oil marketing companies", "input_cost", "dampens", "India prices its import basket off Brent"),
    ("Brent Crude", "Reliance Industries", "input_cost", "amplifies", "Refining spreads widen when crude rises faster than product prices lag"),
    ("WTI Crude", "Airlines", "input_cost", "dampens", "Jet fuel is the largest single airline cost"),
    ("Brent Crude", "Airlines", "input_cost", "dampens", "Jet fuel is the largest single airline cost"),
    ("WTI Crude", "Paints and chemicals", "input_cost", "dampens", "Crude derivatives are the main raw material"),
    ("Brent Crude", "USD/INR", "trade_balance", "amplifies", "India imports most of its crude; a higher barrel widens the deficit and weakens the rupee"),
    ("Brent Crude", "Inflation", "cost_push", "amplifies", "Fuel feeds directly into headline CPI"),
    ("Brent Crude", "Tyre makers", "input_cost", "dampens", "Synthetic rubber is crude-derived"),
    ("Natural Gas", "Fertiliser producers", "input_cost", "dampens", "Gas is the primary feedstock for urea"),
    ("Natural Gas", "City gas distributors", "input_cost", "dampens", "Higher input gas compresses distribution margins"),

    # ── Currency ────────────────────────────────────────────────────────────
    ("USD/INR", "IT exporters", "revenue_translation", "amplifies", "Billing is in dollars, costs are in rupees; a weaker rupee lifts reported margins"),
    ("USD/INR", "Infosys", "revenue_translation", "amplifies", "Dollar revenue translated into a weaker rupee"),
    ("USD/INR", "Tata Consultancy Services", "revenue_translation", "amplifies", "Dollar revenue translated into a weaker rupee"),
    ("USD/INR", "Pharma exporters", "revenue_translation", "amplifies", "US-facing generics earn in dollars"),
    ("USD/INR", "Importers", "input_cost", "dampens", "A weaker rupee raises the rupee cost of every import"),
    ("USD/INR", "Oil marketing companies", "input_cost", "dampens", "Crude is bought in dollars and sold in rupees"),
    ("USD/INR", "Inflation", "imported_inflation", "amplifies", "A weaker rupee raises landed import prices"),
    ("USD/INR", "Gold", "safe_haven", "amplifies", "Rupee gold tracks the dollar price times the exchange rate"),
    ("USD/INR", "Foreign investors", "flow_sensitivity", "dampens", "Currency losses erode dollar returns and slow inflows"),
    ("EUR/USD", "USD/INR", "dollar_index", "dampens", "A stronger euro is a weaker dollar, easing pressure on the rupee"),
    ("US 10Y Yield", "USD/INR", "rate_differential", "amplifies", "Higher US yields pull capital toward the dollar"),
    ("US 10Y Yield", "Foreign investors", "rate_differential", "dampens", "Emerging-market equity is less attractive when US bonds pay more"),
    ("US 10Y Yield", "Gold", "opportunity_cost", "dampens", "Gold pays no coupon; higher real yields raise the cost of holding it"),

    # ── Policy and rates ────────────────────────────────────────────────────
    ("Reserve Bank of India", "Repo rate", "policy", "amplifies", "The RBI sets the policy repo rate"),
    ("Repo rate", "Banks", "net_interest_margin", "amplifies", "Loans reprice faster than deposits, so margins widen first"),
    ("Repo rate", "HDFC Bank", "net_interest_margin", "amplifies", "Loans reprice faster than deposits"),
    ("Repo rate", "State Bank of India", "net_interest_margin", "amplifies", "Loans reprice faster than deposits"),
    ("Repo rate", "Non-bank lenders", "funding_cost", "dampens", "NBFCs borrow wholesale; a higher repo raises their cost of funds"),
    ("Repo rate", "Real estate", "affordability", "dampens", "Home-loan EMIs rise and demand cools"),
    ("Repo rate", "Automobiles", "affordability", "dampens", "Most sales are financed"),
    ("Repo rate", "Bond prices", "discount_rate", "dampens", "Yields up, prices down"),
    ("Repo rate", "Inflation", "policy_transmission", "dampens", "Tighter policy is intended to cool prices"),
    ("Inflation", "Reserve Bank of India", "policy_trigger", "amplifies", "Above-target inflation pushes the RBI toward tightening"),
    ("Inflation", "Consumer staples", "input_cost", "dampens", "Input costs rise faster than shelf prices can follow"),

    # ── Metals and industry ─────────────────────────────────────────────────
    ("Gold", "Jewellery retailers", "input_cost", "dampens", "Higher metal cost compresses volumes and margins"),
    ("Gold", "Silver", "precious_metals", "amplifies", "Precious metals move together on the same macro drivers"),
    ("Silver", "Solar manufacturers", "input_cost", "dampens", "Silver paste is a cell input"),
    ("Copper", "Cable and wire makers", "input_cost", "dampens", "Copper is the primary raw material"),
    ("Copper", "Global growth", "demand_proxy", "amplifies", "Copper demand is a widely used industrial-activity proxy"),
    ("Steel", "Automobiles", "input_cost", "dampens", "Sheet steel is a major bill-of-materials item"),
    ("Steel", "Construction", "input_cost", "dampens", "Rebar and structural steel drive project cost"),
    ("Coal", "Power producers", "input_cost", "dampens", "Thermal generation is coal-fed"),
    ("Coal", "Cement", "input_cost", "dampens", "Kilns are coal or pet-coke fired"),

    # ── Indices and flows ───────────────────────────────────────────────────
    ("NIFTY 50", "Sensex", "same_market", "amplifies", "Overlapping large-cap constituents"),
    ("Sensex", "NIFTY 50", "same_market", "amplifies", "Overlapping large-cap constituents"),
    ("NIFTY 50", "Bank Nifty", "index_weight", "amplifies", "Financials are the largest weight in the broad index"),
    ("Bank Nifty", "NIFTY 50", "index_weight", "amplifies", "Financials are the largest weight in the broad index"),
    ("Foreign investors", "NIFTY 50", "flows", "amplifies", "FPI flows are a primary marginal buyer"),
    ("Foreign investors", "USD/INR", "flows", "dampens", "Inflows are converted into rupees and support the currency"),
    ("India VIX", "NIFTY 50", "risk_appetite", "dampens", "Volatility spikes accompany drawdowns"),
    ("Nasdaq", "IT exporters", "sector_sentiment", "amplifies", "Indian IT trades with global technology sentiment"),
    ("Nasdaq", "NIFTY 50", "global_risk", "amplifies", "Global risk appetite transmits to Indian equities"),
    ("US 10Y Yield", "Banks", "yield_curve", "amplifies", "A steeper curve supports lending spreads"),

    # ── Agriculture and weather ─────────────────────────────────────────────
    ("Monsoon", "Vegetable prices", "supply", "dampens", "A good monsoon raises supply and softens prices"),
    ("Monsoon", "Rural demand", "farm_income", "amplifies", "Better harvests lift rural incomes"),
    ("Monsoon", "Fertiliser producers", "sowing_demand", "amplifies", "Sowing acreage drives fertiliser volumes"),
    ("Monsoon", "Tractor makers", "farm_income", "amplifies", "Farm income drives tractor demand"),
    ("Vegetable prices", "Inflation", "food_basket", "amplifies", "Food is the heaviest CPI component in India"),
    ("Wheat", "Food inflation", "staple", "amplifies", "Wheat is a staple in the CPI basket"),
    ("Wheat", "Bakery and FMCG", "input_cost", "dampens", "Flour is a direct input"),
    ("Rural demand", "Two-wheeler makers", "consumption", "amplifies", "Rural buyers are the volume base"),
    ("Rural demand", "Consumer staples", "consumption", "amplifies", "Rural India is the volume market for staples"),

    # ── Crypto ──────────────────────────────────────────────────────────────
    ("Bitcoin", "Ethereum", "crypto_beta", "amplifies", "Majors move together on the same risk appetite"),
    ("Bitcoin", "Crypto exchanges", "volumes", "amplifies", "Volatility and price drive trading volume"),
    ("US 10Y Yield", "Bitcoin", "liquidity", "dampens", "Tighter dollar liquidity weighs on risk assets"),
]


def build_index(edges: list = None) -> dict:
    """{source: [edge dict, ...]} for traversal without a database."""
    idx: dict = {}
    for src, tgt, rel, direction, note in (edges or SEED_EDGES):
        idx.setdefault(src, []).append(
            {"target": tgt, "relation": rel, "direction": direction, "note": note})
    return idx


_INDEX = build_index()


def traverse(entity: str, depth: int = 2, index: dict = None) -> list:
    """Entities downstream of `entity`, with the path taken.

    Breadth-first, cycle-safe, and capped at MAX_DEPTH — several seeded pairs
    are deliberately mutual (NIFTY 50 <-> Sensex), so an uncapped walk would not
    terminate. Each result carries the edges traversed, because the path IS the
    explanation: "crude -> USD/INR -> IT exporters" is the sentence a card wants.
    """
    idx = _INDEX if index is None else index
    depth = max(0, min(int(depth), MAX_DEPTH))
    if not entity or depth == 0:
        return []

    out, seen = [], {entity}
    frontier = [(entity, [])]
    for _ in range(depth):
        nxt = []
        for node, path in frontier:
            for e in idx.get(node, []):
                tgt = e["target"]
                if tgt in seen:
                    continue
                seen.add(tgt)
                hop = dict(e, source=node)
                out.append({"entity": tgt, "hops": len(path) + 1,
                            "path": path + [hop]})
                nxt.append((tgt, path + [hop]))
        frontier = nxt
        if not frontier:
            break
    return out


def downstream_names(entity: str, depth: int = 2) -> list:
    return [r["entity"] for r in traverse(entity, depth)]


def describe_path(result: dict) -> str:
    """One line for a card: "Brent Crude -> USD/INR -> IT exporters"."""
    hops = result.get("path") or []
    if not hops:
        return result.get("entity", "")
    return " -> ".join([hops[0]["source"]] + [h["target"] for h in hops])


# ─── persistence ─────────────────────────────────────────────────────────────
async def sync_seeds(conn) -> int:
    """Write SEED_EDGES into entity_edges. Idempotent; safe on every boot."""
    n = 0
    for src, tgt, rel, direction, note in SEED_EDGES:
        await conn.execute(
            """
            INSERT INTO entity_edges (source_entity, target_entity, relation,
                                      direction, note)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_entity, target_entity, relation)
            DO UPDATE SET direction = EXCLUDED.direction, note = EXCLUDED.note
            """, src, tgt, rel, direction, note)
        n += 1
    log.info("entity_edges seeded: %d", n)
    return n


async def load(conn) -> dict:
    """The index from the table, so hand edits in SQL take effect without a deploy."""
    rows = await conn.fetch(
        "SELECT source_entity, target_entity, relation, direction, note FROM entity_edges")
    if not rows:
        return build_index()
    return build_index([(r["source_entity"], r["target_entity"], r["relation"],
                         r["direction"], r["note"]) for r in rows])
