"""
decision/rules.py — cross-domain chain evaluation (Sherr-I Decision Engine, Task 5).

Rules live in the chain_rules table (rules-as-data, weights-as-data). A chain fires
for an anchor entity when signals matching EVERY condition exist within the window
and all share that canonical entity_id (post entity-resolution). Confidence is a
log-odds aggregation: sigmoid(bias + Σ weight_i · credibility_i).

Pure core (DB-free, unit-testable):
    direction_matches / signal_matches_condition / chain_matches / sigmoid / chain_confidence
Async:
    run() — evaluate all enabled rules over recent domain_signals → cross_domain_chain insights.
"""

from __future__ import annotations

import json
import logging
import math

from app.spie.discovery.base import write_insight, names_for

log = logging.getLogger("sherbyte.decision")


def _as_json(value, default):
    """Normalize a JSONB column into a Python object.

    asyncpg hands JSONB back as a `str` unless a json codec is registered, and a
    seeded value can itself be a JSON *string* (double-encoded). Decode until we
    get a real list/dict, then fall back to `default`."""
    for _ in range(2):                     # at most one extra unwrap
        if isinstance(value, (list, dict)):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "replace")
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                return default
        else:
            break
    return value if isinstance(value, (list, dict)) else default


def normalize_conditions(raw) -> list[dict]:
    """Turn whatever `conditions_json` holds into a clean list of condition dicts.

    Tolerates every shape we've actually seen or could see from the DB/seeds:
      • a real list of dicts                        → used as-is
      • the whole value as a JSON string (asyncpg)  → decoded, then per element
      • a single condition dict (not wrapped)       → wrapped into a list
      • an element that is a JSON string            → decoded to a dict
      • an element that is a bare domain name       → {"domain": <name>, "direction": 0}
        (direction 0 = wildcard, so a bare domain means "any signal in this domain")
      • anything else                               → dropped

    Guarantees every returned element is a dict, so `.get()` / `c["domain"]` at the
    call site can never raise AttributeError.
    """
    parsed = _as_json(raw, [])
    if isinstance(parsed, dict):            # a single unwrapped condition
        parsed = [parsed]
    if not isinstance(parsed, list):
        return []

    out: list[dict] = []
    for c in parsed:
        if isinstance(c, (bytes, bytearray)):
            c = c.decode("utf-8", "replace")
        if isinstance(c, str):
            s = c.strip()
            if not s:
                continue
            decoded = _as_json(s, None)     # element may itself be JSON
            if isinstance(decoded, dict):
                c = decoded
            else:
                # A bare domain name, e.g. ["weather", "news"].
                c = {"domain": s, "direction": 0}
        if isinstance(c, dict):
            if c.get("domain"):
                out.append(c)
    return out


# ─── Pure matching + scoring ──────────────────────────────────────────────────
def direction_matches(cond_dir: int, sig_dir: int, strict: bool = False) -> bool:
    """A condition direction of 0 is a WILDCARD (any signal direction) unless the
    rule marks it strict; otherwise the signal must move in the required direction."""
    if cond_dir == 0 and not strict:
        return True
    return int(sig_dir) == int(cond_dir)


def signal_matches_condition(cond: dict, sig: dict) -> bool:
    if cond.get("domain") and sig.get("domain") != cond["domain"]:
        return False
    return direction_matches(int(cond.get("direction", 0)),
                             int(sig.get("direction", 0)),
                             bool(cond.get("strict", False)))


def chain_matches(conditions: list, signals: list, anchor) -> tuple[bool, list]:
    """True iff every condition has a matching signal that also contains `anchor`
    (canonical entity overlap). Returns (matched, evidence) where evidence[i] is a
    matching signal for condition i. `signals[*].entity_ids` is a set of str ids."""
    evidence = []
    for cond in conditions:
        found = None
        for sig in signals:
            if anchor in sig["entity_ids"] and signal_matches_condition(cond, sig):
                found = sig
                break
        if found is None:
            return False, []
        evidence.append(found)
    return True, evidence


def sigmoid(x: float) -> float:
    if x <= -60:
        return 0.0
    if x >= 60:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def chain_confidence(weights: dict, evidence: list) -> float:
    """Log-odds: prior bias + each matched condition's weight scaled by the evidence
    source's credibility, squashed through a sigmoid → calibrated 0..1 confidence."""
    weights = weights or {}
    total = float(weights.get("_bias", 0.0))
    for i, sig in enumerate(evidence):
        w = float(weights.get(str(i), weights.get(i, 1.0)))
        cred = float(sig.get("credibility", 1.0) or 1.0)
        total += w * cred
    return sigmoid(total)


# ─── Async evaluation ─────────────────────────────────────────────────────────
async def run(conn, *, default_window_hours: int = 72) -> int:
    """Evaluate all enabled chain rules over recent domain_signals. Returns the
    number of cross_domain_chain insights written (idempotent per rule/anchor/day)."""
    rules = await conn.fetch(
        "SELECT name, conditions_json, weights_json, window_hours FROM chain_rules WHERE enabled = TRUE"
    )
    if not rules:
        return 0
    today = await conn.fetchval("SELECT CURRENT_DATE")

    written = 0
    for rule in rules:
        # asyncpg returns JSONB as a str unless a codec is registered, and a seed
        # can store it as a JSON string — parse defensively either way.
        conds = normalize_conditions(rule["conditions_json"])
        if not conds:
            log.warning("rule %s: no usable conditions in conditions_json (%r) — skipped",
                        rule["name"], rule["conditions_json"])
            continue
        weights = _as_json(rule["weights_json"], {})
        if not isinstance(weights, dict):
            weights = {}
        window = int(rule["window_hours"] or default_window_hours)
        # Every element is a dict by construction here, so .get() is always safe.
        domains = list({c["domain"] for c in conds if c.get("domain")})
        if not domains:
            log.warning("rule %s: conditions carry no domain — skipped", rule["name"])
            continue

        sigs = await conn.fetch(
            "SELECT entity_ids, domain, direction, source_id, credibility "
            "FROM domain_signals "
            "WHERE domain = ANY($1::text[]) AND ts >= now() - ($2 || ' hours')::interval",
            domains, str(window),
        )
        signals = [{
            "entity_ids": {str(e) for e in (s["entity_ids"] or [])},
            "domain": s["domain"], "direction": s["direction"],
            "credibility": s["credibility"], "source_id": s["source_id"],
        } for s in sigs]

        anchors: set = set()
        for s in signals:
            anchors |= s["entity_ids"]

        for anchor in anchors:
            matched, evidence = chain_matches(conds, signals, anchor)
            if not matched:
                continue
            conf = chain_confidence(weights, evidence)
            names = await names_for(conn, [anchor])
            ev_domains = [e["domain"] for e in evidence]
            sources = sorted({e["source_id"] for e in evidence if e["source_id"]})
            why = (f"Cross-domain chain '{rule['name']}' observed around {names[0]}: "
                   f"linked signals across {', '.join(ev_domains)} within {window}h "
                   f"sharing this entity. Observed pattern, not a prediction.")
            explain = {
                "why": why, "method": "rule_chain", "rule": rule["name"],
                "conditions": conds, "matched_domains": ev_domains,
                "source_count": len(sources), "top_sources": sources[:5],
                "confidence": round(conf, 3),
            }
            await write_insight(
                conn, type="cross_domain_chain", entity_ids=[anchor],
                domains=sorted(set(ev_domains)), score=round(conf, 3),
                explain=explain, signature=f"chain:{rule['name']}:{anchor}:{today}",
            )
            written += 1

    log.info("cross_domain_chain: %d insights", written)
    return written
