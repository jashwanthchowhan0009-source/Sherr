"""filings/classify.py — filing type -> event_class, by RULES, never an LLM.

A filing already tells you what kind of event it is: BSE stamps a CATEGORYNAME
("Result", "Board Meeting", "Change in Directors"...), NSE a `desc` ("Financial
Results", "Acquisition"...), and a regulator's title carries the same signal in
words. Mapping that to the analog engine's closed taxonomy is a keyword table,
not a reasoning problem — so it is done here, deterministically, with ZERO
provider calls per filing. That is a hard constraint: the free-tier request
ceiling is rationed by the rewrite drain, and a filing must never touch it.

The taxonomy is the SAME closed set the analog matcher's class_match weight is
built on (imported from event_library, so there is one taxonomy, not two). A
filing that matches no rule is `other`, and `other` contributes no class_match —
exactly the honest default event_library.classify already uses for articles.

Ordered MOST-SPECIFIC FIRST: the first class whose evidence appears wins, so an
RBI "monetary policy" release is central_bank_policy, not regulatory_action, and
a "scheme of arrangement" is m_and_a, not the regulatory_action its NCLT wording
might otherwise catch.
"""

from __future__ import annotations

from app.spie.analog.event_library import EVENT_CLASSES

# (event_class, phrases) — phrases matched as lowercase substrings against
# "<filing_type> <subject>". These are the categories the four sources actually
# emit, not a general newsroom vocabulary: BSE/NSE category labels first, then
# the words RBI/SEBI titles use.
_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("central_bank_policy", (
        "monetary policy", "repo rate", "policy rate", "mpc", "bank rate",
        "liquidity adjustment", "cash reserve ratio", "crr", "slr",
        "open market operation", "omo")),
    ("m_and_a", (
        "acquisition", "amalgamation", "scheme of arrangement", "merger",
        "amalgamat", "takeover", "open offer", "sast",
        "substantial acquisition", "slump sale", "stake", "demerger",
        "spin-off", "spinoff", "disinvest", "divestment")),
    ("leadership_change", (
        "change in director", "change in directors", "change in kmp",
        "appointment", "resignation", "resigned", "cessation",
        "reconstitution of board", "managing director", "chief executive",
        "chief financial officer", "company secretary", "key managerial")),
    ("earnings", (
        "financial result", "financial results", "result", "quarterly",
        "unaudited", "audited results", "outcome of board meeting",
        "board meeting", "integrated filing- financials",
        "integrated filing (financials)", "net profit", "revenue")),
    ("guidance_change", (
        "guidance", "outlook", "business update", "revised estimate",
        "capex plan", "capacity expansion", "capacity addition",
        "commercial production", "commissioning", "expansion")),
    ("default_credit", (
        "credit rating", "rating", "default", "insolvency", "nclt",
        "resolution plan", "debenture", "non-convertible", "ncd",
        "debt", "restructuring", "one time settlement", "wilful defaulter")),
    ("supply_disruption", (
        "plant shutdown", "shutdown", "force majeure", "fire at",
        "disruption", "lock-out", "lockout", "strike", "production halt",
        "suspension of operations")),
    ("sanctions", (
        "debarment", "debar", "export ban", "import ban", "trade restriction",
        "prohibited")),
    ("regulatory_action", (
        "order", "adjudication", "penalty", "show cause", "show-cause",
        "settlement order", "enforcement", "circular", "notification",
        "master direction", "guidelines", "framework", "consultation paper",
        "regulation", "probe", "investigation", "warning", "directions",
        "press release")),
    ("commodity_shock", (
        "crude", "petroleum", "refining", "bullion", "commodity",
        "natural gas")),
    ("currency_move", (
        "foreign exchange", "forex", "rupee", "exchange rate", "fema")),
)

# ── Regulator rule set — RBI / SEBI ONLY ─────────────────────────────────────
# A regulator release is NOT a company filing and must never reach the
# company-filing rules: an RBI KYC amendment DIRECTION is a regulatory_action,
# not an "m_and_a" caught because the word "acquisition" happened to appear
# somewhere in a bulk RSS description. So regulators are classified against a
# closed set of just the three classes a regulator can actually emit —
# central_bank_policy, sanctions, regulatory_action — and everything else is
# 'other' by default, never a guess. Ordered most-specific first so a monetary
# policy release is central_bank_policy, not the regulatory_action its
# "directions"/"framework" wording would otherwise catch. "press release" is
# deliberately NOT a phrase here: it is not a category signal, and matching on it
# would drag a SEBI felicitation release out of the honest 'other'.
_REGULATOR_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("central_bank_policy", (
        "monetary policy", "repo rate", "policy rate", "mpc", "bank rate",
        "liquidity adjustment", "cash reserve ratio", "crr", "slr",
        "open market operation", "omo")),
    ("sanctions", (
        "debarment", "debar", "export ban", "import ban", "trade restriction",
        "prohibited")),
    ("regulatory_action", (
        "order", "adjudication", "penalty", "show cause", "show-cause",
        "settlement order", "enforcement", "circular", "notification",
        "master direction", "direction", "directions", "guidelines",
        "framework", "consultation paper", "regulation", "probe",
        "investigation", "warning", "amendment")),
)

_REGULATORS = {"RBI", "SEBI"}


def classify_filing(source: str, filing_type: str, subject: str = "") -> str:
    """The filing's event_class, or 'other'.

    Deterministic and rule-based on purpose: the class feeds a ranking weight, so
    an LLM must never decide it. `source` SELECTS THE RULE SET: RBI/SEBI releases
    go through the regulator-only rules (central_bank_policy / sanctions /
    regulatory_action, else 'other'), never the company-filing rules — that is
    what stops a regulator release from being mislabelled m_and_a or earnings.
    """
    hay = f"{filing_type or ''} {subject or ''}".lower()
    if not hay.strip():
        return "other"
    rules = _REGULATOR_RULES if (source or "").upper() in _REGULATORS else _RULES
    for klass, phrases in rules:
        if any(p in hay for p in phrases):
            # Defensive: never emit a class the schema CHECK / matcher rejects.
            return klass if klass in EVENT_CLASSES else "other"
    return "other"
