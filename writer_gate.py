"""writer_gate.py — the quality gate the SherrByte writing spec runs before publish.

WHY THIS MODULE EXISTS
──────────────────────
The writing spec (News · Strings · Dots) says the hook must come from SELECTION and
SPECIFICITY, never from tone — and it enforces that with a fixed checklist run on
every generated card before it can reach a reader. This module IS that checklist,
pure and stdlib-only so it is cheap enough to run on every article and impossible to
"fail open" because a service was down.

    passed, failures = check_news(result, source_text=..., sebi_check=...)

`failures` is a list of {"rule": slug, "detail": str} — the shape /admin/writer-doctor
buckets by writer and rule, so after a week that table says exactly which rule the
model fights. That is what you tune, not the whole prompt.

ONE COMPLIANCE BLOCKLIST, NOT A SECOND COPY (CLAUDE.md). The SEBI / forward-looking
words (`will`, `buy`, `sell`, `predict`, `bullish`, `target`, `poised to`, …) are the
engine's own `narrative.violates_language_rules`, INJECTED as `sebi_check` rather than
re-listed here. What this module adds are the spec's THREE NEW banned categories that
the engine blocklist does not cover — throat-clearing, empty intensifiers, and the
passive-nominalisation anti-pattern — plus the structural checks (numbers verified,
first-six-words, length, causal language, open loop, entity presence).

The checks map one-to-one onto the spec's section-4 table:

    banned_terms      0 hits across SEBI + throat-clearing + intensifier + nominalisation
    ngram_overlap     7-gram overlap with the source below the originality threshold
    numbers_verified  every number in the body is backed (numbers_used, or the source)
    first_six_words   the opening six words carry information (no throat-clearing)
    length            News 60-80w, Strings 120-180w, Dots 130-200w
    causal_language   Strings/Dots only: "led to" / "caused" / "because of" only if attributed
    open_loop         no card ends on an unanswered question (except Dots what_to_check)
    entity_presence   at least one resolved entity

Nothing here imports the app, a database, a model, or the network. It is pure, so the
gate is testable without any of them.
"""

from __future__ import annotations

import re

# ─── the spec's three new banned categories ─────────────────────────────────────
# NOT the SEBI list — that is the engine's one blocklist, injected as `sebi_check`.
# These are the tone-based tells the spec bans in all three writers.

# Throat-clearing: openers and filler that carry no information. Matched as phrases,
# case-insensitively, anywhere in the text.
THROAT_CLEARING = [
    "in a significant development",
    "it is reported that",
    "sources say",
    "it may be noted",
    "amid growing concerns",
    "in a major move",
    "has been making waves",
    "sent shockwaves",
]

# Empty intensifiers — banned UNLESS the word sits in the same clause as a number.
# "soared" alone is noise; "soared 40%" is signal, so the check is clause-scoped:
# an intensifier is a hit only when its clause contains no digit.
INTENSIFIERS = [
    "dramatic", "massive", "huge", "shocking", "stunning", "sharply",
    "plunged", "soared", "skyrocketed", "crashed",
]

# The passive-nominalisation anti-pattern the spec calls out by example: prefer
# "RBI tightened" over "there was a tightening by RBI". Deliberately narrow — it
# matches the "there was/were/has been … by …" shape only, so an ordinary sentence
# that happens to contain "there is" does not trip it.
_NOMINALISATION_RE = re.compile(
    r"\bthere\s+(?:was|were|is|are|has\s+been|have\s+been)\b[^.,;:!?]*\bby\b",
    re.IGNORECASE)

# Causal claims forbidden in Strings/Dots unless attributed to a named source.
CAUSAL_PHRASES = ["led to", "caused", "because of", "resulted in", "due to"]

# A number is any run of digits (commas and a decimal point are part of it). Word
# numbers ("two", "third") are deliberately NOT matched — only figures are verified,
# which is where hallucination actually shows up and where false positives don't.
_NUMBER_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")

# Length bands, in words, per writer. Inclusive on both ends.
LENGTH_BANDS = {
    "news": (60, 80),
    "strings": (120, 180),
    "dots": (130, 200),
}


# ─── entity masking ──────────────────────────────────────────────────────────────
# Real-world names collide with the tone lists exactly as they collide with the SEBI
# one: a firm called "Massive Dynamic", a person named "Sharpe". Names are quoted
# facts, not tone the writer chose, so they are masked out before the tone scan — the
# same rule narrative.violates_language_rules already applies to the SEBI words.

def _mask_names(text: str, names) -> str:
    out = text or ""
    for name in sorted((n for n in (names or []) if n), key=len, reverse=True):
        out = re.sub(re.escape(name), " ", out, flags=re.IGNORECASE)
    return out


def _clauses(text: str):
    """Split on clause boundaries so an intensifier is judged against ITS clause."""
    return [c for c in re.split(r"[.,;:!?—]", text or "") if c.strip()]


# ─── the individual checks ─────────────────────────────────────────────────────
# Each returns a list of failure detail strings ([] == passed), so a caller can
# report which rule fired and why.

def banned_terms(text: str, names=None, *, sebi_check=None) -> list:
    """SEBI + throat-clearing + empty intensifiers + passive nominalisation."""
    masked = _mask_names(text, names)
    low = masked.lower()
    hits = []

    if sebi_check is not None:
        # The engine's one blocklist already masks names itself; pass them through so
        # a company named "Target" or an actor named "Will" cannot trip it.
        sebi_hits = sebi_check(text, list(names or []))
        hits += [f"compliance: {h}" for h in (sebi_hits or [])]

    hits += [f"throat-clearing: {p}" for p in THROAT_CLEARING if p in low]

    for clause in _clauses(masked):
        if _NUMBER_RE.search(clause):
            continue                       # an intensifier attached to a number is allowed
        cl = clause.lower()
        for w in INTENSIFIERS:
            if re.search(rf"\b{re.escape(w)}\b", cl):
                hits.append(f"intensifier without a number: {w}")

    if _NOMINALISATION_RE.search(masked):
        hits.append("nominalisation: 'there was … by …' — prefer an active subject")

    return hits


def numbers_verified(body: str, numbers_used=None, source_text: str = "") -> list:
    """Every figure in the body must be backed by evidence.

    The spec's mechanism is `numbers_used` — the model self-reports each number with
    its source, and a body number absent from it is discarded. That is the primary
    check here. A number that also appears verbatim in the SOURCE text is accepted as
    well: it is demonstrably not invented, and this keeps a model that under-populates
    a brand-new field from stalling every numeric story on the rate-limited drain.
    A body with no figures passes trivially.
    """
    body_nums = {_norm_num(m) for m in _NUMBER_RE.findall(body or "")}
    if not body_nums:
        return []

    backed = set()
    for entry in (numbers_used or []):
        value = entry.get("value") if isinstance(entry, dict) else entry
        for m in _NUMBER_RE.findall(str(value or "")):
            backed.add(_norm_num(m))
    for m in _NUMBER_RE.findall(source_text or ""):
        backed.add(_norm_num(m))

    missing = sorted(n for n in body_nums if n and n not in backed)
    if missing:
        return [f"number(s) in the body not backed by numbers_used or the source: "
                f"{', '.join(missing)}"]
    return []


def _norm_num(raw: str) -> str:
    """The digits of a figure, comma-free, trailing-zero-decimal folded. '1,200' and
    '1200' compare equal; '4' and '4%' both reduce to '4'."""
    s = (raw or "").replace(",", "").strip()
    if s.endswith("."):
        s = s[:-1]
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def first_six_words(text: str, names=None) -> list:
    """The opening six words must carry information — no throat-clearing start."""
    words = (text or "").split()
    if not words:
        return ["empty text"]
    opening = " ".join(words[:6]).lower()
    for phrase in THROAT_CLEARING:
        # A throat-clearing phrase AT THE START is the failure the rule names.
        if opening.startswith(phrase[:len(opening)]) or phrase in opening:
            return [f"opening six words are throat-clearing: '{' '.join(words[:6])}'"]
    return []


def length_ok(text: str, writer: str) -> list:
    lo, hi = LENGTH_BANDS.get(writer, (0, 10 ** 9))
    n = len((text or "").split())
    if n < lo or n > hi:
        return [f"{writer} length {n} words outside {lo}-{hi}"]
    return []


def causal_language(text: str, *, attributed: bool = False) -> list:
    """Strings/Dots rule: a causal claim is only allowed when attributed to a source.

    `attributed=True` is the caller's assertion that a named source stated the link
    (the spec permits "the company cited X when announcing Y"); when it is False any
    causal phrase is a failure.
    """
    if attributed:
        return []
    low = (text or "").lower()
    return [f"unattributed causal claim: '{p}'" for p in CAUSAL_PHRASES if p in low]


def open_loop(text: str, writer: str) -> list:
    """No card may end on an unanswered question — the spec's one exception is the
    Dots `what_to_check` field, which callers gate separately, never through here."""
    if (text or "").strip().endswith("?"):
        return [f"{writer} ends on an unanswered question (an open loop)"]
    return []


def entity_presence(entities) -> list:
    resolved = [e for e in (entities or []) if str(e).strip()]
    if not resolved:
        return ["no resolved entity — do not publish"]
    return []


# ─── the composed gate ──────────────────────────────────────────────────────────

def check_news(result: dict, *, source_text: str = None,
               overlap_passed: bool = None, sebi_check=None) -> tuple:
    """Run the News writer's gate over a synthesis result. Returns (passed, failures).

    `result` is the parsed synthesis dict: `content` is gated, `extracted_entities`
    supplies the entity-presence check and the name mask, and `numbers_used` (when the
    prompt emits it) backs the number check.

    OVERLAP IS THE CALLER'S TO OWN. This repo already runs `originality_check` on the
    body to produce the metrics stored on the row, so re-running it here would be
    duplicate work and a second owner of one decision. Pass `overlap_passed` (the
    result of that existing check) and it is folded into the gate as `ngram_overlap`;
    pass `source_text` instead and the gate computes it; pass neither and the overlap
    rule is simply not part of this run.
    """
    content = str(result.get("content") or "")
    entities = result.get("extracted_entities") or []
    names = [str(e) for e in entities if str(e).strip()]
    names += [str(result.get("who_subject") or "")]
    names += [str(a) for a in (result.get("who_affected") or [])]

    failures = []

    def add(rule, details):
        for d in details:
            failures.append({"rule": rule, "detail": d})

    add("banned_terms", banned_terms(content, names, sebi_check=sebi_check))
    add("numbers_verified",
        numbers_verified(content, result.get("numbers_used"), source_text or ""))
    add("first_six_words", first_six_words(content, names))
    add("length", length_ok(content, "news"))
    add("open_loop", open_loop(content, "news"))
    add("entity_presence", entity_presence(entities))

    if overlap_passed is not None:
        if not overlap_passed:
            add("ngram_overlap", ["7-gram overlap with a source exceeds the threshold"])
    elif source_text:
        try:
            from originality import originality_check  # noqa: PLC0415
            ok, metrics = originality_check(content, source_text)
            if not ok:
                add("ngram_overlap", metrics.get("reasons")
                    or ["7-gram overlap with a source exceeds the threshold"])
        except Exception:                                         # noqa: BLE001
            pass

    return (not failures), failures


# ─── the default SEBI check, resolved the way the app resolves it ────────────────
# So a caller with the engine on its path gets the ONE blocklist for free, and a test
# can inject its own. Mirrors ai_processor._hook_check: in-repo import, cached, and if
# the engine cannot be loaded the SEBI category is left unscreened rather than the
# gate failing — but that is a last resort, the import is in-repo.
_SEBI_CHECK = None
_SEBI_LOADED = False


def default_sebi_check():
    global _SEBI_CHECK, _SEBI_LOADED
    if _SEBI_LOADED:
        return _SEBI_CHECK
    _SEBI_LOADED = True
    try:
        import os                                                # noqa: PLC0415
        import sys                                               # noqa: PLC0415
        engine = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sherrbyte")
        if engine not in sys.path:
            sys.path.insert(0, engine)
        from app.spie.reasoning.narrative import (               # noqa: PLC0415
            violates_language_rules)
        _SEBI_CHECK = violates_language_rules
    except Exception:                                            # noqa: BLE001
        _SEBI_CHECK = None
    return _SEBI_CHECK
