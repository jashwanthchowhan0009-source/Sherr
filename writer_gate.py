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


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 OF SHERR_WRITING_SPEC.md — the G1-G7 gate for the single-source
# NEWS writer (ai_processor.news_writer).
#
# This is a SECOND gate that lives alongside the synthesis-card checks above.
# The two coexist by design (owner's decision, 2026-09-25): the checks above
# run on multi-source synthesis cards through check_news(); the G1-G7 rules
# below run on the single-source NEWS writer's structured contract. They share
# this module and the writer-doctor.
#
# WHERE THE SPEC AND THE CODE CONFLICTED, THE CODE WON and the spec file was
# updated to match: G4's body band is LENGTH_BANDS["news"] — the existing
# hard 60-80 words — NOT the 60-110 an early draft of the spec carried.
# ════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta, timezone            # noqa: E402
from typing import Optional                                    # noqa: E402

from originality import (                                      # noqa: E402
    originality_check as _originality_check,
    longest_common_run as _longest_common_run,
    tokenize as _tokenize,
)

# ─── field-length bands (spec 1.2 / 4.1 G4) ──────────────────────────────────
# Body band is the existing news band, the single source of truth for it.
BODY_MIN_WORDS, BODY_MAX_WORDS = LENGTH_BANDS["news"]          # (60, 80)
HEADLINE_MIN_WORDS, HEADLINE_MAX_WORDS = 6, 14
BODY_MIN_SENTENCES = 3
WHY_MIN_WORDS, WHY_MAX_WORDS = 12, 28
# G6: a shared run of THIS many words with a source headline is a lift.
HEADLINE_ECHO_RUN = 5

RULE_IDS = ["G1_ORIGINALITY", "G2_NUMBERS", "G3_BANNED_TERMS", "G4_LENGTH",
            "G5_WHY_GROUNDED", "G6_HEADLINE_ECHO", "G7_SHAPE"]


# ─── G3 banned terms (spec 4.2) ──────────────────────────────────────────────
# The spec 4.2 set EXACTLY — the retained runtime terms plus the additions the
# spec enumerates. Deliberately NOT the engine's full advice blocklist
# (narrative.FORBIDDEN_ADVICE): that list is tuned for market-signal cards, where
# hold/target/position/rally ARE advice, and folding it here would fail G3 on
# ordinary news ("held talks", "target of a probe", "exit polls"). The retained
# six are the continuity with the existing block; the rest of 4.2 extends it.
_G3_WORDS = [
    "will", "buy", "sell", "predict", "bullish", "bearish",
    "forecast", "expect", "anticipate", "project", "estimate", "poised",
    "should", "recommend", "opportunity",
    "undervalued", "overvalued", "cheap", "expensive", "attractive",
    "compelling", "outperform", "underperform",
    "soar", "plunge", "crash", "skyrocket", "collapse", "explode",
    "devastating", "stunning", "shocking",
    "guaranteed", "inevitable",
]
_G3_PHRASES = [
    "set to", "on track to", "likely to", "could see", "is expected to",
    "investors should", "worth watching", "one to watch", "risk to consider",
    "strong buy", "certain to", "no doubt", "clearly shows",
]
# Inflection is semantically lossy for these — freeze them to the bare word.
_G3_NO_INFLECT = {"will"}


def _g3_word_forms(base: str) -> set:
    if base in _G3_NO_INFLECT:
        return {base}
    forms = {base, base + "s", base + "ing", base + "ed"}
    if base.endswith("e"):
        forms |= {base + "d", base[:-1] + "ing"}
    if base.endswith(("s", "x", "z", "ch", "sh")):
        forms.add(base + "es")
    if base.endswith("y") and len(base) > 1 and base[-2] not in "aeiou":
        forms |= {base[:-1] + "ies", base[:-1] + "ied"}
    return forms


def _build_g3_regex() -> "re.Pattern":
    forms: set = set()
    for w in _G3_WORDS:
        forms |= _g3_word_forms(w)
    parts = [re.escape(p) for p in _G3_PHRASES]
    parts += [re.escape(f) for f in sorted(forms, key=len, reverse=True)]
    return re.compile(r"\b(?:" + "|".join(parts) + r")\b", re.IGNORECASE)


_G3_RE = _build_g3_regex()

# Explicit-attribution markers — a banned term reporting what a named party said
# is exempt (spec 4.2). Reporting verbs and "according to" only, so a bare claim
# is never waved through.
_G3_ATTRIB_RE = re.compile(
    r"\b(?:said|says|saying|stated|stating|told|telling|announced|announcing|"
    r"according to|reported|reporting|added|noted|noting|confirmed|confirming|"
    r"wrote|writing|remarked|testified|argued|arguing|claimed|claiming|"
    r"warned|warning|declared|declaring)\b", re.IGNORECASE)

_G3_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_G3_NUMERIC_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _spec_words(text: str) -> list:
    return [w for w in re.split(r"\s+", (text or "").strip()) if w]


def _spec_sentences(text: str) -> list:
    return [s for s in _G3_SENTENCE_SPLIT.split((text or "").strip()) if s.strip()]


def _spec_numeric_cores(text: str) -> set:
    return {m.group(0).replace(",", "") for m in _G3_NUMERIC_RE.finditer(text or "")}


# ─── the seven rules — each returns (passed, reason) ─────────────────────────

def _g1_originality(out: dict, source_text: str, _hs) -> tuple:
    passed, metrics = _originality_check(out.get("body", "") or "", source_text or "")
    if passed:
        return True, ""
    return False, "; ".join(metrics.get("reasons") or ["originality check failed"])


def _g2_numbers(out: dict, source_text: str, _hs) -> tuple:
    fields = " ".join([out.get("headline", "") or "", out.get("body", "") or "",
                       out.get("why_it_matters", "") or ""])
    used = out.get("numbers_used")
    if not isinstance(used, list):
        used = []
    covered: set = set()
    for entry in used:
        if not isinstance(entry, dict):
            return False, "numbers_used contains a non-object entry"
        span = entry.get("source_span", "")
        if not span or span not in (source_text or ""):
            return False, f"source_span {span!r} is not a literal substring of the source"
        covered |= _spec_numeric_cores(str(entry.get("value", "")))
        covered |= _spec_numeric_cores(span)
    for core in _spec_numeric_cores(fields):
        if core not in covered:
            return False, f"number {core!r} is not in numbers_used"
    return True, ""


def _g3_attributed(sentence: str, match: "re.Match") -> bool:
    if not _G3_ATTRIB_RE.search(sentence):
        return False
    quoted = re.findall(r"[\"“”'‘’]([^\"“”'‘’]+)"
                        r"[\"“”'‘’]", sentence)
    term = match.group(0)
    if any(term.lower() in q.lower() for q in quoted):
        return True
    for m in _G3_ATTRIB_RE.finditer(sentence):
        if m.start() < match.start():
            return True
    return False


def _g3_banned(out: dict, _st, _hs) -> tuple:
    for field in ("headline", "body", "why_it_matters"):
        text = out.get(field, "") or ""
        for sentence in _spec_sentences(text) or [text]:
            for m in _G3_RE.finditer(sentence):
                if _g3_attributed(sentence, m):
                    continue
                return False, f"banned term {m.group(0)!r} in {field}"
    return True, ""


def _g4_length(out: dict, _st, _hs) -> tuple:
    hw = len(_spec_words(out.get("headline", "")))
    if not (HEADLINE_MIN_WORDS <= hw <= HEADLINE_MAX_WORDS):
        return False, f"headline {hw} words, outside {HEADLINE_MIN_WORDS}-{HEADLINE_MAX_WORDS}"
    bw = len(_spec_words(out.get("body", "")))
    if not (BODY_MIN_WORDS <= bw <= BODY_MAX_WORDS):
        return False, f"body {bw} words, outside {BODY_MIN_WORDS}-{BODY_MAX_WORDS}"
    bs = len(_spec_sentences(out.get("body", "")))
    if bs < BODY_MIN_SENTENCES:
        return False, f"body has {bs} sentences, under {BODY_MIN_SENTENCES}"
    ww = len(_spec_words(out.get("why_it_matters", "")))
    if not (WHY_MIN_WORDS <= ww <= WHY_MAX_WORDS):
        return False, f"why_it_matters {ww} words, outside {WHY_MIN_WORDS}-{WHY_MAX_WORDS}"
    return True, ""


def _g5_why_grounded(out: dict, _st, _hs) -> tuple:
    why = out.get("why_it_matters", "") or ""
    body = out.get("body", "") or ""
    body_low = body.lower()
    for core in _spec_numeric_cores(why):
        if core not in _spec_numeric_cores(body):
            return False, f"why_it_matters number {core!r} is absent from the body"
    ents = out.get("entities") or {}
    names = [ents.get("subject", "")] + list(ents.get("affected") or [])
    why_low = why.lower()
    for name in names:
        name = (name or "").strip()
        if name and name.lower() in why_low and name.lower() not in body_low:
            return False, f"why_it_matters names {name!r}, absent from the body"
    return True, ""


def _g6_headline_echo(out: dict, _st, source_headlines) -> tuple:
    ours = _tokenize(out.get("headline", "") or "")
    if not ours:
        return True, ""
    for src in (source_headlines or []):
        run = _longest_common_run(ours, _tokenize(src or ""))
        if run >= HEADLINE_ECHO_RUN:
            return False, f"shares a {run}-word run with a source headline"
    return True, ""


def _g7_shape(out: dict, _st, _hs) -> tuple:
    for field in ("headline", "body", "why_it_matters", "primary_source_attribution"):
        if not (out.get(field, "") or "").strip():
            return False, f"missing or empty field: {field}"
    ents = out.get("entities")
    if not isinstance(ents, dict):
        return False, "missing or empty field: entities"
    if not (ents.get("subject", "") or "").strip():
        return False, "missing or empty field: entities.subject"
    if "numbers_used" in out and not isinstance(out.get("numbers_used"), list):
        return False, "numbers_used is not a list"
    if "affected" in ents and not isinstance(ents.get("affected"), list):
        return False, "entities.affected is not a list"
    return True, ""


_SPEC_RULES = [
    ("G1_ORIGINALITY", _g1_originality),
    ("G2_NUMBERS", _g2_numbers),
    ("G3_BANNED_TERMS", _g3_banned),
    ("G4_LENGTH", _g4_length),
    ("G5_WHY_GROUNDED", _g5_why_grounded),
    ("G6_HEADLINE_ECHO", _g6_headline_echo),
    ("G7_SHAPE", _g7_shape),
]


class GateResult:
    """The outcome of running the G1-G7 gate on one NEWS card."""

    def __init__(self, results: list) -> None:
        self.results = results                 # [(rule_id, passed, reason), ...]
        self.passed = all(p for _, p, _ in results)

    @property
    def failures(self) -> list:
        return [{"rule": r, "reason": reason}
                for r, p, reason in self.results if not p]


def run_gate(output: Optional[dict], source_text: str, source_headlines) -> GateResult:
    """Score one NEWS writer output against all seven rules (spec 4.1).

    A None output (writer failed / unparseable JSON) is a G7_SHAPE failure and
    nothing else — the other rules have no fields to read.
    """
    if not isinstance(output, dict):
        return GateResult([("G7_SHAPE", False, "no output / invalid JSON")])
    results = []
    for rule_id, fn in _SPEC_RULES:
        try:
            passed, reason = fn(output, source_text, source_headlines)
        except Exception as e:                                    # noqa: BLE001
            passed, reason = False, f"{rule_id} raised {type(e).__name__}: {e}"
        results.append((rule_id, passed, reason))
    return GateResult(results)


# ─── attempt store for /admin/writer-doctor's Section-5 fields ───────────────
# In-process, no DB — the doctor endpoint must not write to the database (spec
# 5). Bounded, newest last. Separate from main._writer_doctor, which keeps the
# synthesis path's by-writer buckets; the endpoint surfaces both.
_ATTEMPTS: list = []
_MAX_ATTEMPTS = 5000


def _now() -> datetime:
    return datetime.now(timezone.utc)


def record_attempt(*, article_id, writer_id: str, attempt: int,
                   gate: GateResult, is_regen: bool, fell_back: bool,
                   at: Optional[datetime] = None) -> None:
    _ATTEMPTS.append({
        "at": at or _now(),
        "article_id": str(article_id) if article_id is not None else "",
        "writer_id": writer_id or "",
        "attempt": int(attempt),
        "passed": bool(gate.passed),
        "failures": gate.failures,
        "is_regen": bool(is_regen),
        "fell_back": bool(fell_back),
    })
    if len(_ATTEMPTS) > _MAX_ATTEMPTS:
        del _ATTEMPTS[:-_MAX_ATTEMPTS]


def reset_attempts() -> None:
    """Test hook — clear the in-memory store."""
    _ATTEMPTS.clear()


def writer_doctor_report(window_hours: int = 24) -> dict:
    """The Section-5 payload the endpoint folds in. Read-only; no generation, no DB."""
    window_hours = max(1, int(window_hours or 24))
    cutoff = _now() - timedelta(hours=window_hours)
    recs = [r for r in _ATTEMPTS if r["at"] >= cutoff]

    attempted = len(recs)
    passed = sum(1 for r in recs if r["passed"])
    regenerated = sum(1 for r in recs if r["is_regen"])
    fell_back = sum(1 for r in recs if r["fell_back"])

    by_rule = {rid: 0 for rid in RULE_IDS}
    for r in recs:
        for f in r["failures"]:
            if f["rule"] in by_rule:
                by_rule[f["rule"]] += 1

    by_writer: dict = {}
    for r in recs:
        w = by_writer.setdefault(r["writer_id"], {"attempted": 0, "passed": 0})
        w["attempted"] += 1
        if r["passed"]:
            w["passed"] += 1
    for w in by_writer.values():
        w["pass_rate"] = round(w["passed"] / w["attempted"], 4) if w["attempted"] else 0.0

    recent_failures: list = []
    for r in reversed(_ATTEMPTS):
        for f in r["failures"]:
            recent_failures.append({
                "article_id": r["article_id"], "writer_id": r["writer_id"],
                "rule": f["rule"], "reason": f["reason"],
                "attempt": r["attempt"], "at": r["at"].isoformat(),
            })
            if len(recent_failures) >= 50:
                break
        if len(recent_failures) >= 50:
            break

    return {
        "window_hours": window_hours,
        "attempted": attempted,
        "passed": passed,
        "pass_rate": round(passed / attempted, 4) if attempted else 0.0,
        "regenerated": regenerated,
        "fell_back_to_safe": fell_back,
        "by_rule": by_rule,
        "by_writer": by_writer,
        "recent_failures": recent_failures,
    }


# ─── the writer + gate runner (spec 4.3 + 4.4) ───────────────────────────────

class NewsResult:
    """What ingest gets back from one source: a passing card, or a safe fallback."""

    def __init__(self, *, ok: bool, writer_id: str, why_it_matters: str = "",
                 output: Optional[dict] = None, fell_back: bool = False,
                 safe_summary: str = "") -> None:
        self.ok = ok
        self.writer_id = writer_id
        self.why_it_matters = why_it_matters
        self.output = output
        self.fell_back = fell_back
        self.safe_summary = safe_summary


async def process_source(*, article_id, source_text: str, source_headlines,
                         writer=None, limiter=None, gate=run_gate,
                         recorder=record_attempt, safe_summary: str = "") -> NewsResult:
    """Generate a NEWS card, gate it, and honour the spec's failure + rate rules.

    1. First pass: acquire a limiter token (waiting — a first pass is never
       dropped), generate, gate. Pass -> done.
    2. Fail -> regenerate ONCE, but only if a token is free right now
       (try_acquire): a regeneration is lower priority than new ingestion, so a
       saturated limiter skips it and goes straight to the safe fallback (4.4).
    3. Second attempt still failing (or skipped) -> fall back to _SAFE_SUMMARY.
       The failed body is never published (4.3).

    Every attempt is recorded for the doctor; the terminal attempt carries the
    fell_back flag.
    """
    if writer is None:
        import ai_processor
        writer = ai_processor.news_writer
    if limiter is None:
        import rate_limit
        limiter = rate_limit.INGEST_LIMITER
    if not safe_summary:
        try:
            import ai_processor
            safe_summary = ai_processor._SAFE_SUMMARY
        except Exception:                                         # noqa: BLE001
            safe_summary = ""

    records: list = []

    await limiter.acquire()
    out, writer_id = await writer(source_text)
    res = gate(out, source_text, source_headlines)
    records.append(dict(article_id=article_id, writer_id=writer_id, attempt=1,
                        gate=res, is_regen=False))
    if res.passed:
        _flush(records, recorder, fell_back=False)
        return NewsResult(ok=True, writer_id=writer_id,
                          why_it_matters=out.get("why_it_matters", ""), output=out)

    if await limiter.try_acquire():
        out2, writer_id2 = await writer(source_text, corrective=res.failures)
        res2 = gate(out2, source_text, source_headlines)
        records.append(dict(article_id=article_id, writer_id=writer_id2, attempt=2,
                            gate=res2, is_regen=True))
        if res2.passed:
            _flush(records, recorder, fell_back=False)
            return NewsResult(ok=True, writer_id=writer_id2,
                              why_it_matters=out2.get("why_it_matters", ""), output=out2)
        _flush(records, recorder, fell_back=True)
        return NewsResult(ok=False, writer_id=writer_id2, fell_back=True,
                          safe_summary=safe_summary)

    _flush(records, recorder, fell_back=True)
    return NewsResult(ok=False, writer_id=writer_id, fell_back=True,
                      safe_summary=safe_summary)


def _flush(records: list, recorder, *, fell_back: bool) -> None:
    for i, rec in enumerate(records):
        recorder(article_id=rec["article_id"], writer_id=rec["writer_id"],
                 attempt=rec["attempt"], gate=rec["gate"], is_regen=rec["is_regen"],
                 fell_back=fell_back and (i == len(records) - 1))
