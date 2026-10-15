"""Telugu cards: the English card, translated in the SAME provider request.

Adopted 2026-10-15, the one item Sherr Research deliberately left open.

WHY IT IS A TRANSLATION AND NOT A SECOND WRITER
-----------------------------------------------
Two constraints decide the whole design:

1. **The request budget.** Gemini's free tier allows 15 requests a minute and
   `body_drain_job` already spends 12 of them. A second call per article to
   translate would halve the drain's throughput — the one pass actually writing
   the corpus — so Telugu is asked for as one more FIELD in the request that
   already writes the story. It costs zero extra requests, the same posture
   `research.py` takes for its evidence brief.

2. **The compliance gate is English-only.** `writer_gate`'s tone rules and the
   engine's blocklist (`narrative.violates_language_rules`) are written in
   English and cannot read Telugu prose. Rather than pretend otherwise, Telugu
   is only ever a translation of an English body that has ALREADY passed the
   originality check and (for a Research card) the writer gate. It is never
   generated independently, so it carries no claim the English card did not
   already make and clear.

What this module can and cannot check, stated plainly:

* It CAN check that the text is actually in Telugu script, that it invents no
  figure the English did not carry, that it prints no price-shaped figure, that
  it uses no advice word, and that its length is proportionate.
* It CANNOT check forward-looking TENSE. Telugu marks the future with an
  inflectional suffix, not a separate word, so "ధర పెరుగుతుంది" has no "will"
  to find — and a suffix match would also hit the ordinary present. The honest
  answer is the ordering above: the English body it translates is already past
  tense and conditional only, and the prompt tells the model to keep the tense.
  Do NOT add a suffix blocklist here believing it closes that gap; it would
  reject correct prose and still miss the construction.

ONE BLOCKLIST, STILL
--------------------
The Latin half is the engine's list, unchanged: a Telugu card that leaves
"buy", "target" or "$90" in the text is caught by `violates_language_rules`
exactly as an English one is. What this module ADDS is only the Telugu-script
equivalents of that list, and every entry carries the English word it mirrors so
a test can assert the two cannot drift apart.

A failing translation costs NOTHING. The column stays empty, the card renders in
English, and the verdict is recorded so the Telugu pass rate is a number in
`/admin/writer-doctor` rather than a guess. That asymmetry is why the word list
below is deliberately strict: a false positive drops a translation, a false
negative publishes an advice word.
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata

log = logging.getLogger("sherrbyte.telugu")

# The language is published only where a reader asked for it, so the switch is
# needed for one reason only: turning the field off stops the model spending
# output tokens on it, which is what a tight maxOutputTokens budget may need.
def enabled() -> bool:
    return os.getenv("TELUGU_ENABLED", "1") not in ("0", "false", "no")


# ─── 1. is this actually Telugu ───────────────────────────────────────────────
# U+0C00-U+0C7F is the Telugu block. A model that cannot translate tends to
# answer in English rather than refuse, and an English string stored in the
# Telugu column would render as a duplicate card in the "wrong" language with no
# error anywhere — which is why the ratio is checked rather than the field's
# presence.
_TELUGU_RANGE = (0x0C00, 0x0C7F)
MIN_SCRIPT_RATIO = float(os.getenv("TELUGU_MIN_SCRIPT_RATIO", "0.55"))

# Telugu digits ౦-౯. Normalised to ASCII before any figure is compared, so a
# translation that writes the same number in Telugu numerals is not read as a
# different number.
_TELUGU_DIGITS = {chr(0x0C66 + i): str(i) for i in range(10)}


def _normalise_digits(text: str) -> str:
    return "".join(_TELUGU_DIGITS.get(ch, ch) for ch in text or "")


def script_ratio(text: str) -> float:
    """Share of the LETTERS in `text` that are Telugu.

    Digits, punctuation and spaces are excluded: a correct translation carries
    the story's numbers and the Latin tickers, and counting those against it
    would make a numeric card look untranslated.
    """
    letters = [ch for ch in (text or "") if unicodedata.category(ch).startswith("L")]
    if not letters:
        return 0.0
    te = sum(1 for ch in letters if _TELUGU_RANGE[0] <= ord(ch) <= _TELUGU_RANGE[1])
    return te / len(letters)


def looks_telugu(text: str) -> bool:
    return script_ratio(text) >= MIN_SCRIPT_RATIO


# ─── 2. the Telugu half of the one blocklist ──────────────────────────────────
# Each entry is (telugu, english_mirror). The mirror is not decoration: a test
# asserts every one of them is really in the engine's own FORBIDDEN /
# FORBIDDEN_ADVICE list, so this table can never quietly forbid something the
# English card is allowed to say, or keep forbidding a word the engine dropped.
#
# IMPERATIVES AND UNAMBIGUOUS NOUNS ONLY. "అమ్మకం" (sales) and "పెరుగుదల"
# (growth) are ordinary business vocabulary and are NOT here; "అమ్మండి" (sell —
# imperative) and "సిఫార్సు" (recommendation) are advice in any sentence they
# appear in.
FORBIDDEN_TE = (
    ("కొనండి", "buy"),               # buy — imperative
    ("కొనుగోలు చేయండి", "buy"),      # make a purchase — imperative
    ("అమ్మండి", "sell"),             # sell — imperative
    ("అమ్మేయండి", "sell"),           # sell off — imperative
    ("పెట్టుబడి పెట్టండి", "invest"),  # invest — imperative
    ("సిఫార్సు", "recommendation"),
    ("సలహా", "advice"),
    ("లక్ష్య ధర", "target"),          # target price
    ("జోస్యం", "prediction"),
    ("అంచనా", "forecast"),           # forecast / estimate — see the note below
    ("బుల్లిష్", "bullish"),          # transliterations the model may reach for
    ("బేరిష్", "bearish"),
    ("టార్గెట్", "target"),
    ("పోర్ట్‌ఫోలియో", "portfolio"),
)
# "అంచనా" also means a plain estimate, so it is the one entry that will drop
# otherwise-fine translations. That is the trade this module is built to make:
# the cost of a false positive is an English-only card, and the cost of a false
# negative is a forecast published under a compliance posture that forbids one.

_SEBI_CHECK = None
_SEBI_LOADED = False


def _sebi_check():
    """The engine's blocklist, borrowed from writer_gate rather than re-listed."""
    global _SEBI_CHECK, _SEBI_LOADED
    if _SEBI_LOADED:
        return _SEBI_CHECK
    _SEBI_LOADED = True
    try:
        import writer_gate                                        # noqa: PLC0415
        _SEBI_CHECK = writer_gate.default_sebi_check()
    except Exception:                                             # noqa: BLE001
        _SEBI_CHECK = None
    return _SEBI_CHECK


# ─── 3. figures ───────────────────────────────────────────────────────────────
# A translation may DROP a number (Telugu prose is tighter and the summary is
# shorter), but it may never introduce one. Comparing the figure sets is the one
# hallucination check that works identically in any language.
_NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def figures(text: str) -> set:
    return {n.replace(",", "").rstrip(".")
            for n in _NUM_RE.findall(_normalise_digits(text))}


def _tolerated(x: str) -> bool:
    """Is this extra figure a date artefact rather than a claim?

    A year or a day-of-month routinely appears in a translated date phrase
    ("2026లో", "12వ తేదీ") where the English wrote the month in words, and
    dropping a whole card over one is not worth it — the English body is the
    claim and the gate already read it.

    It is deliberately NOT a length rule. "7500" and "2026" are both four
    digits, and the first is exactly the kind of figure this check exists to
    catch, so the test is the VALUE: a plausible year or a small ordinal passes
    and everything else is treated as invented.
    """
    if "." in x:
        return False
    try:
        v = float(x)
    except ValueError:
        return False
    return (1900 <= v <= 2099) or v <= 31


def invented_figures(te: str, en: str) -> list:
    """Figures the Telugu states that the English did not. Sorted, for a stable
    failure string."""
    return sorted(x for x in (figures(te) - figures(en)) if not _tolerated(x))


# ─── 4. the verdict ───────────────────────────────────────────────────────────
# Proportionate length, not a word band of its own. Telugu is agglutinative, so
# the same content lands in roughly 0.5-0.9 of the English word count; a copy
# outside this band is either a truncation or a card with added content.
MIN_RATIO = float(os.getenv("TELUGU_MIN_RATIO", "0.40"))
MAX_RATIO = float(os.getenv("TELUGU_MAX_RATIO", "1.25"))


def check(te: dict, en: dict) -> tuple:
    """(ok, failures) for one translated card against the English it mirrors.

    Pure: no DB, no model, no network. Every failure is a short rule name so
    /admin/writer-doctor can bucket them the same way it buckets the English
    gate's.
    """
    failures = []
    te_body = (te or {}).get("full_body") or ""
    te_head = (te or {}).get("refined_title") or ""
    en_body = (en or {}).get("full_body") or ""

    if not te_body.strip() or not te_head.strip():
        return False, ["missing"]

    for name, text in (("headline", te_head), ("body", te_body)):
        if not looks_telugu(text):
            failures.append(f"not_telugu:{name}")

    # The Latin half of the blocklist is the engine's, unchanged.
    sebi = _sebi_check()
    if sebi:
        try:
            for hit in sebi(f"{te_head}\n{te_body}") or []:
                failures.append(f"compliance:{hit.strip()}")
        except Exception as e:                                    # noqa: BLE001
            log.info("[TELUGU] sebi check failed: %s", e)

    # ...and the Telugu half is this module's, mirrored from it.
    whole = f"{te_head}\n{te_body}"
    for word, mirror in FORBIDDEN_TE:
        if word in whole:
            failures.append(f"advice_te:{mirror}")

    for n in invented_figures(whole, f"{(en or {}).get('refined_title') or ''}\n"
                                     f"{(en or {}).get('summary') or ''}\n{en_body}"):
        failures.append(f"invented_number:{n}")

    en_words = len(en_body.split())
    te_words = len(te_body.split())
    if en_words:
        ratio = te_words / en_words
        if ratio < MIN_RATIO:
            failures.append("too_short")
        elif ratio > MAX_RATIO:
            failures.append("too_long")

    # De-duplicated but order-preserving: the same rule firing on headline and
    # body is one thing to fix, and the doctor's sample reads better for it.
    seen, out = set(), []
    for f in failures:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return (not out), out


# ─── 5. parsing and storage ───────────────────────────────────────────────────
# Stored as JSON-in-TEXT on the article row, like `strings`, `dots`,
# `who_affected` and `synthesis_sources` — the one shape that works unchanged
# over pgcompat/Postgres and the local sqlite backend.
MAX_HEAD = 160
MAX_SUMMARY = 500
MAX_BODY = 2600


def parse(raw) -> dict:
    """The model's `telugu` field, trimmed. {} when there is nothing usable."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw) if raw.strip() else {}
        except Exception:                                         # noqa: BLE001
            return {}
    if not isinstance(raw, dict):
        return {}
    out = {}
    for key, limit in (("refined_title", MAX_HEAD), ("summary", MAX_SUMMARY),
                       ("full_body", MAX_BODY)):
        v = raw.get(key)
        if isinstance(v, str) and v.strip():
            out[key] = v.strip()[:limit]
    return out if out.get("full_body") and out.get("refined_title") else {}


def for_card(stored) -> dict:
    """What the dossier endpoint serves. {} for a row with no translation, which
    is most of them — the surface shows the English card and no language pill."""
    return parse(stored)


def payload(te: dict) -> str:
    return json.dumps(te, ensure_ascii=False)


# ─── 6. the counters /admin/writer-doctor reads ───────────────────────────────
_STATS = {"offered": 0, "written": 0, "failed": 0, "rules": {}}


def note(outcome: str, failures=None) -> None:
    if outcome in _STATS:
        _STATS[outcome] += 1
    for f in failures or []:
        rule = str(f).split(":")[0]
        _STATS["rules"][rule] = _STATS["rules"].get(rule, 0) + 1


def stats() -> dict:
    out = dict(_STATS)
    out["rules"] = dict(sorted(_STATS["rules"].items(),
                               key=lambda kv: -kv[1]))
    offered = _STATS["offered"]
    out["pass_rate"] = (round(_STATS["written"] / offered, 3) if offered else None)
    out["enabled"] = enabled()
    out["min_script_ratio"] = MIN_SCRIPT_RATIO
    if not offered:
        out["DIAGNOSIS"] = ("No card has carried a telugu field yet — either the "
                            "drain has not run since the deploy, or the model is "
                            "omitting the field.")
    elif not _STATS["written"]:
        top = next(iter(out["rules"]), None)
        out["DIAGNOSIS"] = (f"Every translation failed; most-fought rule: {top}. "
                            "Read the prompt's rule 17 against that rule.")
    else:
        out["DIAGNOSIS"] = (f"{_STATS['written']} of {offered} translations "
                            f"published.")
    return out
