"""
llm_bounds.py — the size ceiling on anything a model wrote, before it is stored.

THE GAP THIS CLOSES, measured rather than assumed. Both validators in this repo
bound SOME of what the model returns and not the rest, and the split is not
deliberate — it is where somebody happened to add a cap:

  ai_processor._validate_and_fix   `refined_title` 12 words, `summary` 55 words.
                                   `full_body` has a MINIMUM and no maximum, and
                                   neither do `what_info`, `why_info`,
                                   `how_info`, `who_subject`, `when_info`,
                                   `where_info`. List items are unbounded too.

  synthesis.parse_synthesis        `headline` 12 words; `numbers_used` items cut
                                   at 60/120 chars; every list length bounded.
                                   But `content` has a MINIMUM and no maximum,
                                   and the items of `entities`, `fact_conflicts`
                                   and `who_affected` have no per-item cap — the
                                   module is inconsistent with itself.

Driving `_validate_and_fix` with a long response measured **2.1MB written to one
row**, 300KB in each of seven columns. The output-token ceiling in `_out_tokens`
bounds this in the ordinary case, but it is a per-provider request parameter,
not a property of the column — a provider that ignores it, a cascade fallback
with a different limit, or a response that repeats itself all land in the
database at whatever size the model chose.

That is the same rule CLAUDE.md already states for fetches, one layer over: **a
payload whose size is set by a remote server needs the cap applied on our side.**
It matters here specifically because the database has already been over its size
quota (egress ~487%, size ~248%) and the instance has already been OOM-killed —
and because a 300KB body is served, so one such row makes every `/feed` page
that contains it 300KB heavier for every reader.

CLAMP, NEVER REJECT. A response that overran is still a usable answer, and
rejecting it would send the row back to its placeholder for the next tick to
fetch and fail on identically — the forever-placeholder failure mode CLAUDE.md
records for `no_source_material`. Prose is cut on a WORD boundary so a clamped
body does not end mid-word.

THE CAPS ARE DELIBERATELY FAR ABOVE EVERY LEGITIMATE VALUE. `LENGTH_BANDS` tops
out at 200 words (the Research band), so `MAX_BODY_CHARS` at 8,000 is roughly
six times the longest body any writer is asked for. A clamp firing is therefore
evidence the PROMPT is wrong, not that the cap is tight, which is why every one
is counted and reported rather than applied silently — the same argument
`/admin/writer-doctor` makes for its own buckets.

PURE. No database, no model, no network.
"""

from __future__ import annotations

import logging
from collections import Counter

log = logging.getLogger("sherbyte.llm_bounds")

# ─── the caps ────────────────────────────────────────────────────────────────
# Characters, not words: the column is sized in bytes and a word count does not
# bound one (a "word" can be 9,000 characters long, which is how the unbounded
# list items got through).
MAX_TITLE_CHARS = 300        # 12 words, generously
MAX_SUMMARY_CHARS = 1_200    # 55 words, generously
MAX_BODY_CHARS = 8_000       # ~6x the longest band any writer is asked for
MAX_FIELD_CHARS = 500        # one WWWH answer: what / why / how / who / where
MAX_WHEN_CHARS = 120         # a date or a phrase, never prose
MAX_ITEM_CHARS = 120         # one entity, one affected party, one conflict
MAX_TAG_CHARS = 60           # one micro-tag

# Per-field cap for the card rewrite's dict, by key. A key absent from here is
# not clamped — `clamp_card` therefore cannot silently start truncating a field
# somebody adds later without them choosing a cap for it.
CARD_CAPS: dict[str, int] = {
    "refined_title": MAX_TITLE_CHARS,
    "summary": MAX_SUMMARY_CHARS,
    "full_body": MAX_BODY_CHARS,
    "what_info": MAX_FIELD_CHARS,
    "why_info": MAX_FIELD_CHARS,
    "how_info": MAX_FIELD_CHARS,
    "who_subject": MAX_FIELD_CHARS,
    "where_info": MAX_FIELD_CHARS,
    "when_info": MAX_WHEN_CHARS,
}

CARD_LIST_CAPS: dict[str, int] = {
    "topic_tags": MAX_TAG_CHARS,
    "who_affected": MAX_ITEM_CHARS,
}

# The keys are `parse_synthesis`'s OWN return keys, not the model's JSON keys —
# `extracted_entities` rather than `entities`. The clamp runs on the parsed
# contract, after the parser has renamed and validated, so the names have to be
# the ones in that dict or the cap silently applies to nothing.
#
# `strings`, `dots` and `numbers_used` are deliberately ABSENT: they are nested
# structures whose own parsers already bound both the list length and each
# field, and a flat text clamp over a list of dicts would drop them entirely.
SYNTH_CAPS: dict[str, int] = {
    "headline": MAX_TITLE_CHARS,
    "content": MAX_BODY_CHARS,
    "hook": MAX_SUMMARY_CHARS,
    "why_info": MAX_FIELD_CHARS,
    "who_subject": MAX_FIELD_CHARS,
    "primary_source_attribution": MAX_ITEM_CHARS,
}

SYNTH_LIST_CAPS: dict[str, int] = {
    "extracted_entities": MAX_ITEM_CHARS,
    "fact_conflicts": MAX_ITEM_CHARS,
    "who_affected": MAX_ITEM_CHARS,
}


# ─── counters ────────────────────────────────────────────────────────────────
# Per process, the same stated bound /admin/memory and /admin/you carry. Bounded
# in size by construction: the keys are field names from the tables above, so
# this dict cannot grow with traffic.
_CLAMPS: Counter = Counter()
_WORST: dict[str, int] = {}


def _note(field: str, original_len: int) -> None:
    _CLAMPS[field] += 1
    if original_len > _WORST.get(field, 0):
        _WORST[field] = original_len
    log.warning("[LLM_BOUNDS] clamped %s: %d chars -> cap. A clamp here means "
                "the prompt is asking for more than the band allows.",
                field, original_len)


def stats() -> dict:
    return {"clamps": dict(_CLAMPS),
            "worst_seen_chars": dict(_WORST),
            "total": sum(_CLAMPS.values())}


def reset() -> None:
    """Called from the test conftest — module-level counters would otherwise
    carry one test's clamps into the next."""
    _CLAMPS.clear()
    _WORST.clear()


def diagnose() -> str:
    if not _CLAMPS:
        return ("No model output has been clamped in this process. The caps are "
                "a ceiling, not a formatter — this is the expected reading.")
    worst = max(_WORST.items(), key=lambda kv: kv[1])
    fields = ", ".join(f"{k} x{v}" for k, v in _CLAMPS.most_common(4))
    return (f"{sum(_CLAMPS.values())} clamp(s): {fields}. Largest was "
            f"{worst[0]} at {worst[1]} chars. A clamp is evidence the PROMPT is "
            f"asking for more than the band allows — raise it in the prompt or "
            f"the band, not here.")


# ─── clamping ────────────────────────────────────────────────────────────────
def clamp_text(value, cap: int, *, field: str = "", words: bool = True) -> str:
    """Return `value` as a string of at most `cap` characters.

    `words=True` cuts at the last whitespace inside the cap, so a clamped body
    does not end mid-word. It falls back to a hard cut when there is no
    whitespace to cut at — a single enormous token is exactly the case a word
    boundary cannot help with, and it is the one that must still be bounded.
    """
    s = "" if value is None else str(value)
    if len(s) <= cap:
        return s
    if field:
        _note(field, len(s))
    cut = s[:cap]
    if words:
        sp = cut.rfind(" ")
        # Only honour the boundary if it keeps most of the budget; a string whose
        # single space is at character 3 would otherwise be cut to 3 characters.
        if sp > cap * 0.6:
            cut = cut[:sp]
    return cut.rstrip()


def clamp_items(values, cap: int, *, field: str = "", max_items: int) -> list:
    """Clamp each item of a list, and the list's length.

    Non-string scalars are kept (an int tag is a tag); anything else is dropped,
    because a nested dict or list in a flat field is not a value that was
    shortened — it is the wrong shape, and storing its `repr` would put Python
    syntax on a card.
    """
    if not isinstance(values, (list, tuple)):
        return []
    out = []
    for v in values:
        if isinstance(v, bool) or not isinstance(v, (str, int, float)):
            continue
        s = clamp_text(v, cap, field=field, words=False).strip()
        if s:
            out.append(s)
        if len(out) >= max_items:
            break
    return out


def clamp_card(result: dict, *, caps: dict | None = None,
               list_caps: dict | None = None) -> dict:
    """Clamp every known model-written field of a card dict, in place.

    Only keys PRESENT in the dict and NAMED in the caps table are touched, so
    this can never invent a field or clamp one nobody chose a cap for.
    """
    if not isinstance(result, dict):
        return result
    caps = CARD_CAPS if caps is None else caps
    list_caps = CARD_LIST_CAPS if list_caps is None else list_caps

    for key, cap in caps.items():
        if key in result and result[key] is not None:
            result[key] = clamp_text(result[key], cap, field=key)
    for key, cap in list_caps.items():
        if key in result:
            # The length limits are the callers' own and already applied there;
            # this bounds them again so a list can never arrive unbounded from a
            # path that forgot to.
            result[key] = clamp_items(result[key], cap, field=key, max_items=16)
    return result


def clamp_synthesis(result: dict) -> dict:
    """The synthesis contract's own fields."""
    return clamp_card(result, caps=SYNTH_CAPS, list_caps=SYNTH_LIST_CAPS)


def row_chars(result: dict) -> int:
    """How much a dict would write. Used by the tests to assert the bound, and
    reported so the ceiling is a number somebody can check rather than a
    claim."""
    if not isinstance(result, dict):
        return 0
    total = 0
    for v in result.values():
        if isinstance(v, (list, tuple)):
            total += sum(len(str(x)) for x in v)
        elif v is not None:
            total += len(str(v))
    return total
