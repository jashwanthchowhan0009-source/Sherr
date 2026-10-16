"""Turn ranked stories into narration segments. Templates only — no model.

Every word spoken is either a template from format.json or text the SherrByte
writer already produced and published (headline, hook, summary_60). Nothing is
generated here, so nothing here can invent a fact.
"""
from __future__ import annotations

import re

_SENT = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])")
_ABBR = re.compile(r"\b(U\.S|U\.K|Mr|Mrs|Dr|St|Inc|Ltd|Co|vs|No)\.$")

NUM_WORDS = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight",
             "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
             "sixteen", "seventeen", "eighteen", "nineteen", "twenty"]


def sentences(text: str) -> list[str]:
    parts, buf = [], ""
    for p in _SENT.split(" ".join(str(text or "").split())):
        buf = f"{buf} {p}".strip() if buf else p
        if _ABBR.search(buf):
            continue
        parts.append(buf)
        buf = ""
    if buf:
        parts.append(buf)
    return [s.strip() for s in parts if s.strip()]


def _same(a: str, b: str) -> bool:
    n = lambda s: re.sub(r"[^a-z0-9 ]", "", (s or "").lower()).strip()
    return n(a) == n(b) or (n(a) and n(a) in n(b)) or (n(b) and n(b) in n(a))


def clean_headline(h: str) -> str:
    h = " ".join(str(h or "").split()).strip()
    return h.rstrip(".").strip()


def story_line(row: dict, max_words: int) -> str:
    """The one sentence spoken after the headline: hook first, then summary."""
    head = row.get("headline") or ""
    for src in (row.get("hook"), row.get("summary_60")):
        for s in sentences(src or ""):
            if _same(s, head) or len(s.split()) < 6:
                continue
            words = s.split()
            if len(words) > max_words:
                # cut at the last clause boundary inside the limit, never mid-thought
                cut = " ".join(words[:max_words])
                k = max(cut.rfind(", "), cut.rfind("; "), cut.rfind(" — "))
                if k < len(cut) * 0.5:
                    continue
                s = cut[:k].rstrip(",;— ") + "."
            return s if s.endswith((".", "!", "?")) else s + "."
    return ""


def display_line(row: dict, max_words: int = 26) -> str:
    return story_line(row, max_words)


def _pick(options, key: int):
    if isinstance(options, str):
        return options
    return options[key % len(options)] if options else ""


def _daypart(hour: int) -> str:
    return "morning" if hour < 12 else ("afternoon" if hour < 17 else "evening")


def build(stories: list[dict], fmt: dict, hour: int = 9) -> list[dict]:
    """Narration in the mascot's voice. Variety comes from picking among fixed
    templates by story id — deterministic, so a re-run of the same day says the
    same thing, and nothing a template says is a claim about the story."""
    sc = fmt["script"]
    n = len(stories)
    order = list(range(n))
    if fmt.get("order", "countdown") == "countdown":
        order = order[::-1]           # best story is told LAST, as #1
    every = max(1, int(sc.get("aside_every", 2)))
    segs = []
    for pos, i in enumerate(order):
        r = stories[i]
        rank = i + 1
        key = int(r.get("id") or rank)
        if pos == 0:
            tpl = sc["first"]
        elif pos == n - 1:
            tpl = sc["last"]
        elif pos == n - 2:
            tpl = sc["second_last"]
        else:
            tpl = _pick(sc["middle"], key + pos)
        aside = ""
        if pos % every == 1:
            aside = _pick(sc.get("asides", {}).get(str(r.get("pillar_id") or 1), []), key)
            aside = (aside + " ") if aside else ""
        line = story_line(r, int(sc.get("max_line_words", 30)))
        txt = tpl.format(rank=NUM_WORDS[rank] if rank < len(NUM_WORDS) else rank,
                         headline=clean_headline(r.get("headline")), line=line, aside=aside)
        segs.append({"kind": "story", "rank": rank, "story": r, "text": " ".join(txt.split()),
                     "display_line": display_line(r)})
    ctx = dict(count=NUM_WORDS[n] if n < len(NUM_WORDS) else n,
               brand=fmt["brand"], brand_spoken=fmt.get("brand_spoken", fmt["brand"]),
               mascot=fmt.get("mascot", {}).get("name", "Sherr"))
    intro = sc["intro"]
    intro = intro.get(_daypart(hour), next(iter(intro.values()))) if isinstance(intro, dict) else intro
    segs.insert(0, {"kind": "intro", "text": intro.format(**ctx)})
    segs.append({"kind": "outro", "text": sc["outro"].format(**ctx)})
    # On screen the brand is written the way it is spelled, not the way it is said.
    spoken = ctx["brand_spoken"]
    shown = fmt.get("brand_display", "SherrByte")
    for s in segs:
        s["caption"] = s["text"].replace(spoken, shown)
    return segs
