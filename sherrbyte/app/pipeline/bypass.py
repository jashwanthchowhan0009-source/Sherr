"""
pipeline/bypass.py — release pending_rewrite articles without an AI call.

The AI rewrite path is untouched and still owns every NEW article. This is a
one-shot drain for the backlog that accumulated while the rewrite pass was not
running: classify by keyword, tidy the title, mark passed.

WHAT THIS DOES NOT DO. It does not make the text ours. A bypassed row keeps the
publisher's headline and body, so it is published on the strength of attribution
and a source link, not originality. Every row it touches records
`bypass_rewrite: true` in originality_json so these are distinguishable later from
rows that actually cleared the gate — a re-run of the rewrite pass can find them
with one query.
"""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import datetime, timezone

log = logging.getLogger("sherbyte.bypass")

# Run the startup drain only when the backlog is genuinely a backlog. A handful of
# rows is the rewrite pass being briefly behind, and bypassing those would race it.
STARTUP_THRESHOLD = 50
MAX_TITLE_CHARS = 120

# ─── rule-based classification ────────────────────────────────────────────────
# Pillar ids match config.PILLARS.
PILLAR_KEYWORDS: dict[int, tuple[str, ...]] = {
    1: ("election", "parliament", "government", "minister", "policy", "court",
        "supreme court", "high court", "police", "protest", "bjp", "congress",
        "cabinet", "bill", "law", "verdict", "diplomat", "border", "military",
        "army", "navy", "defence", "summit", "treaty", "sanction", "war"),
    2: ("market", "stock", "sensex", "nifty", "economy", "gdp", "inflation",
        "rbi", "bank", "rupee", "investor", "ipo", "revenue", "profit", "earnings",
        "startup", "funding", "merger", "acquisition", "tariff", "trade", "tax",
        "crypto", "bitcoin", "sebi", "budget", "fiscal", "export", "import"),
    3: ("ai", "artificial intelligence", "software", "chip", "semiconductor",
        "satellite", "isro", "nasa", "rocket", "launch", "research", "scientist",
        "study", "quantum", "robot", "cyber", "data", "algorithm", "openai",
        "google", "microsoft", "apple", "android", "app", "technology", "space"),
    4: ("film", "movie", "cinema", "actor", "actress", "music", "album", "song",
        "book", "author", "novel", "art", "gallery", "museum", "theatre",
        "festival", "award", "bollywood", "hollywood", "series", "netflix"),
    5: ("climate", "weather", "monsoon", "rain", "flood", "cyclone", "earthquake",
        "wildlife", "tiger", "forest", "pollution", "environment", "emission",
        "drought", "heatwave", "glacier", "biodiversity", "conservation"),
    6: ("health", "hospital", "doctor", "disease", "vaccine", "covid", "cancer",
        "mental health", "fitness", "nutrition", "diet", "medicine", "patient",
        "surgery", "wellness", "sleep", "therapy"),
    7: ("religion", "temple", "church", "mosque", "faith", "spiritual", "ethics",
        "philosophy", "belief", "ritual", "pilgrimage"),
    8: ("travel", "food", "recipe", "fashion", "lifestyle", "celebrity",
        "influencer", "social media", "instagram", "wedding", "restaurant"),
    9: ("cricket", "football", "match", "tournament", "ipl", "olympics", "medal",
        "player", "team", "score", "wicket", "goal", "fifa", "tennis", "f1",
        "athlete", "championship", "league", "esports", "gaming"),
}
DEFAULT_PILLAR = 1
# One incidental hit is not a topic. Two distinct keywords is a low bar that still
# rules out a story matching "match" once inside "matches".
MIN_KEYWORD_HITS = 2

# Word boundaries, not substrings: plain `in` fires "ai" inside "said", "rain",
# "chair" and "against", which appear in most articles and would drag the whole
# corpus into tech.
_PATTERNS = {
    pid: [(w, re.compile(rf"\b{re.escape(w)}(?:s|es)?\b", re.IGNORECASE))
          for w in words]
    for pid, words in PILLAR_KEYWORDS.items()
}


def classify(headline: str, body: str = "") -> tuple[int, dict]:
    """(pillar_id, evidence). Evidence is stored so a bad call is explainable."""
    text = f"{headline or ''} {body or ''}"
    scores: dict[int, list[str]] = {}
    for pid, pats in _PATTERNS.items():
        hits = [w for w, rx in pats if rx.search(text)]
        if hits:
            scores[pid] = hits
    if not scores:
        return DEFAULT_PILLAR, {"matched": [], "reason": "no keyword matched"}
    if max(len(h) for h in scores.values()) < MIN_KEYWORD_HITS:
        return DEFAULT_PILLAR, {"matched": [], "weak_signals": dict(sorted(
            (p, h) for p, h in scores.items())),
            "reason": f"fewer than {MIN_KEYWORD_HITS} distinct keywords"}
    # Most distinct keywords wins; ties break to the lower id so runs are stable.
    best = max(sorted(scores), key=lambda p: len(scores[p]))
    return best, {"matched": scores[best][:8], "score": len(scores[best]),
                  "runners_up": {p: len(h) for p, h in sorted(scores.items())
                                 if p != best}}


# ─── title cleanup ────────────────────────────────────────────────────────────
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
# Feed furniture publishers prepend, stripped so the title starts at the story.
_PREFIX = re.compile(r"^\s*(breaking|exclusive|live|just in|update|watch|video)\s*[:\-|]\s*",
                     re.IGNORECASE)


def clean_title(raw: str, limit: int = MAX_TITLE_CHARS) -> str:
    """Strip HTML, unescape entities, drop feed prefixes, truncate on a word.

    Unescaping happens twice because RSS routinely double-encodes: a title arrives
    as `&amp;amp;` and one pass leaves a visible `&amp;` in the app.
    """
    if not raw:
        return ""
    # Unescape BEFORE stripping tags. The other order leaves "&lt;em&gt;Live&lt;/em&gt;"
    # as a literal "<em>Live</em>" in the app, because the tags were still entities
    # when the tag regex ran. Twice, because RSS routinely double-encodes: one pass
    # on "&amp;amp;" leaves a visible "&amp;".
    t = html.unescape(html.unescape(raw))
    t = _TAG.sub(" ", t)
    t = _PREFIX.sub("", t.strip())
    t = _WS.sub(" ", t).strip(" -|–—")
    if len(t) <= limit:
        return t
    # Cut on a word boundary rather than mid-word, then mark the truncation.
    cut = t[:limit].rsplit(" ", 1)[0].rstrip(" ,;:-")
    return (cut or t[:limit]).rstrip() + "…"


def build(row: dict) -> dict:
    """Everything one row needs, computed. Pure — no DB, so it is testable."""
    title = clean_title(row.get("headline") or row.get("title") or "")
    pillar, evidence = classify(title, row.get("body") or "")
    return {
        "id": row.get("id"),
        "headline": title,
        "pillar_id": pillar,
        "audit": {
            "bypass_rewrite": True,
            "note": ("published without an AI rewrite — publisher headline and body "
                     "retained, attribution and source link carry it"),
            "classification": evidence,
            "at": datetime.now(timezone.utc).isoformat(),
        },
    }


# ─── the drain ────────────────────────────────────────────────────────────────
async def pending_count(conn) -> int:
    try:
        return int(await conn.fetchval(
            "SELECT COUNT(*) FROM info_objects WHERE status = 'pending_rewrite'") or 0)
    except Exception as e:
        log.warning("pending count unavailable: %s", e)
        return 0


async def bypass_rewrite(conn, *, limit: int | None = None,
                         dry_run: bool = False) -> dict:
    """Classify and pass every pending_rewrite row. Returns a summary.

    Never raises: this runs from startup and from an admin route, and neither
    should be taken down by a backlog drain.
    """
    try:
        for stmt in (
            "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'published'",
            "ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS originality_json JSONB DEFAULT '{}'::jsonb",
        ):
            await conn.execute(stmt)

        q = ("SELECT id, headline, body FROM info_objects "
             "WHERE status = 'pending_rewrite' ORDER BY created_at DESC")
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = await conn.fetch(q)

        by_pillar: dict[int, int] = {}
        updates = []
        for r in rows:
            b = build(dict(r))
            by_pillar[b["pillar_id"]] = by_pillar.get(b["pillar_id"], 0) + 1
            updates.append((b["headline"], b["pillar_id"],
                            json.dumps(b["audit"]), b["id"]))

        if not dry_run and updates:
            await conn.executemany(
                "UPDATE info_objects SET headline = $1, pillar_id = $2, "
                "status = 'passed', originality_json = $3::jsonb WHERE id = $4",
                updates)

        out = {"found": len(rows), "passed": 0 if dry_run else len(updates),
               "dry_run": dry_run, "by_pillar": dict(sorted(by_pillar.items()))}
        log.info("bypass_rewrite: %s", out)
        return out
    except Exception as e:
        log.error("bypass_rewrite failed: %s", e, exc_info=True)
        return {"error": str(e), "found": 0, "passed": 0}


async def run_once_on_startup(conn) -> dict:
    """Drain only if the backlog is over the threshold. Called from lifespan."""
    n = await pending_count(conn)
    if n <= STARTUP_THRESHOLD:
        log.info("bypass_rewrite skipped: %d pending (threshold %d)",
                 n, STARTUP_THRESHOLD)
        return {"skipped": True, "pending": n, "threshold": STARTUP_THRESHOLD}
    log.warning("bypass_rewrite: draining %d pending articles", n)
    return await bypass_rewrite(conn)
