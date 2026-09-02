"""
discovery/news_match.py — real articles behind a triggered symbol.

WHY THIS IS NOT market_reaction._news_window. That matcher joins `domain_signals`
to `info_objects` inside the engine's schema, over a 48h window, and it is right
for what it does. This one differs on three points that matter here:

  window   +/- 12h, and it is NEVER widened. A match found by relaxing the
           window is not evidence that the news relates to the move; it is
           evidence that the window was relaxed until something appeared.
  source   sherrbyte_app.articles — the corpus a reader can actually open. An
           insight citing a story the app cannot display is not citable.
  STUBS    an article whose body or summary is still a placeholder is SKIPPED.
           A placeholder is not evidence. This is the check nothing else had,
           and it is why a card could previously cite four articles that all
           said "Sherr AI is preparing an original summary of this story."

Zero real matches returns an empty list. The caller then does not build a card,
and no LLM call happens.
"""

from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("sherbyte.detectors.news_match")

WINDOW_HOURS = int(os.getenv("SHERR_I_NEWS_MATCH_HOURS", "12"))
# The card needs at least this many REAL articles before it is worth writing.
MIN_ARTICLES = int(os.getenv("SHERR_I_CARD_MIN_ARTICLES", "2"))

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))


def _body_state():
    """body_state lives at the repo root beside main.py, not in this package."""
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    import body_state
    return body_state


# Term matching happens in Python rather than a join through entity_ids: this
# reads the APP's articles table, which has no resolved entity ids — that
# resolution happens on the engine side, over info_objects.
#
# published_at IS TEXT, NOT timestamptz. main.py's schema is sqlite-shaped and
# reaches Postgres through pgcompat, so the column holds an ISO string. Comparing
# it to a timestamp parameter fails outright, and comparing it as TEXT is worse:
# it would silently half-work, because "2026-08-28 12:00" (what now()::text
# writes) and "2026-08-28T12:00" (what the ingest writes) do not sort together
# across the 'T'. The cast makes both parse.
#
# NULLIF + the regex guard: a row with an unparseable stamp would abort the whole
# query, and one bad row must not cost the entire match.
_SQL = """
SELECT id, headline, summary_60, full_body, source_summary, source_name, url,
       published_at
  FROM sherrbyte_app.articles
 WHERE status = 'published'
   -- published_at::text, NOT published_at. The column is TEXT under the
   -- sqlite-shaped schema and timestamptz once migration 018 has run, and
   -- this query must work against both. Applying ~ to a timestamptz raises
   -- "operator does not exist: timestamp with time zone ~ unknown" and
   -- takes the whole pass down. ::text is a no-op on TEXT and always valid
   -- on timestamptz, so the guard means the same thing either way.
   AND published_at::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
   AND published_at::timestamptz >= $1
   AND published_at::timestamptz <= $2
 ORDER BY published_at DESC
 LIMIT 400
"""


def _terms(names: list) -> list:
    """Match terms from entity names. Multi-word names also contribute their
    longest token, so "Reserve Bank of India" matches a headline saying "RBI
    ... Reserve Bank"—but single short tokens are dropped, because "Gas" or
    "Oil" alone match half the corpus."""
    out = []
    for n in names or []:
        n = (n or "").strip()
        if len(n) < 3:
            continue
        out.append(n.lower())
        for tok in n.split():
            if len(tok) >= 5:
                out.append(tok.lower())
    return list(dict.fromkeys(out))


def score_article(row, terms: list) -> int:
    """How many distinct terms the headline or summary carries."""
    hay = ((row.get("headline") or "") + " " + (row.get("summary_60") or "")).lower()
    return sum(1 for t in terms if t in hay)


def is_real(row) -> bool:
    """True when the article carries our own words in BOTH columns.

    A stub body renders as a placeholder in the reader's article view; a stub
    summary renders as one on the card. Either makes the article unusable as
    evidence, so both are checked.
    """
    bs = _body_state()
    return bs.row_is_healthy({
        "full_body": row.get("full_body") or "",
        "summary_60": row.get("summary_60") or "",
        "source_summary": row.get("source_summary") or "",
    })


async def match(conn, symbol_names: list, ts, *, hours: int = None,
                limit: int = 6) -> list:
    """Published articles within +/- `hours` of `ts` mentioning any name.

    `symbol_names` should be the instrument plus its one-hop downstream
    entities, so news about airlines counts as evidence for a crude move.
    """
    hours = WINDOW_HOURS if hours is None else int(hours)
    terms = _terms(symbol_names)
    if not terms or ts is None:
        return []

    from datetime import timedelta
    lo, hi = ts - timedelta(hours=hours), ts + timedelta(hours=hours)
    rows = [dict(r) for r in await conn.fetch(_SQL, lo, hi)]

    scored = []
    skipped_stub = 0
    for r in rows:
        overlap = score_article(r, terms)
        if not overlap:
            continue
        if not is_real(r):
            skipped_stub += 1
            continue
        scored.append({
            "article_id": r["id"], "title": r["headline"],
            "source": r.get("source_name") or "", "url": r.get("url") or "",
            "published_at": r.get("published_at"),
            "overlap": overlap,
        })

    # Overlap first, then recency. A story naming both the instrument and a
    # downstream entity is better evidence than one naming only the instrument.
    scored.sort(key=lambda a: (a["overlap"], a["published_at"] or 0), reverse=True)
    if skipped_stub:
        log.info("news_match: skipped %d placeholder article(s) — not evidence",
                 skipped_stub)
    return scored[:limit]
