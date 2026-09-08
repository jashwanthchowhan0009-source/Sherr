"""proof/data.py — the only reads the proof run makes against the database.

Two sources, kept apart on purpose:

  market_ticks           the price series behind every PRICE signal and behind
                         RIL's forward reaction. sherrbyte_app.market_ticks,
                         schema-qualified.

  the FINANCIAL corpus   behind every NEWS signal. This is where the feed
                         separation is enforced structurally: the article query
                         binds `source_name = ANY($financial)`, and $financial is
                         feeds_financial.financial_sources() — the ONE registry.
                         There is no query here that reads articles without that
                         clause, so a general feed cannot reach a signal even by
                         accident.
"""

from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("sherbyte.proof.data")

# feeds_financial.py lives at the repo root beside main.py, the same place
# news_match.py reaches body_state from. One registry, imported, never copied.
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))


def financial_sources() -> list[str]:
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    import feeds_financial
    return feeds_financial.financial_sources()


# ─── prices ──────────────────────────────────────────────────────────────────
_SERIES_SQL = """
SELECT (ts AT TIME ZONE 'UTC') AS ts, price
  FROM sherrbyte_app.market_ticks
 WHERE symbol = $1
 ORDER BY ts
"""


async def load_series(conn, symbol: str) -> list:
    """Every stored close for one symbol, ascending: [(ts, close), ...].

    ts is returned as a naive UTC timestamp so it compares cleanly with the
    session anchors reaction.py builds; price is float for the pure math.
    """
    rows = await conn.fetch(_SERIES_SQL, symbol)
    return [(r["ts"], float(r["price"])) for r in rows]


# ─── the financial corpus (NEWS signals only) ────────────────────────────────
# published_at::text ~ guard, then ::timestamptz — the same handling news_match
# and event_library use, for the same reason (the column is TEXT under pgcompat
# and timestamptz after migration 018; both must work).
#
# source_name = ANY($3) is the structural feed gate. It is not optional and it is
# not a post-filter — it is in the WHERE clause of the only article read the
# proof makes.
_ARTICLES_SQL = """
SELECT id, headline, summary_60, full_body, source_summary, source_name,
       published_at
  FROM sherrbyte_app.articles
 WHERE status = 'published'
   AND source_name = ANY($3::text[])
   AND published_at::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
   AND published_at::timestamptz >= $1
   AND published_at::timestamptz <= $2
 ORDER BY published_at DESC
 LIMIT 800
"""


def _body_state():
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    import body_state
    return body_state


def _is_real(row) -> bool:
    """Skip placeholder rows — a stub summary is not evidence. Same check
    news_match makes; a firing must never be corroborated by an article that
    only says 'Sherr AI is preparing an original summary'."""
    bs = _body_state()
    return bs.row_is_healthy({
        "full_body": row.get("full_body") or "",
        "summary_60": row.get("summary_60") or "",
        "source_summary": row.get("source_summary") or "",
    })


async def financial_articles(conn, lo, hi) -> list:
    """Real, published, FINANCIAL-source articles in [lo, hi].

    Returns dicts with id + the text fields, so the caller can term-match each
    news topic without re-querying per topic — one window read serves all news
    signals for that window.
    """
    rows = [dict(r) for r in await conn.fetch(_ARTICLES_SQL, lo, hi, financial_sources())]
    out = []
    for r in rows:
        if not _is_real(r):
            continue
        r["_hay"] = ((r.get("headline") or "") + " " +
                     (r.get("summary_60") or "")).lower()
        out.append(r)
    return out


def match_topic(articles: list, terms: list) -> list:
    """Article ids whose headline+summary carries any of the topic's terms."""
    hits = []
    for a in articles:
        hay = a.get("_hay") or ""
        if any(t in hay for t in terms):
            hits.append(int(a["id"]))
    return hits


# ─── the hand-authored edges ─────────────────────────────────────────────────
_EDGES_SQL = """
SELECT edge_key, head, tail, mechanism, signal_keys
  FROM sherrbyte_app.ril_edges
 ORDER BY edge_key
"""


async def load_edges(conn) -> list:
    """The four edges, exactly as seeded by hand. The engine only ever READS
    this table — it has no write path to it, so it cannot invent an edge."""
    return [dict(r) for r in await conn.fetch(_EDGES_SQL)]
