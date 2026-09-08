"""feeds_financial.py — the ONE registry of Indian financial sources.

WHY THIS FILE EXISTS, AND WHY IT IS SEPARATE FROM RSS_FEEDS
==========================================================
Sherr-I once linked a silver move to video-game guide articles: the financial
signal path read the whole corpus and matched on the word "silver". The fix is
not another `WHERE title NOT LIKE '%game%'` filter — a filter is something a
future query forgets. The fix is structural: there is exactly one set of source
names the financial signal path is allowed to read, it lives here, and every
financial reader derives its SQL from `FINANCIAL_SOURCES`. A general feed can
never reach the signal path because its name is not in the set, not because a
filter happened to exclude it.

TWO ROLES, ONE SET
==================
  FINANCIAL_FEEDS     RSS the ingest should also pull (markets desks, exchange
                      and regulator releases, crude and metals). Root main.py
                      appends these to RSS_FEEDS so the rows land in the corpus
                      with these source names.
  FINANCIAL_SOURCES   every source_name the signal path may read. It is the feed
                      names above UNION the financial source names already in the
                      368-day corpus (Mint, Economic Times, ...), so a backwards
                      run over history actually finds those rows.

General feeds stay in RSS_FEEDS untouched — they feed the consumer news tabs.
They are simply absent from FINANCIAL_SOURCES, so the signal path never sees
them.

No percentages, no new asset classes, no other companies are introduced here —
this only decides which publishers count as financial evidence.
"""

from __future__ import annotations

# (url, source_name). Kept as (url, name) to match RSS_FEEDS' shape exactly, so
# root main.py can concatenate the two lists with no adapter.
#
# A feed that 404s or blocks scraping costs only itself — the collector already
# tolerates a non-200 per feed. Its NAME still governs the whitelist, so any
# row already in the corpus under that name is usable even when the live feed is
# down. Exchange (BSE/NSE) and regulator (RBI/SEBI) endpoints are the fragile
# ones; they are listed for their names first, their URLs best-effort.
FINANCIAL_FEEDS: list[tuple[str, str]] = [
    # ── Market desks (Indian) ────────────────────────────────────────────────
    ("https://www.livemint.com/rss/markets",                              "Mint Markets"),
    ("https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "ET Markets"),
    ("https://www.business-standard.com/rss/markets-106.rss",             "Business Standard Markets"),
    ("https://www.moneycontrol.com/rss/marketreports.xml",               "Moneycontrol Markets"),
    ("https://www.moneycontrol.com/rss/business.xml",                    "Moneycontrol Business"),
    ("https://www.reutersagency.com/feed/?best-sectors=business-finance&post_type=best", "Reuters India Business"),
    # ── Exchanges: corporate announcements ───────────────────────────────────
    ("https://www.bseindia.com/data/xml/notices.xml",                    "BSE Announcements"),
    ("https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml", "NSE Announcements"),
    # ── Regulators ───────────────────────────────────────────────────────────
    ("https://www.rbi.org.in/pressreleases_rss.xml",                     "RBI Releases"),
    ("https://www.sebi.gov.in/sebirss.xml",                              "SEBI Releases"),
    # ── Crude and metals (the priced inputs behind the RIL edges) ────────────
    ("https://oilprice.com/rss/main",                                    "OilPrice.com"),
    ("https://www.mining.com/feed/",                                     "Mining.com"),
]

# The financial source names already present in the 368-day corpus. A backwards
# run reads history, and history was ingested under THESE names, not the desk
# names above. Both belong in the whitelist.
_CORPUS_FINANCIAL_SOURCES: frozenset[str] = frozenset({
    "Mint",
    "Economic Times",
    "MoneyControl",
    "Business Standard",
    "OilPrice.com",
    "Reuters",
})

# The one set the signal path is allowed to read. Anything not here is a consumer
# feed and can never become financial evidence.
FINANCIAL_SOURCES: frozenset[str] = frozenset(
    name for _url, name in FINANCIAL_FEEDS
) | _CORPUS_FINANCIAL_SOURCES


def financial_sources() -> list[str]:
    """The whitelist as a sorted list — the parameter every financial-path
    query binds to. One place builds it; no query hand-rolls its own."""
    return sorted(FINANCIAL_SOURCES)


def feed_class(source_name: str) -> str:
    """'financial' for a source in FINANCIAL_SOURCES, else 'general'.

    This is the second belt beside the query whitelist: ingest stamps every row's
    `feed_class` column from THIS function, so the persisted column and the
    `source_name = ANY(financial_sources())` filter derive from one set and can
    never disagree. Exact-match on the stored source_name — no fuzzy rule, so
    "Mint Markets" is financial and any general feed is not.
    """
    return "financial" if (source_name or "").strip() in FINANCIAL_SOURCES else "general"
