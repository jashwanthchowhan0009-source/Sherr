"""
financial_feeds.py — the ONE list of sources the market engines are allowed to see.

WHY THIS FILE EXISTS
====================
Sherr-I's analog engine and market_reaction detector answer "what news
accompanied this price move". Fed the whole 87-feed general corpus, they will
happily link a silver price move to a video-game guide that happens to contain
the word "silver" — a real bug this product hit. The two market engines must
only ever read financial reporting.

The separation is enforced in the SCHEMA, not by a filter each caller
remembers to add: every ingested row is stamped with a `feed_class`
(`financial` | `general`) at write time, and the market engines carry
`WHERE feed_class = 'financial'` in their SQL. A new caller that forgets the
filter reads nothing market-relevant by accident rather than everything.

This module is that stamp's single source of truth. Both ingestion paths — the
root app (`main.py`) and the SPIE pipeline (`sherrbyte/app/pipeline/collector.py`)
— import `FINANCIAL_FEEDS` and classify by `feed_class(source_name)`, so a
source is financial in exactly one place and can never be financial to one
pipeline and general to the other.

Pure data + one function. No imports beyond stdlib, so it is safe to load from
either package without dragging dependencies along.
"""

from __future__ import annotations

FINANCIAL = "financial"
GENERAL = "general"

# Indian financial reporting + commodity sources. Each entry is (url, source_name).
# The source_name is what gets written to articles.source_name / info_objects.
# source_name, and it is the key feed_class() classifies on — keep it stable.
#
# Some of these publishers rate-limit or move their RSS paths; the collectors
# already skip a feed that 404s or times out, so an occasionally-dead URL costs
# coverage, never a crash. Breadth here is deliberate: the market engines need a
# real spread of financial publishers to corroborate a move across sources.
FINANCIAL_FEEDS: list[tuple[str, str]] = [
    # ── Indian markets desks ──────────────────────────────────────────────────
    ("https://www.livemint.com/rss/markets", "Mint Markets"),
    ("https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "ET Markets"),
    ("https://www.business-standard.com/rss/markets-106.rss", "Business Standard Markets"),
    ("https://www.moneycontrol.com/rss/marketreports.xml", "Moneycontrol Markets"),
    ("https://www.moneycontrol.com/rss/business.xml", "Moneycontrol Business"),
    ("https://www.moneycontrol.com/rss/results.xml", "Moneycontrol Results"),
    ("http://feeds.reuters.com/reuters/INbusinessNews", "Reuters India Business"),
    # ── Exchange & regulator filings ──────────────────────────────────────────
    ("https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml", "NSE Announcements"),
    ("https://www.bseindia.com/data/xml/notices.xml", "BSE Announcements"),
    ("https://www.rbi.org.in/pressreleases_rss.xml", "RBI Press Releases"),
    ("https://www.rbi.org.in/notifications_rss.xml", "RBI Notifications"),
    ("https://www.sebi.gov.in/sebirss.xml", "SEBI Press Releases"),
    # ── Commodities ───────────────────────────────────────────────────────────
    ("https://www.moneycontrol.com/rss/commodity.xml", "Moneycontrol Commodities"),
    ("https://oilprice.com/rss/main", "OilPrice.com"),
]

# The set the stamp checks against. Derived from FINANCIAL_FEEDS so the list
# above is the only thing to edit.
FINANCIAL_SOURCE_NAMES: frozenset[str] = frozenset(name for _url, name in FINANCIAL_FEEDS)


def feed_class(source_name: str) -> str:
    """`financial` for a source in FINANCIAL_SOURCE_NAMES, else `general`.

    Exact-match on the stored source_name. There is no fuzzy matching on
    purpose: "Mint Markets" is financial and the general "Mint" top-news feed is
    not, and a substring rule would collapse that distinction.
    """
    return FINANCIAL if (source_name or "").strip() in FINANCIAL_SOURCE_NAMES else GENERAL
