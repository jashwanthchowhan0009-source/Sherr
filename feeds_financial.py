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

# THE STRUCTURED REGISTRY — one Feed per financial source, with the metadata the
# exposure brief (Phase A2) requires on every row:
#
#   weight        source_weight — how much this source vouches for a row.
#                 1.0 for primary/official (exchange, regulator, central bank,
#                 official statistics); 0.6 for financial press.
#   jurisdiction  IN | US | GLOBAL — where the source sits. Macro feeds transmit
#                 INTO India (Fed, EIA, OPEC), so they are kept and labelled.
#   licence       commercial_ok | attribution_required | blocked. SherrByte is a
#                 paid product, so a source's licence is explicit, never assumed.
#                 - Government/regulatory/exchange/official-statistics output is
#                   public record → commercial_ok.
#                 - Financial press headlines are used with a Source: credit and
#                   an original rewrite → attribution_required.
#                 - A `blocked` feed NEVER enters the ingest list below and its
#                   name is NOT in the whitelist — it cannot become evidence.
#
# A feed that 404s or blocks scraping costs only itself — the collector already
# tolerates a non-200 per feed. Its NAME still governs the whitelist, so a row
# already in the corpus under that name is usable even when the live feed is
# down. Exchange (BSE/NSE), regulator (RBI/SEBI/MCA/IBBI) and macro endpoints are
# the fragile ones; they are listed for their names first, their URLs best-effort.
from dataclasses import dataclass


@dataclass(frozen=True)
class Feed:
    url: str
    name: str
    weight: float          # source_weight: 1.0 primary/official · 0.6 press
    jurisdiction: str      # IN | US | GLOBAL
    licence: str           # commercial_ok | attribution_required | blocked


_PRIMARY, _PRESS = 1.0, 0.6

_FEEDS: list[Feed] = [
    # ── Exchanges: corporate announcements (primary, public record) ───────────
    Feed("https://www.bseindia.com/data/xml/notices.xml",                    "BSE Announcements", _PRIMARY, "IN", "commercial_ok"),
    Feed("https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml", "NSE Announcements", _PRIMARY, "IN", "commercial_ok"),
    # ── Regulators (primary, public record) ──────────────────────────────────
    Feed("https://www.rbi.org.in/pressreleases_rss.xml",                     "RBI Releases",        _PRIMARY, "IN", "commercial_ok"),
    Feed("https://www.rbi.org.in/notifications_rss.xml",                     "RBI Notifications",   _PRIMARY, "IN", "commercial_ok"),
    Feed("https://www.rbi.org.in/Speeches_RSS.xml",                          "RBI Speeches",        _PRIMARY, "IN", "commercial_ok"),
    Feed("https://www.sebi.gov.in/sebirss.xml",                              "SEBI Releases",       _PRIMARY, "IN", "commercial_ok"),
    Feed("https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRss=yes&rssType=ordersRss", "SEBI Orders", _PRIMARY, "IN", "commercial_ok"),
    Feed("https://www.mca.gov.in/bin/dms/getdocument?rss=whatsnew",          "MCA Notices",         _PRIMARY, "IN", "commercial_ok"),
    Feed("https://ibbi.gov.in/rss/notifications.xml",                        "IBBI Notices",        _PRIMARY, "IN", "commercial_ok"),
    # ── Financial press (Indian) — attribution + original rewrite ─────────────
    Feed("https://www.livemint.com/rss/markets",                            "Mint Markets",        _PRESS, "IN", "attribution_required"),
    Feed("https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "ET Markets",     _PRESS, "IN", "attribution_required"),
    Feed("https://www.business-standard.com/rss/markets-106.rss",           "Business Standard Markets", _PRESS, "IN", "attribution_required"),
    Feed("https://www.moneycontrol.com/rss/marketreports.xml",             "Moneycontrol Markets", _PRESS, "IN", "attribution_required"),
    Feed("https://www.moneycontrol.com/rss/business.xml",                  "Moneycontrol Business", _PRESS, "IN", "attribution_required"),
    Feed("https://www.reutersagency.com/feed/?best-sectors=business-finance&post_type=best", "Reuters India Business", _PRESS, "GLOBAL", "attribution_required"),
    Feed("https://www.ndtvprofit.com/stories.rss",                          "NDTV Profit (BQ)",    _PRESS, "IN", "attribution_required"),
    # ── Macro / global — transmit into India (primary official statistics) ────
    Feed("https://www.federalreserve.gov/feeds/press_monetary.xml",         "Fed / FOMC",          _PRIMARY, "US", "commercial_ok"),
    Feed("https://home.treasury.gov/rss/press.xml",                         "US Treasury",         _PRIMARY, "US", "commercial_ok"),
    Feed("https://www.eia.gov/rss/todayinenergy.xml",                       "EIA Energy",          _PRIMARY, "US", "commercial_ok"),
    Feed("https://www.opec.org/opec_web/en/rss/rss.xml",                    "OPEC",                _PRIMARY, "GLOBAL", "commercial_ok"),
    Feed("https://www.lbma.org.uk/feeds/news",                             "LBMA Metals",         _PRIMARY, "GLOBAL", "commercial_ok"),
    # ── Crude and metals press (the priced inputs behind the RIL edges) ───────
    Feed("https://oilprice.com/rss/main",                                   "OilPrice.com",        _PRESS, "GLOBAL", "attribution_required"),
    Feed("https://www.mining.com/feed/",                                    "Mining.com",          _PRESS, "GLOBAL", "attribution_required"),
]

# Feeds whose licence is `blocked` are NEVER ingested. They keep their row in the
# registry (so /admin/licence-audit can show WHY they are excluded) but are
# absent from the ingest list and the whitelist.
_INGESTIBLE: list[Feed] = [f for f in _FEEDS if f.licence != "blocked"]

# (url, source_name), the shape RSS_FEEDS expects — root main.py concatenates the
# two lists with no adapter. Blocked feeds are already gone.
FINANCIAL_FEEDS: list[tuple[str, str]] = [(f.url, f.name) for f in _INGESTIBLE]

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
# feed and can never become financial evidence. Blocked feeds are excluded above,
# so they can never leak in through the whitelist either.
FINANCIAL_SOURCES: frozenset[str] = frozenset(
    f.name for f in _INGESTIBLE
) | _CORPUS_FINANCIAL_SOURCES

# name -> Feed, for weight / jurisdiction / licence lookups by source_name.
_BY_NAME: dict[str, Feed] = {f.name: f for f in _FEEDS}


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


def source_weight(source_name: str) -> float:
    """How much this source vouches for a row: 1.0 primary/official, 0.6 press.

    A corpus-history name we did not re-list (e.g. 'Mint', 'Reuters') is press —
    it is real financial evidence but not a primary filing, so it earns the press
    weight rather than a silent 0."""
    f = _BY_NAME.get((source_name or "").strip())
    if f is not None:
        return f.weight
    return _PRESS if (source_name or "").strip() in FINANCIAL_SOURCES else 0.0


def licence_audit() -> dict:
    """Every financial feed with its licence posture — the /admin/licence-audit
    payload. A paid product must be able to answer 'is this source cleared?' per
    source, so this is a first-class report, not a comment in the registry."""
    by_licence: dict[str, list] = {}
    for f in _FEEDS:
        by_licence.setdefault(f.licence, []).append({
            "name": f.name, "url": f.url, "weight": f.weight,
            "jurisdiction": f.jurisdiction, "ingestible": f.licence != "blocked",
        })
    return {
        "total": len(_FEEDS),
        "ingestible": len(_INGESTIBLE),
        "blocked": sum(1 for f in _FEEDS if f.licence == "blocked"),
        "unlicensed": sum(1 for f in _FEEDS if f.licence not in
                          ("commercial_ok", "attribution_required", "blocked")),
        "by_licence": {k: sorted(v, key=lambda d: d["name"])
                       for k, v in sorted(by_licence.items())},
        # Corpus-history names carried in the whitelist without a registry row.
        # They are usable evidence but have no explicit licence row here.
        "corpus_history_sources": sorted(
            _CORPUS_FINANCIAL_SOURCES - {f.name for f in _FEEDS}),
    }
