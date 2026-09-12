"""filings/sources.py — the ONE registry of filing sources and their shapes.

Four sources, two kinds:

  BSE / NSE   exchange corporate announcements. JSON APIs. Each record already
              carries a company code (BSE scrip code / NSE symbol), a category
              and a date — structured, not prose.
  RBI / SEBI  regulator press releases. RSS/XML. No company code; the company (if
              any) is resolved from the title at ingest.

BUILT BLIND, ON PURPOSE. These four endpoints are blocked by the build sandbox's
network policy, so every shape here is written against the source's DOCUMENTED
response, and `kind`/`fields` record exactly which keys the parser reads. The
truth is verified in production through /admin/filing-doctor, which fetches each
source live and shows the RAW record when a shape is wrong — so a shape drift is
a visible record, never a silent parse of zero.

Headers matter: NSE refuses a request without a browser-like User-Agent and a
prior cookie, and both exchanges reject the default httpx UA. The doctor reports
the real HTTP status either way, which is the point — an honest 401/403 in
production is the signal to adjust, not a crash.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Source:
    name: str                    # BSE | NSE | RBI | SEBI — matches Filing.source
    kind: str                    # 'bse_json' | 'nse_json' | 'rss'
    url: str                     # the documented endpoint
    label: str                   # human label for the doctor
    headers: dict = field(default_factory=dict)
    # A one-line note on the documented shape, shown by the doctor so a reader
    # comparing a live raw record against expectations has the contract to hand.
    shape: str = ""


# A browser-like UA is the minimum both exchanges require; without it the CDN
# returns 403 with an HTML body, which the parser would (correctly) count as 0
# parsed and the doctor would surface as a non-JSON raw record.
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/122.0 Safari/537.36")

_BSE_HEADERS = {"User-Agent": _UA, "Referer": "https://www.bseindia.com/",
                "Accept": "application/json"}
_NSE_HEADERS = {"User-Agent": _UA, "Referer": "https://www.nseindia.com/",
                "Accept": "application/json"}


# The registry. Order is the order the doctor reports them in.
SOURCES: list[Source] = [
    Source(
        name="BSE", kind="bse_json",
        # AnnGetData is BSE's corporate-announcements API. strCat=-1 = all
        # categories; strType=C = company announcements. The date window is
        # filled in per-call by ingest/doctor (strPrevDate/strToDate, YYYYMMDD).
        url="https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
            "?pageno=1&strCat=-1&strPrevDate={from}&strToDate={to}"
            "&strScrip=&strSearch=P&strType=C",
        label="BSE corporate announcements",
        headers=_BSE_HEADERS,
        shape="JSON {Table:[{NEWSID, SCRIP_CD, SLONGNAME, NEWSSUB, "
              "CATEGORYNAME, NEWS_DT, ATTACHMENTNAME}], Table1:[{ROWCNT}]}"),
    Source(
        name="NSE", kind="nse_json",
        url="https://www.nseindia.com/api/corporate-announcements?index=equities",
        label="NSE corporate announcements",
        headers=_NSE_HEADERS,
        shape="JSON array [{symbol, sm_name, desc, attchmntText, attchmntFile, "
              "an_dt, sort_date}]"),
    Source(
        name="RBI", kind="rss",
        url="https://www.rbi.org.in/pressreleases_rss.xml",
        label="RBI press releases",
        headers={"User-Agent": _UA},
        shape="RSS 2.0 items [{title, link, pubDate, description, guid}]"),
    Source(
        name="SEBI", kind="rss",
        url="https://www.sebi.gov.in/sebirss.xml",
        label="SEBI press releases",
        headers={"User-Agent": _UA},
        shape="RSS 2.0 items [{title, link, pubDate, description, guid}]"),
]


def by_name(name: str) -> Source | None:
    for s in SOURCES:
        if s.name == name:
            return s
    return None
