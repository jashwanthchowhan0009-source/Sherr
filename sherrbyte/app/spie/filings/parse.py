"""filings/parse.py — documented raw shape -> Filing, and every failure kept.

PURE on purpose: no database, no `app.workers`, no network. It imports only the
standard library, feedparser (for the two RSS sources) and the filings classifier
(a rule table). That is what lets /admin/filing-doctor run this under the deployed
root app, which does not ship the engine's asyncpg/Supabase stack.

Every parser returns a ParseResult carrying the Filings it built AND the raw
records it could not parse — each failure keeps the RAW record and a reason, so
the doctor can show the raw record rather than a stack trace when a shape drifts.
A source's whole body failing to decode is one failure with the (truncated) body,
not an exception that takes the pass down.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.spie.filings.classify import classify_filing


@dataclass
class Filing:
    source: str                      # BSE | NSE | RBI | SEBI
    external_id: str                 # the source's own id — the dedup key
    filing_type: str                 # the source's own category/subject label
    subject: str = ""
    company_code: str | None = None
    company_name: str | None = None
    filing_date: str | None = None   # ISO date 'YYYY-MM-DD', or None if unparseable
    url: str | None = None
    raw: dict = field(default_factory=dict)

    @property
    def event_class(self) -> str:
        """Mapped by RULES from filing_type + subject. Computed, never stored on
        the dataclass, so the doctor and the ingest path agree by construction."""
        return classify_filing(self.source, self.filing_type, self.subject)

    def to_row(self) -> dict:
        """The dict the ingest UPSERT binds (entity_id/symbol resolved later)."""
        return {
            "source": self.source,
            "external_id": self.external_id,
            "company_code": self.company_code,
            "company_name": self.company_name,
            "filing_type": self.filing_type or "",
            "event_class": self.event_class,
            "subject": self.subject or "",
            "filing_date": self.filing_date,
            "url": self.url,
            "raw": self.raw,
        }


@dataclass
class ParseResult:
    filings: list = field(default_factory=list)
    failures: list = field(default_factory=list)   # [{"reason", "raw"}]

    @property
    def parsed(self) -> int:
        return len(self.filings)

    @property
    def failed(self) -> int:
        return len(self.failures)


def _fail(raw, reason: str) -> dict:
    """A failure record: the reason plus the raw record itself (truncated if it
    is a big string), which is what the doctor shows instead of a trace."""
    if isinstance(raw, str) and len(raw) > 2000:
        raw = raw[:2000] + "…"
    return {"reason": reason, "raw": raw}


def _iso_date(value) -> str | None:
    """Best-effort date -> 'YYYY-MM-DD'. Tolerant of the several shapes the four
    sources emit; returns None rather than raising when nothing parses (a filing
    with no readable date is still a filing, its date simply unknown)."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # EACH SOURCE EMITS A DIFFERENT SHAPE, and every one below is a real value
    # seen in production, not a guess:
    #   NSE  "13-Sep-2026 01:11:09"     -> %d-%b-%Y %H:%M:%S   (an_dt)
    #   RBI  "Fri, 11 Sep 2026 21:40:00" -> %a, %d %b %Y %H:%M:%S  (RFC822, NO tz)
    #   SEBI "11 Sep, 2026 +0530"       -> %d %b, %Y %z
    # The tz-bearing RFC822 form and the JSON APIs' ISO/space forms are kept too.
    # Each attempt is cheap and the first that parses wins.
    fmts = (
        "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S",                       # RBI pubDate without a tz
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y",   # NSE an_dt
        "%d %b, %Y %z", "%d %b, %Y",                          # SEBI
        "%d %b %Y %H:%M:%S", "%d %b %Y",
        "%Y-%m-%d", "%d-%m-%Y",
    )
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.date().isoformat()
        except ValueError:
            continue
    # Last resort: an ISO-ish prefix 'YYYY-MM-DD...'.
    head = s[:10]
    try:
        datetime.strptime(head, "%Y-%m-%d")
        return head
    except ValueError:
        return None


def _str(v) -> str:
    return "" if v is None else str(v).strip()


_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _strip_html(v) -> str:
    """Plain text out of an HTML fragment. RBI's RSS <description> is a whole
    HTML table of markup — stored raw it makes `subject` unreadable and feeds the
    classifier tag noise. Drop tags, unescape entities, collapse whitespace."""
    import html as _html

    s = _str(v)
    if not s:
        return ""
    s = _TAG_RE.sub(" ", s)
    s = _html.unescape(s)
    return _WS_RE.sub(" ", s).strip()


def _rss_date(entry) -> str | None:
    """The most reliable date for an RSS item: feedparser's parsed struct_time
    when it managed to read one, else the raw published/updated string through
    _iso_date (which now knows RBI's no-tz and SEBI's 'dd Mon, yyyy +0530')."""
    for key in ("published_parsed", "updated_parsed"):
        st = entry.get(key) if hasattr(entry, "get") else None
        if st:
            try:
                return datetime(*st[:6], tzinfo=timezone.utc).date().isoformat()
            except (TypeError, ValueError):
                pass
    return _iso_date(_str(entry.get("published")) or _str(entry.get("updated"))
                     or _str(entry.get("pubDate")))


# ─── BSE: AnnGetData JSON ─────────────────────────────────────────────────────
_BSE_ATTACH_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"


def _is_bse_empty(body) -> bool:
    """BSE's HTTP-200 empty-window sentinel: the literal 'No Record Found!',
    with or without surrounding quotes/whitespace."""
    if not isinstance(body, str):
        return False
    return "no record found" in body.strip().strip('"').lower()


def parse_bse(body) -> ParseResult:
    """BSE corporate announcements. Documented shape:
        {"Table": [{NEWSID, SCRIP_CD, SLONGNAME, NEWSSUB, CATEGORYNAME,
                    NEWS_DT, ATTACHMENTNAME, HEADLINE}, ...], "Table1": [...]}
    """
    res = ParseResult()
    # BSE answers a quiet window with HTTP 200 and the literal string
    # "No Record Found!" (sometimes JSON-quoted). That is an EMPTY result — zero
    # filings, common on a weekend — not a shape mismatch. Counting it as a parse
    # failure would light up the doctor red for a market that was simply closed.
    if _is_bse_empty(body):
        return res
    data = body if isinstance(body, (dict, list)) else _loads(body, res)
    if data is None:
        return res
    if isinstance(data, str):                 # JSON-decoded to a bare string
        if _is_bse_empty(data):
            return res
        res.failures.append(_fail(data, "BSE response is a string, not a table"))
        return res
    rows = data.get("Table") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        res.failures.append(_fail(data, "no 'Table' array in BSE response"))
        return res
    for row in rows:
        if not isinstance(row, dict):
            res.failures.append(_fail(row, "BSE row is not an object"))
            continue
        newsid = _str(row.get("NEWSID")) or _str(row.get("NEWS_DT")) + _str(row.get("SCRIP_CD"))
        if not newsid:
            res.failures.append(_fail(row, "BSE row has no NEWSID / usable id"))
            continue
        attach = _str(row.get("ATTACHMENTNAME"))
        res.filings.append(Filing(
            source="BSE",
            external_id=newsid,
            company_code=_str(row.get("SCRIP_CD")) or None,
            company_name=_str(row.get("SLONGNAME")) or None,
            filing_type=_str(row.get("CATEGORYNAME")),
            subject=_str(row.get("NEWSSUB")) or _str(row.get("HEADLINE")),
            filing_date=_iso_date(row.get("NEWS_DT")),
            url=(_BSE_ATTACH_BASE + attach) if attach else None,
            raw=row,
        ))
    return res


# ─── NSE: corporate-announcements JSON ────────────────────────────────────────
def parse_nse(body) -> ParseResult:
    """NSE corporate announcements. Documented shape: a JSON ARRAY of
        {symbol, sm_name, desc, attchmntText, attchmntFile, an_dt, sort_date}
    """
    res = ParseResult()
    data = body if isinstance(body, (dict, list)) else _loads(body, res)
    if data is None:
        return res
    # NSE returns a bare array; some deployments wrap it as {"data": [...]}.
    rows = data.get("data") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        res.failures.append(_fail(data, "NSE response is not an array"))
        return res
    for row in rows:
        if not isinstance(row, dict):
            res.failures.append(_fail(row, "NSE row is not an object"))
            continue
        symbol = _str(row.get("symbol"))
        an_dt = _str(row.get("an_dt")) or _str(row.get("sort_date"))
        # NSE has no stable public id on the record; (symbol, an_dt, desc) is the
        # natural key and is stable enough to dedup the same announcement.
        ext = _str(row.get("seqId")) or f"{symbol}|{an_dt}|{_str(row.get('desc'))[:60]}"
        if not ext.strip("|"):
            res.failures.append(_fail(row, "NSE row has no symbol/date/desc to key on"))
            continue
        res.filings.append(Filing(
            source="NSE",
            external_id=ext,
            company_code=symbol or None,
            company_name=_str(row.get("sm_name")) or None,
            filing_type=_str(row.get("desc")),
            subject=_str(row.get("attchmntText")) or _str(row.get("desc")),
            filing_date=_iso_date(an_dt),
            url=_str(row.get("attchmntFile")) or None,
            raw=row,
        ))
    return res


# ─── RBI / SEBI: RSS ──────────────────────────────────────────────────────────
def parse_rss(source: str, body) -> ParseResult:
    """RBI / SEBI press releases. RSS 2.0 items {title, link, pubDate,
    description, guid}. The regulator has no company code — filing_type is the
    title, because that is where the category signal lives, and the company (if
    any) is resolved from the title later at ingest."""
    import feedparser  # local: keeps the module importable where feedparser isn't

    res = ParseResult()
    if body is None or (isinstance(body, str) and not body.strip()):
        res.failures.append(_fail(body, f"{source} RSS body empty"))
        return res
    feed = feedparser.parse(body)
    entries = getattr(feed, "entries", None) or []
    if not entries:
        # bozo=1 means feedparser flagged the XML as malformed; show the raw head.
        reason = f"{source} RSS has no entries"
        if getattr(feed, "bozo", 0):
            reason += f" (malformed: {getattr(feed, 'bozo_exception', '')})"
        res.failures.append(_fail(body if isinstance(body, str) else str(body), reason))
        return res
    for e in entries:
        title = _str(e.get("title"))
        link = _str(e.get("link"))
        ext = _str(e.get("id")) or _str(e.get("guid")) or link or title
        if not ext:
            res.failures.append(_fail(dict(e), f"{source} item has no id/link/title"))
            continue
        # STRIPPED before use: RBI's <description> is raw HTML table markup, so
        # the classifier must not see the tags and the stored subject must be
        # readable text, not markup.
        desc = _strip_html(e.get("summary")) or _strip_html(e.get("description"))
        res.filings.append(Filing(
            source=source,
            external_id=ext,
            company_code=None,
            company_name=None,
            filing_type=title,               # the category signal is in the title
            subject=(title + (" — " + desc if desc else "")).strip(" —"),
            filing_date=_rss_date(e),
            url=link or None,
            raw={"title": title, "link": link, "id": ext,
                 "published": _str(e.get("published")), "summary": desc},
        ))
    return res


def _loads(body, res: ParseResult):
    """json.loads, but a decode error becomes ONE failure carrying the raw body
    (so the doctor shows what actually came back — often an HTML error page from
    the CDN, which is exactly the diagnosis) rather than raising."""
    if body is None:
        res.failures.append(_fail("", "empty body"))
        return None
    try:
        return json.loads(body)
    except (json.JSONDecodeError, TypeError) as ex:
        res.failures.append(_fail(body, f"body is not JSON ({ex})"))
        return None


# name -> parser dispatch, keyed by Source.kind.
def parse(kind: str, source_name: str, body) -> ParseResult:
    if kind == "bse_json":
        return parse_bse(body)
    if kind == "nse_json":
        return parse_nse(body)
    if kind == "rss":
        return parse_rss(source_name, body)
    res = ParseResult()
    res.failures.append(_fail(body, f"unknown source kind '{kind}'"))
    return res
