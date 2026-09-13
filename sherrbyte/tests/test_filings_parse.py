"""test_filings_parse.py — the four documented source shapes, against fixtures.

The four filing sources are blocked by the build sandbox, so the parsers are
written against DOCUMENTED shapes and proved here with fixtures that mirror those
shapes. Two things every parser must guarantee:

  1. a well-formed record becomes a Filing with the right fields and a mapped
     event_class;
  2. a malformed record is KEPT as a failure carrying the raw record — never
     dropped silently and never an exception — because that raw record is exactly
     what /admin/filing-doctor shows when a live shape has drifted.
"""

from __future__ import annotations

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))          # sherrbyte/ -> import app.*

from app.spie.filings import parse as P              # noqa: E402


# ─── BSE: AnnGetData JSON ─────────────────────────────────────────────────────
BSE_BODY = json.dumps({
    "Table": [
        {"NEWSID": "F1A2-9", "SCRIP_CD": 500325, "SLONGNAME": "Reliance Industries Ltd",
         "NEWSSUB": "Outcome of Board Meeting - Financial Results for Q1 FY27",
         "CATEGORYNAME": "Result", "NEWS_DT": "2026-09-12T18:30:00",
         "ATTACHMENTNAME": "f1a2.pdf", "HEADLINE": "Board approves results"},
        {"NEWSID": "B7C3-2", "SCRIP_CD": 532540, "SLONGNAME": "Tata Consultancy Services Ltd",
         "NEWSSUB": "Change in Directors / Key Managerial Personnel",
         "CATEGORYNAME": "Company Update", "NEWS_DT": "2026-09-11T10:00:00",
         "ATTACHMENTNAME": ""},
    ],
    "Table1": [{"ROWCNT": 2}],
})


def test_parse_bse_wellformed():
    r = P.parse_bse(BSE_BODY)
    assert r.parsed == 2 and r.failed == 0
    a, b = r.filings
    assert a.source == "BSE"
    assert a.external_id == "F1A2-9"
    assert a.company_code == "500325"
    assert a.company_name == "Reliance Industries Ltd"
    assert a.filing_date == "2026-09-12"
    assert a.event_class == "earnings"
    assert a.url and a.url.endswith("/f1a2.pdf")
    # second row: no attachment -> url None, and 'Change in Directors' -> leadership
    assert b.url is None
    assert b.event_class == "leadership_change"


def test_parse_bse_missing_table_is_a_failure_with_raw():
    r = P.parse_bse(json.dumps({"Nope": []}))
    assert r.parsed == 0 and r.failed == 1
    assert "Table" in r.failures[0]["reason"]
    assert r.failures[0]["raw"] == {"Nope": []}


def test_parse_bse_html_error_page_kept_as_raw():
    # A CDN 403 returns HTML, not JSON; the raw body must survive for the doctor.
    r = P.parse_bse("<html><body>Access Denied</body></html>")
    assert r.parsed == 0 and r.failed == 1
    assert "not JSON" in r.failures[0]["reason"]
    assert "Access Denied" in r.failures[0]["raw"]


def test_parse_bse_no_record_found_is_empty_not_a_failure():
    """BSE answers a quiet window (e.g. a weekend) with HTTP 200 and the literal
    'No Record Found!'. That is zero filings, not a shape mismatch — it must not
    register as a parse failure and light the doctor red."""
    for body in ("No Record Found!", '"No Record Found!"', "  No Record Found!  "):
        r = P.parse_bse(body)
        assert r.parsed == 0 and r.failed == 0, body


# ─── NSE: corporate-announcements JSON ────────────────────────────────────────
NSE_BODY = json.dumps([
    {"symbol": "RELIANCE", "sm_name": "Reliance Industries Limited",
     "desc": "Acquisition", "attchmntText": "Acquisition of majority stake in XYZ",
     "attchmntFile": "https://nsearchives.nseindia.com/x.pdf",
     "an_dt": "2026-09-12 18:30:00", "sort_date": "2026-09-12 18:30:00"},
])


def test_parse_nse_wellformed():
    r = P.parse_nse(NSE_BODY)
    assert r.parsed == 1 and r.failed == 0
    f = r.filings[0]
    assert f.source == "NSE"
    assert f.company_code == "RELIANCE"
    assert f.company_name == "Reliance Industries Limited"
    assert f.filing_date == "2026-09-12"
    assert f.event_class == "m_and_a"
    assert f.url.endswith("x.pdf")
    # No stable id on the record -> the (symbol|date|desc) natural key is used.
    assert f.external_id.startswith("RELIANCE|2026-09-12")


def test_parse_nse_wrapped_in_data_key():
    r = P.parse_nse(json.dumps({"data": json.loads(NSE_BODY)}))
    assert r.parsed == 1


def test_parse_nse_dd_mon_yyyy_date():
    """NSE's an_dt is 'dd-Mon-yyyy HH:MM:SS' — the production shape that was
    coming back as a null filing_date, which the analog engine cannot use."""
    body = json.dumps([{"symbol": "TCS", "sm_name": "TCS Ltd", "desc": "Result",
                        "an_dt": "13-Sep-2026 01:11:09",
                        "sort_date": "2026-09-13 01:11:09"}])
    f = P.parse_nse(body).filings[0]
    assert f.filing_date == "2026-09-13"


def test_parse_nse_non_array_is_a_failure():
    r = P.parse_nse(json.dumps({"unexpected": "object"}))
    assert r.parsed == 0 and r.failed == 1


# ─── RBI / SEBI: RSS ──────────────────────────────────────────────────────────
RBI_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>RBI Press Releases</title>
  <item>
    <title>RBI keeps repo rate unchanged; Monetary Policy Statement</title>
    <link>https://www.rbi.org.in/pr/1</link>
    <guid>rbi-pr-1</guid>
    <pubDate>Fri, 12 Sep 2026 12:00:00 +0530</pubDate>
    <description>The Monetary Policy Committee decided to hold the policy repo rate.</description>
  </item>
</channel></rss>"""

SEBI_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>SEBI</title>
  <item>
    <title>SEBI passes settlement order in the matter of ABC Ltd</title>
    <link>https://www.sebi.gov.in/pr/9</link>
    <guid>sebi-pr-9</guid>
    <pubDate>Thu, 11 Sep 2026 17:00:00 +0530</pubDate>
    <description>Adjudication concluded.</description>
  </item>
</channel></rss>"""


def test_parse_rbi_rss():
    r = P.parse_rss("RBI", RBI_RSS)
    assert r.parsed == 1 and r.failed == 0
    f = r.filings[0]
    assert f.source == "RBI"
    assert f.company_code is None            # a regulator release has no company code
    assert f.filing_date == "2026-09-12"
    assert f.event_class == "central_bank_policy"
    assert f.external_id == "rbi-pr-1"


def test_parse_sebi_rss():
    r = P.parse_rss("SEBI", SEBI_RSS)
    assert r.parsed == 1
    assert r.filings[0].event_class == "regulatory_action"


def test_iso_date_parses_every_source_shape():
    """The three production date shapes that were all coming back null:
    NSE dd-Mon-yyyy, RBI RFC822 without a tz, SEBI 'dd Mon, yyyy +0530'."""
    assert P._iso_date("13-Sep-2026 01:11:09") == "2026-09-13"      # NSE
    assert P._iso_date("Fri, 11 Sep 2026 21:40:00") == "2026-09-11"  # RBI, no tz
    assert P._iso_date("11 Sep, 2026 +0530") == "2026-09-11"         # SEBI


def test_rbi_pubdate_without_tz_yields_a_date():
    """RBI's live pubDate carries no timezone; the filing_date must still parse
    (without a date the analog engine cannot use the filing at all)."""
    rss = ("""<?xml version="1.0"?><rss version="2.0"><channel><item>"""
           "<title>RBI issues KYC Direction amendment</title>"
           "<link>https://www.rbi.org.in/pr/2</link><guid>rbi-2</guid>"
           "<pubDate>Fri, 11 Sep 2026 21:40:00</pubDate>"
           "<description>Direction amended.</description>"
           "</item></channel></rss>")
    f = P.parse_rss("RBI", rss).filings[0]
    assert f.filing_date == "2026-09-11"


def test_rbi_html_description_is_stripped():
    """RBI's <description> is raw HTML table markup — the stored subject must be
    readable text, not tags, and the classifier must not see the markup."""
    rss = ("""<?xml version="1.0"?><rss version="2.0"><channel><item>"""
           "<title>RBI Press Release</title>"
           "<link>https://www.rbi.org.in/pr/3</link><guid>rbi-3</guid>"
           "<pubDate>Fri, 11 Sep 2026 21:40:00 +0530</pubDate>"
           "<description>&lt;table&gt;&lt;tr&gt;&lt;td&gt;Repo rate held"
           "&lt;/td&gt;&lt;/tr&gt;&lt;/table&gt;</description>"
           "</item></channel></rss>")
    f = P.parse_rss("RBI", rss).filings[0]
    assert "<" not in f.subject and ">" not in f.subject
    assert "table" not in f.raw["summary"].lower()
    assert "Repo rate held" in f.subject


def test_parse_rss_empty_body_is_a_failure():
    r = P.parse_rss("RBI", "")
    assert r.parsed == 0 and r.failed == 1


def test_parse_rss_malformed_xml_keeps_raw():
    r = P.parse_rss("SEBI", "<not-rss>garbage")
    assert r.parsed == 0 and r.failed == 1
    assert "raw" in r.failures[0]


# ─── dispatch + to_row ────────────────────────────────────────────────────────
def test_dispatch_by_kind():
    assert P.parse("bse_json", "BSE", BSE_BODY).parsed == 2
    assert P.parse("nse_json", "NSE", NSE_BODY).parsed == 1
    assert P.parse("rss", "RBI", RBI_RSS).parsed == 1
    assert P.parse("mystery", "X", "x").failed == 1


def test_to_row_shape_matches_insert_binding():
    f = P.parse_bse(BSE_BODY).filings[0]
    row = f.to_row()
    for key in ("source", "external_id", "company_code", "company_name",
                "filing_type", "event_class", "subject", "filing_date", "url",
                "raw"):
        assert key in row
    assert row["feed_class"] if "feed_class" in row else True   # feed_class is a DB default
    assert row["event_class"] == "earnings"
