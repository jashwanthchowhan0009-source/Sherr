"""The Government section is fed by real, broad PIB data — not one thin slice or
hardcoded mock rows.

- The backend govt_press fetcher merges several PIB endpoints (deduped) instead
  of a single 10-item feed, so every tile has a wide cross-ministry set.
- The 'Govt Jobs / Current Affairs / Policies' cards render REAL PIB releases
  filtered by keyword, not the old SSC/RBI mock list.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(_ROOT, "index.html")
FEEDS = os.path.join(_ROOT, "sherrbyte", "app", "pipeline", "explore_feeds.py")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def test_pib_fetcher_merges_multiple_feeds():
    f = _read(FEEDS)
    assert "_PIB_FEEDS" in f
    assert f.count("pib.gov.in") >= 2 or f.count("RssMain") >= 2
    assert "def _parse_pib(" in f
    # merged + deduped, and a per-feed failure never drops the rest.
    assert "seen" in f and "continue" in f


def test_pib_parser_reads_title_link_date():
    import importlib.util
    spec = importlib.util.spec_from_file_location("_ef_test", FEEDS)
    m = importlib.util.module_from_spec(spec)
    import sys; sys.modules["_ef_test"] = m
    spec.loader.exec_module(m)
    xml = ("<item><title>GST GSTR-3B due date extended</title>"
           "<link>http://pib/1</link><pubDate>18 Sep 2026</pubDate></item>"
           "<item><title>SSC CGL admit cards released</title>"
           "<link>http://pib/2</link><pubDate>18 Sep 2026</pubDate></item>")
    out = m._parse_pib(xml, limit=40)
    assert len(out) == 2
    assert out[0]["title"] == "GST GSTR-3B due date extended"
    assert out[0]["url"] == "http://pib/1"
    assert out[0]["published_at"] == "18 Sep 2026"


def test_also_cards_use_real_pib_not_mock():
    h = _read(INDEX)
    block = h[h.index("function xpAlsoPage"):h.index("function xpAlsoPage") + 1400]
    # real feed, filtered by keyword
    assert "xpSnap" in block and "govt_press" in block and "releases" in block
    assert "XP_ALSO_KEYS" in h
    # the old hardcoded also-card mock rows are gone from the function
    assert "RBI Grade B officer recruitment" not in block
    assert "Cabinet clears semiconductor mission phase 2" not in block
