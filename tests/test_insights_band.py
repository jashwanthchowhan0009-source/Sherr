"""Details & insights on Home and Explore:
- a page-level insights band (stat chips) on both pages, fed from data the page
  already loaded (the feed, the markets poll) — no extra fetch;
- card-level depth: a meta row (reading time, via-publisher, a why line) under
  every standard feed card.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(_ROOT, "index.html")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


H = _read(INDEX)


def test_insight_band_css_and_helpers_exist():
    assert ".insband" in H and ".insband-chip" in H
    assert ".nc-insight" in H
    for fn in ("function renderInsightBand(", "function homeInsightChips(",
               "function renderHomeInsights(", "function exploreInsightChips(",
               "function renderExploreInsights(", "function readMins("):
        assert fn in H, f"missing {fn}"


def test_home_band_is_mounted_and_rendered():
    # Home band is inserted after the section tabs and rendered on feed load.
    assert "id: 'home-insband'" in H or "band.id = 'home-insband'" in H
    loadfeed = H[H.index("async function loadFeed("):
                 H.index("async function loadFeed(") + 7200]
    assert "renderHomeInsights()" in loadfeed
    # It reads the loaded feed, not a new request.
    hchips = H[H.index("function homeInsightChips()"):
               H.index("function homeInsightChips()") + 900]
    assert "homeArticles" in hchips


def test_explore_band_mount_and_wiring():
    assert 'id="xp-insband"' in H
    # rendered when Explore opens and when fresh market data lands.
    assert "renderExploreInsights()" in H
    echips = H[H.index("function exploreInsightChips()"):
               H.index("function exploreInsightChips()") + 700]
    assert "_topMarketMove()" in echips
    # market poll refreshes both bands
    mk = H[H.index("window._lastMkt = data"):
           H.index("window._lastMkt = data") + 500]
    assert "renderExploreInsights()" in mk and "renderHomeInsights()" in mk


def test_card_depth_row_present():
    card = H[H.index("function renderStandardCard("):
             H.index("function renderStandardCard(") + 1600]
    assert 'class="nc-insight"' in card
    assert "readMins(a)" in card and "min read" in card
    assert "orig_source" in card         # via <publisher>


def test_band_hides_when_empty():
    # An empty band must hide rather than render a blank strip.
    rib = H[H.index("function renderInsightBand("):
            H.index("function renderInsightBand(") + 700]
    assert "display = 'none'" in rib
    assert "filter(c => c && c.v" in rib
