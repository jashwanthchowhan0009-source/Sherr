"""Explore drill-downs: one source per number, no fabricated market data.

The same disease as the mandi bug, one layer up. Every index value lived in
THREE hardcoded places — XP_PAGES, MOCK_EXPLORE.matrix and
MOCK_EXPLORE.stocksDetail — so the tile, the list and the drill-down could each
show a different price for NIFTY. Gold, Bitcoin, USD/INR and WTI each had two.

The fix is the same shape as mandi's: the standalone tables are gone and one
catalogue (MARKETS) carries labels only — no prices. Every number is fetched.
These assert that structurally, because three tables agreeing today is not the
property that matters.
"""
import os
import re
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

INDEX = os.path.join(_ROOT, "index.html")


@pytest.fixture(scope="module")
def html():
    with open(INDEX, encoding="utf-8") as fh:
        return fh.read()


def test_the_hardcoded_price_tables_are_gone(html):
    assert "XP_PAGES" not in html.split("// XP_PAGES WAS HERE")[1] \
        .split("const MARKETS")[0] or "const XP_PAGES" not in html
    assert "const XP_PAGES" not in html, "the XP_PAGES price table is still defined"
    assert "stocksDetail: {" not in html, "the stocksDetail price table is still defined"


def test_the_catalogue_carries_no_prices(html):
    """MARKETS says what exists and how to label it. A number in it would be a
    price with no source — exactly what was removed."""
    block = html[html.index("const MARKETS = {"):]
    block = block[:block.index("\n};")]
    # No rupee/dollar amounts, no percentage literals, no comma-grouped prices.
    assert "₹" not in block and "$" not in block
    assert not re.search(r"\d,\d{3}", block), "a comma-grouped price is in the catalogue"


def test_every_matrix_tile_opens_the_real_market_page(html):
    """All six, not just stocks. The tap dispatch goes to dpMarket for every
    key, and dpMarket fetches its numbers."""
    assert "function xpMatrixTap(key) { return dpMarket(key); }" in html
    body = html[html.index("function dpMarket("):]
    dpm = body[:body.index("\nconst DP_SORT_LABEL")]
    for endpoint in ("/markets/history?symbol=", "/markets/table?group="):
        assert endpoint in body[:body.index("function dpNews(")], \
            f"dpMarket never calls {endpoint}"
    assert "mktLive(" in dpm, "dpMarket does not read the live quote"


def test_the_market_page_has_all_four_required_sections(html):
    body = html[html.index("function dpMarket("):html.index("const DP_SORT_LABEL")]
    assert "dp-hero" in body                       # header with value + change
    assert "dpMktChart" in body                    # history chart
    assert "dpMktTable" in body and "dp-sort" in body   # full sortable list
    assert "dpNews(" in body                       # related news from corpus


def test_no_market_number_is_hardcoded_in_the_drilldowns(html):
    """dpMarket and its helpers must derive every number, never carry one."""
    body = html[html.index("function dpMarket("):html.index("async function dpNews(")]
    # A hardcoded index level like 24,231.85 would be the bug coming back.
    assert not re.search(r"\d,\d{3}\.\d", body)


def test_sports_and_movies_do_not_fabricate_scores(html):
    """We hold no scores or listings feed. The pages lead with real corpus
    coverage and say plainly what is not connected — no invented fixtures."""
    assert "function dpSports()" in html and "function dpMovies()" in html
    sports = html[html.index("function dpSports()"):][:400]
    assert "not connected" in sports
    # The "Take a look" cards no longer render MOCK fixtures as scores.
    band = html[html.index("xpTal === 'Sports'"):][:900]
    assert "MOCK_EXPLORE.sports.fixtures" not in band


def test_an_unreachable_store_shows_an_honest_empty_chart(html):
    """dpMktChart must render a named empty state, never a shaped curve, when
    the history is not `ticks`."""
    body = html[html.index("async function dpMktChart("):][:1200]
    assert "dp-empty" in body
    assert "No price history stored" in body
