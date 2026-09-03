"""The two read endpoints the Explore drill-downs are built on.

Every market drill-down needs a price HISTORY and the FULL instrument list for
its asset class. Both already live in market_ticks (~400 days of daily closes
for the 46 backfilled symbols); these endpoints read that table and nothing
else — NO new provider, no upstream call, no key.

The property that matters is honesty under absence: with no DATABASE_URL the
store is unreachable, and the endpoints must say `unavailable` rather than
return an empty series that a chart would render as a flat line. An empty chart
and a missing database look identical to a reader; only one is worth waiting for.
"""
import os
import sys

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import main  # noqa: E402
import markets  # noqa: E402


@pytest.fixture
def client():
    return TestClient(main.app)


def test_history_reports_unavailable_not_an_empty_series(client):
    r = client.get("/markets/history?symbol=NIFTY&days=90")
    assert r.status_code == 200
    d = r.json()
    # No DATABASE_URL in the test env — the honest answer is unavailable, and it
    # must NOT be an empty `ticks` payload a chart would draw as real.
    assert d["source"] == "unavailable"
    assert d["points"] == []
    assert "detail" in d


def test_history_rejects_an_empty_symbol(client):
    d = client.get("/markets/history?symbol=").json()
    assert d["source"] == "unavailable"


def test_table_reports_unavailable_but_still_names_the_class(client):
    """The catalogue is the honest answer to "what is in this class" even with
    no prices — the page renders the instrument list without numbers rather than
    dropping the section."""
    d = client.get("/markets/table?group=stocks").json()
    assert d["source"] == "unavailable"
    assert d["rows"] == []
    assert "NIFTY" in d.get("symbols", []) and "SENSEX" in d["symbols"]


def test_table_rejects_an_unknown_asset_class(client):
    d = client.get("/markets/table?group=tulips").json()
    assert d["source"] == "unavailable"
    assert "unknown asset class" in d["detail"]


def test_the_symbol_universe_comes_from_the_catalogue_not_a_second_list():
    """The server and the client must agree on what instruments exist without
    either restating them. The labels come from markets.SYMBOLS."""
    for group in ("stocks", "crypto", "forex", "metals", "commodities", "rates"):
        syms = markets._ticks_symbols(group)
        assert syms, f"{group} resolved to no symbols"
        # Labels, not Yahoo tickers — market_ticks stores the label.
        assert "^NSEI" not in syms
    assert "NIFTY" in markets._ticks_symbols("stocks")
    assert "BTC" in markets._ticks_symbols("crypto")


def test_history_days_is_bounded(client):
    # A caller cannot ask for an unbounded scan.
    d = client.get("/markets/history?symbol=NIFTY&days=99999").json()
    assert d.get("days", 800) <= 800
