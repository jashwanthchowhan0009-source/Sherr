"""One commodity, one price — enforced structurally, not by looking.

MOCK_EXPLORE carried TWO mandi tables. The "Take a look" card read one and the
Mandi prices page read the other, so Onion showed ₹2,450 at Bowenpally on one
screen and ₹2,400 at Bowenpally on the next, and Tomato was ₹1,620 at
Gaddiannaram against ₹1,180 at Gudimalkapur. Nothing was broken in either
renderer; there were simply two answers to one question.

The fix is not "make the numbers match" — that would drift again the first time
either table was edited. It is that the second table no longer exists and every
reader goes through one accessor. These tests assert that shape, because two
tables agreeing today is not the property that matters.
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


def test_the_second_mandi_table_is_gone(html):
    """The stale one. It held three commodities at prices that disagreed with
    mandiDetail, plus a hardcoded arrival_date of 21/08/2026 rendered as
    provenance under a number nobody could source."""
    assert not re.search(r"^\s*mandi: \{ state:", html, re.M)
    # The DATA, not the word — the tombstone comment names Gaddiannaram on
    # purpose, so the next reader knows what was removed and why.
    assert "market:'Gaddiannaram'" not in html, \
        "the stale table's Tomato row is still in the file"
    assert "arrival_date:'21/08/2026'" not in html


def test_exactly_one_mandi_table_remains(html):
    assert html.count("mandiDetail: {") == 1
    # Its rows are the superset the page always rendered: 6 commodities across
    # 3 states, each with a weekly change and a real history.
    table = html[html.index("mandiDetail: {"):]
    table = table[:table.index("\n  },\n")]
    rows = re.findall(r"commodity:'([^']+)'", table)
    assert len(rows) == 6 and len(set(rows)) == 6


def test_every_reader_goes_through_the_one_accessor(html):
    """Four call sites read the two tables. If a fifth is added tomorrow it has
    to go through mandiRows() too, or this comes straight back."""
    assert "function mandiRows()" in html
    body = html[html.index("<script"):]
    # No reader may reach past the accessor to a raw table.
    for reader in ("xpMandiPage", "xpMandiSheet", "dpMandi"):
        start = body.index(f"function {reader}(")
        chunk = body[start:start + 2000]
        assert "mandiRows()" in chunk, f"{reader} does not read the one table"
    assert "MOCK_EXPLORE.mandi." not in body.replace("MOCK_EXPLORE.mandiDetail.", "")


def test_the_card_and_the_list_render_the_same_fields(html):
    """The two screens in the bug report. Both now read `modal`, so they cannot
    disagree; the old card read `modal_price` off the other table."""
    body = html[html.index("<script"):]
    card = body[body.index("if (xpTal === 'Mandi')"):][:1400]
    assert "mandiRows()" in card and "x.modal||0" in card.replace(" ", "")
    assert "modal_price" not in card, "the card still reads the old table's field"


def test_an_indicative_price_says_so_instead_of_borrowing_a_date(html):
    """A fabricated arrival_date under a fabricated price is the worst kind of
    placeholder: it looks exactly like sourced data. A row the API did not
    return is labelled, not dated."""
    assert "function mandiSourceLine(" in html
    line = html[html.index("function mandiSourceLine("):][:400]
    assert "x.live" in line and "indicative" in line


def test_the_sheet_charts_the_real_history_not_a_shaped_curve(html):
    """The sparkline used to be the modal price times a fixed set of ratios —
    a confident-looking trend drawn out of a single number."""
    body = html[html.index("<script"):]
    sheet = body[body.index("function xpMandiSheet("):][:1200]
    assert "x.history" in sheet
    assert "0.94,0.97,0.95" not in sheet


def test_an_empty_table_renders_an_honest_empty_state(html):
    body = html[html.index("<script"):]
    card = body[body.index("if (xpTal === 'Mandi')"):][:1400]
    assert "if (!it.length)" in card
    assert "No mandi" in card
