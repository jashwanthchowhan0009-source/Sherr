"""Home feed: the four fixes asked for from the running app.

None of these need the backend — they are the frontend contracts that decide
what a reader sees, and they broke in ways a screenshot showed but a unit test
did not guard. Asserted structurally against index.html.
"""
import os
import re
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(_ROOT, "index.html")


@pytest.fixture(scope="module")
def html():
    with open(INDEX, encoding="utf-8") as fh:
        return fh.read()


# ── 1. "All" must never be empty when the corpus has stories ────────────────
def test_all_falls_back_to_every_published_story(html):
    """The default view uses the personalised /feed, which returns a thin or
    empty set for a reader with no history — so All looked empty though the
    corpus was full. It now falls back to /explore, every story by recency."""
    body = html[html.index("async function loadFeed"):]
    body = body[:body.index("\nasync function ") + 1] if "\nasync function " in body[10:] else body
    assert "_isDefaultFeedView() && reset && !(data.articles || []).length" in html
    assert "`/explore?page=${feedPage}&limit=30${sc}`" in html


# ── 2. Top Headlines: 3 per block, interleaved, repeating ───────────────────
def test_top_headlines_are_three_per_block_not_four(html):
    assert "const HL_PER_BLOCK = 3;" in html
    # The old top strip took four; the interleaved block takes HL_PER_BLOCK.
    block = html[html.index("function renderHeadlinesBlock"):][:900]
    assert "HL_PER_BLOCK" in block and "arts.length < HL_PER_BLOCK" in block


def test_top_headlines_are_in_the_feed_not_pinned_on_top(html):
    """The banner strip is retired to a no-op; headlines now punctuate the feed."""
    strip = html[html.index("function renderHomeHeadlines()"):][:400]
    assert "host.innerHTML = ''" in strip and "_legacyRenderHomeHeadlines" in html
    # First block after the 3rd story, then every 9.
    assert "const HL_FIRST_AFTER = 3;" in html
    assert "const HL_EVERY = 9;" in html
    assert "feedCardCount === HL_FIRST_AFTER" in html


def test_every_feed_card_path_can_interleave_headlines(html):
    """feedCard wraps the story render and drops in a block at the cadence, and
    the feed's render paths go through it — not raw renderStandardCard."""
    assert "function feedCard(a, container)" in html
    loadfeed = html[html.index("async function loadFeed"):html.index("async function loadFeedPulse")]
    assert "feedCard(a, c)" in loadfeed
    # The counter resets on a fresh load so the cadence restarts.
    assert "feedCardCount = 0; _hlCursor = 0;" in html


# ── 3. The "not written up yet" popup must not appear ───────────────────────
def test_an_unwritten_article_opens_the_source_not_the_popup(html):
    """If SherrByte has nothing of its own, the overlay was a dead end that said
    so. Tapping now goes straight to the publisher instead."""
    op = html[html.index("function openArticle(rawA)"):][:900]
    assert "window.open(src, '_blank', 'noopener'); return;" in op
    assert "!_ours(a.body_ai) && !_ours(a.preview) && !_ours(a.summary)" in op


# ── 4. Desktop: Home is a portal grid, phone is untouched ───────────────────
def test_home_is_a_grid_on_desktop(html):
    block = html[html.index("@media (min-width: 1024px)"):]
    block = block[:block.index("/* Wider still")]
    assert "#home-feed {" in block and "grid-template-columns: repeat(2, minmax(0, 1fr))" in block
    # Full-bleed rows span the grid so they punctuate it.
    assert "#home-feed > .hero-card" in block and "grid-column: 1 / -1" in block


def test_the_phone_home_feed_is_not_a_grid(html):
    """The grid lives ONLY inside the desktop breakpoint; below it, Home is the
    untouched single column."""
    block = html[html.index("@media (min-width: 1024px)"):html.index("/* Wider still")]
    base = html.replace(block, "")
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" not in base
