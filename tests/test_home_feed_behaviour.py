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


# ── 5. Sideways swipe steps categories even on an EMPTY pillar ───────────────
def test_category_swipe_is_bound_to_the_whole_home_view(html):
    """A thin/empty pillar collapses #home-feed to a few pixels, so a swipe bound
    to it had nothing to grab — "after Arts & Culture it won't slide". The swipe
    binds to the full-height #v-home instead, so the gesture is always available."""
    fn = html[html.index("function bindHomeSwipe()"):]
    fn = fn[:fn.index("\n}")]
    assert "getElementById('v-home')" in fn
    # …but horizontal scrollers inside it keep their own scroll.
    assert "#chip-row" in fn and ".trending-strip" in fn and "closest(NO_SWIPE)" in fn


# ── 6. An empty pillar reads as "no stories here", not "server has nothing" ──
def test_empty_pillar_shows_a_topic_specific_state_not_an_outage(html):
    """A selected pillar with no rows is not an outage — the server has plenty,
    just nothing in this topic. The message says so and offers a way out."""
    lf = html[html.index("async function loadFeed"):html.index("async function loadFeedPulse")]
    assert "stories right now" in lf              # "No <Pillar> stories right now"
    assert "Browse all stories" in lf
    assert "function goAllFeed()" in html


# ── 7. The client feed cache is a first-paint only, not a staleness trap ─────
def test_client_feed_cache_expires(html):
    """_readFeedCache returned a stored snapshot forever, so a reopened app could
    keep showing an old set — "articles don't refresh". It now honours an age."""
    assert "_FEED_CACHE_MAX_AGE" in html
    rf = html[html.index("function _readFeedCache()"):]
    rf = rf[:rf.index("\n}")]
    assert "Date.now() - c.t > _FEED_CACHE_MAX_AGE" in rf


def test_returning_to_a_stale_home_pulls_fresh_stories(html):
    """Coming back to Home (nav re-entry or tab re-focus) after the feed has aged
    triggers a fresh fetch, so returning readers don't sit on yesterday's set."""
    assert "_FEED_STALE_MS" in html
    assert "Date.now() - _lastFeedLoadAt > _FEED_STALE_MS" in html
    # Wired to both re-entry paths: the Home nav and the visibility change.
    assert "document.body.dataset.view !== 'home'" in html
