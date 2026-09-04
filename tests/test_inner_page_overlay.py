"""Explore drill-downs are body-level overlays, not trapped inside .phone.

Third report of the same visual bug: at a desktop-ish width the Mandi / Markets
drill-downs rendered as a squeezed mobile column with the home "Take a look"
section and the bottom bar bleeding in underneath. The cause was NOT a missing
rule — it was two DOM facts:

  1. `.phone` carries `transform: translateX(-50%)` between 500px and 1023px. A
     transformed ancestor becomes the containing block for `position: fixed`
     descendants, so every overlay inside `.phone` (`#xp-page`, `#xp-sheet`) was
     contained by `.phone`'s 440px column instead of the viewport.
  2. `#xp-page` sat inside `#v-explore`, AFTER the explore modules, and the hide
     rule `#v-explore > #xp-page ~ *` can only hide FOLLOWING siblings — never
     `#xp-matrix` / `#xp-tal`, which precede it. So those bled around the page.

The fix moves the overlays to body level (out of `.phone`) and blanks `.phone`
while a full page is open. These assert that structure so a future edit that
drops an overlay back inside `.phone` fails here instead of in production.
"""
import os
import re

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(_ROOT, "index.html")


@pytest.fixture(scope="module")
def html():
    with open(INDEX, encoding="utf-8") as fh:
        return fh.read()


# The last <div id="v-*"> view is the anchor for "body level": every named
# view lives inside .phone, so an element that appears AFTER all of them and
# after the real </nav> is a body-level sibling. (The literal "</nav>" also
# appears inside a CSS comment far earlier, so it is not a reliable anchor.)
def _last_view_offset(html):
    return max(html.index(f'<div id="v-{v}"')
               for v in ("home", "explore", "profile", "bytes", "spie", "notifs"))


def test_the_overlays_are_body_level_not_inside_phone(html):
    after_views = _last_view_offset(html)
    for overlay in ('id="xp-page"', 'id="xp-sheet"', 'id="xp-scrim"'):
        assert html.index(overlay) > after_views, \
            f"{overlay} is not past every view — it is still inside .phone, where " \
            f"its position:fixed is trapped by .phone's transform at 500–1023px"


def test_xp_page_is_not_a_child_of_v_explore(html):
    """It used to live inside #v-explore, which is what made the ~ hide rule
    necessary and insufficient at once. Now it sits past every view."""
    assert html.index('id="xp-page"') > _last_view_offset(html)


def test_the_fragile_sibling_hide_rule_is_gone(html):
    # The RULE, not the words: the explanatory comment names it on purpose.
    assert "#v-explore > #xp-page ~ *{display:none" not in html


def test_phone_is_transformed_in_the_tablet_band(html):
    """This is the trap the move exists to escape — documented so nobody
    'simplifies' the move away without understanding why it was made."""
    assert "transform: translateX(-50%)" in html  # the 500px .phone rule
    # And desktop clears it.
    block = html[html.index("@media (min-width: 1024px)"):html.index("/* Wider still")]
    assert ".phone { max-width: none; left: 0; transform: none" in block


def test_home_content_is_blanked_under_a_full_page(html):
    """Not a bet on the overlay being perfectly opaque: .phone is hidden while a
    full page is open. dp-open is set for pages only, never the bottom sheet."""
    assert "body.dp-open .phone { visibility: hidden !important; }" in html


def test_desktop_offsets_the_page_beside_the_sidebar(html):
    block = html[html.index("@media (min-width: 1024px)"):html.index("/* Wider still")]
    assert ".xp-page {" in block and "left: var(--sbw)" in block
    # Header comes back on desktop — the page is offset, not covering it.
    assert "body.dp-open > header, body.dp-open #hdr { display: flex" in block
