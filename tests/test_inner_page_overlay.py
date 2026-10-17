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


def _desktop_block(html: str) -> str:
    """The text of the one brace-balanced laptop media query."""
    start = html.index("@media (min-width: 1024px)")
    depth, j = 0, html.index("{", start)
    while True:
        if html[j] == "{":
            depth += 1
        elif html[j] == "}":
            depth -= 1
            if depth == 0:
                return html[start:j + 1]
        j += 1


# The last <div id="v-*"> view is the anchor for "body level": every named
# view lives inside .phone, so an element that appears AFTER all of them and
# after the real </nav> is a body-level sibling. (The literal "</nav>" also
# appears inside a CSS comment far earlier, so it is not a reliable anchor.)
def _last_view_offset(html):
    return max(html.index(f'<div id="v-{v}"')
               for v in ("home", "explore", "profile", "myfeed", "spie", "notifs"))


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


def test_phone_is_transformed_at_every_width_above_500(html):
    """This is the trap the move exists to escape — documented so nobody
    'simplifies' the move away without understanding why it was made.

    The desktop block used to clear the transform (`left: 0; width: 100vw`),
    which is what made this a 500–1023px "band". Now that a laptop shows the
    same centred column, the transform is in force at EVERY width above 500px —
    so the containing-block trap is permanent and this move is what keeps the
    drill-downs out of it."""
    assert "transform: translateX(-50%)" in html  # the 500px .phone rule
    block = _desktop_block(html)
    assert "transform: none" not in block
    assert "width: 100vw" not in block


def test_home_content_is_blanked_under_a_full_page(html):
    """Not a bet on the overlay being perfectly opaque: .phone is hidden while a
    full page is open. dp-open is set for pages only, never the bottom sheet."""
    assert "body.dp-open .phone { visibility: hidden !important; }" in html


def test_the_drilldown_page_itself_stays_visible_under_dp_open(html):
    """The overlays are 'body level' by TEXT ORDER only — in the parsed DOM the
    browser keeps #xp-page a descendant of .phone (the tests above check string
    position, not real nesting). So `body.dp-open .phone{visibility:hidden}`
    ALSO hid the drill-down page itself: every Explore tile/tab opened to a
    blank black screen with its back arrow hidden, and the reader was stuck.
    This asserts the re-assert-visibility guard that fixes it stays present."""
    assert "body.dp-open #xp-page { visibility: visible !important; }" in html


def test_desktop_centres_the_page_on_the_app_column(html):
    """`#xp-page` is positioned against the viewport, so on a 1440px screen it
    painted its 16px-gutter mobile column across the whole width unless placed
    by hand. It is placed on the app column — the same place it covers on a
    phone."""
    block = _desktop_block(html)
    assert ".xp-sheet, .cmt-sheet, .xp-page {" in block
    assert "width: var(--appw); max-width: var(--appw);" in block
    # The header stays hidden under a full page at every width now: the page is
    # full-bleed over the column and carries its own back arrow, exactly as on a
    # phone. A desktop-only "display: flex" override would uncover it.
    assert "body.dp-open > header, body.dp-open #hdr { display: flex" not in block
