"""The desktop layout, and the promise that the phone is untouched.

The mobile app is the product. Desktop is a second arrangement of the same DOM,
and the only way to guarantee a desktop tweak can never reach a phone is for
every desktop rule to live inside one media query. That is a structural property
of the file, so it can be asserted rather than eyeballed in a browser.

These also pin the client half of real URLs: the server answering /explore is
useless if the app never puts /explore in the address bar, and vice versa. The
two halves are checked against each other here because they are written in two
different languages in two different files and nothing else compares them.
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


def _block(html: str, opener: str) -> str:
    """The text of one brace-balanced @media block."""
    start = html.index(opener)
    i = html.index("{", start)
    depth, j = 0, i
    while j < len(html):
        if html[j] == "{":
            depth += 1
        elif html[j] == "}":
            depth -= 1
            if depth == 0:
                return html[start:j + 1]
        j += 1
    raise AssertionError(f"unbalanced block at {opener}")


# ─── the phone is untouched ─────────────────────────────────────────────────

def test_there_is_exactly_one_desktop_breakpoint(html):
    """One block, one breakpoint. Desktop rules scattered through the sheet are
    how a phone regression arrives disguised as a desktop fix."""
    assert len(re.findall(r"@media\s*\(min-width:\s*1024px\)", html)) == 1


def test_every_desktop_rule_lives_inside_it(html):
    """The selectors the desktop layout changes must not appear as desktop
    overrides anywhere else — a bare `nav { flex-direction: column }` outside
    the block would turn the phone's bottom bar into a sidebar."""
    block = _block(html, "@media (min-width: 1024px)")
    for selector in (".xp-matrix", ".bytes-feed", ".byte-card", "nav {",
                     "header {", ".nb-tabs", ".view {"):
        assert selector in block, f"{selector} is not restyled for desktop"
    outside = html.replace(block, "")
    # The one rule the desktop block overrides by name, proven not to have been
    # edited in place: the phone still gets its bottom-nav hide-on-scroll.
    assert "nav.hidden { transform: translateX(-50%) translateY(110px);" in outside


def test_the_phone_matrix_is_still_two_columns(html):
    """Pixel-identical below the breakpoint means the base rule is unchanged."""
    block = _block(html, "@media (min-width: 1024px)")
    base = html.replace(block, "")
    assert ".xp-matrix{display:grid;grid-template-columns:1fr 1fr;" in base


def test_the_phone_bytes_feed_is_still_a_full_screen_snap(html):
    block = _block(html, "@media (min-width: 1024px)")
    base = html.replace(block, "")
    assert "scroll-snap-type: y mandatory" in base
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in block


# ─── what desktop actually gets ─────────────────────────────────────────────

def test_the_bottom_nav_becomes_a_sidebar(html):
    block = _block(html, "@media (min-width: 1024px)")
    assert "flex-direction: column" in block
    assert "width: var(--sbw)" in block
    # A sidebar that slid away on scroll would take the app's only navigation
    # with it — the phone's hide-on-scroll must be neutralised, not inherited.
    assert "nav.hidden, nav.compact { transform: none;" in block


def test_the_reading_column_is_centred_and_bounded(html):
    block = _block(html, "@media (min-width: 1024px)")
    assert "--col" in block and "max(24px, calc((100vw" in block


def test_the_matrix_of_markets_is_three_across(html):
    """Six tiles: 3 x 2 is one glance where 2 x 3 was a scroll."""
    block = _block(html, "@media (min-width: 1024px)")
    assert ".xp-matrix { grid-template-columns: repeat(3, 1fr);" in block


def test_the_column_variables_are_declared_once(html):
    """Two numbers, one place. Eleven hardcoded 236px is how a sidebar and the
    header it sits beside drift out of alignment."""
    assert html.count("--sbw: 236px") == 1
    assert html.count("--col: 720px") == 1


def test_overlays_do_not_inherit_the_column_padding(html):
    """The article overlay is full-bleed on top of everything and carries its
    own padding; inheriting the view's gutter would double it."""
    block = _block(html, "@media (min-width: 1024px)")
    assert "#article-overlay" in block and "padding-left: 0" in block


# ─── real URLs: the client half ─────────────────────────────────────────────

def test_the_client_and_the_server_agree_on_every_path(html):
    """THE TWO HALVES ARE WRITTEN IN DIFFERENT LANGUAGES IN DIFFERENT FILES.
    A path the client pushes but the server does not serve is a 404 on refresh;
    a path the server serves but the client never pushes is dead weight."""
    import main
    js = re.search(r"const VIEW_PATHS = \{(.*?)\};", html, re.S).group(1)
    client_paths = set(re.findall(r"'(/[a-z]*)'", js))
    server_paths = {"/" + r if r else "/" for r in main._SPA_ROUTES}
    missing = client_paths - server_paths
    assert not missing, f"the client pushes paths the server will 404: {missing}"


def test_the_slug_is_built_identically_on_both_sides(html):
    """The server writes /bytes/<slug> into the sitemap and the og:url; the
    client writes it into the address bar. If they disagree, a shared link and
    an indexed link point at different URLs for the same story."""
    import main
    assert "function articleSlug(a)" in html
    assert "articleIdFromSlug" in html
    # The property both implementations must share: the trailing id resolves.
    assert main.article_id_from_slug(
        main.article_slug(99, "Some headline about markets")) == 99
    assert ".slice(0, 9)" in html, "the client must cap the slug at 9 words too"


def test_a_deep_link_opens_the_story_on_boot(html):
    """Without this the server's og: tags would be right and the reader would
    still land on the feed — the exact failure the URLs exist to fix."""
    assert "async function routeFromPath()" in html
    assert "openArticle(a)" in html
    assert "routeFromPath()" in html.split("DOMContentLoaded")[1][:200]


def test_navigation_pushes_the_path_not_just_the_state(html):
    assert "history.pushState({ view: name }, '', pathForView(name))" in html
    assert "'/bytes/' + articleSlug(a)" in html


def test_the_desktop_nav_is_a_full_height_rail_not_a_floating_card(html):
    """#2/#3: the desktop sidebar showed as a floating frosted pill with a
    stray circular button, overlapping content. The rail itself must carry the
    surface and the pill/orb chrome must be flattened."""
    block = _block(html, "@media (min-width: 1024px)")
    # nav is a fixed full-height rail with its own background.
    assert "position: fixed; top: 0; bottom: 0; left: 0" in block
    assert "background: var(--glass)" in block
    # the tab group is flattened (no pill background / radius / shadow).
    assert "background: transparent; box-shadow: none; border: 0;" in block
    assert "border-radius: 0;" in block
    # the scan orb becomes a flush full-width row, not a floating circle.
    assert "width: 100%; height: auto; margin: auto 0 0 0; border-radius: 12px;" in block
