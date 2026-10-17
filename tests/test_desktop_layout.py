"""The laptop layout, and the promise that the phone is untouched.

The Android app is the product, and on a laptop it is the SAME app: the phone
layout in a centred column, not a second arrangement of the same DOM. An earlier
pass built a desktop website here — a left sidebar, a 720px reading column, a
two- and three-column Home — and that is explicitly not what this surface wants;
sherrbyte.vercel.app is also the TWA's origin, so the browser and the app should
show the same screen.

Both halves of that are structural properties of the file rather than things to
eyeball in a browser, so they are asserted: every laptop rule lives in one media
query (which is what makes "a phone cannot be reached by a desktop fix" a
guarantee), and what that one query does is frame the column rather than
re-arrange it.

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


def test_the_laptop_block_frames_the_app_it_does_not_re_lay_it_out(html):
    """The whole point of this surface: a laptop gets the phone, centred. Any of
    these properties in the laptop block means a screen is being re-arranged for
    desktop, which is the layout this replaced."""
    block = _block(html, "@media (min-width: 1024px)")
    for banned in ("grid-template-columns",   # the two/three-column Home and myFeed
                   "flex-direction: column",  # the bottom nav rebuilt as a sidebar
                   "--sbw", "--col)",         # the sidebar width / reading column
                   "scroll-snap-type"):       # myFeed's snap taken off
        assert banned not in block, f"the laptop block re-lays out the app: {banned}"
    # And it stays small. Two hundred lines of laptop CSS is a second layout by
    # another name.
    assert block.count("{") < 12


def test_the_phone_layout_is_what_a_laptop_shows(html):
    block = _block(html, "@media (min-width: 1024px)")
    assert ".phone" in block
    assert "var(--deskbg)" in block, "the surround must not be the app's own --bg"


def test_the_column_width_is_one_number(html):
    """--appw is the app column. Hardcoding 440px in the two rules that need it
    is how the column and the sheets centred on it drift apart."""
    assert html.count("--appw: 440px") == 1
    assert "width: var(--appw); max-width: var(--appw); right: auto;" in html


def test_the_phone_bottom_nav_still_hides_on_scroll(html):
    """The rule the old desktop block neutralised for its sidebar, proven not to
    have been edited in place."""
    block = _block(html, "@media (min-width: 1024px)")
    outside = html.replace(block, "")
    assert "nav.hidden { transform: translateX(-50%) translateY(110px);" in outside
    assert "nav.hidden" not in block


def test_the_matrix_is_two_columns_at_every_width(html):
    block = _block(html, "@media (min-width: 1024px)")
    base = html.replace(block, "")
    assert ".xp-matrix{display:grid;grid-template-columns:1fr 1fr;" in base
    assert ".xp-matrix" not in block


def test_myfeed_is_a_full_screen_snap_at_every_width(html):
    """One card per screen is the surface's whole reading model, on a phone and
    on a laptop alike."""
    block = _block(html, "@media (min-width: 1024px)")
    base = html.replace(block, "")
    assert "scroll-snap-type: y mandatory" in base
    assert ".myfeed-feed" not in block and ".mf-card" not in block


# ─── what the laptop actually gets ──────────────────────────────────────────

def test_the_body_level_sheets_follow_the_column(html):
    """A bottom sheet pinned to the full width of a 1440px screen is a banner.
    These three are positioned against the viewport, so they are the only
    things the laptop has to place by hand."""
    block = _block(html, "@media (min-width: 1024px)")
    assert ".xp-sheet, .cmt-sheet, .xp-page {" in block


def test_the_sheets_are_centred_with_a_margin_not_a_transform(html):
    """Both sheets animate their own transform (translateY(102%) closed), so a
    horizontal transform here would be overwritten by the open state and the
    sheet would slide back to the left edge the moment it opened."""
    block = _block(html, "@media (min-width: 1024px)")
    rule = block[block.index(".xp-sheet, .cmt-sheet, .xp-page {"):]
    rule = rule[:rule.index("}")]
    assert "margin-left: calc(var(--appw) / -2);" in rule
    assert "transform" not in rule


def test_the_surround_has_a_colour_in_every_theme(html):
    """An unset --deskbg falls back to nothing and the backdrop paints white in
    a dark app — so each theme that redefines --bg must redefine this too."""
    for theme in ('', '[data-theme="navy"]', '[data-theme="light"]'):
        root = ":root {" if not theme else theme + " {"
        chunk = html[html.index(root):]
        chunk = chunk[:chunk.index("\n}")]
        assert "--deskbg:" in chunk, f"{theme or ':root'} has no backdrop colour"


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
    """The server writes /myfeed/<slug> into the sitemap and the og:url; the
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
    # myFeed is the deep-reading surface, so a deep link opens the dossier.
    assert "openDossier(" in html
    assert "routeFromPath()" in html.split("DOMContentLoaded")[1][:200]


def test_navigation_pushes_the_path_not_just_the_state(html):
    assert "history.pushState({ view: name }, '', pathForView(name))" in html
    assert "'/myfeed/' + articleSlug(a)" in html
