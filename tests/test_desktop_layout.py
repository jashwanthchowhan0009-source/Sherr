"""The website layer, and the promise that the phone is untouched.

sherrbyte.vercel.app is BOTH the Android TWA's origin and the public website,
and from 2026-10-24 those are two different layouts: below 768px a reader gets
the app, exactly as the TWA ships it; above it they get a news site — a top
bar, a masthead, a multi-column feed grid, a reading column and a footer.

What makes that safe is a structural property rather than something to eyeball
in a browser: EVERY rule that re-arranges anything lives inside the website
breakpoints, so a website fix cannot reach the app. That is asserted here,
along with the pieces of the phone those rules used to own.

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

WEB = "@media (min-width: 768px)"
WIDE = "@media (min-width: 1200px)"


# ─── the phone is untouched ─────────────────────────────────────────────────

def test_the_website_lives_in_exactly_two_breakpoints(html):
    """One shell query and one density query. Website rules scattered through
    the sheet are how a phone regression arrives disguised as a website fix."""
    assert len(re.findall(r"@media\s*\(min-width:\s*768px\)", html)) == 1
    assert len(re.findall(r"@media\s*\(min-width:\s*1200px\)", html)) == 1


def test_no_breakpoint_below_768_rearranges_anything(html):
    """The TWA renders below 768px. The only query under it is the app-column
    rule, which centres .phone and changes nothing inside it."""
    for m in re.finditer(r"@media\s*\(min-width:\s*(\d+)px\)", html):
        width = int(m.group(1))
        if width >= 768:
            continue
        block = _block(html, html[m.start():m.start() + 40])
        for banned in ("grid-template-columns", "flex-direction: row",
                       "scroll-snap-type", "#home-feed", "#ds-hero", "#ds-foot"):
            assert banned not in block, (
                f"the {width}px block re-lays out the app: {banned}")


def test_the_app_column_rule_stops_where_the_website_starts(html):
    """500-767px is a big phone or a small tablet: the same app, centred. The
    moment the website shell takes over, the column cap has to be gone or the
    site renders as a 440px strip in the middle of a monitor."""
    assert "@media (min-width: 500px) and (max-width: 767px) {" in html
    assert "width: var(--appw); max-width: var(--appw); right: auto;" in html


def test_the_phone_bottom_nav_still_hides_on_scroll(html):
    """The rule the website neutralises for its top bar, proven not to have
    been edited in place — the phone keeps it."""
    web = _block(html, WEB)
    outside = html.replace(web, "")
    assert "nav.hidden { transform: translateX(-50%) translateY(110px);" in outside


def test_myfeed_is_a_full_screen_snap_at_every_width(html):
    """One card per screen is the surface's whole reading model, and that is as
    true on a laptop as on a phone. The website changes the card's proportions,
    never the deck's behaviour."""
    web = _block(html, WEB)
    base = html.replace(web, "")
    assert "scroll-snap-type: y mandatory" in base
    assert "scroll-snap-type" not in web


def test_the_matrix_is_two_columns_at_every_width(html):
    web = _block(html, WEB)
    base = html.replace(web, "")
    assert ".xp-matrix{display:grid;grid-template-columns:1fr 1fr;" in base
    assert ".xp-matrix" not in web


def test_the_website_furniture_does_not_exist_on_a_phone(html):
    """The masthead and the footer are in the markup at every width and are
    DISPLAYED only by the website query — so there is no runtime branch
    deciding which surface is rendering, and the TWA's Home is the card feed it
    has always been."""
    assert "#ds-hero, #ds-foot { display: none; }" in html
    web = _block(html, WEB)
    assert "#ds-hero {" in web and "display: grid;" in web
    assert "#ds-foot {" in web


# ─── what the website actually is ───────────────────────────────────────────

def test_the_shell_block_builds_a_site_not_a_column(html):
    """The inverse of the rule this file used to assert. A laptop gets a top
    bar, a grid and a footer; if these stop being here, the website has
    silently collapsed back into a centred phone."""
    web = _block(html, WEB)
    for needed in ("#home-feed {", "grid-template-columns",
                   "header {", "nav {", "#ds-foot {", "--ds-shoulder"):
        assert needed in web, f"the website block lost: {needed}"


def test_the_wide_block_only_changes_density(html):
    """1200px is three columns instead of two and nothing else. A laptop and a
    monitor must be the same site, or there are two websites to maintain."""
    wide = _block(html, WIDE)
    assert "#home-feed {" in wide
    assert wide.count("{") < 6, "the wide block is growing a second layout"
    for banned in ("header", "nav ", "#ds-foot", "position:"):
        assert banned not in wide


def test_every_measurement_is_a_token(html):
    """One page width, one reading measure, one tonal ramp. A hardcoded pixel
    here is how the grid and the things centred on it drift apart."""
    for token in ("--ds-page:", "--ds-read:", "--ds-topbar:",
                  "--ds-f-display:", "--ds-f-ui:", "--ds-f-read:",
                  "--ds-spring:", "--ds-stagger:", "--ds-mesh:"):
        assert token in html, f"missing design token {token}"


def test_the_type_is_not_a_default_stack(html):
    """Space Grotesk / Archivo / Newsreader are loaded and used. Inter and
    Poppins stay the PHONE's faces, which is why they are still in the link."""
    link = re.search(r'fonts\.googleapis\.com/css2\?[^"]+', html).group(0)
    for fam in ("Space+Grotesk", "Archivo", "Newsreader"):
        assert fam in link, f"{fam} is used by the website but never loaded"
    web = _block(html, WEB)
    assert "var(--ds-f-display)" in web and "var(--ds-f-read)" in web


def test_the_background_is_never_a_flat_fill(html):
    """Depth comes from layered washes plus a masked dot mesh, per theme."""
    assert "--ds-mesh:" in html
    web = _block(html, WEB)
    assert "background: var(--ds-mesh);" in web
    assert "mask-image" in web, "the grain must be masked off the reading band"


def test_motion_is_tokenised_and_can_be_switched_off(html):
    web = _block(html, WEB)
    assert "animation: dsRise" in web
    assert "var(--ds-stagger)" in web
    assert "@media (prefers-reduced-motion: reduce)" in web


def test_the_footer_does_not_hardcode_the_legal_paths(html):
    """These pages are FastAPI on the Render host while this file is served
    from the static Vercel deploy, so a literal href="/privacy" 404s for every
    reader — the trap PLAY_STORE.md already records. dsFootLinks() fills them
    from API_URL, which also keeps them real, focusable links."""
    foot = html[html.index('<footer id="ds-foot">'):]
    foot = foot[:foot.index("</footer>")]
    for path in ("/privacy", "/terms", "/delete-account"):
        assert f'data-legal="{path}"' in foot
        assert f'href="{path}"' not in foot
    assert "function dsFootLinks()" in html
    assert "a.href = legalUrl(" in html
    assert "dsFootLinks();" in html.split("function dsFootLinks()")[0] or \
           "dsFootLinks();" in html.split("function dsFootLinks()")[1]


def test_the_footer_nav_groups_are_pulled_out_of_the_app_nav_rule(html):
    """The base sheet styles the element `nav` as the app's centred bottom
    pill. The footer's link groups are <nav> too, so without this reset they
    fly to the middle of the screen — which is exactly what they did."""
    web = _block(html, WEB)
    assert "#ds-foot nav {" in web
    rule = web[web.index("#ds-foot nav {"):]
    assert "position: static;" in rule[:rule.index("}")]


def test_the_surround_has_a_colour_in_every_theme(html):
    """--deskbg is what sits beside the app column between 500 and 767px. An
    unset one falls back to nothing and paints white behind a dark app."""
    for theme in ('', '[data-theme="navy"]', '[data-theme="light"]'):
        root = ":root {" if not theme else theme + " {"
        chunk = html[html.index(root):]
        chunk = chunk[:chunk.index("\n}")]
        assert "--deskbg:" in chunk, f"{theme or ':root'} has no backdrop colour"


def test_every_theme_retunes_the_tonal_ramp(html):
    """The ramp is four steps of ONE tone. A theme that redefines --bg and not
    these gets a dark card on a light page."""
    for theme in ('[data-theme="navy"]', '[data-theme="light"]'):
        chunk = html[html.index(theme + " {"):]
        chunk = chunk[:chunk.index("\n}")]
        for token in ("--ds-t0:", "--ds-t2:", "--ds-ink:", "--ds-mesh:"):
            assert token in chunk, f"{theme} never retunes {token}"


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
