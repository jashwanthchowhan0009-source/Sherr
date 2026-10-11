"""The myFeed card's TL;DR control and its share button.

Both of these were wired to nothing, and neither failure was visible from the
server: the share button built a corrupt file, and the TL;DR control sat at the
bottom of the pane showing a block that had already been built. These are
structural assertions over index.html, which is where the bugs live — the same
approach tests/test_desktop_layout.py takes for the breakpoint.
"""
import os
import re

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HTML = open(os.path.join(_ROOT, "index.html"), encoding="utf-8").read()


def test_the_canvas_card_builder_does_not_shadow_the_dom_card_builder():
    """Two `function buildMyfeedCard` declarations existed ~4,500 lines apart —
    one returning a PNG Blob, one returning a <div>. The later wins, so
    shareArticleCard wrapped an HTMLElement in a File and produced a 25-byte
    "[object HTMLDivElement]" image. One name, one meaning."""
    assert len(re.findall(r"function buildMyfeedCard\b", _HTML)) == 1
    assert "async function buildArticleByteBlob(a)" in _HTML
    assert "await buildArticleByteBlob(a)" in _HTML


def test_share_always_carries_the_link_not_only_an_image():
    """A story shared as an image alone arrives with no way back to the
    article. The share payload carries title, text and url together."""
    assert "function articleShareUrl(a)" in _HTML
    assert "function articleShareText(a)" in _HTML
    body = _HTML[_HTML.index("async function shareArticleCard(a)"):]
    body = body[:body.index("\n}")]
    assert "navigator.share({ ...payload, files: [file] })" in body
    assert "await navigator.share(payload)" in body          # no file support
    assert "navigator.clipboard.writeText(clip)" in body     # no share sheet
    assert "document.execCommand('copy')" in body            # no clipboard API


def test_the_tldr_button_sits_between_the_headline_and_the_body():
    """It was pinned under the whole pane — the last thing a reader scrolls to,
    which is the wrong place for "give me the short version"."""
    news = _HTML[_HTML.index('<section class="mfd-pane news on"'):]
    news = news[:news.index('<section class="mfd-pane strings"')]
    cue = news.index('class="mfd-cue"')
    keypoints = news.index('class="mfd-keypoints"')
    lead = news.index('class="mfd-lead"')
    assert cue < keypoints < lead, "cue row and key points must precede the body"
    assert 'class="mfd-tldr-btn"' in news[cue:keypoints]
    assert "mfd-tldr-row" not in _HTML          # the old bottom row is gone
    # The sector badge ("the dots") moved into the same row, as asked.
    assert "${leadBadge}" in news[cue:keypoints]


def test_the_hidden_attribute_is_honoured_by_the_panes_css():
    """`.mfd-block{display:flex}` and `.mfd-keypoints{display:flex}` are class
    rules and beat the browser's own `[hidden]{display:none}`, an unqualified
    element rule. So What happened, Who & Where and the Key Points panel all
    rendered regardless of the attribute — the TL;DR panel was open on a card
    nobody had tapped, which is the bug in the screenshot."""
    assert ".mfd-block[hidden],.mfd-keypoints[hidden]{display:none;}" in _HTML


def test_the_tldr_is_generated_on_tap_and_not_before():
    """Hydration used to build the Key Points block for every card in the deck
    and merely unhide it on tap. The tap is now what produces it."""
    assert "api('/article/' + a.id + '/tldr'" in _HTML
    # Hydration stashes the node for the offline fallback; it renders nothing.
    hyd = _HTML[_HTML.index("async function hydrateDossier(el, id)"):]
    hyd = hyd[:hyd.index("\nfunction _mfdMarkPending")]
    assert "el._mfdNode = {" in hyd
    assert "mfdKeyPointsHTML" not in hyd


def test_the_article_overlay_no_longer_calls_a_route_that_does_not_exist():
    """/sherr/tldr has never been declared in main.py, so the overlay's
    Summarize button answered "Summarize failed — try again" on every tap it
    had ever received."""
    main_py = open(os.path.join(_ROOT, "main.py"), encoding="utf-8").read()
    assert not re.search(r'@app\.(get|post)\("/sherr/tldr"', main_py)
    assert "api('/sherr/tldr'" not in _HTML
    assert "'/tldr'," in _HTML


def test_the_topic_list_is_fetched_rather_than_hardcoded_twice():
    """index.html carried the list as two literal arrays that had already
    drifted apart (55 entries vs 49) and matched neither MICRO_TOPICS nor each
    other. One fallback remains, for a cold start."""
    assert len(re.findall(r"FEED_TOPICS_FALLBACK\s*=", _HTML)) == 1
    assert "api('/topics/catalogue'" in _HTML
    assert "function renderTopicChips(container, opts)" in _HTML
    # Both screens go through the one renderer.
    for fn in ("function renderFeedTopics()", "function renderFemTopics()"):
        body = _HTML[_HTML.index(fn):]
        body = body[:body.index("\n}")]
        assert "renderTopicChips(" in body
