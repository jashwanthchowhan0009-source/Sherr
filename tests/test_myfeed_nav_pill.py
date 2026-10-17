"""myFeed must never be a navigation dead end.

While the bottom nav is compact, `nav.compact .nb:not(.on)` gives every tab but
the active one `pointer-events: none` and zero width. On myFeed that leaves the
minimized myFeed pill as the ONLY tappable thing on screen — so a tap on it is
the reader's single route back to the full bar, and from there to any other tab.

That tap was dead. Two things made it so, and both are asserted here:

  1. myFeed's scroll handler sets `compact` from `scrollTop` alone, unlike the
     other sections' handlers which compact on a DOWNWARD scroll. Anything that
     expanded the bar mid-deck was undone by the next scroll event — including
     the settle events the tap itself produces — so the bar re-collapsed within
     a frame and the tap looked ignored.
  2. The re-tap branch called `loadMyfeed(true)`, which refetches and re-renders
     the whole deck, throwing away the story the reader was on. Home has carried
     the `_reTap && _wasCompact` rule for exactly this case; myFeed did not.

These are structural assertions over index.html, which is where the bug lives —
the same approach tests/test_myfeed_card_controls.py and
tests/test_desktop_layout.py take. Driven end to end in a browser as well:
tapping the pill at 390x844 and at 1440x900 restores all four tabs, leaves the
deck's scrollTop untouched, and issues no feed request.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HTML = open(os.path.join(_ROOT, "index.html"), encoding="utf-8").read()


def _fn(name: str) -> str:
    """One brace-matched function body — never a fixed slice of characters."""
    start = _HTML.index(name)
    depth, j = 0, _HTML.index("{", start)
    while True:
        if _HTML[j] == "{":
            depth += 1
        elif _HTML[j] == "}":
            depth -= 1
            if depth == 0:
                return _HTML[start:j + 1]
        j += 1


def test_the_compact_bar_leaves_only_the_active_tab_tappable():
    """The premise of everything below. If this rule ever softens, the pill is
    no longer the only way out and these tests are measuring the wrong thing."""
    rule = _HTML[_HTML.index("nav.compact .nb:not(.on)"):]
    rule = rule[:rule.index("}")]
    assert "pointer-events: none" in rule


def test_a_tap_on_the_minimized_pill_is_inert_and_never_reloads_the_deck():
    """It is the reader's only way to reach the other tabs from a scrolled
    deck. Pulling a fresh batch there loses the story they were reading."""
    body = _fn("function navTo(name)")
    assert "const _wasCompact = navEl.classList.contains('compact');" in body
    assert "if (_reTap && _wasCompact) { mfHoldNav(); }" in body
    # The refresh still exists — for a tap on the FULL bar, which is what it
    # was always meant for.
    assert "else if (_reTap) { myfeedRetryCount = 0; loadMyfeed(true); }" in body


def test_the_deck_scroll_handler_respects_an_explicit_expand():
    """Without the hold, the handler's `toggle(compact, scrollTop > 24)` undid
    the expand on the next scroll event and the tap looked ignored."""
    body = _fn("async function initMyfeed()")
    assert "navEl.classList.toggle('compact', feed.scrollTop > 24);" in body
    assert "if (_mfNavHold !== null)" in body
    assert "< MF_NAV_HOLD_SLACK) return;" in body
    assert "_mfNavHold = null;" in body


def test_the_hold_clears_itself_so_the_bar_still_gets_out_of_the_way():
    """A hold that never cleared would pin the full bar over every card the
    reader scrolled to for the rest of the session."""
    body = _fn("function mfHoldNav()")
    assert "_mfNavHold = feed ? feed.scrollTop : 0;" in body
    assert "navEl.classList.remove('compact');" in body
    # It is a distance from where the reader asked, not a timer: a timer would
    # either outlive a slow reader or expire during the tap's own settle.
    init = _fn("async function initMyfeed()")
    assert "Math.abs(feed.scrollTop - _mfNavHold)" in init
