"""The feed action bar (like · comment · repost · bookmark) needs a backend.

The frontend has always called /signal/counts, /signal/comments and
/signal/comment, but those routes never existed on the server — so counts
stayed blank and the comment box was inert ("I can't comment"). These assert
the endpoints and the backing table are present, and that the card wiring is in
place, so a future edit that drops one fails here instead of in the user's hand.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
INDEX = os.path.join(_ROOT, "index.html")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def test_backend_has_the_signal_endpoints():
    m = _read(MAIN)
    assert '@app.get("/signal/counts")' in m
    assert '@app.get("/signal/comments/{article_id}")' in m
    assert '@app.post("/signal/comment")' in m
    assert '@app.post("/signal/like/{article_id}")' in m


def test_backend_has_the_comments_table():
    m = _read(MAIN)
    assert "CREATE TABLE IF NOT EXISTS article_comments" in m


def test_comment_post_requires_a_real_session():
    # require_user (401), never the lenient get_current_user which would let
    # anonymous uid=1 post as everyone.
    m = _read(MAIN)
    block = m[m.index('@app.post("/signal/comment")'):m.index('@app.post("/signal/like')]
    assert "require_user(authorization)" in block


def test_feed_card_has_the_action_bar_not_the_mid_card_share():
    h = _read(INDEX)
    # The bar exists with all four actions...
    assert 'class="nc-actbar"' in h
    for act in ('nca-like', 'nca-cmt', 'nca-rp', 'nca-save'):
        assert act in h, act
    # ...and the standard card no longer renders the old mid-card share pill.
    card = h[h.index("function renderStandardCard"):h.index("function ncAct")]
    assert "nc-acts-row" not in card
    assert "nc-share" not in card


def test_comments_sheet_and_wiring_exist():
    h = _read(INDEX)
    assert 'id="cmt-sheet"' in h
    assert "function openComments" in h and "function postComment" in h
    # device-back closes the sheet first
    assert "cmt.classList.contains('on')) closeComments()" in h
