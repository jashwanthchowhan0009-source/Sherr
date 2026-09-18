"""Two returning-user fixes:

1. A pull-to-refresh (or a re-tap of the home bar) actually delivers new
   articles. `/feed` and `/explore` take a `fresh` flag that skips the read
   cache, so stories ingested inside the 30s/600s cache window show up NOW
   instead of after the window expires. The client sends `fresh=1` only on an
   explicit hard refresh, so the crowd path still hits the cache.

2. A returning user is recognised on the FIRST frame. The last `/me` payload is
   cached in localStorage and painted at boot before the (cold-starting) /me
   request returns, so the profile never flashes defaults that read as
   "logged out".
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
INDEX = os.path.join(_ROOT, "index.html")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def test_feed_endpoint_has_fresh_bypass():
    m = _read(MAIN)
    block = m[m.index('@app.get("/feed")'):m.index('@app.get("/feed")') + 1600]
    assert "fresh: int = Query(0" in block
    # The read is skipped only when fresh is set; the crowd path still caches.
    assert "if not fresh:" in block
    assert "await cache.get(ck)" in block


def test_explore_endpoint_has_fresh_bypass():
    m = _read(MAIN)
    start = m.index("async def explore_feed(")
    block = m[start:start + 3200]
    assert "fresh: int = Query(0" in block
    # fresh produces live and still refreshes the shared + stale cache copies.
    assert "if fresh:" in block
    assert '"stale:" + ck' in block


def test_client_sends_fresh_only_on_hard_refresh():
    h = _read(INDEX)
    assert "async function loadFeed(reset = false, hard = false)" in h
    assert "const fr = hard ? '&fresh=1' : '';" in h
    # Pull-to-refresh and the full-bar re-tap are hard refreshes.
    assert "loadFeed(true, true)" in h
    assert "attachPullToRefresh(document.getElementById('v-home'), () => loadFeed(true, true))" in h


def test_profile_paints_from_cache_on_boot():
    h = _read(INDEX)
    # applyMe caches the payload...
    assert "ST.meCache = me" in h
    # ...and bootstrapSession paints it before the network /me resolves.
    boot = h[h.index("async function bootstrapSession()"):
             h.index("async function bootstrapSession()") + 900]
    assert "if (ST.meCache)" in boot
    assert "applyMe(ST.meCache)" in boot
