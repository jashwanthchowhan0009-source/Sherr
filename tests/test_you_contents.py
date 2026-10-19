"""The You.com page fallback: a second attempt, on a budget, that cannot bite.

`article_reader` returns "" for a paywalled or JavaScript-only page, so the row
keeps its placeholder body at every later tick — the next tick fetches the same
URL and fails the same way. `you_contents` is the second attempt at exactly
those URLs, and because it is billed per page the properties worth pinning are
the ones that keep it from costing money it should not:

  * It is a FALLBACK. A batch our own reader read in full makes no call.
  * The daily page budget is a HARD ceiling, and a failed call refunds what it
    never fetched.
  * No key means no call and no exception — the reader behaves as it did before.
  * Keys are read from the plural `YOU_KEYS` and rotated on 401/403/429 only.
  * The recovered text faces the SAME word floor our own extraction does.
  * Nothing in the module can raise into the rewrite pass.
"""
import asyncio
import os
import sys

import httpx
import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import article_reader   # noqa: E402
import you_contents     # noqa: E402

LONG = " ".join(f"word{i}" for i in range(200))


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("YOU_KEYS", "YOU_API_KEY", "YDC_API_KEY",
                 "YOU_CONTENTS_ENABLED", "YOU_PAGE_BUDGET_PER_DAY",
                 "YOU_API_BASE"):
        monkeypatch.delenv(name, raising=False)
    you_contents._budget = you_contents._Budget()
    for k in you_contents._stats:
        you_contents._stats[k] = 0
    yield


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _ok(pages):
    return lambda req: httpx.Response(200, json=pages)


# ── keys ─────────────────────────────────────────────────────────────────────

def test_plural_keys_are_split_and_deduplicated():
    env = {"YOU_KEYS": "k1, k2\nk3", "YOU_API_KEY": "k1", "YDC_API_KEY": "k4"}
    assert you_contents.collect_keys(env) == ["k1", "k2", "k3", "k4"]


def test_no_key_means_disabled_and_no_call(monkeypatch):
    called = []

    def handler(req):
        called.append(req)
        return httpx.Response(200, json=[])

    assert you_contents.enabled() is False
    got = asyncio.run(you_contents.fetch_contents(["https://a.test/x"],
                                                  _client(handler)))
    assert got == {} and called == []


def test_switch_off_is_honoured_even_with_keys(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    monkeypatch.setenv("YOU_CONTENTS_ENABLED", "0")
    assert you_contents.enabled() is False


# ── the happy path ───────────────────────────────────────────────────────────

def test_returns_prose_and_strips_markdown_furniture(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    seen = {}

    def handler(req):
        seen["url"] = str(req.url)
        seen["key"] = req.headers.get("x-api-key")
        return httpx.Response(200, json=[
            {"url": "https://a.test/x",
             "markdown": f"# Head\n\n![img](p.png)\n[Subscribe](/s)\n\n{LONG}"}])

    got = asyncio.run(you_contents.fetch_contents(["https://a.test/x"],
                                                  _client(handler)))
    assert seen["url"].endswith("/v1/contents")
    assert seen["key"] == "k1"
    body = got["https://a.test/x"]
    assert "![img]" not in body and "(p.png)" not in body
    assert body.startswith("Head") and "word199" in body


def test_envelope_or_bare_array_both_parse():
    pages = [{"url": "https://a.test/x", "markdown": LONG}]
    assert you_contents._pages(pages)
    assert you_contents._pages({"results": pages})
    assert you_contents._pages({"data": pages})
    assert you_contents._pages({"unknown": pages}) == []
    assert you_contents._pages("nonsense") == []


def test_base_url_is_configurable(monkeypatch):
    monkeypatch.setenv("YOU_API_BASE", "https://api.you.com/")
    assert you_contents.api_base() == "https://api.you.com"


# ── the budget ───────────────────────────────────────────────────────────────

def test_budget_is_a_hard_ceiling(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    monkeypatch.setenv("YOU_PAGE_BUDGET_PER_DAY", "2")
    asked = []

    def handler(req):
        import json as _json
        urls = _json.loads(req.content)["urls"]
        asked.extend(urls)
        return httpx.Response(200, json=[{"url": u, "markdown": LONG}
                                         for u in urls])

    urls = [f"https://a.test/{i}" for i in range(5)]
    got = asyncio.run(you_contents.fetch_contents(urls, _client(handler)))
    assert len(asked) == 2 and len(got) == 2
    # Spent is spent: a second pass the same day buys nothing.
    assert asyncio.run(you_contents.fetch_contents(urls, _client(handler))) == {}
    assert len(asked) == 2


def test_failed_call_refunds_the_pages_it_never_fetched(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    monkeypatch.setenv("YOU_PAGE_BUDGET_PER_DAY", "4")

    def dead(req):
        return httpx.Response(500)

    asyncio.run(you_contents.fetch_contents(
        ["https://a.test/1", "https://a.test/2"], _client(dead)))
    assert you_contents._budget.snapshot()["spent"] == 0

    def ok(req):
        import json as _json
        urls = _json.loads(req.content)["urls"]
        return httpx.Response(200, json=[{"url": u, "markdown": LONG}
                                         for u in urls])

    got = asyncio.run(you_contents.fetch_contents(
        ["https://a.test/3", "https://a.test/4"], _client(ok)))
    assert len(got) == 2


def test_batches_are_capped_at_ten_urls(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    monkeypatch.setenv("YOU_PAGE_BUDGET_PER_DAY", "100")
    sizes = []

    def handler(req):
        import json as _json
        urls = _json.loads(req.content)["urls"]
        sizes.append(len(urls))
        return httpx.Response(200, json=[{"url": u, "markdown": LONG}
                                         for u in urls])

    urls = [f"https://a.test/{i}" for i in range(23)]
    asyncio.run(you_contents.fetch_contents(urls, _client(handler)))
    assert sizes == [10, 10, 3]
    assert max(sizes) <= you_contents.MAX_URLS_PER_CALL


# ── failures never bite ──────────────────────────────────────────────────────

@pytest.mark.parametrize("handler", [
    lambda req: httpx.Response(500),
    lambda req: httpx.Response(200, content=b"not json"),
    lambda req: httpx.Response(200, json={"weird": 1}),
])
def test_bad_answers_return_nothing_and_do_not_raise(monkeypatch, handler):
    monkeypatch.setenv("YOU_KEYS", "k1")
    assert asyncio.run(you_contents.fetch_contents(["https://a.test/x"],
                                                   _client(handler))) == {}


def test_transport_error_is_swallowed(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")

    def boom(req):
        raise httpx.ConnectError("no route")

    assert asyncio.run(you_contents.fetch_contents(["https://a.test/x"],
                                                   _client(boom))) == {}
    assert you_contents._stats["transport_errors"] == 1


def test_key_is_rotated_on_401_not_on_500(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "bad,good")
    keys = []

    def handler(req):
        key = req.headers.get("x-api-key")
        keys.append(key)
        if key == "bad":
            return httpx.Response(401)
        return httpx.Response(200, json=[{"url": "https://a.test/x",
                                          "markdown": LONG}])

    got = asyncio.run(you_contents.fetch_contents(["https://a.test/x"],
                                                  _client(handler)))
    assert keys == ["bad", "good"] and got

    keys.clear()
    you_contents._budget = you_contents._Budget()
    asyncio.run(you_contents.fetch_contents(["https://a.test/y"],
                                            _client(lambda r: httpx.Response(500))))
    # A 500 is about the request, not the key, so the pool is not spent on it.
    assert you_contents._stats["rotations"] == 1


def test_stats_never_expose_a_key(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "s3cret-one,s3cret-two")
    blob = repr(you_contents.stats()) + you_contents.diagnosis()
    assert "s3cret" not in blob
    assert you_contents.stats()["keys"] == 2


# ── the seam in article_reader ───────────────────────────────────────────────

def test_fallback_runs_only_for_pages_our_reader_missed(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    asked = {}

    async def fake_fetch_article(url, client=None):
        return LONG if url.endswith("/read") else ""

    async def fake_contents(urls, client=None):
        asked["urls"] = list(urls)
        return {u: LONG for u in urls}

    monkeypatch.setattr(article_reader, "fetch_article", fake_fetch_article)
    monkeypatch.setattr(you_contents, "fetch_contents", fake_contents)

    out = asyncio.run(article_reader.fetch_many(
        {1: "https://a.test/read", 2: "https://a.test/blocked"}))
    assert asked["urls"] == ["https://a.test/blocked"]
    assert out[1] and out[2]


def test_no_fallback_call_when_every_page_was_read(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")
    called = []

    async def fake_fetch_article(url, client=None):
        return LONG

    async def fake_contents(urls, client=None):
        called.append(urls)
        return {}

    monkeypatch.setattr(article_reader, "fetch_article", fake_fetch_article)
    monkeypatch.setattr(you_contents, "fetch_contents", fake_contents)
    asyncio.run(article_reader.fetch_many({1: "https://a.test/x"}))
    assert called == []


def test_redirected_url_still_matches_its_row(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")

    async def fake_fetch_article(url, client=None):
        return ""

    async def fake_contents(urls, client=None):
        return {"https://www.a.test/x/": LONG}

    monkeypatch.setattr(article_reader, "fetch_article", fake_fetch_article)
    monkeypatch.setattr(you_contents, "fetch_contents", fake_contents)
    out = asyncio.run(article_reader.fetch_many({7: "http://a.test/x"}))
    assert out[7] == LONG


def test_recovered_text_faces_the_same_word_floor(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")

    async def fake_fetch_article(url, client=None):
        return ""

    async def fake_contents(urls, client=None):
        return {u: "Too short to be an article." for u in urls}

    monkeypatch.setattr(article_reader, "fetch_article", fake_fetch_article)
    monkeypatch.setattr(you_contents, "fetch_contents", fake_contents)
    assert asyncio.run(article_reader.fetch_many({1: "https://a.test/x"})) == {}


def test_fallback_failure_leaves_the_old_behaviour(monkeypatch):
    monkeypatch.setenv("YOU_KEYS", "k1")

    async def fake_fetch_article(url, client=None):
        return LONG if url.endswith("/read") else ""

    async def boom(urls, client=None):
        raise RuntimeError("you.com is down")

    monkeypatch.setattr(article_reader, "fetch_article", fake_fetch_article)
    monkeypatch.setattr(you_contents, "fetch_contents", boom)
    out = asyncio.run(article_reader.fetch_many(
        {1: "https://a.test/read", 2: "https://a.test/blocked"}))
    assert list(out) == [1]


def test_admin_endpoint_is_guarded():
    import main
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        assert c.get("/admin/you").status_code == 403
        r = c.get("/admin/you", params={"token": main.ADMIN_TOKEN})
        assert r.status_code == 200
        assert "DIAGNOSIS" in r.json()
