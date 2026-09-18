"""A 429 from the AI provider must be retried, not dropped to a stub body.

On a free tier (Groq's tokens-per-minute cap especially) a burst of rewrites
gets 429'd. Before, the single-key path gave up on the first 429 and the article
fell back to the placeholder — so `original` stayed 0 and `stub` stayed at the
whole corpus. _openai_chat_once now retries, honouring Retry-After.
"""
import asyncio


async def _noop_sleep(*_a, **_k):
    return None


class _Resp:
    def __init__(self, status, headers=None, content=None):
        self.status_code = status
        self.headers = headers or {}
        self._content = content or {
            "choices": [{"message": {"content": '{"refined_title":"x"}'}}]
        }

    def json(self):
        return self._content

    @property
    def text(self):
        return "rate limited" if self.status_code == 429 else "ok"


class _Client:
    """Returns 429 twice, then a 200 — proving the retry loop persists."""
    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    async def post(self, *a, **k):
        self.calls += 1
        return self.script.pop(0)


def test_429_is_retried_then_succeeds(monkeypatch):
    import ai_processor as ap
    # Don't actually sleep through the backoff during the test.
    monkeypatch.setattr(ap.asyncio, "sleep", _noop_sleep)
    client = _Client([
        _Resp(429, {"retry-after": "0"}),
        _Resp(429, {"retry-after": "0"}),
        _Resp(200),
    ])
    result, status = asyncio.get_event_loop().run_until_complete(
        ap._openai_chat_once("key", "T", "B", client,
                             url="http://x", model="m", label="Groq"))
    assert status == 200 and result is not None
    assert client.calls == 3            # two 429s, then success


def test_gives_up_after_the_retry_budget(monkeypatch):
    import ai_processor as ap
    monkeypatch.setattr(ap.asyncio, "sleep", _noop_sleep)
    client = _Client([_Resp(429, {"retry-after": "0"})] * 5)
    result, status = asyncio.get_event_loop().run_until_complete(
        ap._openai_chat_once("key", "T", "B", client,
                             url="http://x", model="m", label="Groq"))
    assert status == 429 and result is None
    assert client.calls == 4            # 4 attempts, then stop
