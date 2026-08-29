"""
API-key rotation pools.

The deployment carries several keys per provider — GEMINI_API_KEY,
GEMINI_API_KEY_4, GEMINI_API_KEY_9, GROQ_API_KEY_4, GPT_API_KEY_4 — but the code
read one fixed name per provider. The spares were dead weight, GROQ_API_KEY_4 was
never read at all, and one 429 on the single Gemini key took the whole rewrite
pass down with it.
"""

import asyncio
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import key_pool as kp  # noqa: E402

ENV = {
    "GEMINI_API_KEY": "g1", "GEMINI_API_KEY_4": "g4", "GEMINI_API_KEY_9": "g9",
    "GROQ_API_KEY_4": "q4", "GPT_API_KEY_4": "o4", "GROK_API_KEY": "x1",
    # Must not be mistaken for keys.
    "GEMINI_MODEL": "gemini-2.5-flash", "GROQ_MODEL": "llama-3.1-8b-instant",
    "DATABASE_URL": "postgres://x", "ADMIN_TOKEN": "t",
}


# ─── collection ──────────────────────────────────────────────────────────────
def test_the_reported_environment_produces_the_expected_pool_sizes():
    assert kp.PoolSet(ENV).sizes() == {"gemini": 3, "groq": 1, "openai": 1, "grok": 1}


def test_groq_and_grok_never_share_a_key():
    """One letter apart, two different services with different endpoints. The
    code used to read GROK_API_KEY and post it to api.groq.com, so whichever key
    existed, one provider was always called with the other's credential."""
    got = kp.collect(ENV)
    assert got["groq"] == ["q4"]
    assert got["grok"] == ["x1"]
    assert not set(got["groq"]) & set(got["grok"])


def test_a_model_variable_is_not_collected_as_a_key():
    """GEMINI_MODEL shares the GEMINI_ stem but is not GEMINI_API_KEY-prefixed."""
    assert "gemini-2.5-flash" not in kp.collect(ENV)["gemini"]


def test_the_base_key_comes_first_then_numeric_suffixes_in_order():
    """Deterministic, so "key #2 failed" means the same key tomorrow."""
    assert kp.collect(ENV)["gemini"] == ["g1", "g4", "g9"]


def test_suffixes_sort_numerically_not_as_text():
    env = {"GEMINI_API_KEY_2": "b", "GEMINI_API_KEY_10": "c", "GEMINI_API_KEY": "a"}
    assert kp.collect(env)["gemini"] == ["a", "b", "c"]


def test_the_same_secret_under_two_names_counts_once():
    """Counting it twice makes the pool look deeper than it is at exactly the
    moment that matters."""
    env = {"GROQ_API_KEY": "same", "GROQ_API_KEY_2": "same", "GROQ_API_KEY_3": "other"}
    assert kp.collect(env)["groq"] == ["same", "other"]


def test_blank_and_missing_values_are_not_keys():
    env = {"GEMINI_API_KEY": "", "GEMINI_API_KEY_4": "   ", "GEMINI_API_KEY_9": "real"}
    assert kp.collect(env)["gemini"] == ["real"]


def test_an_empty_environment_yields_empty_pools_not_an_error():
    ps = kp.PoolSet({})
    assert ps.sizes() == {"gemini": 0, "groq": 0, "openai": 0, "grok": 0}
    assert ps.configured() == []


# ─── rotation ────────────────────────────────────────────────────────────────
def test_rotation_walks_every_key_once_then_reports_the_pool_spent():
    pool = kp.KeyPool("gemini", ["a", "b", "c"])
    assert pool.current() == "a"
    assert pool.rotate("429") and pool.current() == "b"
    assert pool.rotate("429") and pool.current() == "c"
    assert pool.rotate("429") is False, "a spent pool must say so"


def test_reset_makes_the_pool_usable_again_for_the_next_request():
    """Exhaustion is per-request: a key rate limited a minute ago is usually
    fine now, so it must not be burned for the process lifetime."""
    pool = kp.KeyPool("gemini", ["a", "b"])
    pool.rotate("429"); pool.rotate("429")
    assert pool.rotate("429") is False
    pool.reset()
    assert pool.rotate("429") is True


def test_a_single_key_pool_reports_spent_immediately():
    pool = kp.KeyPool("grok", ["only"])
    assert pool.current() == "only"
    assert pool.rotate("429") is False


def test_an_empty_pool_is_safe_to_use():
    pool = kp.KeyPool("openai", [])
    assert pool.current() is None and pool.rotate("429") is False
    assert pool.label().endswith("none")


def test_the_label_never_contains_the_key_itself():
    """It goes to the logs."""
    pool = kp.KeyPool("gemini", ["super-secret-value"])
    assert "super-secret" not in pool.label()


def test_only_key_specific_statuses_trigger_rotation():
    """429 is 'this key is limited'; 401/403 is 'this key is bad'. A 500 is the
    service, and rotating would spend the pool repeating one failure."""
    assert kp.ROTATE_STATUSES == {401, 403, 429}
    for bad in (500, 502, 400, 404, 200):
        assert bad not in kp.ROTATE_STATUSES


# ─── the cascade, end to end ─────────────────────────────────────────────────
def _ai(env):
    """Import ai_processor against a specific environment."""
    for k in list(os.environ):
        if any(k.startswith(p) for p in kp.PROVIDER_PREFIXES.values()):
            os.environ.pop(k, None)
    os.environ.update(env)
    for m in ("ai_processor",):
        sys.modules.pop(m, None)
    import ai_processor
    return ai_processor


def test_available_providers_reports_pool_sizes():
    ai = _ai(ENV)
    got = ai.available_providers()
    assert got["gemini"] == 3 and got["groq"] == 1
    assert got["openai"] == 1 and got["grok"] == 1
    assert got["primary"] == "gemini"
    assert got["cascade"] == ["gemini", "groq", "openai", "grok"]


def test_a_size_is_still_truthy_so_existing_callers_keep_working():
    ai = _ai(ENV)
    p = ai.available_providers()
    assert bool(p["gemini"]) is True
    assert p["primary"] != "rule-based"


def test_with_no_keys_at_all_the_primary_is_rule_based():
    ai = _ai({})
    p = ai.available_providers()
    assert p["primary"] == "rule-based" and p["total_keys"] == 0


def test_a_429_rotates_to_the_next_key_before_leaving_the_provider():
    """The behaviour the whole change exists for: key 1 is rate limited, key 2
    answers, and Gemini is never abandoned for the fallback."""
    ai = _ai(ENV)
    seen = []

    async def fake(key, title, body, client):
        seen.append(key)
        if key == "g1":
            return None, 429
        return {"ok": True, "key": key}, 200

    ai._PROVIDER_CALLS["gemini"] = fake
    got = asyncio.run(ai._call_provider("gemini", "t", "b", None))
    assert got == {"ok": True, "key": "g4"}
    assert seen == ["g1", "g4"], "must try the rate-limited key once, then rotate"


def test_only_when_every_key_is_spent_does_the_next_provider_run():
    ai = _ai(ENV)
    tried = []

    async def all_limited(key, title, body, client):
        tried.append(("gemini", key)); return None, 429

    async def groq_ok(key, title, body, client):
        tried.append(("groq", key)); return {"ok": True}, 200

    ai._PROVIDER_CALLS["gemini"] = all_limited
    ai._PROVIDER_CALLS["groq"] = groq_ok
    got = asyncio.run(ai._call_cascade("t", "b", None))
    assert got == {"ok": True}
    assert [p for p, _ in tried] == ["gemini", "gemini", "gemini", "groq"]
    assert sorted(k for p, k in tried if p == "gemini") == ["g1", "g4", "g9"]


def test_a_server_error_does_not_burn_the_pool():
    """A 500 is the service, not the key. Rotating would spend all three keys
    learning the same thing and leave nothing for the retry."""
    ai = _ai(ENV)
    seen = []

    async def down(key, title, body, client):
        seen.append(key); return None, 500

    ai._PROVIDER_CALLS["gemini"] = down
    assert asyncio.run(ai._call_provider("gemini", "t", "b", None)) is None
    assert seen == ["g1"], "one attempt only"


def test_an_auth_failure_rotates_like_a_rate_limit():
    """A revoked key is as key-specific as a throttled one."""
    ai = _ai(ENV)
    seen = []

    async def revoked_then_ok(key, title, body, client):
        seen.append(key)
        return (None, 401) if key == "g1" else ({"ok": True}, 200)

    ai._PROVIDER_CALLS["gemini"] = revoked_then_ok
    assert asyncio.run(ai._call_provider("gemini", "t", "b", None)) == {"ok": True}
    assert seen == ["g1", "g4"]


def test_each_provider_calls_its_own_endpoint():
    """Groq and Grok are one letter apart and were sharing an endpoint."""
    import inspect
    ai = _ai(ENV)
    src = inspect.getsource(ai)
    assert "api.groq.com" in src and "api.x.ai" in src and "api.openai.com" in src
