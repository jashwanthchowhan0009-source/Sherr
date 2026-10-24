"""circuit.py, telemetry.py, and the provider cascade they wrap.

THE POLICY IS THE POINT, not the state machine. A breaker on the wrong signal is
worse than no breaker: `body_drain_job` already backs off on a 429 (read from
`ai_processor.PROVIDER_ERRORS`), so a breaker that opened on one would take a
HEALTHY provider out of service for a cooldown and stall the only pass writing
the corpus. These tests pin which statuses trip it and which can never, and they
pin the two ways a breaker can fail worse than it helps — never closing, and
sending a whole batch at a provider that is down.

The clock is injected, so every cooldown is asserted without waiting for one.
"""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import circuit      # noqa: E402
import key_pool     # noqa: E402
import telemetry    # noqa: E402


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


@pytest.fixture
def clk():
    return Clock()


@pytest.fixture
def b(clk, monkeypatch):
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "3")
    monkeypatch.setenv("CIRCUIT_COOLDOWN_S", "60")
    monkeypatch.setenv("CIRCUIT_MAX_COOLDOWN_S", "240")
    return circuit.Breaker("gemini", clock=clk)


# ─── which statuses trip it ──────────────────────────────────────────────────
@pytest.mark.parametrize("status", [0, 500, 502, 503, 504, 599])
def test_no_response_and_5xx_are_evidence_the_provider_is_down(status):
    assert circuit.trips_breaker(status)


@pytest.mark.parametrize("status", [200, 400, 401, 403, 404, 422, 429])
def test_a_status_the_provider_answered_with_never_trips_it(status):
    """401/403/429 belong to key_pool, which rotates on exactly those. 400/404
    are configuration errors — a 404 is how a RETIRED MODEL ID presents, and
    cooling down and retrying is the wrong shape for something only a config
    change fixes."""
    assert not circuit.trips_breaker(status)


def test_the_rotate_statuses_and_the_trip_statuses_do_not_overlap():
    """The invariant behind the whole policy: a status cannot both mean "this
    key is the problem" and "this provider is down". If key_pool ever adds one
    that trips, a bad key in a pool of good ones would take the provider out."""
    assert not any(circuit.trips_breaker(s) for s in key_pool.ROTATE_STATUSES)


def test_a_run_of_429s_can_never_open_the_breaker(b):
    """The single most important property here. The free tier bounces bursts
    routinely; a breaker opening on it would be the opposite of the correct
    response."""
    for _ in range(50):
        b.record_failure(429)
    assert b.state() == circuit.CLOSED
    assert b.allow() is True
    assert b.failures_seen == 50       # still counted, for the diagnosis
    assert b.trips == 0


# ─── opening ─────────────────────────────────────────────────────────────────
def test_one_timeout_does_not_open_it(b):
    """A single timeout is ordinary on a free tier and on a cold start."""
    b.record_failure(0)
    assert b.state() == circuit.CLOSED
    assert b.allow() is True


def test_it_opens_on_the_threshold_and_short_circuits(b):
    for _ in range(3):
        b.record_failure(0, "connect timeout")
    assert b.state() == circuit.OPEN
    assert b.allow() is False
    assert b.trips == 1
    assert b.short_circuited == 1
    assert "timeout" in b.last_reason


def test_a_success_clears_the_count_rather_than_decrementing_it(b):
    """Four failures and then a success is not three failures; it is a provider
    that works."""
    b.record_failure(0)
    b.record_failure(0)
    b.record_success()
    b.record_failure(0)
    b.record_failure(0)
    assert b.state() == circuit.CLOSED


def test_mixed_statuses_do_not_accumulate_towards_a_trip(b, clk):
    """A 429 between two timeouts means the host answered, so the consecutive
    unreachable count starts again."""
    b.record_failure(0)
    b.record_failure(0)
    b.record_failure(429)
    b.record_failure(0)
    b.record_failure(0)
    assert b.state() == circuit.CLOSED


# ─── half-open ───────────────────────────────────────────────────────────────
def _open(b, clk):
    for _ in range(3):
        b.record_failure(0)
    assert b.state() == circuit.OPEN


def test_the_cooldown_is_honoured_then_one_probe_is_allowed(b, clk):
    _open(b, clk)
    clk.advance(59)
    assert b.allow() is False
    assert b.retry_in() == pytest.approx(1.0)
    clk.advance(2)
    assert b.state() == circuit.HALF_OPEN
    assert b.allow() is True


def test_only_one_probe_goes_through_at_a_time(b, clk):
    """Otherwise the whole batch is sent at a provider that is down, which is
    the cost the breaker exists to avoid."""
    _open(b, clk)
    clk.advance(61)
    assert b.allow() is True
    assert b.allow() is False
    assert b.allow() is False


def test_a_successful_probe_closes_it_and_resets_the_cooldown(b, clk):
    _open(b, clk)
    clk.advance(61)
    b.allow()
    b.record_success()
    assert b.state() == circuit.CLOSED
    assert b.allow() is True
    assert b.snapshot()["cooldown_s"] == 60


def test_a_failed_probe_reopens_with_a_doubled_cooldown(b, clk):
    """A provider that has been down for an hour should not be probed every
    minute."""
    _open(b, clk)
    for expected in (120, 240, 240):      # doubles, then hits the ceiling
        clk.advance(b.retry_in() + 1)
        assert b.allow() is True
        b.record_failure(0)
        assert b.state() == circuit.OPEN
        assert b.snapshot()["cooldown_s"] == expected


def test_any_http_answer_during_a_probe_closes_it(b, clk):
    """A 429 is proof the host is reachable and serving. Leaving the breaker
    open on evidence that contradicts it would keep skipping a provider that is
    up."""
    _open(b, clk)
    clk.advance(61)
    b.allow()
    b.record_failure(429)
    assert b.state() == circuit.CLOSED
    assert b.allow() is True


def test_a_probe_with_no_recorded_outcome_is_abandoned(b, clk):
    """The way a breaker fails worse than it helps. `allow()` marks a probe in
    flight so the rest of the batch is not sent; if the caller raises before
    recording an outcome, that flag would never clear and the provider would be
    skipped FOREVER."""
    _open(b, clk)
    clk.advance(61)
    assert b.allow() is True          # probe taken, outcome never recorded
    assert b.allow() is False
    clk.advance(61)
    assert b.allow() is True          # stale probe abandoned, a new one allowed


def test_the_breaker_always_eventually_probes_again(b, clk):
    """A breaker that cannot close is a worse outage than the one it was
    protecting against, so the doubling has a ceiling."""
    _open(b, clk)
    for _ in range(20):
        clk.advance(b.retry_in() + 1)
        assert b.allow() is True
        b.record_failure(0)
    assert b.retry_in() <= circuit.max_cooldown_s()


# ─── switches and reporting ──────────────────────────────────────────────────
def test_the_switch_makes_it_measure_without_short_circuiting(b, clk, monkeypatch):
    """Measured always, enforcing optionally — the same posture
    WRITER_GATE_ENABLED has, so the trip rate can be read off a live instance
    before anything is skipped on it."""
    _open(b, clk)
    monkeypatch.setenv("CIRCUIT_ENABLED", "0")
    assert b.allow() is True
    assert b.trips == 1
    assert "never short-circuit" in circuit.diagnose()


def test_tuning_is_read_at_call_time(monkeypatch):
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "7")
    assert circuit.threshold() == 7
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "nonsense")
    assert circuit.threshold() == 4


def test_the_snapshot_reports_a_reason_and_never_a_key(b, clk):
    _open(b, clk)
    snap = b.snapshot()
    assert snap["state"] == "open"
    assert snap["trips"] == 1 and snap["threshold"] == 3
    assert snap["retry_in_s"] > 0
    assert "key" not in str(snap).lower()


def test_the_registry_is_per_provider_and_resettable():
    circuit.reset_all()
    assert circuit.get("gemini") is circuit.get("gemini")
    assert circuit.get("gemini") is not circuit.get("groq")
    assert set(circuit.snapshot()) == {"gemini", "groq"}
    circuit.reset_all()
    assert circuit.snapshot() == {}


def test_each_state_gets_its_own_diagnosis(monkeypatch, clk):
    circuit.reset_all()
    assert "No provider call" in circuit.diagnose()
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "1")
    br = circuit.Breaker("gemini", clock=clk)
    circuit._BREAKERS["gemini"] = br
    assert "closed" in circuit.diagnose()
    br.record_failure(0, "connect timeout")
    d = circuit.diagnose()
    assert "gemini OPEN" in d and "next probe" in d
    clk.advance(10_000)
    assert "half-open" in circuit.diagnose()


# ─── telemetry ───────────────────────────────────────────────────────────────
def test_a_span_records_count_duration_and_outcome():
    telemetry.reset()
    with telemetry.span("llm.call", "process", provider="gemini") as sp:
        sp["status"] = 200
    s = telemetry.stats()["llm.call:process"]
    assert s["n"] == 1 and s["ok"] == 1 and s["fail"] == 0
    assert s["ms_avg"] >= 0


def test_an_explicit_ok_false_wins_over_not_raising():
    """A call that returned a 500 without raising did not succeed, and the
    caller is what knows it."""
    telemetry.reset()
    with telemetry.span("llm.call", "process") as sp:
        sp["ok"] = False
    assert telemetry.stats()["llm.call:process"]["fail"] == 1


def test_an_exception_is_recorded_and_re_raised():
    """Swallowing it would turn an instrumented call site into a silently
    different one."""
    telemetry.reset()
    with pytest.raises(ValueError):
        with telemetry.span("llm.call", "process"):
            raise ValueError("boom")
    s = telemetry.stats()["llm.call:process"]
    assert s["fail"] == 1 and s["n"] == 1


def test_a_credential_never_reaches_a_line(caplog):
    telemetry.reset()
    with caplog.at_level("INFO", logger="sherbyte.telemetry"):
        telemetry.event("llm.call", api_key="sk-realsecret", GEMINI_API_KEY="x",
                        authorization="Bearer abc", provider="gemini")
    text = caplog.text
    assert "sk-realsecret" not in text and "Bearer abc" not in text
    assert "[redacted]" in text and "gemini" in text


def test_a_long_attribute_is_bounded(caplog):
    """A span attribute is a label, not a payload. An untrimmed body or SQL
    string is how a log quota goes."""
    telemetry.reset()
    with caplog.at_level("INFO", logger="sherbyte.telemetry"):
        telemetry.event("llm.call", prompt="x" * 5000)
    assert len(caplog.text) < 1500


def test_the_series_dict_is_bounded(monkeypatch):
    """An op name is a string a caller supplies; one built from reader input
    would grow this dict with traffic, which is the shape CLAUDE.md's 512MB
    entry warns about."""
    telemetry.reset()
    monkeypatch.setattr(telemetry, "MAX_SERIES", 5)
    for i in range(50):
        with telemetry.span("llm.call", f"op{i}"):
            pass
    assert len(telemetry.stats()) == 5
    assert telemetry.health()["dropped_series"] == 45


def test_top_by_time_orders_by_total_not_average():
    """A 3-second call made once is not the problem; a 90ms one made ten
    thousand times is."""
    telemetry.reset()
    telemetry._record("llm.call", "rare", 3000.0, True)
    for _ in range(100):
        telemetry._record("llm.call", "common", 90.0, True)
    assert telemetry.top_by_time(1)[0]["series"] == "llm.call:common"


def test_the_line_switch_is_separate_from_the_counters(monkeypatch, caplog):
    """The counters are what /admin reads and they cost nothing; the lines are
    what fills a log quota."""
    telemetry.reset()
    monkeypatch.setenv("TELEMETRY_LOG", "0")
    with caplog.at_level("INFO", logger="sherbyte.telemetry"):
        with telemetry.span("llm.call", "process"):
            pass
    assert caplog.text == ""
    assert telemetry.stats()["llm.call:process"]["n"] == 1


def test_telemetry_never_raises_on_an_unserialisable_attribute():
    telemetry.reset()

    class Awkward:
        def __str__(self):
            raise RuntimeError("no")

    telemetry.event("llm.call", thing=Awkward())      # must not propagate


# ─── the cascade actually goes through the breaker ───────────────────────────
# `_try_pool` exists because the three writers — the card rewrite, synthesis and
# the news writer — each had their own copy of the same key-rotation loop. A
# breaker added to only one of them would cover almost none of the drain's
# actual spend, so these tests drive all three.
import asyncio   # noqa: E402

import ai_processor as ai   # noqa: E402


def _pool_with(monkeypatch, provider, keys):
    """Install a real pool with a known set of keys for one provider.

    It goes into `KEYS.pools`, not onto the object `KEYS.get()` hands back:
    `PoolSet.get` returns a FRESH empty KeyPool for a provider that has no keys
    in the environment, so patching the returned object patches a throwaway and
    the call site sees an empty pool. A test written that way makes no calls at
    all and fails for a reason that has nothing to do with what it asserts.

    A real KeyPool, so the actual rotation logic runs — a test must not pass
    because the behaviour it depends on was stubbed out.
    """
    pool = key_pool.KeyPool(provider, list(keys))
    monkeypatch.setitem(ai.KEYS.pools, provider, pool)
    return pool


@pytest.fixture
def one_key(monkeypatch):
    """A one-key pool for gemini and nothing else configured."""
    monkeypatch.setattr(ai.KEYS, "configured", lambda: ["gemini"])
    return _pool_with(monkeypatch, "gemini", ["k1"])


def _count_calls(monkeypatch, table, status, payload=None):
    calls = []

    async def fn(key, *a, **k):
        calls.append(key)
        return payload, status

    monkeypatch.setitem(getattr(ai, table), "gemini", fn)
    return calls


def test_a_dead_provider_is_skipped_once_the_breaker_opens(
        one_key, monkeypatch):
    """The cost this exists to remove: without it every article pays a connect
    timeout per key to re-learn what the last N articles already proved."""
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "3")
    circuit.reset_all()
    calls = _count_calls(monkeypatch, "_PROVIDER_CALLS", 0)

    for _ in range(10):
        asyncio.run(ai._call_provider("gemini", "t", "b", None))

    assert len(calls) == 3                 # then short-circuited
    assert circuit.get("gemini").state() == circuit.OPEN
    assert circuit.get("gemini").short_circuited == 7


def test_a_rate_limited_provider_keeps_being_called(one_key, monkeypatch):
    """The regression that would matter most. A 429 is the free tier bouncing a
    burst; if it opened the breaker the drain would stop writing."""
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "3")
    circuit.reset_all()
    calls = _count_calls(monkeypatch, "_PROVIDER_CALLS", 429)

    for _ in range(10):
        asyncio.run(ai._call_provider("gemini", "t", "b", None))

    assert len(calls) == 10
    assert circuit.get("gemini").state() == circuit.CLOSED


def test_a_working_provider_is_never_short_circuited(one_key, monkeypatch):
    circuit.reset_all()
    calls = _count_calls(monkeypatch, "_PROVIDER_CALLS", 200, payload={"ok": 1})
    for _ in range(10):
        assert asyncio.run(ai._call_provider("gemini", "t", "b", None))
    assert len(calls) == 10
    assert circuit.get("gemini").short_circuited == 0


def test_an_exception_out_of_a_provider_still_records_an_outcome(
        one_key, monkeypatch):
    """`allow()` marks a probe in flight, and a probe with no outcome wedges the
    breaker shut — so the record happens in a `finally`."""
    circuit.reset_all()

    async def boom(key, *a, **k):
        raise RuntimeError("socket blew up")

    monkeypatch.setitem(ai._PROVIDER_CALLS, "gemini", boom)
    with pytest.raises(RuntimeError):
        asyncio.run(ai._call_provider("gemini", "t", "b", None))
    br = circuit.get("gemini")
    assert br.failures_seen == 1
    assert br.allow() is True              # not wedged


def test_an_unconfigured_pool_does_not_open_a_breaker(monkeypatch):
    """A status of 0 for "no call was made" would read as "no response" and
    open the breaker over a key-configuration problem."""
    circuit.reset_all()
    monkeypatch.setattr(ai.KEYS, "configured", lambda: ["gemini"])
    _pool_with(monkeypatch, "gemini", [])
    for _ in range(10):
        assert asyncio.run(ai._call_provider("gemini", "t", "b", None)) is None
    assert circuit.get("gemini").state() == circuit.CLOSED


def test_the_cascade_falls_through_an_open_breaker_to_the_next_provider(
        monkeypatch):
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "1")
    circuit.reset_all()
    monkeypatch.setattr(ai.KEYS, "configured", lambda: ["gemini", "groq"])
    for name in ("gemini", "groq"):
        _pool_with(monkeypatch, name, ["k"])

    seen = []

    async def dead(key, *a, **k):
        seen.append("gemini")
        return None, 0

    async def alive(key, *a, **k):
        seen.append("groq")
        return {"ok": 1}, 200

    monkeypatch.setitem(ai._PROVIDER_CALLS, "gemini", dead)
    monkeypatch.setitem(ai._PROVIDER_CALLS, "groq", alive)

    assert asyncio.run(ai._call_cascade("t", "b", None))
    seen.clear()
    assert asyncio.run(ai._call_cascade("t", "b", None))
    assert "gemini" not in seen            # skipped, groq answered immediately


def test_synthesis_goes_through_the_breaker(one_key, monkeypatch):
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "2")
    circuit.reset_all()
    calls = _count_calls(monkeypatch, "_SYNTH_CALLS", 0)
    for _ in range(6):
        asyncio.run(ai.synthesize("prompt", n_sources=2))
    assert len(calls) == 2
    assert circuit.get("gemini").state() == circuit.OPEN


def test_the_news_writer_goes_through_the_breaker(one_key, monkeypatch):
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "2")
    circuit.reset_all()
    calls = _count_calls(monkeypatch, "_NEWS_CALLS", 0)
    for _ in range(6):
        asyncio.run(ai.news_writer("some source text"))
    assert len(calls) == 2
    assert circuit.get("gemini").state() == circuit.OPEN


def test_a_rejected_synthesis_is_not_a_breaker_failure(one_key, monkeypatch):
    """The provider answered, and the next provider would be asked the same
    question with the same prompt. It is recorded in PROVIDER_ERRORS, which is
    where /admin/body-audit reads it."""
    circuit.reset_all()
    _count_calls(monkeypatch, "_SYNTH_CALLS", 200, payload="not json at all")
    for _ in range(10):
        assert asyncio.run(ai.synthesize("prompt", n_sources=2)) is None
    assert circuit.get("gemini").state() == circuit.CLOSED
    assert circuit.get("gemini").successes == 10


def test_every_provider_call_is_measured(one_key, monkeypatch):
    telemetry.reset()
    circuit.reset_all()
    _count_calls(monkeypatch, "_PROVIDER_CALLS", 200, payload={"ok": 1})
    asyncio.run(ai._call_provider("gemini", "t", "b", None))
    _count_calls(monkeypatch, "_SYNTH_CALLS", 200, payload='{"a":1}')
    asyncio.run(ai.synthesize("p"))
    assert set(telemetry.stats()) == {"llm.call:process", "llm.call:synthesize"}


def test_a_short_circuit_is_visible_as_its_own_event(one_key, monkeypatch, caplog):
    """A skip is not a call, so it must not be counted as a failed one — the
    pass rate would be wrong. It gets its own event instead."""
    monkeypatch.setenv("CIRCUIT_THRESHOLD", "1")
    telemetry.reset()
    circuit.reset_all()
    _count_calls(monkeypatch, "_PROVIDER_CALLS", 0)
    asyncio.run(ai._call_provider("gemini", "t", "b", None))
    with caplog.at_level("INFO", logger="sherbyte.telemetry"):
        asyncio.run(ai._call_provider("gemini", "t", "b", None))
    assert "circuit_open" in caplog.text
    assert telemetry.stats()["llm.call:process"]["n"] == 1


def test_the_key_count_is_not_mistaken_for_a_credential(caplog):
    """`keys` is how many keys the pool holds. A substring match on "key"
    redacted it, which removed a number the diagnosis wants and revealed
    nothing by keeping it."""
    telemetry.reset()
    with caplog.at_level("INFO", logger="sherbyte.telemetry"):
        telemetry.event("llm.call", keys=3, key="sk-secret")
    assert '"keys": 3' in caplog.text
    assert "sk-secret" not in caplog.text


# ─── /admin/pipeline ─────────────────────────────────────────────────────────
from fastapi.testclient import TestClient   # noqa: E402

import main   # noqa: E402


def test_admin_pipeline_is_guarded():
    """tests/test_play_store_readiness.py asserts no /admin route is unguarded,
    and two already shipped that way. Asserted here too, next to the endpoint."""
    assert TestClient(main.app).get("/admin/pipeline").status_code in (401, 403)


def test_admin_pipeline_reports_both_ledgers_and_touches_no_database(monkeypatch):
    """No database, deliberately: the moment you want this endpoint is the
    moment a query would time out."""
    monkeypatch.setattr(main, "_check_admin", lambda t: None)

    def _no_db():
        raise AssertionError("/admin/pipeline must not open a connection")

    monkeypatch.setattr(main, "get_db", _no_db)
    circuit.reset_all()
    telemetry.reset()
    circuit.get("gemini").record_failure(0, "connect timeout")
    with telemetry.span("llm.call", "process"):
        pass

    body = TestClient(main.app).get("/admin/pipeline").json()
    assert body["circuits"]["breakers"]["gemini"]["failures_seen"] == 1
    assert body["circuits"]["DIAGNOSIS"]
    assert body["telemetry"]["series"]["llm.call:process"]["n"] == 1
    assert body["telemetry"]["top_by_time"][0]["series"] == "llm.call:process"
    assert body["telemetry"]["health"]["max_series"] == telemetry.MAX_SERIES
