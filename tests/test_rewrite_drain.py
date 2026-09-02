"""The continuous rewrite drain: unattended, and inside the free tier.

The backlog is ~25,000 articles and the rewrite is RATE limited, not compute
limited — Gemini's free tier allows 15 requests a minute and one article is one
request. So the only way through is to keep going slowly without anyone pressing
a button, and the only way to stay inside the tier is for the schedule itself to
bound the rate.

These pin the three things that make it safe to leave running: it cannot exceed
the quota, it resumes on its own after a restart, and it actually backs off when
the provider says 429 rather than hammering a saturated tier.
"""
import asyncio
import os
import sqlite3
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import ai_processor  # noqa: E402
import main  # noqa: E402


def _live_ai():
    """The ai_processor module `main` will actually resolve at call time.

    A module-level `import ai_processor` here can end up bound to a DIFFERENT
    object than sys.modules holds, because other test files reload modules
    mid-suite. main._seen_rate_limit() does its own import, so recording a 429
    on the stale object left it looking at an empty list — the backoff tests
    passed alone and failed in the suite for exactly that reason.
    """
    return sys.modules["ai_processor"]


def _record_429():
    ap = _live_ai()
    ap._record_error("gemini", 429, "rate limited")


@pytest.fixture(autouse=True)
def _clean_state():
    """RESET to a known baseline, do not save-and-restore.

    Save/restore preserves whatever the previous test left: each test restores
    the value IT saw at setup, so pollution chains down the file instead of
    being cleared. These two tests passed alone and failed in the suite for
    exactly that reason.
    """
    baseline = {
        "enabled": True, "rpm": main.BODY_DRAIN_RPM, "ticks": 0,
        "rewritten_total": 0, "failed_total": 0, "skipped_ticks": 0,
        "last_tick_at": None, "last_rewritten": 0, "last_failed": 0,
        "backoff_ticks_left": 0, "backoff_reason": None,
        "consecutive_429": 0, "remaining": None, "last_error": None,
    }
    main._rewrite_drain.clear()
    main._rewrite_drain.update(baseline)
    _live_ai().PROVIDER_ERRORS.clear()
    # _body_task is module state another test file leaves set. The drain
    # correctly yields to a run in progress, so a stale task made every backoff
    # test take that early return and assert nothing — passing alone, failing in
    # the suite.
    main._body_task = None
    yield
    main._rewrite_drain.clear()
    main._rewrite_drain.update(baseline)
    _live_ai().PROVIDER_ERRORS.clear()


def _provider(monkeypatch):
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "gemini": 1,
                                 "total_keys": 1, "model": "test"})


# ─── the rate ceiling ───────────────────────────────────────────────────────

def test_the_configured_rate_is_under_the_free_tier_limit():
    """15/min is the published ceiling. Sitting at 15 leaves no room for the
    ingest pass or a manual run sharing the same quota."""
    per_min = main.BODY_DRAIN_RPM * 60 / main.BODY_DRAIN_INTERVAL_S
    assert per_min <= 12, f"{per_min}/min is too close to the 15/min ceiling"


def test_a_tick_never_asks_for_more_than_the_rate_allows(monkeypatch):
    """The tick IS the quota window: whatever else happens, one tick cannot
    request more articles than the rate permits."""
    _provider(monkeypatch)
    seen = {}

    def fake(limit, batch, concurrency=None):
        seen.update(limit=limit, batch=batch, concurrency=concurrency)
        return {"rewritten": 0, "failed": 0, "remaining": 5}

    monkeypatch.setattr(main, "_reprocess_bodies_sync", fake)
    asyncio.run(main.body_drain_job())
    assert seen["limit"] <= main.BODY_DRAIN_RPM
    assert seen["batch"] <= main.BODY_DRAIN_RPM


def test_the_drain_forces_serial_requests(monkeypatch):
    """Five in flight at once is five requests against the same per-minute
    quota — concurrency defeats the rate limit it is supposed to respect."""
    _provider(monkeypatch)
    seen = {}
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: seen.update(concurrency=c) or
                        {"rewritten": 0, "failed": 0, "remaining": 0})
    asyncio.run(main.body_drain_job())
    assert seen["concurrency"] == 1


# ─── unattended operation ───────────────────────────────────────────────────

def test_it_runs_without_anyone_triggering_it():
    """Registered as an interval job, not a cron and not an endpoint."""
    src = open(main.__file__).read()
    assert 'scheduler.add_job(body_drain_job, "interval"' in src
    assert 'id="body_drain"' in src


def test_ticks_accumulate_across_calls(monkeypatch):
    """Progress is cumulative — the point is that it grinds down a backlog."""
    _provider(monkeypatch)
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: {"rewritten": 3, "failed": 1,
                                              "remaining": 100})
    for _ in range(3):
        asyncio.run(main.body_drain_job())
    assert main._rewrite_drain["ticks"] == 3
    assert main._rewrite_drain["rewritten_total"] == 9
    assert main._rewrite_drain["failed_total"] == 3


def test_it_resumes_from_whatever_is_left_not_from_a_cursor(monkeypatch):
    """There is no offset to lose across a restart: the selector asks for rows
    that are still placeholders, so the remaining work IS the state."""
    import body_state
    assert "reprocessed" in body_state.SELECT_NEEDING_REWRITE
    assert "OFFSET" not in body_state.SELECT_NEEDING_REWRITE.upper()


# ─── backoff ────────────────────────────────────────────────────────────────

def test_a_429_makes_it_sit_out_ticks(monkeypatch):
    _provider(monkeypatch)
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: {"rewritten": 0, "failed": 1,
                                              "remaining": 10})
    _record_429()
    asyncio.run(main.body_drain_job())
    assert main._rewrite_drain["backoff_ticks_left"] > 0
    assert "429" in (main._rewrite_drain["backoff_reason"] or "")


def test_backoff_grows_with_consecutive_rate_limits(monkeypatch):
    """A tier that is genuinely saturated should be left alone, not poked every
    minute."""
    _provider(monkeypatch)
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: {"rewritten": 0, "failed": 1,
                                              "remaining": 10})
    waits = []
    for _ in range(3):
        main._rewrite_drain["backoff_ticks_left"] = 0
        _record_429()
        asyncio.run(main.body_drain_job())
        waits.append(main._rewrite_drain["backoff_ticks_left"])
    assert waits[1] > waits[0] and waits[2] > waits[1]


def test_backoff_is_capped_so_it_never_stops_forever(monkeypatch):
    _provider(monkeypatch)
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: {"rewritten": 0, "failed": 1,
                                              "remaining": 10})
    for _ in range(20):
        main._rewrite_drain["backoff_ticks_left"] = 0
        _record_429()
        asyncio.run(main.body_drain_job())
    assert main._rewrite_drain["backoff_ticks_left"] <= main.BODY_DRAIN_MAX_BACKOFF


def test_a_backoff_tick_does_no_work(monkeypatch):
    _provider(monkeypatch)
    calls = []
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: calls.append(1) or
                        {"rewritten": 0, "failed": 0, "remaining": 0})
    main._rewrite_drain["backoff_ticks_left"] = 2
    asyncio.run(main.body_drain_job())
    assert calls == [] and main._rewrite_drain["backoff_ticks_left"] == 1


def test_a_non_rate_limit_failure_does_not_trigger_backoff(monkeypatch):
    """Backing off for a failure that has nothing to do with the quota slows
    the drain for no reason."""
    _provider(monkeypatch)
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: {"rewritten": 0, "failed": 12,
                                              "remaining": 10})
    _live_ai()._record_error("gemini", 500, "server error")
    asyncio.run(main.body_drain_job())
    assert main._rewrite_drain["backoff_ticks_left"] == 0


def test_a_successful_tick_clears_the_backoff_counter(monkeypatch):
    _provider(monkeypatch)
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: {"rewritten": 5, "failed": 0,
                                              "remaining": 10})
    main._rewrite_drain["consecutive_429"] = 4
    asyncio.run(main.body_drain_job())
    assert main._rewrite_drain["consecutive_429"] == 0


# ─── it declines to run when running would be pointless or unsafe ───────────

def test_it_does_not_run_without_a_provider(monkeypatch):
    """Rewriting with no provider writes the placeholder again."""
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "rule-based", "total_keys": 0})
    calls = []
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: calls.append(1))
    asyncio.run(main.body_drain_job())
    assert calls == []
    assert "no AI provider" in (main._rewrite_drain["backoff_reason"] or "")


def test_it_yields_to_a_manual_run_rather_than_doubling_the_rate(monkeypatch):
    """Two passes against one quota is how a limit gets breached while each
    caller believes it is inside it."""
    _provider(monkeypatch)
    calls = []
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: calls.append(1))

    class _Busy:
        def done(self):
            return False

    monkeypatch.setattr(main, "_body_task", _Busy())
    asyncio.run(main.body_drain_job())
    assert calls == []


def test_it_can_be_switched_off(monkeypatch):
    monkeypatch.setattr(main, "BODY_DRAIN_ENABLED", False)
    calls = []
    monkeypatch.setattr(main, "_reprocess_bodies_sync",
                        lambda l, b, c=None: calls.append(1))
    asyncio.run(main.body_drain_job())
    assert calls == []


# ─── what /admin/body-audit reports ─────────────────────────────────────────

def test_body_audit_reports_progress_and_a_time_to_clear(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "ADMIN_TOKEN", "tok")
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        d = c.get("/admin/body-audit", params={"token": "tok"}).json()
    drain = d["drain"]
    for key in ("enabled", "rpm", "ticks", "rewritten_total", "remaining",
                "per_hour", "hours_to_clear", "status", "backoff_reason"):
        assert key in drain, key
    assert drain["per_hour"] == main.BODY_DRAIN_RPM * 3600 // main.BODY_DRAIN_INTERVAL_S
