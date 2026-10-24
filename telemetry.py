"""
telemetry.py — one structured line per model call, per retrieval query, per tool.

WHAT THIS IS FOR. The admin endpoints in this repo each answer one subsystem's
question well (`/admin/body-audit`, `/admin/writer-doctor`, `/admin/research`,
`/admin/you`, `/admin/memory`, `/admin/search`). What none of them answers is
the one that spans them: *where did the last hour go*. A model call, the page
fetch before it and the query after it are three log lines in three formats,
written by three modules, with no field in common — so "the drain is slow" or
"search got slower after the index" cannot be read off anything.

A span fixes that with one shape:

    {"ev":"llm.call","op":"synthesize","provider":"gemini","ms":812,
     "ok":true,"status":200,"n":3}

One line, one logger (`sherbyte.telemetry`), parseable by `grep | jq` on
Render's log tail, which is the only log tool this deployment has.

WHY NOT OpenTelemetry. It was asked for and it is the wrong tool here, for two
reasons that are about this deployment and not about OTel:

  • There is nowhere to send it. A trace that is exported to no collector is a
    dependency, a background exporter thread and a queue, in exchange for
    nothing. Standing up a collector is a hosting decision, not a code one.
  • `opentelemetry-sdk` plus an exporter and its protobuf runtime is tens of
    megabytes resident on a 512MB instance that has already been OOM-killed for
    reasons CLAUDE.md records in detail. The same entry's rule — that cost which
    scales with traffic needs a bound — applies to instrumentation too.

The span shape here is deliberately OTel-compatible in spirit (a name, a
duration, an ok flag, flat attributes) so that if a collector ever exists, this
becomes an exporter and not a rewrite.

PURE, except for emitting a log line. No network, no database, no background
thread. The counters are a per-process dict — the same stated bound as
`you_contents`' ledger and `circuit`'s breakers — and they are bounded in SIZE
too: `MAX_SERIES` distinct op names, because an op name built from a reader's
query would otherwise grow the dict without limit, which is exactly the shape
the 512MB entry warns about.
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager

log = logging.getLogger("sherbyte.telemetry")

# A distinct op name is a code path, so a few dozen is the true cardinality.
# The cap exists because an op name is a string a caller supplies, and one built
# from reader input would make this dict grow with traffic.
MAX_SERIES = 200

_STATS: dict[str, dict] = {}
_DROPPED_SERIES = 0


def enabled() -> bool:
    """Read at call time, so an instance can be quieted without a deploy."""
    return os.getenv("TELEMETRY_ENABLED", "1") not in ("0", "false", "False")


def log_lines() -> bool:
    """Counters are always kept; the per-event LINE can be turned off on its
    own. They are separate switches because the counters are what /admin reads
    and they cost nothing, while the lines are what fills a log quota."""
    return os.getenv("TELEMETRY_LOG", "1") not in ("0", "false", "False")


# Fields that must never reach a log line, whatever a caller passes — a
# telemetry line is the easiest place to leak a credential into a log that is
# shipped somewhere.
#
# The substring list deliberately does NOT contain a bare "key". It did, and it
# redacted `keys` — the number of keys in the pool, which is a COUNT that the
# diagnosis wants and that reveals nothing. A credential-shaped name is matched
# as a substring (`GEMINI_API_KEY`, `authz`); the bare word is matched exactly.
_REDACT_SUBSTR = ("token", "secret", "password", "authorization", "cookie",
                  "api_key", "apikey", "access_key", "private_key", "dsn",
                  "credential", "passwd")
_REDACT_EXACT = ("key", "auth", "authz", "bearer", "pwd")


def _is_sensitive(name: str) -> bool:
    lk = str(name).lower()
    return lk in _REDACT_EXACT or any(r in lk for r in _REDACT_SUBSTR)


def _safe(fields: dict) -> dict:
    out = {}
    for k, v in (fields or {}).items():
        if _is_sensitive(k):
            out[k] = "[redacted]"
        elif isinstance(v, (int, float, bool)) or v is None:
            out[k] = v
        else:
            # Bounded: a span attribute is a label, not a payload. An untrimmed
            # body or SQL string here is how a log quota goes.
            out[k] = str(v)[:200]
    return out


def event(ev: str, **fields) -> None:
    """Emit one structured line. Never raises — telemetry that can fail the
    thing it is measuring is worse than no telemetry."""
    if not enabled() or not log_lines():
        return
    try:
        payload = {"ev": ev}
        payload.update(_safe(fields))
        log.info(json.dumps(payload, default=str, sort_keys=True))
    except Exception:                                          # pragma: no cover
        pass


def _record(ev: str, op: str, ms: float, ok: bool) -> None:
    global _DROPPED_SERIES
    key = f"{ev}:{op}"
    s = _STATS.get(key)
    if s is None:
        if len(_STATS) >= MAX_SERIES:
            _DROPPED_SERIES += 1
            return
        s = _STATS[key] = {"n": 0, "ok": 0, "fail": 0, "ms_total": 0.0,
                           "ms_max": 0.0}
    s["n"] += 1
    s["ok" if ok else "fail"] += 1
    s["ms_total"] += ms
    s["ms_max"] = max(s["ms_max"], ms)


@contextmanager
def span(ev: str, op: str, **fields):
    """Time a block and emit one line when it ends, whichever way it ends.

    The yielded dict is the span's attributes: a caller mutates it to attach
    what is only known afterwards — the status, the row count, the provider that
    actually answered. An exception is recorded as `ok: false` with its type and
    then RE-RAISED; swallowing it here would turn an instrumented call site into
    a silently different one.
    """
    attrs = dict(fields)
    t0 = time.monotonic()
    ok = True
    try:
        yield attrs
    except BaseException as e:
        ok = False
        attrs.setdefault("error", type(e).__name__)
        raise
    finally:
        ms = (time.monotonic() - t0) * 1000.0
        # An explicit ok=False in the attributes wins: a call that returned a
        # 500 without raising did not succeed, and the caller is what knows it.
        ok = bool(attrs.get("ok", ok))
        attrs["ok"] = ok
        _record(ev, op, ms, ok)
        event(ev, op=op, ms=round(ms, 1), **attrs)


# ─── reporting ───────────────────────────────────────────────────────────────
def stats() -> dict:
    """Per-series counters, with the averages computed on read.

    `ms_avg` is stored as a total and divided here rather than kept as a running
    mean: a running mean cannot be summed across series, and the total can.
    """
    out = {}
    for key, s in sorted(_STATS.items()):
        n = s["n"] or 1
        out[key] = {
            "n": s["n"],
            "ok": s["ok"],
            "fail": s["fail"],
            "ms_avg": round(s["ms_total"] / n, 1),
            "ms_max": round(s["ms_max"], 1),
            "ms_total": round(s["ms_total"], 1),
        }
    return out


def top_by_time(n: int = 8) -> list:
    """Where the time actually went — total, not average. A 3-second call made
    once is not the problem; a 90ms one made ten thousand times is."""
    rows = [{"series": k, **v} for k, v in stats().items()]
    rows.sort(key=lambda r: r["ms_total"], reverse=True)
    return rows[:n]


def health() -> dict:
    return {"series": len(_STATS), "max_series": MAX_SERIES,
            "dropped_series": _DROPPED_SERIES,
            "logging_lines": log_lines(), "enabled": enabled()}


def reset() -> None:
    """Called from the test conftest — module-level counters would otherwise
    carry one test's calls into the next."""
    global _DROPPED_SERIES
    _STATS.clear()
    _DROPPED_SERIES = 0


def diagnose() -> str:
    if not enabled():
        return "TELEMETRY_ENABLED=0 — no spans are recorded."
    if not _STATS:
        return "No instrumented call has run in this process yet."
    rows = top_by_time(3)
    worst = ", ".join(f"{r['series']} {r['ms_total']:.0f}ms/{r['n']}"
                      for r in rows)
    failing = [k for k, v in stats().items() if v["fail"] and not v["ok"]]
    if failing:
        return f"{', '.join(failing[:3])} has not succeeded once. Time: {worst}."
    return f"Time spent, highest first: {worst}."
