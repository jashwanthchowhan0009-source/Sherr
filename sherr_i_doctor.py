"""
sherr_i_doctor.py — why is /patterns not saying "engine"?

/patterns has three outcomes and they mean very different things:

    seed         SHERR_I_DATABASE_URL and DATABASE_URL are both empty, so the app
                 never looked at Postgres at all and served local demo rows.
    unavailable  A DSN IS set, but building the pool or running the insights
                 query RAISED. Missing table, wrong database, bad credentials.
    engine       The query succeeded. Note this says nothing about there being
                 any insights — an empty `insights` table also reports "engine",
                 with patterns: [].

That last line is the one worth internalising: **an empty engine is not
"unavailable"**. So "no patterns on the page" and "source is not engine" are two
separate faults with two separate fixes, and this module reports on both without
guessing between them.

It answers, in one call, with evidence rather than inference:

    a) is a DSN configured, and does the pool actually connect (with the error)
    b) which of the tables the 20 migrations create exist, and which are missing
    c) row counts for the engine's inputs — signals by domain, entities,
       co-occurrence, the news side, and insights by type
    d) market_reaction run by hand against whatever data is there, with its own
       stage-by-stage funnel (opt-in: it writes)
    e) what is actually scheduled to run the detectors in this process

READ-ONLY BY DEFAULT. Everything except (d) is SELECTs and catalogue lookups.
(d) runs a real detector, which seeds instrument_keywords and can write insights,
so it only runs when explicitly asked for.

NO APP IMPORTS AT MODULE LEVEL. The engine's detectors live under sherrbyte/app
and are imported by path inside the function that needs them — app/spie/discovery
happens to depend on nothing but stdlib, but app.config (pydantic-settings) and
app.db are NOT in the deployed service's requirements, so anything that reaches
them would fail on Render only.
"""

from __future__ import annotations

import logging
import pathlib
import re
import sys
import time

log = logging.getLogger("sherbyte.sherr_i_doctor")

_ROOT = pathlib.Path(__file__).resolve().parent
_ENGINE_ROOT = _ROOT / "sherrbyte"
_MIGRATIONS = _ENGINE_ROOT / "app" / "db" / "migrations"

# The engine's inputs, in the order the pipeline fills them. Each is
# (table, label, why it matters) so a zero reads as a diagnosis, not a number.
_INPUT_TABLES = [
    ("info_objects", "news articles (engine side)",
     "the news half of every news<->market link"),
    ("entities", "canonical entities",
     "instruments and news subjects both resolve to these"),
    ("entity_aliases", "entity aliases", "alias -> canonical resolution"),
    ("domain_signals", "universal signals",
     "EVERY detector reads only from here, never from raw domain data"),
    ("cooccurrence", "entity pair counts",
     "how market_reaction decides which news is 'related'"),
    ("instrument_keywords", "seeded instrument links",
     "the per-instrument half of 'related' (Iran->crude, not Iran->bitcoin)"),
    ("insights", "detector output", "exactly what /patterns serves"),
    ("articles", "engine articles", "raw ingest rows"),
]


def _migration_tables() -> list[str]:
    """Every table the migration files create, read from the files themselves so
    this cannot drift as migrations are added."""
    names = set()
    for f in sorted(_MIGRATIONS.glob("*.sql")):
        for m in re.finditer(r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z_.\"]+)",
                             f.read_text(encoding="utf-8")):
            names.add(m.group(1).replace('"', ""))
    return sorted(names)


def redact(dsn: str) -> str:
    """user@host:port/db — never the password."""
    if not dsn:
        return ""
    try:
        from urllib.parse import urlsplit
        p = urlsplit(dsn)
        # urlsplit does not raise on garbage — it just parses it as a bare path.
        # Without this check a malformed DSN renders as "://?:***@?not a url",
        # which reads like a redacted value rather than a broken one.
        if not p.scheme or not p.hostname:
            return "<unparseable>"
        user = (p.username or "?")
        port = f":{p.port}" if p.port else ""
        return f"{p.scheme}://{user}:***@{p.hostname}{port}{p.path}"
    except Exception:
        return "<unparseable>"


# ─── a) the connection ───────────────────────────────────────────────────────
async def check_connection(dsn: str, which: str, pool_factory) -> dict:
    """Is a DSN configured, and does the pool actually come up?"""
    out = {
        "configured": bool(dsn),
        "source_env_var": which,
        "dsn": redact(dsn),
        "connected": False,
        "error": None,
    }
    if not dsn:
        out["verdict"] = ("No DSN. /patterns cannot reach Postgres at all and "
                          "reports source='seed' (local demo rows).")
        return out
    t0 = time.time()
    try:
        pool = await pool_factory()
        if pool is None:
            out["error"] = ("pool factory returned None — see the "
                            "'Sherr-I Postgres unavailable' line in the logs "
                            "for the underlying asyncpg error")
            out["verdict"] = "DSN set but the pool did not come up -> 'unavailable'."
            return out
        async with pool.acquire() as conn:
            out["server_version"] = (await conn.fetchval("SHOW server_version"))
            out["database"] = await conn.fetchval("SELECT current_database()")
            out["user"] = await conn.fetchval("SELECT current_user")
            out["search_path"] = await conn.fetchval("SHOW search_path")
        out["connected"] = True
        out["connect_ms"] = round((time.time() - t0) * 1000, 1)
        out["verdict"] = "Pool is up."
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        out["verdict"] = "DSN set but connecting/querying raised -> 'unavailable'."
    return out


# ─── b) the migrations ───────────────────────────────────────────────────────
async def check_migrations(conn) -> dict:
    """Which of the tables the migrations create actually exist here.

    Nothing in the DEPLOYED service applies migrations — run_migrations() is
    called by app/main.py's lifespan and by workers.bootstrap(), neither of which
    the Render start command runs. So what is present is whatever the GitHub
    Actions ingest cron last applied, which is why this is worth checking rather
    than assuming.
    """
    expected = _migration_tables()
    rows = await conn.fetch(
        """
        SELECT table_schema, table_name
          FROM information_schema.tables
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """)
    present = {r["table_name"] for r in rows}
    qualified = {f"{r['table_schema']}.{r['table_name']}" for r in rows}

    missing, found = [], []
    for name in expected:
        hit = name in qualified if "." in name else name in present
        (found if hit else missing).append(name)

    return {
        "migration_files": len(list(_MIGRATIONS.glob("*.sql"))),
        "tables_expected": len(expected),
        "tables_present": found,
        "tables_missing": missing,
        "schemas_seen": sorted({r["table_schema"] for r in rows}),
        "verdict": ("All migration tables exist." if not missing else
                    f"{len(missing)} table(s) the migrations create are missing "
                    f"from this database — the migrations have not been applied "
                    f"here, or were applied to a different database."),
    }


# ─── c) the inputs ───────────────────────────────────────────────────────────
async def check_inputs(conn) -> dict:
    """Row counts for everything the detectors read."""
    counts: dict = {}
    for table, label, why in _INPUT_TABLES:
        entry = {"label": label, "matters_because": why}
        try:
            entry["rows"] = int(await conn.fetchval(f"SELECT COUNT(*) FROM {table}"))
        except Exception as e:
            entry["rows"] = None
            entry["error"] = f"{type(e).__name__}: {e}"
        counts[table] = entry

    # domain_signals is the one that decides whether market_reaction can run at
    # all, so it gets broken out by domain and by recency rather than totalled.
    try:
        counts["domain_signals"]["by_domain"] = {
            r["domain"]: {"rows": int(r["c"]),
                          "earliest": str(r["lo"]) if r["lo"] else None,
                          "latest": str(r["hi"]) if r["hi"] else None,
                          "distinct_days": int(r["days"] or 0)}
            for r in await conn.fetch(
                """
                SELECT domain, COUNT(*) AS c, MIN(ts) AS lo, MAX(ts) AS hi,
                       COUNT(DISTINCT (ts AT TIME ZONE 'UTC')::date) AS days
                  FROM domain_signals GROUP BY domain ORDER BY domain
                """)}
    except Exception as e:
        counts["domain_signals"]["by_domain_error"] = f"{type(e).__name__}: {e}"

    try:
        counts["insights"]["by_type"] = {
            r["type"]: int(r["c"]) for r in await conn.fetch(
                "SELECT type, COUNT(*) AS c FROM insights GROUP BY type ORDER BY 1")}
    except Exception as e:
        counts["insights"]["by_type_error"] = f"{type(e).__name__}: {e}"

    # The price store this app now keeps. Not read by any detector yet — it is
    # reported so "do we have price history" is answerable in the same call.
    try:
        counts["sherrbyte_app.market_ticks"] = {
            "label": "daily price history (this app)",
            "matters_because": ("market_reaction needs >=6 daily observations per "
                                "instrument; market_signals only ever writes today"),
            "rows": int(await conn.fetchval(
                "SELECT COUNT(*) FROM sherrbyte_app.market_ticks")),
            "distinct_symbols": int(await conn.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM sherrbyte_app.market_ticks")),
        }
    except Exception as e:
        counts["sherrbyte_app.market_ticks"] = {"rows": None,
                                                "error": f"{type(e).__name__}: {e}"}

    market = (counts.get("domain_signals", {}).get("by_domain", {})
              .get("market", {}))
    market_days = market.get("distinct_days", 0)
    if counts.get("domain_signals", {}).get("rows") in (0, None):
        verdict = "domain_signals is empty — no detector can do anything."
    elif not market:
        verdict = ("domain_signals has no domain='market' rows. market_reaction "
                   "iterates instruments FROM those rows, so it returns 0 before "
                   "testing anything. The producer is app.workers.market_signals.")
    elif market_days < 6:
        verdict = (f"Only {market_days} distinct day(s) of market signals. "
                   f"market_reaction needs min_history+1 = 6 before an instrument "
                   f"is even tested for significance.")
    else:
        verdict = f"{market_days} days of market signals — enough to test."
    counts["_verdict"] = verdict
    return counts


# ─── d) the detector ─────────────────────────────────────────────────────────
async def run_market_reaction(conn) -> dict:
    """Run the real detector against whatever is in the database.

    WRITES: seeds instrument_keywords and persists any insight it finds. That is
    the point — "does it fire" cannot be answered read-only — but it is why the
    caller has to ask for this explicitly.
    """
    if str(_ENGINE_ROOT) not in sys.path:
        sys.path.insert(0, str(_ENGINE_ROOT))
    out: dict = {"ran": False}
    try:
        from app.spie.discovery import market_reaction
    except Exception as e:
        out["error"] = f"import failed: {type(e).__name__}: {e}"
        out["verdict"] = ("The engine package could not be imported by the "
                          "deployed service. app/spie/discovery needs only "
                          "stdlib, so an ImportError here means a dependency "
                          "crept in that root requirements.txt does not carry.")
        return out
    t0 = time.time()
    try:
        written = await market_reaction.run(conn)
        out.update({
            "ran": True,
            "insights_written": written,
            "elapsed_s": round(time.time() - t0, 2),
            # The stage-by-stage counts behind that number. A 0 is usually
            # correct; the funnel is what says whether it is correct.
            "funnel": dict(getattr(market_reaction, "LAST_RUN", {}) or {}),
        })
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        out["verdict"] = "The detector raised — this is a fault, not a data state."
        return out

    f = out["funnel"]
    if out["insights_written"]:
        out["verdict"] = f"Fires. {out['insights_written']} insight(s) written."
    elif not f.get("instruments"):
        out["verdict"] = ("No instruments. domain_signals has no domain='market' "
                          "rows in the history window — MISSING PRICE HISTORY.")
    elif not f.get("with_enough_history"):
        out["verdict"] = (f"{f.get('instruments')} instrument(s), none with the 6 "
                          f"daily observations needed — MISSING PRICE HISTORY.")
    elif not f.get("significant_moves"):
        out["verdict"] = ("History is there and no move cleared z>=2.5 — "
                          "GENUINELY NOTHING ANOMALOUS. Not a fault.")
    elif not f.get("with_related_entities"):
        out["verdict"] = ("Significant moves found, but no entities relate to the "
                          "instruments — MISSING ENTITY LINKS (cooccurrence and "
                          "instrument_keywords are both empty for them).")
    elif not f.get("with_related_news"):
        out["verdict"] = ("Significant moves with related entities, but no news in "
                          "the +/-48h window — MISSING NEWS OVERLAP.")
    else:
        out["verdict"] = "Reached the final stage without writing — check min_clusters."
    return out


# ─── e) what is scheduled ────────────────────────────────────────────────────
def check_schedule(scheduler=None) -> dict:
    """What this process will actually run, and what only runs on call.

    Static, because that is where the answer lives: the detectors are scheduled
    in app/main.py's lifespan, and app/main.py is not what Render starts.
    """
    jobs = []
    if scheduler is not None:
        try:
            for j in scheduler.get_jobs():
                jobs.append({"id": j.id, "trigger": str(j.trigger),
                             "next_run": (str(j.next_run_time)
                                          if getattr(j, "next_run_time", None) else None)})
        except Exception as e:
            jobs = [{"error": f"{type(e).__name__}: {e}"}]

    detector_ids = [j for j in jobs
                    if "detector" in j.get("id", "") or "market_signal" in j.get("id", "")]
    return {
        "this_process_jobs": jobs,
        "runs_detectors_here": bool(detector_ids),
        "where_detectors_are_scheduled": {
            "sherrbyte/app/main.py lifespan": (
                "market_signals at 01,07,13,19 UTC and detectors at 02:00 UTC — "
                "but render.yaml starts `uvicorn main:app`, the ROOT app, so this "
                "scheduler never runs in production."),
            ".github/workflows/cron_ingest.yml": (
                "runs app.workers.ingest_worker every 15 min and app.workers."
                "als_worker nightly. Neither runs the detectors or market_signals."),
            "app/workers/__init__.py ARQ cron_jobs": (
                "ingest, embed, signal, als — no detectors. Needs an ARQ worker "
                "process running against Redis, which nothing deploys."),
        },
        "verdict": ("Nothing in production runs app.workers.detectors or "
                    "app.workers.market_signals. They are on-call only."
                    if not detector_ids else
                    f"This process schedules: {[j['id'] for j in detector_ids]}"),
    }


# ─── the whole report ────────────────────────────────────────────────────────
async def diagnose(dsn: str, which: str, pool_factory, *,
                   scheduler=None, run_detector: bool = False) -> dict:
    """a) through e) in one call."""
    report: dict = {"checked_at": time.time()}
    report["a_connection"] = await check_connection(dsn, which, pool_factory)
    report["e_schedule"] = check_schedule(scheduler)

    if not report["a_connection"]["connected"]:
        report["b_migrations"] = {"skipped": "no connection"}
        report["c_inputs"] = {"skipped": "no connection"}
        report["d_market_reaction"] = {"skipped": "no connection"}
        report["blocker"] = "a) the engine database is not reachable"
        return report

    pool = await pool_factory()
    async with pool.acquire() as conn:
        report["b_migrations"] = await check_migrations(conn)
        report["c_inputs"] = await check_inputs(conn)
        if run_detector:
            report["d_market_reaction"] = await run_market_reaction(conn)
        else:
            report["d_market_reaction"] = {
                "skipped": "pass &run_detector=1 — it writes (seeds "
                           "instrument_keywords, persists any insight it finds)"}

    report["blocker"] = _blocker(report)
    return report


def _blocker(report: dict) -> str:
    """The first thing in the chain that is actually broken. Ordered, because
    fixing anything downstream of a break changes nothing."""
    a, b, c = report["a_connection"], report["b_migrations"], report["c_inputs"]
    if not a["configured"]:
        return ("a) No DSN — /patterns reports 'seed'. Set DATABASE_URL (or "
                "SHERR_I_DATABASE_URL) on the service.")
    if not a["connected"]:
        return f"a) DSN set but unreachable -> 'unavailable'. {a['error']}"
    if b.get("tables_missing"):
        return (f"b) Missing tables: {', '.join(b['tables_missing'][:6])}"
                f"{' …' if len(b['tables_missing']) > 6 else ''}. Migrations have "
                f"not been applied to THIS database.")
    ds = c.get("domain_signals", {})
    if not ds.get("rows"):
        return "c) domain_signals is empty — nothing has been ingested here."
    market = (ds.get("by_domain") or {}).get("market") or {}
    if not market.get("rows"):
        return ("c/e) No domain='market' signals. app.workers.market_signals has "
                "never run against this database — and nothing in production is "
                "scheduled to run it.")
    if market.get("distinct_days", 0) < 6:
        return (f"c) Only {market['distinct_days']} day(s) of market signals; "
                f"market_reaction needs 6. market_signals writes one day per run, "
                f"so this needs either 6 more daily runs or a history backfill.")
    if not report.get("e_schedule", {}).get("runs_detectors_here"):
        return ("e) Inputs are present but nothing runs the detectors, so "
                "`insights` is never written. /patterns will say 'engine' with "
                "an empty list.")
    return "No structural blocker found — see d) for whether the detector fires."
