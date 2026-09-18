"""
workers/detectors.py — run the pattern detectors (Intelligence Engine V1, Step 4).

Standalone:
    python -m app.workers.detectors                 # run all detectors
    python -m app.workers.detectors --only emergence
Also invoked nightly by the in-process APScheduler (see app/main.py).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from app.db import db
from app.spie.discovery import REGISTRY
from app.spie.graph import cooccurrence
from app.spie.decision import rules as decision_rules

log = logging.getLogger("sherbyte.worker.detectors")

# All runnable jobs: discovery detectors + the decision-engine chain evaluator.
_CHAIN = "cross_domain_chain"
_REASONED = "reasoned"
# The analog engine (SHAE). Phase 1 builds the event library, Phase 3 measures
# each instrument's forward reaction. Nothing else runs these, so without this
# step hist_events / analog_reactions — and the /api/sherr-i/analogs surface —
# stay permanently empty.
_ANALOG = "analog"

# Last analog build/compute funnels, for --diagnostics. Same LAST_RUN pattern the
# discovery detectors use, so _funnels() can surface them without re-running.
_LAST_ANALOG: dict = {}


async def run(only: str | None = None, *, diagnostics: bool = False) -> dict:
    """Run detectors and return {name: insights_written}.

    With diagnostics=True the result also carries `_funnels`: for the two
    news↔market detectors, the stage-by-stage counts behind their number. A count
    of 0 is often correct (no unusual move, or no news overlap in the window) and
    the funnel is what tells those apart from a wiring fault.
    """
    results: dict[str, int] = {}
    async with db.acquire() as conn:
        # Refresh NPMI first so detector ranking uses current association strengths.
        try:
            await cooccurrence.compute_npmi(conn)
        except Exception as e:
            log.warning("npmi refresh failed: %s", e)
        for name, fn in REGISTRY.items():
            if only and name != only:
                continue
            try:
                results[name] = await fn(conn)
            except Exception as e:
                log.error("detector %s failed: %s", name, e, exc_info=True)
                results[name] = -1
        # Decision Engine: cross-domain chain rules.
        if not only or only == _CHAIN:
            try:
                results[_CHAIN] = await decision_rules.run(conn)
            except Exception as e:
                log.error("decision rules failed: %s", e, exc_info=True)
                results[_CHAIN] = -1
        # Reasoning Engine — runs LAST of the insight detectors so it can reason
        # over everything above.
        if not only or only == _REASONED:
            try:
                from app.spie.reasoning import engine as reasoning_engine
                results[_REASONED] = await reasoning_engine.run(conn)
            except Exception as e:
                log.error("reasoning engine failed: %s", e, exc_info=True)
                results[_REASONED] = -1

        # Analog engine: build the event library from the corpus (Phase 1), then
        # measure the stored reactions (Phase 3). Both are idempotent — an upsert
        # and an ON CONFLICT — so a nightly re-run only refreshes. They read the
        # SAME conn and depend on nothing but stdlib + the pool. Run after the
        # detectors so cooccurrence NPMI is current for the matcher.
        if not only or only == _ANALOG:
            try:
                from app.spie.analog import event_library, reaction
                lib = await event_library.build(conn)
                rx = await reaction.compute(conn)
                results["analog_library"] = lib.get("written", 0)
                results["analog_reactions"] = rx["funnel"].get("groups_written", 0)
                _LAST_ANALOG.clear()
                _LAST_ANALOG.update({"library": lib, "reactions": rx["funnel"]})
            except Exception as e:
                log.error("analog engine failed: %s", e, exc_info=True)
                results[_ANALOG] = -1

    if diagnostics:
        results["_funnels"] = _funnels()
    return results


def _funnels() -> dict:
    """Stage-by-stage counts from the detectors that can legitimately return 0."""
    out: dict = {}
    try:
        from app.spie.discovery.market_reaction import LAST_RUN as mr
        if mr:
            out["market_reaction"] = mr
    except Exception:
        pass
    try:
        from app.spie.reasoning.engine import LAST_RUN as re_
        if re_:
            out["reasoned"] = re_
    except Exception:
        pass
    try:
        from app.spie.discovery.emergence import LAST_RUN as em
        if em:
            out["emergence"] = em
    except Exception:
        pass
    if _LAST_ANALOG:
        out["analog"] = _LAST_ANALOG
    return out


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Run Intelligence Engine detectors.")
    parser.add_argument("--only", choices=sorted(list(REGISTRY) + [_CHAIN, _REASONED, _ANALOG]), default=None,
                        help="run a single detector instead of all")
    parser.add_argument("--diagnostics", action="store_true",
                        help="also print the news<->market funnels behind each count")
    args = parser.parse_args()

    from app.workers import bootstrap, teardown
    await bootstrap()
    try:
        result = await run(only=args.only, diagnostics=args.diagnostics)
        log.info("detectors complete: %s", result)
        print(json.dumps(result, indent=2, default=str))
    finally:
        await teardown()


if __name__ == "__main__":
    asyncio.run(_main())
