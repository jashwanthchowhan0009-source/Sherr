"""filings/doctor.py — fetch each source live and REPORT, per source.

This is the whole answer to "build blind, verify in production". The four
sources are blocked by the build sandbox, so their shapes are written against
documentation; this doctor fetches each one live from production and shows, per
source:

  • fetch success and the HTTP status (an honest 401/403 from NSE is a finding,
    not a crash),
  • a SAMPLE RAW record — the actual bytes the source returned,
  • how many records PARSED vs FAILED,
  • and when a shape is wrong, the RAW failing record with a reason — never a
    stack trace.

PURE of the database: it fetches and parses only, so /admin/filing-doctor can run
it under the deployed root app (which has httpx but not the engine's asyncpg
stack). Entity/instrument resolution and the UPSERT live in ingest.py, off this
path, so the doctor is safe to hit at any time with no writes.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from app.spie.filings import sources as S
from app.spie.filings import parse as P

# How far back BSE's date window reaches. The API needs strPrevDate/strToDate.
# FOUR days, not two: a two-day window run on a Monday (or after a holiday)
# spans only the weekend and BSE answers "No Record Found!" — an empty result
# that looks like silence. Four days always reaches back across a weekend to the
# previous trading session, so a Monday run still catches Friday's filings.
BSE_WINDOW_DAYS = 4
FETCH_TIMEOUT_S = 20.0
_MAX_FAILURES_SHOWN = 3


def _bse_url(src: S.Source) -> str:
    today = datetime.now(timezone.utc)
    frm = (today - timedelta(days=BSE_WINDOW_DAYS)).strftime("%Y%m%d")
    to = today.strftime("%Y%m%d")
    return src.url.format(**{"from": frm, "to": to})


def resolved_url(src: S.Source) -> str:
    """The URL actually fetched (BSE carries a date window; the rest are static)."""
    return _bse_url(src) if src.kind == "bse_json" else src.url


async def _fetch(client, src: S.Source) -> dict:
    """One GET. Returns status/body/error without raising — a source being down
    costs only itself, exactly like the RSS collector's per-feed tolerance."""
    url = resolved_url(src)
    t0 = time.monotonic()
    try:
        resp = await client.get(url, headers=src.headers, timeout=FETCH_TIMEOUT_S)
        return {"url": url, "status": resp.status_code, "text": resp.text,
                "error": None, "elapsed_ms": int((time.monotonic() - t0) * 1000)}
    except Exception as ex:                        # network / TLS / timeout
        return {"url": url, "status": None, "text": None,
                "error": f"{type(ex).__name__}: {ex}",
                "elapsed_ms": int((time.monotonic() - t0) * 1000)}


def _sample_raw(pr: P.ParseResult, fetched: dict):
    """The most useful raw record to show: the first parsed record's raw if any
    parsed, else the first failure's raw, else the (truncated) fetched body."""
    if pr.filings:
        return pr.filings[0].raw
    if pr.failures:
        return pr.failures[0]["raw"]
    body = fetched.get("text") or ""
    return body[:2000] + "…" if len(body) > 2000 else body


def _diagnose(src: S.Source, fetched: dict, pr: P.ParseResult) -> str:
    if fetched["error"]:
        return f"fetch failed ({fetched['error']}) — likely blocked by network policy"
    st = fetched["status"]
    if st is not None and st != 200:
        return (f"HTTP {st} — the source refused the request "
                f"(NSE/BSE need browser headers + a prior cookie); see sample_raw")
    if pr.parsed == 0 and pr.failed > 0:
        return "fetched OK but SHAPE MISMATCH — inspect sample_raw against 'shape'"
    if pr.parsed == 0:
        return "fetched OK but returned zero records (quiet window, or empty feed)"
    return f"OK — parsed {pr.parsed} filing(s)"


async def report_source(client, src: S.Source) -> dict:
    fetched = await _fetch(client, src)
    body = fetched["text"]
    pr = P.parse(src.kind, src.name, body) if body is not None else P.ParseResult()
    if body is None:                                # fetch error: nothing to parse
        pr.failures.append(P._fail(fetched["error"] or "", "no response body"))

    sample = None
    if pr.filings:
        f0 = pr.filings[0]
        sample = {**f0.to_row(), "resolved_event_class": f0.event_class}

    return {
        "source": src.name,
        "label": src.label,
        "kind": src.kind,
        "url": fetched["url"],
        "shape": src.shape,
        "fetch_ok": fetched["error"] is None and fetched["status"] == 200,
        "http_status": fetched["status"],
        "error": fetched["error"],
        "elapsed_ms": fetched["elapsed_ms"],
        "parsed": pr.parsed,
        "failed": pr.failed,
        "sample_raw": _sample_raw(pr, fetched),
        "sample_filing": sample,
        "failures": pr.failures[:_MAX_FAILURES_SHOWN],
        "diagnosis": _diagnose(src, fetched, pr),
    }


async def run(only: str | None = None) -> dict:
    """Fetch and report every source (or just `only`, by name). Returns a dict
    ready to serialise straight to /admin/filing-doctor."""
    import httpx

    srcs = [s for s in S.SOURCES if (only is None or s.name == only)]
    reports = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for src in srcs:
            reports.append(await report_source(client, src))

    total_parsed = sum(r["parsed"] for r in reports)
    return {
        "ok": True,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "sources": reports,
        "totals": {
            "sources": len(reports),
            "reachable": sum(1 for r in reports if r["fetch_ok"]),
            "parsed": total_parsed,
        },
    }
