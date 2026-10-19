"""
you_contents.py — the second attempt at a page our own reader could not read.

WHY THIS EXISTS
───────────────
`article_reader` fetches the publisher's page and extracts the prose, and when
it cannot — a paywall, a bot wall, a page whose body only exists after
JavaScript runs — it returns "" by design. The caller then falls back to the
40-word RSS blurb, and CLAUDE.md already records what that produces: "a single
200-character blurb cannot be turned into an original 60-80 word body". The row
keeps the placeholder, forever, because every later tick fails the same way at
the same URL.

You.com's Contents API fetches and extracts the same page from their side. So
this module is exactly one thing: a SECOND attempt at the URLs our own reader
returned nothing for. It is never the first attempt — our own reader is free
and this one is not.

THE MONEY IS THE DESIGN CONSTRAINT
──────────────────────────────────
The account holds a fixed $100 of credit and Sherr has no paid-API budget
beyond it, so this is a finite free tier, not a utility. Contents is billed per
PAGE FETCHED at $1.00 per 1,000 pages, which is $0.001 a page. The drain reads
up to `BODY_DRAIN_RPM` articles a minute — ~17,000 a day — so "retry every
failure" would be tens of dollars a week and the credit would be gone before
anyone looked at a card.

`YOU_PAGE_BUDGET_PER_DAY` (200) is therefore a HARD CEILING, not a rate hint:
at the published price that is $0.20 a day and the credit outlives a year of
unattended running. The budget is spent newest-first by whatever the drain
happens to be working on; nothing queues, because a page we skipped today is
offered again tomorrow by the same selector that offered it today.

**The ledger is per-process and that is a known bound, not an oversight.** It
lives in memory for the same reason `/admin/memory` touches no database: the
moment the budget matters is the moment a DB write is one more thing that can
fail, and a failed write that silently resets a spend counter is worse than a
counter that is honest about its scope. With N instances the real ceiling is
N × the budget. Sherr runs one. Move this into `cache.py`'s Redis layer BEFORE
scaling out, not after — the same note `_token_version`'s TTL cache carries.

THE HOST IS AN ENVIRONMENT VARIABLE ON PURPOSE
──────────────────────────────────────────────
You.com's own documentation gives the Contents endpoint as
`https://ydc-index.io/v1/contents`, while their API catalogue lists a base of
`https://api.you.com/v1`. Both hosts are unreachable from CI and from the
container this was written in, so NEITHER was verified against a live response.
`YOU_API_BASE` makes picking the right one a dashboard edit rather than a
deploy, and `_pages()` accepts a bare array or a `results`/`data`/`contents`
envelope because a wrong guess about the wrapper must degrade to "no text",
never to a crash in the middle of the rewrite pass.

KEYS
────
The deployment's variable is `YOU_KEYS`, plural, so it is read as a LIST —
comma, whitespace or newline separated — and rotated through `key_pool.KeyPool`
on exactly the statuses that mean "this key is the problem" (401/403/429), the
same rule the model providers already use. `YOU_API_KEY` and `YDC_API_KEY`
(the name You.com's own tooling uses) are accepted as singles so a key set
under either name is not dead weight.

Never raises, never logs a key, and returns {} whenever anything at all is
wrong. `YOU_CONTENTS_ENABLED=0` switches it off.
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
import re
import threading

import httpx

from key_pool import ROTATE_STATUSES, KeyPool

log = logging.getLogger("sherbyte.you")

DEFAULT_BASE = "https://ydc-index.io"

# You.com caps a Contents request at 10 URLs; sending more is a 4xx, not a
# truncation, so the batch size is theirs and not a tuning choice.
MAX_URLS_PER_CALL = 10

TIMEOUT = 25.0          # their side crawls the page, so it is slower than a GET
MAX_CHARS = 7000        # article_reader.MAX_CHARS — one prompt budget, one cap


def _env_flag(name: str, default: str = "1") -> bool:
    return (os.getenv(name, default) or default).strip().lower() \
        not in ("0", "false", "no")


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


def api_base() -> str:
    return (os.getenv("YOU_API_BASE") or DEFAULT_BASE).strip().rstrip("/")


def enabled() -> bool:
    """Read at CALL time, never at import.

    CLAUDE.md's test note: module-level configuration is what forces
    `importlib.reload`, and reloading a module other tests hold references to
    is a trap this repo has already fallen into twice."""
    return _env_flag("YOU_CONTENTS_ENABLED") and bool(collect_keys())


def page_budget() -> int:
    return max(0, _env_int("YOU_PAGE_BUDGET_PER_DAY", 200))


_KEY_SPLIT = re.compile(r"[,\s]+")


def collect_keys(env=None) -> list:
    """Every You.com key in the environment, de-duplicated, in a stable order.

    `YOU_KEYS` is plural in the dashboard and is split on commas and whitespace.
    De-duplication matters for the same reason it does in `key_pool.collect`:
    the same secret exported under two names is one key, and counting it twice
    makes the pool look deeper than it is at the moment that matters."""
    env = os.environ if env is None else env
    out, seen = [], set()
    for name in ("YOU_KEYS", "YOU_API_KEY", "YDC_API_KEY"):
        for part in _KEY_SPLIT.split((env.get(name) or "").strip()):
            part = part.strip()
            if part and part not in seen:
                seen.add(part)
                out.append(part)
    return out


class _Budget:
    """Pages bought today, against the daily ceiling.

    The day boundary is UTC and is read from the clock on every call rather
    than reset by a timer — a timer is one more thing that does not fire on an
    instance that was asleep, which on Render's free tier is most of them."""

    def __init__(self):
        self._lock = threading.Lock()
        self._day = ""
        self._spent = 0
        self.denied = 0

    @staticmethod
    def _today() -> str:
        return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")

    def _roll(self) -> None:
        day = self._today()
        if day != self._day:
            self._day, self._spent = day, 0

    def take(self, want: int) -> int:
        """Reserve up to `want` pages; returns how many are actually affordable."""
        with self._lock:
            self._roll()
            allowed = max(0, min(want, page_budget() - self._spent))
            self._spent += allowed
            if allowed < want:
                self.denied += want - allowed
            return allowed

    def refund(self, n: int) -> None:
        """Give back pages a failed CALL never fetched.

        Billing is per page fetched, so a request that never completed bought
        nothing. Not refunding would let a run of timeouts spend a day's
        ceiling without reading one article."""
        if n <= 0:
            return
        with self._lock:
            self._roll()
            self._spent = max(0, self._spent - n)

    def snapshot(self) -> dict:
        with self._lock:
            self._roll()
            budget = page_budget()
            return {"day": self._day, "spent": self._spent, "budget": budget,
                    "remaining": max(0, budget - self._spent),
                    "denied_today": self.denied}


_budget = _Budget()

# Cumulative counters, for /admin/you. They survive across ticks so "is the
# fallback earning its credit" is a number rather than a log-grep.
_stats = {"calls": 0, "pages_requested": 0, "pages_returned": 0,
          "articles_recovered": 0, "http_errors": 0, "rotations": 0,
          "transport_errors": 0, "short_or_empty": 0}
_last_error = ""


def stats() -> dict:
    out = dict(_stats)
    out["budget"] = _budget.snapshot()
    out["enabled"] = enabled()
    out["keys"] = len(collect_keys())        # the COUNT, never a key
    out["base"] = api_base()
    out["last_error"] = _last_error
    return out


# ── markdown → plain prose ───────────────────────────────────────────────────
# What comes back is markdown of the whole page, so it carries the furniture
# our own extractor drops by tag: nav lists, image embeds, link syntax. The
# text is only ever INPUT to the rewrite (the same guarantee article_reader
# makes — never stored, never served), but furniture inside it costs prompt
# budget and teaches the model the wrong shape, so it is stripped here.

_MD_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_MD_MARKS = re.compile(r"[*_`>#]+")
_MD_RULE = re.compile(r"^\s*([-*_]\s*){3,}$")


def markdown_to_text(md: str) -> str:
    if not md:
        return ""
    md = _MD_IMAGE.sub(" ", md)
    md = _MD_LINK.sub(r"\1", md)
    lines = []
    for raw in md.splitlines():
        line = _MD_MARKS.sub("", raw).strip()
        if not line or _MD_RULE.match(raw):
            continue
        lines.append(re.sub(r"\s+", " ", line))
    return "\n".join(lines).strip()[:MAX_CHARS]


def _pages(payload) -> list:
    """The per-URL objects, whatever envelope they arrived in.

    The documented response is a bare array. A catalogue entry shows an
    envelope. Neither host answers from here, so both are accepted and anything
    else is simply no pages — a guess about the wrapper must never become an
    exception inside the rewrite pass."""
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        for key in ("results", "data", "contents", "pages"):
            v = payload.get(key)
            if isinstance(v, list):
                return [p for p in v if isinstance(p, dict)]
    return []


def _text_of(page: dict) -> str:
    for key in ("markdown", "text", "content"):
        v = page.get(key)
        if isinstance(v, str) and v.strip():
            return markdown_to_text(v)
    return ""


def _chunks(items: list, n: int):
    for i in range(0, len(items), n):
        yield items[i:i + n]


async def fetch_contents(urls: list, client: httpx.AsyncClient | None = None) -> dict:
    """{url: article text} for the URLs You.com could read. Never raises.

    Only URLs that produced prose appear, so the caller's "did this work"
    check is the same one `article_reader.fetch_many` already uses: a missing
    key means no text, not an empty one."""
    global _last_error
    urls = [u for u in dict.fromkeys(urls or [])
            if isinstance(u, str) and u.startswith(("http://", "https://"))]
    if not urls or not enabled():
        return {}

    affordable = _budget.take(len(urls))
    if affordable <= 0:
        log.info("[YOU] daily page budget spent; %d url(s) skipped", len(urls))
        return {}
    urls = urls[:affordable]

    pool = KeyPool("you", collect_keys())
    own = client is None
    client = client or httpx.AsyncClient()
    out: dict = {}
    try:
        for batch in _chunks(urls, MAX_URLS_PER_CALL):
            pool.reset()
            got = await _one_call(client, pool, batch)
            out.update(got)
            # Pages the call did not come back with were never fetched, so they
            # were never billed.
            _budget.refund(len(batch) - len(got))
    except Exception as e:                                        # noqa: BLE001
        _last_error = type(e).__name__
        log.warning("[YOU] contents failed: %s", type(e).__name__)
    finally:
        if own:
            await client.aclose()
    if out:
        _stats["articles_recovered"] += len(out)
        log.info("[YOU] recovered %d of %d page(s) our reader could not read",
                 len(out), len(urls))
    return out


async def _one_call(client: httpx.AsyncClient, pool: KeyPool, batch: list) -> dict:
    """One Contents request for up to MAX_URLS_PER_CALL urls, with key rotation."""
    global _last_error
    url = f"{api_base()}/v1/contents"
    body = {"urls": batch, "formats": ["markdown"]}
    while True:
        key = pool.current()
        if not key:
            return {}
        _stats["calls"] += 1
        _stats["pages_requested"] += len(batch)
        try:
            r = await client.post(url, json=body, timeout=TIMEOUT,
                                  headers={"X-API-Key": key,
                                           "Content-Type": "application/json"})
        except Exception as e:                                    # noqa: BLE001
            _stats["transport_errors"] += 1
            _last_error = type(e).__name__
            log.info("[YOU] transport error: %s", type(e).__name__)
            return {}

        if r.status_code in ROTATE_STATUSES:
            _stats["http_errors"] += 1
            _last_error = f"HTTP {r.status_code}"
            if pool.rotate(f"HTTP {r.status_code}"):
                _stats["rotations"] += 1
                continue
            log.warning("[YOU] every key answered %d", r.status_code)
            return {}
        if r.status_code != 200:
            _stats["http_errors"] += 1
            _last_error = f"HTTP {r.status_code}"
            log.info("[YOU] HTTP %d", r.status_code)
            return {}

        try:
            payload = r.json()
        except Exception:                                         # noqa: BLE001
            _stats["http_errors"] += 1
            _last_error = "unparseable body"
            return {}

        out = {}
        for page in _pages(payload):
            page_url = page.get("url") or page.get("source_url") or ""
            text = _text_of(page)
            if not page_url or not text:
                _stats["short_or_empty"] += 1
                continue
            _stats["pages_returned"] += 1
            out[page_url] = text
        return out


def diagnosis() -> str:
    """One line naming why the fallback is producing nothing, when it is.

    Four causes with four different fixes, which is the same argument
    `cluster_articles` records: a counter per cause is what separates them."""
    if not _env_flag("YOU_CONTENTS_ENABLED"):
        return "DISABLED: YOU_CONTENTS_ENABLED=0."
    if not collect_keys():
        return "NO KEY: set YOU_KEYS (comma-separated for several)."
    b = _budget.snapshot()
    if b["remaining"] <= 0:
        return (f"BUDGET SPENT: {b['spent']}/{b['budget']} pages used today, "
                f"{b['denied_today']} skipped. Raise YOU_PAGE_BUDGET_PER_DAY "
                f"to spend more of the credit.")
    if not _stats["calls"]:
        return ("IDLE: no call yet — our own reader has not failed on a page "
                "since this process started, which is the good case.")
    if _stats["http_errors"] and not _stats["pages_returned"]:
        return (f"EVERY CALL FAILED ({_last_error}). Check the key and "
                f"YOU_API_BASE — the documented host is {DEFAULT_BASE}, the "
                f"catalogue lists https://api.you.com. Neither was verifiable "
                f"from CI.")
    if _stats["pages_returned"] and not _stats["articles_recovered"]:
        return ("PAGES RETURNED BUT TOO SHORT: You.com read the pages and the "
                "text did not reach article_reader's word floor.")
    return (f"OK: {_stats['articles_recovered']} article(s) recovered from "
            f"{_stats['calls']} call(s).")
