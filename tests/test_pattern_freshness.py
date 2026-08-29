"""
Sherr-I freshness window.

An insight is a claim about what is happening NOW. "FIFA and X are newly
connected" stops being that the moment it is three days old, and the 2026-07-30
seed rows were still surfacing months later as though they were today's
findings.

FILTERED IN SQL, NOT IN THE UI. A client-side filter still ships the stale rows
over the wire, still counts them in `total`, and is one forgotten caller away
from putting them back on screen — and /patterns has more than one caller.
"""

import asyncio
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

TEST_DSN = os.getenv("SHERR_ENGINE_TEST_DSN", "")
needs_pg = pytest.mark.skipif(not TEST_DSN, reason="SHERR_ENGINE_TEST_DSN not set")

# (type, why, age in hours)
ROWS = [
    ("reasoned",    "fresh reasoned",     2),
    ("observation", "fresh observation",  30),
    ("observation", "just inside",        71),
    ("observation", "just outside",       73),
    ("reasoned",    "stale reasoned",     96),
    ("emergence",   "THE STALE FIFA ROW", 24 * 60),
]


async def _seed():
    import asyncpg
    c = await asyncpg.connect(TEST_DSN, statement_cache_size=0)
    await c.execute("TRUNCATE insights")
    for t, why, age in ROWS:
        await c.execute(
            "INSERT INTO insights (type,entity_ids,domains,score,explain_json,"
            "signature,created_at) VALUES ($1,'{}'::uuid[],$2,1,$3::jsonb,$4,"
            "now() - ($5||' hours')::interval)",
            t, ["news"], json.dumps({"why": why}), why, str(age))
    await c.close()


def _main():
    """main with its engine pool pointed at the test database.

    setdefault on the environment is not enough: another test module may have
    imported main already, and SHERR_I_DATABASE_URL is read once at import. The
    module attribute is set directly and the cached pool cleared, so the pool is
    rebuilt against this DSN rather than whatever the first importer had.
    """
    os.environ.setdefault("ADMIN_TOKEN", "t")
    os.environ.setdefault("JWT_SECRET", "x" * 40)
    import main
    main.SHERR_I_DATABASE_URL = TEST_DSN
    # Cleared unconditionally, not only on a DSN change: every test runs its own
    # asyncio.run(), and an asyncpg pool belongs to the loop that created it.
    # Reusing one across loops fails with "another operation is in progress".
    main._spie_pool = None
    return main


def _whys(d):
    return sorted((p.get("explain_json") or {}).get("why", "") for p in d["patterns"])


@needs_pg
def test_nothing_older_than_the_window_is_served():
    async def go():
        await _seed()
        return await _main()._spie_patterns("", 50, 0, None)

    d = asyncio.run(go())
    assert _whys(d) == ["fresh observation", "fresh reasoned", "just inside"]


@needs_pg
def test_the_stale_fifa_row_never_reaches_the_page():
    """The specific complaint: long-dead topics still appearing."""
    async def go():
        await _seed()
        return await _main()._spie_patterns("", 50, 0, None)

    assert "THE STALE FIFA ROW" not in _whys(asyncio.run(go()))


@needs_pg
def test_the_boundary_is_the_window_not_a_day_either_side():
    async def go():
        await _seed()
        return _whys(await _main()._spie_patterns("", 50, 0, None))

    got = asyncio.run(go())
    assert "just inside" in got        # 71h
    assert "just outside" not in got   # 73h


@needs_pg
def test_the_total_matches_the_rows_actually_served():
    """`total` was a bare COUNT(*) over the whole table, so a filtered request
    reported a number with nothing to do with the rows beside it — the page
    saying "319 patterns detected" above four cards is exactly that."""
    async def go():
        await _seed()
        m = _main()
        return [await m._spie_patterns(t, 50, 0, None)
                for t in ("", "reasoned", "observation")]

    for d in asyncio.run(go()):
        assert d["total"] == len(d["patterns"]), d


@needs_pg
def test_the_type_filter_and_the_window_compose():
    async def go():
        await _seed()
        return await _main()._spie_patterns("reasoned", 50, 0, None)

    d = asyncio.run(go())
    assert _whys(d) == ["fresh reasoned"], "stale reasoned must not survive"


@needs_pg
def test_zero_disables_the_window_for_an_admin_reading_the_raw_table():
    async def go():
        await _seed()
        return await _main()._spie_patterns("", 50, 0, 0)

    d = asyncio.run(go())
    assert len(d["patterns"]) == len(ROWS)
    assert "THE STALE FIFA ROW" in _whys(d)


def test_the_window_defaults_to_72_hours():
    assert _main().PATTERN_MAX_AGE_HOURS == 72


def test_the_by_type_endpoint_carries_the_same_window():
    """A second door to the same table. A filter only one endpoint enforces is
    a filter one caller can walk around."""
    import inspect
    src = inspect.getsource(_main().patterns_by_type)
    assert "max_age_hours" in src
    assert "return await patterns(" in src, "must delegate, not re-query"


def test_the_page_does_not_filter_on_freshness_itself():
    """The UI states the window; the server enforces it. A second filter in the
    page would only hide a server-side regression."""
    html = open(os.path.join(ROOT, "index.html")).read()
    start = html.index("async function loadPatterns()")
    body = html[start:start + 3000]
    assert "SHERR_I_WINDOW_H" in body, "the page should SAY the window"
    for banned in ("created_at", "Date.parse", "max_age_hours"):
        assert banned not in body, f"page filters on {banned}; the server must"


def test_the_page_has_no_tab_switcher_left():
    """One page, two stacked sections."""
    html = open(os.path.join(ROOT, "index.html")).read()
    assert "spiePatternType" not in html
    assert "renderSpieChips" not in html
    assert 'id="spie-chips"' not in html
