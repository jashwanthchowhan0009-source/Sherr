"""
articles.published_at — one canonical shape.

The column carried four formats and produced two separate bugs.

SORTING. `ORDER BY published_at DESC` on TEXT is a byte comparison. 'T' is 0x54
and ' ' is 0x20, so within any day EVERY 'T' row sorts above EVERY space row
regardless of the actual time — a six-hour-old article above a four-hour-old
one. That column orders the Home feed, Bytes, trending, search, the story thread
and the recommender's candidate pull.

AGE. A naive stamp has no offset, so the browser reads it as LOCAL time while it
was written as UTC. For a reader in IST that adds 5h30m to every ingest-written
article, which is why every card read "1d ago".
"""

import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import timestamps as T  # noqa: E402

TEST_DSN = os.getenv("MARKET_TICKS_TEST_DSN", "")
needs_pg = pytest.mark.skipif(not TEST_DSN, reason="MARKET_TICKS_TEST_DSN not set")

# The four shapes that actually reached the column, newest first by REAL time.
REAL_ORDER = [
    ("ingest, no feed date (T, naive)", "2026-08-29T18:00:00.912324"),
    ("schema default (space, +00)",     "2026-08-29 16:00:00.123456+00"),
    ("NewsAPI (T, Z)",                  "2026-08-29T12:00:00Z"),
    ("older default (space, +00)",      "2026-08-26 18:00:00.000000+00"),
]


# ─── the formatter ───────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw", [v for _, v in REAL_ORDER])
def test_every_stored_format_normalises(raw):
    assert T.is_canonical(T.to_canonical(raw))


def test_a_naive_value_is_read_as_utc_not_local():
    """The writers meant UTC — feedparser's struct_time is UTC and the schema
    default is now() on a UTC server. Assuming local would re-introduce the
    exact offset error this exists to remove."""
    assert T.to_canonical("2026-08-29T18:00:00") == "2026-08-29T18:00:00+00:00"


def test_an_offset_value_is_converted_not_truncated():
    assert T.to_canonical("2026-08-29 18:00:00+05:30") == "2026-08-29T12:30:00+00:00"


def test_microseconds_are_dropped_so_every_value_is_one_width():
    assert T.to_canonical("2026-08-29T18:00:00.912324") == "2026-08-29T18:00:00+00:00"


def test_a_datetime_object_normalises_too():
    """Once the column is timestamptz the driver hands back a datetime."""
    dt = datetime(2026, 8, 29, 18, 0, tzinfo=timezone.utc)
    assert T.to_canonical(dt) == "2026-08-29T18:00:00+00:00"
    naive = datetime(2026, 8, 29, 18, 0)
    assert T.to_canonical(naive) == "2026-08-29T18:00:00+00:00"


@pytest.mark.parametrize("junk", ["", None, "not a date", "   "])
def test_unusable_input_yields_empty_not_an_exception(junk):
    assert T.to_canonical(junk) == ""
    assert T.parse(junk) is None


def test_a_bare_date_still_parses():
    assert T.to_canonical("2026-08-29") == "2026-08-29T00:00:00+00:00"


def test_normalising_is_idempotent():
    once = T.to_canonical("2026-08-29 16:00:00.123456+00")
    assert T.to_canonical(once) == once


# ─── the bug, and the fix ────────────────────────────────────────────────────
def test_the_raw_formats_sort_wrongly_as_text():
    """Pins the bug itself, so a revert to raw values fails here loudly."""
    ordered = sorted((v for _, v in REAL_ORDER), reverse=True)
    assert ordered[1] == "2026-08-29T12:00:00Z", (
        "as raw text the 6h-old NewsAPI row outranks the 4h-old default row")


def test_rows_written_in_both_formats_sort_correctly_once_canonical():
    """The test the fix exists for: mixed input, one correct order out."""
    canon = [(label, T.to_canonical(raw)) for label, raw in REAL_ORDER]
    got = [label for label, _ in sorted(canon, key=lambda x: x[1], reverse=True)]
    assert got == [label for label, _ in REAL_ORDER]


def test_canonical_text_order_matches_true_chronological_order():
    """Byte order and time order must agree — that is the whole point of a
    fixed-width format with an explicit offset."""
    canon = [T.to_canonical(v) for _, v in REAL_ORDER]
    by_text = sorted(canon, reverse=True)
    by_time = sorted(canon, key=lambda s: T.parse(s), reverse=True)
    assert by_text == by_time


def test_the_age_a_browser_computes_is_now_unambiguous():
    """A naive value has no offset, so new Date() reads it as the reader's local
    time. The canonical form carries +00:00, so every reader gets the same age."""
    canon = T.to_canonical("2026-08-29T18:00:00")
    assert canon.endswith("+00:00")
    assert T.parse(canon) == datetime(2026, 8, 29, 18, 0, tzinfo=timezone.utc)


# ─── the sqlite backfill path ────────────────────────────────────────────────
def _sqlite_with_rows(tmp_path, values):
    import main
    db = tmp_path / "pa.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for stmt in main._MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass
    for i, v in enumerate(values):
        conn.execute("INSERT INTO articles (url, headline, published_at) VALUES (?,?,?)",
                     (f"http://x/{i}", f"H{i}", v))
    conn.commit()
    return conn


def test_the_sqlite_backfill_rewrites_every_non_canonical_row(tmp_path):
    import main
    conn = _sqlite_with_rows(tmp_path, [v for _, v in REAL_ORDER])
    out = main.normalise_published_at(conn)
    got = [r["published_at"] for r in
           conn.execute("SELECT published_at FROM articles ORDER BY id").fetchall()]
    conn.close()
    assert out["rewritten"] == len(REAL_ORDER)
    assert all(T.is_canonical(v) for v in got)


def test_the_backfill_is_idempotent(tmp_path):
    import main
    conn = _sqlite_with_rows(tmp_path, [v for _, v in REAL_ORDER])
    main.normalise_published_at(conn)
    second = main.normalise_published_at(conn)
    conn.close()
    assert second["rewritten"] == 0, "a second run must find nothing to do"


def test_the_feed_order_is_correct_after_the_backfill(tmp_path):
    """End to end through the actual ORDER BY the feed uses."""
    import main
    conn = _sqlite_with_rows(tmp_path, [v for _, v in REAL_ORDER])
    main.normalise_published_at(conn)
    rows = conn.execute(
        "SELECT headline FROM articles ORDER BY published_at DESC, id DESC").fetchall()
    conn.close()
    # H0..H3 were inserted newest-first, so correct order is H0, H1, H2, H3.
    assert [r["headline"] for r in rows] == ["H0", "H1", "H2", "H3"]


def test_an_unparseable_row_does_not_abort_the_backfill(tmp_path):
    import main
    conn = _sqlite_with_rows(tmp_path, ["not a date", "2026-08-29T18:00:00"])
    out = main.normalise_published_at(conn)
    conn.close()
    assert "error" not in out
    assert out["rewritten"] == 1


# ─── the Postgres path, including the type change ────────────────────────────
@needs_pg
def test_postgres_backfills_and_makes_the_column_timestamptz():
    """After the ALTER the DATABASE does the comparison, so no future format can
    drift back in and mis-sort."""
    import asyncio

    import market_ticks as mt

    async def go():
        conn = await mt.connect(TEST_DSN)
        try:
            await conn.execute("DROP TABLE IF EXISTS sherrbyte_app.pa_probe")
            await conn.execute(
                "CREATE TABLE sherrbyte_app.pa_probe (id serial primary key, "
                "headline text, published_at text)")
            for i, (_, v) in enumerate(REAL_ORDER):
                await conn.execute(
                    "INSERT INTO sherrbyte_app.pa_probe (headline, published_at) "
                    "VALUES ($1,$2)", f"H{i}", v)
            # The same normalisation the boot path applies.
            await conn.execute("""
                UPDATE sherrbyte_app.pa_probe
                   SET published_at = to_char(published_at::timestamptz
                         AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') || '+00:00'
                 WHERE published_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            """)
            await conn.execute(
                "ALTER TABLE sherrbyte_app.pa_probe ALTER COLUMN published_at "
                "TYPE timestamptz USING published_at::timestamptz")
            typ = await conn.fetchval(
                "SELECT data_type FROM information_schema.columns WHERE "
                "table_schema='sherrbyte_app' AND table_name='pa_probe' "
                "AND column_name='published_at'")
            order = [r["headline"] for r in await conn.fetch(
                "SELECT headline FROM sherrbyte_app.pa_probe "
                "ORDER BY published_at DESC, id DESC")]
            await conn.execute("DROP TABLE sherrbyte_app.pa_probe")
            return typ, order
        finally:
            await conn.close()

    typ, order = asyncio.run(go())
    assert typ == "timestamp with time zone"
    assert order == ["H0", "H1", "H2", "H3"]
