"""
Unit tests for the pure core of pipeline/cooccurrence (Intelligence Engine V1, Step 3).

Covers canonical pair generation and daily bucketing — the logic every downstream
count depends on. The async update/backfill paths are integration-tested against
Postgres, not here.
"""

from datetime import date, datetime, timezone

from app.pipeline.cooccurrence import pairs_from_entities, bucket_of


# ─── pairs_from_entities ──────────────────────────────────────────────────────
def test_pairs_are_canonical_and_complete():
    pairs = pairs_from_entities(["b", "a", "c"])
    # 3 entities → 3 unordered pairs, each ordered a<b
    assert set(pairs) == {("a", "b"), ("a", "c"), ("b", "c")}
    assert all(a < b for a, b in pairs)


def test_pairs_dedupe_within_signal():
    assert pairs_from_entities(["a", "a", "b"]) == [("a", "b")]


def test_pairs_order_independent():
    assert pairs_from_entities(["z", "a"]) == [("a", "z")]
    assert pairs_from_entities(["a", "z"]) == [("a", "z")]


def test_pairs_need_two_entities():
    assert pairs_from_entities([]) == []
    assert pairs_from_entities(["solo"]) == []
    assert pairs_from_entities(None) == []


def test_pair_count_formula():
    # n entities → n*(n-1)/2 pairs
    ents = [f"e{i}" for i in range(5)]
    assert len(pairs_from_entities(ents)) == 10


def test_pairs_stringify_non_str_ids():
    # UUID-like objects / ints must still pair by their string form. Pair order in
    # the list is irrelevant (upserts are independent), so compare as a set.
    assert set(pairs_from_entities([2, 1, 3])) == {("1", "2"), ("1", "3"), ("2", "3")}
    assert all(a < b for a, b in pairs_from_entities([2, 1, 3]))


# ─── bucket_of ────────────────────────────────────────────────────────────────
def test_bucket_from_aware_datetime_is_utc_date():
    ts = datetime(2026, 7, 26, 23, 30, tzinfo=timezone.utc)
    assert bucket_of(ts) == date(2026, 7, 26)


def test_bucket_crosses_day_in_utc():
    # 30 min past midnight UTC → that UTC day
    ts = datetime(2026, 7, 27, 0, 30, tzinfo=timezone.utc)
    assert bucket_of(ts) == date(2026, 7, 27)


def test_bucket_from_date_and_isostring():
    assert bucket_of(date(2026, 1, 1)) == date(2026, 1, 1)
    assert bucket_of("2026-03-15T08:00:00Z") == date(2026, 3, 15)
