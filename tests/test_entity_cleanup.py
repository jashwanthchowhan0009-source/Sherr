"""
Entity extraction cleanup and the backfill that applies it to stored entities.

/patterns was serving "India's" beside "India", "Fifa's" beside "FIFA", and
"Moreover", "Test" and "Kevin M" as named entities — with two of the top ten
patterns being an entity paired with what is really itself, the same name split
across two ids. The resolver decides this at EXTRACTION time, so tightening it
fixes new mentions only; everything already stored needs the backfill.

The Postgres half is skipped unless MARKET_TICKS_TEST_DSN is set — merging an
entity graph is all foreign keys, array columns and unique constraints, none of
which a stub can stand in for.
"""

import asyncio
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "sherrbyte"))

from app.spie.knowledge import entity_resolver as er   # noqa: E402
from app.workers import entity_cleanup as ec           # noqa: E402

# A DIFFERENT database from MARKET_TICKS_TEST_DSN: these need the engine schema
# (entities, domain_signals, cooccurrence, insights — migrations 010 through 019),
# where the market_ticks tests only need migration 020. Pointing them at the same
# DSN makes whichever schema is missing look like a broken test.
TEST_DSN = os.getenv("SHERR_ENGINE_TEST_DSN", "")
needs_pg = pytest.mark.skipif(not TEST_DSN, reason="SHERR_ENGINE_TEST_DSN not set")


# ─── possessives ─────────────────────────────────────────────────────────────
@pytest.mark.parametrize("possessive,plain", [
    ("India's", "India"), ("Fifa's", "FIFA"), ("Modi's", "Modi"),
    ("Apple’s", "Apple"), ("Students'", "Students"), ("Nations’", "Nations"),
])
def test_a_possessive_normalizes_to_the_same_key_as_the_plain_name(possessive, plain):
    """Punctuation stripping turns the apostrophe into a space, so "India's"
    became "india s" — its own node, splitting that name's co-occurrence counts
    across two ids and letting a detector pair an entity with itself."""
    assert er.normalize_name(possessive) == er.normalize_name(plain)


def test_a_name_that_merely_ends_in_s_is_untouched():
    """The rule must key on the apostrophe, not on a trailing s."""
    assert er.normalize_name("Reuters") == "reuters"
    assert er.normalize_name("Philippines") == "philippines"


def test_case_variants_already_collapsed_and_still_do():
    """Case was never the problem — normalize_name has always lowercased. Pinned
    so a future change to the possessive rule cannot regress it."""
    assert er.normalize_name("FIFA") == er.normalize_name("fifa") == "fifa"


# ─── the junk filter ─────────────────────────────────────────────────────────
@pytest.mark.parametrize("junk", [
    "Moreover", "However", "Meanwhile", "Furthermore", "Therefore", "According",
    "Test", "Results", "Update", "Details", "Options", "Kevin M", "Sarah J",
])
def test_headline_furniture_and_byline_fragments_are_rejected(junk):
    assert er.is_valid_mention(junk) is False


@pytest.mark.parametrize("real", [
    "India", "FIFA", "Reserve Bank of India", "Narendra Modi", "Manchester City",
    "World Bank", "Times of India", "Spider-Man", "John F Kennedy",
    "Boxing Day Test", "Nifty 50",
])
def test_real_entities_survive_the_tightened_filter(real):
    """The filter is only worth having if it keeps everything that matters —
    including names built entirely from ordinary words."""
    assert er.is_valid_mention(real) is True


def test_a_middle_initial_is_kept_but_a_trailing_one_is_not():
    """"Kevin M" is a byline fragment; "John F Kennedy" is a person."""
    assert er.is_valid_mention("John F Kennedy") is True
    assert er.is_valid_mention("Kevin M") is False


# ─── the merge plan ──────────────────────────────────────────────────────────
def _row(eid, name, etype="MISC", norm=None, count=0):
    return {"id": eid, "canonical_name": name, "type": etype,
            "norm_key": norm if norm is not None else name.lower(),
            "mention_count": count}


def test_possessive_and_plain_forms_merge_onto_the_plain_one():
    p = ec.plan([_row("a", "India", "GPE", "india", 120),
                 _row("b", "India's", "GPE", "india s", 45)])
    assert p["merge"] == {"b": "a"}


def test_the_survivor_is_the_most_referenced_form():
    """Whichever spelling the corpus actually uses is the one a reader should
    see, so counts decide before name length does."""
    p = ec.plan([_row("a", "Fifa's", "ORG", "fifa s", 900),
                 _row("b", "FIFA", "ORG", "fifa", 5)])
    assert p["merge"] == {"b": "a"}


def test_a_seeded_entity_wins_regardless_of_counts():
    """The curated identity is the point of seeding it."""
    p = ec.plan([_row("a", "Modi", "PERSON", "modi", 900),
                 _row("b", "Narendra Modi", "PERSON", "narendra modi", 1)])
    assert p["merge"] == {"a": "b"}


def test_a_seeded_short_form_merges_into_its_canonical_entity():
    """Grouping on the bare normalized form would merge the possessives and
    still leave every seeded alias sitting beside its own canonical entity."""
    p = ec.plan([_row("a", "Narendra Modi", "PERSON", "narendra modi", 90),
                 _row("b", "Modi's", "PERSON", "modi s", 30)])
    assert p["merge"] == {"b": "a"}


def test_misc_folds_into_the_one_real_type_it_shares_a_name_with():
    """MISC is the resolver's fallback when NER offered nothing usable, not a
    claim about identity — and the same org arriving ORG one day and MISC the
    next is a large share of the duplication."""
    p = ec.plan([_row("a", "FIFA", "ORG", "fifa", 60),
                 _row("b", "Fifa", "MISC", "fifa", 8)])
    assert p["merge"] == {"b": "a"}


def test_two_real_types_sharing_a_name_are_never_merged():
    """entities is keyed (norm_key, type) on purpose: Jordan the country and
    Jordan the person are different things."""
    p = ec.plan([_row("a", "Jordan", "GPE", "jordan", 14),
                 _row("b", "Jordan", "PERSON", "jordan", 11)])
    assert p["merge"] == {}


def test_junk_entities_are_listed_for_deletion_not_merged():
    p = ec.plan([_row("a", "Moreover", "MISC", "moreover", 15),
                 _row("b", "India", "GPE", "india", 3)])
    assert [j["name"] for j in p["junk"]] == ["Moreover"]
    assert p["merge"] == {}


def test_a_clean_graph_produces_an_empty_plan():
    """Idempotency at the planning layer: nothing to do is the steady state."""
    p = ec.plan([_row("a", "India", "GPE", "india", 10),
                 _row("b", "FIFA", "ORG", "fifa", 8)])
    assert p == {"merge": {}, "junk": [], "renorm": {}, "examples": []}


def test_a_stale_norm_key_is_scheduled_for_correction():
    """A surviving row still carrying "india s" would make the next mention of
    "India" miss it and create a third entity."""
    p = ec.plan([_row("a", "India", "GPE", "india s", 10)])
    assert p["renorm"] == {"a": "india"}


# ─── the graph repair, against real Postgres ─────────────────────────────────
async def _connect():
    import asyncpg
    return await asyncpg.connect(TEST_DSN, statement_cache_size=0)


DIRTY = [("India", "GPE", 120), ("India's", "GPE", 45),
         ("FIFA", "ORG", 60), ("Fifa's", "ORG", 22), ("Fifa", "MISC", 8),
         ("Moreover", "MISC", 15), ("Kevin M", "PERSON", 5),
         ("Manchester City", "ORG", 25), ("World Bank", "ORG", 18)]


async def _seed(conn):
    for t in ("insights", "domain_signals", "cooccurrence", "cooccurrence_events",
              "watchlist", "entity_aliases", "entities"):
        await conn.execute(f"TRUNCATE {t} CASCADE")
    ids = {}
    for name, typ, cnt in DIRTY:
        old_norm = name.lower().replace("'", " ").strip()
        ids[name] = await conn.fetchval(
            "INSERT INTO entities (canonical_name,type,norm_key,mention_count) "
            "VALUES ($1,$2,$3,$4) RETURNING id", name, typ, old_norm, cnt)
    import json
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    for eids, cid in (([ids["India"], ids["India's"]], 1),
                      ([ids["India"], ids["FIFA"]], 2),
                      ([ids["Moreover"], ids["India"]], 3),
                      ([ids["Manchester City"], ids["World Bank"]], 4)):
        for d in range(4):
            await conn.execute(
                "INSERT INTO domain_signals (entity_ids,domain,ts,magnitude,"
                "direction,source_id,cluster_id) VALUES ($1::uuid[],'news',$2,1,0,'t',$3)",
                eids, now - timedelta(days=d), cid * 100 + d)
    for eids, sig in (([ids["India"], ids["India's"]], "self-1"),
                      ([ids["FIFA"], ids["Fifa's"]], "self-2"),
                      ([ids["Manchester City"], ids["World Bank"]], "real-1")):
        await conn.execute(
            "INSERT INTO insights (type,entity_ids,domains,score,explain_json,signature)"
            " VALUES ('emergence',$1::uuid[],$2,1,$3::jsonb,$4)",
            eids, ["news"], json.dumps({"why": "t"}), sig)
    return ids


@needs_pg
def test_the_graph_has_no_duplicates_or_dangling_references_afterwards():
    """The whole repair in one assertion set: nothing points at a deleted
    entity, no name exists twice under one type, and the self-pair insights the
    reader saw are gone."""
    async def go():
        conn = await _connect()
        try:
            await _seed(conn)
            await ec.run(conn, days=90)
            return {
                "dupes": await conn.fetchval(
                    "SELECT COUNT(*) FROM (SELECT norm_key,type FROM entities "
                    "GROUP BY 1,2 HAVING COUNT(*)>1) t"),
                "dangling_signals": await conn.fetchval(
                    "SELECT COUNT(*) FROM domain_signals ds WHERE EXISTS (SELECT 1 "
                    "FROM unnest(ds.entity_ids) x WHERE NOT EXISTS "
                    "(SELECT 1 FROM entities e WHERE e.id=x))"),
                "dangling_insights": await conn.fetchval(
                    "SELECT COUNT(*) FROM insights i WHERE EXISTS (SELECT 1 "
                    "FROM unnest(i.entity_ids) x WHERE NOT EXISTS "
                    "(SELECT 1 FROM entities e WHERE e.id=x))"),
                "collapsed_insights": await conn.fetchval(
                    "SELECT COUNT(*) FROM insights "
                    "WHERE COALESCE(array_length(entity_ids,1),0) < 2"),
                "junk_left": await conn.fetchval(
                    "SELECT COUNT(*) FROM entities WHERE canonical_name "
                    "IN ('Moreover','Kevin M','India''s','Fifa''s')"),
            }
        finally:
            await conn.close()

    out = asyncio.run(go())
    assert out == {"dupes": 0, "dangling_signals": 0, "dangling_insights": 0,
                   "collapsed_insights": 0, "junk_left": 0}


@needs_pg
def test_a_signal_mentioning_both_forms_ends_up_with_one_entity_not_two():
    """The merge has to de-duplicate WITHIN each array, or a story that named
    "India" and "India's" leaves the survivor listed twice — which is exactly
    how a pattern comes to pair an entity with itself."""
    async def go():
        conn = await _connect()
        try:
            ids = await _seed(conn)
            await ec.run(conn, days=90)
            return await conn.fetchval(
                "SELECT COUNT(*) FROM domain_signals ds WHERE "
                "array_length(ds.entity_ids,1) <> "
                "(SELECT COUNT(DISTINCT x) FROM unnest(ds.entity_ids) x)")
        finally:
            await conn.close()

    assert asyncio.run(go()) == 0


@needs_pg
def test_a_dry_run_reports_the_same_plan_and_changes_nothing():
    async def go():
        conn = await _connect()
        try:
            await _seed(conn)
            before = await conn.fetchval("SELECT COUNT(*) FROM entities")
            rep = await ec.run(conn, dry_run=True)
            after = await conn.fetchval("SELECT COUNT(*) FROM entities")
            return rep, before, after
        finally:
            await conn.close()

    rep, before, after = asyncio.run(go())
    assert rep["dry_run"] is True
    assert rep["to_merge"] == 3 and rep["to_drop"] == 2
    assert before == after, "a dry run modified the graph"


@needs_pg
def test_running_it_twice_finds_nothing_the_second_time():
    async def go():
        conn = await _connect()
        try:
            await _seed(conn)
            first = await ec.run(conn, days=90)
            second = await ec.run(conn, days=90)
            return first, second
        finally:
            await conn.close()

    first, second = asyncio.run(go())
    assert first["to_merge"] > 0
    assert second["to_merge"] == 0 and second["to_drop"] == 0
    assert second["entities_after"] == first["entities_after"]
