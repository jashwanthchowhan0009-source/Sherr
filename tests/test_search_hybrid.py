"""search_rank.py and the /search endpoint it ranks.

WHAT THESE PIN. `/search` used to be `headline LIKE '%q%' ORDER BY published_at
DESC` — a substring match with no index, no relevance ordering, and no bound on
the corpus it scanned on a reader's request. The three properties that replaced
it are the three that can regress silently:

  • the LIKE leg matches each WORD, so "oil price" finds a headline containing
    both and not only the exact phrase;
  • a priority leg cannot outvote relevance, so a merely-recent weak match does
    not outrank the story the reader asked for;
  • every statement is bounded, including the union fetch.

The fusion is pure, so most of this runs without a database. The endpoint tests
use the sqlite backend, which is the no-capability path — the fallback leg plus
the two priority legs. The Postgres legs are asserted as SQL shape, because the
one thing that makes them worth having is that the ranking expression matches
the index expression exactly, and that is a string comparison.
"""
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import cache          # noqa: E402
import main           # noqa: E402
import search_rank as S   # noqa: E402


@pytest.fixture(autouse=True)
def _clean():
    cache._local.clear()
    S.set_capabilities(())
    for k in ("SEARCH_HYBRID_ENABLED", "SEARCH_LEG_LIMIT", "SEARCH_RRF_K",
              "SEARCH_PAGE_SIZE"):
        os.environ.pop(k, None)
    yield
    cache._local.clear()
    S.set_capabilities(())


# ─── query sanitisation ──────────────────────────────────────────────────────
def test_query_is_bounded_and_stripped():
    assert S.clean_query("  oil   price \n") == "oil price"
    assert S.clean_query("a\x00b") == "a b"
    # A reader can paste a page into the box; a 10KB bind parameter would be
    # sent to every leg.
    assert len(S.clean_query("x" * 5000)) == S.MAX_QUERY_CHARS


def test_single_character_query_is_not_a_search():
    assert not S.is_usable("o")
    assert not S.is_usable("  ")
    assert S.is_usable("oil")


def test_query_words_drops_punctuation_and_single_letters():
    assert S.query_words("Oil-price: a RISE!") == ["oil", "price", "rise"]


# ─── pre-filtering happens in the WHERE clause ───────────────────────────────
def test_filters_are_applied_inside_every_leg():
    """PRE-filtering, not post. Narrowing to a pillar must return that pillar's
    best N matches, not whatever survives filtering a corpus-wide N."""
    f = S.Filters(pillar=3, scope="local", feed_class="financial",
                  min_significance=42, since="2026-01-01T00:00:00+00:00")
    for name, sql, params in S.relevance_legs("oil", f, 50, caps=["fts", "trgm"]):
        assert "pillar_id=?" in sql, name
        assert "scope=?" in sql, name
        assert "feed_class=?" in sql, name
        assert "significance >= ?" in sql, name
        assert "published_at >= ?" in sql, name
        assert "ai_processed=1" in sql and "status='published'" in sql, name
        assert sql.rstrip().endswith("LIMIT ?"), name
        assert params[-1] == 50, name


def test_unscored_rows_survive_a_significance_filter():
    """-1 means the backfill has not reached the row. That is not the same claim
    as "scored below the bar", and hiding it would shrink the corpus by whatever
    significance_backfill_job has not got to yet."""
    sql, _ = S.prefilter(S.Filters(min_significance=42))
    assert "significance >= ?" in sql and "significance < 0" in sql


def test_published_at_is_compared_never_matched_as_a_regex():
    """CLAUDE.md: `published_at ~ '...'` raises on a converted column and is what
    took Phase 1 down in production. The column is TEXT on some deployments and
    timestamptz on others, so only a plain comparison is valid on both."""
    sql, _ = S.prefilter(S.Filters(since="2026-01-01T00:00:00+00:00"))
    assert "published_at >= ?" in sql
    assert "~" not in sql


# ─── the Postgres legs ───────────────────────────────────────────────────────
def test_fts_ranking_expression_is_the_indexed_expression():
    """The only reason this leg is worth having is that the planner can use the
    index. A different COALESCE or column order and it is a sequential scan with
    a to_tsvector call per row — strictly worse than the LIKE it replaced."""
    sql, _ = S.leg_fts_sql("oil", S.Filters(), 10)
    indexed = S._INDEX_SQL[S.CAP_FTS][0]
    assert S._FTS_VECTOR in indexed
    assert sql.count(S._FTS_VECTOR) == 2        # the match and the ranking
    assert "websearch_to_tsquery" in sql
    # to_tsquery raises on malformed input; websearch_to_tsquery is the one
    # constructor documented not to, which is why reader input goes to it.
    assert "plainto_tsquery" not in sql and " to_tsquery" not in sql


def test_trgm_leg_uses_the_index_backed_operator():
    sql, _ = S.leg_trgm_sql("oil", S.Filters(), 10)
    assert "<%" in sql                       # gin_trgm_ops supports this
    assert "gin_trgm_ops" in S._INDEX_SQL[S.CAP_TRGM][1]


def test_like_leg_ands_the_words_so_a_phrase_is_found_across_them():
    """The old endpoint matched the literal substring, so "oil price" found
    nothing — the two words are never adjacent in a real headline."""
    sql, params = S.leg_like_sql("oil price", S.Filters(), 10)
    assert sql.count("LIKE ?") == 4          # two words x headline + summary
    assert " AND " in sql.split("WHERE")[1].split("ai_processed")[0]
    assert "%oil%" in params and "%price%" in params


def test_like_leg_bounds_the_number_of_words():
    sql, _ = S.leg_like_sql(" ".join(f"w{i}" for i in range(20)), S.Filters(), 10)
    assert sql.count("LIKE ?") == 12         # 6 words, capped


# ─── capability degradation ──────────────────────────────────────────────────
def test_the_coarse_leg_is_not_added_alongside_the_indexed_ones():
    """A substring match is a coarser view of the same question. Giving it a
    second vote would pull the fusion back towards the old behaviour."""
    names = [n for n, _, _ in S.relevance_legs("oil", S.Filters(), 10,
                                               caps=["fts", "trgm"])]
    assert names == ["fts", "trgm"]
    assert [n for n, _, _ in S.relevance_legs("oil", S.Filters(), 10,
                                              caps=["fts"])] == ["fts"]
    assert [n for n, _, _ in S.relevance_legs("oil", S.Filters(), 10,
                                              caps=[])] == ["like"]


def test_sqlite_grants_no_capability_and_builds_no_index():
    """There is no fts5 shadow table and no triggers to keep one in sync, for a
    backend that only ever holds a test fixture."""
    def _boom():
        raise AssertionError("ensure_indexes must not open a connection on sqlite")
    assert S.ensure_indexes(_boom, is_postgres=False) == frozenset()


def test_a_refused_statement_removes_only_its_own_capability():
    """CREATE EXTENSION is the one a managed Postgres refuses. Full-text must
    survive that, and each statement needs its OWN connection — a failed DDL
    aborts the transaction, so a shared one would make the first failure look
    like every capability was missing."""
    opened = []

    class _Conn:
        def execute(self, sql):
            if "pg_trgm" in sql:
                raise RuntimeError("permission denied to create extension")
            return None

        def commit(self):
            pass

        def close(self):
            pass

    def _factory():
        opened.append(1)
        return _Conn()

    caps = S.ensure_indexes(_factory, is_postgres=True)
    assert caps == frozenset({S.CAP_FTS})
    assert len(opened) == 2        # the fts index, then the refused extension


def test_both_statements_failing_leaves_the_fallback_and_says_so():
    class _Conn:
        def execute(self, sql):
            raise RuntimeError("nope")

        def close(self):
            pass

    assert S.ensure_indexes(lambda: _Conn(), is_postgres=True) == frozenset()
    assert "NO search index" in S.diagnose(is_postgres=True)


@pytest.mark.parametrize("caps,needle", [
    (["fts", "trgm"], "OK"),
    (["fts"], "pg_trgm"),
    (["trgm"], "idx_articles_fts"),
    ([], "NO search index"),
])
def test_every_capability_state_has_its_own_diagnosis(caps, needle):
    """Four causes, four fixes. A single "search is degraded" line would send
    somebody looking in the wrong place."""
    assert needle in S.diagnose(caps, is_postgres=True)


# ─── Reciprocal Rank Fusion ──────────────────────────────────────────────────
def test_rrf_sums_only_the_legs_a_row_appears_in():
    """Absence from a leg is not evidence against the row — the legs ask
    different questions — so it contributes nothing rather than a penalty."""
    fused = S.rrf_fuse({"a": [1, 2], "b": [2]},
                       weights={"a": 1.0, "b": 1.0}, k=60)
    by_id = {h.id: h for h in fused}
    assert by_id[1].ranks == {"a": 1}
    assert by_id[2].ranks == {"a": 2, "b": 1}
    assert by_id[2].score == pytest.approx(1 / 62 + 1 / 61)
    assert by_id[1].score == pytest.approx(1 / 61)
    # Two legs beat one better placement.
    assert [h.id for h in fused] == [2, 1]


def test_weights_are_honoured_and_a_zero_weight_leg_is_skipped():
    fused = S.rrf_fuse({"a": [1], "b": [2]}, weights={"a": 1.0, "b": 0.0}, k=60)
    assert [h.id for h in fused] == [1]


def test_a_priority_leg_cannot_outvote_relevance():
    """The property the weights exist for. Row 9 is the only relevance match;
    row 1 is the newest and the most significant. Relevance must win, or the
    endpoint is back to ORDER BY published_at DESC with extra steps."""
    fused = S.rrf_fuse({
        "fts": [9],
        "recency": [1, 2, 3, 9],
        "significance": [1, 2, 3, 9],
    })
    assert fused[0].id == 9


def test_fusion_is_deterministic_on_ties():
    """A fusion that reordered equal scores would make every cached page and
    every test flaky."""
    r = {"a": [5, 7], "b": [7, 5]}
    first = [h.id for h in S.rrf_fuse(r)]
    assert first == [h.id for h in S.rrf_fuse(r)]
    assert first == [7, 5]          # equal scores, higher id first


def test_a_duplicated_id_in_one_leg_is_credited_once():
    fused = S.rrf_fuse({"a": [4, 4]}, weights={"a": 1.0}, k=60)
    assert len(fused) == 1
    assert fused[0].score == pytest.approx(1 / 61)


def test_empty_rankings_fuse_to_nothing():
    assert S.rrf_fuse({}) == []
    assert S.rrf_fuse({"a": []}) == []


# ─── priority legs are derived, not queried ──────────────────────────────────
def test_priority_rankings_order_the_matched_rows_only():
    rows = [
        {"id": 1, "published_at": "2026-10-01T00:00:00+00:00", "significance": 90},
        {"id": 2, "published_at": "2026-10-20T00:00:00+00:00", "significance": 10},
        {"id": 3, "published_at": "2026-10-10T00:00:00+00:00", "significance": -1},
    ]
    r = S.priority_rankings(rows)
    assert r["recency"] == [2, 3, 1]
    assert r["significance"] == [1, 2, 3]      # -1 sorts last on this leg only
    assert set(r["recency"]) == {1, 2, 3}


def test_an_unscored_row_still_carries_its_other_ranks():
    """Sorting -1 last on the significance leg is not the same as excluding it:
    it keeps its relevance and recency ranks, which is what stops a backfill
    gap from hiding a story."""
    rows = [{"id": 7, "published_at": "2026-10-20T00:00:00+00:00",
             "significance": -1}]
    r = S.priority_rankings(rows)
    assert r["significance"] == [7] and r["recency"] == [7]


def test_a_missing_significance_column_does_not_raise():
    rows = [{"id": 1, "published_at": "2026-10-01T00:00:00+00:00"}]
    assert S.priority_rankings(rows)["significance"] == [1]


# ─── tuning is read at call time ─────────────────────────────────────────────
def test_tuning_is_read_at_call_time_not_import_time():
    """CLAUDE.md: reading configuration at call time is what removes the need
    for importlib.reload, which has broken this suite before."""
    assert S.leg_limit() == 200
    os.environ["SEARCH_LEG_LIMIT"] = "25"
    assert S.leg_limit() == 25
    os.environ["SEARCH_RRF_K"] = "10"
    assert S.rrf_k() == 10
    os.environ["SEARCH_HYBRID_ENABLED"] = "0"
    assert S.enabled() is False


@pytest.mark.parametrize("val", ["", "abc", "-5", "99999"])
def test_a_bad_tuning_value_never_removes_the_bound(val):
    os.environ["SEARCH_LEG_LIMIT"] = val
    assert 10 <= S.leg_limit() <= 1000


# ─── the endpoint, on the sqlite (no-capability) path ────────────────────────
_ROWS = [
    # headline,                                      published_at,          sig
    ("Oil price climbs as OPEC weighs output cuts",  "2026-10-01T00:00:00+00:00", 80),
    ("Markets steady ahead of the oil inventory report", "2026-10-20T00:00:00+00:00", 30),
    ("A price war breaks out in telecom",            "2026-10-21T00:00:00+00:00", 70),
    ("Monsoon floods close three districts",         "2026-10-22T00:00:00+00:00", 15),
]


def _seed(tmp_path, monkeypatch, rows=None):
    db = str(tmp_path / "search.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    for i, (head, pa, sig) in enumerate(rows or _ROWS, start=1):
        conn.execute(
            "INSERT INTO articles (id, url, headline, summary_60, source_headline, "
            "published_at, significance, pillar_id, scope, feed_class, "
            "ai_processed, status) VALUES (?,?,?,?,?,?,?,?,?,?,1,'published')",
            (i, f"https://example.test/{i}", head, head,
             "publisher: " + head, pa, sig,
             1 if i % 2 else 2, "national", "general"))
    conn.commit()
    conn.close()

    def _get_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(main, "get_db", _get_db)
    # The stock-image pass makes a network call per subject; it is wired into
    # every card endpoint and is not what these tests are about.
    async def _noop(arts):
        return arts
    monkeypatch.setattr(main, "_apply_stock_images", _noop)
    return db


@pytest.fixture
def client():
    return TestClient(main.app)


def test_a_two_word_query_finds_the_row_that_contains_both(client, tmp_path, monkeypatch):
    """The regression the old endpoint shipped with: "oil price" is never a
    literal substring of a real headline, so LIKE '%oil price%' returned
    nothing at all."""
    _seed(tmp_path, monkeypatch)
    r = client.get("/search", params={"q": "oil price"})
    assert r.status_code == 200
    ids = [a["id"] for a in r.json()["articles"]]
    assert ids == [1], r.json()


def test_on_the_fallback_leg_the_match_is_binary_and_priority_orders_it(
        client, tmp_path, monkeypatch):
    """A stated limit of the sqlite / no-index path, pinned so nobody reads the
    fusion as doing more than it does here.

    The LIKE leg ANDs the query's words, so a row either matches or does not —
    there is no "matched better". Ordering among the matches is therefore the
    two priority legs, and a one-word query is ranked by recency and
    significance. The leg that discriminates BETWEEN matches is
    `ts_rank_cd`, which only exists on Postgres with the index; it is asserted
    as SQL shape above, and the property that relevance outvotes priority is
    pinned purely in test_a_priority_leg_cannot_outvote_relevance.

    What still holds on every backend, and is what the old endpoint got wrong:
    a row sharing no word with the query never appears at all.
    """
    _seed(tmp_path, monkeypatch)
    body = client.get("/search", params={"q": "oil", "explain": 1}).json()
    ids = [a["id"] for a in body["articles"]]
    assert set(ids) == {1, 2}            # the two rows containing "oil"
    assert 4 not in ids                  # shares no word with the query
    # Row 2 is newer and row 1 is more significant; both legs voted.
    assert set(body["report"]["explain"][0]["ranks"]) == {
        "like", "recency", "significance"}


def test_explain_reports_the_per_leg_ranks(client, tmp_path, monkeypatch):
    """The only way to answer "why is this above that".  It reveals nothing a
    reader cannot already see on the page, which is why it is not admin-only."""
    _seed(tmp_path, monkeypatch)
    body = client.get("/search", params={"q": "oil", "explain": 1}).json()
    rep = body["report"]
    assert rep["legs"]["like"]["hits"] >= 1
    assert set(rep["explain"][0]["ranks"]) <= {"like", "recency", "significance"}
    assert rep["explain"][0]["score"] > 0
    assert rep["caps"] == []       # sqlite grants none


def test_filters_narrow_the_result_set(client, tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    hi = client.get("/search", params={"q": "price", "min_significance": 60}).json()
    assert {a["id"] for a in hi["articles"]} == {1, 3}
    one = client.get("/search", params={"q": "price", "pillar": main.PILLARS[1]["slug"]}).json()
    assert {a["id"] for a in one["articles"]} == {1, 3}


def test_an_unknown_pillar_slug_degrades_to_no_filter(client, tmp_path, monkeypatch):
    """A link shared before a slug was renamed must not come back empty."""
    _seed(tmp_path, monkeypatch)
    body = client.get("/search", params={"q": "oil", "pillar": "no-such-pillar"}).json()
    assert body["articles"]


def test_a_short_query_is_answered_not_scanned(client, tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    body = client.get("/search", params={"q": "o"}).json()
    assert body == {"articles": [], "query": "o", "total": 0}


def test_no_query_returns_nothing(client, tmp_path, monkeypatch):
    _seed(tmp_path, monkeypatch)
    assert client.get("/search", params={"q": ""}).json()["articles"] == []


def test_paging_does_not_repeat_a_row(client, tmp_path, monkeypatch):
    rows = [(f"Oil story number {i}", f"2026-10-{(i % 28) + 1:02d}T00:00:00+00:00", 50)
            for i in range(1, 11)]
    _seed(tmp_path, monkeypatch, rows)
    monkeypatch.setenv("SEARCH_PAGE_SIZE", "4")
    p1 = [a["id"] for a in client.get("/search", params={"q": "oil"}).json()["articles"]]
    p2 = [a["id"] for a in client.get("/search", params={"q": "oil", "page": 2}).json()["articles"]]
    assert len(p1) == 4 and len(p2) == 4
    assert not set(p1) & set(p2)


def test_the_switch_falls_back_to_one_bounded_leg(client, tmp_path, monkeypatch):
    """SEARCH_HYBRID_ENABLED=0 must still answer, and still be bounded — it is
    the behaviour the endpoint had before the fusion, not an outage."""
    _seed(tmp_path, monkeypatch)
    monkeypatch.setenv("SEARCH_HYBRID_ENABLED", "0")
    body = client.get("/search", params={"q": "oil", "explain": 1}).json()
    assert body["report"]["hybrid"] is False
    assert [a["id"] for a in body["articles"]]


def test_a_failing_leg_degrades_rather_than_erroring(client, tmp_path, monkeypatch):
    """One leg failing is a degraded search, not a failed request. The likely
    cause is an index dropped underneath us, and the other leg still answers."""
    _seed(tmp_path, monkeypatch)
    S.set_capabilities(["fts"])      # sqlite cannot run it
    body = client.get("/search", params={"q": "oil", "explain": 1}).json()
    assert body["articles"] == []
    assert "error" in body["report"]["legs"]["fts"]


# ─── the diagnosis endpoint ──────────────────────────────────────────────────
def test_admin_search_is_guarded(client):
    assert client.get("/admin/search").status_code in (401, 403)


def test_admin_search_reports_the_capabilities_and_runs_nothing(client, monkeypatch):
    monkeypatch.setattr(main, "_check_admin", lambda t: None)
    body = client.get("/admin/search", params={"q": "oil price"}).json()
    assert body["capabilities"] == []
    assert body["DIAGNOSIS"]
    assert [w["leg"] for w in body["would_run"]] == ["like"]
    assert body["words"] == ["oil", "price"]
