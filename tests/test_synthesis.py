"""Multi-source synthesis: the clustering, the prompt, and the write path.

One 200-character blurb cannot become an original 60-80 word article — anything
added beyond it is invented and anything kept is a paraphrase, which is why the
single-article rewrite spent months producing rows the originality gate then
rejected. Several blurbs about ONE event can: the facts they agree on are
corroborated and re-authoring their union is ordinary journalism.

These pin the properties that make that true rather than merely plausible:

  * clustering groups an event and refuses to merge across the window;
  * the prompt reaching the model is the specified one, unedited;
  * a short, empty or malformed answer is REJECTED, never written;
  * the publisher's source_summary survives the synthesis write, exactly as it
    survives the single-article write (see test_source_summary_preserved.py);
  * one cluster becomes ONE article, with every member id recorded on it;
  * a failed synthesis leaves every row it touched exactly as it found it.
"""
import ast
import asyncio
import json
import os
import sqlite3
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import synthesis  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_synth_counters():
    """RESET to a known baseline, do not save-and-restore.

    _synth_run is module state that only _reprocess_bodies_sync clears. These
    tests call _synthesise_clusters directly, so without this the per-run
    counters accumulate down the file and a test asserting "one request was
    spent" silently measures eight — passing alone and failing in the file.
    """
    import main
    baseline = {"clusters": 0, "written": 0, "merged": 0, "failed": 0,
                "sources_used": 0, "reasons": {}, "pool": {}, "pairs": {},
                "near_misses": []}
    main._synth_run.clear()
    main._synth_run.update(baseline)
    yield
    main._synth_run.clear()
    main._synth_run.update(baseline)


A = ("Oil prices rose on Monday after OPEC+ delegates said the group was "
     "weighing deeper output cuts at its next meeting in Vienna.")
B = ("Crude prices rose on Monday as OPEC+ delegates said the group was "
     "weighing deeper output cuts when ministers next meet in Vienna.")
FLOOD = ("Floods across three districts of Kerala displaced thousands of "
         "residents on Sunday, the state disaster authority said.")
SYNTH = (" ".join(["Benchmark crude settled higher on Monday."] * 4) +
         " Officials described the discussion as preliminary and said no "
         "decision had been taken ahead of the ministerial meeting.")


def _row(rid, headline, pillar=2, when="2026-09-01T10:00:00+00:00",
         tags=None, summary="", source="Wire"):
    return {"id": rid, "headline": headline, "pillar_id": pillar,
            "published_at": when, "micro_tags": json.dumps(tags or []),
            "source_summary": summary, "summary_60": "", "full_body": "",
            "source_name": source, "source_headline": headline, "url": f"u{rid}"}


# ─── clustering ─────────────────────────────────────────────────────────────

def test_articles_about_one_event_land_in_one_cluster():
    rows = [
        _row(1, "Crude climbs as OPEC+ weighs deeper output cuts", summary=A),
        _row(2, "Oil advances after OPEC+ signals further restraint", summary=B),
        _row(3, "Kerala floods displace thousands across three districts",
             pillar=5, summary=FLOOD),
    ]
    groups = {tuple(sorted(r["id"] for r in g))
              for g in synthesis.cluster_events(rows)}
    assert (1, 2) in groups
    assert (3,) in groups


def test_the_same_story_outside_the_window_is_not_the_same_event():
    """THE WINDOW IS WHAT SEPARATES A STORY FROM ITS ANNIVERSARY COVERAGE.

    Without it, a follow-up written seven weeks later shares every significant
    term with the original and would be merged into it — and the merge is
    destructive, because the losing rows leave the feed.
    """
    rows = [
        _row(1, "Crude climbs as OPEC+ weighs deeper output cuts",
             when="2026-09-01T10:00:00+00:00", summary=A),
        _row(2, "Oil advances after OPEC+ signals further restraint",
             when="2026-10-20T10:00:00+00:00", summary=B),
    ]
    groups = synthesis.cluster_events(rows, window_hours=24)
    assert sorted(len(g) for g in groups) == [1, 1]
    # …and with a window wide enough to contain both, they do merge — proving
    # the window is what separated them and not some other accident.
    assert len(synthesis.cluster_events(rows, window_hours=24 * 90)) == 1


def test_a_different_pillar_is_a_different_story():
    rows = [_row(1, "Crude climbs as OPEC+ weighs deeper output cuts",
                 pillar=1, summary=A),
            _row(2, "Oil advances after OPEC+ signals further restraint",
                 pillar=2, summary=B)]
    assert len(synthesis.cluster_events(rows)) == 2


def test_every_input_row_comes_back_exactly_once():
    """A row that fell out of clustering would silently never be rewritten."""
    rows = [_row(i, f"Story number {i} about markets and trade",
                 summary=f"Unrelated dispatch {i} concerning several distinct topics.")
            for i in range(1, 12)]
    out = [r["id"] for g in synthesis.cluster_events(rows) for r in g]
    assert sorted(out) == list(range(1, 12))


def test_two_sources_is_the_floor_not_three():
    """MEASURED-CORPUS DECISION, recorded as a test so it cannot drift back.

    A feed ingesting a wide spread of publishers produces mostly 1- and
    2-source events. A pass that only fired at 3+ would idle on that corpus,
    so the floor is 2 — two publishers still corroborate.
    """
    assert synthesis.MIN_CLUSTER == 2


def test_a_missing_timestamp_does_not_block_clustering():
    """Rows predating the published_at normalisation carry ''. Refusing to
    cluster them would exclude the oldest and largest part of the backlog."""
    rows = [_row(1, "Crude climbs as OPEC+ weighs deeper output cuts",
                 when="", summary=A),
            _row(2, "Oil advances after OPEC+ signals further restraint",
                 when="", summary=B)]
    assert len(synthesis.cluster_events(rows)) == 1


# ─── the prompt ─────────────────────────────────────────────────────────────

def test_the_prompt_is_the_specified_one():
    """Every clause here is load-bearing — fact isolation is what keeps the
    output out of copyright, the anti-hallucination rule is what keeps it
    compliant, and the JSON shape is what parse_synthesis validates."""
    p = synthesis.SYNTHESIS_PROMPT
    assert p.startswith("You are an objective news synthesis engine.")
    for clause in ("FACT ISOLATION", "SYNTHESIS & RE-AUTHORING",
                   "STRICT PROHIBITIONS", "inverted pyramid",
                   "Do NOT hallucinate names, dates, or numbers",
                   "If the sources conflict, mention the discrepancy explicitly",
                   '"primary_source_attribution"'):
        assert clause in p, f"missing from the prompt: {clause}"


def test_every_source_reaches_the_prompt():
    rows = [_row(1, "A", summary="First publisher text about the event."),
            _row(2, "B", summary="Second publisher text about the event.")]
    built = synthesis.build_prompt(rows, source_text_of=lambda r: r["source_summary"])
    assert "{{SOURCE_ARTICLES}}" not in built
    assert "[SOURCE 1]" in built and "[SOURCE 2]" in built
    assert "First publisher text" in built and "Second publisher text" in built


# ─── validating the answer ──────────────────────────────────────────────────

def _answer(**over):
    base = {"headline": "Refiner posts record quarterly profit",
            "content": " ".join(["word"] * 70),
            "extracted_entities": ["Reliance", "Mumbai"],
            "primary_source_attribution": "Wire"}
    base.update(over)
    return json.dumps(base)


def test_a_valid_answer_parses():
    got = synthesis.parse_synthesis(_answer(), n_sources=3)
    assert got["n_sources"] == 3 and got["words"] == 70
    assert got["extracted_entities"] == ["Reliance", "Mumbai"]


def test_a_fenced_answer_still_parses():
    """Some providers wrap JSON in a code fence whatever the response_format."""
    assert synthesis.parse_synthesis("```json\n" + _answer() + "\n```")["words"] == 70


@pytest.mark.parametrize("bad", [
    _answer(content="too short"),
    _answer(headline=""),
    "not json at all",
    json.dumps(["a", "list"]),
])
def test_a_bad_answer_is_rejected_rather_than_written(bad):
    """A fragment on a published row is worse than the placeholder it replaces:
    the placeholder is honest and the next tick retries it."""
    with pytest.raises(synthesis.SynthesisRejected):
        synthesis.parse_synthesis(bad)


# ─── the write path ─────────────────────────────────────────────────────────

def _db(tmp_path, rows):
    import main
    conn = sqlite3.connect(str(tmp_path / "s.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    for r in rows:
        conn.execute(
            "INSERT INTO articles (id, url, headline, source_headline, full_body,"
            " summary_60, source_summary, status, pillar_id, published_at,"
            " micro_tags, source_name, ai_processed, reprocessed)"
            " VALUES (?,?,?,?,?,?,?,'published',?,?,?,?,1,0)",
            (r["id"], r["url"], r["headline"], r["source_headline"], "",
             "", r["source_summary"], r["pillar_id"], r["published_at"],
             r["micro_tags"], r["source_name"]))
    conn.commit()
    return conn


def _cluster_rows():
    return [_row(1, "Crude climbs as OPEC+ weighs deeper output cuts", summary=A),
            _row(2, "Oil advances after OPEC+ signals further restraint", summary=B)]


def test_one_cluster_becomes_one_article_carrying_every_source_id(tmp_path,
                                                                  monkeypatch):
    """THE ATTRIBUTION TRAIL. A synthesised body is not traceable to any
    publisher by inspection, so synthesis_sources is the only record of what it
    was written from — and without it the pass would be unauditable."""
    import main
    conn = _db(tmp_path, _cluster_rows())

    async def fake(prompt, n_sources=0):
        assert "[SOURCE 1]" in prompt and "[SOURCE 2]" in prompt
        assert "OPEC+ delegates" in prompt, "the publisher's own text must reach the model"
        return synthesis.parse_synthesis(
            _answer(content=SYNTH, headline="Producers weigh deeper output cuts"),
            n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    leftover = main._synthesise_clusters(conn, work)

    assert leftover == [], "a synthesised cluster must not also go to the single pass"
    rows = {r["id"]: r for r in conn.execute(
        "SELECT id, status, full_body, summary_60, source_summary,"
        " synthesis_sources FROM articles").fetchall()}
    written = [r for r in rows.values() if r["synthesis_sources"]]
    assert len(written) == 1, "a cluster must produce exactly one article"
    got = written[0]
    assert json.loads(got["synthesis_sources"]) == [1, 2]
    assert got["full_body"] == SYNTH
    assert got["summary_60"], "the card renders summary_60 — it cannot stay empty"
    assert got["status"] == "published"

    other = rows[2 if got["id"] == 1 else 1]
    assert other["status"] == "merged", \
        "the same body on two rows is two identical cards in the feed"
    assert main._synth_run["written"] == 1 and main._synth_run["merged"] == 1


def test_the_publishers_text_survives_the_synthesis_write(tmp_path, monkeypatch):
    """source_summary is the ONLY copy of the source this schema keeps: the
    originality reference AND the material a retry rewrites from."""
    import main
    conn = _db(tmp_path, _cluster_rows())

    async def fake(prompt, n_sources=0):
        return synthesis.parse_synthesis(_answer(content=SYNTH), n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    main._synthesise_clusters(
        conn, conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall())
    got = {r["id"]: r["source_summary"] for r in
           conn.execute("SELECT id, source_summary FROM articles").fetchall()}
    assert got[1] == A and got[2] == B


def test_no_synthesis_update_names_source_summary():
    """Static proof, not a sampled one: the statement itself must not be able
    to touch the column, whatever the data happens to be."""
    src = open(os.path.join(_ROOT, "main.py")).read()
    tree = ast.parse(src)
    lines = src.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_synthesise_clusters":
            body = "\n".join(lines[node.lineno - 1:node.end_lineno])
            break
    else:
        pytest.fail("_synthesise_clusters not found")
    for stmt in ("UPDATE articles SET headline=", "UPDATE articles SET status='merged'"):
        assert stmt in body
    updates = body[body.index("UPDATE articles"):]
    assert "source_summary=" not in updates


def test_a_failed_synthesis_changes_nothing_and_hands_the_rows_back(tmp_path,
                                                                    monkeypatch):
    """The rows go to the single-article path this tick and stay candidates
    after it. A row whose body is still a placeholder must never be flagged
    done — that is how work disappears silently."""
    import main
    conn = _db(tmp_path, _cluster_rows())

    async def refuse(prompt, n_sources=0):
        return None

    monkeypatch.setattr(main.ai_processor, "synthesize", refuse)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    # An explicit budget: the default is len(work), and the failed cluster spends
    # one request of it, so the default would trim a row for a reason that has
    # nothing to do with what this test is asserting.
    leftover = main._synthesise_clusters(conn, work, budget=10)
    assert sorted(r["id"] for r in leftover) == [1, 2]
    after = conn.execute("SELECT id, status, reprocessed, synthesis_sources,"
                         " full_body FROM articles").fetchall()
    for r in after:
        assert r["status"] == "published" and r["reprocessed"] == 0
        assert not r["synthesis_sources"] and not r["full_body"]


def test_a_synthesis_that_reproduces_its_source_is_never_written(tmp_path,
                                                                 monkeypatch):
    """A "synthesis" that came back as one source's prose is a paraphrase of
    that source, whatever else went into it."""
    import main
    conn = _db(tmp_path, _cluster_rows())

    async def copies(prompt, n_sources=0):
        return synthesis.parse_synthesis(_answer(content=A + " " + A),
                                         n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", copies)
    leftover = main._synthesise_clusters(
        conn, conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall(),
        budget=10)
    assert sorted(r["id"] for r in leftover) == [1, 2]
    assert not any(r["synthesis_sources"] for r in
                   conn.execute("SELECT synthesis_sources FROM articles"))


def test_singletons_are_left_for_the_single_article_pass(tmp_path, monkeypatch):
    """Synthesis is not attempted on one source. That is the impossible task
    this whole pass exists to stop attempting."""
    import main
    conn = _db(tmp_path, [_row(1, "A lone story nobody else covered today",
                               summary=A)])
    called = []
    monkeypatch.setattr(main.ai_processor, "synthesize",
                        lambda *a, **k: called.append(1))
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall()
    assert [r["id"] for r in main._synthesise_clusters(conn, work)] == [1]
    assert not called


def test_a_cluster_costs_one_request_not_one_per_article(tmp_path, monkeypatch):
    """The free tier's scarce resource is REQUESTS. Synthesis has to spend
    fewer of them than the path it replaces, or the drain gets slower."""
    import main
    rows = [_row(i, "OPEC+ weighs deeper output cuts as crude climbs",
                 summary=A + str(i)) for i in range(1, 6)]
    conn = _db(tmp_path, rows)
    calls = []

    async def fake(prompt, n_sources=0):
        calls.append(n_sources)
        return synthesis.parse_synthesis(_answer(content=SYNTH), n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    main._synthesise_clusters(
        conn, conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (10,)).fetchall())
    assert len(calls) == 1, f"5 articles cost {len(calls)} requests, expected 1"
    assert calls[0] == 5


def test_the_audit_reports_the_pass(tmp_path):
    """/admin/body-audit is the only window onto this; a pass nobody can see
    working is a pass nobody can tell has stopped."""
    import main
    src = open(main.__file__).read()
    assert 'out["synthesis"] = synth' in src
    assert '"articles_per_request"' in src


# ─── why the first production run found nothing ─────────────────────────────
#
# clusters_seen 10, size_histogram {"1": 10}, min_cluster 2 — every cluster a
# singleton. The threshold was not the cause and lowering it would have started
# merging unrelated stories. The cause was the SAMPLE: the drain clustered
# exactly the rows its per-minute rate limit allowed, so the clusterer was asked
# to find pairs from a 24-hour window inside a sample spanning a few minutes.

def _timed_rows(n, minutes_apart, event_every=None):
    """n rows, `minutes_apart` apart, optionally with a duplicate every k."""
    out = []
    for i in range(n):
        when = f"2026-09-01T{10 + (i * minutes_apart) // 60:02d}:" \
               f"{(i * minutes_apart) % 60:02d}:00+00:00"
        if event_every and i and i % event_every == 0:
            src, head = A, "Crude climbs as OPEC+ weighs deeper output cuts"
        else:
            src = (f"Company {i} reported unrelated quarterly figures {i} on "
                   f"a separate matter numbered {i} with distinct detail {i}.")
            head = f"Company {i} reports unrelated quarterly figures {i}"
        out.append(_row(i + 1, head, when=when, summary=src))
    return out


def test_a_sample_spanning_minutes_cannot_contain_a_days_worth_of_pairs():
    """THE DIAGNOSIS, as an assertion. Twelve consecutive articles from a feed
    that publishes steadily span minutes, not a day — so the 24h window never
    gets to apply and no threshold change could rescue it."""
    twelve = _timed_rows(12, 1)
    report = synthesis.pool_report(twelve, window_hours=24)
    assert report["span_hours"] < 1
    assert report["sample_covers_window"] is False


def test_the_same_corpus_clusters_once_the_pool_is_large_enough():
    """Same articles, same thresholds, bigger sample — the pairs appear. That
    is what makes this a sample-size problem rather than a threshold problem."""
    corpus = _timed_rows(120, 12, event_every=40)   # a repeat every 8 hours
    small, large = corpus[:12], corpus
    assert not [g for g in synthesis.cluster_events(small) if len(g) >= 2]
    assert [g for g in synthesis.cluster_events(large) if len(g) >= 2]


def test_the_stop_reason_for_every_pair_is_counted():
    """"No clusters" has four causes and four different fixes. Counting only
    the clusters cannot tell them apart; counting the stop reason can."""
    stats = {}
    synthesis.cluster_events(_timed_rows(30, 20, event_every=10), stats=stats)
    assert stats["pairs_examined"] > 0
    assert set(stats["stopped"]) <= {"joined", "shared_below_min",
                                     "ratio_below_min", "different_pillar",
                                     "outside_window"}
    assert sum(stats["stopped"].values()) == stats["pairs_examined"]
    assert stats["ratio_histogram"] and stats["shared_histogram"]


def test_the_near_misses_carry_headlines():
    """The one question a histogram cannot answer: are these the same story?"""
    stats = {}
    synthesis.cluster_events(_timed_rows(20, 20, event_every=7), stats=stats)
    assert stats["best_pairs"]
    for pair in stats["best_pairs"]:
        assert len(pair["headlines"]) == 2 and pair["headlines"][0]
        assert 0.0 <= pair["ratio"] <= 1.0


# ─── the two hazards a bigger pool introduces ───────────────────────────────

def test_boilerplate_cannot_chain_the_whole_pool_into_one_cluster():
    """MEASURED, not feared: with a flat generic-term cap, a 115-row pool of
    articles sharing publisher boilerplate ("the company said", "on Monday")
    collapsed into ONE 115-member cluster. The cap now scales with the sample,
    because a term in 40 of 50 rows is corpus vocabulary however you count it."""
    rows = [_row(i + 1,
                 f"Firm {i} posted results as the company said on Monday",
                 when=f"2026-09-01T{10 + i // 6:02d}:{(i * 10) % 60:02d}:00+00:00",
                 summary=("The company said the figures were released on Monday "
                          "according to a statement issued after the meeting."))
            for i in range(60)]
    stats = {}
    clusters = synthesis.cluster_events(rows, stats=stats)
    assert max(len(g) for g in clusters) <= synthesis.MAX_EVENT_SIZE
    assert stats["term_doc_cap"] <= max(3, int(0.10 * len(rows)))


def test_an_oversized_cluster_is_refused_not_truncated():
    """Truncating a 40-member "event" to five would merge four arbitrary rows
    out of the feed and leave the rest — worse than writing nothing, and
    impossible to explain afterwards. It is broken back into singletons.

    The shape that produces one is a CHAIN, not a blob: union-find is
    transitive, so a~b, b~c, c~d joins all four even though a and d share
    nothing. That is the failure mode a large pool makes reachable, and the one
    the generic-term cap cannot catch — each link here is a term appearing in
    exactly two rows.
    """
    chain = synthesis.MAX_EVENT_SIZE + 4
    rows, filler = [], 90
    for i in range(chain):
        # Row i shares its "left" terms with row i-1 and its "right" with i+1.
        left = " ".join(f"linkterm{i:02d}x{k}" for k in range(9))
        right = " ".join(f"linkterm{i + 1:02d}x{k}" for k in range(9))
        rows.append(_row(i + 1, f"Chained item {i}",
                         when="2026-09-01T10:00:00+00:00",
                         summary=f"{left} {right}"))
    for j in range(filler):
        rows.append(_row(1000 + j, f"Filler {j}",
                         when="2026-09-01T10:00:00+00:00",
                         summary=" ".join(f"fill{j:03d}y{k}" for k in range(18))))
    stats = {}
    clusters = synthesis.cluster_events(rows, stats=stats)
    assert stats["oversized_clusters_refused"] >= 1
    assert max(len(g) for g in clusters) <= synthesis.MAX_EVENT_SIZE
    # Nothing is lost — every row still comes back, just unclustered.
    assert sum(len(g) for g in clusters) == len(rows)
    assert {r["id"] for g in clusters for r in g} == {r["id"] for r in rows}


# ─── the pool, and the request budget it must not spend ─────────────────────

def test_the_clustering_pool_is_bigger_than_the_request_budget(tmp_path,
                                                               monkeypatch):
    """The pool costs one SELECT; only a written cluster costs a request. Tying
    them together is what made the pass blind."""
    import main
    rows = ([_row(1, "Crude climbs as OPEC+ weighs deeper output cuts", summary=A),
             _row(2, "Oil advances after OPEC+ signals further restraint", summary=B)]
            + [_row(i, f"Unrelated story {i} on a separate matter entirely {i}",
                    summary=f"Separate dispatch {i} about distinct subject {i}.")
               for i in range(3, 12)])
    conn = _db(tmp_path, rows)
    seen = {}

    async def fake(prompt, n_sources=0):
        seen["n"] = n_sources
        return synthesis.parse_synthesis(_answer(content=SYNTH), n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    # The tick owns ONE row — id 1. Its partner is only reachable through the
    # pool, which is exactly the case the old code could never see.
    work = [r for r in conn.execute(main.body_state.SELECT_NEEDING_REWRITE,
                                    (20,)).fetchall() if r["id"] == 1]
    main._synthesise_clusters(conn, work, budget=4)
    assert seen.get("n") == 2, "the partner outside the tick's rows was not found"
    assert main._synth_run["pool"]["rows"] > len(work)


def test_clusters_and_singles_share_one_request_allowance(tmp_path, monkeypatch):
    """A cluster costs exactly one request — the same unit a single rewrite
    costs — so the leftovers handed to the single-article batch are trimmed by
    what the clusters already spent. Otherwise the tick quietly exceeds the
    free tier's per-minute rate."""
    import main
    rows = [_row(1, "Crude climbs as OPEC+ weighs deeper output cuts", summary=A),
            _row(2, "Oil advances after OPEC+ signals further restraint", summary=B)]
    rows += [_row(i, f"Unrelated story {i} on a separate matter entirely {i}",
                  summary=f"Separate dispatch {i} about distinct subject {i}.")
             for i in range(3, 9)]
    conn = _db(tmp_path, rows)

    async def fake(prompt, n_sources=0):
        return synthesis.parse_synthesis(_answer(content=SYNTH), n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (20,)).fetchall()
    budget = 4
    leftover = main._synthesise_clusters(conn, work, budget=budget)
    spent = main._synth_run["clusters"]
    assert spent >= 1
    assert spent + len(leftover) <= budget, \
        f"{spent} cluster call(s) + {len(leftover)} single(s) exceeds {budget}"


def test_a_deferred_row_is_not_marked_done(tmp_path, monkeypatch):
    """A row trimmed by the allowance must stay a candidate. Flagging it is how
    work disappears silently, which is the failure this whole pass exists to
    end."""
    import main
    rows = [_row(1, "Crude climbs as OPEC+ weighs deeper output cuts", summary=A),
            _row(2, "Oil advances after OPEC+ signals further restraint", summary=B)]
    rows += [_row(i, f"Unrelated story {i} on a separate matter entirely {i}",
                  summary=f"Separate dispatch {i} about distinct subject {i}.")
             for i in range(3, 9)]
    conn = _db(tmp_path, rows)

    async def fake(prompt, n_sources=0):
        return synthesis.parse_synthesis(_answer(content=SYNTH), n_sources=n_sources)

    monkeypatch.setattr(main.ai_processor, "synthesize", fake)
    work = conn.execute(main.body_state.SELECT_NEEDING_REWRITE, (20,)).fetchall()
    leftover = main._synthesise_clusters(conn, work, budget=3)
    handled = {r["id"] for r in conn.execute(
        "SELECT id FROM articles WHERE reprocessed=1 OR status='merged'")}
    left = {r["id"] for r in leftover}
    for r in rows:
        if r["id"] not in handled and r["id"] not in left:
            row = conn.execute("SELECT reprocessed, status FROM articles WHERE id=?",
                               (r["id"],)).fetchone()
            assert row["reprocessed"] == 0 and row["status"] == "published", \
                f"deferred row {r['id']} was flagged done without being rewritten"


def test_the_audit_says_which_cause_it_is(tmp_path, monkeypatch):
    """A one-line verdict, so nobody re-derives the argument from a JSON blob —
    and so a threshold is never loosened to fix a sample-size problem."""
    import main
    main._synth_stats["last_pool"] = {
        "rows": 12, "span_hours": 0.2, "window_hours": 24,
        "sample_covers_window": False, "eligible_pairs": 66}
    main._synth_stats["last_pairs"] = {"pairs_examined": 66,
                                       "stopped": {"shared_below_min": 66}}
    verdict = main._synthesis_diagnosis(dict(main._synth_stats,
                                             articles_written=0))
    assert "SYNTHESIS_POOL" in verdict
    assert "0.2" in verdict and "24" in verdict
