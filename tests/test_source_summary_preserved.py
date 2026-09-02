"""The publisher's text must survive the AI pass.

`source_summary` holds `clean[:200]` from ingest and is the ONLY copy of the
source this schema keeps. Both AI update paths used to overwrite it with
`result["summary"]` — our own text — under a comment reading "kept for
back-compat".

Two things broke, and both were silent:

  * body_state.classify uses source_summary as the ORIGINALITY REFERENCE. Once
    a row had been through the pass, the gate compared our body against our own
    summary — text checked against itself.
  * source_material() had nothing left to rewrite FROM, so every retry
    regenerated the placeholder. The rows now reported "no_source_material" are
    the ones that line destroyed; for those the text is gone for good and only
    re-ingest recovers it.

Found while fixing the above: /admin/reprocess read `row` where the loop
variable is `r` (NameError on every iteration) and supplied 11 values for 16
placeholders. Both were swallowed by a bare except that logged "update failed",
so the endpoint has never updated a single row. Those are covered here too,
because a statement that cannot execute cannot be shown to preserve anything.
"""
import ast
import os
import re
import sqlite3
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import body_state  # noqa: E402

PUBLISHER = ("Oil prices rose on Monday after OPEC+ delegates said the group "
             "was weighing deeper output cuts at its next meeting.")
OURS = ("Delegates indicated a further reduction is under consideration when "
        "the group next convenes. Benchmark prices settled higher.")


# ─── the statements themselves ──────────────────────────────────────────────

def _update_sql(func_name: str) -> str:
    """The UPDATE statement inside a named function, from the real source."""
    src = open(os.path.join(_ROOT, "main.py")).read()
    tree = ast.parse(src)
    lines = src.splitlines()
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and n.name == func_name:
            body = "\n".join(lines[n.lineno - 1:n.end_lineno])
            i = body.index("UPDATE articles SET")
            return body[i:body.index('"""', i)]
    raise AssertionError(f"{func_name} not found")


@pytest.mark.parametrize("func", ["run_ai_batch", "admin_reprocess"])
def test_no_ai_update_writes_source_summary(func):
    """THE BUG. Neither path may set the column that holds the only copy of the
    publisher's words."""
    sql = _update_sql(func)
    assert "source_summary" not in sql, (
        f"{func} writes source_summary — that destroys the originality "
        f"reference and the material a retry rewrites from")


@pytest.mark.parametrize("func", ["run_ai_batch", "admin_reprocess"])
def test_the_update_still_sets_what_it_should(func):
    sql = _update_sql(func)
    cols = set(re.findall(r"(\w+)=\?", sql))
    assert {"headline", "summary_60", "full_body", "status"} <= cols


@pytest.mark.parametrize("func", ["run_ai_batch", "admin_reprocess"])
def test_the_statement_can_actually_execute(func):
    """A binding-count mismatch is invisible: it raises into a bare except and
    logs "update failed". admin_reprocess supplied 11 values for 16 slots and
    had never run."""
    sql = _update_sql(func)
    n = sql.count("?")
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE articles (id INTEGER PRIMARY KEY, headline TEXT,"
        " summary_60 TEXT, full_body TEXT, source_summary TEXT, when_info TEXT,"
        " where_info TEXT, pillar_id INT, micro_tags TEXT, is_trending INT,"
        " sentiment TEXT, ai_processed INT, reprocessed INT, status TEXT,"
        " originality_json TEXT, originality_overlap REAL, originality_run INT,"
        " originality_checked_at TEXT)")
    conn.execute("INSERT INTO articles (id) VALUES (1)")
    # n-1 real values plus the id; any mismatch raises here.
    conn.execute(sql, tuple(["x"] * (n - 1) + [1]))
    conn.close()


def test_admin_reprocess_does_not_read_an_unbound_name():
    """It read `row` where the loop variable is `r` — NameError every time."""
    src = open(os.path.join(_ROOT, "main.py")).read()
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and n.name == "admin_reprocess":
            assigned = {x.id for x in ast.walk(n)
                        if isinstance(x, ast.Name) and isinstance(x.ctx, ast.Store)}
            read = {x.id for x in ast.walk(n)
                    if isinstance(x, ast.Name) and isinstance(x.ctx, ast.Load)}
            assert "row" not in read or "row" in assigned, \
                "admin_reprocess reads 'row', which is never bound in it"
            return
    raise AssertionError("admin_reprocess not found")


# ─── the consequences the bug had ───────────────────────────────────────────

def test_the_originality_gate_has_a_real_reference_after_a_rewrite():
    """With source_summary overwritten by our own summary, classify() compared
    our body against our own text — a gate checking itself."""
    # Preserved: our body is original against the PUBLISHER's words.
    assert body_state.classify(OURS * 2, "", PUBLISHER) == body_state.ORIGINAL
    # Destroyed: the reference is now our own summary, so the same body is
    # judged a reproduction of itself.
    assert body_state.classify(OURS * 2, "", OURS) == body_state.SOURCE_TEXT


def test_a_retry_still_has_material_when_the_publisher_text_survives():
    from ai_processor import _SAFE_BODY, _SAFE_SUMMARY
    # The row after a failed pass: stub body, stub summary, blurb intact.
    assert body_state.has_usable_source(_SAFE_SUMMARY, PUBLISHER, _SAFE_BODY)
    mat = body_state.source_material("A headline", _SAFE_SUMMARY, PUBLISHER,
                                     _SAFE_BODY)
    assert "OPEC+ delegates" in mat


def test_a_row_whose_blurb_was_overwritten_has_nothing_left():
    """What the old code produced, and why those rows now report
    no_source_material: this state is unrecoverable without re-ingest."""
    from ai_processor import _SAFE_BODY, _SAFE_SUMMARY
    # source_summary was replaced by our summary, then the pass failed and put
    # the stub back into body and summary. Nothing publisher-written remains.
    assert not body_state.has_usable_source(_SAFE_SUMMARY, _SAFE_SUMMARY,
                                            _SAFE_BODY)


# ─── end to end ─────────────────────────────────────────────────────────────

def test_a_full_ai_pass_leaves_the_publisher_text_untouched(tmp_path,
                                                            monkeypatch):
    """The property that matters, through the real run_ai_batch."""
    import main
    db = str(tmp_path / "c.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    conn.execute(
        "INSERT INTO articles (url, headline, source_headline, full_body,"
        " summary_60, source_summary, status, pillar_id, published_at)"
        " VALUES ('u','Crude climbs as OPEC+ weighs cuts','Wire copy',?,?,?,"
        "'pending_rewrite',2,'2026-08-31T10:00:00+00:00')",
        (PUBLISHER, PUBLISHER[:120], PUBLISHER))
    conn.commit()

    # Patch main.process_batch — the seam run_ai_batch actually calls.
    #
    # Patching ai_processor._call_cascade looks equivalent and is not: several
    # other test modules importlib.reload(main), and under the full suite
    # `main.process_batch is ai_processor.process_batch` comes out False, so a
    # patch applied to ai_processor never reaches the function run_ai_batch
    # invokes. The test then silently measured the rule-based placeholder
    # instead of a rewrite — passing alone, failing in the suite.
    #
    # This is also the tighter contract to test: given results, does the write
    # path store them correctly AND leave source_summary alone.
    async def fake_batch(batch_input, concurrency=5):
        assert len(batch_input) == 1
        # What the pass hands the model must be the PUBLISHER's text.
        assert "OPEC+ delegates" in batch_input[0]["body"]
        return [{"refined_title": "Producers weigh a further reduction",
                 "summary": OURS, "full_body": OURS * 2, "category": "economy",
                 "topic_tags": ["OPEC"], "is_trending": False,
                 "sentiment": "neutral", "when_info": "Monday",
                 "where_info": "Not specified"}]

    monkeypatch.setattr(main, "process_batch", fake_batch)
    # run_ai_batch returns 0 before doing anything when no provider is
    # configured — correct behaviour (rewriting without one only rewrites the
    # placeholder), but it means the test measures nothing unless satisfied.
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "gemini": 1,
                                 "total_keys": 1, "model": "test"})

    import asyncio
    refined = asyncio.run(main.run_ai_batch(conn))

    # PROVE THE PASS ACTUALLY RAN before asserting what it preserved. A test
    # that skips the work and then finds the column unchanged proves nothing —
    # of course it is unchanged if nothing touched it.
    assert refined == 1, f"run_ai_batch refined {refined} rows, expected 1"

    got = conn.execute("SELECT headline, summary_60, full_body, source_summary,"
                       " ai_processed FROM articles WHERE url='u'").fetchone()
    conn.close()
    assert got["ai_processed"] == 1, "the row was not processed"
    assert got["summary_60"] == OURS, "our summary was not written"
    assert got["full_body"] == OURS * 2, "our body was not written"
    assert got["source_summary"] == PUBLISHER, \
        "the publisher's text was overwritten by the AI pass"
