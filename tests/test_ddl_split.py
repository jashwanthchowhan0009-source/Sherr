"""
Statement splitting in pgcompat.executescript.

This was `script.split(";")`, and it cost a table. The comment above CREATE TABLE
insights in main.py's CREATE_TABLES contained "...runs on the Postgres stack;
this table lets..." — so the split landed mid-comment, the next fragment began
`this table lets the deployed app show it.) CREATE TABLE ...`, Postgres reported
a syntax error at "this", and executescript's except swallowed it at debug level.
sherrbyte_app.insights was never created, silently, and /patterns/type/{ptype}
(which reads it through get_db()) could only ever 500.

Prose in a comment is not something a schema author should have to think about,
so these pin the splitter rather than the one comment that tripped it.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pgcompat  # noqa: E402


def test_a_semicolon_inside_a_line_comment_does_not_split_the_statement():
    """The exact bug."""
    sql = """
    -- the engine runs on the Postgres stack; this table lets the app show it.
    CREATE TABLE IF NOT EXISTS insights (id INTEGER);
    """
    out = pgcompat.split_statements(sql)
    assert len(out) == 1
    assert "CREATE TABLE IF NOT EXISTS insights" in out[0]
    assert not out[0].startswith("this table")


def test_the_real_schema_still_yields_one_intact_insights_statement():
    """A regression test against the live CREATE_TABLES, so re-introducing a
    semicolon in any comment fails here rather than in production."""
    import main
    frags = pgcompat.split_statements(main.CREATE_TABLES)
    creates = [f for f in frags if "CREATE TABLE IF NOT EXISTS insights" in f]
    assert len(creates) == 1
    assert creates[0].rstrip().endswith(")")
    # No fragment may begin with prose — that is the signature of a bad split.
    for f in frags:
        assert f.lstrip().startswith(("--", "/*", "CREATE", "PRAGMA")), f[:60]


def test_a_semicolon_inside_a_string_literal_does_not_split():
    sql = "INSERT INTO t (a) VALUES ('one; two'); INSERT INTO t (a) VALUES ('x')"
    out = pgcompat.split_statements(sql)
    assert len(out) == 2
    assert "'one; two'" in out[0]


def test_an_escaped_quote_does_not_end_the_literal_early():
    """'' inside a literal is one escaped quote, not a close followed by an open —
    getting this wrong would flip the parser's state for the rest of the script."""
    sql = "INSERT INTO t (a) VALUES ('it''s; fine'); CREATE TABLE u (id INT)"
    out = pgcompat.split_statements(sql)
    assert len(out) == 2
    assert out[1].startswith("CREATE TABLE u")


def test_a_semicolon_inside_a_block_comment_does_not_split():
    sql = "/* first; second */ CREATE TABLE t (id INT); CREATE TABLE u (id INT)"
    out = pgcompat.split_statements(sql)
    assert len(out) == 2


def test_nested_block_comments_close_correctly():
    """Postgres nests /* */, unlike C. Treating the first */ as the end would
    spill comment text into the next statement."""
    sql = "/* a /* b; */ c; */ CREATE TABLE t (id INT)"
    out = pgcompat.split_statements(sql)
    assert len(out) == 1
    assert "CREATE TABLE t" in out[0]


def test_a_quoted_identifier_protects_its_contents():
    sql = 'CREATE TABLE "odd;name" (id INT); CREATE TABLE t (id INT)'
    out = pgcompat.split_statements(sql)
    assert len(out) == 2
    assert '"odd;name"' in out[0]


def test_empty_fragments_and_trailing_semicolons_are_dropped():
    assert pgcompat.split_statements(";;  ;\n;") == []
    assert len(pgcompat.split_statements("CREATE TABLE t (id INT);;")) == 1


def test_a_final_statement_without_a_trailing_semicolon_is_not_lost():
    out = pgcompat.split_statements("CREATE TABLE t (id INT); CREATE TABLE u (id INT)")
    assert len(out) == 2
