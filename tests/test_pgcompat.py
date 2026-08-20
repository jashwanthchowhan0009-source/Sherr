"""
The sqlite -> Postgres translation layer.

main.py is written against sqlite3 and stays that way; pgcompat adapts the driver
to the code. That means every translation here is load-bearing for ~80 untouched
call sites, and a subtly wrong rewrite fails at runtime against a database this
environment cannot reach. So the rewrites are pinned here instead.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pgcompat import (  # noqa: E402
    Row, is_postgres_url, qmark_to_dollar, sanitize_dsn, translate,
)


# ─── placeholders ─────────────────────────────────────────────────────────────
def test_qmarks_become_numbered_parameters():
    assert qmark_to_dollar("SELECT * FROM a WHERE x=? AND y=?") == \
        "SELECT * FROM a WHERE x=$1 AND y=$2"


def test_a_question_mark_inside_a_literal_is_not_a_placeholder():
    """A LIKE pattern or a seeded headline can contain '?'. Renumbering it
    silently shifts every following parameter by one."""
    out = qmark_to_dollar("SELECT * FROM a WHERE h LIKE '%what?%' AND id=?")
    assert out == "SELECT * FROM a WHERE h LIKE '%what?%' AND id=$1"


def test_escaped_quotes_inside_a_literal_do_not_end_it():
    out = qmark_to_dollar("SELECT * FROM a WHERE h='it''s a ? really' AND id=?")
    assert out.endswith("id=$1")
    assert "it''s a ? really" in out


def test_double_quoted_identifiers_are_left_alone():
    out = qmark_to_dollar('SELECT "we?rd" FROM a WHERE id=?')
    assert out == 'SELECT "we?rd" FROM a WHERE id=$1'


# ─── schema ───────────────────────────────────────────────────────────────────
def test_autoincrement_becomes_serial():
    assert "SERIAL PRIMARY KEY" in translate("id INTEGER PRIMARY KEY AUTOINCREMENT")
    assert "AUTOINCREMENT" not in translate("id INTEGER PRIMARY KEY AUTOINCREMENT")


def test_a_text_timestamp_default_is_cast_to_text():
    """Every timestamp column in this schema is TEXT holding an ISO string.
    Postgres rejects a timestamptz default on one, so CREATE TABLE fails outright
    without the cast — and that is the whole schema, not one column."""
    out = translate("published_at TEXT DEFAULT (datetime('now'))")
    assert out == "published_at TEXT DEFAULT (now()::text)"


# ─── date arithmetic ──────────────────────────────────────────────────────────
def test_datetime_with_a_bound_modifier_becomes_interval_arithmetic():
    """datetime('now', ?) with '-7 days' is a date computation, not a constant.
    Collapsing it to now() would silently return today's rows only."""
    out = translate("SELECT * FROM a WHERE published_at >= datetime('now', ?) AND x=?")
    assert "(now() + $1::interval)::text" in out
    assert "x=$2" in out                       # numbering survives the rewrite


def test_datetime_with_a_literal_modifier_keeps_the_interval():
    out = translate("SELECT * FROM a WHERE t >= datetime('now', '-30 days')")
    assert "interval '-30 days'" in out


def test_the_two_argument_form_is_matched_before_the_one_argument_form():
    """Rewritten in the wrong order the modifier is left dangling as a stray
    parameter, which is a syntax error at the database, not here."""
    out = translate("SELECT datetime('now', ?), datetime('now')")
    assert "::interval" in out and "now()::text" in out
    assert "'now'" not in out


# ─── upserts ──────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("verb", ["IGNORE", "REPLACE"])
def test_insert_or_x_becomes_on_conflict_do_nothing(verb):
    out = translate(f"INSERT OR {verb} INTO feeds(user_id, article_id) VALUES(?, ?)")
    assert out.startswith("INSERT INTO feeds")
    assert out.endswith("ON CONFLICT DO NOTHING")
    assert "VALUES($1, $2)" in out


def test_a_plain_insert_gains_no_conflict_clause():
    """ON CONFLICT DO NOTHING on an ordinary INSERT would swallow real integrity
    errors instead of surfacing them."""
    assert "ON CONFLICT" not in translate("INSERT INTO articles(url) VALUES(?)")


def test_a_trailing_semicolon_does_not_strand_the_conflict_clause():
    out = translate("INSERT OR IGNORE INTO t(a) VALUES(?);")
    assert out.endswith("ON CONFLICT DO NOTHING")


# ─── DSN handling ─────────────────────────────────────────────────────────────
def test_pooler_only_params_are_stripped():
    """Supabase hands out ?pgbouncer=true&sslmode=require; the drivers raise on
    both, so a copy-pasted dashboard URL fails before it connects."""
    out = sanitize_dsn("postgresql://u:p@h:6543/db?pgbouncer=true&sslmode=require&x=1")
    assert "pgbouncer" not in out and "sslmode" not in out
    assert "x=1" in out


def test_a_dsn_without_a_query_is_untouched():
    dsn = "postgresql://u:p@h:5432/db"
    assert sanitize_dsn(dsn) == dsn


@pytest.mark.parametrize("url,expected", [
    ("postgres://h/db", True), ("postgresql://h/db", True),
    ("sherbyte.db", False), ("", False), ("   ", False),
])
def test_backend_is_chosen_from_the_url(url, expected):
    assert is_postgres_url(url) is expected


# ─── row access ───────────────────────────────────────────────────────────────
def test_rows_answer_to_both_name_and_position():
    """sqlite3.Row supports both, and main.py uses both."""
    r = Row({"id": 7, "headline": "x"})
    assert r["id"] == 7 and r[0] == 7
    assert r["headline"] == "x" and r[1] == "x"


def test_row_keys_preserve_column_order():
    assert list(Row({"b": 1, "a": 2}).keys()) == ["b", "a"]


def test_row_supports_the_membership_check_main_uses():
    """`"source_name" in row.keys()` guards the aggregator credit line."""
    r = Row({"source_name": "Reuters"})
    assert "source_name" in r.keys()
    assert "url" not in r.keys()
