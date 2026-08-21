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
    Row, is_postgres_url, qmark_to_pyformat, sanitize_dsn, translate,
)


# ─── placeholders ─────────────────────────────────────────────────────────────
def test_qmarks_become_pyformat_placeholders():
    """psycopg speaks %s. $1 is asyncpg's syntax and psycopg does not see it as a
    placeholder at all — it reports "0 placeholders but N parameters were
    passed", so every parameterised query fails while DDL keeps working."""
    assert qmark_to_pyformat("SELECT * FROM a WHERE x=? AND y=?") == \
        "SELECT * FROM a WHERE x=%s AND y=%s"


def test_no_dollar_placeholders_are_ever_emitted():
    out = translate("SELECT COUNT(*) as c FROM articles WHERE pillar_id=?")
    assert "$1" not in out and "%s" in out


def test_a_question_mark_inside_a_literal_is_not_a_placeholder():
    """A LIKE pattern or a seeded headline can contain '?'. Turning it into a
    placeholder adds one psycopg then demands a parameter for."""
    out = qmark_to_pyformat("SELECT * FROM a WHERE h LIKE '%what?%' AND id=?",
                            escape_percent=False)
    assert out == "SELECT * FROM a WHERE h LIKE '%what?%' AND id=%s"


def test_escaped_quotes_inside_a_literal_do_not_end_it():
    out = qmark_to_pyformat("SELECT * FROM a WHERE h='it''s a ? really' AND id=?")
    assert out.endswith("id=%s")
    assert "it''s a ? really" in out


def test_double_quoted_identifiers_are_left_alone():
    out = qmark_to_pyformat('SELECT "we?rd" FROM a WHERE id=?')
    assert out == 'SELECT "we?rd" FROM a WHERE id=%s'


# ─── the percent sign, which is the other half of pyformat ───────────────────
def test_a_literal_percent_is_doubled_when_parameters_are_bound():
    """psycopg scans the whole query for % when binding, so LIKE '%sherrbyte%'
    reads as a malformed placeholder unless it is doubled."""
    out = translate("SELECT * FROM a WHERE u NOT LIKE '%sherrbyte%' AND id=?",
                    bind=True)
    assert "'%%sherrbyte%%'" in out and "id=%s" in out


def test_a_literal_percent_is_left_alone_with_no_parameters():
    """With no parameters psycopg does no interpolation, so doubling would put a
    literal %% into the SQL and the LIKE would stop matching."""
    out = translate("UPDATE a SET image_url='' WHERE image_url NOT LIKE '%sherrbyte%'",
                    bind=False)
    assert "'%sherrbyte%'" in out and "%%" not in out


def test_like_patterns_passed_as_parameters_need_no_escaping_in_the_sql():
    out = translate("SELECT * FROM a WHERE headline LIKE ? OR summary_60 LIKE ?")
    assert out.count("%s") == 2 and "%%" not in out


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
    assert "(now() + %s::interval)::text" in out
    assert out.count("%s") == 2                 # both placeholders survive


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
    assert "VALUES(%s, %s)" in out


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


# ─── the connection surface main.py actually calls ────────────────────────────
def test_conn_exposes_cursor():
    """init_db() seeds the topics table through conn.cursor(). Without it the
    very first boot on Postgres dies with AttributeError before a single row is
    written."""
    from pgcompat import Conn
    assert callable(getattr(Conn, "cursor", None))


@pytest.mark.parametrize("name", ["execute", "executemany", "executescript",
                                  "cursor", "commit", "rollback", "close"])
def test_conn_covers_every_method_main_uses(name):
    """Discovering these one AttributeError per deploy is a slow way to find them.
    grep for `conn.<method>(` in main.py produced exactly this list."""
    from pgcompat import Conn
    assert callable(getattr(Conn, name, None)), name


class _FakeConn:
    def __init__(self):
        self.seen = []

    def execute(self, sql, params=()):
        from pgcompat import Cursor, Row
        self.seen.append((sql, params))
        return Cursor([Row({"id": len(self.seen)})], 1, len(self.seen))

    def executemany(self, sql, seq):
        from pgcompat import Cursor
        return Cursor([], len(list(seq)), None)


def test_a_cursor_can_be_reused_across_executes():
    """sqlite3 lets you hold one cursor and execute on it repeatedly, which is
    exactly what the topics seed loop does."""
    from pgcompat import ReusableCursor
    fake = _FakeConn()
    cur = ReusableCursor(fake)
    for i in range(3):
        cur.execute("INSERT INTO topics(name) VALUES(?)", (f"t{i}",))
    assert len(fake.seen) == 3
    assert cur.rowcount == 1 and cur.lastrowid == 3


def test_the_cursor_result_set_refreshes_between_executes():
    from pgcompat import ReusableCursor
    cur = ReusableCursor(_FakeConn())
    cur.execute("SELECT 1")
    first = cur.fetchone()["id"]
    cur.execute("SELECT 1")
    assert cur.fetchone()["id"] != first


def test_a_fresh_cursor_reports_no_rows_rather_than_raising():
    from pgcompat import ReusableCursor
    cur = ReusableCursor(_FakeConn())
    assert cur.fetchone() is None and cur.fetchall() == []


# ─── ALTER TABLE ADD COLUMN is re-run on every boot by design ─────────────────
def test_add_column_becomes_idempotent():
    """_MIGRATIONS replays every ADD COLUMN on each boot and relies on the error
    being swallowed. On Postgres that is a DuplicateColumn exception per column,
    per boot, filling the logs."""
    out = translate("ALTER TABLE articles ADD COLUMN status TEXT DEFAULT 'published'")
    assert out == ("ALTER TABLE articles ADD COLUMN IF NOT EXISTS status "
                   "TEXT DEFAULT 'published'")


def test_add_column_guard_is_not_applied_twice():
    sql = "ALTER TABLE a ADD COLUMN IF NOT EXISTS x TEXT"
    assert translate(sql) == sql


def test_add_column_is_matched_case_insensitively():
    assert "ADD COLUMN IF NOT EXISTS y" in translate(
        "ALTER TABLE a add column y INTEGER DEFAULT 0")


# ─── the app's tables must not collide with the engine's ─────────────────────
def test_the_app_uses_its_own_schema():
    """Supabase already holds an `articles` table — the engine's, with a totally
    different shape. Sharing the public schema means CREATE TABLE IF NOT EXISTS
    silently binds to it and every later query fails on a missing column."""
    import pgcompat
    assert pgcompat.APP_SCHEMA and pgcompat.APP_SCHEMA != "public"


def test_search_path_excludes_public_so_the_engine_cannot_shadow_us():
    """Assert on the statement, not the source: naming public in the search_path
    is what lets public.articles (the engine's, different shape) answer an
    unqualified `articles` here."""
    import pgcompat
    assert pgcompat.SEARCH_PATH_SQL == f'SET search_path TO "{pgcompat.APP_SCHEMA}"'
    assert "public" not in pgcompat.SEARCH_PATH_SQL
    assert pgcompat.CREATE_SCHEMA_SQL.startswith("CREATE SCHEMA IF NOT EXISTS")


def test_every_pooled_connection_gets_the_search_path():
    """The transaction pooler hands out a different backend each checkout, and a
    session SET does not follow the app — configuring only the first connection
    would leave later ones pointed at public."""
    import inspect

    import pgcompat
    assert "configure=_configure" in inspect.getsource(pgcompat._get_pool)


# ─── end to end: the SQL and the parameters must agree at the driver ──────────
class _PsycopgLike:
    """Stands in for a psycopg cursor, enforcing the check that produced the
    reported failure: with parameters, %% is consumed as an escaped literal and
    the remaining %s must match the parameter count exactly. With params=None
    psycopg does no placeholder processing at all, so the query goes verbatim."""

    description = [("c",)]
    rowcount = 1

    def __init__(self, log):
        self._log = log

    def execute(self, q, p=None):
        self._log.append((q, p))
        if p is None:
            return
        n = q.replace("%%", "").count("%s")
        if n != len(p):
            raise AssertionError(
                f"query has {n} placeholders but {len(p)} parameters: {q}")

    def executemany(self, q, seq):
        seq = list(seq)
        self.execute(q, seq[0] if seq else None)

    def fetchall(self):
        return [{"c": 7}]


@pytest.fixture
def conn(monkeypatch):
    import pgcompat
    log = []

    class Raw:
        def cursor(self):
            return _PsycopgLike(log)

    class CM:
        def __enter__(self):
            return Raw()

        def __exit__(self, *a):
            return False

    class Pool:
        def connection(self):
            return CM()

    monkeypatch.setattr(pgcompat, "_pool", Pool())
    c = pgcompat.connect("postgresql://stub")
    c.log = log
    return c


def test_the_reported_query_now_binds(conn):
    """main.py:2637 — the failure that started this: 0 placeholders, 1 param."""
    assert conn.execute(
        "SELECT COUNT(*) as c FROM articles WHERE pillar_id=?", (3,)
    ).fetchone()["c"] == 7
    assert conn.log[-1][0].endswith("pillar_id=%s")


def test_a_literal_percent_alongside_a_parameter(conn):
    """'%sherrbyte%' contains the literal sequence %s. Undoubled, psycopg counts
    it as a placeholder and the parameter count no longer matches."""
    conn.execute("SELECT * FROM articles WHERE image_url NOT LIKE '%sherrbyte%' "
                 "AND id=?", (1,))
    assert "'%%sherrbyte%%'" in conn.log[-1][0]


def test_a_literal_percent_with_no_parameters_is_sent_verbatim(conn):
    """Doubling here would put a literal %% into the pattern and the LIKE would
    stop matching — psycopg skips processing entirely when params is None."""
    conn.execute("UPDATE articles SET image_url='' "
                 "WHERE image_url NOT LIKE '%sherrbyte%'")
    q, p = conn.log[-1]
    assert p is None and "'%sherrbyte%'" in q and "%%" not in q


def test_like_patterns_bound_as_parameters(conn):
    conn.execute("SELECT * FROM articles WHERE headline LIKE ? OR summary_60 LIKE ?",
                 ("%rbi%", "%rbi%"))
    assert conn.log[-1][0].count("%s") == 2


def test_interval_arithmetic_keeps_its_parameters_aligned(conn):
    conn.execute("SELECT * FROM articles WHERE published_at >= datetime('now', ?) "
                 "AND scope=?", ("-7 days", "global"))
    assert "(now() + %s::interval)::text" in conn.log[-1][0]


def test_insert_or_ignore_binds(conn):
    conn.execute("INSERT OR IGNORE INTO topics (name, slug) VALUES (?,?)",
                 ("AI", "ai"))
    q = conn.log[-1][0]
    assert "VALUES (%s,%s)" in q and q.endswith("ON CONFLICT DO NOTHING")


def test_the_cursor_wrapper_translates_too(conn):
    """conn.cursor().execute() is a second path to the driver, and it has to
    translate exactly as conn.execute() does."""
    conn.cursor().execute("INSERT INTO topics (name, slug) VALUES (?,?)",
                          ("AI", "ai"))
    assert "VALUES (%s,%s)" in conn.log[-1][0]


def test_executemany_translates_too(conn):
    conn.executemany("UPDATE articles SET status=? WHERE id=?",
                     [("published", 1), ("published", 2)])
    assert conn.log[-1][0] == "UPDATE articles SET status=%s WHERE id=%s"


# ─── activity.py shares the backend ───────────────────────────────────────────
def test_activity_follows_the_same_backend():
    """activity.py opened sqlite3 directly, so with DATABASE_URL set every
    screen-time row went to Render's ephemeral file — created fresh each deploy,
    read by nobody."""
    import inspect

    import activity
    src = inspect.getsource(activity._db)
    assert "pgcompat.connect" in src and "is_postgres_url" in src


# ─── Supabase's transaction pooler ────────────────────────────────────────────
def test_prepared_statements_are_disabled():
    """psycopg3 auto-prepares after a query's fifth execution and names them
    _pg3_0, _pg3_1, … The pooler multiplexes clients over fewer backends, so the
    name gets prepared on one and re-used against another — DuplicatePreparedStatement,
    surfacing only once an endpoint has been hit five times."""
    import pgcompat
    assert pgcompat.PREPARE_THRESHOLD is None


def test_the_threshold_is_none_and_not_zero():
    """0 means "prepare immediately" in psycopg — the exact opposite of what is
    wanted here. Only None disables preparation, and 0 is the plausible typo."""
    import pgcompat
    assert pgcompat.PREPARE_THRESHOLD is None
    assert not isinstance(pgcompat.PREPARE_THRESHOLD, int)


def test_the_pool_passes_the_threshold_to_every_connection():
    import inspect

    import pgcompat
    src = inspect.getsource(pgcompat._get_pool)
    assert '"prepare_threshold": PREPARE_THRESHOLD' in src


def test_configure_also_disables_preparation_per_connection():
    """Belt and braces: a connection made outside the pool kwargs still must not
    prepare."""
    import inspect

    import pgcompat
    assert "prepare_threshold" in inspect.getsource(pgcompat._configure)


def test_search_path_is_a_startup_option_not_only_a_runtime_set():
    """In transaction mode the backend is bound to the client only for the length
    of a transaction, and with autocommit every statement is its own transaction —
    so a session-level SET can be lost the moment the next statement lands on a
    different backend. As a libpq startup option it is part of the connection's
    identity and pgbouncer keys its pools by it."""
    import inspect

    import pgcompat
    assert pgcompat.CONN_OPTIONS == f"-c search_path={pgcompat.APP_SCHEMA}"
    assert '"options": CONN_OPTIONS' in inspect.getsource(pgcompat._get_pool)


def test_configure_sets_prepare_threshold_before_running_any_sql():
    """_configure runs statements itself; disabling preparation after them would
    leave those first statements eligible."""
    import pgcompat

    class FakeConn:
        def __init__(self):
            self.order = []
            self._pt = "unset"

        @property
        def prepare_threshold(self):
            return self._pt

        @prepare_threshold.setter
        def prepare_threshold(self, v):
            self._pt = v
            self.order.append("threshold")

        def execute(self, q):
            self.order.append("sql")

    c = FakeConn()
    pgcompat._configure(c)
    assert c.prepare_threshold is None
    assert c.order[0] == "threshold"


def test_configure_survives_a_connection_that_rejects_the_attribute():
    """Older/odd drivers may not expose it; the schema setup must still run."""
    import pgcompat

    class Stubborn:
        def __init__(self):
            self.sql = []

        def __setattr__(self, k, v):
            if k == "prepare_threshold":
                raise AttributeError("read-only")
            object.__setattr__(self, k, v)

        def execute(self, q):
            self.sql.append(q)

    s = Stubborn()
    pgcompat._configure(s)
    assert len(s.sql) == 2
