"""
pgcompat.py — a sqlite3-shaped cursor over Postgres.

main.py is written against sqlite3: ~80 `conn.execute(sql, params)` call sites,
145 `?` placeholders, `row["col"]` access, `.lastrowid`, `INSERT OR IGNORE`,
`datetime('now')`. Rewriting all of that by hand is a large, mechanical change
with a lot of places to get one query subtly wrong, and there is no Postgres here
to test the result against.

So this adapts the driver to the code rather than the code to the driver. A
`Conn` exposes exactly the surface main.py already uses, and translates on the
way through. Every existing query keeps working unmodified; the only edit at the
call sites is none.

WHY THIS EXISTS AT ALL: Render's free tier has ephemeral disk, so sherbyte.db is
destroyed on every deploy and the feed resets to zero. That is not something the
app can work around — the storage has to move off local disk.

WHAT IS TRANSLATED
    ?                 -> %s          (skipping ? inside string literals)
    literal %         -> %%          (ONLY when parameters are bound)
    INSERT OR IGNORE  -> INSERT … ON CONFLICT DO NOTHING
    INSERT OR REPLACE -> INSERT … ON CONFLICT DO NOTHING   (see caveat below)
    datetime('now')   -> now()::text
    datetime('now',?) -> (now() + ?::interval)::text
    ADD COLUMN        -> ADD COLUMN IF NOT EXISTS
    AUTOINCREMENT     -> (dropped; the schema uses SERIAL)
    INTEGER PRIMARY KEY AUTOINCREMENT -> SERIAL PRIMARY KEY

PLACEHOLDER STYLE IS psycopg's, NOT asyncpg's. %s, not $1. They are different
drivers with different syntaxes, and $1 in a psycopg query is not a placeholder
at all — it reports "0 placeholders but N parameters were passed" and every
parameterised query in the app fails at runtime while DDL keeps working.

CAVEATS, stated rather than hidden:
  * INSERT OR REPLACE becomes DO NOTHING, not DO UPDATE, because the update
    clause needs the conflict target and the column list, which cannot be
    derived reliably from arbitrary SQL. The two call sites in main.py are
    feeds and user_preferences, both of which are recomputed wholesale, so
    ignoring a duplicate is correct there. A new caller must not assume upsert.
  * Everything is synchronous, on a small connection pool, because that is what
    the sqlite call sites expect. FastAPI already runs these in a threadpool.
  * Server-side prepared statements are disabled outright. Supabase's
    transaction pooler cannot support them, and psycopg's auto-preparation
    would otherwise start colliding after any query's fifth execution.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from collections.abc import Mapping
from urllib.parse import urlsplit, urlunsplit

log = logging.getLogger("sherbyte.pg")

_PG_PREFIXES = ("postgres://", "postgresql://")
# Supabase's pooled URL carries libpq-only options the drivers reject.
_STRIP_PARAMS = {"pgbouncer", "options", "sslmode", "connect_timeout",
                 "prepared_statement_cache_size", "statement_cache_size"}


def is_postgres_url(url: str) -> bool:
    return (url or "").strip().lower().startswith(_PG_PREFIXES)


def sanitize_dsn(url: str) -> str:
    parts = urlsplit(url)
    if not parts.query:
        return url
    kept = [kv for kv in parts.query.split("&")
            if kv and kv.split("=", 1)[0].lower() not in _STRIP_PARAMS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       "&".join(kept), parts.fragment))


# ─── SQL translation ──────────────────────────────────────────────────────────
def qmark_to_pyformat(sql: str, escape_percent: bool = True) -> str:
    """?  ->  %s, leaving ? inside quoted literals alone.

    psycopg speaks pyformat (%s). It does NOT understand $1/$2 — that is
    asyncpg's style, and feeding it to psycopg yields "the query has 0
    placeholders but N parameters were passed", because to psycopg there is no
    placeholder in the string at all.

    THE PERCENT SIGN IS THE OTHER HALF OF THIS. When psycopg binds parameters it
    scans the whole query for %, so a literal one — `LIKE '%sherrbyte%'` — is read
    as a malformed placeholder. It has to be doubled. But only when binding:
    with no parameters psycopg does no interpolation at all, and doubling then
    would put a literal %% into the SQL. Hence the flag rather than always-on.
    """
    out, i, quote = [], 0, None
    pct = "%%" if escape_percent else "%"
    while i < len(sql):
        c = sql[i]
        if quote:
            # '' inside a single-quoted literal is an escaped quote, not the end
            if c == "'" == quote and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            if c == quote:
                quote = None
            out.append(pct if c == "%" else c)
        elif c in ("'", '"'):
            quote = c
            out.append(c)
        elif c == "?":
            out.append("%s")
        else:
            out.append(pct if c == "%" else c)
        i += 1
    return "".join(out)


# Order matters: the two-argument datetime() has to be rewritten before the
# one-argument form, or its modifier is left dangling.
_SUBS = (
    (re.compile(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.I), "INSERT INTO"),
    (re.compile(r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", re.I), "INSERT INTO"),
    (re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", re.I), "SERIAL PRIMARY KEY"),
    (re.compile(r"\bAUTOINCREMENT\b", re.I), ""),
    # datetime('now', '-7 days') is interval arithmetic, not a constant. sqlite's
    # modifier strings ('-7 days', '-12 hours') are already valid Postgres
    # interval literals, so the value passes straight through as a cast.
    (re.compile(r"datetime\(\s*'now'\s*,\s*\?\s*\)", re.I), "(now() + ?::interval)::text"),
    (re.compile(r"datetime\(\s*'now'\s*,\s*'([^']*)'\s*\)", re.I),
     r"(now() + interval '\1')::text"),
    # ::text, not a bare now(): every timestamp column in this schema is TEXT
    # holding an ISO string, and Postgres refuses a timestamptz default on one.
    (re.compile(r"datetime\(\s*'now'\s*\)", re.I), "now()::text"),
    (re.compile(r"\bCOLLATE\s+NOCASE\b", re.I), ""),
    # _MIGRATIONS re-runs every ADD COLUMN on every boot and relies on the error
    # being swallowed. Postgres has IF NOT EXISTS for exactly this, so the
    # statement becomes a no-op instead of an exception the logs fill up with.
    # Guarded against double-adding it if a caller already wrote one.
    (re.compile(r"\bADD\s+COLUMN\s+(?!IF\s+NOT\s+EXISTS)", re.I),
     "ADD COLUMN IF NOT EXISTS "),
)


# :name -> %(name)s. Skipped inside literals, and ::cast is not a placeholder.
_NAMED = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


def named_to_pyformat(sql: str) -> str:
    """:url -> %(url)s, for the callers that bind a dict instead of a tuple.

    sqlite3 accepts :name with a dict; psycopg wants %(name)s. _insert_with_dedup
    binds all nineteen article columns this way, so without this EVERY ingest
    insert raised, was swallowed by a debug-level except, and the collector
    logged "0 new articles inserted" forever while looking healthy.

    ::text and ::interval must survive — a Postgres cast is not a parameter.
    """
    out, i, quote = [], 0, None
    while i < len(sql):
        c = sql[i]
        if quote:
            if c == "'" == quote and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            if c == quote:
                quote = None
            out.append(c)
            i += 1
            continue
        if c in ("'", '"'):
            quote = c
            out.append(c)
            i += 1
            continue
        if c == ":":
            if sql[i:i + 2] == "::":              # a cast, not a placeholder
                out.append("::")
                i += 2
                continue
            m = _NAMED.match(sql, i)
            if m:
                out.append(f"%({m.group(1)})s")
                i = m.end()
                continue
        out.append(c)
        i += 1
    return "".join(out)


# A ';' only ends a statement outside string literals and comments.
#
# This was a plain script.split(";"), and the cost of that was a table. The
# comment above CREATE TABLE insights in main.py's CREATE_TABLES read
# "...runs on the Postgres stack; this table lets..." — so the split landed
# mid-comment, the next fragment began `this table lets the deployed app show
# it.) CREATE TABLE IF NOT EXISTS insights (...`, Postgres reported a syntax
# error at "this", and executescript's except swallowed it at debug level. The
# table was simply never created on Postgres, silently, and stayed missing until
# something queried it.
#
# Prose in a comment is not something a schema author should have to think about,
# so the splitter tracks state instead: '' inside a single-quoted literal is an
# escaped quote, not the end of it; -- runs to end of line; /* */ nests in
# Postgres. Dollar-quoting is not handled because nothing in this codebase's DDL
# uses it — add it here if a function body ever appears.
def split_statements(script: str) -> list:
    """Split a SQL script on statement-level semicolons only."""
    out, buf, i, n = [], [], 0, len(script)
    quote = None          # "'" or '"' when inside a literal/identifier
    line_comment = False
    block_depth = 0

    def flush():
        stmt = "".join(buf).strip()
        if stmt:
            out.append(stmt)
        buf.clear()

    while i < n:
        c = script[i]
        nxt = script[i + 1] if i + 1 < n else ""

        if line_comment:
            if c == "\n":
                line_comment = False
            buf.append(c)
            i += 1
            continue

        if block_depth:
            if c == "*" and nxt == "/":
                block_depth -= 1
                buf.append("*/")
                i += 2
                continue
            if c == "/" and nxt == "*":
                block_depth += 1
                buf.append("/*")
                i += 2
                continue
            buf.append(c)
            i += 1
            continue

        if quote:
            # '' inside a literal is an escaped quote, not its end.
            if c == quote and nxt == quote:
                buf.append(c + nxt)
                i += 2
                continue
            if c == quote:
                quote = None
            buf.append(c)
            i += 1
            continue

        if c == "-" and nxt == "-":
            line_comment = True
            buf.append("--")
            i += 2
            continue
        if c == "/" and nxt == "*":
            block_depth = 1
            buf.append("/*")
            i += 2
            continue
        if c in ("'", '"'):
            quote = c
            buf.append(c)
            i += 1
            continue
        if c == ";":
            flush()
            i += 1
            continue

        buf.append(c)
        i += 1

    flush()
    return out


def translate(sql: str, bind: bool = True) -> str:
    """Rewrite one statement for Postgres.

    `bind` says whether parameters will be passed with it, which decides only
    whether literal % is doubled — see qmark_to_pyformat.
    """
    had_or = re.search(r"\bINSERT\s+OR\s+(IGNORE|REPLACE)\b", sql, re.I)
    for rx, rep in _SUBS:
        sql = rx.sub(rep, sql)
    if had_or:
        # Appended rather than woven in: deriving a DO UPDATE target from
        # arbitrary SQL is not reliable, and a wrong conflict target is worse
        # than a skipped duplicate.
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    return qmark_to_pyformat(sql, escape_percent=bind)


class Row(dict):
    """dict that also answers row[0] — sqlite3.Row supports both."""

    __slots__ = ("_order",)

    def __init__(self, mapping):
        super().__init__(mapping)
        self._order = list(mapping.keys())

    def __getitem__(self, k):
        if isinstance(k, int):
            return super().__getitem__(self._order[k])
        return super().__getitem__(k)

    def keys(self):
        return self._order


class Cursor:
    def __init__(self, rows, rowcount, lastrowid):
        self._rows = rows
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def fetchmany(self, size=1):
        out, self._rows = self._rows[:size], self._rows[size:]
        return out

    def __iter__(self):
        return iter(self._rows)


class ReusableCursor:
    """What conn.cursor() hands back.

    sqlite3 lets you keep one cursor and call execute() on it repeatedly, which
    is what init_db does when seeding topics. Each execute replaces this
    cursor's result set, exactly as sqlite3 does, so the fetch* and rowcount
    surface stays honest between calls.
    """

    def __init__(self, conn: "Conn"):
        self._conn = conn
        self._last = Cursor([], -1, None)

    def execute(self, sql, params=()):
        self._last = self._conn.execute(sql, params)
        return self._last

    def executemany(self, sql, seq):
        self._last = self._conn.executemany(sql, seq)
        return self._last

    def fetchone(self):
        return self._last.fetchone()

    def fetchall(self):
        return self._last.fetchall()

    def fetchmany(self, size=1):
        return self._last.fetchmany(size)

    @property
    def rowcount(self):
        return self._last.rowcount

    @property
    def lastrowid(self):
        return self._last.lastrowid

    def close(self):
        return None

    def __iter__(self):
        return iter(self._last)


# ─── connection ───────────────────────────────────────────────────────────────
_pool = None
_pool_lock = threading.Lock()


# The app's tables live in their own Postgres schema.
#
# The Supabase database ALREADY has an `articles` table — the Sherr-I engine's,
# with a completely different shape (UUID id, title, body, processed). main.py's
# `articles` is a different table that happens to share a name: integer id,
# headline, full_body, status. Point main.py at the public schema and
# `CREATE TABLE IF NOT EXISTS articles` finds the engine's table, skips
# creation, and every subsequent query fails on a column that was never there.
#
# A dedicated schema keeps the two apart with no query changes at all: the
# search_path is set to this schema ALONE, so an unqualified `articles` can only
# ever mean this app's. The engine keeps talking to public.articles through its
# own asyncpg pool, untouched.
APP_SCHEMA = "sherrbyte_app"


CREATE_SCHEMA_SQL = f'CREATE SCHEMA IF NOT EXISTS "{APP_SCHEMA}"'
# This schema ALONE. Adding the public schema here is what would let the engine's
# same-named tables shadow this app's.
SEARCH_PATH_SQL = f'SET search_path TO "{APP_SCHEMA}"'


# Supabase's transaction pooler is the constraint behind everything below.
#
# PREPARED STATEMENTS. psycopg3 auto-prepares a query after the fifth execution
# and names them sequentially (_pg3_0, _pg3_1, …). The pooler multiplexes many
# client connections over fewer server backends, so the name is prepared on one
# backend and re-used against another that has never seen it — or worse, one
# that already holds a different statement under that name. That is
# DuplicatePreparedStatement, and it surfaces only after an endpoint has been
# hit five times, which is why it looks like a random runtime failure rather
# than a startup one. prepare_threshold=None turns auto-preparation off.
PREPARE_THRESHOLD = None

# SEARCH PATH. Setting it with a runtime SET is not durable here for the same
# reason: in transaction mode the server connection is only bound to the client
# for the length of a transaction, and with autocommit every statement is its
# own transaction, so a session-level SET can be lost the moment the next
# statement lands on a different backend. Passing it as a libpq startup option
# instead makes it part of the connection's identity — pgbouncer keys its pools
# by these options, so every backend serving this app starts with the schema
# already in place.
CONN_OPTIONS = f"-c search_path={APP_SCHEMA}"


def _configure(conn) -> None:
    """Belt and braces on top of CONN_OPTIONS, and where the schema is created."""
    try:
        conn.prepare_threshold = PREPARE_THRESHOLD
    except Exception:
        pass
    conn.execute(CREATE_SCHEMA_SQL)
    conn.execute(SEARCH_PATH_SQL)


# THE HARD CAP ON THIS PROCESS'S SHARE OF THE DATABASE.
#
# Supabase's transaction pooler and Render's free tier both allow connections in
# the low tens, TOTAL, across every client. An uncapped pool does not degrade
# under a reader crowd — it takes every slot, and then the scheduler, the admin
# endpoints and the engine's own pool all fail to connect at once, which looks
# like the database is down when it is only fully booked.
#
# 8 is deliberately small: FastAPI runs these sync calls in a threadpool, so 8
# in-flight queries is already 8 concurrent readers reaching Postgres, and the
# read cache in cache.py is what absorbs the other 2,992. Raise PG_POOL_MAX only
# alongside a bigger database plan, never to make a slow endpoint feel faster.
POOL_MAX = int(os.getenv("PG_POOL_MAX", "8"))
# Queue rather than fail when all 8 are busy: a reader waiting 30s is bad, a
# reader getting a 500 because the 9th slot did not exist is worse.
POOL_TIMEOUT = float(os.getenv("PG_POOL_TIMEOUT", "30"))


def _get_pool(dsn: str):
    """A small psycopg pool, created once. Import is lazy so a sqlite-only
    deployment never needs the driver installed."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                from psycopg_pool import ConnectionPool          # noqa: PLC0415
                from psycopg.rows import dict_row                # noqa: PLC0415
                _pool = ConnectionPool(
                    sanitize_dsn(dsn), min_size=1, max_size=POOL_MAX,
                    kwargs={
                        "row_factory": dict_row,
                        "autocommit": True,
                        # Not prepared_statement_cache_size — that is asyncpg's
                        # name for this. psycopg spells it prepare_threshold, and
                        # None (not 0) is what disables preparation; 0 would mean
                        # "prepare immediately", the exact opposite.
                        "prepare_threshold": PREPARE_THRESHOLD,
                        "options": CONN_OPTIONS,
                    },
                    configure=_configure,
                    open=True, timeout=POOL_TIMEOUT,
                    max_waiting=int(os.getenv("PG_POOL_MAX_WAITING", "200")),
                )
    return _pool


class Conn:
    """The sqlite3 connection surface main.py actually uses.

    Autocommit is on and commit() is a no-op. main.py calls commit() in some
    paths and not others — it relied on sqlite's implicit behaviour — so making
    every statement durable immediately is the only translation that preserves
    what the existing call sites expect.
    """

    def __init__(self, dsn: str):
        self._changes = 0
        self._pool = _get_pool(dsn)
        self._cm = self._pool.connection()
        self._conn = self._cm.__enter__()

    def execute(self, sql, params=()):
        # bind mirrors exactly what is handed to psycopg below. Getting these two
        # out of step is the whole bug class: translate(bind=True) with params=None
        # leaves a literal %% in the SQL, and bind=False with params breaks any
        # query containing a literal %.
        #
        # A Mapping means the caller used :name placeholders and psycopg wants the
        # dict passed through unchanged, not coerced to a tuple.
        if isinstance(params, Mapping):
            args = dict(params)
            q = named_to_pyformat(translate(sql, bind=True))
        else:
            args = tuple(params) if params else None
            q = translate(sql, bind=args is not None)
        cur = self._conn.cursor()
        try:
            cur.execute(q, args)
        except Exception as e:
            log.error("pg query failed: %s | %s", e, q[:200])
            raise
        self._changes += max(cur.rowcount or 0, 0)
        rows, last = [], None
        if cur.description:
            rows = [Row(r) for r in cur.fetchall()]
            # sqlite's lastrowid after an INSERT; emulated where the caller asked
            # for it via RETURNING id.
            if rows and "id" in rows[0]:
                last = rows[0]["id"]
        return Cursor(rows, cur.rowcount, last)

    def executemany(self, sql, seq):
        rows = [tuple(p) for p in seq]
        q = translate(sql, bind=True)          # executemany always binds
        cur = self._conn.cursor()
        cur.executemany(q, rows)
        return Cursor([], cur.rowcount, None)

    def executescript(self, script):
        # DDL only, split at statement level. Each runs independently so one
        # CREATE TABLE that already exists cannot abort the rest.
        for stmt in split_statements(script):
            try:
                # bind=False: DDL takes no parameters, so psycopg does no
                # interpolation and a literal % must stay a single %.
                self._conn.cursor().execute(translate(stmt, bind=False))
            except Exception as e:
                log.debug("ddl skipped: %s | %s", e, stmt[:90])
        return Cursor([], 0, None)

    @property
    def total_changes(self):
        """sqlite3.Connection.total_changes. _insert_with_dedup returns
        `conn.total_changes > 0` to report whether the row landed; without this
        it raised AttributeError inside the same swallowed except that hid the
        placeholder failure, so every insert silently reported "not inserted"."""
        return self._changes

    def cursor(self):
        """sqlite3.Connection.cursor(). init_db() seeds the topics table through
        one, so without this the very first boot on Postgres dies with
        AttributeError before a single row is written."""
        return ReusableCursor(self)

    def commit(self):
        return None            # autocommit

    def rollback(self):
        return None

    def close(self):
        try:
            self._cm.__exit__(None, None, None)
        except Exception:
            pass


def connect(dsn: str) -> Conn:
    return Conn(dsn)
