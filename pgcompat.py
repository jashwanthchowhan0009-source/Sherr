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
    ?              -> $1, $2, …      (skipping ? inside string literals)
    INSERT OR IGNORE  -> INSERT … ON CONFLICT DO NOTHING
    INSERT OR REPLACE -> INSERT … ON CONFLICT DO NOTHING   (see caveat below)
    datetime('now')   -> now()
    AUTOINCREMENT     -> (dropped; the schema uses SERIAL)
    INTEGER PRIMARY KEY AUTOINCREMENT -> SERIAL PRIMARY KEY

CAVEATS, stated rather than hidden:
  * INSERT OR REPLACE becomes DO NOTHING, not DO UPDATE, because the update
    clause needs the conflict target and the column list, which cannot be
    derived reliably from arbitrary SQL. The two call sites in main.py are
    feeds and user_preferences, both of which are recomputed wholesale, so
    ignoring a duplicate is correct there. A new caller must not assume upsert.
  * Everything is synchronous, on a small connection pool, because that is what
    the sqlite call sites expect. FastAPI already runs these in a threadpool.
"""

from __future__ import annotations

import logging
import re
import threading
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
def qmark_to_dollar(sql: str) -> str:
    """?  ->  $1, $2 …, leaving ? inside quoted literals alone.

    A naive replace corrupts any query containing a literal question mark, e.g.
    a LIKE pattern or a seeded headline.
    """
    out, n, i, quote = [], 0, 0, None
    while i < len(sql):
        c = sql[i]
        if quote:
            if c == quote:
                # '' inside a single-quoted literal is an escaped quote
                if c == "'" and i + 1 < len(sql) and sql[i + 1] == "'":
                    out.append("''")
                    i += 2
                    continue
                quote = None
            out.append(c)
        elif c in ("'", '"'):
            quote = c
            out.append(c)
        elif c == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(c)
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


def translate(sql: str) -> str:
    had_or = re.search(r"\bINSERT\s+OR\s+(IGNORE|REPLACE)\b", sql, re.I)
    for rx, rep in _SUBS:
        sql = rx.sub(rep, sql)
    if had_or:
        # Appended rather than woven in: deriving a DO UPDATE target from
        # arbitrary SQL is not reliable, and a wrong conflict target is worse
        # than a skipped duplicate.
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    return qmark_to_dollar(sql)


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


def _configure(conn) -> None:
    conn.execute(CREATE_SCHEMA_SQL)
    conn.execute(SEARCH_PATH_SQL)


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
                    sanitize_dsn(dsn), min_size=1, max_size=8,
                    kwargs={"row_factory": dict_row, "autocommit": True},
                    # Every pooled connection gets the search_path, not just the
                    # first: the transaction pooler hands out a different backend
                    # each time, and a session setting does not follow the app.
                    configure=_configure,
                    open=True, timeout=30,
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
        self._pool = _get_pool(dsn)
        self._cm = self._pool.connection()
        self._conn = self._cm.__enter__()

    def execute(self, sql, params=()):
        q = translate(sql)
        cur = self._conn.cursor()
        try:
            cur.execute(q, tuple(params) if params else None)
        except Exception as e:
            log.error("pg query failed: %s | %s", e, q[:200])
            raise
        rows, last = [], None
        if cur.description:
            rows = [Row(r) for r in cur.fetchall()]
            # sqlite's lastrowid after an INSERT; emulated where the caller asked
            # for it via RETURNING id.
            if rows and "id" in rows[0]:
                last = rows[0]["id"]
        return Cursor(rows, cur.rowcount, last)

    def executemany(self, sql, seq):
        q = translate(sql)
        cur = self._conn.cursor()
        cur.executemany(q, [tuple(p) for p in seq])
        return Cursor([], cur.rowcount, None)

    def executescript(self, script):
        # DDL only, and split on ';' at statement level. Each runs independently
        # so one CREATE TABLE that already exists cannot abort the rest.
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            try:
                self._conn.cursor().execute(translate(stmt))
            except Exception as e:
                log.debug("ddl skipped: %s | %s", e, stmt[:90])
        return Cursor([], 0, None)

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
