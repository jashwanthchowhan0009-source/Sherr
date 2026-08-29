"""
timestamps.py — one canonical form for articles.published_at.

THE COLUMN CARRIED FOUR FORMATS, and two separate bugs came out of it.

    ingest, no feed date   datetime.now().isoformat()          2026-08-29T18:00:00.9
    ingest, with feed date datetime(*parsed[:6]).isoformat()   2026-08-29T18:00:00
    NewsAPI                a["publishedAt"]                    2026-08-29T18:00:00Z
    schema default         datetime('now') -> now()::text      2026-08-29 18:00:00+00

BUG 1 — SORTING. `ORDER BY published_at DESC` on a TEXT column is a byte
comparison. 'T' is 0x54 and ' ' is 0x20, so within any given day EVERY 'T' row
sorts above EVERY space row regardless of the actual time. A six-hour-old
article outranks a four-hour-old one. This orders the Home feed, Bytes, trending,
search, the story thread and the recommender's candidate pull.

BUG 2 — AGE. A naive stamp has no offset, so `new Date("2026-08-29T18:00:00")`
is parsed by the browser as LOCAL time. The value was written as UTC. For a
reader in IST that adds 5h30m to every ingest-written article: anything over
about 18.5 real hours tips into "1d ago", which is why every card read the same.

Both are the same root cause, and one format fixes both:

    2026-08-29T18:00:00+00:00

UTC, explicit offset, 'T' separator, no microseconds. Fixed width, so it sorts
lexicographically against itself; explicit offset, so no parser has to guess.

No app imports — loaded by main.py and by scripts, same posture as body_state.py.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

# What a normalised value looks like. Used by the backfill to find rows that
# still need converting, and by the tests.
CANONICAL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$")

# Postgres renders a timestamptz with a space and a 2-digit offset; Python's
# fromisoformat before 3.11 rejects both, and 'Z' until 3.11 as well. Normalising
# the string before parsing is cheaper than version-gating.
_TZ_SHORT = re.compile(r"([+-]\d{2})$")


def parse(value) -> datetime | None:
    """Any of the four stored forms -> an aware UTC datetime. None if unusable.

    A naive value is assumed to be UTC. That is what the writers meant: the
    ingest path builds it from feedparser's UTC struct_time, and the schema
    default is now() on a UTC server. Assuming local would re-introduce the
    same offset error this module exists to remove.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo \
            else value.replace(tzinfo=timezone.utc)

    s = str(value).strip()
    if not s:
        return None
    s = s.replace(" ", "T", 1) if ("T" not in s and " " in s) else s
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    s = _TZ_SHORT.sub(r"\1:00", s)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Last resort: a bare date.
        try:
            dt = datetime.fromisoformat(s[:10])
        except ValueError:
            return None
    return dt.astimezone(timezone.utc) if dt.tzinfo \
        else dt.replace(tzinfo=timezone.utc)


def to_canonical(value) -> str:
    """Any stored form -> `2026-08-29T18:00:00+00:00`. "" when unparseable.

    Microseconds are dropped so every value is the same width. They would sort
    correctly anyway, but a fixed width means a human comparing two rows in psql
    is looking at the same thing the database is.
    """
    dt = parse(value)
    return dt.replace(microsecond=0).isoformat() if dt else ""


def now_canonical() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def is_canonical(value) -> bool:
    return bool(value) and bool(CANONICAL_RE.match(str(value)))


def to_iso(value) -> str:
    """For JSON. Accepts a datetime (once the column is timestamptz) or a string
    (while it is still TEXT), so the API emits one shape either way."""
    return to_canonical(value)
