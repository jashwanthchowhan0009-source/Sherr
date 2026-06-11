"""
identity.py — pure helpers for @usernames.

No DB, no FastAPI — just normalization/validation so it can be unit-tested and
reused by auth + profile endpoints. Uniqueness against the database is enforced
by the caller (and a UNIQUE index), not here.
"""

from __future__ import annotations

import re

# 3–20 chars, lowercase letters / digits / underscore. Display adds the leading @.
_USERNAME_RE = re.compile(r"^[a-z0-9_]{3,20}$")

# Handles we never hand out to users (routes, brand, ambiguous words).
RESERVED_USERNAMES = {
    "admin", "administrator", "root", "sherr", "sherrbyte", "support", "help",
    "api", "me", "you", "null", "undefined", "user", "users", "login", "signup",
    "auth", "settings", "profile", "system", "moderator", "mod", "official",
}


def normalize_username(raw: str) -> str:
    """Lowercase, strip whitespace and a leading @. Does NOT validate length."""
    return (raw or "").strip().lstrip("@").lower()


def valid_username(raw: str) -> bool:
    """True if `raw` is a well-formed, non-reserved username."""
    u = normalize_username(raw)
    return bool(_USERNAME_RE.fullmatch(u)) and u not in RESERVED_USERNAMES


def username_from_email(email: str) -> str:
    """Derive a sensible base handle from an email local-part. Always returns a
    string that satisfies the length rules (3–18 chars; suffixing for uniqueness
    is the caller's job)."""
    local = (email or "").split("@")[0].lower()
    base = re.sub(r"[^a-z0-9_]", "", local)[:18]
    if len(base) < 3:
        base = (base + "user")[:18]
    return base or "user"
