"""Unit tests for the @username helpers (pure, no DB)."""

from app.identity import (
    RESERVED_USERNAMES, normalize_username, username_from_email, valid_username,
)


def test_normalize_strips_at_and_lowercases():
    assert normalize_username("  @Jashwanth ") == "jashwanth"
    assert normalize_username("@AB_cd") == "ab_cd"
    assert normalize_username(None) == ""


def test_valid_username_accepts_well_formed():
    assert valid_username("jashwanth")
    assert valid_username("@jash_01")
    assert valid_username("abc")            # exactly 3 chars
    assert valid_username("a" * 20)         # exactly 20 chars


def test_valid_username_rejects_bad_shapes():
    assert not valid_username("ab")                 # too short
    assert not valid_username("a" * 21)             # too long
    assert not valid_username("has space")          # space
    assert not valid_username("has-dash")           # dash
    assert not valid_username("emoji😀name")          # non-ascii
    assert not valid_username("")                   # empty


def test_valid_username_rejects_reserved():
    for name in ("admin", "ME", "@Sherr", "support"):
        assert not valid_username(name)
    assert "admin" in RESERVED_USERNAMES


def test_username_from_email_sanitizes_and_pads():
    assert username_from_email("jashwanth@gmail.com") == "jashwanth"
    assert username_from_email("a.b+tag@x.com") == "abtag"
    # very short local-parts get padded to satisfy the 3-char minimum
    assert len(username_from_email("a@x.com")) >= 3
    # garbage local-part falls back to a usable handle
    assert valid_username(username_from_email("a@x.com"))


def test_username_from_email_truncates_long_locals():
    out = username_from_email(("z" * 40) + "@x.com")
    assert len(out) <= 18
