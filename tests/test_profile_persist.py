"""Bio / name / @handle must persist server-side (and cross-device).

Two bugs made a profile "reset" on return:
  1. PUT /me used the lenient get_current_user, so an edit on a stale token was
     silently written to the anonymous row (uid=1) — the reader's own bio then
     read empty on their next /me. It must require_user (401 → client refreshes).
  2. @username lived only in the browser (no users column), so it never crossed
     devices and the profile looked half-empty on a fresh sign-in. It is a real
     column now, saved by PUT /me and returned by GET /me.
"""
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")


def _read():
    with open(MAIN, encoding="utf-8") as fh:
        return fh.read()


def test_put_me_requires_a_real_user():
    m = _read()
    block = m[m.index('@app.put("/me")'):m.index('@app.put("/me/topics")')]
    assert "uid = require_user(authorization)" in block
    assert "uid = get_current_user(" not in block   # the lenient path is the bug


def test_users_gets_a_username_column():
    m = _read()
    assert 'ALTER TABLE users ADD COLUMN username TEXT' in m


def test_put_me_persists_username_with_uniqueness():
    m = _read()
    block = m[m.index('@app.put("/me")'):m.index('@app.put("/me/topics")')]
    assert '"username"' in block
    assert "409" in block                     # taken handle rejected
    assert "bio" in block


def test_get_me_returns_username():
    m = _read()
    block = m[m.index('@app.get("/me")'):m.index('@app.put("/me")')]
    assert '"username"' in block
