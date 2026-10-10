"""Deleting an account must actually delete it.

The client has shown a "Delete Account" row, a type-DELETE-to-confirm modal and
an "Account deleted" toast since launch. It called `POST /account/delete`, which
did not exist: the 404 landed in a bare `catch(e){}`, the toast fired anyway, and
the email, password hash, bookmarks, comments and push tokens all stayed in the
database. The reader was told their data was gone while it was not.

That is the claim Google Play's User Data policy requires an app with accounts to
honour, so these tests drive the real endpoint over a live app and assert the
rows are gone — not that a route exists.

Two properties beyond the delete itself:

  * the session dies with the account. `_token_version` used to read a MISSING
    row as version 0, which is what most tokens were minted under, so every
    token a deleted account held kept verifying.
  * an unreadable database still FAILS OPEN (see test_token_revocation.py). The
    deleted-row answer must not be confused with the cannot-read answer.

Same harness as tests/test_token_revocation.py: no importlib.reload (CLAUDE.md),
module-level DB_PATH/USE_POSTGRES monkeypatched, since get_db() reads both at
call time.
"""
import importlib
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
main = importlib.import_module("main")


@pytest.fixture()
def app_db(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "USE_POSTGRES", False)
    monkeypatch.setattr(main, "DB_PATH", str(tmp_path / "deletion.db"))
    main._TOKEN_VERSIONS.clear()
    main.init_db()
    yield TestClient(main.app)
    main._TOKEN_VERSIONS.clear()


def _signup(client, email="ada@example.com", password="hunter2hunter2"):
    r = client.post("/signup", json={"email": email, "password": password, "name": "Ada"})
    assert r.status_code == 200, r.text
    return r.json()


def _auth(token):
    return {"Authorization": "Bearer " + token}


def _count(table, uid):
    conn = main.get_db()
    try:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE user_id=?", (uid,)).fetchone()
        return row["c"]
    finally:
        conn.close()


def _user_rows(uid):
    conn = main.get_db()
    try:
        return conn.execute("SELECT COUNT(*) AS c FROM users WHERE id=?", (uid,)).fetchone()["c"]
    finally:
        conn.close()


# ── the endpoint exists and is wired to the path the client calls ───────────

def test_the_endpoint_the_client_calls_exists(app_db):
    """index.html posts to /account/delete. A 404 here is the original bug."""
    data = _signup(app_db)
    r = app_db.post("/account/delete", headers=_auth(data["token"]))
    assert r.status_code == 200, r.text
    assert r.json()["deleted"] is True


def test_the_client_posts_to_the_route_that_exists():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "index.html"), encoding="utf-8") as fh:
        html = fh.read()
    assert "'/account/delete'" in html
    with open(os.path.join(root, "main.py"), encoding="utf-8") as fh:
        src = fh.read()
    assert '@app.post("/account/delete")' in src


def test_delete_requires_a_session(app_db):
    assert app_db.post("/account/delete").status_code == 401
    assert app_db.post("/account/delete", headers=_auth("garbage")).status_code == 401


# ── the rows actually go ────────────────────────────────────────────────────

def test_the_user_row_is_gone(app_db):
    data = _signup(app_db)
    uid = data["user_id"]
    assert _user_rows(uid) == 1
    app_db.post("/account/delete", headers=_auth(data["token"]))
    assert _user_rows(uid) == 0


def test_the_email_is_released_for_reuse(app_db):
    """A half-delete that leaves the row makes the address unusable forever —
    users.email is UNIQUE, so signing up again would collide."""
    data = _signup(app_db, "ada@example.com")
    app_db.post("/account/delete", headers=_auth(data["token"]))
    again = app_db.post("/signup", json={"email": "ada@example.com",
                                         "password": "hunter2hunter2", "name": "Ada"})
    assert again.status_code == 200, again.text


def test_every_user_owned_table_is_emptied(app_db):
    """The reader's content, not just their login."""
    data = _signup(app_db)
    uid, tok = data["user_id"], data["token"]

    # Seed one row per table, built from the live schema rather than hardcoded
    # column names — the point is that EVERY listed table is covered, and a
    # hand-written INSERT silently stops covering one when a column is renamed.
    conn = main.get_db()
    try:
        for table in main._USER_OWNED_TABLES:
            info = conn.execute(f"PRAGMA table_info({table})").fetchall()
            cols = {r[1]: (r[2] or "").upper() for r in info}
            notnull = {r[1] for r in info if r[3] and r[1] != "id"}
            assert "user_id" in cols, f"{table} has no user_id — not a user-owned table"
            fields, values = ["user_id"], [uid]
            for name in notnull - {"user_id"}:
                fields.append(name)
                values.append(1 if ("INT" in cols[name] or "REAL" in cols[name]) else "x")
            ph = ",".join("?" for _ in fields)
            conn.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({ph})", values)
        conn.commit()
    finally:
        conn.close()

    for table in main._USER_OWNED_TABLES:
        assert _count(table, uid) == 1, f"{table} was not seeded — the test proves nothing"

    r = app_db.post("/account/delete", headers=_auth(tok))
    assert r.status_code == 200, r.text

    for table in main._USER_OWNED_TABLES:
        assert _count(table, uid) == 0, f"{table} still holds rows for the deleted account"


def test_the_list_covers_every_user_owned_table_in_the_schema():
    """Drift guard. `_USER_OWNED_TABLES` is a hand-written tuple, so a table
    added later is silently not deleted — which is how `password_resets` (added
    with the forgot-password flow) came within one merge of surviving every
    account deletion. Read the schema and require the tuple to cover it.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "main.py"), encoding="utf-8") as fh:
        src = fh.read()
    import re
    owned = set()
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\);", src, re.S):
        if re.search(r"^\s*user_id\b", m.group(2), re.M):
            owned.add(m.group(1))
    assert owned, "no user-owned tables found — did the schema style change?"
    missed = owned - set(main._USER_OWNED_TABLES)
    assert missed == set(), (
        f"these tables have a user_id but are never deleted: {sorted(missed)}. "
        "Add them to _USER_OWNED_TABLES or the rows outlive the account.")


def test_one_account_delete_does_not_touch_another(app_db):
    a = _signup(app_db, "a@example.com")
    b = _signup(app_db, "b@example.com")
    conn = main.get_db()
    try:
        conn.execute("INSERT INTO bookmarks (user_id, article_id) VALUES (?,?)", (b["user_id"], 7))
        conn.commit()
    finally:
        conn.close()

    app_db.post("/account/delete", headers=_auth(a["token"]))

    assert _user_rows(b["user_id"]) == 1
    assert _count("bookmarks", b["user_id"]) == 1
    assert app_db.get("/me", headers=_auth(b["token"])).status_code == 200


# ── the session dies with the account ───────────────────────────────────────

def test_the_token_stops_working_immediately(app_db):
    """Reading a missing row as version 0 kept the deleted account signed in."""
    data = _signup(app_db)
    tok = data["token"]
    assert app_db.get("/me", headers=_auth(tok)).status_code == 200

    app_db.post("/account/delete", headers=_auth(tok))

    assert main.verify_token(tok) is None, "a deleted account's token still verifies"
    assert app_db.get("/me", headers=_auth(tok)).status_code == 401


def test_the_refresh_token_cannot_resurrect_the_session(app_db):
    data = _signup(app_db)
    app_db.post("/account/delete", headers=_auth(data["token"]))
    r = app_db.post("/auth/refresh", json={"refresh_token": data["refresh_token"]})
    assert r.status_code == 401, "a deleted account must not be able to mint a new session"


def test_deleting_twice_is_not_a_500(app_db):
    data = _signup(app_db)
    assert app_db.post("/account/delete", headers=_auth(data["token"])).status_code == 200
    # The token is dead now, so the second tap is an ordinary 401.
    assert app_db.post("/account/delete", headers=_auth(data["token"])).status_code == 401


# ── the two answers that must stay distinct ─────────────────────────────────

def test_a_missing_row_rejects_but_an_unreadable_database_still_accepts(app_db, monkeypatch):
    """The deleted answer must not be built out of the cannot-read answer."""
    data = _signup(app_db)
    uid = data["user_id"]

    app_db.post("/account/delete", headers=_auth(data["token"]))
    main._TOKEN_VERSIONS.clear()
    assert main._token_version(uid) == main._TOKEN_VERSION_DELETED

    def boom():
        raise RuntimeError("remaining connection slots are reserved")

    main._TOKEN_VERSIONS.clear()
    monkeypatch.setattr(main, "get_db", boom)
    assert main._token_version(uid) is None, "an unreadable version must read as 'cannot check'"
    assert main.verify_token(data["token"]) == uid, "and that must still FAIL OPEN"


def test_the_first_registered_account_can_still_delete_itself(app_db):
    """`get_current_user` returns a bare 1 for anonymous readers, but nothing
    seeds a users row with that id — uid 1 is simply whoever signed up first.
    Refusing to delete it (an early version of this endpoint did) would strand
    that one real reader with no way out."""
    data = _signup(app_db)
    assert data["user_id"] == 1, "first signup should be uid 1 on a fresh database"
    r = app_db.post("/account/delete", headers=_auth(data["token"]))
    assert r.status_code == 200, r.text
    assert _user_rows(1) == 0
