"""Logout revokes server-side now, so these run the real thing.

Before `users.token_version`, "log out" could only delete the client's copy of a
stateless HMAC: the access token stayed valid for 30 days and the refresh token
for 180, on every device, whatever the reader did. tests/test_auth_session.py
covers the client half (it can only prove the device forgets). This covers the
half that actually revokes, against a live FastAPI app over a temp sqlite file.

Two properties matter as much as the revocation itself and each has a test:

  * a token minted BEFORE the column existed carries no "v" and must keep
    working — otherwise the deploy that adds revocation signs everybody out;
  * a database that cannot be read must FAIL OPEN. Failing closed would sign
    every reader out the moment Supabase refused a connection, which is a far
    worse outage than an unrevoked token living out its expiry.

No importlib.reload here (see CLAUDE.md): main is imported once and its
module-level DB_PATH/USE_POSTGRES are monkeypatched, which works because
get_db() reads both at call time.
"""
import base64
import importlib
import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
main = importlib.import_module("main")


@pytest.fixture()
def app_db(tmp_path, monkeypatch):
    """A live app on its own sqlite file, with the token-version cache cleared."""
    monkeypatch.setattr(main, "USE_POSTGRES", False)
    monkeypatch.setattr(main, "DB_PATH", str(tmp_path / "revocation.db"))
    main._TOKEN_VERSIONS.clear()
    main.init_db()
    yield TestClient(main.app)
    main._TOKEN_VERSIONS.clear()


def _signup(client, email="ada@example.com", password="hunter2hunter2"):
    r = client.post("/signup", json={"email": email, "password": password, "name": "Ada"})
    assert r.status_code == 200, r.text
    return r.json()


def _claims(token):
    raw = token.rsplit(".", 1)[0]
    return json.loads(base64.urlsafe_b64decode(raw + "=="))


def _auth(token):
    return {"Authorization": "Bearer " + token}


# ── the column and the claim ────────────────────────────────────────────────

def test_users_table_carries_a_token_version(app_db):
    conn = main.get_db()
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    finally:
        conn.close()
    assert "token_version" in cols


def test_every_issued_token_carries_the_version_it_was_minted_under(app_db):
    data = _signup(app_db)
    for field in ("token", "access_token", "refresh_token"):
        assert _claims(data[field])["v"] == 0
    assert _claims(data["refresh_token"])["typ"] == "refresh"


# ── revocation ──────────────────────────────────────────────────────────────

def test_logout_revokes_the_access_token_everywhere(app_db):
    data = _signup(app_db)
    token = data["token"]
    assert app_db.get("/me", headers=_auth(token)).status_code == 200

    assert app_db.post("/auth/logout", headers=_auth(token)).json()["token_version"] == 1

    # The same token — the copy another device still holds — is now dead.
    assert app_db.get("/me", headers=_auth(token)).status_code == 401


def test_logout_revokes_the_refresh_token_too(app_db):
    data = _signup(app_db)
    app_db.post("/auth/logout", headers=_auth(data["token"]))
    r = app_db.post("/auth/refresh", json={"refresh_token": data["refresh_token"]})
    assert r.status_code == 401, "a revoked refresh token must not mint a new session"


def test_signing_in_again_works_and_the_old_token_stays_dead(app_db):
    old = _signup(app_db)
    app_db.post("/auth/logout", headers=_auth(old["token"]))

    fresh = app_db.post("/login", json={"email": "ada@example.com",
                                        "password": "hunter2hunter2"})
    assert fresh.status_code == 200
    new = fresh.json()
    assert _claims(new["token"])["v"] == 1, "a new session is minted at the CURRENT version"
    assert app_db.get("/me", headers=_auth(new["token"])).status_code == 200
    assert app_db.get("/me", headers=_auth(old["token"])).status_code == 401


def test_refresh_mints_at_the_current_version(app_db):
    data = _signup(app_db)
    r = app_db.post("/auth/refresh", json={"refresh_token": data["refresh_token"]})
    assert r.status_code == 200
    rotated = r.json()
    assert _claims(rotated["access_token"])["v"] == 0
    assert app_db.get("/me", headers=_auth(rotated["access_token"])).status_code == 200


def test_repeated_logout_is_idempotent_not_an_error(app_db):
    data = _signup(app_db)
    first = app_db.post("/auth/logout", headers=_auth(data["token"]))
    assert first.status_code == 200 and first.json()["token_version"] == 1
    # The second tap carries a token that is already revoked: 401, not a 500.
    assert app_db.post("/auth/logout", headers=_auth(data["token"])).status_code == 401


def test_logout_requires_a_session(app_db):
    assert app_db.post("/auth/logout").status_code == 401
    assert app_db.post("/auth/logout", headers=_auth("garbage")).status_code == 401


def test_one_account_logout_does_not_touch_another(app_db):
    a = _signup(app_db, "a@example.com")
    b = _signup(app_db, "b@example.com")
    app_db.post("/auth/logout", headers=_auth(a["token"]))
    assert app_db.get("/me", headers=_auth(a["token"])).status_code == 401
    assert app_db.get("/me", headers=_auth(b["token"])).status_code == 200


# ── the two properties that must not regress ────────────────────────────────

def test_a_token_minted_before_the_column_existed_still_works(app_db):
    """The deploy that adds revocation must not sign everybody out."""
    _signup(app_db)
    legacy = json.dumps({
        "id": 1, "typ": "access",
        "exp": (main.datetime.now(main.timezone.utc) + main.timedelta(days=30)).isoformat(),
    })                                        # note: no "v" claim at all
    raw = base64.urlsafe_b64encode(legacy.encode()).decode()
    sig = main.hmac_module.new(main.JWT_SECRET.encode(), raw.encode(),
                               main.hashlib.sha256).hexdigest()
    assert main.verify_token(f"{raw}.{sig}") == 1


def test_an_unreadable_database_fails_open(app_db, monkeypatch):
    """A refused connection must not log the whole userbase out."""
    data = _signup(app_db)
    main._TOKEN_VERSIONS.clear()

    def boom():
        raise RuntimeError("remaining connection slots are reserved")

    monkeypatch.setattr(main, "get_db", boom)
    assert main._token_version(1) is None, "an unreadable version reads as 'cannot check'"
    assert main.verify_token(data["token"]) == 1, "and that must accept, not reject"


def test_the_version_is_cached_and_the_bump_refreshes_it(app_db):
    """verify_token runs on every authenticated request; it must not query every time."""
    data = _signup(app_db)
    main._TOKEN_VERSIONS.clear()

    reads = {"n": 0}
    real = main.get_db

    def counting():
        reads["n"] += 1
        return real()

    main.get_db = counting
    try:
        for _ in range(5):
            assert main.verify_token(data["token"]) == 1
        assert reads["n"] == 1, f"expected one cached read, made {reads['n']}"
    finally:
        main.get_db = real
    # The logout bump writes the new value straight into the cache, so the
    # revocation is live on this instance immediately, not after the TTL lapses.
    assert app_db.post("/auth/logout", headers=_auth(data["token"])).status_code == 200
    assert main._TOKEN_VERSIONS[1][0] == 1
    assert main.verify_token(data["token"]) is None


# ── wiring: the client half must actually call it ───────────────────────────

def test_the_client_revokes_before_it_clears():
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(_root, "index.html"), encoding="utf-8") as fh:
        html = fh.read()
    assert "function revokeSessionOnServer(token)" in html
    assert "'/auth/logout'" in html
    i = html.index("function signOut(opts)")
    body = html[i:i + 1600]
    # It needs the token that is about to be scrubbed, so it fires first…
    assert body.index("revokeSessionOnServer(ST.token)") < body.index("clearAuthFields(ST)")
    # …and it is never awaited: the device clears whether or not the call lands.
    assert "await revokeSessionOnServer" not in html
