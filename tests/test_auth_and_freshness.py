"""Sign-in / sign-up fixes and the news-freshness trigger, executed end to end.

Each test drives the real FastAPI routes against a throwaway sqlite database,
the same pattern test_dossier.py uses.

  * Email is normalised: an account made as "Ravi@Gmail.com " signs in as
    "ravi@gmail.com" (phone keyboards capitalise the first letter).
  * The username chosen at sign-up is stored (the model used to drop it).
  * /auth/check-username, /forgot-password and /reset-password exist — the
    client has called all three for months and got 404s.
  * The personalised feed is REPLACED on recompute, so a week-old story's
    stale score cannot outrank today's.
  * /cron/collect is token-guarded and starts a cycle; /status/freshness
    reports the age of the newest servable story.
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import main  # noqa: E402


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "auth.db")
    conn = sqlite3.connect(path)
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

    def _get_db():
        c = sqlite3.connect(path)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(main, "get_db", _get_db)
    main._AUTH_HITS.clear()
    for k in ("RESEND_API_KEY", "SMTP_HOST", "ENV"):
        monkeypatch.delenv(k, raising=False)
    return _get_db


@pytest.fixture
def client(db):
    return TestClient(main.app)


def _signup(client, **kw):
    body = {"email": "Ravi@Gmail.com ", "password": "secret1", "name": "Ravi"}
    body.update(kw)
    return client.post("/signup", json=body)


# ── email normalisation ─────────────────────────────────────────────────────
def test_signup_stores_a_normalised_email_and_login_ignores_case(client, db):
    r = _signup(client)
    assert r.status_code == 200, r.text
    row = db().execute("SELECT email FROM users").fetchone()
    assert row["email"] == "ravi@gmail.com"
    r = client.post("/login", json={"email": "  RAVI@gmail.com", "password": "secret1"})
    assert r.status_code == 200, r.text
    assert r.json()["token"]


def test_a_legacy_mixed_case_account_can_still_sign_in(client, db):
    c = db()
    c.execute("INSERT INTO users (email, password, name) VALUES (?,?,?)",
              ("Old@Example.com", main.hash_password("hunter22"), "Old"))
    c.commit()
    r = client.post("/login", json={"email": "old@example.com", "password": "hunter22"})
    assert r.status_code == 200, r.text


def test_duplicate_signup_is_refused_whatever_the_case(client):
    assert _signup(client).status_code == 200
    r = _signup(client, email="RAVI@gmail.com")
    assert r.status_code == 400
    assert "already registered" in r.json()["detail"]


def test_wrong_password_is_a_401(client):
    _signup(client)
    r = client.post("/login", json={"email": "ravi@gmail.com", "password": "nope123"})
    assert r.status_code == 401


def test_signup_validates_email_and_password(client):
    assert _signup(client, email="not-an-email").status_code == 400
    assert _signup(client, password="123").status_code == 400


# ── username ────────────────────────────────────────────────────────────────
def test_username_chosen_at_signup_is_stored_and_checked(client, db):
    r = client.get("/auth/check-username", params={"u": "@Ravi_K"})
    assert r.json() == {"username": "ravi_k", "valid": True, "available": True}
    assert _signup(client, username="@Ravi_K").status_code == 200
    assert db().execute("SELECT username FROM users").fetchone()["username"] == "ravi_k"
    assert client.get("/auth/check-username", params={"u": "ravi_k"}).json()["available"] is False
    r = _signup(client, email="other@x.com", username="ravi_k")
    assert r.status_code == 409


def test_check_username_rejects_invalid_handles(client):
    assert client.get("/auth/check-username", params={"u": "ab"}).json()["valid"] is False


# ── password reset ──────────────────────────────────────────────────────────
def test_forgot_and_reset_password_round_trip_in_dev(client):
    _signup(client)
    r = client.post("/forgot-password", json={"email": "RAVI@gmail.com"})
    assert r.status_code == 200, r.text
    code = r.json()["debug_otp"]              # dev has no mail provider
    assert len(code) == 6

    bad = client.post("/reset-password",
                      json={"email": "ravi@gmail.com", "otp": "000000" if code != "000000" else "111111",
                            "password": "newpass1"})
    assert bad.status_code == 400

    ok = client.post("/reset-password",
                     json={"email": "ravi@gmail.com", "otp": code, "password": "newpass1"})
    assert ok.status_code == 200, ok.text
    assert client.post("/login", json={"email": "ravi@gmail.com",
                                        "password": "newpass1"}).status_code == 200
    # a code is single-use
    again = client.post("/reset-password",
                        json={"email": "ravi@gmail.com", "otp": code, "password": "other12"})
    assert again.status_code == 400


def test_forgot_password_does_not_reveal_unknown_emails(client):
    r = client.post("/forgot-password", json={"email": "nobody@x.com"})
    assert r.status_code == 200
    assert "debug_otp" not in r.json()


def test_forgot_password_in_prod_without_a_provider_says_so(client, monkeypatch):
    monkeypatch.setenv("ENV", "prod")
    _signup(client)
    r = client.post("/forgot-password", json={"email": "ravi@gmail.com"})
    assert r.status_code == 503


def test_reset_code_dies_after_too_many_wrong_guesses(client):
    _signup(client)
    code = client.post("/forgot-password", json={"email": "ravi@gmail.com"}).json()["debug_otp"]
    wrong = "123456" if code != "123456" else "654321"
    for _ in range(main.RESET_MAX_ATTEMPTS):
        main._AUTH_HITS.clear()
        client.post("/reset-password", json={"email": "ravi@gmail.com", "otp": wrong,
                                             "password": "newpass1"})
    main._AUTH_HITS.clear()
    r = client.post("/reset-password", json={"email": "ravi@gmail.com", "otp": code,
                                             "password": "newpass1"})
    assert r.status_code == 400


# ── personalised feed staleness ─────────────────────────────────────────────
def test_recompute_replaces_the_feed_rather_than_accumulating(db):
    c = db()
    c.execute("INSERT INTO users (id, email, password) VALUES (7, 'a@b.co', 'x')")
    c.execute("INSERT INTO user_preferences (user_id, topic_name, pillar_id, weight) "
              "VALUES (7, 'AI', 3, 1.0)")
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    new = datetime.now(timezone.utc).isoformat()
    c.execute("INSERT INTO articles (id, url, headline, pillar_id, published_at, status, "
              "ai_processed) VALUES (1, 'u1', 'old', 3, ?, 'published', 1)", (old,))
    c.execute("INSERT INTO articles (id, url, headline, pillar_id, published_at, status, "
              "ai_processed) VALUES (2, 'u2', 'new', 3, ?, 'published', 1)", (new,))
    # A score frozen from when article 1 was fresh.
    c.execute("INSERT INTO feeds (user_id, article_id, score) VALUES (7, 1, 99.0)")
    c.commit()
    c.close()

    main.compute_feed_for_user(7)

    ids = [r["article_id"] for r in db().execute(
        "SELECT article_id FROM feeds WHERE user_id=7").fetchall()]
    assert ids == [2]


# ── freshness trigger ───────────────────────────────────────────────────────
def test_cron_collect_requires_a_token(client):
    assert client.get("/cron/collect").status_code == 403
    assert client.get("/cron/collect", params={"token": "wrong"}).status_code == 403


def test_cron_collect_starts_a_cycle(client, monkeypatch):
    calls = []

    async def fake_cycle():
        calls.append(1)

    monkeypatch.setattr(main, "_collect_news_cycle", fake_cycle)
    monkeypatch.setitem(main._collect_state, "last_start", None)
    monkeypatch.setitem(main._collect_state, "running", False)
    r = client.get("/cron/collect", params={"token": main.ADMIN_TOKEN})
    assert r.status_code == 200
    assert r.json()["status"] == "started"


def test_cron_collect_skips_when_a_cycle_just_ran(client, monkeypatch):
    monkeypatch.setitem(main._collect_state, "running", False)
    monkeypatch.setitem(main._collect_state, "last_start",
                        datetime.now(timezone.utc).isoformat())
    r = client.get("/cron/collect", params={"token": main.ADMIN_TOKEN})
    assert r.json()["status"] == "fresh"


def test_freshness_reports_the_newest_servable_story(client, db):
    c = db()
    two_h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    c.execute("INSERT INTO articles (url, headline, pillar_id, published_at, status, "
              "ai_processed) VALUES ('u', 'h', 3, ?, 'published', 1)", (two_h,))
    c.commit()
    body = client.get("/status/freshness").json()
    assert body["status"] == "ok"
    assert 1.9 <= body["newest_age_hours"] <= 2.1
    assert body["published_last_24h"] == 1
