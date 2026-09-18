"""Real notification system: a persistent per-user inbox with read/unread, the
FCM register/test endpoints the frontend was already calling (and that did not
exist), comment-reply notifications, and a header-bell unread badge.

Device push (FCM HTTP v1) is best-effort and env-gated; the inbox works without
it, so these tests exercise the inbox SQL + the wiring, not a live push.
"""
import os
import re
import sqlite3

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
INDEX = os.path.join(_ROOT, "index.html")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


# ── inbox SQL against sqlite (the pgcompat-shaped queries the endpoints run) ──

def _schema(con):
    con.execute(
        "CREATE TABLE notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "user_id INTEGER NOT NULL, kind TEXT NOT NULL DEFAULT 'general', "
        "title TEXT NOT NULL, body TEXT DEFAULT '', article_id INTEGER, "
        "color TEXT DEFAULT '', image_url TEXT DEFAULT '', read_at TEXT, "
        "created_at TEXT DEFAULT (datetime('now')))")
    con.execute(
        "CREATE TABLE push_tokens (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "user_id INTEGER NOT NULL, token TEXT NOT NULL UNIQUE, "
        "created_at TEXT DEFAULT (datetime('now')))")


def test_inbox_insert_unread_and_mark_read():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    _schema(con)
    for i in range(3):
        con.execute("INSERT INTO notifications (user_id, kind, title, body) "
                    "VALUES(1,'reply',?,?)", (f"t{i}", "b"))
    con.execute("INSERT INTO notifications (user_id, kind, title, body) "
                "VALUES(2,'reply','other','b')")
    unread = con.execute("SELECT COUNT(*) AS n FROM notifications WHERE "
                         "user_id=1 AND read_at IS NULL").fetchone()["n"]
    assert unread == 3
    # mark one read by id
    con.execute("UPDATE notifications SET read_at=datetime('now') WHERE "
                "user_id=1 AND read_at IS NULL AND id IN (1)")
    assert con.execute("SELECT COUNT(*) AS n FROM notifications WHERE user_id=1 "
                       "AND read_at IS NULL").fetchone()["n"] == 2
    # mark all read
    con.execute("UPDATE notifications SET read_at=datetime('now') WHERE "
                "user_id=1 AND read_at IS NULL")
    assert con.execute("SELECT COUNT(*) AS n FROM notifications WHERE user_id=1 "
                       "AND read_at IS NULL").fetchone()["n"] == 0
    # user 2 untouched
    assert con.execute("SELECT COUNT(*) AS n FROM notifications WHERE user_id=2 "
                       "AND read_at IS NULL").fetchone()["n"] == 1
    con.close()


def test_push_token_reassigns_to_current_user():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    _schema(con)
    # same device token registered by user 1, then user 2 (delete-then-insert)
    for uid in (1, 2):
        con.execute("DELETE FROM push_tokens WHERE token=?", ("devtok",))
        con.execute("INSERT INTO push_tokens (user_id, token) VALUES(?,?)",
                    (uid, "devtok"))
    rows = con.execute("SELECT user_id FROM push_tokens WHERE token='devtok'"
                       ).fetchall()
    assert len(rows) == 1 and rows[0]["user_id"] == 2   # no duplicates, reassigned
    con.close()


def test_reply_fanout_excludes_the_commenter():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE article_comments (id INTEGER PRIMARY KEY, "
                "user_id INTEGER, article_id INTEGER, body TEXT)")
    for uid in (5, 7, 5, 9):
        con.execute("INSERT INTO article_comments (user_id, article_id, body) "
                    "VALUES(?,?,?)", (uid, 100, "hi"))
    others = [r["user_id"] for r in con.execute(
        "SELECT DISTINCT user_id FROM article_comments WHERE article_id=? "
        "AND user_id<>? LIMIT 50", (100, 5)).fetchall()]
    assert set(others) == {7, 9}     # 5 (the commenter) is not notified
    con.close()


# ── endpoints + helpers present ──────────────────────────────────────────────

def test_notification_endpoints_registered():
    m = _read(MAIN)
    for route in ('@app.post("/api/notifications/register")',
                  '@app.post("/api/notifications/test")',
                  '@app.get("/notifications/unread-count")',
                  '@app.post("/notifications/read")'):
        assert route in m, f"missing {route}"
    assert "def _notify(" in m and "def _fcm_send(" in m
    # comment posting fans a reply notification out.
    cblock = m[m.index("async def signal_post_comment("):
               m.index("async def signal_post_comment(") + 2400]
    assert "_notify(" in cblock and "kind=\"reply\"" in cblock


def test_push_is_env_gated_and_never_hard_dep():
    m = _read(MAIN)
    # FCM imports are lazy (inside _fcm_send), so a missing google-auth or unset
    # creds can never break import or the inbox.
    fblock = m[m.index("def _fcm_send("):m.index("def _fcm_send(") + 1500]
    assert "from google.oauth2 import service_account" in fblock
    assert "FCM_SA_JSON" in m


# ── frontend wiring ──────────────────────────────────────────────────────────

def test_frontend_badge_and_bell():
    h = _read(INDEX)
    assert "function setNotifBadge(" in h
    assert "function refreshNotifBadge(" in h
    assert 'id="h-notif"' in h and "notif-badge" in h
    # renderNotifs uses the new stored+suggestions shape and marks read on open.
    r = h[h.index("async function renderNotifs()"):
          h.index("async function renderNotifs()") + 2600]
    assert "data.suggestions" in r
    assert "/notifications/read" in r
    assert "setNotifBadge(0)" in r
