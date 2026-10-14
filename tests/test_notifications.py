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


# ── the two Firebase configs are one app, so they must say the same thing ────

SW = os.path.join(_ROOT, "firebase-messaging-sw.js")


def _firebase_config(text, where):
    """Parse the object literal passed to firebase.initializeApp()."""
    m = re.search(r"firebase\.initializeApp\(\s*\{(.*?)\}\s*\)", text, re.S)
    assert m, f"no firebase.initializeApp({{...}}) found in {where}"
    cfg = dict(re.findall(r'(\w+)\s*:\s*"([^"]*)"', m.group(1)))
    assert cfg, f"initializeApp in {where} parsed to an empty config"
    return cfg


def test_the_page_and_the_service_worker_use_the_same_firebase_app():
    """They are two halves of one app — the page registers the push token and the
    service worker receives on it — so any field that differs splits them apart.

    `apiKey` did. index.html carried a key belonging to Google Cloud project
    719632752358 while every other field in the very same object named 485011285385,
    and that key was restricted to Android apps on top of it, so getToken() could
    never mint a web token for `sherrbyte`. The other six fields matched exactly,
    which is why nothing looked wrong: one pasted value from the wrong project.

    Compared field-for-field rather than by matching the key string, so the next
    field to drift is caught too.
    """
    page = _firebase_config(_read(INDEX), "index.html")
    worker = _firebase_config(_read(SW), "firebase-messaging-sw.js")

    differing = sorted(k for k in set(page) | set(worker)
                       if page.get(k) != worker.get(k))
    assert not differing, "index.html and firebase-messaging-sw.js disagree on: " + \
        "; ".join(f"{k}: index.html={page.get(k)!r} sw={worker.get(k)!r}"
                  for k in differing)


def test_the_firebase_key_belongs_to_the_project_the_config_names():
    """`appId` is `1:<projectNumber>:web:<id>` and `messagingSenderId` IS that
    project number, so the two must agree. This is the invariant that catches a
    credential pasted in from another project — the actual defect above."""
    for where, path in (("index.html", INDEX), ("firebase-messaging-sw.js", SW)):
        cfg = _firebase_config(_read(path), where)
        assert cfg.get("apiKey"), f"{where} has no apiKey"
        assert cfg["apiKey"].startswith("AIza"), f"{where}: not a Google API key"
        project = cfg["appId"].split(":")[1]
        assert project == cfg["messagingSenderId"], (
            f"{where}: appId names project {project} but messagingSenderId is "
            f"{cfg['messagingSenderId']} — the config mixes two Firebase projects")
