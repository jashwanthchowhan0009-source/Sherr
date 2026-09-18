"""Profile insights are real now: /me/analytics and /me/activity — the endpoints
the frontend already called but that never existed, which is why Profile showed
"No data yet" for every reader. The payload shape must match exactly what
loadProfileAnalytics renders, or a.daily_sec.map(...) throws straight into the
"No data yet" catch.

Also: a compact depth chip (reading time, via-publisher) on the myFeed News node.
"""
import os
import sqlite3

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(_ROOT, "main.py")
INDEX = os.path.join(_ROOT, "index.html")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


M = _read(MAIN)
H = _read(INDEX)


def test_analytics_endpoints_registered():
    assert '@app.get("/me/analytics")' in M
    assert '@app.get("/me/activity")' in M


def test_analytics_returns_every_key_the_profile_renders():
    block = M[M.index('@app.get("/me/analytics")'):
              M.index('@app.get("/me/activity")')]
    # loadProfileAnalytics reads all of these; a missing one breaks the render.
    for key in ("current_streak", "longest_streak", "daily_sec",
                "streak_history", "articles_today", "articles_week",
                "time_today_formatted", "time_week_formatted", "categories",
                "fastest_growth"):
        assert key in block, f"/me/analytics missing {key}"
    # exact nested shapes the SVG builders read
    assert '"seconds"' in block and '"date"' in block   # daily_sec bars
    assert '"active"' in block and '"streak"' in block   # streak_history cells
    assert '"color"' in block and '"count"' in block     # donut categories


def test_activity_row_shape():
    block = M[M.index('@app.get("/me/activity")'):
              M.index('@app.get("/me/activity")') + 1400]
    for key in ('"headline"', '"image_url"', '"category"', '"updated_at"',
                '"scroll_pct"', '"completed"'):
        assert key in block, f"/me/activity missing {key}"


def test_analytics_read_query_runs_on_sqlite():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE user_interactions (id INTEGER PRIMARY KEY, "
                "user_id INTEGER, article_id INTEGER, action TEXT, "
                "timestamp TEXT DEFAULT (datetime('now')))")
    con.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, pillar_id INTEGER, "
                "full_body TEXT, headline TEXT, image_url TEXT, source_name TEXT)")
    con.execute("INSERT INTO articles (id,pillar_id,full_body,headline) "
                "VALUES(1,3,'word '*400,'H')")
    con.execute("INSERT INTO user_interactions (user_id,article_id,action) "
                "VALUES(9,1,'read')")
    rows = con.execute(
        "SELECT date(ui.timestamp) AS d, a.pillar_id AS pid, ui.article_id AS aid, "
        "LENGTH(COALESCE(a.full_body,'')) AS ch FROM user_interactions ui "
        "JOIN articles a ON a.id=ui.article_id "
        "WHERE ui.user_id=? AND ui.action='read'", (9,)).fetchall()
    con.close()
    assert len(rows) == 1 and rows[0]["pid"] == 3 and rows[0]["ch"] > 0


# ── myFeed card depth ────────────────────────────────────────────────────────

def test_myfeed_news_node_has_depth_chip():
    assert ".mfd-meta" in H and ".mfd-mchip" in H
    card = H[H.index("function buildMyfeedCard(a){"):
             H.index("function buildMyfeedCard(a){") + 2600]
    assert 'class="mfd-meta"' in card
    assert "readMins(a)" in card and "min read" in card


def test_readmins_is_robust_to_raw_and_normalized_articles():
    # myFeed passes a RAW article (full_body / summary), Home a normalized one
    # (body_ai / preview) — readMins must handle both.
    fn = H[H.index("function readMins(a)"):H.index("function readMins(a)") + 320]
    assert "full_body" in fn and "summary" in fn and "body_ai" in fn
