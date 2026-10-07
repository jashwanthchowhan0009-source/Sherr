"""significance.py + the myFeed bar it feeds.

The cases are real headlines from one 48h production window (2026-10-06/07),
chosen from both sides of the bar: the class the product is about must clear
it, and the class the owner named as junk ("a local accident, a dispute between
five people") must not.
"""
import sqlite3

import pytest

import significance as S


def sc(head, src, summary="", fc="general"):
    return S.score(head, summary, src, fc)


# ─── must clear the myFeed bar ───────────────────────────────────────────────
@pytest.mark.parametrize("head,src,fc", [
    ("Iraq devalues its currency as Iran-US war disrupts oil shipping routes", "Economic Times", "general"),
    ("Iran-Backed Houthis Say They Attacked Riyadh Airport, Aramco Refinery", "NDTV", "general"),
    ("11 Indian crew injured after ship hit by projectile in Strait of Hormuz; India condemns attack", "The Hindu", "general"),
    ("World Bank Raises India's GDP Growth Forecast To 7.1% For This Fiscal", "NDTV", "general"),
    ("Global Market: Japan 30-year bond yield hits record high ahead of 10-year auction", "ET Markets", "financial"),
    ("Rate hike: Behind the RBI's pivot, a need to safeguard price stability", "Indian Express", "general"),
    ("Neutrino physicist wins 2026 Nobel Physics Prize", "Ars Technica", "general"),
    ("Mistral's new 1T model aims to leapfrog closed and open rivals", "TechCrunch", "general"),
    ("Saudi Arabia says East-West pipeline pumping 5.8 million bpd", "Al Jazeera", "general"),
    ("RBI issues Directions on 'Credit Valuation Adjustment (CVA) Framework'", "RBI Releases", "financial"),
])
def test_world_events_clear_the_bar(head, src, fc):
    assert sc(head, src, fc=fc) >= S.feed_min(), S.score_article(head, "", src, fc)


# ─── must not ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("head,src", [
    ("Parents arrested for repeated rape, sexual abuse of minor daughter in Gujarat", "Indian Express"),
    ("Residents of three south Chennai zones will not receive piped water supply for 24 hours", "The Hindu"),
    ("Nagaland woman pulled towards car by 3 men at 5-star hotel, friends attacked", "Indian Express"),
    ("86 to 101: Kohli's final ODI window to break Tendulkar's century record", "Times of India"),
    ("Ajay Devgn doesn't rule out Drishyam 4, says Mohanlal film 'most suited'", "Indian Express"),
    ("Nintendo Switch Sports Resort Appears To Solve All of Switch Sports' Shortcomings", "IGN"),
    ("Xbox bets on Gears of War in battle of gaming giants", "BBC News"),
    ("Top 5 stocks to buy: Jyothy Labs, Chalet Hotels, Orkla India, Union Bank", "Mint Markets"),
    ("Axis Bank Share Price Live Updates: Positive Momentum for Axis Bank as it Exceeds 20-Day SMA", "ET Markets"),
    ("Infinix Smart 20 HD Launched in India With 120Hz Display, 5,000mAh Battery: Price, Features", "Gadgets 360"),
    ("Justin Ellis helps US cap a perfect international window with 1-0 win over Canada", "Guardian Sport"),
])
def test_local_and_soft_news_stay_out(head, src):
    assert sc(head, src) < S.feed_min(), S.score_article(head, "", src)


def test_sebi_micro_filing_is_capped_whatever_the_source():
    head = ("General Remittance Advice against: Virtual Business Solution Pvt. Ltd. "
            "(PAN: AAFCV0106J) [Defaulter] in the matter of trading based stock "
            "recommendations using social media YouTube in the scrip of Sadhna "
            "Broadcast Ltd.: under Recovery Certificate No. 9245 of 2026.")
    score, why = S.score_article(head, head, "SEBI Releases", "financial")
    assert score <= 20 and why.get("filing")


def test_score_is_bounded_and_explained():
    score, why = S.score_article("x", "", "", "general")
    assert 0 <= score <= 100
    assert set(why) >= {"tier", "topics", "entities"}


def test_blurb_cannot_add_a_junk_penalty_on_its_own():
    head = "RBI raises repo rate by 25 basis points to curb inflation"
    clean = sc(head, "The Hindu")
    noisy = sc(head, "The Hindu", summary="Police said traffic was diverted near the RBI office.")
    assert noisy >= clean


def test_thresholds_read_at_call_time(monkeypatch):
    monkeypatch.setenv("SIG_FEED_MIN", "60")
    assert S.feed_min() == 60
    monkeypatch.delenv("SIG_REWRITE_MIN", raising=False)
    assert S.rewrite_min() == 60          # follows the feed bar unless set
    monkeypatch.setenv("SIG_REWRITE_MIN", "30")
    assert S.rewrite_min() == 30


# ─── the rewrite selector spends the budget on significant stories ──────────
def _mk(conn):
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE articles (id INTEGER PRIMARY KEY, url TEXT,
        headline TEXT, source_headline TEXT, full_body TEXT, summary_60 TEXT,
        source_summary TEXT, status TEXT, ai_processed INTEGER DEFAULT 0,
        reprocessed INTEGER DEFAULT 0, pillar_id INTEGER, micro_tags TEXT,
        source_name TEXT, published_at TEXT, significance INTEGER DEFAULT -1)""")


def test_selector_skips_low_scores_and_queues_unscored_last():
    import body_state
    conn = sqlite3.connect(":memory:")
    _mk(conn)
    rows = [  # id, significance, published_at
        (1, 80, "2026-10-07T10:00:00"),   # significant, older
        (2, 10, "2026-10-07T12:00:00"),   # junk, newest — must be skipped
        (3, -1, "2026-10-07T11:00:00"),   # unscored — eligible, but last
        (4, 55, "2026-10-07T11:30:00"),   # significant, newer
    ]
    for i, sig, ts in rows:
        conn.execute("INSERT INTO articles (id, headline, source_headline, full_body, "
                     "summary_60, source_summary, status, reprocessed, published_at, "
                     "significance) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (i, "h", "sh", "", "", "src", "published", 0, ts, sig))
    q = body_state._needing_rewrite_sql(sig_min=42)
    got = [r["id"] for r in conn.execute(q, (10,))]
    assert got == [4, 1, 3]


# ─── myFeed: written + significant only, relaxing instead of blanking ───────
def _main_db(tmp_path):
    import main
    conn = sqlite3.connect(str(tmp_path / "m.db"))
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            conn.execute(st)
        except sqlite3.OperationalError:
            pass
    return conn


def _add(conn, i, *, headline, source_headline, summary, sig, ts="2026-10-07T10:00:00"):
    conn.execute(
        "INSERT INTO articles (id, url, title_hash, headline, source_headline, "
        "summary_60, full_body, source_summary, status, ai_processed, published_at, "
        "significance) VALUES (?,?,?,?,?,?,?,?,'published',1,?,?)",
        (i, f"u{i}", f"t{i}", headline, source_headline, summary, summary,
         source_headline, ts, sig))


WRITTEN = "SEBI moved to recover dues from five entities tied to YouTube stock tips."
PLACEHOLDER = ("SherrByte has not yet published its own write-up of this story. "
               "The headline and link below go to the original report.")


def test_myfeed_serves_only_written_significant_cards(tmp_path):
    import main
    conn = _main_db(tmp_path)
    for i in range(1, 7):     # six good cards: rewritten headline, real summary, significant
        _add(conn, i, headline=f"Ours {i}", source_headline=f"Theirs {i}",
             summary=WRITTEN, sig=70, ts=f"2026-10-07T1{i}:00:00")
    _add(conn, 7, headline="Raw headline", source_headline="Raw headline",   # aggregator
         summary=PLACEHOLDER, sig=90, ts="2026-10-07T19:00:00")
    _add(conn, 8, headline="Ours 8", source_headline="Theirs 8",              # written junk
         summary=WRITTEN, sig=12, ts="2026-10-07T19:30:00")
    conn.commit()
    ids = [r["id"] for r in main._myfeed_rows(conn, 10, 0)]
    assert 7 not in ids and 8 not in ids
    assert ids == [6, 5, 4, 3, 2, 1]


def test_myfeed_relaxes_rather_than_blanks(tmp_path):
    """With the writer down, few rows are significant AND written. The surface
    falls back to written-only, then to everything, instead of an empty deck."""
    import main
    conn = _main_db(tmp_path)
    for i in range(1, 4):
        _add(conn, i, headline=f"Raw {i}", source_headline=f"Raw {i}",
             summary=PLACEHOLDER, sig=80, ts=f"2026-10-07T1{i}:00:00")
    conn.commit()
    assert len(main._myfeed_rows(conn, 10, 0)) == 3


def test_dossier_node_never_echoes_the_headline(tmp_path):
    import main
    conn = _main_db(tmp_path)
    head = "General Remittance Advice against: X Pvt. Ltd. [Defaulter]"
    conn.execute(
        "INSERT INTO articles (id, url, title_hash, headline, source_headline, what_info, "
        "where_info, summary_60, status, ai_processed) "
        "VALUES (1,'u','t',?,?,?, 'Not specified', 's', 'published', 1)",
        (head, head, head + "."))
    conn.commit()
    row = conn.execute("SELECT * FROM articles WHERE id=1").fetchone()
    node = main._dossier_payload(row)["node"]
    assert node["what"] == "" and node["where"] == ""


def test_publish_pending_replaces_the_publishers_summary_too(tmp_path):
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
    import publish_pending
    conn = _main_db(tmp_path)
    conn.execute(
        "INSERT INTO articles (id, url, title_hash, headline, source_headline, summary_60, "
        "full_body, source_summary, status, source_name) VALUES "
        "(1,'u','t','H','H','Publisher prose, sentence one. Sentence two.',"
        "'Publisher prose, sentence one. Sentence two.','Publisher prose','pending_rewrite','X')")
    conn.commit()
    publish_pending.drain_articles(conn, mode="aggregator")
    r = conn.execute("SELECT summary_60, full_body, source_summary FROM articles").fetchone()
    assert "Publisher prose" not in r["summary_60"]
    assert r["summary_60"] == r["full_body"]
    assert r["source_summary"] == "Publisher prose"     # the reference copy survives
