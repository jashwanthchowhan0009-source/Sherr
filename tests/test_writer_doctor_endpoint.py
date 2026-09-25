"""
/admin/writer-doctor (SHERR_WRITING_SPEC.md §5) and the run_ai_batch wiring that
feeds a card's why-it-matters line from the NEWS writer.

The endpoint must be read-only, token-guarded, carry the Section-5 fields for the
G1-G7 gate AND stay back-compatible with the pre-existing synthesis block.
"""

import asyncio
import os
import sqlite3
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import writer_gate as g          # noqa: E402


def _client():
    import main
    from fastapi.testclient import TestClient
    main.ADMIN_TOKEN = "test-token"
    return main, TestClient(main.app)


MAIN, CLIENT = _client()


def _seed_one_failure():
    g.reset_attempts()
    bad = {"headline": "RBI holds rate", "body": "Too short.", "why_it_matters": "x",
           "numbers_used": [], "entities": {"subject": "RBI", "affected": []},
           "primary_source_attribution": "Reuters"}
    gate = g.run_gate(bad, "src text", ["a source headline"])
    g.record_attempt(article_id=42, writer_id="gemini-x/news-v1", attempt=1,
                     gate=gate, is_regen=False, fell_back=True)


def test_endpoint_is_registered():
    from fastapi.routing import APIRoute
    paths = {r.path for r in MAIN.app.routes if isinstance(r, APIRoute)}
    assert "/admin/writer-doctor" in paths


def test_refuses_without_the_admin_token():
    assert CLIENT.get("/admin/writer-doctor").status_code == 403
    assert CLIENT.get("/admin/writer-doctor",
                      params={"token": "wrong"}).status_code == 403


def test_returns_section_5_fields_and_keeps_backcompat_keys():
    _seed_one_failure()
    r = CLIENT.get("/admin/writer-doctor", params={"token": "test-token"})
    assert r.status_code == 200
    j = r.json()
    # Section 5 (the G1-G7 gate)
    for k in ("window_hours", "attempted", "passed", "pass_rate", "regenerated",
              "fell_back_to_safe", "by_rule", "by_writer", "recent_failures"):
        assert k in j, f"missing Section-5 field {k}"
    assert set(j["by_rule"]) == set(g.RULE_IDS)
    assert j["fell_back_to_safe"] == 1
    assert j["by_writer"]["gemini-x/news-v1"]["attempted"] == 1
    assert j["recent_failures"][0]["article_id"] == "42"
    # Back-compat (the synthesis block) is untouched
    for k in ("writers", "gate_rules", "length_bands", "phase_gate"):
        assert k in j, f"back-compat key {k} disappeared"


def test_window_hours_query_param_is_honoured():
    _seed_one_failure()
    j = CLIENT.get("/admin/writer-doctor",
                   params={"token": "test-token", "window_hours": 6}).json()
    assert j["window_hours"] == 6


def test_endpoint_triggers_no_generation_and_writes_nothing():
    # It reads two in-memory stores; it must never call a writer or a limiter.
    called = {"writer": 0}

    async def _boom(*a, **k):
        called["writer"] += 1
        return None, "x"

    import ai_processor
    orig = ai_processor.news_writer
    ai_processor.news_writer = _boom
    try:
        r = CLIENT.get("/admin/writer-doctor", params={"token": "test-token"})
        assert r.status_code == 200
        assert called["writer"] == 0
    finally:
        ai_processor.news_writer = orig


# ─── run_ai_batch stores why_it_matters from the NEWS writer ──────────────────

def _good_card():
    return {
        "headline": "Reserve Bank held the repo rate steady on Tuesday morning",
        "body": (
            "The Reserve Bank of India kept its policy repo rate unchanged at 6.5 "
            "percent on Tuesday, the central bank decided in Mumbai. The monetary "
            "policy committee took the decision, citing an easing inflation path and "
            "steady bank credit across the wider economy. It is the fourth consecutive "
            "review to leave the rate at 6.5 percent, a level first reached in 2023."),
        "why_it_matters": (
            "The rate has now held at 6.5 percent for a fourth straight review, the "
            "longest such stretch since the 2023 tightening cycle drew to a close."),
        "numbers_used": [
            {"value": "6.5", "unit": "percent", "source_span": "6.5 percent"},
            {"value": "2023", "unit": "year", "source_span": "in 2023"}],
        "entities": {"subject": "Reserve Bank of India", "affected": []},
        "primary_source_attribution": "Reuters",
    }


def test_run_ai_batch_populates_why_it_matters_on_a_published_row(monkeypatch):
    import main

    # Fresh DB for this test.
    d = tempfile.mkdtemp()
    dbp = os.path.join(d, "t.db")
    monkeypatch.setattr(main, "DB_PATH", dbp)
    monkeypatch.setattr(main, "USE_POSTGRES", False)
    conn = sqlite3.connect(dbp)
    conn.row_factory = sqlite3.Row
    monkeypatch.setattr(main, "get_db", lambda: sqlite3.connect(dbp))
    # init the schema on this db
    real_get = sqlite3.connect(dbp)
    real_get.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for stmt in main._MIGRATIONS:
        try:
            conn.execute(stmt)
        except Exception:
            pass
    conn.execute(
        "INSERT INTO articles (id, url, headline, source_headline, full_body, "
        "summary_60, source_summary, pillar_id, micro_tags, ai_processed) "
        "VALUES (1, 'http://x/1', 'Publisher line on the central bank rate hold', "
        "'Publisher line on the central bank rate hold', '', '', "
        "'The RBI kept the repo rate at 6.5 percent in 2023 and again on Tuesday.', "
        "3, '[]', 0)")
    conn.commit()

    # Stub the rewrite: an original headline + body that clears _gate_article
    # (source_body is empty, so originality is trivially clear).
    async def fake_batch(batch_input, concurrency=2):
        return [{
            "refined_title": "Reserve Bank held the repo rate steady on Tuesday",
            "summary": "An original two sentence summary that does not echo the source at all.",
            "full_body": ("An entirely fresh body written in our own words about the "
                          "central bank leaving its benchmark rate untouched this week."),
            "category": "economy", "topic_tags": ["RBI"], "is_trending": False,
            "sentiment": "neutral", "when_info": "", "where_info": "Not specified",
        }]
    monkeypatch.setattr(main, "process_batch", fake_batch)
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "total_keys": 1})

    # Stub the NEWS writer to return a passing card.
    import ai_processor

    async def fake_news(source_text, corrective=None):
        return _good_card(), "gemini-test/news-v1"
    monkeypatch.setattr(ai_processor, "news_writer", fake_news)
    g.reset_attempts()

    n = asyncio.run(main.run_ai_batch(conn))
    assert n == 1
    row = conn.execute(
        "SELECT status, why_it_matters, writer_id FROM articles WHERE id=1").fetchone()
    assert row["status"] == "published", row["status"]
    assert row["why_it_matters"] == _good_card()["why_it_matters"]
    assert row["writer_id"] == "gemini-test/news-v1"
    # And the attempt was recorded for the doctor.
    assert g.writer_doctor_report(24)["passed"] == 1
    conn.close()


def test_news_writer_can_be_switched_off(monkeypatch):
    import main
    import ai_processor
    monkeypatch.setattr(main, "NEWS_WRITER_ENABLED", False)

    called = {"n": 0}

    async def fake_news(source_text, corrective=None):
        called["n"] += 1
        return _good_card(), "x/news-v1"
    monkeypatch.setattr(ai_processor, "news_writer", fake_news)

    d = tempfile.mkdtemp()
    dbp = os.path.join(d, "t.db")
    conn = sqlite3.connect(dbp)
    conn.row_factory = sqlite3.Row
    conn.executescript(main.CREATE_TABLES)
    for stmt in main._MIGRATIONS:
        try:
            conn.execute(stmt)
        except Exception:
            pass
    conn.execute(
        "INSERT INTO articles (id, url, headline, source_headline, full_body, "
        "summary_60, source_summary, pillar_id, micro_tags, ai_processed) "
        "VALUES (1, 'http://x/1', 'Publisher line', 'Publisher line', '', '', "
        "'The RBI kept the repo rate at 6.5 percent.', 3, '[]', 0)")
    conn.commit()

    async def fake_batch(batch_input, concurrency=2):
        return [{"refined_title": "A wholly original headline about the bank",
                 "summary": "Original summary text that shares nothing with the source.",
                 "full_body": "A fresh original body in our own words about the rate.",
                 "category": "economy", "topic_tags": [], "is_trending": False,
                 "sentiment": "neutral", "when_info": "", "where_info": "Not specified"}]
    monkeypatch.setattr(main, "process_batch", fake_batch)
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "total_keys": 1})

    asyncio.run(main.run_ai_batch(conn))
    assert called["n"] == 0          # disabled → the writer is never called
    conn.close()
