"""Read the publisher's article, then rewrite it — never copy it.

The placeholder ("SherrByte has not yet published its own write-up…") stayed on
story after story because the rewrite only ever saw the ~40-word RSS blurb. These
tests pin the fix end to end:

  * article_reader pulls the article prose out of a real-shaped page and drops
    navigation / newsletter boilerplate; JSON-LD articleBody wins when present.
  * With a full article, the model gets the WWWH prompt (what · where & when ·
    why · how) from the owner's spec; with only a blurb, the old prompt.
  * The drain feeds the FULL text to the model, stores the rewrite AND the WWWH
    skeleton, and never stores the publisher's text.
  * The originality gate runs against the full article, so a sentence copied
    from deep in the report is still rejected.
  * Opening a placeholder story queues it for an immediate rewrite, and
    /article/<id>/full never hands the placeholder back as a body.
"""
import asyncio
import json
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "scripts"))

import ai_processor    # noqa: E402
import article_reader  # noqa: E402
import main            # noqa: E402
from publish_pending import STUB  # noqa: E402

PARAS = [
    "Ira Sachs' jury handed Mike Leigh's latest film a trio of prizes on Saturday "
    "night, closing the festival in the Basque city with a clean sweep.",
    "Tender Loving Care took Best Film, Best Screenplay and Best Lead Performance "
    "for Kate O'Flynn, who plays a care worker looking after her ageing mother.",
    "The drama was shot in north London over eleven weeks with a largely "
    "improvised script, a method the director has used for five decades.",
    "Audience awards went to La Bola Negra and to NAZA, a documentary about a "
    "Malaysian car maker that sold out every screening during the week.",
    "Leigh, 83, has said in interviews this year that the film is likely to be "
    "his last feature, though he has made similar remarks before.",
    "The festival's programme director praised the jury for backing a film that "
    "the distributors had initially judged too quiet for a wide release.",
]
FULL_TEXT = "\n\n".join(PARAS)

PAGE = f"""<html><head><title>x</title></head><body>
<header><p>Subscribe to our newsletter for the latest film news every day!</p></header>
<nav><p>Film TV Music Awards Circuit Global Box Office Contact us today</p></nav>
<article>{''.join(f'<p>{p}</p>' for p in PARAS)}
<p>Sign up for Variety's newsletter. For the latest news, follow us on social.</p>
</article>
<footer><p>Copyright © 2026 Variety Media, LLC. All rights reserved worldwide.</p></footer>
<script>var junk = "<p>not text</p>";</script>
</body></html>"""


# ── article_reader ──────────────────────────────────────────────────────────
def test_extracts_article_paragraphs_and_drops_boilerplate():
    text = article_reader.extract_text(PAGE)
    assert "Tender Loving Care took Best Film" in text
    assert "Kate O'Flynn" in text
    for junk in ("Subscribe", "newsletter", "All rights reserved", "not text"):
        assert junk not in text
    assert article_reader.is_full_article(text)


def test_json_ld_article_body_is_preferred():
    ld = json.dumps({"@type": "NewsArticle", "articleBody": FULL_TEXT})
    page = f'<script type="application/ld+json">{ld}</script><p>short teaser only here ok</p>'
    assert article_reader.extract_text(page).startswith("Ira Sachs' jury")


def test_a_teaser_page_yields_nothing():
    assert article_reader.extract_text("<article><p>Only a tiny teaser paragraph "
                                       "sits on this paywalled page.</p></article>") == ""


# ── prompt selection ────────────────────────────────────────────────────────
def test_full_text_gets_the_wwwh_prompt_and_a_blurb_does_not():
    assert ai_processor.instruction_for(FULL_TEXT) is ai_processor.SYSTEM_INSTRUCTION_FULL
    assert ai_processor.instruction_for("A short blurb.") is ai_processor.SYSTEM_INSTRUCTION
    full = ai_processor.SYSTEM_INSTRUCTION_FULL
    for rule in ("what_info", "why_info", "how_info", "who_subject", "who_affected",
                 "THE HOOK + WHAT HAPPENED", "WHY and HOW", "PUBLISHER'S FULL ARTICLE"):
        assert rule in full
    assert "SHORT NEWS BLURB" not in full
    assert "ORIGINALITY" in full           # rule 0 survives the rebuild
    for k in ("what_info", "why_info", "how_info", "who_subject", "who_affected"):
        assert k in ai_processor._GEMINI_SCHEMA["properties"]


def test_validate_normalises_the_wwwh_fields():
    r = ai_processor._validate_and_fix(
        {"refined_title": "t", "summary": "s " * 20, "full_body": "w " * 60,
         "category": "arts", "who_affected": "Kate O'Flynn", "what_info": None},
        "t", "b")
    assert r["who_affected"] == ["Kate O'Flynn"]
    assert r["what_info"] == ""


# ── the drain, end to end on sqlite ─────────────────────────────────────────
@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "rw.db")
    c = sqlite3.connect(path)
    c.executescript(main.CREATE_TABLES)
    for st in main._MIGRATIONS:
        try:
            c.execute(st)
        except sqlite3.OperationalError:
            pass
    c.execute(
        "INSERT INTO articles (id, url, headline, source_headline, summary_60, "
        "full_body, source_summary, pillar_id, status, ai_processed, published_at) "
        "VALUES (5, 'https://variety.com/x', ?, ?, ?, ?, ?, 4, 'published', 1, "
        "'2026-09-27T00:00:00+00:00')",
        ("Mike Leigh's 'Tender Loving Care' Wins Big at San Sebastian",
         "Mike Leigh's 'Tender Loving Care' Wins Big at San Sebastian",
         STUB, STUB, "Ira Sachs' jury handed Leigh's latest a trio of awards."))
    c.commit()
    c.close()

    def _get_db():
        k = sqlite3.connect(path)
        k.row_factory = sqlite3.Row
        return k

    monkeypatch.setattr(main, "get_db", _get_db)
    monkeypatch.setattr(main, "available_providers",
                        lambda: {"primary": "gemini", "gemini": True})
    return _get_db


GOOD_BODY = (
    "An 83-year-old director may have just taken his final bow in style. Mike "
    "Leigh's Tender Loving Care collected three of San Sebastián's biggest prizes, "
    "chosen by a jury that Ira Sachs chaired.\n\n"
    "Kate O'Flynn won for playing a woman caring for her elderly mother, and the "
    "film also earned top honours for its writing and as the festival's best "
    "picture. Viewers separately voted for La Bola Negra and the Malaysian car "
    "documentary NAZA. Leigh built the story the way he always has, letting his "
    "cast improvise it over nearly three months of filming in north London, and "
    "he has hinted this could be his closing feature.")


def _result(body):
    return {"refined_title": "Mike Leigh's quiet care drama sweeps San Sebastián",
            "summary": "Mike Leigh's Tender Loving Care won three top prizes at San "
                       "Sebastián. Kate O'Flynn was named best lead for her role.",
            "full_body": body, "category": "arts", "topic_tags": [],
            "is_trending": False, "sentiment": "positive",
            "when_info": "Saturday", "where_info": "San Sebastián, Spain",
            "what_info": "A jury led by Ira Sachs gave Mike Leigh's film three awards.",
            "why_info": "", "how_info": "Leigh's cast improvised the script over eleven weeks.",
            "who_subject": "Ira Sachs' jury", "who_affected": ["Mike Leigh", "Kate O'Flynn"]}


def _wire(monkeypatch, body):
    seen = {}

    async def fake_fetch_many(urls):
        seen["urls"] = dict(urls)
        return {k: FULL_TEXT for k in urls}

    async def fake_process_batch(items, concurrency=5):
        seen["bodies"] = [i["body"] for i in items]
        return [_result(body) for _ in items]

    monkeypatch.setattr(main.article_reader, "fetch_many", fake_fetch_many)
    monkeypatch.setattr(main, "process_batch", fake_process_batch)
    return seen


def test_drain_reads_the_article_and_stores_an_original_wwwh_rewrite(db, monkeypatch):
    seen = _wire(monkeypatch, GOOD_BODY)
    res = main._reprocess_bodies_sync(1, 1, 1, only_ids=[5])
    assert res["rewritten"] == 1, res
    assert seen["urls"] == {5: "https://variety.com/x"}
    assert seen["bodies"] == [FULL_TEXT]          # the model read the whole report
    row = db().execute("SELECT * FROM articles WHERE id=5").fetchone()
    assert row["full_body"] == GOOD_BODY
    assert "not yet published" not in row["summary_60"]
    assert row["what_info"].startswith("A jury led by Ira Sachs")
    assert row["how_info"].startswith("Leigh's cast improvised")
    assert json.loads(row["who_affected"]) == ["Mike Leigh", "Kate O'Flynn"]
    assert row["where_info"] == "San Sebastián, Spain"
    audit = json.loads(row["originality_json"])
    assert audit["read_full_article"] is True
    # The publisher's prose is never stored anywhere on the row.
    assert all(PARAS[2] not in str(row[k]) for k in row.keys())


def test_a_sentence_copied_from_deep_in_the_article_is_rejected(db, monkeypatch):
    copied = GOOD_BODY + " " + PARAS[5]           # lifted from paragraph six
    _wire(monkeypatch, copied)
    res = main._reprocess_bodies_sync(1, 1, 1, only_ids=[5])
    assert res["rewritten"] == 0
    row = db().execute("SELECT full_body FROM articles WHERE id=5").fetchone()
    assert row["full_body"] == STUB               # left for a retry, never published


# ── rewrite on open ─────────────────────────────────────────────────────────
def test_opening_a_placeholder_story_queues_a_rewrite_and_hides_the_stub(db, monkeypatch):
    queued = []
    monkeypatch.setattr(main, "_ondemand_job", lambda aid: queued.append(aid))
    main._ondemand_inflight.clear()
    main._ondemand_hits.clear()
    body = TestClient(main.app).get("/article/5/full").json()
    assert body["body"] == ""                     # the stub is never served as a body
    assert body["rewrite_pending"] is True
    for _ in range(50):
        if queued:
            break
        asyncio.run(asyncio.sleep(0.01))
    assert queued == [5]
    main._ondemand_inflight.clear()


def test_full_endpoint_reports_why_and_how_under_their_own_names(db):
    c = db()
    c.execute("UPDATE articles SET full_body=?, summary_60='ok summary here', "
              "why_info='Because of X.', how_info='Step one, then two.' WHERE id=5",
              (GOOD_BODY,))
    c.commit()
    body = TestClient(main.app).get("/article/5/full").json()
    assert body["wwww"]["why"] == "Because of X."
    assert body["wwww"]["how"] == "Step one, then two."
    assert body["rewrite_pending"] is False
