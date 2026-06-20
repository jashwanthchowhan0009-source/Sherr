from app.pipeline.understander import (
    classify_pillar, classify_scope, extract_entities,
)


def test_classify_pillar_sports():
    pid, tags = classify_pillar("Virat Kohli scores century in IPL final", "cricket match")
    assert pid == 9
    assert any("cricket" in t.lower() or "ipl" in t.lower() for t in tags)


def test_classify_pillar_economy():
    pid, _ = classify_pillar("Sensex and Nifty hit record on RBI rate decision", "stock market rally")
    assert pid == 2


def test_classify_pillar_defaults_to_society_when_unknown():
    # No keyword match and no source hint -> neutral general-news bucket.
    pid, tags = classify_pillar("zzz qqq", "nothing relevant here")
    assert pid == 1
    assert tags == []


def test_classify_pillar_uses_source_hint_when_unknown():
    # No keyword match, but the source feed tells us the natural pillar.
    assert classify_pillar("zzz qqq", "no keywords", "BBC Sport")[0] == 9
    assert classify_pillar("zzz qqq", "no keywords", "NYT Business")[0] == 2
    assert classify_pillar("zzz qqq", "no keywords", "Healthline")[0] == 6


def test_classify_scope_national_vs_global():
    assert classify_scope("India PM Modi addresses parliament in Delhi", "") == "national"
    assert classify_scope("China and Russia hold talks in Moscow", "") == "global"


def test_extract_entities_regex_fallback():
    ents = extract_entities("Narendra Modi met Joe Biden in Washington.")
    names = {e.name for e in ents}
    assert any("Modi" in n for n in names)
