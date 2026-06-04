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


def test_classify_pillar_defaults_to_tech_when_unknown():
    pid, tags = classify_pillar("zzz qqq", "nothing relevant here")
    assert pid == 3
    assert tags == []


def test_classify_scope_national_vs_global():
    assert classify_scope("India PM Modi addresses parliament in Delhi", "") == "national"
    assert classify_scope("China and Russia hold talks in Moscow", "") == "global"


def test_extract_entities_regex_fallback():
    ents = extract_entities("Narendra Modi met Joe Biden in Washington.")
    names = {e.name for e in ents}
    assert any("Modi" in n for n in names)
