"""video_studio: who gets into the Top-N video, and what the narrator may say.

Pure — no database, no TTS, no ffmpeg. The renderer is checked by eye; what is
pinned here is the part that can be wrong silently: eligibility, ordering, and
that no fixed template trips the engine's compliance blocklist.
"""
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from video_studio import script, stories

ROOT = Path(__file__).resolve().parent.parent
FMT = json.loads((ROOT / "video_studio" / "format.json").read_text())
NOW = datetime(2026, 10, 17, 6, 0, tzinfo=timezone.utc)


def _narrative():
    p = ROOT / "sherrbyte" / "app" / "spie" / "reasoning" / "narrative.py"
    spec = importlib.util.spec_from_file_location("_narrative_for_video", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def row(i, pillar=3, sig=70, hours_ago=2, written=True, **kw):
    r = {"id": i, "headline": f"Our headline number {i} about a real event",
         "source_headline": f"Publisher headline {i}",
         "summary_60": f"Story {i} has a written summary long enough to be read aloud as one sentence. Second sentence here.",
         "pillar_id": pillar, "significance": sig, "story_id": 0,
         "published_at": (NOW - timedelta(hours=hours_ago)).isoformat()}
    if not written:
        r["source_headline"] = r["headline"]
    r.update(kw)
    return r


def test_only_written_stories_are_eligible():
    rows = [row(1), row(2, written=False), row(3, summary_60="Sherr AI is preparing an original summary of this story.")]
    assert [r["id"] for r in stories.rank(rows, FMT, NOW)] == [1]


def test_window_significance_and_order():
    rows = [row(1, sig=60), row(2, sig=90), row(3, sig=10), row(4, sig=95, hours_ago=40)]
    got = [r["id"] for r in stories.rank(rows, FMT, NOW)]
    assert got == [2, 1]          # 3 is below myFeed's floor, 4 is outside 24h


def test_pillar_cap_and_story_thread_dedupe():
    rows = [row(i, pillar=2, sig=90 - i) for i in range(1, 6)]
    rows += [row(10, pillar=5, story_id=7), row(11, pillar=5, story_id=7, sig=60)]
    got = stories.rank(rows, {**FMT, "max_per_pillar": 3}, NOW)
    assert sum(1 for r in got if r["pillar_id"] == 2) == 3
    assert sum(1 for r in got if r.get("story_id") == 7) == 1


def test_never_padded_beyond_what_is_eligible():
    assert len(stories.rank([row(1), row(2)], {**FMT, "count": 10}, NOW)) == 2


def test_countdown_tells_best_story_last():
    segs = script.build([row(1), row(2), row(3)], FMT, hour=9)
    assert [s["kind"] for s in segs] == ["intro", "story", "story", "story", "outro"]
    assert [s["rank"] for s in segs if s["kind"] == "story"] == [3, 2, 1]
    assert "number one" in segs[-2]["text"].lower()


def test_story_line_never_repeats_the_headline():
    r = row(1, hook="Our headline number 1 about a real event.",
            summary_60="A different, informative first sentence for the listener to hear. More.")
    assert script.story_line(r, 30).startswith("A different")


def test_script_is_deterministic():
    rs = [row(i) for i in range(1, 6)]
    assert [s["text"] for s in script.build(rs, FMT, 9)] == [s["text"] for s in script.build(rs, FMT, 9)]


def test_no_template_trips_the_compliance_blocklist():
    nar = _narrative()
    sc = FMT["script"]
    texts = list(sc["intro"].values()) + [sc["first"], sc["second_last"], sc["last"], sc["outro"]]
    texts += sc["middle"] + [a for v in sc["asides"].values() for a in v]
    for t in texts:
        filled = t.format(rank="ten", headline="", line="", aside="", count="ten",
                          mascot="Sherr", brand_spoken="Sherr Byte", brand="SHERRBYTE")
        assert not nar.violates_language_rules(filled), (t, nar.violates_language_rules(filled))
