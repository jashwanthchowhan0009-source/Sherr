"""Two Sherr-I exposure-brief guards.

1. SEBI posture (brief standing rule 3): the score is `signal_strength`, an
   integer 0-100, NEVER the word "confidence" and NEVER rendered as a
   percentage. The reasoning narrative used to print `Confidence: moderate (5%)`
   — a probability-looking number on the card, which is the one thing this
   engine may not imply. Assert the rendered narrative never does that again.

2. Corpus gate (brief Phase A1): every Sherr-I query that reads the news corpus
   as financial EVIDENCE must carry the structural financial filter — the
   `source_name = ANY($financial)` whitelist or `feed_class = 'financial'` — so
   a general-news row (the silver-move-linked-to-a-video-game-guide bug) can
   never reach the signal path. Fails if any evidence-read query omits it.
"""
import os
import re

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPIE = os.path.join(_ROOT, "sherrbyte", "app", "spie")


def _read(p):
    with open(p, encoding="utf-8") as fh:
        return fh.read()


# ── 1. compliance: signal_strength is never "confidence (N%)" ────────────────

def test_narrative_renders_signal_strength_not_a_percentage():
    import importlib.util
    import sys
    sb = os.path.join(_ROOT, "sherrbyte")
    if sb not in sys.path:
        sys.path.insert(0, sb)
    path = os.path.join(SPIE, "reasoning", "narrative.py")
    spec = importlib.util.spec_from_file_location("_narr_test", path)
    m = importlib.util.module_from_spec(spec)
    sys.modules["_narr_test"] = m
    spec.loader.exec_module(m)

    # A reasoned payload with a mid confidence — the exact shape that produced
    # "Confidence: moderate (5%)".
    r = {
        "instrument": "Brent", "confidence": 0.05,
        "evidence": {"articles": 3, "sources": 2},
    }
    text = m.build_narrative(r)
    # It must state the score out of 100...
    assert "signal strength" in text.lower()
    # ...and must NOT print it as a percentage or label it "confidence".
    assert "5%)" not in text
    assert not re.search(r"[Cc]onfidence:\s*\w+\s*\(\d+%\)", text)


def test_narrative_source_has_no_confidence_percent_render():
    src = _read(os.path.join(SPIE, "reasoning", "narrative.py"))
    # The specific violating render must be gone; move_pct (a real price move)
    # may still legitimately use '%', so this targets the score line only.
    assert 'f"Confidence: {confidence_word(conf)} ({round(conf * 100)}%)' not in src


# ── 2. corpus filter: every evidence read carries the financial gate ─────────

# Modules that read the corpus (articles / info_objects) as FINANCIAL evidence.
# Each must gate; the test names them so a new evidence reader is a deliberate
# addition here, not a silent omission.
_EVIDENCE_MODULES = [
    ("discovery", "news_match.py"),
    ("discovery", "market_reaction.py"),
    ("analog", "event_library.py"),
    ("proof", "data.py"),
]

_GATE_TOKENS = ("source_name = ANY", "feed_class = 'financial'",
                "feed_class='financial'", "financial_sources(")


def test_every_evidence_reader_carries_the_financial_gate():
    missing = []
    for parts in _EVIDENCE_MODULES:
        path = os.path.join(SPIE, *parts)
        src = _read(path)
        # Does this module SELECT from the corpus at all?
        reads_corpus = re.search(r"FROM\s+(sherrbyte_app\.)?articles\b", src) \
            or re.search(r"FROM\s+info_objects\b", src)
        if not reads_corpus:
            continue
        if not any(tok in src for tok in _GATE_TOKENS):
            missing.append("/".join(parts))
    assert not missing, (
        "Sherr-I evidence readers missing the financial filter "
        "(feed_class / source_name whitelist): " + ", ".join(missing))
