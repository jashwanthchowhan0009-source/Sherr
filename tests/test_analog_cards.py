"""Phase 4 and 5: the two things the analog engine can put on screen.

The analog card is the stronger claim and is often silent — below five analogs
it does not exist, and at or below the noise floor it is context rather than
evidence. The observation card exists so the surface is not blank in that case:
one article, one instrument, one measured move, no sample-size floor.

They must never be confused for one another. An analog says "this has happened
before and here is how often"; an observation says only "here is what happened
after this one article". These tests pin that separation, and pin that every
generated string clears the engine's own forbidden-word blocker.
"""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sherrbyte"))

from app.spie.analog import calibration, cards  # noqa: E402
from app.spie.reasoning import narrative  # noqa: E402

ROW = {"symbol": "BZ=F", "event_class": "commodity_shock", "horizon_days": 3,
       "n_analogs": 11, "n_exceeded": 8, "sign_agreement": 0.82,
       "median_abs_z": 2.1, "dispersion": 1.4, "recency_weight": 0.7,
       "signal_strength": 64, "noise_floor": 11}


# ─── the language gate ──────────────────────────────────────────────────────

def test_the_blocker_is_the_engines_own_not_a_second_copy():
    """One blocklist. A second copy would drift, and drift here is a compliance
    failure rather than a style one."""
    src = open(cards.__file__).read()
    assert "violates_language_rules" in src
    assert "FORBIDDEN" not in src.replace("forbidden-word", "")


def test_a_clean_string_passes_and_advice_does_not():
    assert cards.check_language("Brent Crude rose 4.2% within 3 sessions")[0]
    assert not cards.check_language("You should buy crude now")[0]
    assert not cards.check_language("Crude will rally from here")[0]


def test_the_blocker_failing_closed_rejects_rather_than_allows(monkeypatch):
    """If the blocker cannot be loaded we do not get to assume the text is fine.

    sys.modules entry set to None, which is what makes `from ... import
    narrative` raise. Patching __import__ does not work here: the module is
    already imported, so the hook is never consulted — the first version of
    this test passed the text and asserted nothing.
    """
    import app.spie.reasoning as reasoning_pkg
    # BOTH are needed. `from pkg import mod` finds an already-imported submodule
    # as an ATTRIBUTE of the package and never consults sys.modules, so blanking
    # sys.modules alone left the real blocker reachable and the test asserted
    # nothing.
    monkeypatch.setitem(sys.modules, "app.spie.reasoning.narrative", None)
    monkeypatch.delattr(reasoning_pkg, "narrative", raising=False)
    ok, bad = cards.check_language("anything at all")
    assert not ok and "blocker-unavailable" in bad


def test_every_generated_analog_string_clears_the_blocker():
    for strength in (2, 11, 64, 100):
        row = {**ROW, "signal_strength": strength}
        for text in cards.analog_text(row, "Brent Crude"):
            assert not narrative.violates_language_rules(text, ["Brent Crude"]), \
                text


def test_every_generated_observation_string_clears_the_blocker():
    for z, pct in ((3.2, 4.1), (-2.8, -3.5), (1.6, 0.9)):
        for text in cards.observation_text("Brent Crude", z, pct, 3):
            assert not narrative.violates_language_rules(text, ["Brent Crude"]), \
                text


# ─── wording: past tense, no prediction, no percentage-as-probability ───────

def test_the_analog_card_states_a_frequency_not_a_probability():
    headline, _ = cards.analog_text(ROW, "Brent Crude")
    assert "in 8 of them" in headline and "11 comparable" in headline
    assert "%" not in headline


def test_the_card_reports_the_noise_floor_beside_the_score():
    """11 against 11 and 60 against 11 must not read the same."""
    _, weak = cards.analog_text({**ROW, "signal_strength": 11}, "Brent Crude")
    _, strong = cards.analog_text({**ROW, "signal_strength": 64}, "Brent Crude")
    assert "noise floor of 11" in weak and "noise floor of 11" in strong
    assert "context rather than as evidence" in weak
    assert "above what unrelated data reaches" in strong


def test_an_observation_never_claims_a_pattern():
    """It is one measurement. Saying otherwise would make it an analog with a
    sample of one."""
    headline, detail = cards.observation_text("Gold", 3.1, 2.4, 3)
    assert "single observation, not a pattern" in detail
    assert "comparable" not in headline


def test_no_template_contains_a_word_about_what_comes_next():
    src = open(cards.__file__).read()
    lowered = src.lower()
    for banned in ("expect", "likely", "should rise", "should fall", "forecast"):
        assert banned not in lowered, banned


# ─── the cards themselves ───────────────────────────────────────────────────

def test_every_analog_card_carries_its_noise_floor():
    c = cards.AnalogCard(noise_floor=11, signal_strength=64)
    assert c.as_dict()["noise_floor"] == 11


def test_clears_noise_is_strict_at_the_boundary():
    assert not calibration.clears_noise(11, 3)
    assert calibration.clears_noise(12, 3)


def test_the_ticker_is_never_what_a_reader_sees():
    """'CL=F rose 8%' means nothing to anybody."""
    assert cards._display("CL=F") == "WTI Crude"
    assert cards._display("BZ=F") == "Brent Crude"


def test_an_unknown_symbol_falls_back_to_itself_rather_than_blank():
    assert cards._display("ZZZZ") == "ZZZZ"


def test_a_card_whose_text_trips_the_blocker_is_dropped(monkeypatch):
    """A card that cannot be phrased compliantly is not shown at all."""
    c = cards.AnalogCard(display_name="Brent Crude",
                         headline="You should buy this now", detail="fine")
    assert not cards._vet(c)


def test_a_clean_card_survives_vetting():
    c = cards.AnalogCard(display_name="Brent Crude",
                         headline="Brent Crude rose within 3 sessions",
                         detail="Measured against its own normal daily range.")
    assert cards._vet(c)


def test_the_display_name_is_masked_so_a_company_called_target_is_not_dropped():
    """Real names collide with the blocklist; the name is a quoted fact, not
    something the template asserts."""
    c = cards.AnalogCard(display_name="Target",
                         headline="Target rose within 3 sessions",
                         detail="Measured against its own normal daily range.")
    assert cards._vet(c)


# ─── the observation floor is a significance bar, not a sample bar ──────────

def test_the_observation_threshold_is_about_significance_not_sample_size():
    """It has no n_analogs floor by design — that is the whole point — but it
    still refuses to narrate a quiet day."""
    src = open(cards.__file__).read()
    assert "MIN_ANALOGS" not in src.split("async def observation_cards")[1]
    assert cards.OBSERVATION_MIN_Z > 0
