"""test_filings_classify.py — filing type -> event_class is a RULE table.

A filing's class feeds the analog matcher's class_match weight, so it must be
deterministic and driven by the source's own category labels — never an LLM.
This pins the mapping for the category strings BSE/NSE/RBI/SEBI actually emit,
and guarantees every output is a member of the closed taxonomy the schema CHECK
and the matcher share.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))          # sherrbyte/ -> import app.*

from app.spie.analog.event_library import EVENT_CLASSES     # noqa: E402
from app.spie.filings.classify import classify_filing        # noqa: E402


def test_category_labels_map_to_expected_classes():
    cases = {
        # BSE / NSE category labels
        "Result": "earnings",
        "Financial Results": "earnings",
        "Board Meeting": "earnings",
        "Change in Directors": "leadership_change",
        "Resignation of Company Secretary": "leadership_change",
        "Acquisition": "m_and_a",
        "Scheme of Arrangement": "m_and_a",
        "Substantial Acquisition of Shares (SAST)": "m_and_a",
        "Credit Rating": "default_credit",
        "Capacity Expansion": "guidance_change",
        "Plant Shutdown": "supply_disruption",
        # Regulator titles
        "RBI announces Monetary Policy Statement, repo rate unchanged": "central_bank_policy",
        "SEBI settlement order in the matter of XYZ Ltd": "regulatory_action",
    }
    for label, expected in cases.items():
        assert classify_filing("BSE", label) == expected, label


def test_unknown_maps_to_other():
    assert classify_filing("BSE", "Trading Window Closure") == "other"
    assert classify_filing("NSE", "") == "other"


def test_every_output_is_in_the_closed_taxonomy():
    samples = ["Result", "Acquisition", "Credit Rating", "Monetary Policy",
               "Change in Directors", "random noise", "", "Plant Shutdown"]
    for s in samples:
        assert classify_filing("BSE", s) in EVENT_CLASSES


def test_most_specific_rule_wins():
    # An RBI monetary-policy release also contains 'press release'-ish wording,
    # but central_bank_policy is ordered first and must win.
    assert classify_filing(
        "RBI", "Press Release", "Monetary Policy Committee keeps repo rate steady"
    ) == "central_bank_policy"


def test_regulator_release_never_reaches_the_company_rules():
    """An RBI KYC amendment DIRECTION is a regulatory_action — never m_and_a,
    even when bulk RSS text drags in a stray 'acquisition'/'merger'. Regulators
    are classified against central_bank_policy / regulatory_action / sanctions
    only, so a company class cannot be emitted for them."""
    got = classify_filing(
        "RBI",
        "Amendment to Master Direction - Know Your Customer (KYC) Directions",
        "Bulk feed also mentions an acquisition and a merger elsewhere")
    assert got == "regulatory_action"


def test_regulator_only_ever_emits_regulator_classes():
    allowed = {"central_bank_policy", "regulatory_action", "sanctions", "other"}
    samples = ["Acquisition of majority stake", "Financial Results Q1",
               "Change in Directors", "Credit Rating downgrade",
               "Plant Shutdown", "Capacity Expansion", "random noise"]
    for src in ("RBI", "SEBI"):
        for s in samples:
            assert classify_filing(src, s) in allowed, (src, s)


def test_sebi_felicitation_press_release_stays_other():
    """A SEBI felicitation carries no category signal; 'press release' is not one,
    so it must default to 'other' rather than being guessed into a class."""
    assert classify_filing(
        "SEBI", "SEBI felicitates awardees at its annual function",
        "Press release") == "other"
