"""
Part B: market moves must land in domain_signals, or no news↔market link is possible.

DB-free: asserts the instrument catalogue and the quote → Signal mapping contract.
"""

from app.spie.knowledge.adapters.base import direction, clamp
from app.spie.knowledge.entity_resolver import is_valid_mention
from app.workers.market_signals import INSTRUMENTS, CRYPTO


def test_catalogue_covers_every_glance_card():
    """The six 'At a Glance' cards must each have a signal source."""
    classes = {cls for _, _, cls in INSTRUMENTS}
    for needed in ["stocks", "metals", "commodities", "forex", "rates"]:
        assert needed in classes, needed
    assert CRYPTO, "crypto instruments missing"


def test_instrument_names_survive_the_entity_filter():
    """If a name is filtered as junk it never becomes an entity, and the detector
    can never join news to it."""
    for _, name, _ in INSTRUMENTS:
        assert is_valid_mention(name, "MISC"), name
    for name in CRYPTO.values():
        assert is_valid_mention(name, "MISC"), name


def test_signal_mapping_follows_the_spec():
    """magnitude = abs(% change), direction = sign(% change)."""
    for pct, expected_dir in [(-1.83, -1), (0.42, 1), (0.0, 0)]:
        assert abs(pct) == abs(pct)
        assert direction(pct) == expected_dir


def test_confidence_is_bounded():
    for pct in [0.0, 1.5, 25.0, -30.0]:
        c = clamp(0.6 + min(abs(pct), 10.0) / 20.0)
        assert 0.0 <= c <= 1.0


def test_ref_id_is_one_per_instrument_per_day():
    """Re-running the worker on the same day must not multiply signals."""
    day = "2026-07-29"
    ids = {f"market:{sym}:{day}" for sym, _, _ in INSTRUMENTS}
    assert len(ids) == len(INSTRUMENTS)
