"""Phases 2 and 3 of the analog engine: ranking, and the reaction statistics.

The single most important test in this file is
test_the_volatility_window_cannot_see_the_move_itself. Lookahead here would be
invisible: the numbers stay plausible and become meaningless, because a move
that inflates its own denominator scores as unremarkable. Everything else is
recoverable by reading output; that one is not.
"""
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "sherrbyte"))

from app.spie.analog import matcher as M      # noqa: E402
from app.spie.analog import reaction as R     # noqa: E402

UTC = timezone.utc
E1 = "11111111-1111-1111-1111-111111111111"
E2 = "22222222-2222-2222-2222-222222222222"
E3 = "33333333-3333-3333-3333-333333333333"


# ══ PHASE 2: ranking ═════════════════════════════════════════════════════════

def test_entity_jaccard_is_intersection_over_union():
    assert M.entity_jaccard([E1, E2], [E1, E2]) == 1.0
    assert M.entity_jaccard([E1, E2], [E2, E3]) == pytest.approx(1 / 3)
    assert M.entity_jaccard([E1], [E2]) == 0.0


def test_entity_jaccard_is_zero_when_a_side_is_empty():
    """An event with no entities must not score as similar to anything."""
    assert M.entity_jaccard([], [E1]) == 0.0
    assert M.entity_jaccard([E1], []) == 0.0


def test_npmi_is_normalised_against_the_engine_threshold_not_minus_one():
    """A pair sitting exactly on the engine's 'beyond chance' bar earns nothing;
    only association ABOVE the bar carries weight."""
    assert M.npmi_strength(M.MIN_NPMI) == 0.0
    assert M.npmi_strength(1.0) == 1.0
    assert M.npmi_strength(0.0) == 0.0          # below the bar, clamped
    assert M.npmi_strength(None) == 0.0
    mid = M.npmi_strength((M.MIN_NPMI + 1.0) / 2)
    assert 0.49 < mid < 0.51


def test_the_weights_are_the_agreed_ones_and_sum_to_one():
    assert (M.W_ENTITY, M.W_CLASS, M.W_NPMI) == (0.45, 0.35, 0.20)
    assert M.W_ENTITY + M.W_CLASS + M.W_NPMI == pytest.approx(1.0)


def _code_only(module) -> str:
    """Source with comments and docstrings stripped.

    These bans are about what the code DOES. The modules explain at length why
    the vector term was dropped and why the score is not a confidence, and
    matching those explanations was a false positive — the naive grep failed on
    the very prose that documents the decision.
    """
    import io                                                  # noqa: PLC0415
    import tokenize                                            # noqa: PLC0415
    src = open(module.__file__).read()
    out, prev_type = [], tokenize.INDENT
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            continue
        if tok.type == tokenize.STRING and prev_type in (
                tokenize.INDENT, tokenize.DEDENT, tokenize.NEWLINE,
                tokenize.NL, tokenize.ENCODING):
            continue                                            # a docstring
        out.append(tok.string)
        if tok.type not in (tokenize.NL, tokenize.NEWLINE):
            prev_type = tok.type
    return " ".join(out).lower()


def test_there_is_no_vector_term_anywhere_in_the_matcher():
    """The decision that embeddings are a hash fallback, enforced in code."""
    code = _code_only(M)
    for banned in ("pgvector", "cosine", "embedding", "sentence_transformers"):
        assert banned not in code, banned


def test_a_perfect_match_scores_one():
    assert M.score(entity_ids=[E1, E2], event_class="earnings",
                   cand_entity_ids=[E1, E2], cand_class="earnings",
                   npmi=1.0) == pytest.approx(1.0)


def test_class_match_alone_cannot_outrank_entity_overlap_plus_class():
    same_class_only = M.score(entity_ids=[E1], event_class="earnings",
                              cand_entity_ids=[E2], cand_class="earnings",
                              npmi=M.MIN_NPMI)
    both = M.score(entity_ids=[E1], event_class="earnings",
                   cand_entity_ids=[E1], cand_class="earnings",
                   npmi=M.MIN_NPMI)
    assert both > same_class_only


def test_recency_is_not_a_matcher_term():
    """It belongs to Phase 3's signal_strength; in both places it double-counts."""
    assert "recency" not in _code_only(M)


# ─── the 48h cluster collapse ────────────────────────────────────────────────

def _cand(sym, hours_ago, sim, klass="earnings"):
    return {"linked_symbols": [sym] if isinstance(sym, str) else list(sym),
            "occurred_at": datetime(2026, 6, 1, tzinfo=UTC) - timedelta(hours=hours_ago),
            "similarity": sim, "event_class": klass}


def test_a_republished_story_collapses_to_its_best_version():
    kept = M._collapse_clusters([_cand("BZ=F", 0, 0.9), _cand("BZ=F", 6, 0.4)])
    assert len(kept) == 1 and kept[0]["similarity"] == 0.9


def test_two_events_outside_the_window_both_survive():
    kept = M._collapse_clusters([_cand("BZ=F", 0, 0.9), _cand("BZ=F", 72, 0.8)])
    assert len(kept) == 2


def test_the_collapse_is_per_symbol():
    """Same moment, different instruments — two separate pieces of evidence."""
    kept = M._collapse_clusters([_cand("BZ=F", 0, 0.9), _cand("GC=F", 0, 0.8)])
    assert len(kept) == 2


def test_a_multi_symbol_event_survives_if_any_symbol_is_still_free():
    """Dropping it for one overlap would lose the evidence it carries for the
    other symbol."""
    kept = M._collapse_clusters([
        _cand("BZ=F", 0, 0.9),
        _cand(["BZ=F", "GC=F"], 1, 0.5),
    ])
    assert len(kept) == 2


def test_a_multi_symbol_event_is_dropped_when_every_symbol_is_claimed():
    kept = M._collapse_clusters([
        _cand("BZ=F", 0, 0.9), _cand("GC=F", 0, 0.8),
        _cand(["BZ=F", "GC=F"], 1, 0.5),
    ])
    assert len(kept) == 2


# ══ PHASE 3: the reaction math ═══════════════════════════════════════════════

def _flat_then_jump(n=120, jump_at=100, jump=0.25):
    """A quiet series with one large move, so z is unambiguous.

    The wiggle is deterministic pseudo-random rather than a clean alternation:
    a perfectly alternating +0.1%/-0.1% series has a MAD of exactly ZERO (half
    the absolute deviations are 0, half are 0.002, so their median is 0), which
    the zero-volatility guard correctly refuses. Real prices are never that
    tidy; the fixture should not be either.

    `jump_at` is the index whose CLOSE carries the jump, so an event anchored at
    jump_at - 1 sees it in its forward window.
    """
    import random                                              # noqa: PLC0415
    rng = random.Random(20260901)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    out, price = [], 100.0
    for i in range(n):
        price *= 1.0 + rng.uniform(-0.004, 0.004)
        if i == jump_at:
            price *= (1 + jump)
        out.append((base + timedelta(days=i), price))
    return out


def test_statistics_helpers():
    assert R.median([3, 1, 2]) == 2
    assert R.median([4, 1, 2, 3]) == 2.5
    assert R.median([]) == 0.0
    assert R.mad([1, 1, 1, 1]) == 0.0
    assert R.iqr([1, 2, 3, 4, 5]) == pytest.approx(2.0)
    assert R.log_return(100, 110) == pytest.approx(math.log(1.1))
    assert R.log_return(0, 10) is None
    assert R.log_return(10, -1) is None


# ─── THE ONE THAT MUST NOT BE WRONG ──────────────────────────────────────────

def test_the_volatility_window_cannot_see_the_move_itself():
    """NO LOOKAHEAD.

    Two series identical up to and including the event date, differing only in
    what happens AFTER it. The trailing volatility — and therefore the
    denominator — must be byte-identical, because at the moment of the event the
    future had not happened yet.

    If this ever fails, every z in the system is wrong in the direction that
    hides real moves.
    """
    quiet = _flat_then_jump(n=120, jump_at=999)          # no jump at all
    violent = list(quiet)
    # Rewrite everything strictly after the event with enormous moves.
    event_idx = 80
    price = violent[event_idx][1]
    for i in range(event_idx + 1, len(violent)):
        price *= 1.5
        violent[i] = (violent[i][0], price)

    ts = quiet[event_idx][0]
    a = R.measure(quiet, ts, 1)
    b = R.measure(violent, ts, 1)
    assert a["ok"] and b["ok"]
    assert a["mad_sigma"] == b["mad_sigma"], "trailing volatility saw the future"


def test_the_trailing_window_ends_at_the_anchor_inclusive():
    """One row of leakage is still leakage."""
    series = _flat_then_jump(n=120, jump_at=999)
    idx = 80
    got = R.measure(series, series[idx][0], 1)
    # 60-session window over 60 rows yields 59 usable one-day returns.
    assert got["trailing_sessions"] == R.VOL_WINDOW - 1


def test_a_big_move_scores_a_large_z():
    """Anchored at 99: the jump lands in the forward window, not in the anchor
    price. Anchoring AT the jump would measure the day after it instead."""
    series = _flat_then_jump(jump_at=100, jump=0.25)
    got = R.measure(series, series[99][0], 1)
    assert got["ok"] and abs(got["z"]) > R.Z_EXCEEDED


def test_a_normal_day_scores_a_small_z():
    series = _flat_then_jump(jump_at=999)
    got = R.measure(series, series[99][0], 1)
    assert got["ok"] and abs(got["z"]) < R.Z_EXCEEDED


def test_horizons_are_trading_rows_not_calendar_days():
    series = _flat_then_jump(n=120, jump_at=999)
    got = R.measure(series, series[70][0], 5)
    assert got["target_ts"] == series[75][0]


def test_an_event_between_sessions_anchors_to_the_prior_close():
    """A Saturday event is measured from Friday, not skipped."""
    series = _flat_then_jump(n=120, jump_at=999)
    between = series[70][0] + timedelta(hours=7)
    assert R.measure(series, between, 1)["anchor_ts"] == series[70][0]


def test_too_few_trailing_sessions_drops_the_cell():
    """45 of 60 is the floor; an event too near the start of the series has no
    trustworthy denominator."""
    series = _flat_then_jump(n=120, jump_at=999)
    got = R.measure(series, series[10][0], 1)
    assert not got["ok"] and "trailing sessions" in got["reason"]


def test_no_room_after_the_event_drops_the_cell():
    series = _flat_then_jump(n=120, jump_at=999)
    got = R.measure(series, series[-1][0], 10)
    assert not got["ok"] and "after the event" in got["reason"]


def test_a_flat_instrument_has_no_normal_range_to_be_unusual_against():
    base = datetime(2026, 1, 1, tzinfo=UTC)
    flat = [(base + timedelta(days=i), 100.0) for i in range(120)]
    got = R.measure(flat, flat[80][0], 1)
    assert not got["ok"] and "zero trailing volatility" in got["reason"]


def test_an_event_before_the_series_starts_is_refused():
    series = _flat_then_jump(n=120, jump_at=999)
    got = R.measure(series, datetime(2020, 1, 1, tzinfo=UTC), 1)
    assert not got["ok"] and "predates" in got["reason"]


# ─── aggregation and suppression ─────────────────────────────────────────────

def _cell(z, age=10.0):
    return {"ok": True, "z": z, "r": z / 100.0, "mad_sigma": 0.01,
            "age_days": age, "event_id": "e", "occurred_at": "2026-06-01"}


def test_a_thin_sample_is_suppressed_entirely():
    """Four analogs produce NOTHING — not a low score, not a zeroed row."""
    assert R.aggregate([_cell(3.0)] * 4) is None


def test_five_analogs_clear_the_floor():
    assert R.aggregate([_cell(3.0)] * 5) is not None


def test_unusable_cells_do_not_count_toward_the_floor():
    cells = [_cell(3.0)] * 4 + [{"ok": False, "reason": "no data"}] * 5
    assert R.aggregate(cells) is None


def test_sign_agreement_measures_the_dominant_direction():
    assert R.aggregate([_cell(3.0)] * 5)["sign_agreement"] == 1.0
    mixed = R.aggregate([_cell(3.0)] * 3 + [_cell(-3.0)] * 2)
    assert mixed["sign_agreement"] == pytest.approx(0.6)


def test_dispersion_is_over_signed_z_so_a_split_sample_reads_wide():
    """Folding the sign would make 'half up, half down' look tight."""
    split = R.aggregate([_cell(3.0)] * 3 + [_cell(-3.0)] * 3)
    same = R.aggregate([_cell(3.0)] * 6)
    assert split["dispersion"] > same["dispersion"]


def test_n_exceeded_counts_only_moves_past_the_threshold():
    agg = R.aggregate([_cell(3.0)] * 3 + [_cell(0.5)] * 3)
    assert agg["n_analogs"] == 6 and agg["n_exceeded"] == 3


# ─── signal_strength ─────────────────────────────────────────────────────────

def test_signal_strength_is_an_integer_zero_to_hundred():
    s = R.signal_strength(n_analogs=20, n_exceeded=20, sign_agreement=1.0,
                          recency=1.0)
    assert isinstance(s, int) and s == 100


def test_a_split_direction_scores_below_a_consistent_one():
    consistent = R.signal_strength(n_analogs=20, n_exceeded=20,
                                   sign_agreement=1.0, recency=1.0)
    split = R.signal_strength(n_analogs=20, n_exceeded=20,
                              sign_agreement=0.5, recency=1.0)
    assert split < consistent


def test_a_small_sample_is_penalised_against_a_large_one():
    small = R.signal_strength(n_analogs=5, n_exceeded=5, sign_agreement=1.0,
                              recency=1.0)
    large = R.signal_strength(n_analogs=15, n_exceeded=15, sign_agreement=1.0,
                              recency=1.0)
    assert small < large


def test_stale_analogs_score_below_fresh_ones():
    fresh = R.signal_strength(n_analogs=20, n_exceeded=20, sign_agreement=1.0,
                              recency=1.0)
    stale = R.signal_strength(n_analogs=20, n_exceeded=20, sign_agreement=1.0,
                              recency=0.0)
    assert stale < fresh


def test_never_exceeding_the_threshold_scores_zero():
    assert R.signal_strength(n_analogs=20, n_exceeded=0, sign_agreement=1.0,
                             recency=1.0) == 0


def test_recency_weight_decays_with_age():
    assert R.recency_weight(0) == 1.0
    assert R.recency_weight(540) == pytest.approx(math.exp(-1))
    assert R.recency_weight(-5) == 1.0          # a future stamp is not a bonus


def test_signal_strength_is_never_called_confidence_or_a_percentage():
    """SEBI posture, enforced in code rather than in review."""
    code = _code_only(R)
    for banned in ("confidence", "probability", "forecast", "target_price",
                   "predict"):
        assert banned not in code, banned


def test_the_migration_enforces_the_same_floor_as_the_code():
    sql = open(os.path.join(
        _ROOT, "sherrbyte/app/db/migrations/023_analog_reactions.sql")).read()
    assert f"n_analogs >= {R.MIN_ANALOGS}" in sql
    assert "signal_strength BETWEEN 0 AND 100" in sql
