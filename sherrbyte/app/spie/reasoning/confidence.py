"""
reasoning/confidence.py — M5 evidence combination (SPIE Reasoning Engine).

Confidence answers "how well-evidenced is this reasoning?", NOT "how likely is a
future move" — there is no forecast anywhere in SPIE.

M5 combines independent evidence in LOG-ODDS space rather than averaging:

    logit(confidence) = logit(prior) + Σ weightᵢ · logit(strengthᵢ)

Averaging lets one strong factor mask everything else; log-odds accumulates
evidence multiplicatively in probability space (the naive-Bayes form), so several
independent moderate signals can together justify high confidence while any single
one cannot. Every factor's contribution is returned so the card shows WHY.

WEIGHTS LIVE IN CONFIG (WEIGHTS below / settings override), never inline in the
reasoning code, so they can be tuned by the eval loop without touching logic.
"""

from __future__ import annotations

from app.spie.reasoning.methods import combine_log_odds

# Tunable evidence weights (config, not hardcoded at the call site).
WEIGHTS: dict[str, float] = {
    "source_diversity": 1.0,      # independent outlets corroborating
    "npmi_strength": 0.9,         # M1 association beyond chance
    "lag_evidence": 0.8,          # M2 news genuinely preceded the move
    "historical_consistency": 0.8,  # M3 same-direction follow-through
    "cross_market": 0.7,          # M7 several asset classes on one driver
}

# Base rate before any evidence — deliberately low so a bare move is not "confident".
PRIOR = 0.25

# A single prior instance is not evidence; pin thin history low rather than
# letting 1/1 read as certainty.
THIN_HISTORY_STRENGTH = 0.30
MIN_HISTORY = 2


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def build_factors(*, source_count: int, npmi_values: list[float],
                  similar_count: int, followed_count: int, co_moving: int,
                  lag_result: dict | None = None) -> list[dict]:
    """Normalize raw evidence into 0..1 strengths with human-readable detail."""
    diversity = _clamp(min(int(source_count or 0), 6) / 6)

    vals = [v for v in (npmi_values or []) if v is not None]
    npmi = _clamp(sum(vals) / len(vals)) if vals else 0.05

    if int(similar_count or 0) >= MIN_HISTORY:
        history = _clamp(int(followed_count or 0) / int(similar_count))
        hist_detail = f"{int(followed_count)} of {int(similar_count)} prior clusters"
    else:
        history = THIN_HISTORY_STRENGTH
        hist_detail = "limited history"

    breadth = _clamp(min(int(co_moving or 0), 3) / 3)

    lag_result = lag_result or {}
    if lag_result.get("passed"):
        lag_strength = _clamp(abs(float(lag_result.get("rho") or 0.0)))
        lag_detail = f"news led by {lag_result.get('lag')}d (rho {lag_result.get('rho')})"
    else:
        lag_strength = 0.10          # not disqualifying, but no credit either
        lag_detail = lag_result.get("reason", "no lag evidence")

    return [
        {"name": "source_diversity", "strength": diversity,
         "weight": WEIGHTS["source_diversity"],
         "detail": f"{int(source_count or 0)} sources"},
        {"name": "npmi_strength", "strength": npmi, "weight": WEIGHTS["npmi_strength"],
         "detail": f"mean NPMI {round(npmi, 2)}"},
        {"name": "lag_evidence", "strength": lag_strength,
         "weight": WEIGHTS["lag_evidence"], "detail": lag_detail},
        {"name": "historical_consistency", "strength": history,
         "weight": WEIGHTS["historical_consistency"], "detail": hist_detail},
        {"name": "cross_market", "strength": breadth, "weight": WEIGHTS["cross_market"],
         "detail": f"{int(co_moving or 0)} co-moving markets"},
    ]


def evaluate(**kwargs) -> dict:
    """Full M5 result: {confidence, log_odds, prior, breakdown}."""
    return combine_log_odds(build_factors(**kwargs), prior=PRIOR)


def score(**kwargs) -> float:
    """Just the calibrated confidence, 0..1."""
    return evaluate(**kwargs)["confidence"]


def components(**kwargs) -> dict:
    """Back-compat view: {factor_name: strength}."""
    return {f["name"]: f["strength"] for f in build_factors(**kwargs)}
