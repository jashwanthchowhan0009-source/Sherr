"""proof/signals.py — the signals on the RIL edges, and what "moved" means.

A signal is one of two things, and both answer the same yes/no question — "did
this move in the window?":

  PRICE signal   an instrument in market_ticks. It moved on a session when that
                 session's return cleared the instrument's OWN normal daily
                 range, measured with NO LOOKAHEAD — the volatility it is judged
                 against comes only from sessions strictly before it. This is the
                 same discipline as analog/reaction.py, and it reuses that
                 module's primitives so there is one definition of "beyond normal
                 range", not two.

  NEWS signal    a topic (refining margins, telecom regulation, competitor
                 capacity). It moved when enough FINANCIAL-SOURCE articles about
                 the topic landed in the window. "Financial-source" is the whole
                 point of the feed separation — a video-game guide that says
                 "silver" is not in the corpus the signal path reads, so it can
                 never be the second signal that makes an edge fire.

The move THRESHOLD is Z_MOVE. It is deliberately the same 2.5 that reaction.py
uses for "exceeded its normal range" (SHERR_I_ANALOG_Z_EXCEEDED), so a signal
"moving" here and RIL's forward move "exceeding" there mean the same thing.

No percentages are produced anywhere in this module.
"""

from __future__ import annotations

import os

from app.spie.analog import reaction as R

# A move is a session return at least this many MAD-sigmas from flat. Shared with
# reaction.py's exceedance test on purpose — one bar, one meaning.
Z_MOVE = float(os.getenv("SHERR_I_PROOF_Z_MOVE", str(R.Z_EXCEEDED)))

# A news topic needs at least this many real financial-source articles in the
# window before it counts as having moved. Two independent desks, not one.
MIN_NEWS_ARTICLES = int(os.getenv("SHERR_I_PROOF_MIN_NEWS", "2"))

# The window an edge is evaluated over: +/- this many hours around the session.
# Never widened — a match found by relaxing the window is evidence about the
# window, not about the edge (the same rule news_match.py holds to).
WINDOW_HOURS = int(os.getenv("SHERR_I_PROOF_WINDOW_HOURS", "36"))


# ─── price signals: symbol in market_ticks ───────────────────────────────────
# key -> (market_ticks symbol, display label). RELIANCE.NS is the only single
# stock the proof adds; Brent and USD/INR are already in the tick universe.
PRICE_SIGNALS: dict[str, tuple[str, str]] = {
    "ril":    ("RELIANCE.NS", "Reliance Industries"),
    "brent":  ("BZ=F",        "Brent Crude"),
    "usdinr": ("USDINR=X",    "USD/INR"),
}

RIL_SYMBOL = PRICE_SIGNALS["ril"][0]


# ─── news signals: a topic, matched only against FINANCIAL_SOURCES ───────────
# Terms are lowercase substrings tested against headline + summary. They lean
# specific — the feed whitelist already narrows the corpus to financial desks,
# and these narrow it to the topic. A bare "oil" or "gas" is left out on purpose;
# it would match half a markets desk.
NEWS_SIGNALS: dict[str, list[str]] = {
    "refining_margin": [
        "refining margin", "gross refining margin", "grm", "crack spread",
        "o2c", "oil-to-chemicals", "oil to chemicals", "petrochemical",
        "refinery throughput", "product cracks",
    ],
    "telecom_regulation": [
        "trai", "telecom regulation", "telecom tariff", "spectrum",
        "arpu", "agr dues", "dot ", "telecom regulator", "tariff hike",
    ],
    "competitor_capacity": [
        "refinery capacity", "petrochemical capacity", "capacity addition",
        "capacity expansion", "bpcl", "hpcl", "indian oil", "iocl",
        "nayara energy", "new refinery",
    ],
}


def is_price_signal(key: str) -> bool:
    return key in PRICE_SIGNALS


def is_news_signal(key: str) -> bool:
    return key in NEWS_SIGNALS


# ─── the no-lookahead daily move ─────────────────────────────────────────────
def daily_move(series: list, session_ts) -> dict | None:
    """The return INTO `session_ts`, scored against the sessions before it.

    `series` is [(ts, close), ...] ascending, the instrument's own closes.
    Returns {"ok": True, "z", "r", "sigma"} when the session's move can be
    scored, or None when it cannot (no previous close, too little trailing
    history, or a flat instrument with no normal range to be unusual against).

    THE NO-LOOKAHEAD BOUNDARY. The trailing window is series[.. idx] — up to but
    NOT including the session being scored — so the day's own move never inflates
    the denominator it is judged by. Same rule as reaction.measure, different
    direction (this scores the move that happened, that one scores the move that
    followed).
    """
    if not series:
        return None
    idx = R._anchor_index(series, session_ts)
    if idx is None or idx < 1:
        return None

    r = R.log_return(series[idx - 1][1], series[idx][1])
    if r is None:
        return None

    trailing = series[max(0, idx - R.VOL_WINDOW):idx]     # strictly before idx
    rets = []
    for i in range(1, len(trailing)):
        lr = R.log_return(trailing[i - 1][1], trailing[i][1])
        if lr is not None:
            rets.append(lr)
    if len(rets) < R.MIN_VOL_SESSIONS:
        return None

    sigma = R._MAD_TO_SIGMA * R.mad(rets)
    if sigma <= 0:
        return None

    return {"ok": True, "r": r, "sigma": sigma, "z": r / sigma}


def price_moved(series: list, session_ts) -> dict | None:
    """A price signal's move for the session, only if it cleared Z_MOVE."""
    m = daily_move(series, session_ts)
    if not m or abs(m["z"]) < Z_MOVE:
        return None
    return m


def price_moved_in_window(series: list, center_ts, window_hours: int) -> dict | None:
    """The strongest qualifying move within +/- window_hours of center_ts.

    "Same window", not "same session": the four edges connect an RIL arm to an
    input (crude, the rupee), and those do not have to print their unusual move
    on the identical session for the edge to have transmitted — a rupee shock and
    RIL's reaction to it land a session apart. So a price signal counts as having
    moved when ANY session inside the window cleared Z_MOVE, and the strongest
    such move (by |z|) is the one returned. The window is fixed at WINDOW_HOURS
    and never widened — a co-move found only by relaxing it is evidence about the
    window, not the edge.

    THE WINDOW IS TRAILING: [center - window_hours, center], never into the
    future. This is not a nicety — the firing's forward RIL reaction is measured
    from `center` onward, so a move at center+1 that both triggered the firing AND
    fell inside a forward horizon would be pure circularity. A trailing window
    makes every triggering move at or before the anchor, and every measured
    reaction strictly after it.
    """
    if not series:
        return None
    from datetime import timedelta
    lo = center_ts - timedelta(hours=window_hours)
    hi = center_ts
    best = None
    for ts, _price in series:
        if ts < lo or ts > hi:
            continue
        m = daily_move(series, ts)
        if not m or abs(m["z"]) < Z_MOVE:
            continue
        if best is None or abs(m["z"]) > abs(best["z"]):
            best = m
    return best


def firing_strength(price_moves: list, n_moved: int) -> int:
    """A 0-100 integer describing how strong THIS firing was at fire time.

    Math only, no lookahead, never a percentage. It scales with how far beyond
    normal the priced signals moved (their |z| over the Z_MOVE bar) and with how
    many signals lined up. A firing on two just-barely moves scores low; two
    violent moves score high. News-only signals contribute the count but no
    magnitude — a headline has no z.
    """
    if n_moved <= 0:
        return 0
    zs = [abs(m["z"]) for m in price_moves if m]
    # Magnitude term: mean exceedance ratio, capped so one huge move can't peg it.
    mag = (sum(min(z / Z_MOVE, 3.0) for z in zs) / len(zs)) if zs else 1.0
    base = 100.0 * (mag / 3.0)
    # Corroboration term: more signals on the same edge is stronger evidence.
    base *= min(1.0, n_moved / 3.0) + (0.34 if n_moved >= 2 else 0.0)
    return int(round(max(0.0, min(100.0, base))))
