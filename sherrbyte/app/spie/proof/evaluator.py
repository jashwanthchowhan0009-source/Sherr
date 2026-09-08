"""proof/evaluator.py — the one question the engine asks each edge.

"Did two or more of this edge's signals move in the same window?"

That is the whole engine. It reads the hand-authored edges, checks each edge's
signals against the prices and the financial corpus for a session, and emits a
Firing when two or more moved. It never invents an edge and never widens the
window to manufacture a coincidence.

Kept pure and input-driven so the backwards run can evaluate ~600 sessions
without a database round-trip per session: prices are loaded once into
`series_by_symbol`, and the financial articles for each window are matched in
memory.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.spie.analog.calibration import noise_floor
from app.spie.proof import data as D
from app.spie.proof import signals as S


@dataclass
class Firing:
    edge_key: str
    event_date: object                    # datetime.date
    moved_signals: list                   # signal keys that moved
    evidence_article_ids: list = field(default_factory=list)
    signal_strength: int = 0
    noise_floor: int = 0
    rendered: bool = False


def evaluate_session(edges: list, series_by_symbol: dict, session_ts,
                     articles_window: list) -> list:
    """Every edge that fired on this session.

    `edges`               rows from ril_edges (dicts with signal_keys).
    `series_by_symbol`    {symbol: [(ts, close), ...]} for the price signals.
    `session_ts`          the session being evaluated (a datetime).
    `articles_window`     financial-source articles already narrowed to this
                          session's +/- WINDOW_HOURS window.
    """
    session_date = session_ts.date() if hasattr(session_ts, "date") else session_ts
    firings = []

    for edge in edges:
        moved, price_moves, evidence = [], [], []
        for key in edge["signal_keys"]:
            if S.is_price_signal(key):
                sym = S.PRICE_SIGNALS[key][0]
                m = S.price_moved_in_window(series_by_symbol.get(sym, []),
                                            session_ts, S.WINDOW_HOURS)
                if m:
                    moved.append(key)
                    price_moves.append(m)
            elif S.is_news_signal(key):
                ids = D.match_topic(articles_window, S.NEWS_SIGNALS[key])
                if len(ids) >= S.MIN_NEWS_ARTICLES:
                    moved.append(key)
                    evidence.extend(ids)

        # The rule: two or more signals on the SAME edge in the SAME window.
        if len(moved) < 2:
            continue

        strength = S.firing_strength(price_moves, len(moved))
        floor = noise_floor(1)            # the reader's bar at the daily horizon
        firings.append(Firing(
            edge_key=edge["edge_key"],
            event_date=session_date,
            moved_signals=moved,
            evidence_article_ids=sorted(set(evidence)),
            signal_strength=strength,
            noise_floor=floor,
            # A card renders only above the measured floor; at or below it the
            # firing is logged as context, never suppressed from the log itself.
            rendered=strength > floor,
        ))

    return firings
