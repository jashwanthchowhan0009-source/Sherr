"""
reasoning/methods.py — the M1–M7 mathematical methods (SPIE Reasoning Engine).

Pure, deterministic, stdlib-only — every method here is unit-testable without a DB.
Reference: docs/SPIE_REASONING_MATH.md. Only M1–M7 are implemented; anything the
reference marks V2/V3/DEFERRED is deliberately absent.

  M1 NPMI                    association beyond chance (see graph/cooccurrence)
  M2 lagged Spearman         did news precede the move, and at what lag
  M3 pgvector cosine         historical echo (executed in engine.py against the DB)
  M4 degree / PageRank       which connected entity is central
  M5 log-odds combination    calibrated confidence with a per-factor breakdown
  M6 MAD z-score             significance vs the series' own EWMA baseline
  M7 cross-market convergence  co-movers on a shared driver (assembled in engine.py)
"""

from __future__ import annotations

import math

# M2 — the lag set the reference fixes, plus its guards.
LAGS = (0, 1, 2, 3, 7)
MIN_BUCKETS = 8          # overlapping daily buckets required
MIN_PERIODS = 2          # the pattern must appear in >= 2 separate periods
MIN_ABS_R = 0.5          # |rho| threshold


# ─── M2: rank correlation (Spearman) ──────────────────────────────────────────
def _rank(values: list[float]) -> list[float]:
    """Fractional ranks, averaging ties — required for a correct Spearman rho."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs)
    dy = sum((y - my) ** 2 for y in ys)
    if dx <= 0 or dy <= 0:
        return 0.0
    return num / math.sqrt(dx * dy)


def spearman(xs: list[float], ys: list[float]) -> float:
    """Spearman rank correlation — robust to the outliers that dominate news volume
    and price series, which is why the reference specifies it over Pearson."""
    if len(xs) < 2 or len(xs) != len(ys):
        return 0.0
    return pearson(_rank(xs), _rank(ys))


def align_at_lag(a: dict, b: dict, lag: int) -> tuple[list[float], list[float]]:
    """Pairs (a[d], b[d+lag]) over days both series cover — a LEADING b by `lag`.
    Keys are date-like objects supporting ordinal arithmetic."""
    from datetime import timedelta
    xs, ys = [], []
    for d in sorted(a):
        d2 = d + timedelta(days=lag)
        if d2 in b:
            xs.append(a[d])
            ys.append(b[d2])
    return xs, ys


def two_period_consistent(a: dict, b: dict, lag: int, min_r: float = 0.3) -> bool:
    """Guard: the relationship must hold in >= 2 separate periods. Split the aligned
    pairs chronologically and require both halves to agree in sign and strength —
    once is coincidence, twice is a pattern."""
    from datetime import timedelta
    pairs = [(a[d], b[d + timedelta(days=lag)]) for d in sorted(a)
             if (d + timedelta(days=lag)) in b]
    if len(pairs) < 4:
        return False
    mid = len(pairs) // 2
    r1 = spearman([x for x, _ in pairs[:mid]], [y for _, y in pairs[:mid]])
    r2 = spearman([x for x, _ in pairs[mid:]], [y for _, y in pairs[mid:]])
    return abs(r1) >= min_r and abs(r2) >= min_r and (r1 > 0) == (r2 > 0)


def best_lag(news: dict, market: dict, lags=LAGS) -> dict:
    """M2 — the lag at which news best precedes the market move, with the guards
    applied. `passed` is False when the evidence does not clear them."""
    best = {"lag": None, "rho": 0.0, "buckets": 0, "passed": False,
            "reason": "insufficient overlap"}
    for lag in lags:
        xs, ys = align_at_lag(news, market, lag)
        if len(xs) < 2:
            continue
        rho = spearman(xs, ys)
        if abs(rho) > abs(best["rho"]):
            best = {"lag": lag, "rho": round(rho, 3), "buckets": len(xs),
                    "passed": False, "reason": ""}
    if best["lag"] is None:
        return best
    if best["buckets"] < MIN_BUCKETS:
        best["reason"] = f"only {best['buckets']} overlapping buckets (need {MIN_BUCKETS})"
    elif abs(best["rho"]) < MIN_ABS_R:
        best["reason"] = f"|rho| {abs(best['rho']):.2f} below {MIN_ABS_R}"
    elif not two_period_consistent(news, market, best["lag"]):
        best["reason"] = f"not consistent across {MIN_PERIODS} separate periods"
    else:
        best["passed"] = True
        best["reason"] = "passed"
    return best


# ─── M4: centrality over the connected sub-graph ──────────────────────────────
def degree_centrality(edges: list[tuple]) -> dict:
    """Normalized degree: share of the sub-graph each node is connected to."""
    nodes = {n for e in edges for n in e[:2]}
    if len(nodes) < 2:
        return {n: 0.0 for n in nodes}
    deg: dict = {n: 0 for n in nodes}
    for a, b, *_ in edges:
        deg[a] += 1
        deg[b] += 1
    denom = len(nodes) - 1
    return {n: round(d / denom, 3) for n, d in deg.items()}


def pagerank(edges: list[tuple], damping: float = 0.85, iterations: int = 30) -> dict:
    """Weighted PageRank over an undirected sub-graph. Edges are (a, b) or
    (a, b, weight); weights let NPMI-strong links carry more importance."""
    nodes = sorted({n for e in edges for n in e[:2]})
    if not nodes:
        return {}
    if len(nodes) == 1:
        return {nodes[0]: 1.0}

    out: dict = {n: [] for n in nodes}
    for e in edges:
        a, b = e[0], e[1]
        w = float(e[2]) if len(e) > 2 and e[2] is not None else 1.0
        w = max(w, 1e-6)
        out[a].append((b, w))
        out[b].append((a, w))

    n = len(nodes)
    rank = {v: 1.0 / n for v in nodes}
    for _ in range(iterations):
        nxt = {v: (1.0 - damping) / n for v in nodes}
        for v in nodes:
            total = sum(w for _, w in out[v]) or 1.0
            for nb, w in out[v]:
                nxt[nb] += damping * rank[v] * (w / total)
        rank = nxt
    total = sum(rank.values()) or 1.0
    return {v: round(r / total, 4) for v, r in rank.items()}


# ─── M5: log-odds evidence combination ────────────────────────────────────────
# A saturated factor must not swamp the sum: logit(1.0) is unbounded, so a single
# maxed-out signal would alone drive confidence to 100%. Squashing every strength
# into an open interval caps any one factor's contribution at ±logit(0.95) ≈ 2.94,
# so high confidence requires SEVERAL independent factors — which is the point of
# combining evidence at all.
STRENGTH_FLOOR = 0.05
STRENGTH_CEIL = 0.95


def logit(p: float) -> float:
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def _squash(strength: float) -> float:
    """Map a 0..1 strength into (FLOOR, CEIL) so no factor can dominate."""
    s = min(max(float(strength), 0.0), 1.0)
    return STRENGTH_FLOOR + s * (STRENGTH_CEIL - STRENGTH_FLOOR)


def sigmoid(x: float) -> float:
    if x <= -60:
        return 0.0
    if x >= 60:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def combine_log_odds(factors: list[dict], prior: float = 0.25) -> dict:
    """M5 — combine independent evidence factors in log-odds space.

        logit(confidence) = logit(prior) + Σ weightᵢ · logit(strengthᵢ)

    Each factor is {name, strength (0..1), weight}. Working in log-odds means
    evidence accumulates multiplicatively in probability space (the naive-Bayes
    form) instead of averaging, so several independent weak signals can together
    raise confidence while a single one cannot.

    Returns the score AND a per-factor contribution breakdown, so the card can show
    WHY the confidence is what it is.
    """
    # Weights are NORMALISED to sum to 1 and each contribution is CENTRED at a
    # neutral strength of 0.5. Two consequences that keep confidence honest:
    #   • all-neutral evidence returns exactly the prior (evidence must earn a move)
    #   • the total is bounded by ±logit(CEIL), so no amount of piling on reaches
    #     100% — the strongest possible case lands near 0.85, not certainty.
    raw = [max(float(f.get("weight", 1.0)), 0.0) for f in factors]
    wsum = sum(raw) or 1.0
    neutral = logit(0.5)

    total = logit(prior)
    breakdown = []
    for f, w in zip(factors, raw):
        strength = min(max(float(f.get("strength", 0.0)), 0.0), 1.0)
        contrib = (w / wsum) * (logit(_squash(strength)) - neutral)
        total += contrib
        breakdown.append({
            "factor": f.get("name", "factor"),
            "strength": round(strength, 3),
            "weight": round(w / wsum, 3),
            "contribution": round(contrib, 3),
            "detail": f.get("detail", ""),
        })
    return {"confidence": round(sigmoid(total), 3), "log_odds": round(total, 3),
            "prior": prior, "breakdown": breakdown}
