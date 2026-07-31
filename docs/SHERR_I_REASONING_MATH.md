# Sherr-I Reasoning Engine — Mathematical Methods (M1–M7)
## Authoritative reference for `app/spie/reasoning/`

> **Provenance note.** The original `SHERR_I_REASONING_MATH.md` attachment did not arrive in
> the working session. This document is **transcribed from the specification as stated in
> the task prompt**, and reconciled line-by-line against the shipped implementation so the
> two cannot drift. If the original file is later supplied, replace this file with it and
> re-check the implementation against the differences — treat that version as canonical.

*Scope: **only M1–M7**. Anything marked V2 / V3 / DEFERRED below is documented so it is not
re-derived by accident, and is deliberately **not implemented**.*

---

## 0. What "reasoning" means here

The Reasoning Engine connects **news coverage** to **market moves** across all eight asset
classes, and states the connection with **evidence**. It replaces count-based cards
("FIFA and World Cup co-occurred 12 times") with reasoned intelligence.

Three properties hold everywhere in this module:

1. **Deterministic.** Every number is computed by a pure function over stored rows. No LLM
   is involved anywhere in reasoning — narrative is *template-filled*, never generated.
2. **Detection, not prediction.** The engine reports what *has* happened and what *has*
   co-occurred. It never states what *will* happen.
3. **Never fabricate to fill a card.** A window with thin overlap yields a short honest
   narrative or a "monitoring" state — not padded filler.

### Language contract

| Allowed | Forbidden |
|---|---|
| moved · rose · fell · coverage aligns with · connected to · preceded · observed · co-moved | will · causes / caused / causing · predict · forecast · expect · should · likely to · set to · poised to · impact on · due to |

Enforced by `narrative.violates_language_rules()` and asserted in `tests/test_reasoning.py`.
Correlation is never rendered as causation; a lead-lag relationship is stated as
"coverage has moved N days ahead of this instrument", never "news caused the move".

---

## M1 — NPMI (normalized pointwise mutual information)

**Question it answers:** are these two entities associated *beyond chance*, or do they merely
both appear a lot?

```
PMI(a,b)  = log( p(a,b) / (p(a)·p(b)) )
NPMI(a,b) = PMI(a,b) / −log p(a,b)              ∈ [−1, +1]
```

- Counts come from **cluster-deduped** documents (SimHash story clusters), so a wire story
  republished by 40 outlets counts once. Raw co-occurrence counts are not admissible input.
- `min_count = 3`, else NPMI is **NULL** — not 0. Absence of evidence is not evidence.
- +1 = always together, 0 = independent, −1 = never together.

Computed in `app/spie/graph/cooccurrence.py`; consumed by reasoning as edge weight and as the
`npmi_strength` confidence factor.

---

## M2 — Lagged Spearman rank correlation

**Question it answers:** did the news *precede* the market move, and by how long?

```
for lag in {0, 1, 2, 3, 7} days:
    pair (news_volume[d], market_move[d + lag])   # news LEADS market by `lag`
    rho = spearman(pairs)
best = argmax |rho|
```

Spearman (rank correlation), not Pearson: both daily news volume and daily returns are
heavy-tailed, and a single spike would otherwise dominate a Pearson coefficient. Ties take
averaged fractional ranks.

**Guards — all three must pass, or `passed = False` and the lag sentence is omitted:**

| Guard | Value | Why |
|---|---|---|
| `MIN_BUCKETS` | ≥ 8 overlapping daily buckets | fewer points make \|rho\| meaninglessly easy to hit |
| `MIN_ABS_R` | \|rho\| ≥ 0.5 | weak monotone agreement is not evidence |
| `MIN_PERIODS` | holds in ≥ 2 separate periods | pairs split chronologically; both halves must agree in **sign** and reach \|rho\| ≥ 0.3. Once is coincidence. |

A failed check returns a **stated `reason`** (`"only 5 overlapping buckets (need 8)"`), which
is what the confidence breakdown displays instead of silently scoring zero.

`methods.best_lag()` · `methods.spearman()` · `methods.two_period_consistent()`

---

## M3 — pgvector cosine similarity (historical echo)

**Question it answers:** has coverage that looks like *this* appeared before, and what moved
afterwards?

```
similarity = 1 − (embedding_a <=> embedding_b)        -- pgvector cosine distance
```

Executed in SQL against `story_clusters` over a 365-day lookback. For each similar prior
cluster the engine checks whether the same instrument moved in the **same direction** in the
following window, producing `followed_direction / similar_count`.

Honesty rules:
- `similar_count == 1` → *"Limited history: one prior similar cluster on record."* One instance
  is never rendered as a rate; the confidence factor is pinned to `THIN_HISTORY_STRENGTH = 0.30`
  rather than 1/1 = 100%.
- `similar_count == 0` → *"No comparable prior coverage on record yet."*
- Never claim a lookback longer than the data. With <30 days of history the engine does not
  say "over 90 days".

---

## M4 — Degree centrality / PageRank

**Question it answers:** of the entities connected to this story, which one is structurally
*central* — i.e. which is the actual hub rather than a bystander?

```
degree(v)   = deg(v) / (|V| − 1)                       -- normalized share of the sub-graph
PageRank    : rank ← (1−d)/n + d · Σ_{u→v} rank(u)·w(u,v)/Σw(u,·)
              d = 0.85, 30 iterations, undirected, NPMI-weighted
```

The sub-graph is the focal entity plus its M1 neighbours. Edge weights are NPMI, so
association strength — not raw link count — drives centrality. The winner is named in the
narrative as *"(most central: X)"*.

`methods.pagerank()` · `methods.degree_centrality()`

---

## M5 — Log-odds evidence combination

**Question it answers:** how well-evidenced is this reasoning? (Not: how likely is a future
move — there is no forecast anywhere in Sherr-I.)

```
logit(confidence) = logit(prior) + Σ wᵢ · ( logit(squash(strengthᵢ)) − logit(0.5) )
```

Log-odds, not a weighted average: averaging lets one strong factor mask everything else,
while log-odds accumulates evidence multiplicatively in probability space (the naive-Bayes
form), so several independent moderate signals can jointly justify confidence that no single
one can.

**Factors and weights** (in `confidence.WEIGHTS` — config, never inline at the call site, so
the eval loop can tune them without touching logic):

| Factor | Weight | Strength derived from |
|---|---|---|
| `source_diversity` | 1.0 | independent outlets, saturating at 6 |
| `npmi_strength` | 0.9 | mean M1 NPMI across the linked entities |
| `lag_evidence` | 0.8 | \|rho\| when M2 passed; 0.10 with the failure reason otherwise |
| `historical_consistency` | 0.8 | M3 `followed / similar`, pinned to 0.30 when history is thin |
| `cross_market` | 0.7 | M7 co-moving markets, saturating at 3 |

`PRIOR = 0.25` — a bare move with no evidence is not "confident".

**Two calibration properties the tests pin down:**

- **Weights are normalized to sum to 1 and each contribution is centred at neutral 0.5.**
  All-neutral evidence therefore returns *exactly* the prior — evidence has to earn a move
  away from it, in either direction.
- **Strengths are squashed into `[0.05, 0.95]`.** `logit(1.0)` is unbounded, so without this
  one saturated factor alone would reach 100%. With it, the strongest possible case lands
  near 0.85 and **certainty is unreachable**, which is the honest outcome for observational
  evidence.

Every factor's contribution is returned in `breakdown`, so the card can show *why* the
confidence is what it is.

Observed calibration ladder. The tests assert the *properties* — strict monotonicity, that a
single saturated factor cannot reach certainty, that the strongest case stays in (0.70, 0.95),
and that no one factor dominates the breakdown — rather than these exact figures, so weight
tuning does not break the suite:

| Evidence | Confidence |
|---|---|
| bare move, nothing else | 0.037 |
| 1 source | 0.053 |
| 6 sources and nothing else | 0.135 |
| moderate across several factors | 0.217 |
| strong across all five factors | 0.758 |

Confidence words: ≥0.80 high · ≥0.50 moderate · ≥0.30 limited · else low.

---

## M6 — MAD z-score vs EWMA baseline

**Question it answers:** is this move/spike significant relative to *this series' own*
normal behaviour?

```
z = 0.6745 · (x − median) / MAD          MAD = median(|xᵢ − median|)
baseline = EWMA of the series
```

Median absolute deviation, **not** standard deviation: news volume and returns are
heavy-tailed, and σ is inflated by the very spikes being detected — which makes real events
look normal. The 0.6745 factor puts MAD on a σ-comparable scale for normal data.

Used twice: to select focal market moves (`z_threshold = 2.0`, `min_history = 4`) and as the
significance gate on cross-market co-movers (`m6_threshold = 1.5`), so M7 cannot fill a card
with noise-level wiggles.

---

## M7 — Cross-market convergence

**Question it answers:** did the *same* coverage line up with moves in several markets at
once?

For the focal window, find other instruments in **different asset classes** that (a) moved
significantly by M6 and (b) share the focal entity set through M1 links. The count feeds the
`cross_market` confidence factor and the narrative states:

> "This coverage also aligns with Gold +1.10% and USD/INR +0.40% — a pattern observed across
> 3 markets."

Never "the news moved three markets". Alignment, not agency.

---

## Not built (V2 / V3 / DEFERRED)

Recorded so they are not re-derived by accident. **Do not implement without an explicit ask.**

| Method | Tier | Why deferred |
|---|---|---|
| Granger causality | V2 | needs stationarity testing + far more history than exists; "causality" naming also conflicts with the language contract |
| Transfer entropy | V3 | data-hungry; no interpretable card at current volume |
| Hawkes / self-exciting point process | V3 | requires dense timestamped event streams |
| Structural VAR, regime-switching models | V3 | forecast-shaped; out of scope by design |
| LLM-generated narrative | Rejected | narrative is template-filled; free generation can hallucinate facts not in the data |

---

## Implementation map

| Method | Code |
|---|---|
| M1 | `app/spie/graph/cooccurrence.py` (NPMI), consumed in `reasoning/engine.py::_related_entities` |
| M2 | `reasoning/methods.py::best_lag`, fed by `engine.py::_daily_news_series` / `_daily_market_series` |
| M3 | `engine.py::_historical_echo` (pgvector, SQL) |
| M4 | `reasoning/methods.py::pagerank` / `degree_centrality`, via `engine.py::_centrality` |
| M5 | `reasoning/confidence.py` → `reasoning/methods.py::combine_log_odds` |
| M6 | `engine.py::significant_market_moves`, and the M6 gate inside `_cross_market` |
| M7 | `engine.py::_cross_market` |
| Language contract | `reasoning/narrative.py` (`FORBIDDEN`, `violates_language_rules`) |

Output is persisted as an `insights` row of type `reasoned`, with the full method output in
`explain_json` and the template narrative in `explain_json.why`.
