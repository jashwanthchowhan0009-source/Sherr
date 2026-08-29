# Sherr-I build progress

Resume from the first unchecked item. Update this file after every step.

## Architecture rule (do not violate)

Deterministic math decides whether a signal exists. The LLM is called **only
after** a signal fires, and **only** to write prose from a fixed evidence
payload. It never sees raw prices and never decides significance. If the math is
silent, no LLM call happens and nothing is rendered. **Silence is a valid
output.**

Not built, deliberately: Granger causality, GARCH, cointegration, intraday
lead-lag. ~90 daily closes cannot support them; they would produce spurious
results. No statsmodels, arch, torch, spacy, scikit-learn.

Stdlib `statistics` + SQL only — numpy is permitted by the brief but is not in
the deployed service's `requirements.txt`, and none of this math needs it.

## Placement rule

Everything lives in `sherrbyte/app/spie/` (knowledge, graph, discovery,
decision, reasoning). **No parallel `intelligence/` package.** The step list
named `intelligence/*.py`; the preamble overrides it, and the mapping is below.

---

## Coverage map — what already existed

| Step | Already covers it | Actually missing |
|---|---|---|
| 0 tick history | `020_market_ticks.sql`, `scripts/backfill_ticks.py`, `market_ticks.py` — all merged | **Running it.** Needs network to Yahoo/CoinGecko + a DSN |
| 1 anomaly | `discovery/anomaly_math.py` (median/ewma/mad/mad_zscore); `discovery/observation.py` scores news volume, EWMA centre, min 5 pts, z≥2.0 | Scoring **market_ticks daily returns**, **median** centre, 30-day window, min 40 obs, \|z\|≥2.5, typed result |
| 2 entity graph | `graph/cooccurrence.py` (learned, undirected, NPMI); `instrument_keywords`; `entity_ticker_map` | A **hand-authored directed causal** edge table + `traverse()` |
| 3 news matcher | `market_reaction._news_window`, `observation._news` — entity overlap over `domain_signals` | **±12h** window, and the **stub check** (`body_state`) — a placeholder is not evidence |
| 4 card | `reasoning/engine.py`, `narrative.py`, `confidence.py` produce reasoned insights | Pydantic **DecisionCard**, **signal_strength** (explicitly *not* confidence), ≥2-article gate, schema validation + one retry |
| 5 API | `/patterns` with the 72h window (PR #196) | `/api/sherr-i/patterns` (15-min cache), `/admin/sherr-i-status` |

New files created (all inside `spie/`):
- `spie/discovery/tick_anomaly.py` — step 1
- `spie/graph/edges.py` + `db/migrations/021_entity_edges.sql` — step 2
- `spie/discovery/news_match.py` — step 3
- `spie/reasoning/card.py` — step 4

---

## Checklist

- [x] **Step 0 — tick history.** Code merged. **Backfill NOT RUN** — this
      sandbox's network policy returns 403 for `query1.finance.yahoo.com` and
      `api.coingecko.com`, and there is no `DATABASE_URL` here. Run
      `GET /admin/backfill-ticks?token=…` on Render, then
      `GET /admin/sherr-i-status?token=…` for the per-symbol counts.
- [x] **/patterns diagnosis.** Root cause found and fixed earlier in this
      branch's history: nothing in production ever ran the detectors. The
      engine's scheduler lives in `sherrbyte/app/main.py`, which `render.yaml`
      does not start, and the Actions cron cannot reach the DB — so every
      insight carried the seed date. Fixed by the in-process nightly job
      (02:10 UTC) plus `GET /admin/run-detectors`. `source` is `"seed"` only
      when no DSN is set and `"unavailable"` only when the query raised; an
      empty `insights` table correctly reports `"engine"` with no rows.
- [x] **Step 1 — anomaly detector** (`spie/discovery/tick_anomaly.py`)
- [x] **Step 2 — entity graph** (`spie/graph/edges.py`, migration 021)
- [x] **Step 3 — news matcher** (`spie/discovery/news_match.py`)
- [x] **Step 4 — card synthesis** (`spie/reasoning/card.py`)
- [x] **Step 5 — API + wiring** (`/api/sherr-i/patterns`, `/admin/sherr-i-status`)
- [x] Full test suite green — **401 passing**
- [ ] **Backfill run against production** — the only remaining blocker; see Step 0

## What a future session should do first

1. `GET /admin/backfill-ticks?token=…` — poll to `complete`
2. `GET /admin/sherr-i-status?token=…` — symbols with ≥40 closes vs total
3. `GET /admin/run-detectors?token=…`
4. `GET /api/sherr-i/patterns` — cards, or an honest empty list

---

## Verified end to end (local Postgres, synthetic ticks)

Six symbols x 90 closes, one planted shock on WTI Crude, three real articles and
one stub inside the +/-12h window:

```
COVERAGE  total=6 scoreable=6 min=40 short=[]
  ANOMALY CL=F   z=17.524  pct=8.2673  n=30      <- planted
  ANOMALY ^TNX   z=-3.674  pct=-0.8733 n=30      <- emergent from the walk
FUNNEL  anomalies=2  with_graph_paths=2  with_enough_articles=1
        cards=1  skipped_no_articles=1  llm_calls={attempted:1, succeeded:1}
  CARD  signal_strength=83  evidence=3  entities=8
        "WTI Crude rose 8.27% alongside the coverage below."
```

Three things this proves:

- **The architecture rule holds.** ^TNX triggered the math but found no
  corroborating articles, so it cost **zero** LLM calls. One anomaly, one call.
- **The stub was excluded.** Four articles sat in the window; three are cited.
  The placeholder one is not evidence.
- **Silence is reachable.** `skipped_no_articles=1` is a normal outcome, not an
  error, and the funnel names which stage stopped it.

## Two bugs found while wiring this

- `news_match` compared `published_at` against timestamp parameters, but the
  app's `articles.published_at` is **TEXT** (sqlite-shaped schema through
  pgcompat). Comparing it as text is worse than failing: `now()::text` writes
  `2026-08-28 12:00` and the ingest writes `2026-08-28T12:00`, which do not sort
  together across the `T`. The column is cast, with a regex guard so one
  unparseable row cannot abort the whole match.
- The card payload carried the raw ticker, so it read "CL=F rose 8.27%" — a
  symbol no reader has seen. It carries the display name now.
