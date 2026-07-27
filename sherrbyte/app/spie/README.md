# SPIE — SherrByte Pattern Intelligence Engine

SPIE is **five engines + one platform property**. This package is *organization*,
not a rewrite — all existing code and **all database tables keep their names**;
modules were moved into the engine they belong to.

```
app/spie/
├── knowledge/       Knowledge Engine
│   ├── entity_resolver.py   canonical entities + aliases (tables: entities, entity_aliases)
│   ├── signals.py           persist domain_signals (+ triggers co-occurrence)
│   └── adapters/            raw → universal Signal (news, stocks, commodities, metals, forex, weather)
├── graph/           Graph Engine
│   └── cooccurrence.py      materialized pair counts (table: cooccurrence); centrality/similarity (future)
├── discovery/       Discovery Engine
│   ├── base.py              write_insight + explainability helpers (table: insights)
│   ├── emergence.py         new entity-pair detector
│   ├── temporal.py          lag-correlation (leading-indicator) detector
│   └── correlation_math.py  pure pearson / lag / two-period math
├── decision/        Decision Engine
│   └── (rule-based cross-domain chains + evidence aggregation — later task)
└── recommendation/  Recommendation Engine
    └── (feed personalization — implemented under app/recommender/; recsys `signals` table untouched)
```

## The five engines

| Engine | Does | Code | Tables |
|---|---|---|---|
| **Knowledge** | Turns raw sources into resolved, canonical signals | `knowledge/` | `entities`, `entity_aliases`, `domain_signals` |
| **Graph** | Relationships between entities (co-occurrence, later centrality/similarity) | `graph/` | `cooccurrence` |
| **Discovery** | Finds patterns → writes insights | `discovery/` | `insights` |
| **Decision** | Rule-based cross-domain chains + evidence aggregation | `decision/` | `chain_rules` (later) |
| **Recommendation** | Personalized feed | `app/recommender/` | recsys `signals`, `feeds`, ALS factors |

## Platform property — Explainable by design

Not a module: **every** insight carries `explain_json` with `{why/method,
evidence counts (article_count / observations), sources (source_count /
top_sources), confidence}`. This is a hard requirement on every current and
future detector, surfaced directly in the app (SPRIE section).

## Rules

- No infrastructure beyond Postgres + Redis + APScheduler.
- Heavy compute at ingest / scheduled time, materialized; read paths are cheap SELECTs.
- Detection language only — "detected" / "historically observed", never "predicts" / "will".

## Ingest wiring

`app/pipeline/run_cycle` (the ingest orchestrator, unchanged) emits one news
Signal per article via `knowledge/adapters/news` → `knowledge/signals.persist_signals`,
which resolves entities and updates co-occurrence incrementally. Backfills and
detectors run as `app/workers/*` jobs and the nightly APScheduler job.
