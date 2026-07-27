"""
spie — SherrByte Pattern Intelligence Engine.

Five engines + one platform property (see app/spie/README.md):
  knowledge/      — entity resolution, domain signals, adapters, embeddings
  graph/          — co-occurrence graph, centrality, similarity
  discovery/      — detectors + insights (emergence, temporal correlation, …)
  decision/       — rule-based cross-domain chains, evidence aggregation
  recommendation/ — feed personalization (see app/recommender/)

Platform property — Explainable by design: every insight carries explain_json
{why/method, evidence counts, sources, confidence}. Enforced on all detectors.
Organization only — no database tables were renamed.
"""
