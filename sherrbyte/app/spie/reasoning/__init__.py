"""
reasoning — the SPIE Reasoning Engine.

Reusable and domain-neutral: it reasons over signals, not over any particular asset
or business. Given a focal signal (a significant market move in ANY asset class, or a
news spike) it assembles evidence-backed intelligence from data the app already has:

    news ↔ market link · connected entities (NPMI) · cross-market co-movement on a
    shared news driver · historical echo (pgvector) · evidence · confidence

Everything is deterministic, and the narrative is TEMPLATE-assembled from real fields
(reasoning/narrative.py) — never free-form LLM output, so it cannot invent a fact.
Observation language only: no forecasting, no causal claims.

    engine.reason_focal(conn, focal)  → reasoned_insight dict
    engine.run(conn)                  → reason + persist (insights type='reasoned')
"""

from app.spie.reasoning import confidence, engine, narrative

__all__ = ["confidence", "engine", "narrative"]
