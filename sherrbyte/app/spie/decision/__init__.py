"""Decision Engine — rule-based cross-domain chains + evidence aggregation.

Rules live in the chain_rules table (rules-as-data, weights-as-data); `rules.run`
evaluates enabled rules over recent domain_signals and writes cross_domain_chain
insights with log-odds confidence."""

from app.spie.decision import rules

__all__ = ["rules"]
