-- 016_chain_rules.sql — Decision Engine: cross-domain chains as data (Sherr-I Task 5).
--
-- A "chain" is a rule: a list of conditions across different domains that must all
-- match within a time window while sharing a canonical entity (post entity-resolution).
-- Rules — and their log-odds weights — live in this table, NOT in code: adding a
-- new chain is adding a row. A nightly job evaluates enabled rules over recent
-- domain_signals and writes cross_domain_chain insights. Language: observed, never predicted.
--
-- condition = {domain, direction (+1/-1/0; 0 = wildcard unless "strict":true),
--              entity_scope (descriptive role), window_hours?}
-- weights_json = {"<condition_index>": weight, "_bias": prior}. Default weight 1.0,
--                bias 0.0. confidence = sigmoid( bias + Σ weight_i · credibility_i ).

CREATE TABLE IF NOT EXISTS chain_rules (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    description     TEXT DEFAULT '',
    conditions_json JSONB NOT NULL,
    weights_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
    window_hours    INTEGER DEFAULT 72,
    enabled         BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- ─── Seed 3 realistic Indian cross-domain chains ────────────────────────────

-- Chain 1 — Monsoon food-supply shock.
-- Real scenario: a rainfall anomaly hits a region, logistics/transport around it
-- takes a negative-news hit, and a food commodity's price moves up within days —
-- the classic weather → logistics → mandi-price cascade unique to India.
INSERT INTO chain_rules (name, description, conditions_json, weights_json, window_hours)
VALUES (
    'monsoon_food_supply_shock',
    'Rainfall anomaly + negative logistics news + food commodity price rise',
    '[{"domain":"weather","direction":1,"entity_scope":"location","window_hours":72},
      {"domain":"news","direction":-1,"entity_scope":"logistics","window_hours":72},
      {"domain":"commodities","direction":1,"entity_scope":"food_commodity","window_hours":72}]'::jsonb,
    '{"0":1.0,"1":1.2,"2":1.0,"_bias":-1.5}'::jsonb, 72
) ON CONFLICT (name) DO NOTHING;

-- Chain 2 — Policy/regulatory sector repricing.
-- Real scenario: a regulatory filing or policy move lands, the affected sector's
-- entities pick up negative news, and the sector index moves down — the
-- policy → sector-narrative → index-repricing pattern.
INSERT INTO chain_rules (name, description, conditions_json, weights_json, window_hours)
VALUES (
    'policy_sector_repricing',
    'Regulatory/policy filing + affected-sector negative news + sector index move',
    '[{"domain":"news","direction":0,"entity_scope":"policy","window_hours":96},
      {"domain":"news","direction":-1,"entity_scope":"affected_sector","window_hours":96},
      {"domain":"stocks","direction":-1,"entity_scope":"sector_index","window_hours":96}]'::jsonb,
    '{"0":1.0,"1":1.0,"2":1.3,"_bias":-1.5}'::jsonb, 96
) ON CONFLICT (name) DO NOTHING;

-- Chain 3 — Rupee-weakness flight to gold.
-- Real scenario: the rupee weakens (USD/INR up), precious metals rise, and FII
-- outflow news appears — the currency-stress → safe-haven → capital-flight pattern.
INSERT INTO chain_rules (name, description, conditions_json, weights_json, window_hours)
VALUES (
    'rupee_gold_flight',
    'USD/INR rise + gold/silver rise + FII outflow news',
    '[{"domain":"forex","direction":1,"entity_scope":"usdinr","window_hours":48},
      {"domain":"metals","direction":1,"entity_scope":"precious_metal","window_hours":48},
      {"domain":"news","direction":-1,"entity_scope":"fii_outflow","window_hours":48}]'::jsonb,
    '{"0":1.0,"1":1.0,"2":1.0,"_bias":-1.5}'::jsonb, 48
) ON CONFLICT (name) DO NOTHING;
