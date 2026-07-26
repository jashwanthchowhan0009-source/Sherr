-- 010_entity_resolution.sql — canonical entity registry (Intelligence Engine V1, Step 1).
--
-- Every entity mention extracted by the understander ("Tata Motors Ltd", "TaMo",
-- "Tata Motors") must collapse to ONE canonical entity id before any co-occurrence
-- or correlation work — otherwise the counts that drive every detector are corrupt.
--
-- Two tables:
--   entities        — one row per real-world thing, keyed by a deterministic
--                     normalized form + a coarse NER type (ORG/PERSON/GPE/MISC).
--   entity_aliases  — every surface form seen for that entity, so the next time
--                     the same string (or a seeded synonym) appears we resolve in
--                     one indexed lookup instead of re-normalizing.
-- Idempotent: safe to re-run on every boot (guarded CREATE ... IF NOT EXISTS).

-- ─── Canonical entities ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS entities (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name TEXT NOT NULL,                 -- display form, e.g. "Tata Motors"
    type           TEXT NOT NULL DEFAULT 'MISC',  -- coarse: ORG | PERSON | GPE | MISC
    norm_key       TEXT NOT NULL,                 -- deterministic normalized key
    mention_count  INTEGER DEFAULT 0,             -- how often resolved (popularity)
    created_at     TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now(),
    -- Same normalized key under two coarse types stays distinct on purpose:
    -- "Jordan"(GPE) and "Jordan"(PERSON) are different entities.
    UNIQUE (norm_key, type)
);
CREATE INDEX IF NOT EXISTS idx_entities_norm ON entities(norm_key);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);

-- ─── Aliases (surface forms + seeded synonyms → entity) ─────────────────────
CREATE TABLE IF NOT EXISTS entity_aliases (
    alias      TEXT NOT NULL,                 -- raw surface form as seen
    norm_alias TEXT NOT NULL,                 -- normalized form used for lookup
    entity_id  UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    source     TEXT NOT NULL DEFAULT 'auto',  -- auto | seed | manual
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (norm_alias, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_aliases_norm   ON entity_aliases(norm_alias);
CREATE INDEX IF NOT EXISTS idx_aliases_entity ON entity_aliases(entity_id);
