-- 023_analog_reactions.sql — cached reaction statistics for the analog engine.
--
-- One row per (symbol, event_class, horizon): "when events of this kind hit this
-- instrument before, how far did it move within h trading days, measured against
-- its own normal range at the time".
--
-- THE FULL BREAKDOWN IS STORED, NOT JUST THE SCORE. Every number a card shows
-- has to be traceable to a row here, and a score with no visible components
-- cannot be argued with, corrected, or audited by a regulator.
--
-- NOTHING HERE IS A PREDICTION. Every column is a count or a dispersion measure
-- over events that already happened. signal_strength is a 0-100 ranking integer,
-- never a probability and never rendered as a percentage.

CREATE TABLE IF NOT EXISTS analog_reactions (
    id              BIGSERIAL PRIMARY KEY,

    symbol          TEXT    NOT NULL,        -- market_ticks.symbol (a ticker)
    event_class     TEXT    NOT NULL,        -- hist_events.event_class
    horizon_days    INTEGER NOT NULL,        -- 1 | 3 | 5 | 10 trading days

    -- ── the sample ──────────────────────────────────────────────────────────
    -- n_analogs is the count that SURVIVED every gate, including the 45-of-60
    -- trailing-session requirement. It is not the number of analogs matched.
    n_analogs       INTEGER NOT NULL,
    n_exceeded      INTEGER NOT NULL,        -- |z| >= 2.5

    -- ── the shape of the move ───────────────────────────────────────────────
    sign_agreement  DOUBLE PRECISION NOT NULL,   -- 0.5 = coin flip, 1.0 = all one way
    median_abs_z    DOUBLE PRECISION NOT NULL,
    dispersion      DOUBLE PRECISION NOT NULL,   -- IQR of z, NOT of |z|
    recency_weight  DOUBLE PRECISION NOT NULL,   -- mean exp(-age_days / 540)

    -- ── the ranking integer ─────────────────────────────────────────────────
    signal_strength INTEGER NOT NULL CHECK (signal_strength BETWEEN 0 AND 100),

    -- Per-analog cells: [{event_id, occurred_at, r, z, mad, age_days}, ...].
    -- This is what makes a card auditable — drop it and the score becomes an
    -- assertion instead of a summary.
    breakdown       JSONB   NOT NULL DEFAULT '[]'::jsonb,

    computed_at     TIMESTAMPTZ DEFAULT now(),

    UNIQUE (symbol, event_class, horizon_days),

    -- Phase 3's suppression rule, enforced by the schema rather than trusted to
    -- every future caller: a thin sample is worse than silence in a paid
    -- product, so a row that could not clear 5 analogs must not exist to be
    -- read back.
    CONSTRAINT analog_reactions_min_sample CHECK (n_analogs >= 5),
    CONSTRAINT analog_reactions_counts_sane CHECK (
        n_exceeded >= 0 AND n_exceeded <= n_analogs),
    CONSTRAINT analog_reactions_horizon_known CHECK (horizon_days IN (1, 3, 5, 10))
);

CREATE INDEX IF NOT EXISTS idx_analog_reactions_symbol
    ON analog_reactions (symbol, horizon_days);
CREATE INDEX IF NOT EXISTS idx_analog_reactions_strength
    ON analog_reactions (signal_strength DESC);
