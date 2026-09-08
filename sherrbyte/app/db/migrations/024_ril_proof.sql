-- 024_ril_proof.sql — the 30-day proof run for ONE company: Reliance Industries.
--
-- Two tables, and they play opposite roles:
--
--   ril_edges       the HAND-AUTHORED graph. Four edges, seeded by hand in THIS
--                   file, each carrying a plain-English mechanism string written
--                   by a person. No automation and no LLM ever writes a row here.
--                   The engine's only job is to READ these four edges and ask
--                   "did two or more signals move on the same edge in the same
--                   window?" — it must never invent an edge, so it has no INSERT
--                   path to this table at all.
--
--   ril_proof_log   the PERMANENT evidence log. One row per firing, appended by
--                   the daily job and by the backwards run. It is the product
--                   evidence, so it is APPEND-ONLY: rows are inserted, never
--                   updated and never deleted. Re-running a window inserts
--                   nothing it already holds (ON CONFLICT DO NOTHING on the
--                   natural key), which is how "never overwritten" is enforced
--                   in the schema rather than trusted to the caller.
--
-- SCHEMA-QUALIFIED ON PURPOSE, exactly like 020_market_ticks.sql. This DB carries
-- two apps. The engine (asyncpg) writes these rows with the schema named; the
-- deployed app (pgcompat, search_path=sherrbyte_app) reads them unqualified for
-- GET /admin/proof-log. Naming the schema in every statement means the file
-- lands in the same place whichever pool applies it.
--
-- Idempotent: safe to re-run on every boot.

CREATE SCHEMA IF NOT EXISTS sherrbyte_app;

-- ─── the hand-authored graph ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sherrbyte_app.ril_edges (
    edge_key     TEXT PRIMARY KEY,          -- stable id the log and code join on
    head         TEXT NOT NULL,             -- where the edge starts (an RIL arm)
    tail         TEXT NOT NULL,             -- what it transmits to
    mechanism    TEXT NOT NULL,             -- hand-written, plain English, by me
    signal_keys  TEXT[] NOT NULL,           -- the observable signals on this edge
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The four edges. This is the seed, by hand, and the mechanism strings are the
-- author's own words. ON CONFLICT DO NOTHING so a boot re-run neither duplicates
-- nor silently rewrites a mechanism someone edited in the table.
--
-- signal_keys name the signals the engine can actually observe on the edge. A
-- firing needs two or more of them to move in the same window; the keys resolve
-- to detectors in app/spie/proof/signals.py (a price instrument, or a financial-
-- news topic). They are NOT every node in the chain — "refining economics" and
-- "margins" are not separately priced, so the edge is observed through the crude
-- input and RIL's own move.
INSERT INTO sherrbyte_app.ril_edges (edge_key, head, tail, mechanism, signal_keys) VALUES
  ('o2c_brent',
   'RIL O2C', 'Brent crude',
   'RIL O2C -> Brent crude -> refining economics -> margins. When Brent moves, refining input costs and product-crack spreads move, and RIL''s O2C margins move with them.',
   ARRAY['ril','brent','refining_margin']),

  ('jio_telecom',
   'RIL Jio', 'telecom regulation',
   'RIL Jio -> telecom regulation -> tariffs -> ARPU. A regulatory or tariff change in telecom flows through to Jio''s average revenue per user.',
   ARRAY['ril','telecom_regulation']),

  ('o2c_competitor',
   'RIL O2C', 'competitor capacity',
   'RIL O2C -> competitor capacity -> supply/pricing pressure. New or lost competitor refining/petchem capacity changes supply and the pricing pressure on RIL''s O2C.',
   ARRAY['ril','competitor_capacity']),

  ('ril_usdinr',
   'RIL', 'USD/INR',
   'RIL -> USD/INR -> import bill and dollar revenue. A rupee move changes RIL''s crude import bill and the rupee value of its dollar-denominated revenue at the same time.',
   ARRAY['ril','usdinr'])
ON CONFLICT (edge_key) DO NOTHING;

-- ─── the permanent firing log ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sherrbyte_app.ril_proof_log (
    id                   BIGSERIAL PRIMARY KEY,
    edge_key             TEXT NOT NULL REFERENCES sherrbyte_app.ril_edges(edge_key),
    event_date           DATE NOT NULL,           -- the session the edge fired on
    run_kind             TEXT NOT NULL DEFAULT 'daily',  -- 'daily' | 'backfill'
    moved_signals        TEXT[] NOT NULL,         -- which signal_keys moved
    evidence_article_ids BIGINT[] NOT NULL DEFAULT '{}',  -- news evidence, if any
    signal_strength      INTEGER NOT NULL,        -- 0-100, math only, never a %
    noise_floor          INTEGER NOT NULL,        -- the measured bar beside it
    rendered             BOOLEAN NOT NULL DEFAULT FALSE,  -- did it reach a card?
    -- Forward outcome, filled by the backwards run: what RIL did over the next
    -- 1/3/5/10 sessions, and whether each move cleared RIL's own normal range.
    -- NULL until measured (a firing too recent to have 10 forward sessions).
    fwd_z                JSONB,                   -- {"1": z1, "3": z3, ...}
    fwd_exceeded         JSONB,                   -- {"1": bool, ...} |z| >= threshold
    logged_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The natural key that makes the log append-only in practice: one firing per
-- (edge, date). A re-run of the same window inserts nothing new and rewrites
-- nothing old. This is the schema-level guarantee behind "never overwritten or
-- pruned".
CREATE UNIQUE INDEX IF NOT EXISTS uq_ril_proof_log_edge_day
    ON sherrbyte_app.ril_proof_log (edge_key, event_date);

-- The read paths: the whole log by date, and per-edge firing counts.
CREATE INDEX IF NOT EXISTS idx_ril_proof_log_date
    ON sherrbyte_app.ril_proof_log (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_ril_proof_log_edge
    ON sherrbyte_app.ril_proof_log (edge_key);
