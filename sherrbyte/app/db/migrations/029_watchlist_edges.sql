-- 029_watchlist_edges.sql — the hand-authored edge graph, generalised beyond RIL.
--
-- 024_ril_proof.sql seeded FOUR edges for ONE company (Reliance Industries) into
-- sherrbyte_app.ril_edges. That proved the loop end to end. This migration keeps
-- that table exactly as it is (nothing here touches it) and generalises the SAME
-- shape so ANY entity can carry hand-authored edges, evaluated by the same daily
-- job — but only for the entities a watchlist actually names.
--
-- TWO tables, and the split is the whole point:
--
--   edge_watchlist   which entities are "on the watchlist". The daily job
--                    evaluates edges ONLY for entities named here — so adding a
--                    company to the engine's attention is one INSERT into this
--                    table, by hand, and removing it is one DELETE. Nothing else
--                    changes. RIL is seeded here so the existing proof keeps
--                    firing unchanged.
--
--   hand_edges       the generalised edge graph — ril_edges plus a `watch_entity`
--                    column saying which watchlisted entity each edge belongs to.
--                    HAND-AUTHORED ONLY. Exactly like ril_edges, the engine has
--                    NO INSERT path to this table: it only ever READS it, so it
--                    can never invent an edge. Rows arrive by migration seed or by
--                    a person's hand, never from automation and never from an LLM.
--
-- The four RIL edges are copied in under watch_entity='RIL' so the daily job,
-- once it reads hand_edges ∩ edge_watchlist, evaluates precisely what it did
-- before — the generalisation is structural, with zero behavioural change until
-- someone hand-authors an edge for a second entity.
--
-- SCHEMA-QUALIFIED sherrbyte_app, like 024. Idempotent: safe to re-run on boot.

CREATE SCHEMA IF NOT EXISTS sherrbyte_app;

-- ─── the watchlist: whose edges the daily job is allowed to evaluate ─────────
CREATE TABLE IF NOT EXISTS sherrbyte_app.edge_watchlist (
    watch_entity TEXT PRIMARY KEY,          -- the entity key hand_edges.watch_entity joins on
    note         TEXT NOT NULL DEFAULT '',  -- why it is watched, in one line, by hand
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO sherrbyte_app.edge_watchlist (watch_entity, note) VALUES
  ('RIL', 'Reliance Industries — the original proof run (migration 024). O2C/Jio/USD-INR edges.')
ON CONFLICT (watch_entity) DO NOTHING;

-- ─── the generalised hand-authored edge graph ───────────────────────────────
-- Same columns as ril_edges plus watch_entity. signal_keys still resolve to
-- detectors in app/spie/proof/signals.py; an edge on an entity whose signals are
-- not yet defined there simply never fires, which is correct — widening the
-- signal set is hand data-entry, not engineering (the same posture instrument_map
-- and linked_symbols already take).
CREATE TABLE IF NOT EXISTS sherrbyte_app.hand_edges (
    edge_key     TEXT PRIMARY KEY,          -- stable id the code joins on
    watch_entity TEXT NOT NULL,             -- which watchlisted entity this edge belongs to
    head         TEXT NOT NULL,             -- where the edge starts
    tail         TEXT NOT NULL,             -- what it transmits to
    mechanism    TEXT NOT NULL,             -- hand-written, plain English
    signal_keys  TEXT[] NOT NULL,           -- the observable signals on this edge
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hand_edges_watch_entity
    ON sherrbyte_app.hand_edges (watch_entity);

-- Copy the four RIL edges in under watch_entity='RIL'. INSERT ... SELECT from the
-- existing ril_edges so the mechanism strings stay identical to their authored
-- form — no re-typing, no chance of drift. ON CONFLICT DO NOTHING so a boot
-- re-run neither duplicates nor rewrites an edge a person edited by hand.
INSERT INTO sherrbyte_app.hand_edges (edge_key, watch_entity, head, tail, mechanism, signal_keys)
SELECT edge_key, 'RIL', head, tail, mechanism, signal_keys
  FROM sherrbyte_app.ril_edges
ON CONFLICT (edge_key) DO NOTHING;
