-- 030_recover_stranded_users.sql — recover app user data stranded in `public`.
--
-- THE FINDING (scripts/schema_audit.py, 2026-09-14)
-- =================================================
-- The deployed app (main.py, pgcompat) reads and writes with
-- search_path=sherrbyte_app; the engine workers write UNQUALIFIED, into `public`.
-- The audit shows the split is, for the CONTENT tables, already correct and must
-- stay that way:
--
--   articles          sherrbyte_app 47,720 (fresh, 07:01)  |  public 94,343 (engine-shaped)
--   topics            sherrbyte_app 168                    |  public 0
--   feeds             sherrbyte_app 12,297                 |  public 2,943 (Jul)
--   user_interactions sherrbyte_app 27                     |  public 0
--   insights          public 355 (live, engine)            |  sherrbyte_app 3 (orphan demo)
--
-- public.articles is the ENGINE's table and does NOT carry the app's columns
-- (status, ai_processed, originality_*, synthesis_sources, why/who/hook). Pointing
-- the app's search_path at `public` — the "one search_path change" fix — is exactly
-- the shadowing failure pgcompat.py was written to prevent: CREATE TABLE IF NOT
-- EXISTS finds the engine's table and every column-specific query then fails. So
-- the app STAYS on sherrbyte_app; that is the canonical schema for app tables.
--
-- What is actually broken is narrower: two user-facing tables have their historical
-- rows stranded in `public`, written before the app adopted the sherrbyte_app
-- search_path. The site now reads sherrbyte_app and sees almost none of them:
--
--   users             sherrbyte_app 1    |  public 29    (real accounts, Jul 2026)
--   user_preferences  sherrbyte_app 12   |  public 1,225
--
-- Those 29 accounts cannot log in — their credentials live in public.users, which
-- the app no longer reads. This migration RECOVERS them into sherrbyte_app.
--
-- WHY ONLY users + user_preferences
-- ---------------------------------
-- bookmarks and reading_progress are NOT migrated: their article_id points at
-- public.articles ids, which do not correspond to any sherrbyte_app.articles row,
-- so moving them would mis-link every bookmark to the wrong (or a missing) article.
-- They are left in place. insights (public) and the orphan sherrbyte_app.insights
-- are engine concerns handled elsewhere (scripts/oneoff_drop_legacy_insights.sql).
--
-- SAFE + IDEMPOTENT
-- -----------------
-- Purely ADDITIVE. Original ids are preserved so user_preferences.user_id keeps
-- pointing at the right person. Both id and email collisions are pre-filtered with
-- NOT EXISTS, so no row already in sherrbyte_app is ever touched or duplicated and
-- a re-run inserts nothing. The public copies are left intact as a recoverable
-- backup (nothing reads them while the app stays on sherrbyte_app, so they cannot
-- shadow anything). Guarded by to_regclass so it is a no-op on a fresh database
-- where one of the tables does not exist.

DO $$
BEGIN
    IF to_regclass('public.users') IS NULL
       OR to_regclass('sherrbyte_app.users') IS NULL THEN
        RAISE NOTICE '030: users table missing in one schema — skipping';
        RETURN;
    END IF;

    -- 1) Users: carry the original id, skip any id OR email already present.
    INSERT INTO sherrbyte_app.users (id, email, password, name, created_at, last_login)
    SELECT pu.id, pu.email, pu.password, pu.name, pu.created_at, pu.last_login
      FROM public.users pu
     WHERE NOT EXISTS (SELECT 1 FROM sherrbyte_app.users su  WHERE su.id = pu.id)
       AND NOT EXISTS (SELECT 1 FROM sherrbyte_app.users su2 WHERE lower(su2.email) = lower(pu.email));

    -- Keep the SERIAL ahead of the ids we just forced in, so the next signup does
    -- not collide with a recovered id.
    IF pg_get_serial_sequence('sherrbyte_app.users', 'id') IS NOT NULL THEN
        PERFORM setval(pg_get_serial_sequence('sherrbyte_app.users', 'id'),
                       GREATEST((SELECT COALESCE(MAX(id), 1) FROM sherrbyte_app.users), 1));
    END IF;

    -- 2) Preferences: only for users that now exist here, deduped on (user_id, topic).
    IF to_regclass('public.user_preferences') IS NOT NULL
       AND to_regclass('sherrbyte_app.user_preferences') IS NOT NULL THEN
        INSERT INTO sherrbyte_app.user_preferences (user_id, topic_name, pillar_id, weight)
        SELECT pp.user_id, pp.topic_name, pp.pillar_id, pp.weight
          FROM public.user_preferences pp
         WHERE EXISTS (SELECT 1 FROM sherrbyte_app.users su WHERE su.id = pp.user_id)
           AND NOT EXISTS (
                 SELECT 1 FROM sherrbyte_app.user_preferences sp
                  WHERE sp.user_id = pp.user_id AND sp.topic_name = pp.topic_name);
    END IF;
END $$;
