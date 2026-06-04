-- 002_story_graph.sql — Connect stage: story threading & timeline linking.

-- A story thread groups info objects that describe the same evolving story
-- (e.g. an election, a court case, a launch sequence).
CREATE TABLE IF NOT EXISTS story_threads (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title      TEXT NOT NULL,
    topic      TEXT DEFAULT '',
    pillar_id  SMALLINT DEFAULT 1,
    centroid   vector(384),          -- running centroid of member embeddings
    node_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_threads_updated ON story_threads(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_threads_centroid_hnsw
    ON story_threads USING hnsw (centroid vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- A node ties one info object to its thread, with a position on the timeline
-- and an edge back to the node it most directly follows (backstory link).
CREATE TABLE IF NOT EXISTS story_nodes (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    thread_id      UUID NOT NULL REFERENCES story_threads(id) ON DELETE CASCADE,
    info_object_id UUID NOT NULL REFERENCES info_objects(id) ON DELETE CASCADE,
    parent_node_id UUID REFERENCES story_nodes(id) ON DELETE SET NULL,
    position       INTEGER DEFAULT 0,    -- chronological rank within the thread
    similarity     REAL DEFAULT 0.0,     -- cosine sim to the thread at link time
    created_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (thread_id, info_object_id)
);
CREATE INDEX IF NOT EXISTS idx_nodes_thread ON story_nodes(thread_id, position);
CREATE INDEX IF NOT EXISTS idx_nodes_info   ON story_nodes(info_object_id);

-- Wire info_objects.thread_id to story_threads now that the table exists.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_info_thread'
    ) THEN
        ALTER TABLE info_objects
            ADD CONSTRAINT fk_info_thread
            FOREIGN KEY (thread_id) REFERENCES story_threads(id) ON DELETE SET NULL;
    END IF;
END $$;
