-- 018_expected_pairs.sql — the "we already knew that" blocklist for emergence.
--
-- Emergence fires on entity pairs that co-occur now and did not before. That is
-- statistically true and journalistically worthless for pairs that are definitionally
-- linked: FIFA and the World Cup, Apple and the iPhone, India and the RBI. Detecting
-- them makes the engine look naive, and they crowd out the connections a reader could
-- not have guessed.
--
-- Pairs are stored as NORMALIZED entity keys (entities.norm_key), lexically ordered
-- so lookup is direction-independent — (a,b) and (b,a) are the same row.
--
-- Add more freely: INSERT a row with the two norm_keys in sorted order. No code
-- change needed; the detector reads this table on every run.

CREATE TABLE IF NOT EXISTS expected_pairs (
    norm_a     TEXT NOT NULL,          -- lexically smaller normalized key
    norm_b     TEXT NOT NULL,          -- lexically larger normalized key
    reason     TEXT DEFAULT '',        -- why it is uninteresting, for auditability
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (norm_a, norm_b),
    CHECK (norm_a <= norm_b)
);

INSERT INTO expected_pairs (norm_a, norm_b, reason) VALUES
    ('fifa',      'world cup',              'the tournament and its governing body'),
    ('fifa',      'uefa',                   'the two governing bodies of the same sport'),
    ('uefa',      'world cup',              'confederation and tournament, definitionally linked'),
    ('champions league', 'uefa',            'the competition and its organiser'),
    ('argentina', 'fifa',                   'perennial participant, not a new link'),
    ('argentina', 'world cup',              'perennial participant, not a new link'),
    ('lionel messi', 'world cup',           'the sport''s most-covered player'),
    ('covid',     'fauci',                  'defining public role during the pandemic'),
    ('covid 19',  'fauci',                  'defining public role during the pandemic'),
    ('apple',     'iphone',                 'the company and its flagship product'),
    ('india',     'rbi',                    'the country and its central bank'),
    ('india',     'reserve bank of india',  'the country and its central bank'),
    ('nifty 50',  'sensex',                 'the two headline indices of the same market'),
    ('bitcoin',   'cryptocurrency',         'the asset and its category')
ON CONFLICT (norm_a, norm_b) DO NOTHING;
