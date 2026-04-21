-- Sherlock Tier 2 memory: persisted investigation outcomes per instrument.
-- Sherlock reads these at the start of each investigation as prior context.

CREATE TABLE IF NOT EXISTS instrument_memory (
    id            TEXT        NOT NULL DEFAULT gen_random_uuid()::text,
    instrument_id TEXT        NOT NULL,
    entity_id     TEXT        NOT NULL,
    error_type    TEXT        NOT NULL DEFAULT '',
    stage         TEXT        NOT NULL DEFAULT '',
    classification TEXT       NOT NULL DEFAULT 'unknown',
    confidence    TEXT        NOT NULL DEFAULT 'low',
    summary       TEXT        NOT NULL DEFAULT '',
    recommendation TEXT       NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('instrument_memory', 'created_at',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists       => TRUE);

CREATE INDEX IF NOT EXISTS idx_memory_instrument_time
    ON instrument_memory (instrument_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_memory_entity
    ON instrument_memory (entity_id);

SELECT add_retention_policy('instrument_memory', INTERVAL '1 year', if_not_exists => TRUE);
