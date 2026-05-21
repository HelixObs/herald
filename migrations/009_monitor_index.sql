-- Composite index for the monitor bins query.
-- Enables chunk pruning on created_at AND pushes down the instrument_id filter,
-- avoiding a full scan of 1M+ rows on every /api/v1/monitor/bins request.
CREATE INDEX IF NOT EXISTS entities_instrument_created_idx
    ON entities (instrument_id, created_at DESC);
