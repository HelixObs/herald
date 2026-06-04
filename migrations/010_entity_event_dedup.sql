-- Deduplication table for repeated helix.* span events.
--
-- entity_events is a TimescaleDB hypertable and cannot carry a unique constraint
-- on non-partition columns. This regular table acts as a dedup layer: on each
-- WriteEntityEvent call the herald upserts here (incrementing occurrence_count
-- and refreshing last_seen_at) and writes to entity_events only on the first
-- occurrence of each (entity_id, event_name, normalised_message) triple.
--
-- normalised_message is produced by fingerprint.Normalise — the same stripping
-- logic used by the notification system, so volatile tokens (paths, IDs, numbers)
-- do not create spurious distinct rows from a pipeliner retrying the same failure.

CREATE TABLE IF NOT EXISTS entity_event_dedup (
    entity_id          TEXT        NOT NULL,
    instrument_id      TEXT        NOT NULL,
    event_name         TEXT        NOT NULL,
    normalised_message TEXT        NOT NULL DEFAULT '',
    trace_id           TEXT,
    metadata           JSONB       NOT NULL DEFAULT '{}',
    occurrence_count   INT         NOT NULL DEFAULT 1,
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id, event_name, normalised_message)
);

-- Supports per-instrument alert queries filtered by event_name and time window.
CREATE INDEX IF NOT EXISTS idx_entity_event_dedup_instrument
    ON entity_event_dedup (instrument_id, event_name, last_seen_at DESC);
